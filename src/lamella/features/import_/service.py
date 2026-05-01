# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""/import pipeline orchestrator.

Owns the state machine:
  uploaded → classified → mapped → ingested → categorized → committed
                                                    ↓
                                                 error / cancelled

`ImportService` exposes methods that correspond 1:1 to the route transitions:
  * `register_upload(...)` — hash, de-dup against existing uploads, save file.
  * `classify(...)` — populate `sources` rows via `importer.classify`.
  * `save_mapping(...)` — persist AI/user column mapping into sources.notes.
  * `ingest(...)` — run per-source ingesters for every `sources` row.
  * `categorize(...)` — async (uses AI).
  * `commit(...)` — async (uses LedgerReader + bean-check + rollback).
  * `cancel(...)`, `hard_delete(...)`.
"""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.fs import UnsafePathError, validate_safe_path
from lamella.features.import_ import classify as classify_mod
from lamella.features.import_ import emit as emit_mod
from lamella.features.import_ import ledger_dedup
from lamella.features.import_ import preview as preview_mod
from lamella.features.import_ import transfers
from lamella.features.import_._db import upsert_source
from lamella.features.import_.archive import (
    ALLOWED_FORMATS,
    archive_file,
)
from lamella.features.import_.categorize import categorize_import
from lamella.features.import_.mapping import MappingResult, serialize_mapping
from lamella.features.import_.sources import for_source_class
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)


STATUS_UPLOADED = "uploaded"
STATUS_CLASSIFIED = "classified"
STATUS_MAPPED = "mapped"
STATUS_INGESTED = "ingested"
STATUS_CATEGORIZED = "categorized"
STATUS_PREVIEWED = "previewed"
STATUS_COMMITTED = "committed"
STATUS_CANCELLED = "cancelled"
STATUS_ERROR = "error"

OPEN_STATES = (
    STATUS_CLASSIFIED,
    STATUS_MAPPED,
    STATUS_INGESTED,
    STATUS_CATEGORIZED,
    STATUS_PREVIEWED,
)


@dataclass
class ImportRecord:
    id: int
    created_at: str
    filename: str
    content_sha256: str
    stored_path: str
    status: str
    source_class: str | None
    entity: str | None
    rows_imported: int
    rows_committed: int
    error: str | None
    committed_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ImportRecord":
        return cls(
            id=int(row["id"]),
            created_at=str(row["created_at"]),
            filename=row["filename"],
            content_sha256=row["content_sha256"],
            stored_path=row["stored_path"],
            status=row["status"],
            source_class=row["source_class"],
            entity=row["entity"],
            rows_imported=int(row["rows_imported"] or 0),
            rows_committed=int(row["rows_committed"] or 0),
            error=row["error"],
            committed_at=row["committed_at"],
        )


@dataclass
class UploadOutcome:
    record: ImportRecord
    was_new: bool
    existing_status: str | None = None


@dataclass
class IngestSummary:
    per_source: dict[int, int] = field(default_factory=dict)
    transfers: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(self.per_source.values())


class ImportError_(Exception):
    """Import pipeline's own error class. Avoids shadowing the builtin name
    via the `_` suffix."""


class ImportService:
    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        settings: Settings,
        ai: AIService | None = None,
        reader: LedgerReader | None = None,
        reviews: ReviewService | None = None,
        rules: RuleService | None = None,
    ):
        self.conn = conn
        self.settings = settings
        self.ai = ai
        self.reader = reader
        self.reviews = reviews or ReviewService(conn)
        self.rules = rules or RuleService(conn)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def get(self, import_id: int) -> ImportRecord | None:
        row = self.conn.execute(
            "SELECT * FROM imports WHERE id = ?", (import_id,)
        ).fetchone()
        return ImportRecord.from_row(row) if row else None

    def list_recent(self, limit: int = 50) -> list[ImportRecord]:
        rows = self.conn.execute(
            "SELECT * FROM imports ORDER BY created_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [ImportRecord.from_row(r) for r in rows]

    def count_open(self) -> int:
        placeholders = ",".join("?" * len(OPEN_STATES))
        row = self.conn.execute(
            f"SELECT COUNT(*) AS n FROM imports WHERE status IN ({placeholders})",
            tuple(OPEN_STATES),
        ).fetchone()
        return int(row["n"] if row else 0)

    def list_sources(self, import_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM sources WHERE upload_id = ? ORDER BY id ASC",
            (import_id,),
        ).fetchall()

    def get_source(self, source_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM sources WHERE id = ?", (source_id,)
        ).fetchone()

    # ------------------------------------------------------------------
    # Upload registration
    # ------------------------------------------------------------------

    def register_upload(self, *, filename: str, body: bytes) -> UploadOutcome:
        sha = hashlib.sha256(body).hexdigest()
        existing = self.conn.execute(
            "SELECT * FROM imports WHERE content_sha256 = ?", (sha,)
        ).fetchone()
        if existing and existing["status"] not in (STATUS_CANCELLED, STATUS_ERROR):
            return UploadOutcome(
                record=ImportRecord.from_row(existing),
                was_new=False,
                existing_status=existing["status"],
            )
        upload_root = self.settings.import_upload_dir_resolved
        upload_root.mkdir(parents=True, exist_ok=True)

        # Create the imports row to get an id, then save the file beneath it.
        cursor = self.conn.execute(
            """
            INSERT INTO imports (filename, content_sha256, stored_path, status)
                 VALUES (?, ?, ?, ?)
            """,
            (filename, sha, "", STATUS_UPLOADED),
        )
        import_id = int(cursor.lastrowid)
        file_dir = upload_root / str(import_id)
        file_dir.mkdir(parents=True, exist_ok=True)
        safe_name = Path(filename).name or "upload"
        # ADR-0030: re-validate the resolved file path lands under
        # upload_root. ``Path(filename).name`` already strips any
        # directory components, so this is defense-in-depth against a
        # future change that lets the directory survive.
        try:
            file_path = validate_safe_path(
                file_dir / safe_name, allowed_roots=[upload_root],
            )
        except UnsafePathError as exc:
            raise ValueError(f"refusing unsafe upload filename: {exc}") from exc
        file_path.write_bytes(body)
        self.conn.execute(
            "UPDATE imports SET stored_path = ? WHERE id = ?",
            (str(file_path), import_id),
        )
        # ADR-0060 — also archive the upload under <ledger_dir>/imports/
        # so the file is part of ADR-0001 authoritative state and every
        # staged row can carry a {file_id, sheet, row} source_ref. The
        # archive call is idempotent (full-file SHA-256 dedup) so re-
        # uploading the same content reuses the existing file_id.
        # Format inferred from the extension; unknown extensions skip
        # archiving rather than crashing the upload — those land via
        # the legacy path-only flow until they're added to the
        # allowlist.
        ext = Path(filename).suffix.lower().lstrip(".")
        if ext in ALLOWED_FORMATS:
            try:
                archive_file(
                    self.conn,
                    ledger_dir=self.settings.ledger_dir,
                    content=body,
                    original_filename=filename,
                    source_format=ext,
                )
            except Exception:  # noqa: BLE001
                log.warning(
                    "register_upload: archive_file failed for %s — "
                    "continuing with legacy upload path",
                    filename,
                    exc_info=True,
                )
        record = self.get(import_id)
        assert record is not None
        return UploadOutcome(record=record, was_new=True)

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def classify(self, import_id: int) -> list[int]:
        # NOTE: this method classifies the SHEET (source_class /
        # sheet_type) — not the per-row transactions. The WP11 Site 6
        # principle-3 fix lives in importer/categorize.py
        # (categorize_import) where the per-row AI classify call is
        # made: claim_from_csv_row runs before the AI step and routes
        # loan-claimed rows to needs_review with a pointer to the
        # /settings/loans/{slug}/backfill flow.
        record = self.get(import_id)
        if record is None:
            raise ImportError_(f"import {import_id} not found")
        path = Path(record.stored_path)
        previews = preview_mod.preview_workbook(path)
        created: list[int] = []
        primary_class: str | None = None
        primary_entity: str | None = None
        for preview in previews:
            source_class, sheet_type, entity, notes_str = classify_mod.classify_source(
                record.filename,
                preview.sheet_name,
                preview.columns,
                preview.row_count,
            )
            src_id = upsert_source(
                self.conn,
                upload_id=import_id,
                path=record.filename,
                sheet_name=preview.sheet_name,
                sheet_type=sheet_type,
                source_class=source_class,
                entity=entity,
                notes=notes_str or None,
                rows_read=0,
            )
            created.append(src_id)
            if primary_class is None and sheet_type == "primary":
                primary_class = source_class
                primary_entity = entity
        self.conn.execute(
            """
            UPDATE imports
               SET status = ?, source_class = ?, entity = ?
             WHERE id = ?
            """,
            (STATUS_CLASSIFIED, primary_class, primary_entity, import_id),
        )
        return created

    def update_source_overrides(
        self,
        *,
        source_id: int,
        source_class: str | None = None,
        entity: str | None = None,
        sheet_type: str | None = None,
    ) -> None:
        fields: list[str] = []
        args: list[Any] = []
        if source_class is not None:
            fields.append("source_class = ?")
            args.append(source_class)
        if entity is not None:
            fields.append("entity = ?")
            args.append(entity)
        if sheet_type is not None:
            fields.append("sheet_type = ?")
            args.append(sheet_type)
        if not fields:
            return
        args.append(source_id)
        self.conn.execute(
            f"UPDATE sources SET {', '.join(fields)} WHERE id = ?", tuple(args)
        )

    def mark_classify_complete(self, import_id: int) -> bool:
        """Return True if all sources have known ingesters and the import can
        advance to 'mapped'. False if any source requires the column-map step.
        """
        sources = self.list_sources(import_id)
        needs_map = any(
            classify_mod.is_generic(s["source_class"])
            and s["sheet_type"] == "primary"
            for s in sources
        )
        status = STATUS_CLASSIFIED if needs_map else STATUS_MAPPED
        self.conn.execute(
            "UPDATE imports SET status = ? WHERE id = ?", (status, import_id)
        )
        return not needs_map

    # ------------------------------------------------------------------
    # Mapping
    # ------------------------------------------------------------------

    def save_mapping(self, *, source_id: int, mapping: MappingResult) -> None:
        blob = serialize_mapping(mapping)
        self.conn.execute(
            "UPDATE sources SET notes = ? WHERE id = ?", (blob, source_id)
        )
        row = self.conn.execute(
            "SELECT upload_id FROM sources WHERE id = ?", (source_id,)
        ).fetchone()
        if row is None:
            return
        import_id = int(row["upload_id"])
        # If every generic source now has a mapping, move to 'mapped'.
        remaining = self.conn.execute(
            """
            SELECT COUNT(*) AS n
              FROM sources
             WHERE upload_id = ?
               AND source_class IN ('generic_csv', 'generic_xlsx')
               AND sheet_type = 'primary'
               AND (notes IS NULL OR notes = '')
            """,
            (import_id,),
        ).fetchone()
        if int(remaining["n"]) == 0:
            self.conn.execute(
                "UPDATE imports SET status = ? WHERE id = ?",
                (STATUS_MAPPED, import_id),
            )

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def ingest(self, import_id: int) -> IngestSummary:
        record = self.get(import_id)
        if record is None:
            raise ImportError_(f"import {import_id} not found")
        path = Path(record.stored_path)
        summary = IngestSummary()
        sources = self.list_sources(import_id)
        for src in sources:
            if src["sheet_type"] != "primary":
                continue
            ingester = for_source_class(src["source_class"])
            if ingester is None:
                summary.errors.append(
                    f"source {src['id']}: no ingester for {src['source_class']!r}"
                )
                continue
            column_map = None
            if classify_mod.is_generic(src["source_class"]):
                blob = src["notes"]
                if not blob:
                    summary.errors.append(
                        f"source {src['id']}: generic source without column map"
                    )
                    continue
                try:
                    payload = json.loads(blob)
                    column_map = payload.get("column_map")
                except Exception:
                    column_map = None
                if not column_map:
                    summary.errors.append(
                        f"source {src['id']}: column map parse failed"
                    )
                    continue
            try:
                n = ingester(
                    self.conn,
                    int(src["id"]),
                    path,
                    src["sheet_name"] if src["sheet_name"] != "(csv)" else None,
                    column_map=column_map,
                )
                summary.per_source[int(src["id"])] = n
                self.conn.execute(
                    "UPDATE sources SET rows_read = ? WHERE id = ?",
                    (n, int(src["id"])),
                )
            except Exception as exc:  # noqa: BLE001
                summary.errors.append(
                    f"source {src['id']} ({src['sheet_name']}): {exc}"
                )
                self.conn.execute(
                    """
                    INSERT INTO import_notes (import_id, source_id, topic, body)
                         VALUES (?, ?, 'error', ?)
                    """,
                    (import_id, int(src["id"]), str(exc)),
                )

        # Transfers detection (scoped to this import_id).
        summary.transfers = transfers.detect(self.conn, import_id)

        rows_total = summary.total_rows
        status = STATUS_INGESTED if rows_total else STATUS_ERROR
        err = None if rows_total else ("no rows ingested; " + "; ".join(summary.errors))
        self.conn.execute(
            "UPDATE imports SET status = ?, rows_imported = ?, error = ? WHERE id = ?",
            (status, rows_total, err, import_id),
        )
        return summary

    # ------------------------------------------------------------------
    # Categorize
    # ------------------------------------------------------------------

    async def categorize(self, import_id: int):
        result = await categorize_import(
            self.conn,
            import_id=import_id,
            ai=self.ai,
            rules=self.rules,
            ai_confidence_threshold=self.settings.import_ai_confidence_threshold,
        )
        # Surface needs_review rows into the Phase 1 review queue.
        if result.needs_review:
            self._enqueue_review_rows(import_id)
        self.conn.execute(
            "UPDATE imports SET status = ? WHERE id = ?",
            (STATUS_CATEGORIZED, import_id),
        )
        # NEXTGEN Phase C2a: run the unified cross-source matcher now
        # that this upload's rows + categorizations are on the staging
        # surface. Pairs a newly-categorized CSV row with any pending
        # SimpleFIN row (or another CSV upload's row) that matches.
        try:
            from lamella.features.import_.staging import sweep as staging_sweep

            sweep_stats = staging_sweep(self.conn)
            if sweep_stats["applied"]:
                log.info(
                    "importer: matcher paired %d staged rows "
                    "(%d candidates found)",
                    sweep_stats["applied"], sweep_stats["found"],
                )
        except Exception:  # noqa: BLE001
            log.exception("importer: matcher sweep failed")
        return result

    def _enqueue_review_rows(self, import_id: int) -> None:
        rows = self.conn.execute(
            """
            SELECT rr.id
              FROM raw_rows rr
              JOIN sources s ON s.id = rr.source_id
              JOIN categorizations cat ON cat.raw_row_id = rr.id
             WHERE s.upload_id = ? AND cat.needs_review = 1
            """,
            (import_id,),
        ).fetchall()
        for row in rows:
            source_ref = f"import:{import_id}:row:{int(row['id'])}"
            existing = self.conn.execute(
                "SELECT 1 FROM review_queue WHERE kind = 'import_categorization' "
                "AND source_ref = ? AND resolved_at IS NULL",
                (source_ref,),
            ).fetchone()
            if existing is not None:
                continue
            self.reviews.enqueue(
                kind="import_categorization",
                source_ref=source_ref,
                priority=3,
            )

    # ------------------------------------------------------------------
    # Preview
    # ------------------------------------------------------------------

    def preview_rows(self, import_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT rr.id AS raw_row_id,
                   rr.date, rr.amount, rr.payee, rr.description, rr.payment_method,
                   cat.account, cat.entity, cat.confidence, cat.needs_review,
                   cat.reason,
                   COALESCE(cls.status, 'imported') AS status,
                   s.source_class, s.sheet_name,
                   rp.kind AS pair_kind
              FROM raw_rows rr
              JOIN sources s ON s.id = rr.source_id
              LEFT JOIN categorizations cat ON cat.raw_row_id = rr.id
              LEFT JOIN classifications cls ON cls.raw_row_id = rr.id
              LEFT JOIN row_pairs rp ON rp.row_a_id = rr.id OR rp.row_b_id = rr.id
             WHERE s.upload_id = ?
             ORDER BY rr.date, rr.id
            """,
            (import_id,),
        ).fetchall()

    def recategorize(
        self,
        *,
        raw_row_id: int,
        account: str,
        entity: str | None = None,
        schedule_c_category: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE categorizations
               SET account = ?,
                   entity = ?,
                   schedule_c_category = ?,
                   needs_review = 0,
                   confidence = 'user',
                   reason = 'User override on preview page',
                   decided_at = datetime('now')
             WHERE raw_row_id = ?
            """,
            (account, entity, schedule_c_category, raw_row_id),
        )
        source = self.conn.execute(
            """
            SELECT s.upload_id
              FROM raw_rows rr
              JOIN sources s ON s.id = rr.source_id
             WHERE rr.id = ?
            """,
            (raw_row_id,),
        ).fetchone()
        if source is not None:
            self.conn.execute(
                """
                INSERT INTO import_notes (import_id, raw_row_id, topic, body)
                     VALUES (?, ?, 'decision', ?)
                """,
                (
                    int(source["upload_id"]),
                    raw_row_id,
                    f"User set account={account} entity={entity} cat={schedule_c_category}",
                ),
            )

    # ------------------------------------------------------------------
    # Commit
    # ------------------------------------------------------------------

    def commit(self, import_id: int) -> emit_mod.EmitResult:
        if self.reader is None:
            raise ImportError_("LedgerReader not available — cannot commit")
        record = self.get(import_id)
        if record is None:
            raise ImportError_(f"import {import_id} not found")
        if record.status == STATUS_COMMITTED:
            raise ImportError_("import already committed")

        # 1. Cross-ledger dedup.
        ld_result = ledger_dedup.drop_duplicates(
            self.conn, import_id=import_id, reader=self.reader
        )

        # 2. Populate txn_postings.
        emit_mod.build_postings(self.conn, import_id)

        # 3. Write chunks + bean-check.
        output_dir = self.settings.import_ledger_output_dir_resolved
        main_bean = self.settings.ledger_main
        try:
            emit_result = emit_mod.emit_to_ledger(
                self.conn,
                import_id=import_id,
                main_bean=main_bean,
                output_dir=output_dir,
                run_check=True,
            )
        except BeanCheckError as exc:
            self.conn.execute(
                "UPDATE imports SET status = ?, error = ? WHERE id = ?",
                (STATUS_ERROR, str(exc)[:2000], import_id),
            )
            self.conn.execute(
                """
                INSERT INTO import_notes (import_id, topic, body)
                     VALUES (?, 'error', ?)
                """,
                (import_id, f"bean-check failed: {exc}"),
            )
            raise

        rows_committed = sum(emit_result.per_year.values())
        self.conn.execute(
            """
            UPDATE imports
               SET status = ?,
                   rows_committed = ?,
                   committed_at = datetime('now'),
                   error = NULL
             WHERE id = ?
            """,
            (STATUS_COMMITTED, rows_committed, import_id),
        )
        # Invalidate the ledger reader cache so Fava / next /review sees the new rows.
        self.reader.invalidate()
        return emit_result

    # ------------------------------------------------------------------
    # Cancel / hard delete
    # ------------------------------------------------------------------

    def cancel(self, import_id: int) -> None:
        record = self.get(import_id)
        if record is None:
            return
        # Remove uploaded file, keep the DB rows for audit.
        try:
            file_path = Path(record.stored_path)
            if file_path.exists():
                file_path.unlink()
            if file_path.parent.exists() and not any(file_path.parent.iterdir()):
                file_path.parent.rmdir()
        except Exception as exc:  # noqa: BLE001
            log.warning("cancel cleanup failed: %s", exc)
        self.conn.execute(
            "UPDATE imports SET status = ?, error = NULL WHERE id = ?",
            (STATUS_CANCELLED, import_id),
        )

    def hard_delete(self, import_id: int) -> None:
        record = self.get(import_id)
        if record is None:
            return
        if record.status not in (STATUS_CANCELLED, STATUS_ERROR):
            raise ImportError_("hard delete only allowed on cancelled or errored imports")
        # CASCADE deletes sources / raw_rows / etc. via FKs.
        self.conn.execute("DELETE FROM imports WHERE id = ?", (import_id,))
        # Also remove on-disk upload dir if still present.
        try:
            file_path = Path(record.stored_path)
            if file_path.parent.exists():
                shutil.rmtree(file_path.parent, ignore_errors=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("hard_delete cleanup failed: %s", exc)
