# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Local cache of Paperless documents.

The matcher needs to search Paperless by amount, date, and text to find
candidate receipts for a transaction. Running those queries live against the
Paperless API means N round-trips per page render and depends on Paperless
exposing amount as a filterable field (most installs don't). So we mirror the
metadata we care about into a local SQLite table and query it instead.

Sync model:
  * First run after boot / manual trigger: `full=True` — pulls every doc
    created within `lookback_days`, ordered by created asc (deterministic),
    upserts.
  * Subsequent runs: incremental — `modified__gt=<cursor>`, ordered by
    modified asc. Cursor = max(modified_at) observed.
  * Every sync also pulls correspondents + document_types lists so the
    denormalized name columns stay fresh.

The sync is idempotent: a repeated full pull is safe. Deletions are not
handled automatically — a stale row remains in paperless_doc_index until
manual cleanup. Paperless deletion events don't come through the REST API,
so detection would require a separate crawl; deferred.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.features.paperless_bridge.field_map import (
    FieldAccessor,
    get_map,
    sync_fields,
)
from lamella.adapters.paperless.schemas import Document

log = logging.getLogger(__name__)

CONTENT_EXCERPT_CHARS = 4000


@dataclass
class SyncResult:
    mode: str
    docs_seen: int = 0
    docs_written: int = 0
    error: str | None = None
    cursor_before: datetime | None = None
    cursor_after: datetime | None = None
    field_stats: dict[str, int] = field(default_factory=dict)


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def is_paperless_syncing(conn) -> bool:
    """Returns True when a Paperless sync is currently in flight.
    Callers that touch paperless_doc_index or call receipt_hunt
    should short-circuit while this is True — linking receipts
    while rows are landing mid-query produces stale/racing
    matches. Same check the AI enricher uses to defer its
    receipt-context fetches until sync completes."""
    try:
        row = conn.execute(
            "SELECT last_status FROM paperless_sync_state WHERE id = 1"
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    if row is None:
        return False
    return (row["last_status"] or "").strip().lower() == "syncing"


def _custom_fields_as_dicts(doc: Document) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for cf in doc.custom_fields:
        out.append({"field": cf.field, "value": cf.value})
    return out


class PaperlessSync:
    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        client: PaperlessClient,
        lookback_days: int = 730,
    ):
        self.conn = conn
        self.client = client
        self.lookback_days = lookback_days
        self._lock = asyncio.Lock()

    def _state(self) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT last_full_sync_at, last_incremental_sync_at, "
            "       last_modified_cursor, doc_count, last_error, last_status "
            "FROM paperless_sync_state WHERE id = 1"
        ).fetchone()
        return dict(row) if row else {}

    def _update_state(self, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values())
        self.conn.execute(
            f"UPDATE paperless_sync_state SET {assignments} WHERE id = 1",
            values,
        )

    async def sync(self, *, full: bool = False) -> SyncResult:
        """Full or incremental sync. Use `full=True` on first run or when
        the user explicitly re-seeds. Safe to call concurrently; the async
        lock serializes overlapping invocations.

        Marks `last_status='syncing'` for the duration so readers
        (receipt-hunt, /status) can short-circuit while the index is
        in flight — linking a receipt to a txn while the index is
        growing from 1,944 → 25,000 docs is unsafe because the
        matcher may race new rows landing mid-query.
        """
        async with self._lock:
            self._update_state(last_status="syncing")
            try:
                return await self._do_sync(full=full)
            finally:
                # _do_sync sets last_status to 'ok'/'error' on its
                # happy path; this finally catches the case where
                # it raised before marking final state.
                row = self._state()
                if (row.get("last_status") or "") == "syncing":
                    self._update_state(
                        last_status="error",
                        last_error="sync task exited without marking final state",
                    )

    async def _do_sync(self, *, full: bool) -> SyncResult:
        state = self._state()
        cursor_str = state.get("last_modified_cursor")
        cursor_before = _to_datetime(cursor_str) if cursor_str else None

        # Refresh the custom-fields mapping first so any new Paperless field
        # gets a row in paperless_field_map before we try to read docs.
        try:
            field_stats = await sync_fields(self.conn, self.client)
        except PaperlessError as exc:
            self._update_state(
                last_status="error", last_error=f"field sync: {exc}"
            )
            return SyncResult(mode="full" if full else "incremental", error=str(exc))

        # Pull correspondent/doc-type maps so we can denormalize into the index.
        try:
            correspondents = await self.client.get_correspondents()
        except PaperlessError as exc:
            correspondents = {}
            log.warning("paperless correspondents fetch failed: %s", exc)
        try:
            doc_types = await self.client.get_document_types()
        except PaperlessError as exc:
            doc_types = {}
            log.warning("paperless document types fetch failed: %s", exc)

        mapping = get_map(self.conn)

        params: dict[str, Any]
        if full or cursor_before is None:
            mode = "full"
            since = (date.today() - timedelta(days=self.lookback_days)).isoformat()
            params = {"created__date__gte": since, "ordering": "created"}
        else:
            mode = "incremental"
            # Paperless accepts ISO datetime for modified__gt
            params = {
                "modified__gt": cursor_before.isoformat(),
                "ordering": "modified",
            }

        result = SyncResult(
            mode=mode,
            cursor_before=cursor_before,
            field_stats=field_stats,
        )

        max_modified = cursor_before
        try:
            async for doc in self.client.iter_documents(params):
                result.docs_seen += 1
                self._upsert_doc(doc, correspondents, doc_types, mapping)
                result.docs_written += 1
                modified_dt = _to_datetime(doc.modified) or _to_datetime(doc.created)
                if modified_dt and (max_modified is None or modified_dt > max_modified):
                    max_modified = modified_dt
        except PaperlessError as exc:
            result.error = str(exc)
            self._update_state(
                last_status="error", last_error=f"{mode} sync: {exc}"
            )
            return result

        now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(
            sep=" ", timespec="seconds"
        )
        updates: dict[str, Any] = {
            "last_status": "ok",
            "last_error": None,
        }
        if mode == "full":
            updates["last_full_sync_at"] = now_iso
        updates["last_incremental_sync_at"] = now_iso
        if max_modified is not None:
            updates["last_modified_cursor"] = max_modified.isoformat(
                sep=" ", timespec="seconds"
            )
        # Refresh the doc count
        count_row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM paperless_doc_index"
        ).fetchone()
        updates["doc_count"] = int(count_row["n"]) if count_row else 0
        self._update_state(**updates)

        result.cursor_after = max_modified
        log.info(
            "paperless sync %s: seen=%d written=%d cursor_after=%s",
            mode, result.docs_seen, result.docs_written, max_modified,
        )
        return result

    def _upsert_doc(
        self,
        doc: Document,
        correspondents: dict[int, str],
        doc_types: dict[int, str],
        mapping,
    ) -> None:
        access = FieldAccessor(_custom_fields_as_dicts(doc), mapping)
        total = access.total
        subtotal = access.subtotal
        tax = access.tax
        vendor = access.vendor
        last_four = access.payment_last_four
        receipt_date = access.receipt_date

        content = (doc.content or "")[:CONTENT_EXCERPT_CHARS]
        created_date = _to_date(doc.created)
        modified_at = _to_datetime(doc.modified)
        tags_json = json.dumps(list(doc.tags))

        corr_name = correspondents.get(doc.correspondent) if doc.correspondent else None
        dt_name = doc_types.get(doc.document_type) if doc.document_type else None

        self.conn.execute(
            """
            INSERT INTO paperless_doc_index (
                paperless_id, title, correspondent_id, correspondent_name,
                document_type_id, document_type_name, created_date, modified_at,
                content_excerpt, total_amount, subtotal_amount, tax_amount,
                vendor, payment_last_four, receipt_date, last_synced_at, tags_json,
                original_checksum, mime_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
            ON CONFLICT(paperless_id) DO UPDATE SET
                title              = excluded.title,
                correspondent_id   = excluded.correspondent_id,
                correspondent_name = excluded.correspondent_name,
                document_type_id   = excluded.document_type_id,
                document_type_name = excluded.document_type_name,
                created_date       = excluded.created_date,
                modified_at        = excluded.modified_at,
                content_excerpt    = excluded.content_excerpt,
                total_amount       = excluded.total_amount,
                subtotal_amount    = excluded.subtotal_amount,
                tax_amount         = excluded.tax_amount,
                vendor             = excluded.vendor,
                payment_last_four  = excluded.payment_last_four,
                receipt_date       = excluded.receipt_date,
                last_synced_at     = CURRENT_TIMESTAMP,
                tags_json          = excluded.tags_json,
                original_checksum  = COALESCE(excluded.original_checksum, paperless_doc_index.original_checksum),
                mime_type          = COALESCE(excluded.mime_type, paperless_doc_index.mime_type)
            """,
            (
                doc.id,
                doc.title,
                doc.correspondent,
                corr_name,
                doc.document_type,
                dt_name,
                created_date.isoformat() if created_date else None,
                modified_at.isoformat(sep=" ", timespec="seconds") if modified_at else None,
                content,
                str(total) if total is not None else None,
                str(subtotal) if subtotal is not None else None,
                str(tax) if tax is not None else None,
                vendor,
                last_four,
                receipt_date.isoformat() if receipt_date else None,
                tags_json,
                doc.original_checksum,
                doc.mime_type,
            ),
        )
