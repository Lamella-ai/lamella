# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shared DB helpers for the importer pipeline.

Replaces importer_bundle/importers/ledger_db.py's env-var-rooted LedgerDB
with thin functions that operate on a Connector sqlite3.Connection.
"""
from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import hashlib
import json
import math
import sqlite3
from decimal import Decimal
from typing import Any


DEDUCTED_ELSEWHERE_PATTERNS: tuple[str, ...] = (
    "deducted elsewhere",
    "deducted else where",
    "categorized elsewhere",
    "handled elsewhere",
    "tracked elsewhere",
    "split elsewhere",
    "paid via",
)


def is_deducted_elsewhere(*strings: Any) -> bool:
    joined = " ".join(str(s or "").lower() for s in strings)
    return any(p in joined for p in DEDUCTED_ELSEWHERE_PATTERNS)


def stable_hash(*parts: Any) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(repr(p).encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


def _json_default(o: Any) -> Any:
    if isinstance(o, (_dt.date, _dt.datetime)):
        return o.isoformat()
    if isinstance(o, _decimal.Decimal):
        return float(o)
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    try:
        import pandas as pd  # type: ignore

        if pd.isna(o):
            return None
    except Exception:
        pass
    if hasattr(o, "isoformat"):
        try:
            return o.isoformat()
        except Exception:
            pass
    return str(o)


def upsert_source(
    conn: sqlite3.Connection,
    *,
    upload_id: int,
    path: str,
    sheet_name: str,
    sheet_type: str,
    source_class: str,
    year: int | None = None,
    entity: str | None = None,
    notes: str | None = None,
    rows_read: int = 0,
) -> int:
    row = conn.execute(
        "SELECT id FROM sources WHERE upload_id=? AND sheet_name=?",
        (upload_id, sheet_name),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE sources SET
                year=?, sheet_type=?, source_class=?, entity=?,
                notes=?, rows_read=?, path=?, discovered_at=datetime('now')
            WHERE id=?
            """,
            (year, sheet_type, source_class, entity, notes, rows_read, path, row["id"]),
        )
        return int(row["id"])
    cur = conn.execute(
        """
        INSERT INTO sources
            (upload_id, year, path, sheet_name, sheet_type, source_class,
             entity, notes, rows_read)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (upload_id, year, path, sheet_name, sheet_type, source_class,
         entity, notes, rows_read),
    )
    return int(cur.lastrowid)


def insert_raw_row(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    row_num: int,
    raw: dict,
    date: str | None = None,
    amount: Decimal | None = None,
    currency: str = "USD",
    payee: str | None = None,
    description: str | None = None,
    memo: str | None = None,
    location: str | None = None,
    payment_method: str | None = None,
    transaction_id: str | None = None,
    ann_master_category: str | None = None,
    ann_subcategory: str | None = None,
    ann_business_expense: str | None = None,
    ann_business: str | None = None,
    ann_expense_category: str | None = None,
    ann_expense_memo: str | None = None,
    ann_amount2: Decimal | None = None,
    is_deducted_elsewhere_flag: int = 0,
) -> int:
    hk = stable_hash(date, amount, description, source_id)
    # raw_rows.amount / ann_amount2 are TEXT (post-migration 057) for
    # ADR-0022 Decimal fidelity. Bind Decimals as their canonical string
    # form so SQLite stores the exact value the parser produced.
    amount_text = str(amount) if amount is not None else None
    ann_amount2_text = str(ann_amount2) if ann_amount2 is not None else None
    cur = conn.execute(
        """
        INSERT OR REPLACE INTO raw_rows
            (source_id, row_num, date, amount, currency, payee, description, memo,
             location, payment_method, transaction_id,
             ann_master_category, ann_subcategory, ann_business_expense,
             ann_business, ann_expense_category, ann_expense_memo, ann_amount2,
             is_deducted_elsewhere, raw_json, hash_key)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_id, row_num, date, amount_text, currency,
            payee, description, memo, location, payment_method, transaction_id,
            ann_master_category, ann_subcategory, ann_business_expense,
            ann_business, ann_expense_category, ann_expense_memo, ann_amount2_text,
            is_deducted_elsewhere_flag,
            json.dumps(raw, default=_json_default), hk,
        ),
    )
    raw_row_id = int(cur.lastrowid)
    _mirror_to_staging(
        conn,
        raw_row_id=raw_row_id,
        source_id=source_id,
        row_num=row_num,
        date=date,
        amount=amount,
        currency=currency,
        payee=payee,
        description=description,
        memo=memo,
        hash_key=hk,
        raw=raw,
    )
    return raw_row_id


def _mirror_to_staging(
    conn: sqlite3.Connection,
    *,
    raw_row_id: int,
    source_id: int,
    row_num: int,
    date: str | None,
    amount: Decimal | None,
    currency: str,
    payee: str | None,
    description: str | None,
    memo: str | None,
    hash_key: str,
    raw: dict,
) -> None:
    """Mirror an importer raw row into the unified staging table.

    NEXTGEN.md Phase A: every source (SimpleFIN, CSV, paste, reboot)
    must land in ``staged_transactions`` so the transfer matcher and
    downstream pipeline see cross-source data in one place. The
    importer keeps writing to its own ``raw_rows`` for backward
    compatibility; this mirror adds the parallel staging row with the
    same content keyed by ``(source='csv', source_ref={raw_row_id})``.

    Skipped when staging tables aren't present (e.g., tests with a
    partial schema) — the importer must stay functional even on a DB
    that predates migration 021.
    """
    # Defer import to avoid a circular/boot-time dependency on the
    # staging package when running the importer standalone.
    try:
        from lamella.features.import_.staging.service import StagingService
    except Exception:
        return

    try:
        meta = conn.execute(
            "SELECT s.upload_id, s.sheet_name, im.content_sha256 "
            "FROM sources s "
            "LEFT JOIN imports im ON im.id = s.upload_id "
            "WHERE s.id = ?",
            (source_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return
    upload_id = meta["upload_id"] if meta else None
    sheet_name = meta["sheet_name"] if meta else None
    upload_sha = meta["content_sha256"] if meta else None

    if date is None or amount is None:
        # Staging rows need a posting date and amount to be useful to
        # the matcher. Skipping preserves backward compatibility with
        # any importer source that doesn't extract one of these.
        return

    # ADR-0060 — resolve the archive file_id when the upload's content
    # has been archived (register_upload arrange this idempotently).
    # When found, source_ref carries {file_id, sheet, row} so re-
    # importing the same archived file lands the same hash and the
    # staging upsert keeps state in place. Falls back to the legacy
    # session-scoped shape on the rare case the archive lookup
    # misses (older uploads, unknown extension, archive write failed).
    file_id: int | None = None
    if upload_sha:
        try:
            row = conn.execute(
                "SELECT id FROM imported_files WHERE content_sha256 = ?",
                (upload_sha,),
            ).fetchone()
            if row is not None:
                file_id = int(row["id"])
        except sqlite3.OperationalError:
            file_id = None

    if file_id is not None:
        # ADR-0060 — keep the dedup-keyed source_ref MINIMAL. The
        # source_ref_hash is computed over the canonicalized JSON of
        # the dict, so any value that varies across re-imports of
        # the same row (raw_row_id is autoincrement-fresh on every
        # `insert_raw_row` call) breaks idempotency. Supplementary
        # join keys (raw_row_id, source_id, hash_key) live on the
        # row's `raw_json` instead, where they don't influence the
        # hash but downstream readers can still resolve them.
        source_ref: dict = {
            "file_id": file_id,
            "row": row_num,
        }
        if sheet_name:
            source_ref["sheet"] = sheet_name
        # Supplementary context lives in raw, not source_ref.
        raw = {
            **raw,
            "_lamella_join": {
                "source_id": source_id,
                "raw_row_id": raw_row_id,
                "hash_key": hash_key,
                "upload_id": upload_id,
            },
        }
    else:
        source_ref = {
            "upload_id": upload_id,
            "source_id": source_id,
            "sheet_name": sheet_name,
            "row_num": row_num,
            "raw_row_id": raw_row_id,
            "hash_key": hash_key,
        }
    try:
        StagingService(conn).stage(
            source="csv",
            source_ref=source_ref,
            session_id=str(upload_id) if upload_id is not None else None,
            posting_date=str(date),
            amount=amount,
            currency=currency,
            payee=payee,
            description=description,
            memo=memo,
            raw=raw,
        )
    except sqlite3.OperationalError:
        # Staging tables missing on a legacy DB — log and skip so the
        # importer doesn't regress.
        return
