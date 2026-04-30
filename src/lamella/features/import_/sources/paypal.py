# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""PayPal ingester — single-sheet port of importer_bundle/ingest_paypal_v2.py.

The bundle's v2 processes all tabs of a workbook at once so annotation tabs
can enrich base rows. Phase 7's `ingest_sheet(source_id, ...)` contract
processes one sheet at a time; cross-sheet enrichment happens afterwards in
`importer.categorize.apply` which reads the `_tabs` metadata we write to
`raw_json`.

Filter: Status == 'Completed' or 'Paid' (PayPal pending/removed/denied rows
would otherwise appear as ghost transactions).

Sign convention: PayPal's `Gross` column is already signed from the PayPal
balance POV (payouts are negative, receipts positive) — same as bundle.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from lamella.features.import_._db import insert_raw_row
from lamella.features.import_._pandas_helpers import clean_str, read_tabular, row_to_raw

log = logging.getLogger(__name__)

_ACCEPT_STATUS = {"completed", "paid", "partially paid"}

_DATE_RE_1 = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})")
_DATE_RE_2 = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})")


def _parse_decimal(s: Any) -> Decimal | None:
    """ADR-0022: PayPal money parsed into Decimal, never float."""
    if s is None or s == "":
        return None
    if isinstance(s, Decimal):
        return s
    if isinstance(s, (int, float)):
        try:
            return Decimal(str(s))
        except (InvalidOperation, ValueError):
            return None
    txt = str(s).strip().replace("$", "").replace(",", "").replace("\u00a0", "")
    if not txt or txt.lower() in ("nan", "none"):
        return None
    if txt.startswith("(") and txt.endswith(")"):
        txt = "-" + txt[1:-1]
    try:
        return Decimal(txt)
    except (InvalidOperation, ValueError):
        return None


def _parse_date(s: Any) -> str | None:
    if s is None:
        return None
    txt = str(s).strip()
    if not txt:
        return None
    m = _DATE_RE_2.match(txt)
    if m:
        y, mth, d = m.groups()
        return f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"
    m = _DATE_RE_1.match(txt)
    if m:
        mth, d, y = m.groups()
        yi = int(y)
        if yi < 100:
            yi += 2000 if yi < 70 else 1900
        return f"{yi:04d}-{int(mth):02d}-{int(d):02d}"
    return None


def _col(row: dict, col_map: dict, key: str) -> str:
    c = col_map.get(key.lower())
    if c is None:
        return ""
    v = row.get(c)
    if v is None:
        return ""
    return str(v).strip()


def ingest_sheet(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    column_map: dict | None = None,
) -> int:
    df = read_tabular(path, sheet_name)
    col_map = {str(c).lower().strip(): c for c in df.columns}

    # Annotation column detection — any of these present means we should
    # carry ann_* fields for this sheet.
    ann_headers = {
        "business expense?",
        "business expense",
        "expense category",
        "expense memo",
        "expense amount",
        "tax category",
    }
    has_ann = bool(ann_headers & set(col_map.keys()))

    row_num = 1
    n = 0
    for _, row in df.iterrows():
        row_num += 1
        rd = row.to_dict()
        status = _col(rd, col_map, "status")
        if status.lower() not in _ACCEPT_STATUS:
            continue
        txn_id = _col(rd, col_map, "transaction id")
        if not txn_id:
            continue
        date = _parse_date(_col(rd, col_map, "date"))
        gross = _parse_decimal(_col(rd, col_map, "gross"))
        net = _parse_decimal(_col(rd, col_map, "net"))
        amount = gross if gross is not None else net
        payee = _col(rd, col_map, "name") or None
        ttype = _col(rd, col_map, "type") or None
        item = _col(rd, col_map, "item title") or None
        subj = _col(rd, col_map, "subject") or None
        note = _col(rd, col_map, "note") or None
        desc_parts = [x for x in (ttype, status, item, subj, note) if x]
        desc = " | ".join(desc_parts)[:500] or None

        raw_meta: dict[str, Any] = {
            "_txn_id": txn_id,
            "_tabs": [sheet_name],
            "_type": ttype,
            "_status": status,
            "_from_email": _col(rd, col_map, "from email address") or None,
            "_to_email": _col(rd, col_map, "to email address") or None,
            "_gross": gross,
            "_fee": _parse_decimal(_col(rd, col_map, "fee")),
            "_net": net,
            "_sales_tax": _parse_decimal(_col(rd, col_map, "sales tax")),
            "_item_title": item,
            "_reference_txn_id": _col(rd, col_map, "reference txn id") or None,
            "_audit": _col(rd, col_map, "audit") or _col(rd, col_map, "in 1099k?") or None,
        }

        ann_business_expense = None
        ann_business = None
        ann_expense_category = None
        ann_expense_memo = None
        ann_amount2 = None
        if has_ann:
            ann_business_expense = (
                _col(rd, col_map, "business expense?")
                or _col(rd, col_map, "business expense")
                or None
            )
            ann_business = _col(rd, col_map, "business") or None
            ann_expense_category = (
                _col(rd, col_map, "expense category")
                or _col(rd, col_map, "tax category")
                or None
            )
            ann_expense_memo = (
                _col(rd, col_map, "expense memo")
                or _col(rd, col_map, "notes")
                or None
            )
            ann_amount2 = _parse_decimal(_col(rd, col_map, "expense amount"))

        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw={"_paypal": raw_meta, **row_to_raw(rd)},
            date=date,
            amount=amount,
            currency=_col(rd, col_map, "currency") or "USD",
            payee=payee,
            description=desc,
            memo=note or subj,
            transaction_id=txn_id,
            ann_business_expense=ann_business_expense,
            ann_business=ann_business,
            ann_expense_category=ann_expense_category,
            ann_expense_memo=ann_expense_memo,
            ann_amount2=ann_amount2,
        )
        n += 1
    log.info("paypal.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
