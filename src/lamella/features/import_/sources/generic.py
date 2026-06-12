# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generic ingester for generic_csv / generic_xlsx sources.

Uses a user-confirmed `column_map` (source_col -> canonical_name|None) to
project rows onto the same `raw_rows` shape as the known ingesters.
`column_map` is the `MappingResult.column_map` dict stored in
`sources.notes` at the end of the /import/{id}/map step.
"""
from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

from lamella.features.import_._db import insert_raw_row, is_deducted_elsewhere
from lamella.features.import_._pandas_helpers import (
    clean_str,
    parse_date,
    read_tabular,
    row_to_raw,
    safe_decimal,
)

log = logging.getLogger(__name__)


def _pick(row: dict, column_map: dict, canonical: str) -> Any:
    for src_col, target in column_map.items():
        if target == canonical:
            value = row.get(src_col)
            if value is not None:
                return value
    return None


def _pick_amount(row: dict, column_map: dict) -> Decimal | None:
    """If the mapping has multiple columns -> amount (e.g. Debit, Credit),
    sum signed values; Debit treated as negative, Credit positive."""
    total: Decimal | None = None
    any_val = False
    for src_col, target in column_map.items():
        if target != "amount":
            continue
        raw = row.get(src_col)
        if raw is None or raw == "":
            continue
        val = safe_decimal(raw)
        if val is None:
            continue
        any_val = True
        low = src_col.lower()
        if "debit" in low or "withdrawal" in low:
            val = -abs(val)
        elif "credit" in low or "deposit" in low:
            val = abs(val)
        total = (total or Decimal("0")) + val
    return total if any_val else None


def ingest_sheet(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    column_map: dict | None = None,
) -> int:
    if not column_map:
        raise ValueError("generic ingester requires a confirmed column_map")
    df = read_tabular(path, sheet_name)
    n = 0
    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        rd = row.to_dict()
        date = parse_date(_pick(rd, column_map, "date"))
        amount = _pick_amount(rd, column_map)
        if date is None and amount is None:
            continue
        payee = clean_str(_pick(rd, column_map, "payee"))
        description = clean_str(_pick(rd, column_map, "description"))
        memo = clean_str(_pick(rd, column_map, "memo"))
        ded = is_deducted_elsewhere(description, memo)
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=row_to_raw(rd),
            date=date,
            amount=amount,
            currency=clean_str(_pick(rd, column_map, "currency")) or "USD",
            payee=payee,
            description=description,
            memo=memo,
            location=clean_str(_pick(rd, column_map, "location")),
            payment_method=clean_str(_pick(rd, column_map, "payment_method")),
            transaction_id=clean_str(_pick(rd, column_map, "transaction_id")),
            ann_master_category=clean_str(_pick(rd, column_map, "ann_master_category")),
            ann_subcategory=clean_str(_pick(rd, column_map, "ann_subcategory")),
            ann_business_expense=clean_str(_pick(rd, column_map, "ann_business_expense")),
            ann_business=clean_str(_pick(rd, column_map, "ann_business")),
            ann_expense_category=clean_str(_pick(rd, column_map, "ann_expense_category")),
            ann_expense_memo=clean_str(_pick(rd, column_map, "ann_expense_memo")),
            ann_amount2=safe_decimal(_pick(rd, column_map, "ann_amount2")),
            is_deducted_elsewhere_flag=1 if ded else 0,
        )
        n += 1
    log.info("generic.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
