# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WF annotated 13-col ingester (ported from importer_bundle/importers/ingest_wf.py).

Columns:
  Master Category | Subcategory | Date | Location | Payee | Description |
  Payment Method | Amount | Business Expense? | Business | Expense Category |
  Amount.1 | Expense Memo
"""
from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal
from pathlib import Path

from lamella.features.import_._db import insert_raw_row, is_deducted_elsewhere
from lamella.features.import_._pandas_helpers import (
    clean_str,
    parse_date,
    read_tabular,
    row_to_raw,
    safe_decimal,
)

log = logging.getLogger(__name__)


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

    def get(row, key: str):
        c = col_map.get(key.lower())
        return row.get(c) if c else None

    n = 0
    for idx, row in df.iterrows():
        row_num = int(idx) + 2  # 1-based + header
        date = parse_date(get(row, "Date"))
        amount = safe_decimal(get(row, "Amount"))
        if date is None and amount is None:
            continue
        if date is None and amount is not None and abs(amount) > Decimal("0.01"):
            continue
        description = clean_str(get(row, "Description"))
        memo = clean_str(get(row, "Expense Memo"))
        ded = is_deducted_elsewhere(description, memo)
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=row_to_raw(row.to_dict()),
            date=date,
            amount=amount,
            payee=clean_str(get(row, "Payee")),
            description=description,
            memo=memo,
            location=clean_str(get(row, "Location")),
            payment_method=clean_str(get(row, "Payment Method")),
            ann_master_category=clean_str(get(row, "Master Category")),
            ann_subcategory=clean_str(get(row, "Subcategory")),
            ann_business_expense=clean_str(get(row, "Business Expense?")),
            ann_business=clean_str(get(row, "Business")),
            ann_expense_category=clean_str(get(row, "Expense Category")),
            ann_expense_memo=memo,
            ann_amount2=safe_decimal(get(row, "Amount.1")),
            is_deducted_elsewhere_flag=1 if ded else 0,
        )
        n += 1
    log.info("wf.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
