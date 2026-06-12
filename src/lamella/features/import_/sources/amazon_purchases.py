# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Amazon order history (own purchases)."""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from lamella.features.import_._db import insert_raw_row
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

    def g(row, k: str):
        c = col_map.get(k.lower())
        return row.get(c) if c else None

    n = 0
    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        date = parse_date(g(row, "Order Date")) or parse_date(g(row, "Date"))
        total = safe_decimal(g(row, "Order Total")) or safe_decimal(g(row, "Total"))
        if date is None and total is None:
            continue
        order_id = clean_str(g(row, "Order ID"))
        desc = clean_str(g(row, "Product Name")) or clean_str(g(row, "Items"))
        raw = row_to_raw(row.to_dict())
        raw["_buyer"] = "amazon"
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=raw,
            date=date,
            amount=(-abs(total)) if total is not None else None,
            payee="Amazon",
            description=desc,
            transaction_id=order_id,
            payment_method=clean_str(g(row, "Payment Instrument")),
        )
        n += 1
    log.info("amazon_purchases.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
