# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""eBay Seller Hub export."""
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
        date = (
            parse_date(g(row, "Transaction Creation Date"))
            or parse_date(g(row, "Transaction Date"))
            or parse_date(g(row, "Date"))
        )
        gross = safe_decimal(g(row, "Gross")) or safe_decimal(g(row, "Net Amount"))
        ttype = clean_str(g(row, "Type"))
        order_id = clean_str(g(row, "Order Number")) or clean_str(g(row, "Transaction ID"))
        desc = clean_str(g(row, "Item Title")) or ttype
        if date is None and gross is None:
            continue
        raw = row_to_raw(row.to_dict())
        raw["_seller"] = "ebay"
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=raw,
            date=date,
            amount=gross,
            payee=clean_str(g(row, "Buyer Username")) or "eBay",
            description=desc,
            transaction_id=order_id,
            payment_method="eBay Balance",
        )
        n += 1
    log.info("ebay.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
