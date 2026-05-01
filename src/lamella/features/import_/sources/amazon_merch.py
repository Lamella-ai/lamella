# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Merch by Amazon royalties."""
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
        date = parse_date(g(row, "Date")) or parse_date(g(row, "Month"))
        royalty = safe_decimal(g(row, "Royalty")) or safe_decimal(g(row, "Amount"))
        if date is None and royalty is None:
            continue
        desc = clean_str(g(row, "Product Name")) or clean_str(g(row, "Design"))
        raw = row_to_raw(row.to_dict())
        raw["_seller"] = "amazon_merch"
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=raw,
            date=date,
            amount=royalty,
            payee="Merch by Amazon",
            description=desc,
            payment_method="Amazon Merch Balance",
        )
        n += 1
    log.info("amazon_merch.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
