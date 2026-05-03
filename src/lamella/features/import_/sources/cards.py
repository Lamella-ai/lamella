# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Amex + Warehouse Club/Citi card exports (shared module, matches bundle's
ingest_cards.py)."""
from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal
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


def _ingest(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    tag: str,
    payment_method: str,
    date_cols: tuple[str, ...],
    amount_cols: tuple[str, ...],
    amount_sign: Decimal = Decimal("1"),
) -> int:
    df = read_tabular(path, sheet_name)
    col_map = {str(c).lower().strip(): c for c in df.columns}

    def g(row, k: str):
        c = col_map.get(k.lower())
        return row.get(c) if c else None

    n = 0
    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        date = None
        for dc in date_cols:
            date = parse_date(g(row, dc))
            if date:
                break
        amount: Decimal | None = None
        for ac in amount_cols:
            amount = safe_decimal(g(row, ac))
            if amount is not None:
                break
        if amount is not None:
            amount *= amount_sign
        if date is None and amount is None:
            continue
        desc = clean_str(g(row, "Description")) or clean_str(g(row, "Details"))
        raw = row_to_raw(row.to_dict())
        raw["_card"] = tag
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=raw,
            date=date,
            amount=amount,
            payee=desc,
            description=desc,
            memo=clean_str(g(row, "Memo")),
            payment_method=payment_method,
        )
        n += 1
    log.info("%s.ingest_sheet source_id=%d rows=%d", tag, source_id, n)
    return n


def ingest_sheet_amex(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    column_map: dict | None = None,
) -> int:
    return _ingest(
        conn,
        source_id,
        path,
        sheet_name,
        tag="amex",
        payment_method="Amex",
        date_cols=("Date", "Transaction Date"),
        amount_cols=("Amount",),
        # Amex exports purchases as positive; our convention is liability +charge, -payment.
        amount_sign=Decimal("1"),
    )


def ingest_sheet_costco(
    conn: sqlite3.Connection,
    source_id: int,
    path: Path,
    sheet_name: str,
    *,
    column_map: dict | None = None,
) -> int:
    return _ingest(
        conn,
        source_id,
        path,
        sheet_name,
        tag="costco_citibank",
        payment_method="Warehouse Club Citi",
        date_cols=("Date", "Transaction Date", "Post Date"),
        amount_cols=("Amount", "Debit", "Credit"),
    )
