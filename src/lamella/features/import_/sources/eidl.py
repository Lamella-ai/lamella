# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""SBA EIDL amortization schedule.

This source is a counterparty sheet (see CONVENTIONS §2): the rows provide
principal/interest splits that match a corresponding WF debit via
`Assets:Clearing:EIDL`. The importer ingests the raw schedule; the
categorizer + transfers.detect pass handles the clearing-account routing.
"""
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
        date = parse_date(g(row, "Date")) or parse_date(g(row, "Payment Date"))
        amount = safe_decimal(g(row, "Amount")) or safe_decimal(g(row, "Payment"))
        principal = safe_decimal(g(row, "Principal"))
        interest = safe_decimal(g(row, "Interest"))
        if date is None and amount is None:
            continue
        raw = row_to_raw(row.to_dict())
        # Decimal isn't JSON-natively serializable; stash as canonical
        # string so the raw envelope round-trips losslessly.
        raw["_principal"] = str(principal) if principal is not None else None
        raw["_interest"] = str(interest) if interest is not None else None
        insert_raw_row(
            conn,
            source_id=source_id,
            row_num=row_num,
            raw=raw,
            date=date,
            amount=amount,
            payee="SBA EIDL",
            description="EIDL payment",
            memo=clean_str(g(row, "Note")),
        )
        n += 1
    log.info("eidl.ingest_sheet source_id=%d rows=%d", source_id, n)
    return n
