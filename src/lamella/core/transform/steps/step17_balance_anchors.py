# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 17: account_balance_anchors reconstruct."""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.dashboard.balances.reader import read_balance_anchors
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step17:balance_anchors",
    state_tables=["account_balance_anchors"],
)
def reconstruct_balance_anchors(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(account_balance_anchors)")]
    if not cols:
        return ReconstructReport(
            pass_name="step17:balance_anchors", rows_written=0,
            notes=["account_balance_anchors table not present — skip"],
        )
    written = 0
    for row in read_balance_anchors(entries):
        conn.execute(
            """
            INSERT INTO account_balance_anchors
                (account_path, as_of_date, balance, currency, source, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (account_path, as_of_date) DO UPDATE SET
                balance  = excluded.balance,
                currency = excluded.currency,
                source   = excluded.source,
                notes    = excluded.notes
            """,
            (
                row["account_path"], row["as_of_date"], row["balance"],
                row["currency"], row["source"], row["notes"],
            ),
        )
        written += 1
    return ReconstructReport(
        pass_name="step17:balance_anchors", rows_written=written,
        notes=[f"rebuilt {written} anchors"] if written else [],
    )
