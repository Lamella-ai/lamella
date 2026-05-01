# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 1 of the reconstruct roadmap: receipt dismissals.

Ledger is the source of truth. The ``receipt_dismissals`` SQLite table
is a cache that rebuilds from ``custom "receipt-dismissed"`` /
``custom "receipt-dismissal-revoked"`` directives in ``connector_links.bean``.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.receipts.dismissals_writer import read_dismissals_from_entries
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register("step1:receipt-dismissals", state_tables=["receipt_dismissals"])
def reconstruct_receipt_dismissals(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    rows = read_dismissals_from_entries(entries)
    written = 0
    for row in rows:
        cursor = conn.execute(
            "INSERT INTO receipt_dismissals (txn_hash, reason, dismissed_by, dismissed_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (txn_hash) DO UPDATE SET "
            "  reason = excluded.reason, "
            "  dismissed_by = excluded.dismissed_by, "
            "  dismissed_at = excluded.dismissed_at",
            (
                row["txn_hash"],
                row["reason"],
                row["dismissed_by"],
                row["dismissed_at"],
            ),
        )
        if cursor.rowcount:
            written += 1
    return ReconstructReport(
        pass_name="step1:receipt-dismissals",
        rows_written=written,
        notes=[f"{len(rows)} active dismissals after revoke-filter"],
    )


register_policy(
    TablePolicy(
        table="receipt_dismissals",
        kind="state",
        primary_key=("txn_hash",),
    )
)
