# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 22: loan_payment_groups reconstruction (WP5).

Rebuilds ``loan_payment_groups`` rows with ``status='confirmed'`` from
``lamella-loan-group-members`` meta on override blocks. Proposed /
dismissed statuses are NOT reconstructable — they're ephemeral UI
state the live proposer re-derives on each page render.

See ``loans/groups.py::read_loan_payment_groups`` for the parser.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.loans.groups import read_loan_payment_groups
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register(
    "step22:loan_payment_groups",
    state_tables=["loan_payment_groups"],
)
def reconstruct_loan_payment_groups(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    notes: list[str] = []

    for row in read_loan_payment_groups(entries):
        conn.execute(
            """
            INSERT INTO loan_payment_groups
                (group_id, loan_slug, member_hashes, aggregate_amount,
                 date_span_start, date_span_end, primary_hash, status,
                 confirmed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmed', CURRENT_TIMESTAMP)
            ON CONFLICT (group_id) DO UPDATE SET
                loan_slug        = excluded.loan_slug,
                member_hashes    = excluded.member_hashes,
                aggregate_amount = excluded.aggregate_amount,
                date_span_start  = excluded.date_span_start,
                date_span_end    = excluded.date_span_end,
                primary_hash     = excluded.primary_hash,
                status           = 'confirmed'
            """,
            (
                row["group_id"], row["loan_slug"], row["member_hashes"],
                row["aggregate_amount"], row["date_span_start"],
                row["date_span_end"], row.get("primary_hash"),
            ),
        )
        written += 1

    if written:
        notes.append(f"rebuilt {written} confirmed loan_payment_groups rows")
    return ReconstructReport(
        pass_name="step22:loan_payment_groups",
        rows_written=written,
        notes=notes,
    )


def _allow_non_confirmed_drift(
    live_rows: list[dict], rebuilt_rows: list[dict],
) -> list[str]:
    """Confirmed rows must match the ledger; proposed/dismissed rows
    are ephemeral UI state the reconstruct pass intentionally does
    not repopulate. Any non-confirmed drift is tolerated; a confirmed
    drift stays flagged.
    """
    tolerated: list[str] = []
    by_gid_live = {r["group_id"]: r for r in live_rows}
    by_gid_rebuilt = {r["group_id"]: r for r in rebuilt_rows}
    for gid, row in by_gid_live.items():
        if gid not in by_gid_rebuilt and row.get("status") != "confirmed":
            tolerated.append(
                f"group_id={gid[:8]} status={row.get('status')} "
                f"(ephemeral — not reconstructed)"
            )
    return tolerated


register_policy(TablePolicy(
    table="loan_payment_groups",
    kind="cache",
    primary_key=("group_id",),
    allow_drift=_allow_non_confirmed_drift,
))
