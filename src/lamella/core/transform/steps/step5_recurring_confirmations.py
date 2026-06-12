# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 5: recurring-expense confirmations as custom directives.

User-confirmed and user-ignored recurring patterns are state and
live in ``connector_rules.bean`` (co-located with classification
rules since they share the same "learned user decision" semantic).
Detected-but-unconfirmed proposals stay in the cache.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.recurring.writer import read_recurring_from_entries
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register(
    "step5:recurring-confirmations",
    state_tables=["recurring_expenses"],
)
def reconstruct_recurring(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    rows = read_recurring_from_entries(entries)
    written = 0
    for row in rows:
        cursor = conn.execute(
            """
            INSERT INTO recurring_expenses
                (label, entity, expected_amount, expected_day,
                 source_account, merchant_pattern, cadence, status,
                 confirmed_at, ignored_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                    CASE WHEN ? = 'confirmed' THEN CURRENT_TIMESTAMP ELSE NULL END,
                    CASE WHEN ? = 'ignored' THEN CURRENT_TIMESTAMP ELSE NULL END)
            ON CONFLICT(merchant_pattern, source_account) DO UPDATE SET
                status = excluded.status,
                label = excluded.label,
                entity = excluded.entity,
                expected_amount = excluded.expected_amount,
                cadence = excluded.cadence,
                confirmed_at = excluded.confirmed_at,
                ignored_at = excluded.ignored_at
            """,
            (
                row["label"],
                row["entity"],
                row["expected_amount"],
                row["expected_day"],
                row["source_account"],
                row["merchant_pattern"],
                row["cadence"],
                row["status"],
                row["status"],
                row["status"],
            ),
        )
        if cursor.rowcount:
            written += 1
    return ReconstructReport(
        pass_name="step5:recurring-confirmations",
        rows_written=written,
        notes=[
            f"{len(rows)} active confirmed/ignored recurring rows after revoke-filter. "
            "Proposed rows regenerate from next detection run."
        ],
    )


register_policy(
    TablePolicy(
        table="recurring_expenses",
        kind="state",
        primary_key=("merchant_pattern", "source_account"),
    )
)
