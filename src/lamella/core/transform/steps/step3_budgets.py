# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 3: budgets persisted as ``custom "budget"`` directives."""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.budgets.writer import read_budgets_from_entries
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register("step3:budgets", state_tables=["budgets"])
def reconstruct_budgets(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    rows = read_budgets_from_entries(entries)
    written = 0
    for row in rows:
        cursor = conn.execute(
            """
            INSERT INTO budgets
                (label, entity, account_pattern, period, amount, alert_threshold)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (
                row["label"],
                row["entity"],
                row["account_pattern"],
                row["period"],
                row["amount"],
                row["alert_threshold"],
            ),
        )
        if cursor.rowcount:
            written += 1
    return ReconstructReport(
        pass_name="step3:budgets",
        rows_written=written,
        notes=[f"{len(rows)} active budgets after revoke-filter"],
    )


register_policy(
    TablePolicy(
        table="budgets",
        kind="state",
        primary_key=("label", "entity", "account_pattern", "period"),
    )
)
