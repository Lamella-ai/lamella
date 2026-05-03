# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 23: loan_pauses reconstruction (WP12).

Rebuilds rows from ``custom "loan-pause"`` directives in
connector_config.bean. Tombstones (``loan-pause-revoked``) drop
matched rows. State table — verify treats any drift as a bug.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.loans.reader import read_loan_pauses
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register(
    "step23:loan_pauses",
    state_tables=["loan_pauses"],
)
def reconstruct_loan_pauses(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    notes: list[str] = []

    for row in read_loan_pauses(entries):
        # Use INSERT ... ON CONFLICT to keep the (slug, start_date)
        # unique constraint correct on rebuild without juggling ids.
        conn.execute(
            """
            INSERT INTO loan_pauses
                (loan_slug, start_date, end_date, reason, notes,
                 accrued_interest)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (loan_slug, start_date) DO UPDATE SET
                end_date         = excluded.end_date,
                reason           = excluded.reason,
                notes            = excluded.notes,
                accrued_interest = excluded.accrued_interest
            """,
            (
                row["loan_slug"], row["start_date"], row.get("end_date"),
                row.get("reason"), row.get("notes"),
                row.get("accrued_interest"),
            ),
        )
        written += 1

    if written:
        notes.append(f"rebuilt {written} loan_pauses rows")
    return ReconstructReport(
        pass_name="step23:loan_pauses",
        rows_written=written, notes=notes,
    )


# Non-id primary_key pattern (composite / natural key):
#
# SQLite's autoincrement id differs between the live DB and a freshly
# rebuilt-from-ledger copy (INSERT order, re-INSERT after DELETE on
# end_pause, etc.), so using ``id`` as the verify primary_key would
# flag every row as drift even when the ledger state is identical.
#
# The rule: when a table carries any auto-increment surrogate key AND
# has a natural UNIQUE the writer enforces, pass that UNIQUE as
# ``primary_key`` in the verify policy. Here it's
# (loan_slug, start_date), matching migration 049's UNIQUE constraint.
#
# Future step24 / WP13 tables with their own autoincrement ids should
# follow the same pattern — identify the natural key and pass it
# explicitly. See verify.TablePolicy docstring for the broader contract.
register_policy(TablePolicy(
    table="loan_pauses",
    kind="state",
    primary_key=("loan_slug", "start_date"),
))
