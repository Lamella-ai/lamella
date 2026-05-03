# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 2: classification rules as custom directives in
``connector_rules.bean``.

The SQLite ``classification_rules`` table becomes a cache of what's
stamped on the ledger. Rule identity (for upsert + revoke matching)
is the tuple (pattern_type, pattern_value, card_account,
target_account).

Cache-only columns that are recomputable from ledger scans:
  * ``hit_count``  (fires against actual transactions)
  * ``last_used``  (most recent firing timestamp)
  * ``confidence``  (demote/promote ladder)

These are NOT rebuilt by this pass — only the rule bodies + provenance.
Cache columns land on their defaults (0 / NULL / 1.0); the engine
rebuilds them via the existing bump / demote / promote paths as
rules fire post-reconstruct.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.rules.rule_writer import read_rules_from_entries
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register(
    "step2:classification-rules",
    state_tables=["classification_rules"],
)
def reconstruct_classification_rules(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    rows = read_rules_from_entries(entries)
    written = 0
    has_added_at = _column_exists(conn, "classification_rules", "added_at")
    for row in rows:
        try:
            if has_added_at:
                cursor = conn.execute(
                    """
                    INSERT INTO classification_rules
                        (pattern_type, pattern_value, card_account,
                         target_account, confidence, created_by, added_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        row["pattern_type"],
                        row["pattern_value"],
                        row["card_account"],
                        row["target_account"],
                        1.0 if row["created_by"] == "user" else 0.85,
                        row["created_by"],
                        row["added_at"],
                    ),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO classification_rules
                        (pattern_type, pattern_value, card_account,
                         target_account, confidence, created_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        row["pattern_type"],
                        row["pattern_value"],
                        row["card_account"],
                        row["target_account"],
                        1.0 if row["created_by"] == "user" else 0.85,
                        row["created_by"],
                    ),
                )
            if cursor.rowcount:
                written += 1
        except sqlite3.IntegrityError as exc:
            # Log only stable identifiers — never the full row, which
            # contains user-entered pattern_value (merchant pattern text)
            # and target_account (specific category path).
            log.warning(
                "skipping rule row rule_id=%s rule_kind=%s: %s",
                row.get("rule_id"), row.get("rule_kind"), exc,
            )
    return ReconstructReport(
        pass_name="step2:classification-rules",
        rows_written=written,
        notes=[
            f"{len(rows)} active rules on ledger after revoke-filter. "
            "hit_count / last_used / confidence are cache-only; the "
            "engine rebuilds them from post-reconstruct firings."
        ],
    )


def _column_exists(
    conn: sqlite3.Connection, table: str, column: str
) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for r in rows:
        name = r["name"] if isinstance(r, sqlite3.Row) else r[1]
        if name == column:
            return True
    return False


def _drift_allowance(live_rows, rebuilt_rows):
    """Cache-table drift budget for classification_rules.

    Allowed drift:
      * hit_count / last_used / confidence: differ arbitrarily (cache).
      * Rule identity (pattern_type + pattern_value + card_account +
        target_account + created_by) must match exactly.

    Anything else counts as a bug.
    """
    # `classification_rules` is actually a state table in our model —
    # the rule body is state, cache-y columns are cache. Since the DB
    # stores them in one table, we classify the whole table as state
    # below. This helper is kept for reference: a future split could
    # move the cache columns to a sidecar table and let this run.
    return []


# NOTE: classification_rules mixes state columns (identity) with cache
# columns (hit_count, last_used, confidence). For verify purposes we
# treat it as state and accept that post-reconstruct the cache columns
# reset to defaults. The test suite validates this by comparing only
# the identity columns.
register_policy(
    TablePolicy(
        table="classification_rules",
        kind="state",
        primary_key=("pattern_type", "pattern_value", "card_account", "target_account"),
    )
)
