# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 4: Paperless field role mapping as custom directives.

Only rows the user has explicitly set (``auto_assigned=0`` in
``paperless_field_map``) are state. Auto-assigned rows regenerate on
the next Paperless sync and don't need to ride in the ledger.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.paperless_bridge.field_map_writer import (
    read_field_mappings_from_entries,
)
from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy

log = logging.getLogger(__name__)


@register("step4:paperless-fields", state_tables=["paperless_field_map"])
def reconstruct_paperless_fields(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    rows = read_field_mappings_from_entries(entries)
    written = 0
    for row in rows:
        cursor = conn.execute(
            """
            INSERT INTO paperless_field_map
                (paperless_field_id, paperless_field_name, canonical_role, auto_assigned)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(paperless_field_id) DO UPDATE SET
                paperless_field_name = excluded.paperless_field_name,
                canonical_role = excluded.canonical_role,
                auto_assigned = 0,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                row["paperless_field_id"],
                row["paperless_field_name"],
                row["canonical_role"],
            ),
        )
        if cursor.rowcount:
            written += 1
    return ReconstructReport(
        pass_name="step4:paperless-fields",
        rows_written=written,
        notes=[
            f"{len(rows)} user-set field mappings. "
            "Auto-assigned rows regenerate from next sync."
        ],
    )


register_policy(
    TablePolicy(
        table="paperless_field_map",
        kind="state",
        primary_key=("paperless_field_id",),
    )
)
