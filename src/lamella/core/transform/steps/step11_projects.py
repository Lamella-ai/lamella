# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 11: project state reconstruction.

project_txns is cache (derivable from the ledger by checking which
entries fall in the project window and match its expected merchants),
so only `projects` is rebuilt here.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.projects.reader import read_projects
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step11:projects",
    state_tables=["projects"],
)
def reconstruct_projects(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    for row in read_projects(entries):
        if not row["start_date"]:
            continue  # start_date is NOT NULL in the schema
        is_active = 1 if row["is_active"] is None else (1 if row["is_active"] else 0)
        conn.execute(
            """
            INSERT INTO projects
                (slug, display_name, description, entity_slug, property_slug,
                 project_type, start_date, end_date, budget_amount,
                 expected_merchants, previous_project_slug, is_active,
                 closed_at, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (slug) DO UPDATE SET
                display_name           = excluded.display_name,
                description            = excluded.description,
                entity_slug            = excluded.entity_slug,
                property_slug          = excluded.property_slug,
                project_type           = excluded.project_type,
                start_date             = excluded.start_date,
                end_date               = excluded.end_date,
                budget_amount          = excluded.budget_amount,
                expected_merchants     = excluded.expected_merchants,
                previous_project_slug  = excluded.previous_project_slug,
                is_active              = excluded.is_active,
                closed_at              = excluded.closed_at,
                notes                  = excluded.notes
            """,
            (
                row["slug"], row["display_name"], row["description"],
                row["entity_slug"], row["property_slug"], row["project_type"],
                row["start_date"], row["end_date"], row["budget_amount"],
                row["expected_merchants"], row["previous_project_slug"],
                is_active, row["closed_at"], row["notes"],
            ),
        )
        written += 1
    return ReconstructReport(
        pass_name="step11:projects", rows_written=written,
        notes=[f"rebuilt {written} projects"] if written else [],
    )
