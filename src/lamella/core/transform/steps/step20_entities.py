# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 20: entity registry reconstruct.

Reads ``custom "entity" "<slug>"`` directives from the ledger and
upserts rows in the ``entities`` table. Without this, a DB wipe
would leave the app with zero registered entities — every
downstream flow (commingle resolver, scaffold routing, classify
whitelist, card UX, reports) that depends on `entities` would
degrade to unknown-entity fallbacks.

Per the CLAUDE.md rule: "the ledger is the source of truth." Before
this step existed, entities were SQLite-only and thus not source-of-
truthful. Writers (routes/entities.py) now persist a directive on
every create/edit; this step replays them on reconstruct.
"""
from __future__ import annotations

import logging

from lamella.core.registry.entity_writer import read_entity_directives
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step20:entities",
    state_tables=["entities"],
)
def reconstruct_entities(conn, entries) -> ReconstructReport:
    """Walk the ledger once, upsert every ``custom "entity"`` row into
    the entities table. Safe to re-run — insert-or-replace semantics.
    """
    directives = read_entity_directives(entries)
    if not directives:
        return ReconstructReport(
            pass_name="step20:entities",
            notes=["no entity directives found"],
        )

    # Insert-or-replace. We preserve `is_active` (defaults to 1) and
    # any other non-directive-backed columns by doing a per-column
    # COALESCE — but since this is reconstruct (DB was just wiped),
    # a simple UPSERT with defaults is fine.
    inserted = 0
    updated = 0
    for d in directives:
        existing = conn.execute(
            "SELECT 1 FROM entities WHERE slug = ?", (d["slug"],),
        ).fetchone()
        # is_active defaults to 1 (active) when the directive omits
        # the lamella-is-active key, matching pre-flag entity creation.
        is_active_val = d.get("is_active")
        if is_active_val is None:
            is_active_val = 1
        conn.execute(
            """
            INSERT INTO entities
                (slug, display_name, entity_type, tax_schedule,
                 start_date, ceased_date, notes, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (slug) DO UPDATE SET
                display_name = COALESCE(excluded.display_name, entities.display_name),
                entity_type  = COALESCE(excluded.entity_type,  entities.entity_type),
                tax_schedule = COALESCE(excluded.tax_schedule, entities.tax_schedule),
                start_date   = COALESCE(excluded.start_date,   entities.start_date),
                ceased_date  = COALESCE(excluded.ceased_date,  entities.ceased_date),
                notes        = COALESCE(excluded.notes,        entities.notes),
                is_active    = excluded.is_active
            """,
            (
                d["slug"],
                d.get("display_name"),
                d.get("entity_type"),
                d.get("tax_schedule"),
                d.get("start_date"),
                d.get("ceased_date"),
                d.get("notes"),
                is_active_val,
            ),
        )
        if existing:
            updated += 1
        else:
            inserted += 1
    return ReconstructReport(
        pass_name="step20:entities",
        rows_written=inserted + updated,
        notes=[
            f"reconstructed {len(directives)} entity directive(s): "
            f"{inserted} inserted, {updated} updated",
        ],
    )
