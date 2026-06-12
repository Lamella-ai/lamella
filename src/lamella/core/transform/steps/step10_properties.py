# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 10: property state reconstruction.

Rebuilds ``properties`` + ``property_valuations`` from ledger directives.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from lamella.features.properties.reader import (
    read_properties,
    read_property_valuations,
)
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


@register(
    "step10:properties",
    state_tables=["properties", "property_valuations"],
)
def reconstruct_properties(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    notes: list[str] = []

    for row in read_properties(entries):
        is_active = 1 if row["is_active"] is None else (1 if row["is_active"] else 0)
        conn.execute(
            """
            INSERT INTO properties
                (slug, display_name, property_type, entity_slug, address, city,
                 state, postal_code, purchase_date, purchase_price, closing_costs,
                 asset_account_path, sale_date, sale_price, is_primary_residence,
                 is_rental, is_active, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (slug) DO UPDATE SET
                display_name          = excluded.display_name,
                property_type         = excluded.property_type,
                entity_slug           = excluded.entity_slug,
                address               = excluded.address,
                city                  = excluded.city,
                state                 = excluded.state,
                postal_code           = excluded.postal_code,
                purchase_date         = excluded.purchase_date,
                purchase_price        = excluded.purchase_price,
                closing_costs         = excluded.closing_costs,
                asset_account_path    = excluded.asset_account_path,
                sale_date             = excluded.sale_date,
                sale_price            = excluded.sale_price,
                is_primary_residence  = excluded.is_primary_residence,
                is_rental             = excluded.is_rental,
                is_active             = excluded.is_active,
                notes                 = excluded.notes
            """,
            (
                row["slug"], row["display_name"], row["property_type"],
                row["entity_slug"], row["address"], row["city"], row["state"],
                row["postal_code"], row["purchase_date"], row["purchase_price"],
                row["closing_costs"], row["asset_account_path"],
                row["sale_date"], row["sale_price"],
                1 if row["is_primary_residence"] else 0,
                1 if row["is_rental"] else 0,
                is_active, row["notes"],
            ),
        )
        written += 1

    for row in read_property_valuations(entries):
        conn.execute(
            """
            INSERT INTO property_valuations
                (property_slug, as_of_date, value, source, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (property_slug, as_of_date) DO UPDATE SET
                value  = excluded.value,
                source = excluded.source,
                notes  = excluded.notes
            """,
            (row["property_slug"], row["as_of_date"], row["value"],
             row["source"], row["notes"]),
        )
        written += 1

    if written:
        notes.append(f"rebuilt {written} property rows")
    return ReconstructReport(
        pass_name="step10:properties", rows_written=written, notes=notes,
    )
