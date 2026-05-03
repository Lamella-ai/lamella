# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 6 — per-entity trip allocation.

When a vehicle's usage splits across multiple entities (a single-
member LLC owner using one truck for both the LLC and personal errands),
each trip's entity still pins where the miles land. The per-trip
`mileage_trip_meta.attributed_entity` override takes precedence when
the user has explicitly reassigned a trip without adding a new
mileage_entries row.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class EntityAllocation:
    entity: str
    miles: float
    share: float   # fraction 0..1


def _filter_values(vehicle: dict) -> list[str]:
    values = {vehicle.get("slug")}
    if vehicle.get("display_name"):
        values.add(vehicle["display_name"])
    return [v for v in values if v]


def allocation_for_year(
    conn: sqlite3.Connection,
    *,
    vehicle: dict,
    year: int,
) -> list[EntityAllocation]:
    """Return per-entity allocation for the year, sorted by miles
    descending. Uses `mileage_trip_meta.attributed_entity` when set,
    falling back to `mileage_entries.entity` otherwise."""
    names = _filter_values(vehicle)
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    rows = conn.execute(
        f"""
        SELECT
            COALESCE(m.attributed_entity, e.entity) AS entity,
            COALESCE(SUM(e.miles), 0) AS miles
          FROM mileage_entries e
          LEFT JOIN mileage_trip_meta m
                 ON m.entry_date = e.entry_date
                AND m.vehicle = e.vehicle
                AND m.miles = e.miles
         WHERE e.entry_date >= ? AND e.entry_date < ?
           AND (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
         GROUP BY COALESCE(m.attributed_entity, e.entity)
        """,
        (
            f"{year:04d}-01-01", f"{year + 1:04d}-01-01",
            vehicle.get("slug"), *names,
        ),
    ).fetchall()
    total = sum(float(r["miles"] or 0) for r in rows)
    if total <= 0:
        return []
    out = [
        EntityAllocation(
            entity=r["entity"] or "Personal",
            miles=round(float(r["miles"] or 0), 1),
            share=round(float(r["miles"] or 0) / total, 4),
        )
        for r in rows
    ]
    out.sort(key=lambda a: a.miles, reverse=True)
    return out


def set_trip_attribution(
    conn: sqlite3.Connection,
    *,
    entry_date,                 # date or ISO string
    vehicle: str,
    miles: float,
    attributed_entity: str | None,
) -> None:
    """Upsert the `mileage_trip_meta.attributed_entity` override for
    a trip keyed on (entry_date, vehicle, miles). Passing
    `attributed_entity=None` clears the override."""
    iso = (
        entry_date.isoformat() if hasattr(entry_date, "isoformat")
        else str(entry_date)[:10]
    )
    # Upsert — if no sidecar row exists for this trip, create one.
    conn.execute(
        """
        INSERT INTO mileage_trip_meta
            (entry_date, vehicle, miles, attributed_entity)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (entry_date, vehicle, miles) DO UPDATE SET
            attributed_entity = excluded.attributed_entity
        """,
        (iso, vehicle, float(miles), attributed_entity),
    )
