# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7 — recurring trip templates.

Capture the constants for a repeating route (vehicle, from/to,
purpose, category, miles) so the user can spawn a trip with one
tap. is_round_trip=1 doubles the miles on spawn and appends
" (round trip)" to the purpose.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


VALID_CATEGORIES = {"business", "commuting", "personal", "mixed"}


@dataclass(frozen=True)
class TripTemplate:
    slug: str
    display_name: str
    vehicle_slug: str | None
    entity: str | None
    default_from: str | None
    default_to: str | None
    default_purpose: str | None
    default_miles: float | None
    default_category: str | None
    is_round_trip: bool
    is_active: bool


def _row_to_template(row: sqlite3.Row) -> TripTemplate:
    return TripTemplate(
        slug=row["slug"],
        display_name=row["display_name"],
        vehicle_slug=row["vehicle_slug"],
        entity=row["entity"],
        default_from=row["default_from"],
        default_to=row["default_to"],
        default_purpose=row["default_purpose"],
        default_miles=float(row["default_miles"]) if row["default_miles"] is not None else None,
        default_category=row["default_category"],
        is_round_trip=bool(row["is_round_trip"]),
        is_active=bool(row["is_active"]),
    )


def upsert_template(
    conn: sqlite3.Connection,
    *,
    slug: str,
    display_name: str,
    vehicle_slug: str | None = None,
    entity: str | None = None,
    default_from: str | None = None,
    default_to: str | None = None,
    default_purpose: str | None = None,
    default_miles: float | None = None,
    default_category: str | None = None,
    is_round_trip: bool = False,
    is_active: bool = True,
    connector_config_path=None,
    main_bean_path=None,
) -> None:
    if default_category is not None and default_category not in VALID_CATEGORIES:
        raise ValueError(f"invalid category {default_category!r}")
    conn.execute(
        """
        INSERT INTO vehicle_trip_templates
            (slug, display_name, vehicle_slug, entity,
             default_from, default_to, default_purpose,
             default_miles, default_category,
             is_round_trip, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (slug) DO UPDATE SET
            display_name     = excluded.display_name,
            vehicle_slug     = excluded.vehicle_slug,
            entity           = excluded.entity,
            default_from     = excluded.default_from,
            default_to       = excluded.default_to,
            default_purpose  = excluded.default_purpose,
            default_miles    = excluded.default_miles,
            default_category = excluded.default_category,
            is_round_trip    = excluded.is_round_trip,
            is_active        = excluded.is_active
        """,
        (
            slug, display_name, vehicle_slug, entity,
            default_from, default_to, default_purpose,
            float(default_miles) if default_miles is not None else None,
            default_category,
            1 if is_round_trip else 0,
            1 if is_active else 0,
        ),
    )
    if connector_config_path is not None and main_bean_path is not None:
        try:
            from lamella.features.vehicles.writer import append_vehicle_trip_template
            append_vehicle_trip_template(
                connector_config=connector_config_path,
                main_bean=main_bean_path,
                slug=slug, display_name=display_name,
                vehicle_slug=vehicle_slug, entity=entity,
                default_from=default_from, default_to=default_to,
                default_purpose=default_purpose,
                default_miles=default_miles,
                default_category=default_category,
                is_round_trip=is_round_trip, is_active=is_active,
            )
        except Exception as exc:  # noqa: BLE001
            import logging as _l
            _l.getLogger(__name__).warning(
                "vehicle-trip-template directive write failed for %s: %s",
                slug, exc,
            )


def delete_template(conn: sqlite3.Connection, slug: str) -> bool:
    cur = conn.execute(
        "DELETE FROM vehicle_trip_templates WHERE slug = ?", (slug,),
    )
    return bool(cur.rowcount and cur.rowcount > 0)


def list_templates(
    conn: sqlite3.Connection, *, include_inactive: bool = False,
) -> list[TripTemplate]:
    clauses = []
    if not include_inactive:
        clauses.append("is_active = 1")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"SELECT * FROM vehicle_trip_templates {where} "
        f"ORDER BY display_name",
    ).fetchall()
    return [_row_to_template(r) for r in rows]


def spawn_from_template(template: TripTemplate) -> dict:
    """Return a dict of form-ready defaults for the /mileage page.
    Round-trip templates double the miles and suffix the purpose."""
    miles = template.default_miles or 0.0
    purpose = template.default_purpose or ""
    if template.is_round_trip:
        miles = miles * 2
        if purpose and "round trip" not in purpose.lower():
            purpose = f"{purpose} (round trip)"
    return {
        "vehicle_slug": template.vehicle_slug,
        "entity": template.entity,
        "from_loc": template.default_from,
        "to_loc": template.default_to,
        "purpose": purpose,
        "miles": miles,
        "category": template.default_category,
    }
