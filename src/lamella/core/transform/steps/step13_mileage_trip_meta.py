# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 13: mileage_trip_meta reconstruction from `custom "mileage-trip-meta"`."""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Iterable

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    custom_arg,
    custom_meta,
)
from lamella.core.transform.reconstruct import ReconstructReport, register

log = logging.getLogger(__name__)


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _read_trip_meta(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str, float], dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "mileage-trip-meta":
            continue
        vehicle = _str(custom_arg(entry, 0))
        miles = _float(custom_arg(entry, 1))
        if not vehicle or miles is None:
            continue
        key = (entry.date.isoformat(), vehicle, float(miles))
        rows[key] = {
            "entry_date": entry.date.isoformat(),
            "vehicle": vehicle,
            "miles": miles,
            "business_miles": _float(custom_meta(entry, "lamella-trip-business-miles")),
            "personal_miles": _float(custom_meta(entry, "lamella-trip-personal-miles")),
            "commuting_miles": _float(custom_meta(entry, "lamella-trip-commuting-miles")),
            "category": _str(custom_meta(entry, "lamella-trip-category")),
            "purpose_parsed": _str(custom_meta(entry, "lamella-trip-purpose-parsed")),
            "entity_parsed": _str(custom_meta(entry, "lamella-trip-entity-parsed")),
            "auto_from_ai": _bool(custom_meta(entry, "lamella-trip-auto-from-ai")),
            "free_text": _str(custom_meta(entry, "lamella-trip-free-text")),
        }
    return list(rows.values())


@register(
    "step13:mileage_trip_meta",
    state_tables=["mileage_trip_meta"],
)
def reconstruct_trip_meta(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    for row in _read_trip_meta(entries):
        conn.execute(
            """
            INSERT INTO mileage_trip_meta
                (entry_date, vehicle, miles, business_miles, personal_miles,
                 commuting_miles, category, purpose_parsed, entity_parsed,
                 auto_from_ai, free_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (entry_date, vehicle, miles) DO UPDATE SET
                business_miles   = excluded.business_miles,
                personal_miles   = excluded.personal_miles,
                commuting_miles  = excluded.commuting_miles,
                category         = excluded.category,
                purpose_parsed   = excluded.purpose_parsed,
                entity_parsed    = excluded.entity_parsed,
                auto_from_ai     = excluded.auto_from_ai,
                free_text        = excluded.free_text
            """,
            (
                row["entry_date"], row["vehicle"], row["miles"],
                row["business_miles"], row["personal_miles"],
                row["commuting_miles"], row["category"],
                row["purpose_parsed"], row["entity_parsed"],
                1 if row["auto_from_ai"] else 0,
                row["free_text"],
            ),
        )
        written += 1
    return ReconstructReport(
        pass_name="step13:mileage_trip_meta", rows_written=written,
        notes=[f"rebuilt {written} trip-meta rows"] if written else [],
    )
