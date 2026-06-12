# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Step 12: vehicle_fuel_log reconstruction.

Directive: `custom "vehicle-fuel-entry" <vehicle_slug>` with full fillup
metadata. Writer lives in vehicles/fuel_writer.py.
"""
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


def _int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _read_fuel_entries(entries: Iterable[Any]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str, str, float], dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, Custom) or entry.type != "vehicle-fuel-entry":
            continue
        slug = _str(custom_arg(entry, 0))
        if not slug:
            continue
        as_of_date = entry.date.isoformat()
        as_of_time = _str(custom_meta(entry, "lamella-fuel-time")) or ""
        quantity = _float(custom_meta(entry, "lamella-fuel-quantity"))
        if quantity is None:
            continue
        unit = _str(custom_meta(entry, "lamella-fuel-unit")) or "gallon"
        fuel_type = _str(custom_meta(entry, "lamella-fuel-type")) or "gasoline"
        cost_cents = _int(custom_meta(entry, "lamella-fuel-cost-cents"))
        odometer = _int(custom_meta(entry, "lamella-fuel-odometer"))
        location = _str(custom_meta(entry, "lamella-fuel-location"))
        paperless_id = _int(custom_meta(entry, "lamella-fuel-paperless-id"))
        notes = _str(custom_meta(entry, "lamella-fuel-notes"))
        source = _str(custom_meta(entry, "lamella-fuel-source")) or "manual"
        rows[(slug, as_of_date, as_of_time, quantity)] = {
            "vehicle_slug": slug,
            "as_of_date": as_of_date,
            "as_of_time": as_of_time or None,
            "fuel_type": fuel_type,
            "quantity": quantity,
            "unit": unit,
            "cost_cents": cost_cents,
            "odometer": odometer,
            "location": location,
            "paperless_id": paperless_id,
            "notes": notes,
            "source": source,
        }
    return list(rows.values())


@register(
    "step12:fuel_log",
    state_tables=["vehicle_fuel_log"],
)
def reconstruct_fuel_log(
    conn: sqlite3.Connection, entries: list[Any],
) -> ReconstructReport:
    written = 0
    # Fuel log is append-only by design; the reconstruct wipes and
    # re-inserts when --force is on.
    for row in _read_fuel_entries(entries):
        conn.execute(
            """
            INSERT INTO vehicle_fuel_log
                (vehicle_slug, as_of_date, as_of_time, fuel_type, quantity,
                 unit, cost_cents, odometer, location, paperless_id, notes,
                 source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["vehicle_slug"], row["as_of_date"], row["as_of_time"],
                row["fuel_type"], row["quantity"], row["unit"],
                row["cost_cents"], row["odometer"], row["location"],
                row["paperless_id"], row["notes"], row["source"],
            ),
        )
        written += 1
    return ReconstructReport(
        pass_name="step12:fuel_log", rows_written=written,
        notes=[f"rebuilt {written} fuel entries"] if written else [],
    )
