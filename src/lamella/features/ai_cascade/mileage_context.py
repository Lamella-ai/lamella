# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Mileage-log context for classification.

The mileage log records each trip with vehicle, purpose, entity,
from/to locations, and notes. Those fields carry context that
disambiguates same-merchant-different-purpose cases the card
binding can't resolve:

  * Warehouse Club fuel for 3–4 vehicles. The transaction on its own
    looks identical across vehicles. A mileage entry on the same
    day "Drove to Warehouse Club for gas — Acme Cargo Van" is the
    signal that pins it to the right vehicle + entity.
  * Business travel. "Drove to Airport for Atlanta trade show"
    on April 14 makes the Fast Food charge on April 17 a
    travel meal, not a personal mcdonalds charge — even if the
    note system didn't capture it.

Same proximity model as ``notes_active_on``: default ±3 days.
The AI treats mileage as a contextual prior, not a hard scope.
A mileage entry mentioning a project amplifies other signals
consistent with that project; it never forces an unrelated txn
(e.g., a mortgage autopay) to miscategorize.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date

log = logging.getLogger(__name__)

__all__ = [
    "MileageContextEntry",
    "VehicleLogDensity",
    "mileage_context_for_txn",
    "vehicle_log_density",
]


@dataclass(frozen=True)
class MileageContextEntry:
    """One mileage row surfaced to the classifier."""
    entry_date: date
    vehicle: str
    entity: str
    miles: float
    purpose: str | None
    from_loc: str | None
    to_loc: str | None
    notes: str | None
    category: str | None = None


@dataclass(frozen=True)
class VehicleLogDensity:
    """Per-vehicle log-density snapshot for the classifier.

    Lets the AI calibrate "vehicle absent from the log today = vehicle
    wasn't driven" on a per-vehicle basis. A densely-logged vehicle's
    absence is informative; a sparsely-logged vehicle's absence is not.
    """
    vehicle: str
    entity: str | None
    days_with_entries: int
    total_entries: int
    window_days: int
    last_entry_date: date | None


def mileage_context_for_txn(
    conn: sqlite3.Connection,
    *,
    txn_date: date | str,
    proximity_days: int = 3,
    entity: str | None = None,
    limit: int = 10,
) -> list[MileageContextEntry]:
    """Return mileage entries within ±``proximity_days`` of
    ``txn_date``.

    When ``entity`` is given, rows matching that entity rank above
    rows with other entities — but other-entity rows are still
    returned (capped by ``limit``) because cross-entity trips are
    exactly the wrong-card detector signal. Sorted recent-first.
    """
    iso = txn_date if isinstance(txn_date, str) else txn_date.isoformat()
    try:
        rows = conn.execute(
            """
            SELECT entry_date, vehicle, entity, miles, purpose,
                   from_loc, to_loc, notes, purpose_category
              FROM mileage_entries
             WHERE ABS(
                     julianday(entry_date) - julianday(?)
                   ) <= ?
             ORDER BY
                 CASE WHEN entity = ? THEN 0 ELSE 1 END,
                 entry_date DESC
             LIMIT ?
            """,
            (iso, int(proximity_days), entity or "", int(limit)),
        ).fetchall()
    except sqlite3.OperationalError:
        # mileage_entries table not present (some test fixtures
        # skip this migration). Non-fatal.
        return []

    out: list[MileageContextEntry] = []
    for r in rows:
        try:
            d = date.fromisoformat(str(r["entry_date"])[:10])
        except Exception:  # noqa: BLE001
            continue
        # purpose_category may be absent on fixtures that pre-date
        # migration 033.
        try:
            category = r["purpose_category"]
        except (IndexError, KeyError):
            category = None
        out.append(
            MileageContextEntry(
                entry_date=d,
                vehicle=r["vehicle"] or "",
                entity=r["entity"] or "",
                miles=float(r["miles"] or 0),
                purpose=r["purpose"],
                from_loc=r["from_loc"],
                to_loc=r["to_loc"],
                notes=r["notes"],
                category=category,
            )
        )
    return out


def vehicle_log_density(
    conn: sqlite3.Connection,
    *,
    as_of_date: date | str,
    window_days: int = 30,
) -> list[VehicleLogDensity]:
    """Per-vehicle logging density over the last ``window_days`` ending
    at ``as_of_date``.

    One row per registered (active) vehicle, even if it has zero entries
    in the window — a zero-density vehicle is a real signal the AI
    needs ("this vehicle has a sparse log; absence isn't informative").

    ``days_with_entries`` is the count of distinct dates with at least
    one mileage row for the vehicle; ``total_entries`` is the row count.
    Division between them tells you whether the user logs once per day
    (ratio ~= 1) or multiple entries per day (ratio > 1).
    """
    iso = as_of_date if isinstance(as_of_date, str) else as_of_date.isoformat()
    try:
        vehicles = conn.execute(
            "SELECT slug, display_name, entity_slug FROM vehicles "
            "WHERE is_active = 1 "
            "ORDER BY COALESCE(display_name, slug)"
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    out: list[VehicleLogDensity] = []
    for v in vehicles:
        slug = v["slug"]
        display = v["display_name"] or slug
        # Tolerant predicate — matches rows keyed by slug or display name
        # (CSV-legacy rows may hold either).
        try:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT entry_date) AS days,
                       COUNT(*)                   AS total,
                       MAX(entry_date)            AS last_date
                  FROM mileage_entries
                 WHERE entry_date > date(?, ?)
                   AND entry_date <= ?
                   AND (vehicle_slug = ? OR vehicle = ? OR vehicle = ?)
                """,
                (iso, f"-{int(window_days)} days", iso,
                 slug, slug, display),
            ).fetchone()
        except sqlite3.OperationalError:
            continue

        last_raw = row["last_date"] if row else None
        last_date: date | None = None
        if last_raw:
            try:
                last_date = date.fromisoformat(str(last_raw)[:10])
            except ValueError:
                last_date = None

        out.append(
            VehicleLogDensity(
                vehicle=display,
                entity=v["entity_slug"],
                days_with_entries=int(row["days"] or 0) if row else 0,
                total_entries=int(row["total"] or 0) if row else 0,
                window_days=int(window_days),
                last_entry_date=last_date,
            )
        )
    return out
