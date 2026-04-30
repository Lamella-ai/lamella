# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicle fuel log — CRUD + MPG / cost-per-mile derivations.

Captures physical fuel events (gallons / kWh / odometer) separately
from the ledger's dollar postings. The two sets compare at the data-
health level: if the ledger shows $4,200 in Fuel expense but the log
shows $3,800 of fuel events, that's a drift worth surfacing.

For EVs, quantity is in kWh and cost_cents is optional — home charging
on an un-sub-metered meter is guesswork, and the plan (§6 decision 2)
is to record it as "unknown" rather than default to a configured rate.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date as date_t
from decimal import Decimal

log = logging.getLogger(__name__)


VALID_UNITS = {"gallon", "kwh"}

VALID_FUEL_TYPES = {
    "gasoline", "diesel", "ev", "phev", "hybrid", "other",
}


class FuelValidationError(ValueError):
    """User-facing validation failure."""


@dataclass(frozen=True)
class FuelEvent:
    id: int
    vehicle_slug: str
    as_of_date: date_t
    as_of_time: str | None
    fuel_type: str
    quantity: float
    unit: str
    cost_cents: int | None
    odometer: int | None
    location: str | None
    notes: str | None
    source: str

    @property
    def cost_usd(self) -> Decimal | None:
        if self.cost_cents is None:
            return None
        return (Decimal(self.cost_cents) / Decimal(100)).quantize(Decimal("0.01"))

    @property
    def cost_per_unit(self) -> Decimal | None:
        """$/gallon or $/kWh. None when cost wasn't recorded."""
        if self.cost_cents is None or self.quantity <= 0:
            return None
        return (Decimal(self.cost_cents) / Decimal(100) / Decimal(str(self.quantity))
                ).quantize(Decimal("0.001"))


def add_event(
    conn: sqlite3.Connection,
    *,
    vehicle_slug: str,
    as_of_date: date_t,
    fuel_type: str,
    quantity: float,
    unit: str,
    cost_cents: int | None = None,
    odometer: int | None = None,
    location: str | None = None,
    notes: str | None = None,
    as_of_time: str | None = None,
    paperless_id: int | None = None,
    source: str = "manual",
    connector_config_path=None,
    main_bean_path=None,
) -> int:
    if fuel_type not in VALID_FUEL_TYPES:
        raise FuelValidationError(f"invalid fuel_type {fuel_type!r}")
    if unit not in VALID_UNITS:
        raise FuelValidationError(f"invalid unit {unit!r}")
    if quantity <= 0:
        raise FuelValidationError("quantity must be positive")
    if cost_cents is not None and cost_cents < 0:
        raise FuelValidationError("cost_cents must be >= 0")
    cur = conn.execute(
        """
        INSERT INTO vehicle_fuel_log
            (vehicle_slug, as_of_date, as_of_time, fuel_type,
             quantity, unit, cost_cents, odometer, location,
             paperless_id, notes, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vehicle_slug, as_of_date.isoformat(), as_of_time, fuel_type,
            float(quantity), unit, cost_cents, odometer, location,
            paperless_id, notes, source,
        ),
    )
    # Mirror to the ledger so fuel entries survive a DB wipe. Callers
    # that don't know about the ledger (tests, background imports)
    # pass connector_config_path=None and we skip the write.
    if connector_config_path is not None and main_bean_path is not None:
        try:
            from lamella.features.vehicles.fuel_writer import append_fuel_entry
            append_fuel_entry(
                connector_config=connector_config_path,
                main_bean=main_bean_path,
                vehicle_slug=vehicle_slug,
                as_of_date=as_of_date,
                quantity=float(quantity),
                unit=unit,
                fuel_type=fuel_type,
                as_of_time=as_of_time,
                cost_cents=cost_cents,
                odometer=odometer,
                location=location,
                paperless_id=paperless_id,
                notes=notes,
                source=source,
            )
        except Exception as exc:  # noqa: BLE001
            import logging as _l
            _l.getLogger(__name__).warning(
                "vehicle-fuel-entry directive write failed for %s: %s",
                vehicle_slug, exc,
            )
    return int(cur.lastrowid)


def delete_event(conn: sqlite3.Connection, event_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM vehicle_fuel_log WHERE id = ?", (int(event_id),),
    )
    return bool(cur.rowcount and cur.rowcount > 0)


def list_events(
    conn: sqlite3.Connection,
    *,
    vehicle_slug: str,
    year: int | None = None,
    limit: int = 200,
) -> list[FuelEvent]:
    clauses = ["vehicle_slug = ?"]
    params: list = [vehicle_slug]
    if year is not None:
        clauses.append("as_of_date >= ? AND as_of_date < ?")
        params.extend([f"{year:04d}-01-01", f"{year + 1:04d}-01-01"])
    where = " AND ".join(clauses)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT id, vehicle_slug, as_of_date, as_of_time, fuel_type,
               quantity, unit, cost_cents, odometer, location,
               notes, source
          FROM vehicle_fuel_log
         WHERE {where}
      ORDER BY as_of_date DESC, COALESCE(as_of_time, '') DESC, id DESC
         LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_row_to_event(r) for r in rows]


def _row_to_event(row: sqlite3.Row) -> FuelEvent:
    raw = row["as_of_date"]
    try:
        d = date_t.fromisoformat(str(raw)[:10])
    except ValueError:
        d = date_t.today()
    return FuelEvent(
        id=int(row["id"]),
        vehicle_slug=row["vehicle_slug"],
        as_of_date=d,
        as_of_time=row["as_of_time"],
        fuel_type=row["fuel_type"],
        quantity=float(row["quantity"]),
        unit=row["unit"],
        cost_cents=int(row["cost_cents"]) if row["cost_cents"] is not None else None,
        odometer=int(row["odometer"]) if row["odometer"] is not None else None,
        location=row["location"],
        notes=row["notes"],
        source=row["source"],
    )


@dataclass(frozen=True)
class FuelStats:
    """Derived metrics for a window. All fields may be None — the
    renderer shows 'unknown' in those cells."""
    events_count: int
    total_gallons: float
    total_kwh: float
    total_cost_usd: Decimal
    miles_covered: int | None     # Δ odometer across events with odometer set
    mpg: float | None             # miles ÷ gallons, when both known
    miles_per_kwh: float | None   # miles ÷ kWh, when both known
    cost_per_mile_cents: int | None
    cost_per_gallon: Decimal | None
    cost_per_kwh: Decimal | None


def compute_stats(events: list[FuelEvent]) -> FuelStats:
    """Derive MPG / cost-per-mile from a run of fuel events. Miles
    covered comes from the Δ between the min and max odometer; if the
    events don't span (0 or 1 event with odometer set), miles_covered
    is None and the downstream ratios stay None.

    This is the lossy reconstruction path — the trip log has the real
    miles. The fuel-derived MPG is a sanity check, not the source of
    truth for mileage.
    """
    n = len(events)
    gal = sum(e.quantity for e in events if e.unit == "gallon")
    kwh = sum(e.quantity for e in events if e.unit == "kwh")
    cost = sum(
        (Decimal(e.cost_cents) / Decimal(100) for e in events if e.cost_cents is not None),
        Decimal("0"),
    )
    odos = [e.odometer for e in events if e.odometer is not None]
    miles_covered: int | None = None
    if len(odos) >= 2:
        miles_covered = max(odos) - min(odos)
        if miles_covered <= 0:
            miles_covered = None
    mpg = (miles_covered / gal) if (miles_covered and gal > 0) else None
    mi_per_kwh = (miles_covered / kwh) if (miles_covered and kwh > 0) else None
    cost_per_mile_cents = None
    if miles_covered and cost > 0:
        cost_per_mile_cents = int(round(
            (cost * Decimal(100) / Decimal(miles_covered)),
        ))
    cost_per_gallon = None
    if gal > 0 and cost > 0:
        # Only count cost from gallon events toward $/gallon.
        gal_cost = sum(
            (Decimal(e.cost_cents) / Decimal(100)
             for e in events
             if e.unit == "gallon" and e.cost_cents is not None),
            Decimal("0"),
        )
        if gal_cost > 0:
            cost_per_gallon = (gal_cost / Decimal(str(gal))).quantize(
                Decimal("0.001"),
            )
    cost_per_kwh = None
    if kwh > 0 and cost > 0:
        kwh_cost = sum(
            (Decimal(e.cost_cents) / Decimal(100)
             for e in events
             if e.unit == "kwh" and e.cost_cents is not None),
            Decimal("0"),
        )
        if kwh_cost > 0:
            cost_per_kwh = (kwh_cost / Decimal(str(kwh))).quantize(
                Decimal("0.001"),
            )
    return FuelStats(
        events_count=n,
        total_gallons=gal,
        total_kwh=kwh,
        total_cost_usd=cost.quantize(Decimal("0.01")),
        miles_covered=miles_covered,
        mpg=round(mpg, 2) if mpg is not None else None,
        miles_per_kwh=round(mi_per_kwh, 2) if mi_per_kwh is not None else None,
        cost_per_mile_cents=cost_per_mile_cents,
        cost_per_gallon=cost_per_gallon,
        cost_per_kwh=cost_per_kwh,
    )
