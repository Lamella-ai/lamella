# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 5E — per-vehicle forecasting.

Pure view-time derivation — nothing is persisted. All three helpers
produce dataclass results that are None-safe on empty input so the
detail template can show "unknown" labels rather than 0-value fakes.

  - project_miles_for_year  — linear YTD extrapolation
  - cost_per_mile_series    — $/mile by month, from trip log +
                              Fuel postings
  - yoy_miles_overlay       — month-bucketed series for N prior
                              years (default 3) to overlay on the
                              current-year monthly chart

"Mid-year projection" in the plan is the project_miles_for_year
helper: given YTD miles through a cutoff date, project year-end
miles linearly. Explicitly labeled as a projection — the trip log
stays authoritative for actual miles driven.
"""
from __future__ import annotations

import logging
import sqlite3
from calendar import monthrange
from dataclasses import dataclass
from datetime import date as date_t
from decimal import Decimal

log = logging.getLogger(__name__)


def _filter_values(vehicle: dict) -> list[str]:
    values = {vehicle.get("slug")}
    if vehicle.get("display_name"):
        values.add(vehicle["display_name"])
    return [v for v in values if v]


@dataclass(frozen=True)
class MilesProjection:
    """Linear YTD-based projection of year-end miles + Schedule-C
    standard-mileage deduction."""
    year: int
    through_date: date_t
    days_elapsed: int
    days_in_year: int
    ytd_miles: float
    ytd_business_miles: float
    projected_total_miles: float | None
    projected_business_miles: float | None
    projected_standard_deduction: Decimal | None
    note: str


def _days_in_year(year: int) -> int:
    return 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365


def project_miles_for_year(
    conn: sqlite3.Connection,
    *,
    vehicle: dict,
    year: int,
    through_date: date_t | None = None,
    rate_per_mile: Decimal = Decimal("0.67"),
) -> MilesProjection:
    """Project year-end miles from year-to-date. through_date defaults
    to today (or Dec 31 if the target year is in the past)."""
    today = date_t.today()
    if through_date is None:
        through_date = min(today, date_t(year, 12, 31))
    # Clamp to the target year.
    year_start = date_t(year, 1, 1)
    year_end = date_t(year, 12, 31)
    if through_date < year_start:
        through_date = year_start
    if through_date > year_end:
        through_date = year_end

    days_elapsed = (through_date - year_start).days + 1
    total_days = _days_in_year(year)

    names = _filter_values(vehicle)
    if not names:
        return MilesProjection(
            year=year, through_date=through_date,
            days_elapsed=days_elapsed, days_in_year=total_days,
            ytd_miles=0.0, ytd_business_miles=0.0,
            projected_total_miles=None,
            projected_business_miles=None,
            projected_standard_deduction=None,
            note="no vehicle filter values",
        )
    placeholders = ",".join(["?"] * len(names))
    trip_sum = conn.execute(
        f"""
        SELECT COALESCE(SUM(e.miles), 0) AS total_miles,
               COALESCE(SUM(m.business_miles), 0) AS biz_miles
          FROM mileage_entries e
          LEFT JOIN mileage_trip_meta m
                 ON m.entry_date = e.entry_date
                AND m.vehicle = e.vehicle
                AND m.miles = e.miles
         WHERE e.entry_date >= ? AND e.entry_date <= ?
           AND (e.vehicle_slug = ? OR e.vehicle IN ({placeholders}))
        """,
        (
            year_start.isoformat(), through_date.isoformat(),
            vehicle.get("slug"), *names,
        ),
    ).fetchone()
    ytd_miles = float(trip_sum["total_miles"] or 0)
    ytd_business = float(trip_sum["biz_miles"] or 0)

    if ytd_miles <= 0 or days_elapsed <= 0:
        return MilesProjection(
            year=year, through_date=through_date,
            days_elapsed=days_elapsed, days_in_year=total_days,
            ytd_miles=ytd_miles, ytd_business_miles=ytd_business,
            projected_total_miles=None,
            projected_business_miles=None,
            projected_standard_deduction=None,
            note="no trips YTD to project from",
        )

    rate_daily_total = ytd_miles / days_elapsed
    rate_daily_biz = ytd_business / days_elapsed if days_elapsed > 0 else 0.0
    projected_total = round(rate_daily_total * total_days, 1)
    projected_biz = round(rate_daily_biz * total_days, 1)
    std_ded = (
        Decimal(str(projected_biz)) * rate_per_mile
    ).quantize(Decimal("0.01"))
    return MilesProjection(
        year=year, through_date=through_date,
        days_elapsed=days_elapsed, days_in_year=total_days,
        ytd_miles=round(ytd_miles, 1),
        ytd_business_miles=round(ytd_business, 1),
        projected_total_miles=projected_total,
        projected_business_miles=projected_biz,
        projected_standard_deduction=std_ded,
        note="linear projection from YTD pace",
    )


@dataclass(frozen=True)
class CostPerMilePoint:
    year: int
    month: int
    miles: float
    cost_usd: Decimal
    cost_per_mile_cents: int | None


def cost_per_mile_series(
    conn: sqlite3.Connection,
    *,
    vehicle: dict,
    year: int,
) -> list[CostPerMilePoint]:
    """Monthly (miles, $ fuel cost, $/mile) for the target year.

    Fuel cost comes from the Phase 5C fuel log rather than ledger
    postings — the log is granular enough to plot per month and
    avoids the fuel-log-vs-ledger-drift that the Phase 5C/6 health
    check will surface separately.
    """
    names = _filter_values(vehicle)
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    trip_rows = conn.execute(
        f"""
        SELECT CAST(strftime('%m', entry_date) AS INTEGER) AS mo,
               COALESCE(SUM(miles), 0) AS miles
          FROM mileage_entries
         WHERE entry_date >= ? AND entry_date < ?
           AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
         GROUP BY mo
        """,
        (
            f"{year:04d}-01-01", f"{year + 1:04d}-01-01",
            vehicle.get("slug"), *names,
        ),
    ).fetchall()
    miles_by_mo = {int(r["mo"]): float(r["miles"] or 0) for r in trip_rows}

    fuel_rows = conn.execute(
        """
        SELECT CAST(strftime('%m', as_of_date) AS INTEGER) AS mo,
               COALESCE(SUM(cost_cents), 0) AS cents
          FROM vehicle_fuel_log
         WHERE vehicle_slug = ?
           AND as_of_date >= ? AND as_of_date < ?
         GROUP BY mo
        """,
        (vehicle.get("slug"), f"{year:04d}-01-01", f"{year + 1:04d}-01-01"),
    ).fetchall()
    cost_by_mo = {int(r["mo"]): int(r["cents"] or 0) for r in fuel_rows}

    out: list[CostPerMilePoint] = []
    for mo in range(1, 13):
        miles = miles_by_mo.get(mo, 0.0)
        cents = cost_by_mo.get(mo, 0)
        usd = (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))
        cpm = None
        if miles > 0 and cents > 0:
            cpm = int(round(cents / miles))
        out.append(CostPerMilePoint(
            year=year, month=mo, miles=round(miles, 1),
            cost_usd=usd, cost_per_mile_cents=cpm,
        ))
    return out


@dataclass(frozen=True)
class YoYSeries:
    year: int
    months: list[float]              # 12 entries, miles per month


def yoy_miles_overlay(
    conn: sqlite3.Connection,
    *,
    vehicle: dict,
    year: int,
    prior_years: int = 3,
) -> list[YoYSeries]:
    """Month-bucketed mileage series for the target year + the N
    prior years. Intended as an overlay on the existing monthly
    bar chart — the UI plots each year as a separate line."""
    names = _filter_values(vehicle)
    if not names:
        return []
    placeholders = ",".join(["?"] * len(names))
    years = [year - i for i in range(0, prior_years + 1)]
    out: list[YoYSeries] = []
    for y in years:
        rows = conn.execute(
            f"""
            SELECT CAST(strftime('%m', entry_date) AS INTEGER) AS mo,
                   COALESCE(SUM(miles), 0) AS miles
              FROM mileage_entries
             WHERE entry_date >= ? AND entry_date < ?
               AND (vehicle_slug = ? OR vehicle IN ({placeholders}))
             GROUP BY mo
            """,
            (
                f"{y:04d}-01-01", f"{y + 1:04d}-01-01",
                vehicle.get("slug"), *names,
            ),
        ).fetchall()
        by_mo = {int(r["mo"]): float(r["miles"] or 0) for r in rows}
        months = [round(by_mo.get(m, 0.0), 1) for m in range(1, 13)]
        # Skip the series entirely if the year has zero data —
        # uncluttered overlay.
        if sum(months) > 0:
            out.append(YoYSeries(year=y, months=months))
    return out
