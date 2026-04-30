# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 5E — vehicle forecasting (projection, $/mile, YoY overlay).

Pure view-time helpers — no state is persisted. All three must be
None-safe on empty input.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from lamella.features.vehicles.forecasting import (
    cost_per_mile_series,
    project_miles_for_year,
    yoy_miles_overlay,
)


def _seed_vehicle(db, slug="SuvA", display_name="2015 SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)", (slug, display_name),
    )


def _seed_trip(
    db, *, entry_date: str, vehicle: str = "2015 SuvA",
    vehicle_slug: str = "SuvA", miles: float,
    business: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, miles, entity, source)
        VALUES (?, ?, ?, ?, 'Personal', 'manual')
        """,
        (entry_date, vehicle, vehicle_slug, miles),
    )
    if business is not None:
        db.execute(
            """
            INSERT INTO mileage_trip_meta
                (entry_date, vehicle, miles, business_miles)
            VALUES (?, ?, ?, ?)
            """,
            (entry_date, vehicle, miles, business),
        )


def _vehicle_dict():
    return {"slug": "SuvA", "display_name": "2015 SuvA"}


# -------------- project_miles_for_year -------------------------------


def test_projection_linear_extrapolation(db):
    _seed_vehicle(db)
    # 500 miles total through March 31 (90 days) in 2026.
    _seed_trip(db, entry_date="2026-01-10", miles=200.0, business=150.0)
    _seed_trip(db, entry_date="2026-02-15", miles=200.0, business=150.0)
    _seed_trip(db, entry_date="2026-03-20", miles=100.0, business=50.0)

    proj = project_miles_for_year(
        db, vehicle=_vehicle_dict(),
        year=2026,
        through_date=date(2026, 3, 31),
        rate_per_mile=Decimal("0.67"),
    )
    assert proj.ytd_miles == 500.0
    assert proj.ytd_business_miles == 350.0
    # Daily rate: 500 / 90 = 5.555..; year total: 5.555 * 365 ≈ 2028
    assert proj.projected_total_miles is not None
    assert 2020 <= proj.projected_total_miles <= 2040
    assert proj.projected_standard_deduction is not None
    assert proj.projected_standard_deduction > Decimal("900")


def test_projection_none_with_no_ytd_trips(db):
    _seed_vehicle(db)
    proj = project_miles_for_year(
        db, vehicle=_vehicle_dict(), year=2026,
        through_date=date(2026, 6, 30),
    )
    assert proj.projected_total_miles is None
    assert proj.projected_business_miles is None
    assert proj.projected_standard_deduction is None
    assert "no trips" in proj.note.lower()


def test_projection_clamps_through_date_to_year_end(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2024-05-15", miles=100.0, business=100.0)
    # Ask for projection of 2024 with through_date=today (2026+) —
    # should clamp to 2024-12-31.
    proj = project_miles_for_year(
        db, vehicle=_vehicle_dict(), year=2024,
        through_date=date(2099, 1, 1),
    )
    assert proj.through_date == date(2024, 12, 31)
    # Full year elapsed → projection ≈ YTD.
    assert proj.days_elapsed == 366        # 2024 is a leap year


# -------------- cost_per_mile_series ---------------------------------


def test_cost_per_mile_combines_trips_and_fuel(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=100.0)
    _seed_trip(db, entry_date="2026-01-20", miles=200.0)
    db.execute(
        """
        INSERT INTO vehicle_fuel_log
            (vehicle_slug, as_of_date, fuel_type, quantity, unit,
             cost_cents)
        VALUES ('SuvA', '2026-01-15', 'gasoline', 10, 'gallon', 4200)
        """
    )
    rows = cost_per_mile_series(db, vehicle=_vehicle_dict(), year=2026)
    jan = rows[0]
    assert jan.month == 1
    assert jan.miles == 300.0
    assert jan.cost_usd == Decimal("42.00")
    # 4200 cents / 300 miles = 14 ¢/mi
    assert jan.cost_per_mile_cents == 14


def test_cost_per_mile_unknown_when_no_fuel_cost(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=100.0)
    # No fuel events.
    rows = cost_per_mile_series(db, vehicle=_vehicle_dict(), year=2026)
    jan = rows[0]
    assert jan.miles == 100.0
    assert jan.cost_per_mile_cents is None


def test_cost_per_mile_empty_year(db):
    _seed_vehicle(db)
    rows = cost_per_mile_series(db, vehicle=_vehicle_dict(), year=2026)
    assert len(rows) == 12
    for r in rows:
        assert r.miles == 0.0
        assert r.cost_per_mile_cents is None


# -------------- yoy_miles_overlay -------------------------------------


def test_yoy_overlay_returns_only_years_with_data(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2024-03-10", miles=100.0)
    _seed_trip(db, entry_date="2026-01-15", miles=200.0)
    # 2023 + 2025 have no trips — should be omitted.

    series = yoy_miles_overlay(
        db, vehicle=_vehicle_dict(), year=2026, prior_years=3,
    )
    years = {s.year for s in series}
    assert years == {2024, 2026}


def test_yoy_overlay_12_month_buckets(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-03-01", miles=50.0)
    _seed_trip(db, entry_date="2026-07-15", miles=75.0)

    series = yoy_miles_overlay(
        db, vehicle=_vehicle_dict(), year=2026, prior_years=1,
    )
    cur = next(s for s in series if s.year == 2026)
    assert len(cur.months) == 12
    assert cur.months[2] == 50.0    # March
    assert cur.months[6] == 75.0    # July
    # Every other month is 0.
    zero_months = [i for i, v in enumerate(cur.months) if v == 0.0]
    assert len(zero_months) == 10


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_forecast_section_renders_on_detail(app_client, settings):
    import sqlite3
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA")
    body = resp.text
    assert 'id="forecast"' in body
    assert "Forecast" in body
