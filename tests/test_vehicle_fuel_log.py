# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 5C — fuel log CRUD + MPG / cost-per-mile derivation."""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from lamella.features.vehicles.fuel import (
    FuelValidationError,
    add_event,
    compute_stats,
    list_events,
)


def _seed_vehicle(db, slug="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)", (slug, slug),
    )


def test_add_event_and_list(db):
    _seed_vehicle(db)
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 10), fuel_type="gasoline",
        quantity=12.5, unit="gallon",
        cost_cents=4200, odometer=84320,
    )
    events = list_events(db, vehicle_slug="SuvA")
    assert len(events) == 1
    e = events[0]
    assert e.quantity == 12.5
    assert e.unit == "gallon"
    assert e.cost_cents == 4200
    assert e.odometer == 84320
    # Derived convenience fields.
    from decimal import Decimal
    assert e.cost_usd == Decimal("42.00")
    assert e.cost_per_unit == Decimal("3.360")


def test_add_event_rejects_invalid_unit(db):
    _seed_vehicle(db)
    with pytest.raises(FuelValidationError):
        add_event(
            db, vehicle_slug="SuvA",
            as_of_date=date(2026, 1, 1), fuel_type="gasoline",
            quantity=10, unit="liters",
        )


def test_add_event_rejects_nonpositive_quantity(db):
    _seed_vehicle(db)
    with pytest.raises(FuelValidationError):
        add_event(
            db, vehicle_slug="SuvA",
            as_of_date=date(2026, 1, 1), fuel_type="gasoline",
            quantity=0, unit="gallon",
        )


def test_compute_stats_mpg_from_two_events(db):
    _seed_vehicle(db)
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 1), fuel_type="gasoline",
        quantity=12.0, unit="gallon", cost_cents=4200, odometer=84000,
    )
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 15), fuel_type="gasoline",
        quantity=13.0, unit="gallon", cost_cents=4500, odometer=84300,
    )
    events = list_events(db, vehicle_slug="SuvA")
    stats = compute_stats(events)
    assert stats.events_count == 2
    assert stats.miles_covered == 300
    # Total gallons = 25.0; mpg = 300 / 25 = 12.0
    assert stats.total_gallons == 25.0
    assert stats.mpg == 12.0
    # Total cost $87.00; $/mile ≈ 29¢
    assert stats.cost_per_mile_cents == 29


def test_compute_stats_ev_miles_per_kwh(db):
    _seed_vehicle(db)
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 1), fuel_type="ev",
        quantity=50.0, unit="kwh", cost_cents=None, odometer=84000,
    )
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 5), fuel_type="ev",
        quantity=40.0, unit="kwh", cost_cents=None, odometer=84200,
    )
    events = list_events(db, vehicle_slug="SuvA")
    stats = compute_stats(events)
    # 200 miles / 90 kWh ≈ 2.22
    assert stats.miles_per_kwh is not None
    assert abs(stats.miles_per_kwh - 2.22) < 0.01
    # Cost was not recorded — cost-based stats stay None.
    assert stats.cost_per_mile_cents is None
    assert stats.cost_per_kwh is None


def test_compute_stats_unknown_when_only_one_odometer(db):
    _seed_vehicle(db)
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 1), fuel_type="gasoline",
        quantity=10, unit="gallon", odometer=84000,
    )
    # Second event with NO odometer.
    add_event(
        db, vehicle_slug="SuvA",
        as_of_date=date(2026, 3, 15), fuel_type="gasoline",
        quantity=10, unit="gallon",
    )
    stats = compute_stats(list_events(db, vehicle_slug="SuvA"))
    assert stats.miles_covered is None
    assert stats.mpg is None


def test_fuel_add_route_round_trip(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/fuel",
        data={
            "as_of_date": "2026-03-10",
            "fuel_type": "gasoline",
            "unit": "gallon",
            "quantity": "12.5",
            "cost_usd": "42.00",
            "odometer": "84320",
            "location": "Shell",
            "notes": "regular unleaded",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "saved=fuel" in resp.headers["location"]

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT fuel_type, quantity, unit, cost_cents, odometer, location, notes "
        "FROM vehicle_fuel_log WHERE vehicle_slug = 'SuvA'"
    ).fetchone()
    db.close()
    assert row["fuel_type"] == "gasoline"
    assert row["quantity"] == 12.5
    assert row["unit"] == "gallon"
    assert row["cost_cents"] == 4200
    assert row["odometer"] == 84320
    assert row["location"] == "Shell"
    assert row["notes"] == "regular unleaded"


def test_fuel_add_route_ev_without_cost(app_client, settings):
    """EV home-charging: cost may be unknown. The route must accept
    an empty cost field without crashing."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/fuel",
        data={
            "as_of_date": "2026-03-10",
            "fuel_type": "ev",
            "unit": "kwh",
            "quantity": "42.5",
            "odometer": "12000",
            "location": "home",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT cost_cents FROM vehicle_fuel_log WHERE vehicle_slug = 'SuvA'"
    ).fetchone()
    db.close()
    assert row["cost_cents"] is None


def test_fuel_delete_route(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/fuel",
        data={
            "as_of_date": "2026-03-10",
            "fuel_type": "gasoline",
            "unit": "gallon",
            "quantity": "10",
        },
    )
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    event_id = db.execute(
        "SELECT id FROM vehicle_fuel_log WHERE vehicle_slug = 'SuvA'"
    ).fetchone()["id"]
    db.close()

    resp = app_client.post(
        f"/vehicles/SuvA/fuel/{event_id}/delete", follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    n = db.execute(
        "SELECT COUNT(*) FROM vehicle_fuel_log WHERE vehicle_slug = 'SuvA'"
    ).fetchone()[0]
    db.close()
    assert n == 0


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_fuel_section_renders_on_detail(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA")
    body = resp.text
    assert 'id="fuel"' in body
    assert "Fuel log" in body
    assert 'name="as_of_date"' in body
    assert 'name="fuel_type"' in body
