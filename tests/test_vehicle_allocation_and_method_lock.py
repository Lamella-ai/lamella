# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 6 — per-entity allocation + method-lock advisory."""
from __future__ import annotations

from lamella.features.vehicles.allocation import (
    allocation_for_year,
    set_trip_attribution,
)
from lamella.features.vehicles.method_lock import advisory_for_vehicle


def _seed_vehicle(db, slug="SuvA", display_name="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)", (slug, display_name),
    )


def _seed_trip(
    db, *, entry_date, vehicle="SuvA", vehicle_slug="SuvA",
    miles=30.0, entity="Personal",
):
    db.execute(
        "INSERT INTO mileage_entries "
        "(entry_date, vehicle, vehicle_slug, miles, entity, source) "
        "VALUES (?, ?, ?, ?, ?, 'manual')",
        (entry_date, vehicle, vehicle_slug, miles, entity),
    )


# -------------- allocation -------------------------------------------


def test_allocation_uses_trip_entity_by_default(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=100, entity="Acme")
    _seed_trip(db, entry_date="2026-02-10", miles=50, entity="Personal")

    alloc = allocation_for_year(
        db, vehicle={"slug": "SuvA", "display_name": "SuvA"},
        year=2026,
    )
    by = {a.entity: a for a in alloc}
    assert by["Acme"].miles == 100
    assert by["Personal"].miles == 50
    # Shares sum to ~1.0.
    assert abs(sum(a.share for a in alloc) - 1.0) < 0.01


def test_allocation_override_wins(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=100, entity="Acme")
    # User reassigns that trip to Ranch via the override.
    set_trip_attribution(
        db, entry_date="2026-01-10", vehicle="SuvA",
        miles=100.0, attributed_entity="Ranch",
    )
    alloc = allocation_for_year(
        db, vehicle={"slug": "SuvA", "display_name": "SuvA"},
        year=2026,
    )
    entities = {a.entity for a in alloc}
    assert "Ranch" in entities
    assert "Acme" not in entities


def test_allocation_clear_override(db):
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=100, entity="Acme")
    set_trip_attribution(
        db, entry_date="2026-01-10", vehicle="SuvA",
        miles=100.0, attributed_entity="Ranch",
    )
    # Clear by setting back to None.
    set_trip_attribution(
        db, entry_date="2026-01-10", vehicle="SuvA",
        miles=100.0, attributed_entity=None,
    )
    alloc = allocation_for_year(
        db, vehicle={"slug": "SuvA", "display_name": "SuvA"},
        year=2026,
    )
    entities = {a.entity for a in alloc}
    assert "Acme" in entities
    assert "Ranch" not in entities


# -------------- method-lock advisory --------------------------------


def test_method_lock_fires_after_macrs_year(db):
    _seed_vehicle(db)
    db.execute(
        "INSERT INTO vehicle_elections (vehicle_slug, tax_year, depreciation_method) "
        "VALUES (?, ?, ?)",
        ("SuvA", 2023, "MACRS-5YR"),
    )
    # No 2024 election on file — comparison page would invite the user
    # to consider switching methods; surface the advisory.
    adv = advisory_for_vehicle(db, vehicle_slug="SuvA", target_year=2024)
    assert adv is not None
    assert adv.first_macrs_year == 2023
    assert "MACRS-5YR" in adv.text
    assert "confirm with your tax professional" in adv.text


def test_method_lock_silent_when_continuing_macrs(db):
    _seed_vehicle(db)
    db.executemany(
        "INSERT INTO vehicle_elections (vehicle_slug, tax_year, depreciation_method) "
        "VALUES (?, ?, ?)",
        [
            ("SuvA", 2023, "MACRS-5YR"),
            ("SuvA", 2024, "MACRS-5YR"),
        ],
    )
    adv = advisory_for_vehicle(db, vehicle_slug="SuvA", target_year=2024)
    assert adv is None


def test_method_lock_silent_with_no_macrs_history(db):
    _seed_vehicle(db)
    # Only standard-mileage-style elections on file.
    adv = advisory_for_vehicle(db, vehicle_slug="SuvA", target_year=2024)
    assert adv is None


def test_attribution_http_route_persists(app_client, settings):
    import sqlite3
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=100, entity="Acme")
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/attribution",
        data={
            "entry_date": "2026-01-10",
            "vehicle": "SuvA",
            "miles": "100",
            "attributed_entity": "Ranch",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT attributed_entity FROM mileage_trip_meta "
        "WHERE entry_date = '2026-01-10' AND vehicle = 'SuvA'"
    ).fetchone()
    db.close()
    assert row["attributed_entity"] == "Ranch"
