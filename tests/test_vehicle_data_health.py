# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 2 — vehicle data-health registry.

Each Phase 2 check gets one fires-on-match + one no-false-positive
case. The registry itself is exercised via `compute_health(...)`
(so we're not poking at `_REGISTRY` directly).
"""
from __future__ import annotations

from datetime import date

from lamella.features.vehicles.health import compute_health


def _seed_vehicle(
    db,
    *,
    slug: str = "suvone",
    display_name: str = "SuvA",
    entity_slug: str | None = None,
    is_active: int = 1,
) -> dict:
    if entity_slug is not None:
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name) "
            "VALUES (?, ?)",
            (entity_slug, entity_slug),
        )
    db.execute(
        "INSERT OR REPLACE INTO vehicles "
        "(slug, display_name, entity_slug, is_active) "
        "VALUES (?, ?, ?, ?)",
        (slug, display_name, entity_slug, is_active),
    )
    return {
        "slug": slug,
        "display_name": display_name,
        "entity_slug": entity_slug,
        "is_active": is_active,
    }


def _seed_trip(
    db,
    *,
    entry_date: str,
    vehicle: str = "SuvA",
    vehicle_slug: str | None = "suvone",
    miles: float = 30.0,
    odometer_end: int | None = None,
    purpose: str | None = "Test trip",
    from_loc: str | None = None,
    to_loc: str | None = None,
    notes: str | None = None,
    entity: str = "Acme",
) -> None:
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, miles, odometer_end,
             purpose, from_loc, to_loc, notes, entity, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual')
        """,
        (
            entry_date, vehicle, vehicle_slug, miles, odometer_end,
            purpose, from_loc, to_loc, notes, entity,
        ),
    )


def _seed_trip_with_split(
    db,
    *,
    entry_date: str,
    vehicle: str = "SuvA",
    miles: float = 30.0,
    business: float | None = None,
    commuting: float | None = None,
    personal: float | None = None,
) -> None:
    _seed_trip(db, entry_date=entry_date, vehicle=vehicle, miles=miles)
    db.execute(
        """
        INSERT INTO mileage_trip_meta
            (entry_date, vehicle, miles, business_miles,
             commuting_miles, personal_miles)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (entry_date, vehicle, miles, business, commuting, personal),
    )


# -------------- missing_splits --------------------------------------


def test_missing_splits_fires_on_unsplit_trip(db):
    vehicle = _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-03-10", miles=30.0, purpose="Site")
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "missing_splits" in kinds
    # Count reflects the one unsplit trip.
    ms = next(i for i in issues if i.kind == "missing_splits")
    assert ms.count == 1


def test_missing_splits_silent_when_all_trips_have_splits(db):
    vehicle = _seed_vehicle(db)
    _seed_trip_with_split(
        db, entry_date="2026-03-10", miles=30.0,
        business=20.0, commuting=5.0, personal=5.0,
    )
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "missing_splits" not in kinds


# -------------- ambiguous_business_use ------------------------------


def test_ambiguous_business_use_fires_when_zero_splits_in_year(db):
    vehicle = _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-04-01", miles=15.0)
    _seed_trip(db, entry_date="2026-04-15", miles=22.0)
    issues = compute_health(db, vehicle=vehicle, year=2026)
    assert any(i.kind == "ambiguous_business_use" for i in issues)


def test_ambiguous_business_use_silent_when_any_split_recorded(db):
    vehicle = _seed_vehicle(db)
    _seed_trip_with_split(
        db, entry_date="2026-04-01", miles=15.0, business=10.0, personal=5.0,
    )
    _seed_trip(db, entry_date="2026-04-15", miles=22.0)   # no split
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "ambiguous_business_use" not in kinds
    # But missing_splits still surfaces the second trip.
    assert "missing_splits" in kinds


# -------------- odometer_non_monotonic ------------------------------


def test_odometer_non_monotonic_flags_decreasing_later_trip(db):
    vehicle = _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-10", miles=20.0, odometer_end=84100)
    _seed_trip(db, entry_date="2026-01-15", miles=30.0, odometer_end=84200)
    # Later trip's odometer goes BACKWARDS — typo or swapped start/end.
    _seed_trip(db, entry_date="2026-02-01", miles=10.0, odometer_end=84150)
    issues = compute_health(db, vehicle=vehicle, year=2026)
    assert any(i.kind == "odometer_non_monotonic" for i in issues)


def test_odometer_non_monotonic_ignores_backdated_import(db):
    """A later-imported, earlier-dated trip whose odometer sits below
    later rows should NOT fire — the check is max-of-strictly-earlier,
    not previous-row."""
    vehicle = _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-02-01", miles=30.0, odometer_end=84200)
    _seed_trip(db, entry_date="2026-03-01", miles=40.0, odometer_end=84300)
    # Backdated — earlier date, smaller odometer. Monotonic overall.
    _seed_trip(db, entry_date="2026-01-15", miles=20.0, odometer_end=84170)
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "odometer_non_monotonic" not in kinds


# -------------- missing_purpose -------------------------------------


def test_missing_purpose_fires_on_empty_substantiation(db):
    vehicle = _seed_vehicle(db)
    _seed_trip(
        db, entry_date="2026-05-01", miles=12.0,
        purpose=None, from_loc=None, to_loc=None, notes=None,
    )
    issues = compute_health(db, vehicle=vehicle, year=2026)
    assert any(i.kind == "missing_purpose" for i in issues)


def test_missing_purpose_silent_when_any_substantiation_present(db):
    vehicle = _seed_vehicle(db)
    _seed_trip(
        db, entry_date="2026-05-01", miles=12.0,
        purpose=None, from_loc="Office", to_loc=None, notes=None,
    )
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "missing_purpose" not in kinds


# -------------- yearly_row_drift ------------------------------------


def test_yearly_row_drift_fires_beyond_5_percent(db):
    vehicle = _seed_vehicle(db)
    # Yearly row claims 4000 business miles…
    db.execute(
        "INSERT INTO vehicle_yearly_mileage "
        "(vehicle_slug, year, business_miles) VALUES (?, 2026, 4000)",
        ("suvone",),
    )
    # …but trip log actually shows only 1000 (business_miles = 1000
    # across the two trip-meta rows).
    _seed_trip_with_split(
        db, entry_date="2026-01-10", miles=500.0, business=500.0,
    )
    _seed_trip_with_split(
        db, entry_date="2026-02-10", miles=500.0, business=500.0,
    )
    issues = compute_health(db, vehicle=vehicle, year=2026)
    assert any(i.kind == "yearly_row_drift" for i in issues)


def test_yearly_row_drift_silent_within_5_percent(db):
    vehicle = _seed_vehicle(db)
    db.execute(
        "INSERT INTO vehicle_yearly_mileage "
        "(vehicle_slug, year, business_miles) VALUES (?, 2026, 1000)",
        ("suvone",),
    )
    _seed_trip_with_split(
        db, entry_date="2026-01-10", miles=480.0, business=480.0,
    )
    _seed_trip_with_split(
        db, entry_date="2026-02-10", miles=505.0, business=505.0,
    )  # total 985 biz vs yearly 1000 — 1.5% drift
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "yearly_row_drift" not in kinds


# -------------- orphaned_trip ---------------------------------------


def test_orphaned_trip_fires_when_name_doesnt_match_registry(db):
    """A trip whose vehicle string matches neither any slug nor any
    display_name in the registry is orphaned. We detect it via a
    vehicle record whose display_name _is_ the orphan string — i.e.
    the user just renamed the vehicle."""
    # "Old Truck" is the current display_name we're querying under;
    # no vehicles row carries it as a slug or display_name except
    # the one we insert here. Orphaned rows are trips whose `vehicle`
    # value isn't present as either a slug or a display_name in the
    # vehicles table — they were stamped against a string that no
    # longer exists anywhere.
    db.execute(
        "INSERT OR REPLACE INTO vehicles "
        "(slug, display_name, is_active) VALUES ('truck', 'New Truck', 1)"
    )
    vehicle = {"slug": "truck", "display_name": "New Truck", "entity_slug": None}
    # Orphan trip references "Old Truck" which exists in no row.
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, miles, entity, source)
        VALUES ('2026-03-01', 'Old Truck', NULL, 15.0, 'Personal', 'csv_legacy')
        """
    )
    # _check_orphaned_trip queries for rows where vehicle = the
    # current display_name, so we need at least one row with the
    # missing name under inspection. To make the test meaningful we
    # also seed "New Truck" as a separate trip — that one stays
    # un-orphaned.
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, miles, entity, source)
        VALUES ('2026-03-02', 'New Truck', 'truck', 10.0, 'Personal', 'manual')
        """
    )
    # The health check fires against a vehicle object whose
    # display_name is the orphaned string. Simulate that by passing a
    # dict with display_name="Old Truck" (as if the user is on the
    # detail page for the vehicle that got renamed). The check reads
    # its own filter values from the passed vehicle dict.
    orphan_vehicle = {
        "slug": "truck",
        "display_name": "Old Truck",
        "entity_slug": None,
    }
    issues = compute_health(db, vehicle=orphan_vehicle, year=2026)
    assert any(i.kind == "orphaned_trip" for i in issues)


# -------------- clean vehicle: no false positives -------------------


def test_clean_fixture_produces_no_issues(db):
    vehicle = _seed_vehicle(db)
    _seed_trip_with_split(
        db, entry_date="2026-01-10", miles=30.0,
        business=25.0, commuting=3.0, personal=2.0,
    )
    # Yearly row matches the trip rollup exactly.
    db.execute(
        "INSERT INTO vehicle_yearly_mileage "
        "(vehicle_slug, year, business_miles, commuting_miles, personal_miles) "
        "VALUES ('suvone', 2026, 25, 3, 2)"
    )
    issues = compute_health(db, vehicle=vehicle, year=2026)
    assert issues == []
