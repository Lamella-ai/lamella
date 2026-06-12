# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Backdated mileage entries (quick-form derive-start path).

The end-only ``add_entry`` path used to anchor odometer_start to the
vehicle's globally-latest entry, so logging a forgotten trip for an
earlier date always conflicted with whatever was logged since. The
chain is now date-aware: a backdated reading slots between its
neighbors in (entry_date, odometer) order, the trip it splits is
re-anchored so miles aren't double-counted, and a genuinely
impossible reading (below the previous one, above the next one)
fails with a message naming the conflicting entry.

Within a single day the odometer IS the timeline — entries carry no
reliable times, so "Day 1 at 8PM I log a 1025 reading" slots between
that day's 1000 and 1050 readings by value.
"""
from __future__ import annotations

from datetime import date

import pytest

from lamella.features.mileage.service import (
    MileageService,
    MileageValidationError,
)

JAN1 = date(2026, 1, 1)
JAN2 = date(2026, 1, 2)
JAN3 = date(2026, 1, 3)


@pytest.fixture
def svc(db, tmp_path):
    return MileageService(conn=db, csv_path=tmp_path / "mileage.csv")


def _entries(db):
    return [
        dict(r) for r in db.execute(
            "SELECT entry_date, odometer_start, odometer_end, miles "
            "FROM mileage_entries ORDER BY entry_date, odometer_end"
        ).fetchall()
    ]


def test_backdated_entry_slots_between_days(svc, db):
    """Day 1 = 1000, Day 3 = 1200 → a Day 2 reading of 1100 lands
    between them and the Day 3 trip re-anchors to 1100→1200."""
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_start=900, odometer_end=1000)
    svc.add_entry(entry_date=JAN3, vehicle="Work Van", entity="Personal",
                  odometer_end=1200)

    row = svc.add_entry(entry_date=JAN2, vehicle="Work Van",
                        entity="Personal", odometer_end=1100)
    assert row.odometer_start == 1000
    assert row.miles == 100.0

    rows = _entries(db)
    assert [
        (r["entry_date"], r["odometer_start"], r["odometer_end"], r["miles"])
        for r in rows
    ] == [
        ("2026-01-01", 900, 1000, 100.0),
        ("2026-01-02", 1000, 1100, 100.0),
        ("2026-01-03", 1100, 1200, 100.0),  # re-anchored from 1000→1200
    ]
    # No double counting: total miles == odometer span.
    assert sum(r["miles"] for r in rows) == 1200 - 900


def test_backdated_below_prior_reading_rejected(svc):
    """Day 2 = 900 conflicts with Day 1's 1000 — readings can't go
    backwards."""
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_start=900, odometer_end=1000)
    with pytest.raises(MileageValidationError) as exc:
        svc.add_entry(entry_date=JAN2, vehicle="Work Van",
                      entity="Personal", odometer_end=900)
    assert "1000" in str(exc.value)
    assert "2026-01-01" in str(exc.value)


def test_backdated_above_next_reading_rejected(svc):
    """Day 2 = 1300 conflicts with Day 3's 1200 — a backdated reading
    must stay below every later one."""
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_start=900, odometer_end=1000)
    svc.add_entry(entry_date=JAN3, vehicle="Work Van", entity="Personal",
                  odometer_end=1200)
    with pytest.raises(MileageValidationError) as exc:
        svc.add_entry(entry_date=JAN2, vehicle="Work Van",
                      entity="Personal", odometer_end=1300)
    assert "1200" in str(exc.value)
    assert "2026-01-03" in str(exc.value)


def test_same_day_insertion_orders_by_odometer(svc, db):
    """Day 1: 1000 logged at 10AM, 1050 at 11AM. At 8PM the user
    backfills a forgotten 1025 reading for the same day — it slots
    between by odometer value and the 1050 trip re-anchors."""
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_start=900, odometer_end=1000)
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_end=1050)

    row = svc.add_entry(entry_date=JAN1, vehicle="Work Van",
                        entity="Personal", odometer_end=1025)
    assert row.odometer_start == 1000
    assert row.miles == 25.0

    rows = _entries(db)
    assert [
        (r["odometer_start"], r["odometer_end"], r["miles"]) for r in rows
    ] == [
        (900, 1000, 100.0),
        (1000, 1025, 25.0),
        (1025, 1050, 25.0),  # re-anchored from 1000→1050
    ]
    assert sum(r["miles"] for r in rows) == 1050 - 900


def test_same_day_below_all_readings_rejected(svc):
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_start=900, odometer_end=1000)
    with pytest.raises(MileageValidationError) as exc:
        svc.add_entry(entry_date=JAN1, vehicle="Work Van",
                      entity="Personal", odometer_end=800)
    assert "go backwards" in str(exc.value)


def test_insertion_inside_user_entered_trip_rejected(svc):
    """A backdated reading that lands inside a trip whose start the
    user typed explicitly (not chained) is refused — those miles are
    already substantiated and silently rewriting them would falsify
    the log."""
    svc.add_entry(entry_date=JAN1, vehicle="Work Van", entity="Personal",
                  odometer_start=900, odometer_end=1000)
    # Gap 1000→1150 (untracked), then an explicit 1150→1200 trip.
    svc.add_entry(entry_date=JAN3, vehicle="Work Van", entity="Personal",
                  odometer_start=1150, odometer_end=1200)
    with pytest.raises(MileageValidationError) as exc:
        svc.add_entry(entry_date=JAN2, vehicle="Work Van",
                      entity="Personal", odometer_end=1175)
    assert "1150" in str(exc.value)
    assert "already counted" in str(exc.value)


def test_quick_form_accepts_backdated_entry(app_client):
    """End-to-end through POST /mileage/quick: backdating an
    in-between date with an in-between reading works."""
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
        "VALUES (?, ?, ?, ?)",
        ("workvan", "Work Van", "Personal", 1),
    )
    db.commit()
    for payload in (
        {"entry_date": "2026-01-01", "odometer_start": "900",
         "odometer_end": "1000", "vehicle": "Work Van",
         "entity": "Personal", "purpose": "Day 1", "category": "business"},
    ):
        r = app_client.post("/mileage", data=payload, follow_redirects=False)
        assert r.status_code in (200, 303), r.text
    r = app_client.post(
        "/mileage/quick",
        data={"entry_date": "2026-01-03", "vehicle_slug": "Work Van",
              "odometer_end": "1200", "what": "Day 3", "category": "business"},
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    # The forgotten Day-2 trip.
    r = app_client.post(
        "/mileage/quick",
        data={"entry_date": "2026-01-02", "vehicle_slug": "Work Van",
              "odometer_end": "1100", "what": "Forgot this one",
              "category": "business"},
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    rows = db.execute(
        "SELECT entry_date, odometer_start, odometer_end "
        "FROM mileage_entries ORDER BY entry_date"
    ).fetchall()
    assert [(r["entry_date"], r["odometer_start"], r["odometer_end"])
            for r in rows] == [
        ("2026-01-01", 900, 1000),
        ("2026-01-02", 1000, 1100),
        ("2026-01-03", 1100, 1200),
    ]


def test_quick_form_backdated_conflict_keeps_input(app_client):
    """ADR-0066 meets backdating: a conflicting backdated reading
    errors with the conflicting entry named AND the input preserved."""
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO vehicles (slug, display_name, entity_slug, is_active) "
        "VALUES (?, ?, ?, ?)",
        ("workvan", "Work Van", "Personal", 1),
    )
    db.commit()
    r = app_client.post(
        "/mileage",
        data={"entry_date": "2026-01-01", "odometer_start": "900",
              "odometer_end": "1000", "vehicle": "Work Van",
              "entity": "Personal", "purpose": "Day 1",
              "category": "business"},
        follow_redirects=False,
    )
    assert r.status_code in (200, 303)
    r = app_client.post(
        "/mileage/quick",
        data={"entry_date": "2026-01-02", "vehicle_slug": "Work Van",
              "odometer_end": "900", "what": "Conflicting trip",
              "category": "business"},
    )
    assert r.status_code == 422
    assert "2026-01-01" in r.text       # names the conflicting entry
    assert "Conflicting trip" in r.text  # input preserved
    assert 'value="900"' in r.text
