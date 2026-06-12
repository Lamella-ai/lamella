# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 5A — full trip list + per-trip edit.

Covers the three gaps the user flagged before starting Phase 5:
  - /mileage/all paginated + filtered view
  - /mileage/{id}/edit form + POST /mileage/{id} update
  - 0-mile trips substantiated by ``notes`` alone are NOT flagged
    as missing_purpose, and the recent-trips partial surfaces the
    notes so the user sees something on those rows.
"""
from __future__ import annotations

import sqlite3
from datetime import date

from lamella.features.vehicles.health import compute_health


def _seed_vehicle(
    db, slug="SuvA", display_name="SuvA", entity_slug=None,
):
    if entity_slug:
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name) "
            "VALUES (?, ?)",
            (entity_slug, entity_slug),
        )
    db.execute(
        "INSERT OR REPLACE INTO vehicles "
        "(slug, display_name, entity_slug, is_active) "
        "VALUES (?, ?, ?, 1)",
        (slug, display_name, entity_slug),
    )


def _seed_trip(
    db, *,
    entry_date: str,
    vehicle: str = "SuvA",
    vehicle_slug: str | None = "SuvA",
    miles: float = 30.0,
    purpose: str | None = None,
    from_loc: str | None = None,
    to_loc: str | None = None,
    notes: str | None = None,
    entity: str = "Personal",
) -> int:
    cur = db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, miles,
             purpose, from_loc, to_loc, notes, entity, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual')
        """,
        (entry_date, vehicle, vehicle_slug, miles,
         purpose, from_loc, to_loc, notes, entity),
    )
    return int(cur.lastrowid)


# -------------- /mileage/all list ------------------------------------


def test_mileage_all_shows_every_trip(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    for i in range(3):
        _seed_trip(db, entry_date=f"2026-01-{i+1:02d}", miles=10.0 + i)
    db.commit()
    db.close()

    resp = app_client.get("/mileage/all")
    assert resp.status_code == 200
    body = resp.text
    assert "3 rows total" in body or "3 rows" in body
    assert "2026-01-01" in body
    assert "2026-01-03" in body


def test_mileage_all_pagination_reports_total(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    for i in range(5):
        _seed_trip(db, entry_date=f"2026-01-{i+1:02d}")
    db.commit()
    db.close()

    resp = app_client.get("/mileage/all")
    assert "5 rows total" in resp.text or "5 rows" in resp.text


def test_mileage_all_fix_purpose_excludes_notes_only_rows(
    app_client, settings,
):
    """Trip with miles=0 and only `notes` populated must NOT surface
    under fix=purpose — notes alone substantiates."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    _seed_trip(db, entry_date="2026-01-01", miles=0.0,
               notes="Oil change + filter")
    _seed_trip(db, entry_date="2026-01-02", miles=15.0)  # NO notes
    db.commit()
    db.close()

    resp = app_client.get("/mileage/all?fix=purpose")
    body = resp.text
    # Only the 01-02 trip (no substantiation of any kind) appears.
    assert "2026-01-02" in body
    assert "2026-01-01" not in body


def test_mileage_all_fix_splits_filters_to_missing_split(
    app_client, settings,
):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    id1 = _seed_trip(db, entry_date="2026-01-01", miles=30.0)
    id2 = _seed_trip(db, entry_date="2026-01-02", miles=20.0)
    db.execute(
        "INSERT INTO mileage_trip_meta "
        "(entry_date, vehicle, miles, business_miles, personal_miles) "
        "VALUES ('2026-01-02', 'SuvA', 20.0, 15.0, 5.0)"
    )
    db.commit()
    db.close()

    resp = app_client.get("/mileage/all?fix=splits")
    body = resp.text
    # Only 01-01 (no sidecar row) surfaces.
    assert "2026-01-01" in body
    assert "2026-01-02" not in body


def test_mileage_all_vehicle_filter(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="SuvA", display_name="SuvA")
    _seed_vehicle(db, slug="TruckB", display_name="TruckB")
    _seed_trip(db, entry_date="2026-01-01", vehicle="SuvA", vehicle_slug="SuvA")
    _seed_trip(db, entry_date="2026-01-02", vehicle="TruckB", vehicle_slug="TruckB")
    db.commit()
    db.close()

    resp = app_client.get("/mileage/all?vehicle=TruckB")
    body = resp.text
    assert "2026-01-02" in body
    assert "2026-01-01" not in body


# -------------- edit + update ----------------------------------------


def test_mileage_edit_form_renders_with_prefilled_values(
    app_client, settings,
):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    trip_id = _seed_trip(
        db, entry_date="2026-03-15", miles=42.5,
        purpose="Client visit", from_loc="Office", to_loc="Client",
        notes="Brought samples",
    )
    db.commit()
    db.close()

    resp = app_client.get(f"/mileage/{trip_id}/edit")
    assert resp.status_code == 200
    body = resp.text
    assert "Edit trip" in body
    assert 'value="2026-03-15"' in body
    assert "Client visit" in body
    assert "Brought samples" in body


def test_mileage_edit_post_updates_row(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="SuvA", display_name="SuvA")
    trip_id = _seed_trip(
        db, entry_date="2026-03-15", miles=42.5, purpose="Client visit",
    )
    db.commit()
    db.close()

    resp = app_client.post(
        f"/mileage/{trip_id}",
        data={
            "entry_date": "2026-03-16",
            "vehicle": "SuvA",
            "entity": "Personal",
            "miles": "50.0",
            "purpose": "Client visit — updated",
            "notes": "Added notes",
            "category": "business",
            "business_miles": "50.0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "saved=1" in resp.headers["location"]

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT entry_date, miles, purpose, notes, purpose_category "
        "FROM mileage_entries WHERE id = ?", (trip_id,),
    ).fetchone()
    meta = db.execute(
        "SELECT business_miles, category FROM mileage_trip_meta "
        "WHERE entry_date = ? AND vehicle = ? AND miles = ?",
        ("2026-03-16", "SuvA", 50.0),
    ).fetchone()
    db.close()
    assert row["entry_date"] == "2026-03-16"
    assert row["miles"] == 50.0
    assert row["purpose"] == "Client visit — updated"
    assert row["notes"] == "Added notes"
    assert row["purpose_category"] == "business"
    assert meta is not None
    assert meta["business_miles"] == 50.0
    assert meta["category"] == "business"


def test_mileage_edit_accepts_zero_miles_with_notes(app_client, settings):
    """Service day: miles=0 with maintenance notes is legitimate."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="SuvA", display_name="SuvA")
    trip_id = _seed_trip(db, entry_date="2026-03-15", miles=10.0)
    db.commit()
    db.close()

    resp = app_client.post(
        f"/mileage/{trip_id}",
        data={
            "entry_date": "2026-03-15",
            "vehicle": "SuvA",
            "entity": "Personal",
            "miles": "0",
            "notes": "Oil change + filter",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT miles, notes FROM mileage_entries WHERE id = ?",
        (trip_id,),
    ).fetchone()
    db.close()
    assert row["miles"] == 0.0
    assert row["notes"] == "Oil change + filter"


def test_mileage_edit_rejects_negative_miles(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="SuvA", display_name="SuvA")
    trip_id = _seed_trip(db, entry_date="2026-03-15")
    db.commit()
    db.close()

    resp = app_client.post(
        f"/mileage/{trip_id}",
        data={
            "entry_date": "2026-03-15",
            "vehicle": "SuvA",
            "entity": "Personal",
            "miles": "-5",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error" in resp.headers["location"]


def test_mileage_delete(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    trip_id = _seed_trip(db, entry_date="2026-03-15")
    db.commit()
    db.close()

    resp = app_client.post(f"/mileage/{trip_id}/delete", follow_redirects=False)
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    row = db.execute(
        "SELECT COUNT(*) FROM mileage_entries WHERE id = ?", (trip_id,),
    ).fetchone()
    db.close()
    assert row[0] == 0


def test_mileage_edit_404_for_missing_entry(app_client):
    resp = app_client.get("/mileage/99999/edit")
    assert resp.status_code == 404


# -------------- 0-mile substantiation ---------------------------------


def test_zero_mile_notes_only_not_flagged_missing_purpose(db):
    """Health check contract: miles=0 + notes="oil change" leaves no
    missing_purpose issue. This is the Phase 2 check; verifying here
    because Phase 5A extends the UX around it."""
    db.execute(
        "INSERT OR REPLACE INTO vehicles "
        "(slug, display_name, is_active) VALUES ('SuvA', 'SuvA', 1)"
    )
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, miles,
             purpose, notes, entity, source)
        VALUES ('2026-05-01', 'SuvA', 'SuvA', 0.0,
                NULL, 'Oil change + filter', 'Personal', 'manual')
        """
    )
    vehicle = {
        "slug": "SuvA", "display_name": "SuvA", "entity_slug": None,
    }
    issues = compute_health(db, vehicle=vehicle, year=2026)
    kinds = {i.kind for i in issues}
    assert "missing_purpose" not in kinds
