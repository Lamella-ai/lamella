# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 3 — POST /vehicles/{slug}/promote-trips.

Copies the per-year trip rollup into vehicle_yearly_mileage so the
user doesn't retype the numbers they already logged.
"""
from __future__ import annotations

import sqlite3


def _seed_vehicle(db, *, slug="suvone", display_name="SuvA",
                  entity_slug: str | None = None) -> None:
    if entity_slug:
        db.execute(
            "INSERT OR IGNORE INTO entities (slug, display_name) "
            "VALUES (?, ?)",
            (entity_slug, entity_slug),
        )
    db.execute(
        "INSERT OR REPLACE INTO vehicles "
        "(slug, display_name, entity_slug, is_active) VALUES (?, ?, ?, 1)",
        (slug, display_name, entity_slug),
    )


def _seed_trip(
    db,
    *,
    entry_date: str,
    vehicle: str,
    vehicle_slug: str | None,
    odometer_start: int | None = None,
    odometer_end: int | None = None,
    miles: float,
    business: float | None = None,
    commuting: float | None = None,
    personal: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug,
             odometer_start, odometer_end, miles,
             entity, source)
        VALUES (?, ?, ?, ?, ?, ?, 'Personal', 'manual')
        """,
        (entry_date, vehicle, vehicle_slug, odometer_start, odometer_end, miles),
    )
    if business is not None or commuting is not None or personal is not None:
        db.execute(
            """
            INSERT INTO mileage_trip_meta
                (entry_date, vehicle, miles, business_miles,
                 commuting_miles, personal_miles)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (entry_date, vehicle, miles, business, commuting, personal),
        )


def test_promote_trips_writes_yearly_row_matching_rollup(
    app_client, settings,
):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="suvone", display_name="SuvA")
    _seed_trip(
        db, entry_date="2026-01-05",
        vehicle="SuvA", vehicle_slug="suvone",
        odometer_start=84000, odometer_end=84050, miles=50.0,
        business=40.0, commuting=5.0, personal=5.0,
    )
    _seed_trip(
        db, entry_date="2026-02-15",
        vehicle="SuvA", vehicle_slug="suvone",
        odometer_start=84050, odometer_end=84100, miles=50.0,
        business=50.0,
    )
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/suvone/promote-trips?year=2026",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "saved=promoted" in resp.headers["location"]
    assert "year=2026" in resp.headers["location"]

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = 'suvone' AND year = 2026"
    ).fetchone()
    db.close()
    assert row is not None
    assert row["start_mileage"] == 84000
    assert row["end_mileage"] == 84100
    assert row["business_miles"] == 90      # 40 + 50
    assert row["commuting_miles"] == 5
    assert row["personal_miles"] == 5


def test_promote_trips_is_idempotent(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="suvone")
    _seed_trip(
        db, entry_date="2026-01-05",
        vehicle="SuvA", vehicle_slug="suvone",
        odometer_start=84000, odometer_end=84025, miles=25.0,
        business=25.0,
    )
    db.commit()
    db.close()

    for _ in range(2):
        resp = app_client.post(
            "/vehicles/suvone/promote-trips?year=2026",
            follow_redirects=False,
        )
        assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    rows = db.execute(
        "SELECT COUNT(*) AS n FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = 'suvone' AND year = 2026"
    ).fetchone()
    db.close()
    assert rows[0] == 1


def test_promote_trips_honors_tolerant_match_predicate(
    app_client, settings,
):
    """A trip stored with vehicle=display_name (legacy / CSV import)
    and vehicle_slug=NULL should still be picked up by the promote
    pass."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="suvone", display_name="SuvA")
    _seed_trip(
        db, entry_date="2026-01-05",
        vehicle="SuvA", vehicle_slug=None,          # legacy row
        odometer_start=84000, odometer_end=84030, miles=30.0,
        business=30.0,
    )
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/suvone/promote-trips?year=2026",
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    row = db.execute(
        "SELECT business_miles, start_mileage, end_mileage "
        "FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = 'suvone' AND year = 2026"
    ).fetchone()
    db.close()
    assert row is not None
    assert row[0] == 30
    assert row[1] == 84000
    assert row[2] == 84030


def test_promote_trips_unknown_vehicle_404(app_client):
    resp = app_client.post(
        "/vehicles/nosuchslug/promote-trips?year=2026",
        follow_redirects=False,
    )
    assert resp.status_code == 404


def test_detail_page_renders_promote_button_per_yearly_row(
    app_client, settings,
):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="suvone", display_name="SuvA")
    db.execute(
        "INSERT INTO vehicle_yearly_mileage "
        "(vehicle_slug, year, business_miles) VALUES ('suvone', 2025, 1000)"
    )
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/suvone")
    assert resp.status_code == 200
    body = resp.text
    assert "/vehicles/suvone/promote-trips?year=2025" in body
    assert "from trips" in body


def test_detail_page_shows_new_row_promote_when_no_yearly_row(
    app_client, settings,
):
    """When the target year has trip miles but no yearly row exists
    yet, the detail page shows a 'Copy <year> trip totals → new
    yearly row' primary action so the first use doesn't require the
    user to first create an empty row."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="suvone", display_name="SuvA")
    _seed_trip(
        db, entry_date="2026-01-05",
        vehicle="SuvA", vehicle_slug="suvone",
        odometer_start=84000, odometer_end=84050, miles=50.0,
        business=50.0,
    )
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/suvone?year=2026")
    assert resp.status_code == 200
    body = resp.text
    assert "/vehicles/suvone/promote-trips?year=2026" in body
    assert "new yearly row" in body
