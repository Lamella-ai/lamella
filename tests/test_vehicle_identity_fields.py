# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 4 — identity fields (GVWR, placed-in-service, fuel type).

These are captured on the vehicle row so Phase 5 / Form 4562 has
what it needs. No computation — we just round-trip the values.
"""
from __future__ import annotations

import sqlite3


def _seed_vehicle(db, slug="SuvA", display_name="2015 SuvA"):
    """Slug must start with a capital letter to survive normalize_slug."""
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)",
        (slug, display_name),
    )


def test_save_vehicle_persists_identity_fields(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles",
        data={
            "slug": "SuvA",
            "display_name": "2015 SuvA",
            "gvwr_lbs": "6800",
            "placed_in_service_date": "2024-01-15",
            "fuel_type": "gasoline",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT gvwr_lbs, placed_in_service_date, fuel_type "
        "FROM vehicles WHERE slug = 'SuvA'"
    ).fetchone()
    db.close()
    assert row["gvwr_lbs"] == 6800
    assert row["placed_in_service_date"] == "2024-01-15"
    assert row["fuel_type"] == "gasoline"


def test_save_vehicle_accepts_all_fuel_types(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="Ev1", display_name="Model 3")
    db.commit()
    db.close()

    for ft in ("gasoline", "diesel", "ev", "phev", "hybrid", "other"):
        app_client.post(
            "/vehicles",
            data={
                "slug": "Ev1", "display_name": "Model 3", "fuel_type": ft,
            },
        )
        db = sqlite3.connect(str(settings.db_path))
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT fuel_type FROM vehicles WHERE slug = 'Ev1'"
        ).fetchone()
        db.close()
        assert row["fuel_type"] == ft


def test_save_vehicle_silently_drops_invalid_fuel_type(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles",
        data={"slug": "SuvA", "display_name": "SuvA", "fuel_type": "plasma"},
    )
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT fuel_type FROM vehicles WHERE slug = 'SuvA'"
    ).fetchone()
    db.close()
    assert row["fuel_type"] is None


def test_identity_panel_renders_on_detail(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.execute(
        "UPDATE vehicles SET gvwr_lbs = 6800, "
        "placed_in_service_date = '2024-01-15', fuel_type = 'gasoline' "
        "WHERE slug = 'SuvA'"
    )
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA")
    assert resp.status_code == 200
    body = resp.text
    assert "Depreciation identity" in body
    assert "6,800" in body
    assert "2024-01-15" in body
    assert "gasoline" in body


def test_identity_panel_absent_when_fields_empty(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA")
    assert resp.status_code == 200
    # Section only renders when at least one identity field is set.
    assert "Depreciation identity" not in resp.text
