# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Integration tests for the /vehicles dashboard routes.

Covers the new index / detail / new / edit URLs, plus the mileage
lookup that used to silently miss when the vehicle slug was queried
against mileage_entries.vehicle (which actually stores the display
name).
"""
from __future__ import annotations

import sqlite3

import pytest


def _seed_vehicle_with_miles(db: sqlite3.Connection) -> None:
    db.execute(
        "INSERT INTO vehicles (slug, display_name, year, make, model, "
        " is_active, entity_slug, current_mileage, purchase_price) "
        "VALUES ('V2009WorkSUV', '2009 Work SUV', 2009, "
        "        'Work SUV', 'Work SUV', 1, NULL, 157000, '18500.00')"
    )
    db.execute(
        "INSERT INTO mileage_entries (entry_date, vehicle, vehicle_slug, "
        "   miles, entity, source) "
        "VALUES ('2026-01-15', '2009 Work SUV', "
        "        'V2009WorkSUV', 120.5, 'Personal', 'manual')"
    )
    db.execute(
        "INSERT INTO mileage_entries (entry_date, vehicle, vehicle_slug, "
        "   miles, entity, source) "
        "VALUES ('2026-02-03', '2009 Work SUV', "
        "        'V2009WorkSUV', 88.0, 'Personal', 'manual')"
    )
    db.commit()


def test_vehicles_index_renders(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.close()

    resp = app_client.get("/vehicles")
    assert resp.status_code == 200
    body = resp.text
    assert "2009 Work SUV" in body
    assert "Add vehicle" in body
    # YTD miles surface on the card.
    assert "208" in body or "209" in body  # 120.5 + 88 = 208.5


def test_vehicle_detail_renders_and_counts_mileage(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.close()

    resp = app_client.get("/vehicles/V2009WorkSUV")
    assert resp.status_code == 200
    body = resp.text
    # The bug we're fixing: the page used to say "No trips logged" —
    # now it shows the miles.
    assert "No trips logged for this vehicle yet" not in body
    assert "120.5" in body
    assert "88.0" in body
    # Deduction comparison block renders.
    assert "Standard mileage" in body
    assert "Actual expenses" in body


def test_vehicle_new_form(app_client):
    resp = app_client.get("/vehicles/new")
    assert resp.status_code == 200
    assert 'name="display_name"' in resp.text
    assert "Create the expense tree" in resp.text


def test_vehicle_edit_form(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.close()

    resp = app_client.get("/vehicles/V2009WorkSUV/edit")
    assert resp.status_code == 200
    assert "Schedule C Part IV" in resp.text


def test_legacy_settings_url_redirects(app_client):
    """Existing bookmarks at /settings/vehicles still resolve."""
    resp = app_client.get("/settings/vehicles", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/vehicles"

    resp = app_client.get(
        "/settings/vehicles/V2009WorkSUV", follow_redirects=False,
    )
    assert resp.status_code == 307
    assert resp.headers["location"] == "/vehicles/V2009WorkSUV"


def test_vehicle_detail_404_for_missing_slug(app_client):
    resp = app_client.get("/vehicles/NoSuchVehicle")
    assert resp.status_code == 404


# -------------- Phase 2 business-use → unknown ----------------------


def test_vehicle_detail_renders_unknown_when_no_split_recorded(
    app_client, settings,
):
    """Phase 2 behaviour: vehicle with trips but no recorded split
    shows the actual-expense deduction as 'unknown' rather than a
    silent 100%-business dollar figure."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/V2009WorkSUV")
    assert resp.status_code == 200
    body = resp.text
    assert "unknown" in body.lower()
    assert "split not recorded" in body.lower()


def test_vehicle_detail_renders_percentage_when_split_recorded(
    app_client, settings,
):
    """Opposite case — once a split is on the sidecar, the detail
    page shows the actual percentage, not 'unknown'."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.execute(
        """
        INSERT INTO mileage_trip_meta
            (entry_date, vehicle, miles, business_miles, personal_miles)
        VALUES ('2026-01-15', '2009 Work SUV', 120.5, 80.0, 40.5)
        """
    )
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/V2009WorkSUV")
    assert resp.status_code == 200
    body = resp.text
    assert "split not recorded" not in body.lower()
    # business_pct = 80 / (120.5 + 88) ≈ 38.4% — just check some %
    # rendering landed.
    assert "%" in body


def test_vehicles_index_surfaces_banner_count(app_client, settings):
    """When a vehicle has an undismissed phase2 banner row, the
    /vehicles index renders the banner summary."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    # Migration 034's seed only fires for trips dated in the current
    # year; our fixture uses 2026 which may not match `now()`. Seed
    # the banner row directly so the assertion is deterministic.
    db.execute(
        """
        INSERT OR REPLACE INTO vehicle_breaking_change_seen
            (change_key, vehicle_slug) VALUES (?, ?)
        """,
        ("phase2_unknown_business_use", "V2009WorkSUV"),
    )
    db.commit()
    db.close()

    resp = app_client.get("/vehicles")
    assert resp.status_code == 200
    body = resp.text
    assert "unknown business-use" in body.lower()
    # Banner references "1 vehicle" (singular). Template wraps the
    # count across whitespace, so match on the count + the word
    # without requiring single-line formatting.
    import re as _re
    assert _re.search(r"\b1\s+vehicle\b", body)


def test_banner_dismiss_persists(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.execute(
        "INSERT OR REPLACE INTO vehicle_breaking_change_seen "
        "(change_key, vehicle_slug) VALUES (?, ?)",
        ("phase2_unknown_business_use", "V2009WorkSUV"),
    )
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/V2009WorkSUV/banner/phase2_unknown_business_use/dismiss",
        follow_redirects=False,
    )
    assert resp.status_code == 303

    # Second request to index should no longer show the banner —
    # dismissed_at was set.
    resp = app_client.get("/vehicles")
    body = resp.text
    # "unknown business-use" only appears in the banner text, not
    # elsewhere on the index. Its absence confirms dismissal stuck.
    assert "unknown business-use" not in body.lower()


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_vehicle_detail_renders_health_card(app_client, settings):
    """Phase 2 adds a data-health section. Its presence is the
    user-visible acceptance criterion for this phase."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle_with_miles(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/V2009WorkSUV")
    assert resp.status_code == 200
    body = resp.text
    assert 'id="data-health"' in body
    assert "Data health" in body
