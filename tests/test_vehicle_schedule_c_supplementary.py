# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 5B — Schedule C Part IV supplementary fields.

Capture only. Five new columns on vehicle_yearly_mileage support
lines 44b (commute_days) and 45 / 46 / 47a / 47b (tri-state yes/no).
"""
from __future__ import annotations

import sqlite3


def _seed_vehicle(db, slug="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)",
        (slug, slug),
    )


def test_yearly_save_persists_all_supplementary_fields(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicles/SuvA/mileage",
        data={
            "year": "2024",
            "start_mileage": "60000",
            "end_mileage": "72000",
            "business_miles": "9000",
            "commuting_miles": "1500",
            "personal_miles": "1500",
            "commute_days": "230",
            "other_vehicle_available_personal": "1",
            "vehicle_available_off_duty": "0",
            "has_evidence": "1",
            "evidence_is_written": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = 'SuvA' AND year = 2024"
    ).fetchone()
    db.close()
    assert row["commute_days"] == 230
    assert row["other_vehicle_available_personal"] == 1
    assert row["vehicle_available_off_duty"] == 0
    assert row["has_evidence"] == 1
    assert row["evidence_is_written"] == 1


def test_yearly_save_unanswered_stays_null(app_client, settings):
    """Empty tri-state → NULL so the Phase 5D worksheet knows to
    render an empty checkbox rather than Yes or No."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/mileage",
        data={
            "year": "2024",
            "business_miles": "5000",
            # commute_days + all four tri-states omitted
        },
    )
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT commute_days, other_vehicle_available_personal, "
        "       vehicle_available_off_duty, has_evidence, evidence_is_written "
        "FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = 'SuvA' AND year = 2024"
    ).fetchone()
    db.close()
    assert row["commute_days"] is None
    assert row["other_vehicle_available_personal"] is None
    assert row["vehicle_available_off_duty"] is None
    assert row["has_evidence"] is None
    assert row["evidence_is_written"] is None


def test_yearly_save_upsert_preserves_original_on_second_save(
    app_client, settings,
):
    """Second POST with the same year replaces — idempotent from the
    user's perspective."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/mileage",
        data={
            "year": "2024",
            "commute_days": "200",
            "has_evidence": "0",
        },
    )
    app_client.post(
        "/vehicles/SuvA/mileage",
        data={
            "year": "2024",
            "commute_days": "250",
            "has_evidence": "1",
        },
    )
    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT commute_days, has_evidence FROM vehicle_yearly_mileage "
        "WHERE vehicle_slug = 'SuvA' AND year = 2024"
    ).fetchall()
    db.close()
    assert len(rows) == 1
    assert rows[0]["commute_days"] == 250
    assert rows[0]["has_evidence"] == 1


def test_edit_page_renders_supplementary_fieldset(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/vehicles/SuvA/edit")
    assert resp.status_code == 200
    body = resp.text
    assert "Schedule C Part IV supplementary" in body
    assert 'name="commute_days"' in body
    assert 'name="other_vehicle_available_personal"' in body
    assert 'name="vehicle_available_off_duty"' in body
    assert 'name="has_evidence"' in body
    assert 'name="evidence_is_written"' in body
