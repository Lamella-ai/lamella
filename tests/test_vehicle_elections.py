# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 4 — §179 / bonus / MACRS elections capture.

Capture only — we don't compute §179 caps or determine eligibility.
PK is (vehicle_slug, tax_year) so editing an existing year replaces
rather than duplicating.
"""
from __future__ import annotations

import sqlite3


def _seed_vehicle(db, slug="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)",
        (slug, slug),
    )


def test_elections_upsert_replaces_on_same_year(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/elections",
        data={
            "tax_year": "2024",
            "depreciation_method": "section-179",
            "section_179_amount": "12000",
            "basis_at_placed_in_service": "35000",
            "listed_property_qualified": "0",
        },
    )
    # Second save for the same year replaces.
    app_client.post(
        "/vehicles/SuvA/elections",
        data={
            "tax_year": "2024",
            "depreciation_method": "MACRS-5YR",
            "section_179_amount": "15000",
        },
    )

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT * FROM vehicle_elections "
        "WHERE vehicle_slug = 'SuvA'"
    ).fetchall()
    db.close()
    assert len(rows) == 1
    row = rows[0]
    assert row["tax_year"] == 2024
    assert row["depreciation_method"] == "MACRS-5YR"
    assert row["section_179_amount"] == "15000"


def test_elections_multi_year(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    for year in (2023, 2024, 2025):
        app_client.post(
            "/vehicles/SuvA/elections",
            data={"tax_year": str(year), "section_179_amount": "1000"},
        )

    db = sqlite3.connect(str(settings.db_path))
    rows = db.execute(
        "SELECT tax_year FROM vehicle_elections "
        "WHERE vehicle_slug = 'SuvA' ORDER BY tax_year"
    ).fetchall()
    db.close()
    assert [r[0] for r in rows] == [2023, 2024, 2025]


def test_elections_invalid_method_becomes_null(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/elections",
        data={
            "tax_year": "2024",
            "depreciation_method": "ACRS-BIZARRO",
        },
    )
    db = sqlite3.connect(str(settings.db_path))
    row = db.execute(
        "SELECT depreciation_method FROM vehicle_elections "
        "WHERE vehicle_slug = 'SuvA' AND tax_year = 2024"
    ).fetchone()
    db.close()
    assert row[0] is None


def test_elections_delete_removes_row(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/elections",
        data={"tax_year": "2024", "section_179_amount": "12000"},
    )
    app_client.post(
        "/vehicles/SuvA/elections/2024/delete",
    )
    db = sqlite3.connect(str(settings.db_path))
    row = db.execute(
        "SELECT COUNT(*) FROM vehicle_elections "
        "WHERE vehicle_slug = 'SuvA' AND tax_year = 2024"
    ).fetchone()
    db.close()
    assert row[0] == 0


def test_elections_card_renders_on_detail(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    app_client.post(
        "/vehicles/SuvA/elections",
        data={
            "tax_year": "2024",
            "depreciation_method": "section-179",
            "section_179_amount": "12000",
            "basis_at_placed_in_service": "35000",
            "listed_property_qualified": "1",
        },
    )
    resp = app_client.get("/vehicles/SuvA")
    body = resp.text
    assert "Tax elections" in body
    assert "section-179" in body
    assert "12000" in body
    assert "35000" in body


def test_elections_unknown_vehicle_404(app_client):
    resp = app_client.post(
        "/vehicles/nonexistent/elections",
        data={"tax_year": "2024"},
        follow_redirects=False,
    )
    assert resp.status_code == 404
