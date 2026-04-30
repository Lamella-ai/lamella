# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end tests for the /mileage/import route. Covers the
preview → commit round trip and the dedupe-on-second-import
behavior."""
from __future__ import annotations

import pytest


@pytest.fixture
def registered_vehicle(app_client):
    """Seed a vehicle in the registry so the import page can pick
    it; in production the user adds it via /settings/vehicles first."""
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO vehicles (slug, display_name, year, is_active) "
        "VALUES (?, ?, ?, ?)",
        ("work SUV-2009", "2009 Work SUV", 2009, 1),
    )
    return "work SUV-2009"


def test_import_page_renders(app_client, registered_vehicle):
    resp = app_client.get("/mileage/import")
    assert resp.status_code == 200
    # Vehicle dropdown lists the registry entry.
    assert "2009 Work SUV" in resp.text
    # Old comma-separated vehicle input must NOT appear.
    assert "mileage_vehicles" not in resp.text


def test_import_preview_flags_format(app_client, registered_vehicle):
    resp = app_client.post(
        "/mileage/import/preview",
        data={
            "vehicle": registered_vehicle,
            "entity": "Personal",
            "text": (
                "2025-01-01 08:00 69001 set out\n"
                "2025-01-01 12:00 PM 69011 done shopping\n"
            ),
        },
    )
    assert resp.status_code == 200
    # Preview shows both rows.
    assert "69011" in resp.text
    # Format detection surfaces.
    assert "text_anchor" in resp.text


def test_import_commit_persists_rows(app_client, registered_vehicle):
    resp = app_client.post(
        "/mileage/import/commit",
        data={
            "vehicle": registered_vehicle,
            "entity": "Personal",
            "text": (
                "2025-01-01,69001,69011,trip A\n"
                "2025-01-02,69011,69025,trip B\n"
            ),
        },
    )
    assert resp.status_code == 200
    assert "Imported 2 trips" in resp.text
    # Rows visible on the main /mileage page.
    page = app_client.get("/mileage")
    assert "14.0" in page.text or "14.00" in page.text  # second trip miles


def test_import_dedupe_skips_second_identical_commit(app_client, registered_vehicle):
    payload = {
        "vehicle": registered_vehicle,
        "entity": "Personal",
        "text": "2025-01-01,69001,69011,first import\n",
    }
    first = app_client.post("/mileage/import/commit", data=payload)
    assert first.status_code == 200
    assert "Imported 1 trip" in first.text

    second = app_client.post("/mileage/import/commit", data=payload)
    assert second.status_code == 200
    # Duplicate detection: nothing written the second time.
    assert "Imported 0 trip" in second.text


def test_import_rejects_unregistered_vehicle(app_client):
    """User has to add the vehicle in Settings → Vehicles first."""
    resp = app_client.post(
        "/mileage/import/preview",
        data={
            "vehicle": "ghost-truck",
            "entity": "Personal",
            "text": "2025-01-01,10,20,x\n",
        },
    )
    assert resp.status_code == 200
    assert "not in the registry" in resp.text


def test_import_batch_undo_removes_rows(app_client, registered_vehicle):
    commit = app_client.post(
        "/mileage/import/commit",
        data={
            "vehicle": registered_vehicle,
            "entity": "Personal",
            "text": "2025-01-01,100,120,x\n2025-01-02,120,140,y\n",
        },
    )
    assert "Imported 2 trips" in commit.text
    # Find the batch id.
    db = app_client.app.state.db
    (batch_id,) = db.execute(
        "SELECT id FROM mileage_imports ORDER BY id DESC LIMIT 1"
    ).fetchone()
    undo = app_client.post(
        f"/mileage/import/batches/{batch_id}/delete",
        follow_redirects=False,
    )
    assert undo.status_code in (303, 302)
    (remaining,) = db.execute(
        "SELECT COUNT(*) FROM mileage_entries"
    ).fetchone()
    assert remaining == 0


def test_file_upload_preview_then_commit_writes_rows(app_client, registered_vehicle):
    """Regression: when the user uploads a FILE (not paste) on the
    preview step, the commit step has to re-parse the same input.
    Previously the upload bytes didn't propagate, so preview showed
    51 rows and commit wrote 0. The preview now decodes the bytes
    into the hidden textarea so commit can re-parse from text."""
    csv_content = (
        b"Date,Starting Mileage,Ending Mileage,Purpose,Personal Miles,Business Miles\n"
        b"02/19/26,285948,286024,Grocery Store gas,0,76\n"
        b"02/20/26,286024,286038,UPS and PO,0,14\n"
    )
    preview = app_client.post(
        "/mileage/import/preview",
        data={"vehicle": registered_vehicle, "entity": ""},
        files={"file": ("log.csv", csv_content, "text/csv")},
    )
    assert preview.status_code == 200
    # Preview saw both rows.
    assert "285948" in preview.text
    assert "286024" in preview.text
    # Crucial: the hidden textarea now carries the decoded CSV so
    # the commit form has something to re-parse.
    assert "02/19/26" in preview.text
    assert "02/20/26" in preview.text
    # Preview shows the split columns because the CSV had them.
    assert "Biz" in preview.text and "Pers" in preview.text

    # Simulate the user clicking "Import 2 trips" — the form posts
    # the hidden text back. We replay the same payload.
    commit = app_client.post(
        "/mileage/import/commit",
        data={
            "vehicle": registered_vehicle,
            "entity": "",
            "filename": "log.csv",
            "text": csv_content.decode("utf-8"),
        },
    )
    assert commit.status_code == 200
    assert "Imported 2 trips" in commit.text


def test_zero_mile_markers_are_imported(app_client, registered_vehicle):
    """No-trips-today rows (odo_start == odo_end) are legitimate
    audit entries, not errors to skip."""
    resp = app_client.post(
        "/mileage/import/commit",
        data={
            "vehicle": registered_vehicle,
            "entity": "",
            "text": (
                "Date,Starting Mileage,Ending Mileage,Purpose\n"
                "02/10/26,285501,285501,No trips. Rebuilt 4x4 actuator\n"
                "02/11/26,285501,285607,Drove to Aurora\n"
            ),
        },
    )
    assert resp.status_code == 200
    assert "Imported 2 trips" in resp.text
    db = app_client.app.state.db
    rows = db.execute(
        "SELECT miles, purpose FROM mileage_entries ORDER BY entry_date"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["miles"] == 0.0
    assert "Rebuilt" in rows[0]["purpose"]


@pytest.mark.xfail(
    reason="mileage page vehicle-dropdown drift; pre-existing soft. "
    "See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_mileage_page_lists_only_registry_vehicles(app_client):
    """After the refactor, /mileage's vehicle dropdown sources
    exclusively from the vehicles registry — no comma-separated
    fallback, no historical-log fallback."""
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, ?)",
        ("suvone", "SuvA", 1),
    )
    # Seed a historical mileage row for a vehicle NOT in the
    # registry — it must NOT appear in the dropdown.
    db.execute(
        "INSERT INTO mileage_entries "
        "(entry_date, vehicle, miles, entity, source) "
        "VALUES (?, ?, ?, ?, ?)",
        ("2024-01-01", "GhostTruck", 50.0, "Personal", "csv_legacy"),
    )
    resp = app_client.get("/mileage")
    assert resp.status_code == 200
    assert "SuvA" in resp.text
    # Presence of GhostTruck only in "recent trips" list, not as a
    # dropdown option. Since recent trips also renders it by name,
    # the tight check is that the <select> section has SuvA but
    # not GhostTruck. Crude but effective: look inside the form.
    form_start = resp.text.find('name="vehicle"')
    form_end = resp.text.find("</select>", form_start)
    vehicle_select = resp.text[form_start:form_end]
    assert "SuvA" in vehicle_select
    assert "GhostTruck" not in vehicle_select
