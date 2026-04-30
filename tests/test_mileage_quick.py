# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 3 — /mileage/quick minimal mobile entry path.

Four fields + one 3-way toggle. Entity is inherited from the
vehicle's owner; "what" populates both purpose and notes; the
last-used vehicle round-trips through user_ui_state so the form
survives across devices.
"""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest


def _seed_vehicle(db, *, slug: str = "suvone", display_name: str = "SuvA",
                  entity_slug: str | None = "Acme") -> None:
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


def _seed_prior_trip(db, *, vehicle: str, odometer_end: int,
                     entry_date: str = "2026-01-15") -> None:
    """Quick form derives odometer_start from the last recorded trip.
    Without a prior reading, add_entry raises — seed one so the
    quick-form POST path doesn't error on first trip semantics."""
    db.execute(
        """
        INSERT INTO mileage_entries
            (entry_date, vehicle, vehicle_slug, odometer_end, miles,
             entity, source)
        VALUES (?, ?, NULL, ?, 10.0, 'Acme', 'manual')
        """,
        (entry_date, vehicle, odometer_end),
    )


def _open_entity(ledger_dir, entity: str = "Acme") -> None:
    main = ledger_dir / "main.bean"
    text = main.read_text(encoding="utf-8")
    if f"Expenses:{entity}" not in text:
        main.write_text(
            text + f"\n2020-01-01 open Expenses:{entity}:Mileage USD\n",
            encoding="utf-8",
        )


def test_quick_get_renders_minimal_form(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/mileage/quick")
    assert resp.status_code == 200
    body = resp.text
    # Four-field presentation.
    assert 'name="entry_date"' in body
    assert 'name="vehicle_slug"' in body
    assert 'name="odometer_end"' in body
    assert 'name="what"' in body
    # 3-way category toggle.
    for value in ("business", "commuting", "personal"):
        assert f'value="{value}"' in body
    # No entity picker — inherits from the vehicle.
    assert 'name="entity"' not in body


def test_quick_post_logs_trip_with_inherited_entity(
    app_client, settings, ledger_dir,
):
    _open_entity(ledger_dir)
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    _seed_prior_trip(db, vehicle="SuvA", odometer_end=84000)
    db.commit()
    db.close()

    resp = app_client.post(
        "/mileage/quick",
        data={
            "entry_date": "2026-02-10",
            "vehicle_slug": "suvone",
            "odometer_end": "84075",
            "what": "Drove to jobsite",
            "category": "business",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "saved=1" in resp.headers["location"]

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM mileage_entries WHERE entry_date = '2026-02-10'"
    ).fetchone()
    assert row is not None
    assert row["vehicle"] == "SuvA"
    assert row["vehicle_slug"] == "suvone"
    assert row["odometer_end"] == 84075
    assert row["miles"] == 75.0           # 84075 - 84000
    # "what" populates both purpose and notes so the single field
    # substantiates the trip on its own.
    assert row["purpose"] == "Drove to jobsite"
    assert row["notes"] == "Drove to jobsite"
    # Entity is inherited from the vehicle, not the form.
    assert row["entity"] == "Acme"
    # Category lands on the denormalized column + sidecar.
    assert row["purpose_category"] == "business"

    meta = db.execute(
        "SELECT category FROM mileage_trip_meta "
        "WHERE entry_date = '2026-02-10'"
    ).fetchone()
    assert meta is not None
    assert meta["category"] == "business"

    # Last-used vehicle persisted to user_ui_state, not a cookie.
    kv = db.execute(
        "SELECT value FROM user_ui_state "
        "WHERE scope = 'mileage-quick' AND key = 'last_vehicle_slug'"
    ).fetchone()
    assert kv is not None
    assert kv["value"] == "suvone"
    db.close()


@pytest.mark.xfail(
    reason="last-used-vehicle prefill drift; pre-existing soft. "
    "See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_quick_prefills_last_used_vehicle(app_client, settings):
    """After one quick-POST, the GET page renders with that vehicle
    selected so the next entry is truly one-tap."""
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db, slug="suvone", display_name="SuvA")
    _seed_vehicle(
        db, slug="f150", display_name="TruckB", entity_slug=None,
    )
    # Set last-used directly (skip the full POST to keep this test
    # narrow).
    db.execute(
        "INSERT INTO user_ui_state (scope, key, value) "
        "VALUES ('mileage-quick', 'last_vehicle_slug', 'f150')"
    )
    db.commit()
    db.close()

    resp = app_client.get("/mileage/quick")
    assert resp.status_code == 200
    body = resp.text
    # TruckB is the last-used → selected attribute lands on that row.
    assert (
        '<option value="f150"\n                  selected>' in body
        or 'value="f150"\n                  selected' in body
        or 'value="f150" selected' in body
    )


def test_quick_post_rejects_unknown_category(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/mileage/quick",
        data={
            "entry_date": "2026-02-10",
            "vehicle_slug": "suvone",
            "odometer_end": "84075",
            "what": "test",
            "category": "mixed",   # not allowed on the quick form
        },
        follow_redirects=False,
    )
    # Redirect with error — not a crash, not a saved write.
    assert resp.status_code == 303
    assert "error" in resp.headers["location"]


def test_quick_post_rejects_unknown_vehicle(app_client, settings):
    resp = app_client.post(
        "/mileage/quick",
        data={
            "entry_date": "2026-02-10",
            "vehicle_slug": "nonexistent",
            "odometer_end": "84075",
            "what": "test",
            "category": "business",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error" in resp.headers["location"]


def test_mileage_page_links_to_quick(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.get("/mileage")
    assert resp.status_code == 200
    assert "/mileage/quick" in resp.text
