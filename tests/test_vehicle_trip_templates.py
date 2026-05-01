# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 7 — trip templates CRUD + spawn-from-template logic."""
from __future__ import annotations

import sqlite3

from lamella.features.vehicles.templates import (
    delete_template,
    list_templates,
    spawn_from_template,
    upsert_template,
)


def _seed_vehicle(db, slug="SuvA"):
    db.execute(
        "INSERT INTO vehicles (slug, display_name, is_active) "
        "VALUES (?, ?, 1)", (slug, slug),
    )


def test_upsert_template_round_trip(db):
    _seed_vehicle(db)
    upsert_template(
        db, slug="ClientA", display_name="Office → Client A",
        vehicle_slug="SuvA", entity="Acme",
        default_from="Office", default_to="Client A",
        default_miles=18.5, default_category="business",
        is_round_trip=True,
    )
    rows = list_templates(db)
    assert len(rows) == 1
    t = rows[0]
    assert t.slug == "ClientA"
    assert t.is_round_trip
    assert t.default_miles == 18.5


def test_upsert_template_replaces_on_same_slug(db):
    _seed_vehicle(db)
    upsert_template(
        db, slug="Route1", display_name="Route 1",
        default_miles=10.0,
    )
    upsert_template(
        db, slug="Route1", display_name="Route 1 v2",
        default_miles=12.0,
    )
    rows = list_templates(db)
    assert len(rows) == 1
    assert rows[0].display_name == "Route 1 v2"
    assert rows[0].default_miles == 12.0


def test_spawn_round_trip_doubles_miles_and_suffixes_purpose(db):
    from lamella.features.vehicles.templates import TripTemplate
    t = TripTemplate(
        slug="ClientA", display_name="Office → Client A",
        vehicle_slug="SuvA", entity="Acme",
        default_from="Office", default_to="Client A",
        default_purpose="Client visit",
        default_miles=18.5, default_category="business",
        is_round_trip=True, is_active=True,
    )
    spawn = spawn_from_template(t)
    assert spawn["miles"] == 37.0
    assert "round trip" in (spawn["purpose"] or "").lower()


def test_spawn_one_way_keeps_miles_and_purpose(db):
    from lamella.features.vehicles.templates import TripTemplate
    t = TripTemplate(
        slug="Drop", display_name="Drop-off",
        vehicle_slug=None, entity=None,
        default_from="Home", default_to="Airport",
        default_purpose="Drop-off",
        default_miles=20.0, default_category="personal",
        is_round_trip=False, is_active=True,
    )
    spawn = spawn_from_template(t)
    assert spawn["miles"] == 20.0
    assert spawn["purpose"] == "Drop-off"


def test_list_templates_filters_inactive(db):
    upsert_template(db, slug="A", display_name="A", is_active=True)
    upsert_template(db, slug="B", display_name="B", is_active=False)
    visible = list_templates(db, include_inactive=False)
    assert {t.slug for t in visible} == {"A"}
    all_rows = list_templates(db, include_inactive=True)
    assert {t.slug for t in all_rows} == {"A", "B"}


def test_delete_template(db):
    upsert_template(db, slug="A", display_name="A")
    assert delete_template(db, "A")
    assert list_templates(db, include_inactive=True) == []


def test_templates_index_renders(app_client):
    resp = app_client.get("/vehicle-templates")
    assert resp.status_code == 200
    assert "Trip templates" in resp.text


def test_templates_http_save_and_delete(app_client, settings):
    db = sqlite3.connect(str(settings.db_path))
    _seed_vehicle(db)
    db.commit()
    db.close()

    resp = app_client.post(
        "/vehicle-templates",
        data={
            "slug": "ClientA",
            "display_name": "Office → Client A",
            "vehicle_slug": "SuvA",
            "entity": "Acme",
            "default_from": "Office",
            "default_to": "Client A",
            "default_purpose": "Client visit",
            "default_miles": "18.5",
            "default_category": "business",
            "is_round_trip": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = sqlite3.connect(str(settings.db_path))
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM vehicle_trip_templates WHERE slug = 'ClientA'"
    ).fetchone()
    db.close()
    assert row is not None
    assert row["is_round_trip"] == 1

    app_client.post("/vehicle-templates/ClientA/delete")

    db = sqlite3.connect(str(settings.db_path))
    n = db.execute("SELECT COUNT(*) FROM vehicle_trip_templates").fetchone()[0]
    db.close()
    assert n == 0
