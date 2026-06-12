# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""/api/slugs/check — slug validation + collision detection helper for
the add-modals' on-blur validator.

Returns JSON {available, suggestion, format_ok} for a given (kind, slug)
pair so the client-side helper can render an inline error / suggestion
without round-tripping a full form submit."""
from __future__ import annotations


def _seed_entity(conn, slug: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO entities "
        "(slug, display_name, entity_type, is_active) "
        "VALUES (?, ?, 'personal', 1)",
        (slug, slug),
    )
    conn.commit()


def _seed_vehicle(conn, slug: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO vehicles "
        "(slug, display_name, year, make, model, entity_slug, is_active) "
        "VALUES (?, ?, 2020, 'make', 'model', 'Personal', 1)",
        (slug, slug),
    )
    conn.commit()


class TestSlugApi:
    def test_unknown_kind_400_shape(self, app_client):
        r = app_client.get("/api/slugs/check?kind=garbage&slug=Acme")
        assert r.status_code == 200  # JSON shape; no HTTPException
        body = r.json()
        assert body["format_ok"] is False
        assert body["available"] is False
        assert "error" in body

    def test_invalid_format_returns_repaired_suggestion(self, app_client):
        r = app_client.get("/api/slugs/check?kind=entities&slug=acme co")
        body = r.json()
        assert body["format_ok"] is False
        assert body["available"] is False
        # suggest_slug PascalCases 'acme co' → 'AcmeCo'
        assert body["suggestion"] == "AcmeCo"

    def test_invalid_leading_digit_gets_x_prefix(self, app_client):
        r = app_client.get("/api/slugs/check?kind=entities&slug=2020Truck")
        body = r.json()
        assert body["format_ok"] is False
        # Leading digit → X-prefix
        assert body["suggestion"] == "X2020Truck"

    def test_available_when_no_collision(self, app_client):
        r = app_client.get("/api/slugs/check?kind=entities&slug=Brandnew")
        body = r.json()
        assert body["format_ok"] is True
        assert body["available"] is True
        assert body["suggestion"] is None

    def test_collision_returns_disambiguated_suggestion_for_vehicles(self, app_client):
        conn = app_client.app.state.db
        _seed_entity(conn, "Personal")
        _seed_vehicle(conn, "VTruck")
        r = app_client.get("/api/slugs/check?kind=vehicles&slug=VTruck")
        body = r.json()
        assert body["format_ok"] is True
        assert body["available"] is False
        # disambiguate_slug appends 2..999
        assert body["suggestion"] == "VTruck2"

    def test_collision_returns_disambiguated_suggestion_for_entities(self, app_client):
        conn = app_client.app.state.db
        _seed_entity(conn, "Acme")
        r = app_client.get("/api/slugs/check?kind=entities&slug=Acme")
        body = r.json()
        assert body["available"] is False
        assert body["suggestion"] == "Acme2"

    def test_collision_skips_existing_disambiguated(self, app_client):
        conn = app_client.app.state.db
        _seed_entity(conn, "Acme")
        _seed_entity(conn, "Acme2")
        r = app_client.get("/api/slugs/check?kind=entities&slug=Acme")
        body = r.json()
        assert body["available"] is False
        assert body["suggestion"] == "Acme3"

    def test_supports_properties_and_loans_kinds(self, app_client):
        # Smoke test — both kinds accepted, no collision returns
        # available=true.
        for kind in ("properties", "loans"):
            r = app_client.get(f"/api/slugs/check?kind={kind}&slug=NeverUsed")
            body = r.json()
            assert body["format_ok"] is True
            assert body["available"] is True
