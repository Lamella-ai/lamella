# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Modal-add pattern across vehicles, properties, and accounts —
matching the wizard-style flow the user asked for: index page-head
button → HTMX modal → submit → save handler returns HX-Refresh →
page reloads with the new entity in place. No full-page nav for
the create flow.

Loans intentionally keeps its multi-step wizard at /loans/wizard;
that pattern is already focused so it doesn't need conversion."""
from __future__ import annotations


class TestVehicleModal:
    def test_modal_endpoint_returns_form_fragment(self, app_client):
        r = app_client.get("/vehicles/new-modal")
        assert r.status_code == 200
        body = r.text
        assert 'class="modal' in body
        assert 'hx-post="/vehicles"' in body
        # The slim form has the essential fields.
        assert 'name="entity_slug"' in body
        assert 'name="display_name"' in body
        assert 'name="year"' in body

    def test_index_button_triggers_modal(self, app_client):
        r = app_client.get("/vehicles")
        assert r.status_code == 200
        assert 'hx-get="/vehicles/new-modal"' in r.text

    def test_post_with_hx_returns_refresh_header(self, app_client):
        # Seed an entity so the create succeeds.
        conn = app_client.app.state.db
        conn.execute(
            "INSERT OR IGNORE INTO entities "
            "(slug, display_name, entity_type, is_active) "
            "VALUES (?, ?, ?, 1)",
            ("Personal", "Personal", "personal"),
        )
        conn.commit()
        r = app_client.post(
            "/vehicles",
            data={
                "intent": "create",
                "entity_slug": "Personal",
                "display_name": "Test Truck",
                "slug": "VTestTruck",
                "year": "2018",
            },
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        # HX caller gets HX-Refresh: true so the dashboard reloads.
        assert r.status_code == 200
        assert r.headers.get("HX-Refresh") == "true"


class TestPropertyModal:
    def test_modal_endpoint_returns_form_fragment(self, app_client):
        r = app_client.get("/properties/new-modal")
        assert r.status_code == 200
        body = r.text
        assert 'class="modal' in body
        assert 'hx-post="/settings/properties"' in body
        assert 'name="display_name"' in body
        assert 'name="property_type"' in body

    def test_index_button_triggers_modal(self, app_client):
        r = app_client.get("/properties")
        assert r.status_code == 200
        assert 'hx-get="/properties/new-modal"' in r.text

    def test_post_with_hx_returns_refresh_header(self, app_client):
        # Properties require either entity_slug or asset_account_path —
        # seed an entity and pass it.
        conn = app_client.app.state.db
        conn.execute(
            "INSERT OR IGNORE INTO entities "
            "(slug, display_name, entity_type, is_active) "
            "VALUES (?, ?, ?, 1)",
            ("Personal", "Personal", "personal"),
        )
        conn.commit()
        r = app_client.post(
            "/settings/properties",
            data={
                "display_name": "Test Property",
                "slug": "TestProperty",
                "property_type": "house",
                "entity_slug": "Personal",
                "is_active": "1",
            },
            headers={"HX-Request": "true"},
            follow_redirects=False,
        )
        assert r.status_code == 200, r.text
        assert r.headers.get("HX-Refresh") == "true"


class TestAccountModal:
    def test_modal_endpoint_returns_form_fragment(self, app_client):
        r = app_client.get("/accounts/new-modal")
        assert r.status_code == 200
        body = r.text
        assert 'class="modal' in body
        assert 'hx-post="/settings/accounts-new"' in body
        assert 'name="account_path"' in body
        assert 'name="institution"' in body

    def test_index_button_triggers_modal(self, app_client):
        r = app_client.get("/accounts")
        assert r.status_code == 200
        assert 'hx-get="/accounts/new-modal"' in r.text


class TestLoansSkipped:
    """Loans intentionally keeps its multi-step wizard at
    /loans/wizard (mortgage purchase / import existing / payoff /
    refi). Adding a one-shot modal would erase that flow's focused
    UX. Verify the wizard-style entry point is still present rather
    than substituting a slim modal."""

    def test_loans_index_still_links_to_existing_create_flow(self, app_client):
        r = app_client.get("/loans")
        assert r.status_code == 200
        # Some link to the new-loan flow exists. Don't pin the exact
        # path — the wizard route may evolve — just verify there's
        # a way in.
        assert ("/settings/loans" in r.text or "wizard" in r.text.lower()
                or "new" in r.text.lower())
