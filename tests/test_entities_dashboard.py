# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0047 user request: /entities is a dashboard, not a long form.
Card grid + per-entity focused edit page replaces "list every entity
in one form". Legacy bulk editor still reachable at /entities/legacy
for users who prefer that shape."""
from __future__ import annotations


def _seed(app_client, slug: str, **kwargs) -> None:
    conn = app_client.app.state.db
    fields = {
        "display_name": kwargs.get("display_name", slug + " Co."),
        "entity_type": kwargs.get("entity_type", "sole_proprietorship"),
        "tax_schedule": kwargs.get("tax_schedule", "C"),
        "is_active": kwargs.get("is_active", 1),
    }
    conn.execute(
        "INSERT INTO entities (slug, display_name, entity_type, "
        "  tax_schedule, is_active) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(slug) DO UPDATE SET "
        "  display_name = excluded.display_name, "
        "  entity_type = excluded.entity_type, "
        "  tax_schedule = excluded.tax_schedule, "
        "  is_active = excluded.is_active",
        (slug, fields["display_name"], fields["entity_type"],
         fields["tax_schedule"], fields["is_active"]),
    )
    conn.commit()


class TestDashboard:
    def test_renders_card_grid_not_long_form(self, app_client):
        _seed(app_client, "Acme")
        r = app_client.get("/entities")
        assert r.status_code == 200
        # Card grid uses record-card.
        assert "record-card" in r.text
        # Each card has a stable id for in-place HTMX swap, and an
        # hx-get pointing at the edit modal endpoint.
        assert 'id="entity-card-Acme"' in r.text
        assert 'hx-get="/entities/Acme/edit-modal"' in r.text

    def test_people_section_appears_first(self, app_client):
        # People before businesses (the setup-wizard pattern). Use the
        # grid-container ids so we don't accidentally match sidebar
        # nav labels that also contain the words "Businesses".
        _seed(app_client, "Acme", entity_type="sole_proprietorship")
        _seed(app_client, "Personal", entity_type="personal", tax_schedule="")
        r = app_client.get("/entities")
        assert r.status_code == 200
        body = r.text
        i_people = body.find('id="entities-people-grid"')
        i_biz = body.find('id="entities-businesses-grid"')
        assert i_people >= 0, "people grid should render"
        assert i_biz >= 0, "businesses grid should render"
        assert i_people < i_biz, (
            "people grid must come before businesses grid in DOM order"
        )

    def test_new_buttons_open_modals(self, app_client):
        r = app_client.get("/entities")
        assert r.status_code == 200
        # Two HTMX buttons (person + business), targeting the modal
        # fragment endpoint.
        assert 'hx-get="/entities/new-modal?kind=person"' in r.text
        assert 'hx-get="/entities/new-modal?kind=business"' in r.text

    def test_legacy_route_still_reachable_directly(self, app_client):
        # Bulk editor is no longer surfaced from the dashboard, but
        # the route exists for direct-URL access until full retirement.
        r = app_client.get("/entities/legacy")
        assert r.status_code == 200


class TestModals:
    def test_edit_modal_returns_form_fragment(self, app_client):
        _seed(app_client, "Acme")
        r = app_client.get("/entities/Acme/edit-modal")
        assert r.status_code == 200
        body = r.text
        # Modal fragment carries the form + targets the card by id.
        assert 'class="modal' in body
        assert 'hx-target="#entity-card-Acme"' in body
        assert 'hx-post="/settings/entities"' in body

    def test_edit_modal_404_for_unknown_slug(self, app_client):
        r = app_client.get("/entities/no-such/edit-modal")
        assert r.status_code == 404

    def test_new_modal_business_has_business_defaults(self, app_client):
        r = app_client.get("/entities/new-modal?kind=business")
        assert r.status_code == 200
        body = r.text
        # Sole prop is the canonical default for new businesses
        # (Schedule C, most common). The <select> emits the option
        # tag with selected for the matching value.
        assert 'value="sole_proprietorship" selected' in body
        assert 'value="C" selected' in body
        # New-modal targets the businesses-grid for in-place append.
        assert 'hx-target="#entities-businesses-grid"' in body

    def test_new_modal_person_has_person_defaults(self, app_client):
        r = app_client.get("/entities/new-modal?kind=person")
        assert r.status_code == 200
        body = r.text
        assert 'value="personal" selected' in body
        # Personal: no tax_schedule default.
        assert 'hx-target="#entities-people-grid"' in body


class TestSavePostHTMX:
    def test_post_with_card_target_returns_card_partial(self, app_client):
        _seed(app_client, "Acme", entity_type="sole_proprietorship", tax_schedule="C")
        # Simulate the modal-edit POST: HX-Request + HX-Target the
        # card id. Server should respond with the card partial,
        # which the modal then swaps into the dashboard.
        r = app_client.post(
            "/settings/entities",
            data={
                "slug": "Acme",
                "display_name": "Acme Co. Updated",
                "entity_type": "sole_proprietorship",
                "tax_schedule": "C",
                "is_active": "1",
            },
            headers={"HX-Request": "true", "HX-Target": "entity-card-Acme"},
        )
        assert r.status_code == 200
        body = r.text
        assert 'id="entity-card-Acme"' in body
        assert "Acme Co. Updated" in body
        # The "entity-saved" trigger fires; entities.html closes the
        # modal in response.
        assert r.headers.get("HX-Trigger") == "entity-saved"


class TestEdit:
    def test_edit_focused_page_still_works(self, app_client):
        # Focused per-entity page kept for direct navigation /
        # bookmarking. Works alongside the modal flow.
        _seed(app_client, "Acme")
        r = app_client.get("/entities/Acme/edit")
        assert r.status_code == 200
        assert "Acme" in r.text


class TestLegacy:
    def test_legacy_route_still_works(self, app_client):
        r = app_client.get("/entities/legacy")
        assert r.status_code == 200
        assert 'action="/settings/entities"' in r.text


class TestRedirect:
    def test_settings_entities_still_redirects(self, app_client):
        r = app_client.get("/settings/entities", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/entities"


class TestNew:
    def test_new_page_renders_focused_form(self, app_client):
        r = app_client.get("/entities/new")
        assert r.status_code == 200
        assert 'name="display_name"' in r.text
        assert 'name="slug"' in r.text
        assert 'name="entity_type"' in r.text
        assert 'name="tax_schedule"' in r.text


class TestEdit:
    def test_edit_page_404s_for_unknown_slug(self, app_client):
        r = app_client.get("/entities/no-such/edit")
        assert r.status_code == 404

    def test_edit_page_renders_for_known_slug(self, app_client):
        _seed(app_client, "Acme")
        r = app_client.get("/entities/Acme/edit")
        assert r.status_code == 200
        # Slug shows up but locked.
        assert "Acme" in r.text
        # Form posts to the canonical save route.
        assert 'action="/settings/entities"' in r.text

    def test_edit_page_links_to_business_dashboard_for_business_type(
        self, app_client,
    ):
        _seed(app_client, "Acme", entity_type="sole_proprietorship")
        r = app_client.get("/entities/Acme/edit")
        assert r.status_code == 200
        # Business type → quick link to per-business dashboard.
        assert "/businesses/Acme" in r.text


class TestLegacy:
    def test_legacy_route_still_works(self, app_client):
        r = app_client.get("/entities/legacy")
        assert r.status_code == 200
        # The legacy template is the long bulk form.
        assert 'action="/settings/entities"' in r.text


class TestRedirect:
    def test_settings_entities_still_redirects(self, app_client):
        r = app_client.get("/settings/entities", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/entities"
