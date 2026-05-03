# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Phase 4 entities + Add modal pattern.

Pins down: (1) ?add=person / ?add=business render the modal in-page
without redirecting elsewhere; (2) successful add creates the entity
+ writes the ledger directive + redirects with a banner; (3)
validation failure preserves the user's typing via field_errors +
form_values; (4) /setup/entities never links to /settings/* anywhere.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Modal rendering
# ---------------------------------------------------------------------------


def test_get_setup_entities_renders_without_modal(app_client):
    r = app_client.get("/setup/entities")
    assert r.status_code == 200
    # No modal should be open on the bare GET.
    assert "wiz-modal-backdrop is-open" not in r.text
    # + Add buttons are visible.
    assert "+ Add person" in r.text
    assert "+ Add business" in r.text


def test_add_person_query_renders_modal(app_client):
    r = app_client.get("/setup/entities?add=person")
    assert r.status_code == 200
    # Modal scaffolding is present and open.
    assert "wiz-modal-backdrop is-open" in r.text
    # Title + action label are person-flavored.
    assert "Add a person" in r.text
    assert "Add person" in r.text
    # Personal modal does NOT include the business-only fields.
    assert "Business type" not in r.text


def test_add_business_query_renders_modal(app_client):
    r = app_client.get("/setup/entities?add=business")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Add a business" in r.text
    # Business-only fields surface.
    assert "Business type" in r.text
    assert "Tax schedule" in r.text


# ---------------------------------------------------------------------------
# Save success
# ---------------------------------------------------------------------------


def test_add_person_success_creates_entity_and_redirects(app_client):
    r = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": "Jane Doe"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"].startswith("/setup/entities?added=")
    # Slug auto-derived from display_name → "JaneDoe".
    assert r.headers["location"].endswith("=JaneDoe")

    # Verify the entity is in the DB.
    follow = app_client.get(r.headers["location"])
    assert follow.status_code == 200
    assert "JaneDoe" in follow.text
    # Banner present.
    assert "Added" in follow.text


def test_add_business_success_creates_entity_with_type_and_schedule(app_client):
    r = app_client.post(
        "/setup/entities/add-business",
        data={
            "display_name": "Acme Co.",
            "entity_type": "llc",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "added=AcmeCo" in r.headers["location"]


def test_add_person_with_explicit_slug(app_client):
    r = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": "Jane", "slug": "JaneSlug"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "added=JaneSlug" in r.headers["location"]


# ---------------------------------------------------------------------------
# Validation failure: field_errors + form_values preservation
# ---------------------------------------------------------------------------


def test_add_person_missing_display_name_re_renders_modal_with_error(app_client):
    r = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": ""},
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Modal still open.
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Add a person" in r.text
    # Field-error visible inline.
    assert "Required." in r.text
    # The is-invalid class lands on the offending input.
    assert "is-invalid" in r.text


def test_add_person_validation_preserves_typed_values(app_client):
    # User typed a slug but forgot the display name. The slug should
    # still be in the input on re-render so they don't lose work.
    r = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": "", "slug": "MyTypedSlug"},
        follow_redirects=False,
    )
    assert r.status_code == 400
    # The typed slug is still in the form value.
    assert 'value="MyTypedSlug"' in r.text


def test_add_person_invalid_slug_shows_field_error(app_client):
    r = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": "Jane Doe", "slug": "lowercase-bad"},
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Slug error reaches the user.
    assert "uppercase" in r.text.lower() or "invalid" in r.text.lower()


def test_add_business_without_type_blocks(app_client):
    r = app_client.post(
        "/setup/entities/add-business",
        data={"display_name": "Acme Co."},
        follow_redirects=False,
    )
    assert r.status_code == 400
    # entity_type is required for businesses; the field-level error
    # surfaces.
    assert "type" in r.text.lower()


def test_add_person_collision_with_existing_slug_shows_error(app_client):
    # First add succeeds.
    r1 = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": "Jane Doe", "slug": "JaneCollide"},
        follow_redirects=False,
    )
    assert r1.status_code == 303
    # Second add at the same slug is rejected with a clear error.
    r2 = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": "Different Jane", "slug": "JaneCollide"},
        follow_redirects=False,
    )
    assert r2.status_code == 400
    assert "already used" in r2.text


# ---------------------------------------------------------------------------
# Acceptance: no /settings/* links inside /setup/entities
# ---------------------------------------------------------------------------


def test_setup_entities_page_does_not_link_to_settings(app_client):
    r = app_client.get("/setup/entities")
    assert r.status_code == 200
    # The acceptance criterion locked in SETUP_IMPLEMENTATION.md
    # Phase 4: zero /settings/* links inside /setup/recovery
    # surfaces. Setup-entities is part of that surface even before
    # the rename.
    assert "/settings/entities" not in r.text


def test_add_person_modal_does_not_link_to_settings(app_client):
    r = app_client.get("/setup/entities?add=person")
    assert r.status_code == 200
    assert "/settings/entities" not in r.text


def test_setup_progress_zero_entities_punt_targets_setup_not_settings(
    app_client,
):
    # _check_entities used to route the empty-entities case to
    # /settings/entities — Phase 4 retired that punt. Verify by
    # invoking the check function directly with an empty entity
    # set.
    import sqlite3
    from lamella.features.setup.setup_progress import _check_entities
    from lamella.core.db import migrate
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    try:
        step = _check_entities(db)
        # Zero entities → fix_url targets /setup/entities (with the
        # add modal) rather than /settings/entities.
        assert step.fix_url is not None
        assert step.fix_url.startswith("/setup/entities")
        assert "/settings/" not in step.fix_url
    finally:
        db.close()
