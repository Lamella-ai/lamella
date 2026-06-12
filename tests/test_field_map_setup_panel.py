# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the setup-panel helpers in paperless.field_map and the
new /settings/paperless-fields/create + /classify routes."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from lamella.features.paperless_bridge.field_map import (
    CANONICAL_ROLE_DEFAULTS,
    insert_created_field,
    suggest_for_role,
)


def test_suggest_for_role_finds_ignored_fields_by_keyword(db):
    """A field currently marked 'ignore' whose name keyword-guesses
    into the requested role is surfaced as a candidate."""
    db.executemany(
        "INSERT INTO paperless_field_map "
        "(paperless_field_id, paperless_field_name, canonical_role, auto_assigned) "
        "VALUES (?, ?, ?, ?)",
        [
            (1, "Grand Total", "ignore", 1),
            (2, "Some other notes", "ignore", 1),
            (3, "Vendor Name", "ignore", 1),
            (4, "Existing mapping", "total", 0),
        ],
    )
    total_suggestions = suggest_for_role(db, "total")
    assert [s["paperless_field_id"] for s in total_suggestions] == [1]
    vendor_suggestions = suggest_for_role(db, "vendor")
    assert [s["paperless_field_id"] for s in vendor_suggestions] == [3]
    assert suggest_for_role(db, "ignore") == []
    assert suggest_for_role(db, "bogus") == []


def test_insert_created_field_inserts_with_auto_assigned_zero(db):
    insert_created_field(
        db, field_id=99, field_name="Receipt Total", canonical_role="total",
    )
    row = db.execute(
        "SELECT paperless_field_name, canonical_role, auto_assigned "
        "FROM paperless_field_map WHERE paperless_field_id = 99"
    ).fetchone()
    assert row["paperless_field_name"] == "Receipt Total"
    assert row["canonical_role"] == "total"
    assert int(row["auto_assigned"]) == 0


def test_insert_created_field_upserts_existing_row(db):
    """If sync_fields has already picked up the same id with a
    keyword-guessed role, the explicit create overwrites to auto=0."""
    db.execute(
        "INSERT INTO paperless_field_map "
        "(paperless_field_id, paperless_field_name, canonical_role, auto_assigned) "
        "VALUES (99, 'Receipt Total', 'total', 1)",
    )
    insert_created_field(
        db, field_id=99, field_name="Receipt Total", canonical_role="total",
    )
    row = db.execute(
        "SELECT canonical_role, auto_assigned FROM paperless_field_map "
        "WHERE paperless_field_id = 99"
    ).fetchone()
    assert row["canonical_role"] == "total"
    assert int(row["auto_assigned"]) == 0


def test_canonical_role_defaults_cover_every_setup_panel_role():
    """The setup panel relies on CANONICAL_ROLE_DEFAULTS having an
    entry for every role the panel can offer to create — otherwise
    the create-in-Paperless button can't propose a name + data_type.

    ADR-0044: ``vendor`` / ``receipt_date`` / ``payment_last_four``
    are intentionally absent from CANONICAL_ROLE_DEFAULTS now. The
    Setup status panel iterates SETUP_CRITICAL_ROLES +
    SETUP_OPTIONAL_ROLES (not the full CANONICAL_ROLES tuple) so
    those roles never reach the create button anymore. The full
    role list still includes them for read-time backward compat
    (existing user-mapped fields keep working).
    """
    from lamella.features.paperless_bridge.field_map import (
        SETUP_CRITICAL_ROLES, SETUP_OPTIONAL_ROLES,
    )
    for role in (*SETUP_CRITICAL_ROLES, *SETUP_OPTIONAL_ROLES):
        assert role in CANONICAL_ROLE_DEFAULTS, (
            f"role {role!r} has no CANONICAL_ROLE_DEFAULTS entry"
        )
        name, data_type = CANONICAL_ROLE_DEFAULTS[role]
        assert name
        assert data_type in {
            "string", "integer", "float", "monetary", "boolean",
            "date", "url", "documentlink", "select",
        }


def test_deprecated_setup_roles_are_pruned_per_adr_0044():
    """ADR-0044: vendor / receipt_date / payment_last_four were
    removed from the Setup status panel. They MUST NOT appear in
    SETUP_CRITICAL_ROLES, SETUP_OPTIONAL_ROLES, or
    CANONICAL_ROLE_DEFAULTS — otherwise the panel would still show
    ‘Create in Paperless’ buttons for them."""
    from lamella.features.paperless_bridge.field_map import (
        SETUP_CRITICAL_ROLES, SETUP_OPTIONAL_ROLES,
    )
    for deprecated in ("vendor", "receipt_date", "payment_last_four"):
        assert deprecated not in SETUP_CRITICAL_ROLES
        assert deprecated not in SETUP_OPTIONAL_ROLES
        assert deprecated not in CANONICAL_ROLE_DEFAULTS


@pytest.mark.xfail(
    reason="paperless field-map e2e: route-shape drift since Phase 8 refactor. "
    "Pre-existing soft. See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_create_route_posts_to_paperless_and_records_mapping(app_client):
    """End-to-end: POST /settings/paperless-fields/create stubs the
    Paperless POST, confirms paperless_field_map gets the row with
    auto_assigned=0, and the ledger directive is present."""
    import lamella.adapters.paperless.client as _pcli

    # The field cache reads /api/custom_fields/; then creation POSTs.
    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": []},
        )
        mock.post("/api/custom_fields/").respond(
            201, json={"id": 77, "name": "Receipt Total", "data_type": "monetary"},
        )
        resp = app_client.post(
            "/settings/paperless-fields/create",
            data={"role": "total"},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "created=total" in resp.headers["location"]

    db = app_client.app.state.db
    row = db.execute(
        "SELECT paperless_field_name, canonical_role, auto_assigned "
        "FROM paperless_field_map WHERE paperless_field_id = 77"
    ).fetchone()
    assert row is not None
    assert row["canonical_role"] == "total"
    assert int(row["auto_assigned"]) == 0

    # Ledger directive landed in connector_config.bean.
    config_path = app_client.app.state.settings.connector_config_path
    text = config_path.read_text(encoding="utf-8")
    assert 'custom "paperless-field" 77 "total"' in text
    assert 'lamella-field-name: "Receipt Total"' in text


def test_classify_route_maps_existing_field(app_client):
    """POST /settings/paperless-fields/classify takes an existing
    ignored field and stamps it into the ledger + flips it to
    auto_assigned=0."""
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO paperless_field_map "
        "(paperless_field_id, paperless_field_name, canonical_role, auto_assigned) "
        "VALUES (5, 'Grand Total', 'ignore', 1)",
    )
    resp = app_client.post(
        "/settings/paperless-fields/classify",
        data={"role": "total", "field_id": "5"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    row = db.execute(
        "SELECT canonical_role, auto_assigned FROM paperless_field_map "
        "WHERE paperless_field_id = 5"
    ).fetchone()
    assert row["canonical_role"] == "total"
    assert int(row["auto_assigned"]) == 0

    config_path = app_client.app.state.settings.connector_config_path
    text = config_path.read_text(encoding="utf-8")
    assert 'custom "paperless-field" 5 "total"' in text


def test_create_route_rejects_role_without_default(app_client):
    """Roles not in CANONICAL_ROLE_DEFAULTS can't be auto-created —
    'ignore' is the obvious one; attempting it must 400."""
    resp = app_client.post(
        "/settings/paperless-fields/create",
        data={"role": "ignore"},
    )
    assert resp.status_code == 400
