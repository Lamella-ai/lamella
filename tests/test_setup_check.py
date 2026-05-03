# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke tests for /setup/check."""
from __future__ import annotations


def test_setup_check_renders(app_client):
    resp = app_client.get("/setup/check")
    assert resp.status_code == 200
    assert "Setup check" in resp.text
    # All seven sections render.
    for title in (
        "Entities", "Accounts", "Vehicles", "Properties",
        "Loans", "Paperless field mapping", "Classification rules",
    ):
        assert title in resp.text, f"missing section {title!r}"


def test_setup_check_flags_unregistered_mileage_vehicle(app_client):
    """Common broken case: the mileage CSV references a vehicle
    that isn't in the registry. Setup-check must flag it."""
    db = app_client.app.state.db
    # Register only Work SUV.
    db.execute(
        "INSERT INTO vehicles (slug, display_name, entity_slug) "
        "VALUES (?, ?, ?)",
        ("work SUV-2009", "2009 Work SUV", "Personal"),
    )
    # But mileage has entries for Cargo Van too.
    db.execute(
        "INSERT INTO mileage_entries "
        "(entry_date, vehicle, miles, entity, csv_row_index, csv_mtime) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("2026-04-17", "Acme Cargo Van", 30.0, "Acme", 1, "2026-04-17 00:00:00"),
    )
    resp = app_client.get("/setup/check")
    assert resp.status_code == 200
    # The vehicle that's in mileage but not registered surfaces.
    assert "Acme Cargo Van" in resp.text
    assert "NOT registered" in resp.text


def test_setup_check_paperless_requires_canonical_roles(app_client):
    """When Paperless is configured but no canonical roles are
    mapped, setup-check flags the missing roles — required 'total'
    is a hard error, 'vendor' and 'receipt_date' are info-level
    because the matcher falls back to Paperless's built-in
    correspondent and created date."""
    # app_client fixture has paperless_configured=True via settings.
    resp = app_client.get("/setup/check")
    assert resp.status_code == 200
    # The 'Missing canonical role' issue fires because the test
    # fixture ledger has no paperless_field_map rows.
    assert (
        "Missing canonical role" in resp.text
        or "No Paperless fields synced" in resp.text
    )
