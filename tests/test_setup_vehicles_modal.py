# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Phase 4 vehicles + Add modal pattern.

Mirrors test_setup_loans_modal.py shape: modal renders in-page,
save success creates the vehicle + redirects, validation failure
preserves typing, linked-loan dropdown pulls from real loans table
with explicit "no loan" option, no auto-scaffold of per-vehicle
accounts (locked Phase 4 constraint), no /settings/* link in the
add path.
"""
from __future__ import annotations

import sqlite3

import pytest


def _seed_entity(app_client, slug: str = "TestVehicleEntity"):
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        existing = db.execute(
            "SELECT slug FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        if existing is not None:
            return
    finally:
        db.close()
    r = app_client.post(
        "/setup/entities/add-person",
        data={"display_name": slug, "slug": slug},
        follow_redirects=False,
    )
    assert r.status_code == 303, f"seed entity failed: {r.text[:300]}"


def _seed_loan(app_client, *, slug: str, entity: str):
    """Seed a loan via the live route so the modal's loan-dropdown
    has something real to validate against."""
    _seed_entity(app_client, entity)
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": f"Loan {slug}",
            "slug": slug,
            "loan_type": "auto",
            "entity_slug": entity,
            "institution": "Test Credit Union",
            "original_principal": "20000",
            "funded_date": "2024-01-15",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, f"seed loan failed: {r.text[:300]}"


# ---------------------------------------------------------------------------
# Modal rendering
# ---------------------------------------------------------------------------


def test_get_setup_vehicles_renders_without_modal(app_client):
    r = app_client.get("/setup/vehicles")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text
    assert "+ Add vehicle" in r.text


def test_add_vehicle_query_renders_modal(app_client):
    r = app_client.get("/setup/vehicles?add=vehicle")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Add a vehicle" in r.text
    # Required fields visible.
    assert "Display name" in r.text
    assert "Owning entity" in r.text


def test_modal_loan_dropdown_includes_no_loan_option(app_client):
    """Locked constraint: cash-purchased vehicles must have a clear
    affordance, not just an unlabelled blank field. The UI moved from
    a <select> with an explicit option to a <datalist>+placeholder, so
    the affordance now lives in the input placeholder + helper text."""
    r = app_client.get("/setup/vehicles?add=vehicle")
    assert r.status_code == 200
    assert "cash" in r.text.lower()


def test_modal_loan_dropdown_lists_real_loans(app_client):
    _seed_loan(app_client, slug="TestVehicleLoan", entity="TestVehicleEntity")
    r = app_client.get("/setup/vehicles?add=vehicle")
    assert r.status_code == 200
    # Dropdown contains the seeded loan.
    assert "TestVehicleLoan" in r.text


def test_modal_exposes_entity_datalist(app_client):
    r = app_client.get("/setup/vehicles?add=vehicle")
    assert r.status_code == 200
    assert 'id="setup-vehicles-entities-datalist"' in r.text
    assert 'list="setup-vehicles-entities-datalist"' in r.text


# ---------------------------------------------------------------------------
# Save success
# ---------------------------------------------------------------------------


def test_add_vehicle_success_creates_row_and_redirects(app_client):
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "2008 Fabrikam Suv",
            "slug": "V2008FabrikamSuv",
            "entity_slug": "TestVehicleEntity",
            "year": "2008",
            "make": "Fabrikam",
            "model": "FabrikamSuv",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert "added=V2008FabrikamSuv" in r.headers["location"]


def test_add_vehicle_auto_derives_slug(app_client):
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "2009 Fabrikam Suv",
            "entity_slug": "TestVehicleEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    # normalize_slug prepends "X" when leading char isn't alpha →
    # "2009 Fabrikam Suv" → "X2009FabrikamSuv".
    assert "added=X2009FabrikamSuv" in r.headers["location"]


def test_add_vehicle_does_not_auto_scaffold_chart_accounts(app_client):
    """Locked Phase 4 constraint: vehicle creation does NOT open the
    Assets:{Entity}:Vehicle:{slug} or Expenses:{Entity}:Vehicle:{slug}:*
    accounts. The user explicitly clicks Scaffold-N-missing on the
    row when ready. Verify by checking accounts_meta after add."""
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Test Truck",
            "slug": "TestTruck",
            "entity_slug": "TestVehicleEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        # No Assets:TestVehicleEntity:Vehicle:TestTruck account_meta row.
        row = db.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE account_path = ?",
            ("Assets:TestVehicleEntity:Vehicle:TestTruck",),
        ).fetchone()
        assert row is None, (
            "vehicle add should NOT have auto-scaffolded the asset account"
        )
        # No Expenses:* per-vehicle subtree either.
        chart_rows = db.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE account_path LIKE 'Expenses:%TestTruck:%'"
        ).fetchall()
        assert chart_rows == [], (
            "vehicle add should NOT have auto-scaffolded any chart accounts"
        )
    finally:
        db.close()


def test_add_vehicle_with_linked_loan_validates(app_client):
    _seed_loan(app_client, slug="VehLoan1", entity="TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Linked Truck",
            "slug": "LinkedTruck",
            "entity_slug": "TestVehicleEntity",
            "linked_loan_slug": "VehLoan1",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]


def test_add_vehicle_with_explicit_no_loan(app_client):
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Cash Truck",
            "slug": "CashTruck",
            "entity_slug": "TestVehicleEntity",
            "linked_loan_slug": "",  # explicit "no loan"
        },
        follow_redirects=False,
    )
    assert r.status_code == 303


# ---------------------------------------------------------------------------
# Validation failure
# ---------------------------------------------------------------------------


def test_add_vehicle_missing_display_name_re_renders_modal(app_client):
    r = app_client.post(
        "/setup/vehicles/add",
        data={"display_name": ""},
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Required." in r.text


def test_add_vehicle_validation_preserves_typed_values(app_client):
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Half Filled",
            "slug": "HalfFilled",
            "year": "2010",
            "make": "Fabrikam",
            "model": "Pilot",
            # No entity_slug.
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert 'value="Half Filled"' in r.text
    assert 'value="HalfFilled"' in r.text
    assert 'value="2010"' in r.text
    assert 'value="Fabrikam"' in r.text
    assert 'value="Pilot"' in r.text


def test_add_vehicle_invalid_year_blocks(app_client):
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Bad Year",
            "slug": "BadYear",
            "entity_slug": "TestVehicleEntity",
            "year": "abcd",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "Year must be" in r.text or "year" in r.text.lower()


def test_add_vehicle_unknown_loan_slug_blocks(app_client):
    """Locked constraint: linked_loan_slug must reference a real loan
    or be blank ("no loan"). Free-text invalid slugs are rejected."""
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Bad Loan",
            "slug": "BadLoan",
            "entity_slug": "TestVehicleEntity",
            "linked_loan_slug": "TotallyMadeUpLoan",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Phase 4-checkpoint audit tightened the error message: vehicles
    # filter to vehicle-shaped loan_types only ("auto", "personal",
    # "student", "eidl", "other"), and the error spells that out.
    assert "vehicle-shaped" in r.text or "No active" in r.text


def test_add_vehicle_collision_offers_disambiguated_slug(app_client):
    _seed_entity(app_client, "TestVehicleEntity")
    r1 = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Dupe",
            "slug": "DupeVehicle",
            "entity_slug": "TestVehicleEntity",
        },
        follow_redirects=False,
    )
    assert r1.status_code == 303
    r2 = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Dupe Again",
            "slug": "DupeVehicle",
            "entity_slug": "TestVehicleEntity",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 400
    assert "already taken" in r2.text


def test_add_vehicle_invalid_fuel_type_blocks(app_client):
    _seed_entity(app_client, "TestVehicleEntity")
    r = app_client.post(
        "/setup/vehicles/add",
        data={
            "display_name": "Weird Fuel",
            "slug": "WeirdFuel",
            "entity_slug": "TestVehicleEntity",
            "fuel_type": "nuclear",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "fuel" in r.text.lower()


# ---------------------------------------------------------------------------
# Acceptance: no /settings/* punts in the modal flow
# ---------------------------------------------------------------------------


def test_setup_vehicles_does_not_link_to_settings(app_client):
    """Recovery layout — no app sidebar in the DOM, simple
    'no /settings/X anywhere' assertion is valid."""
    r = app_client.get("/setup/vehicles")
    assert r.status_code == 200
    # Empty vehicle list state has no row links to /vehicles/{slug}/edit
    # so /settings/vehicles shouldn't appear (it doesn't anyway).
    assert "/settings/vehicles" not in r.text


def test_add_vehicle_modal_does_not_punt_to_main_app(app_client):
    r = app_client.get("/setup/vehicles?add=vehicle")
    assert r.status_code == 200
    # Modal form posts to recovery handler, not /vehicles main app
    # POST.
    assert 'action="/setup/vehicles/add"' in r.text
    # The OLD + Add button used to send the user to /vehicles/new.
    # Phase 4 retired that.
    assert 'href="/vehicles/new"' not in r.text


def test_add_vehicle_button_targets_modal(app_client):
    r = app_client.get("/setup/vehicles")
    assert r.status_code == 200
    assert 'href="/setup/vehicles?add=vehicle"' in r.text


# ===========================================================================
# Phase 8 step 6 — + Edit modal
# ===========================================================================


def _seed_vehicle(
    app_client,
    *,
    slug: str = "EditTestVeh",
    entity: str = "EditVehEntity",
    extras: dict | None = None,
):
    """Seed a vehicle via the + Add path so the Edit tests have a row.
    Returns the slug actually used."""
    _seed_entity(app_client, entity)
    data = {
        "display_name": f"{slug} display",
        "slug": slug,
        "entity_slug": entity,
        "year": "2018",
        "make": "TestMake",
        "model": "TestModel",
    }
    if extras:
        data.update(extras)
    r = app_client.post(
        "/setup/vehicles/add", data=data, follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    return slug


def test_per_row_edit_link_targets_setup_modal_not_settings(app_client):
    """Phase 8 step 6: per-row Edit / Set-owning-entity links route
    to ?edit={slug}. The recovery flow no longer breaks out to
    /vehicles/{slug}/edit for routine field edits."""
    slug = _seed_vehicle(app_client)
    r = app_client.get("/setup/vehicles")
    assert r.status_code == 200
    assert f"/setup/vehicles?edit={slug}" in r.text
    # The bare /vehicles/{slug}/edit link is gone for the row's
    # primary action (still appears as a power-user pointer inside
    # the entity-locked help text in the Edit modal — intentional).
    assert f'href="/vehicles/{slug}/edit"' not in r.text
    assert f'href="/vehicles/{slug}"' not in r.text


def test_edit_query_renders_modal_prefilled_with_row_values(app_client):
    """?edit={slug} opens the modal with the row's current values
    populated."""
    slug = _seed_vehicle(
        app_client,
        extras={"vin": "1HGBH41JXMN109186", "license_plate": "ABC-123"},
    )
    r = app_client.get(f"/setup/vehicles?edit={slug}")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert f"Edit vehicle: {slug}" in r.text
    assert f'value="{slug} display"' in r.text
    assert 'value="2018"' in r.text
    assert 'value="TestMake"' in r.text
    assert 'value="1HGBH41JXMN109186"' in r.text
    # Slug field is read-only.
    assert "readonly" in r.text


def test_edit_query_with_unknown_slug_renders_page_without_modal(app_client):
    r = app_client.get("/setup/vehicles?edit=DoesNotExist")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text


def test_edit_save_updates_row_and_redirects(app_client):
    slug = _seed_vehicle(app_client)
    r = app_client.post(
        f"/setup/vehicles/{slug}/edit",
        data={
            "display_name": "Updated display",
            "entity_slug": "EditVehEntity",
            "year": "2022",
            "make": "NewMake",
            "model": "NewModel",
            "vin": "JN1AZ4EH7AM568231",
            "fuel_type": "ev",
            "gvwr_lbs": "5500",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert r.headers["location"] == f"/setup/vehicles?updated={slug}"

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT display_name, year, make, model, vin, fuel_type, gvwr_lbs "
        "  FROM vehicles WHERE slug = ?",
        (slug,),
    ).fetchone()
    db.close()
    assert row["display_name"] == "Updated display"
    assert row["year"] == 2022
    assert row["make"] == "NewMake"
    assert row["fuel_type"] == "ev"
    assert row["gvwr_lbs"] == 5500


def test_edit_preserves_entity_slug_when_currently_set(app_client):
    """Entity-lock contract: once a vehicle has entity_slug set, the
    Edit modal's entity_slug submission is silently ignored.
    Mirrors test_vehicle_edit_cannot_change_entity_slug for the
    /vehicles save handler."""
    slug = _seed_vehicle(app_client, entity="EditVehEntity")
    _seed_entity(app_client, "OtherVehEntity")

    r = app_client.post(
        f"/setup/vehicles/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "OtherVehEntity",  # attempt to change
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    row = db.execute(
        "SELECT entity_slug FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    db.close()
    assert row[0] == "EditVehEntity", (
        "Edit modal allowed entity_slug change — entity-lock contract "
        "broken; transitions must go through /vehicles/{slug}/"
        "change-ownership"
    )


def test_edit_blank_year_clears_field(app_client):
    """Optional fields like year clear when submitted blank — the
    user might have typed a wrong year and need to remove it."""
    slug = _seed_vehicle(app_client)
    r = app_client.post(
        f"/setup/vehicles/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "EditVehEntity",
            "year": "",  # blank — clears
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    row = db.execute(
        "SELECT year FROM vehicles WHERE slug = ?", (slug,),
    ).fetchone()
    db.close()
    assert row[0] is None


def test_edit_invalid_year_re_renders_with_error(app_client):
    slug = _seed_vehicle(app_client)
    r = app_client.post(
        f"/setup/vehicles/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "EditVehEntity",
            "year": "twenty-five",  # invalid
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "4-digit number" in r.text


def test_edit_rejects_cross_entity_loan_link(app_client):
    """Same entity-mismatch validation as Add."""
    _seed_entity(app_client, "EditVehEntity")
    _seed_entity(app_client, "OtherEntity")
    _seed_loan(
        app_client, slug="OtherAutoLoan", entity="OtherEntity",
    )
    slug = _seed_vehicle(app_client, entity="EditVehEntity")

    r = app_client.post(
        f"/setup/vehicles/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "EditVehEntity",
            "linked_loan_slug": "OtherAutoLoan",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "Mixed-entity links" in r.text


def test_edit_missing_required_field_re_renders_with_error(app_client):
    slug = _seed_vehicle(app_client)
    r = app_client.post(
        f"/setup/vehicles/{slug}/edit",
        data={
            "display_name": "",  # blank — required
            "entity_slug": "EditVehEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert f"Edit vehicle: {slug}" in r.text
    assert "Required." in r.text


def test_edit_unknown_slug_returns_404(app_client):
    r = app_client.post(
        "/setup/vehicles/DoesNotExist/edit",
        data={"display_name": "x", "entity_slug": "x"},
        follow_redirects=False,
    )
    assert r.status_code == 404
