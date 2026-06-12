# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Phase 4 properties + Add modal pattern.

Pair-twin of test_setup_vehicles_modal.py with property-specific
divergences:
  - address fields (free-text, no parse)
  - property_type required, with explicit type list
  - is_primary_residence + is_rental flags
  - linked-loan dropdown filtered to mortgage/heloc loans
  - "No mortgage — owned outright" explicit no-loan label
  - NO auto-scaffold of per-property accounts (locked constraint)
  - single-loan field for Phase 4 (multi-loan deferred)
"""
from __future__ import annotations

import sqlite3

import pytest


def _seed_entity(app_client, slug: str = "TestPropertyEntity"):
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


def _seed_mortgage(app_client, *, slug: str, entity: str):
    """Seed a mortgage loan via the live route — only mortgage/heloc
    loans appear in the property modal's dropdown."""
    _seed_entity(app_client, entity)
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": f"Mortgage {slug}",
            "slug": slug,
            "loan_type": "mortgage",
            "entity_slug": entity,
            "institution": "Test Bank",
            "original_principal": "450000",
            "funded_date": "2024-06-01",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, f"seed mortgage failed: {r.text[:300]}"


def _seed_auto_loan(app_client, *, slug: str, entity: str):
    """Seed a non-mortgage loan to verify the dropdown filters it out."""
    _seed_entity(app_client, entity)
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": f"Auto {slug}",
            "slug": slug,
            "loan_type": "auto",
            "entity_slug": entity,
            "institution": "Test Credit Union",
            "original_principal": "20000",
            "funded_date": "2024-01-15",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, f"seed auto loan failed: {r.text[:300]}"


# ---------------------------------------------------------------------------
# Modal rendering
# ---------------------------------------------------------------------------


def test_get_setup_properties_renders_without_modal(app_client):
    r = app_client.get("/setup/properties")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text
    assert "+ Add property" in r.text


def test_add_property_query_renders_modal(app_client):
    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Add a property" in r.text
    # Property-specific fields visible.
    assert "Property type" in r.text
    assert "Street address" in r.text


def test_modal_loan_section_uses_owned_outright_phrasing(app_client):
    """Locked constraint: property no-loan affordance is mortgage-
    flavored. Phase 8 multi-loan replaced the explicit "No mortgage
    — owned outright" select option with a checkbox list + help
    text saying "Leave all unchecked for 'owned outright.'" — same
    user-facing concept, different mechanism. Test asserts the
    "owned outright" phrasing remains so a future refactor that
    drops it (and silently leaves users wondering how to mark
    no-mortgage) gets caught."""
    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    assert "owned outright" in r.text


def test_modal_loan_dropdown_lists_only_mortgage_kind(app_client):
    """Locked constraint: mortgage/HELOC loans in the property
    dropdown, not auto/student/personal."""
    _seed_mortgage(app_client, slug="PropMortgage1", entity="TestPropertyEntity")
    _seed_auto_loan(app_client, slug="PropAuto1", entity="TestPropertyEntity")

    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    # Mortgage shows up.
    assert "PropMortgage1" in r.text
    # Auto loan is filtered out.
    assert "PropAuto1" not in r.text


def test_modal_exposes_entity_datalist(app_client):
    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    assert 'id="setup-properties-entities-datalist"' in r.text
    assert 'list="setup-properties-entities-datalist"' in r.text


def test_modal_includes_primary_residence_and_rental_checkboxes(app_client):
    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    assert 'name="is_primary_residence"' in r.text
    assert 'name="is_rental"' in r.text


# ---------------------------------------------------------------------------
# Save success
# ---------------------------------------------------------------------------


def test_add_property_success_creates_row_and_redirects(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Main Residence",
            "slug": "MainResidence",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            "address": "123 Main St",
            "city": "Anytown",
            "state": "ST",
            "postal_code": "00000",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert "added=MainResidence" in r.headers["location"]


def test_add_property_auto_derives_slug(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Pinewood House",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "added=PinewoodHouse" in r.headers["location"]


def test_add_property_does_not_auto_scaffold_chart_accounts(app_client):
    """Locked Phase 4 constraint: property creation does NOT open the
    Assets:{Entity}:Property:{slug} or Expenses subtree accounts."""
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Test House",
            "slug": "TestHouse",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        # No Assets:TestPropertyEntity:Property:TestHouse account opened.
        row = db.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE account_path = ?",
            ("Assets:TestPropertyEntity:Property:TestHouse",),
        ).fetchone()
        assert row is None, (
            "property add should NOT have auto-scaffolded the asset account"
        )
        # No Expenses subtree.
        chart_rows = db.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE account_path LIKE 'Expenses:%TestHouse:%'"
        ).fetchall()
        assert chart_rows == [], (
            "property add should NOT have auto-scaffolded any chart accounts"
        )
    finally:
        db.close()


def test_add_property_with_linked_mortgage(app_client):
    _seed_mortgage(
        app_client, slug="LinkedMortgage", entity="TestPropertyEntity",
    )
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Mortgaged House",
            "slug": "MortgagedHouse",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            "linked_loan_slug": "LinkedMortgage",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    # Verify the loan row's property_slug got back-referenced.
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        row = db.execute(
            "SELECT property_slug FROM loans WHERE slug = ?",
            ("LinkedMortgage",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[0] == "MortgagedHouse"


def test_add_property_with_explicit_owned_outright(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Cash Buy",
            "slug": "CashBuy",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            "linked_loan_slug": "",  # explicit "no mortgage"
        },
        follow_redirects=False,
    )
    assert r.status_code == 303


def test_add_property_with_rental_flag(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Rental Unit",
            "slug": "RentalUnit",
            "entity_slug": "TestPropertyEntity",
            "property_type": "rental",
            "is_rental": "1",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        row = db.execute(
            "SELECT is_rental FROM properties WHERE slug = ?",
            ("RentalUnit",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[0] == 1


# ---------------------------------------------------------------------------
# Validation failure
# ---------------------------------------------------------------------------


def test_add_property_missing_required_re_renders_modal(app_client):
    r = app_client.post(
        "/setup/properties/add",
        data={"display_name": ""},
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Required." in r.text


def test_add_property_validation_preserves_typed_values(app_client):
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Half Filled",
            "slug": "HalfFilled",
            "address": "456 Elm",
            "city": "Somewhere",
            # No entity_slug or property_type.
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert 'value="Half Filled"' in r.text
    assert 'value="HalfFilled"' in r.text
    assert 'value="456 Elm"' in r.text
    assert 'value="Somewhere"' in r.text


def test_add_property_unknown_type_blocks(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Bad Type",
            "slug": "BadType",
            "entity_slug": "TestPropertyEntity",
            "property_type": "spaceship",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "type" in r.text.lower()


def test_add_property_unknown_loan_slug_blocks(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Bad Loan",
            "slug": "BadLoan",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            "linked_loan_slug": "TotallyImaginaryMortgage",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "No active mortgage" in r.text


def test_add_property_auto_loan_slug_not_accepted(app_client):
    """The dropdown filters to mortgage/heloc only; if the user crafts
    a POST referencing an auto loan, the validator must refuse — same
    filter as the dropdown."""
    _seed_auto_loan(
        app_client, slug="HiddenAuto", entity="TestPropertyEntity",
    )
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Sneaky",
            "slug": "Sneaky",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            "linked_loan_slug": "HiddenAuto",  # auto loan, not mortgage
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "mortgage" in r.text.lower()


def test_add_property_collision_offers_disambiguated_slug(app_client):
    _seed_entity(app_client, "TestPropertyEntity")
    r1 = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Dupe",
            "slug": "DupeProperty",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
        },
        follow_redirects=False,
    )
    assert r1.status_code == 303
    r2 = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Dupe Again",
            "slug": "DupeProperty",
            "entity_slug": "TestPropertyEntity",
            "property_type": "condo",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 400
    assert "already taken" in r2.text


# ---------------------------------------------------------------------------
# Phase 4-checkpoint audit: entity-match validation
# ---------------------------------------------------------------------------


def test_add_property_rejects_loan_with_mismatched_entity(app_client):
    """Locked constraint added at the Phase 4 checkpoint: if the
    user picks a loan whose entity_slug differs from the property's,
    the save is a 400 (not silent schema garbage). Mixed-entity
    links produce broken account hierarchies on reconstruct."""
    _seed_entity(app_client, "PropEntityA")
    _seed_entity(app_client, "PropEntityB")
    # Mortgage owned by PropEntityA.
    _seed_mortgage(app_client, slug="PropMtgA", entity="PropEntityA")
    # Try to attach it to a property owned by PropEntityB.
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "Mismatch",
            "slug": "MismatchProp",
            "entity_slug": "PropEntityB",  # different entity
            "property_type": "house",
            "linked_loan_slug": "PropMtgA",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "PropEntityA" in r.text  # message names the loan's entity
    assert "PropEntityB" in r.text  # and the property's entity
    assert "Mixed-entity" in r.text or "matching entity" in r.text


# ---------------------------------------------------------------------------
# Acceptance: no /settings/* punts in the modal flow
# ---------------------------------------------------------------------------


def test_add_property_modal_does_not_punt_to_settings(app_client):
    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    # Modal form posts to recovery handler, not /settings/properties.
    assert 'action="/setup/properties/add"' in r.text
    # The OLD + Add button used to send the user to /settings/properties.
    # Phase 4 retired that.
    assert 'href="/settings/properties"' not in r.text


def test_add_property_button_targets_modal(app_client):
    r = app_client.get("/setup/properties")
    assert r.status_code == 200
    assert 'href="/setup/properties?add=property"' in r.text


# ===========================================================================
# Phase 8 step 5 — + Edit modal
# ===========================================================================


def _seed_property(
    app_client,
    *,
    slug: str = "EditTestProp",
    entity: str = "EditPropEntity",
    extras: dict | None = None,
):
    """Seed a property via the + Add path so the Edit tests have a row.
    Returns the slug actually used (Add normalizes slug)."""
    _seed_entity(app_client, entity)
    data = {
        "display_name": f"{slug} display",
        "slug": slug,
        "property_type": "house",
        "entity_slug": entity,
        "address": "123 Main St",
    }
    if extras:
        data.update(extras)
    r = app_client.post(
        "/setup/properties/add", data=data, follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    return slug


def test_per_row_edit_link_targets_setup_modal_not_settings(app_client):
    """Phase 8 step 5: per-row Edit / Set-owning-entity links route
    to ?edit={slug} — the recovery flow no longer breaks out to
    /settings/properties for routine field edits."""
    slug = _seed_property(app_client)
    r = app_client.get("/setup/properties")
    assert r.status_code == 200
    assert f"/setup/properties?edit={slug}" in r.text
    # The bare /settings/properties row link from before Phase 8
    # is gone for the user-clickable surface (still appears in
    # /change-ownership / /dispose deep-link comments inside the
    # Edit modal's entity-locked-help text — those are intentional
    # power-user pointers, not breakouts).
    assert 'href="/settings/properties/' + slug + '"' not in r.text


def test_edit_query_renders_modal_prefilled_with_row_values(app_client):
    """?edit={slug} opens the modal with the row's current values
    populated into the form fields."""
    slug = _seed_property(
        app_client,
        extras={
            "address": "456 Oak Ave", "city": "Anytown", "state": "ST",
            "postal_code": "00000", "is_rental": "1",
        },
    )
    r = app_client.get(f"/setup/properties?edit={slug}")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert f"Edit property: {slug}" in r.text
    # Prefilled values present.
    assert f'value="{slug} display"' in r.text
    assert 'value="456 Oak Ave"' in r.text
    assert 'value="Anytown"' in r.text
    # is_rental checkbox is checked (form_value coerces bool→"1").
    assert 'name="is_rental" value="1"\n             checked' in r.text or \
           'name="is_rental" value="1" checked' in r.text
    # Slug field is read-only.
    assert "readonly" in r.text


def test_edit_query_with_unknown_slug_renders_page_without_modal(app_client):
    r = app_client.get("/setup/properties?edit=DoesNotExist")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text


def test_edit_save_updates_row_and_redirects(app_client):
    slug = _seed_property(app_client)
    r = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "New display",
            "entity_slug": "EditPropEntity",
            "property_type": "rental",
            "address": "789 New Rd",
            "city": "Newcity",
            "state": "NY",
            "postal_code": "10001",
            "is_rental": "1",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert r.headers["location"] == f"/setup/properties?updated={slug}"

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT display_name, property_type, address, city, state, "
        "       is_rental "
        "  FROM properties WHERE slug = ?",
        (slug,),
    ).fetchone()
    db.close()
    assert row["display_name"] == "New display"
    assert row["property_type"] == "rental"
    assert row["address"] == "789 New Rd"
    assert row["city"] == "Newcity"
    assert row["state"] == "NY"
    assert row["is_rental"] == 1


def test_edit_preserves_entity_slug_when_currently_set(app_client):
    """Entity-lock contract: once a property has entity_slug set, the
    Edit modal's entity_slug submission is silently ignored.
    Mirrors test_property_save_cannot_change_entity_slug for the
    /settings/properties save handler — entity transitions go through
    /settings/properties/{slug}/change-ownership, not field edits."""
    slug = _seed_property(app_client, entity="EditPropEntity")
    _seed_entity(app_client, "DifferentEntity")

    r = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "DifferentEntity",  # attempt to change
            "property_type": "house",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    row = db.execute(
        "SELECT entity_slug FROM properties WHERE slug = ?",
        (slug,),
    ).fetchone()
    db.close()
    assert row[0] == "EditPropEntity", (
        "Edit modal allowed entity_slug change — entity-lock contract "
        "broken; transitions must go through change-ownership flow"
    )


def test_edit_blank_address_clears_field(app_client):
    """Address fields are user-entered free-text; blank submission
    clears them (unlike account paths which use COALESCE-NULLIF
    to preserve existing values). Verifies the Edit modal lets the
    user remove an address they typed by mistake."""
    slug = _seed_property(
        app_client, extras={"address": "Original 123 St"},
    )
    r = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "EditPropEntity",
            "property_type": "house",
            "address": "",  # blank — should clear
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    row = db.execute(
        "SELECT address FROM properties WHERE slug = ?", (slug,),
    ).fetchone()
    db.close()
    assert row[0] is None or row[0] == ""


def test_edit_links_loan_via_property_slug_back_reference(app_client):
    """linked_loan_slug isn't a properties column — it's stored as
    loans.property_slug (FK back-reference). Edit handler must:
      1. Set property_slug on the new selection
      2. Clear property_slug on the previously-linked loan if changed
    """
    _seed_entity(app_client, "PropLinkEntity")
    _seed_mortgage(app_client, slug="MortgageA", entity="PropLinkEntity")
    _seed_mortgage(app_client, slug="MortgageB", entity="PropLinkEntity")

    slug = _seed_property(app_client, entity="PropLinkEntity")

    # First edit: link MortgageA.
    r1 = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "PropLinkEntity",
            "property_type": "house",
            "linked_loan_slug": "MortgageA",
        },
        follow_redirects=False,
    )
    assert r1.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    row_a = db.execute(
        "SELECT property_slug FROM loans WHERE slug = ?", ("MortgageA",),
    ).fetchone()
    assert row_a["property_slug"] == slug

    # Second edit: switch to MortgageB. MortgageA must be unlinked.
    r2 = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "PropLinkEntity",
            "property_type": "house",
            "linked_loan_slug": "MortgageB",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 303
    row_a = db.execute(
        "SELECT property_slug FROM loans WHERE slug = ?", ("MortgageA",),
    ).fetchone()
    row_b = db.execute(
        "SELECT property_slug FROM loans WHERE slug = ?", ("MortgageB",),
    ).fetchone()
    db.close()
    assert row_a["property_slug"] is None, (
        "previously-linked loan wasn't unlinked when the user switched "
        "to a different mortgage"
    )
    assert row_b["property_slug"] == slug


def test_edit_rejects_cross_entity_loan_link(app_client):
    """Same entity-mismatch validation as Add: a Personal property
    can't link a BetaCorp mortgage."""
    _seed_entity(app_client, "PropLinkEntity")
    _seed_entity(app_client, "OtherEntity")
    _seed_mortgage(
        app_client, slug="OtherMortgage", entity="OtherEntity",
    )
    slug = _seed_property(app_client, entity="PropLinkEntity")

    r = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "x",
            "entity_slug": "PropLinkEntity",
            "property_type": "house",
            "linked_loan_slug": "OtherMortgage",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "Mixed-entity links" in r.text


def test_edit_missing_required_field_re_renders_with_error(app_client):
    slug = _seed_property(app_client)
    r = app_client.post(
        f"/setup/properties/{slug}/edit",
        data={
            "display_name": "",  # blank — required
            "entity_slug": "EditPropEntity",
            "property_type": "house",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert f"Edit property: {slug}" in r.text
    assert "Required." in r.text


def test_edit_unknown_slug_returns_404(app_client):
    r = app_client.post(
        "/setup/properties/DoesNotExist/edit",
        data={
            "display_name": "x", "entity_slug": "x",
            "property_type": "house",
        },
        follow_redirects=False,
    )
    assert r.status_code == 404


# ===========================================================================
# Phase 8 — multi-loan support (mortgage + HELOC on one property)
# ===========================================================================


def test_modal_renders_loan_checkboxes_not_select(app_client):
    """Phase 8: linked-loan UI is a checkbox list, not a single-select.
    Each candidate loan renders its own checkbox so multi-loan
    properties (mortgage + HELOC) can both be ticked."""
    _seed_mortgage(app_client, slug="MultiLoanA", entity="TestPropertyEntity")
    _seed_mortgage(app_client, slug="MultiLoanB", entity="TestPropertyEntity")
    r = app_client.get("/setup/properties?add=property")
    assert r.status_code == 200
    # Each loan gets its own checkbox sharing name="linked_loan_slug"
    assert 'name="linked_loan_slug"\n                   value="MultiLoanA"' in r.text or \
           'name="linked_loan_slug" value="MultiLoanA"' in r.text
    assert 'type="checkbox"' in r.text
    # The old single-select shouldn't be there.
    assert '<select class="wiz-input' not in r.text or \
           'name="linked_loan_slug"' not in [
               line for line in r.text.split("\n")
               if "<select" in line and "linked_loan_slug" in line
           ]


def test_add_property_with_multiple_linked_loans(app_client):
    """A property created with two ticked loans should have BOTH
    loans' property_slug back-references set to its slug."""
    _seed_mortgage(app_client, slug="DualLoanMortgage", entity="TestPropertyEntity")
    _seed_mortgage(app_client, slug="DualLoanHeloc", entity="TestPropertyEntity")

    # Multiple linked_loan_slug values: httpx form-encodes a list-
    # valued dict entry as repeated key=value pairs, which FastAPI's
    # form.getlist collects.
    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "DualLoanHouse display",
            "slug": "DualLoanHouse",
            "property_type": "house",
            "entity_slug": "TestPropertyEntity",
            "linked_loan_slug": ["DualLoanMortgage", "DualLoanHeloc"],
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT slug, property_slug FROM loans "
        " WHERE slug IN ('DualLoanMortgage', 'DualLoanHeloc') "
        " ORDER BY slug",
    ).fetchall()
    db.close()
    assert len(rows) == 2
    for row in rows:
        assert row["property_slug"] == "DualLoanHouse", (
            f"loan {row['slug']!r} property_slug back-reference not "
            f"set to DualLoanHouse: got {row['property_slug']!r}"
        )


def test_edit_property_renders_all_linked_loans_checked(app_client):
    """When ?edit={slug} renders the modal, every loan currently
    back-referencing this property must render with its checkbox
    pre-checked. Reads the loans.property_slug back-reference set,
    not just the first match."""
    _seed_mortgage(app_client, slug="EditDualA", entity="TestPropertyEntity")
    _seed_mortgage(app_client, slug="EditDualB", entity="TestPropertyEntity")

    # Create the property linking BOTH loans.
    app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "EditDualHouse",
            "slug": "EditDualHouse",
            "property_type": "house",
            "entity_slug": "TestPropertyEntity",
            "linked_loan_slug": ["EditDualA", "EditDualB"],
        },
        follow_redirects=False,
    )

    r = app_client.get("/setup/properties?edit=EditDualHouse")
    assert r.status_code == 200
    # Both loans render with checked attribute.
    text = r.text
    a_idx = text.find('value="EditDualA"')
    b_idx = text.find('value="EditDualB"')
    assert a_idx > -1 and b_idx > -1
    # The "checked" attribute appears within the checkbox label
    # block for each loan — search a small window after each value.
    a_window = text[a_idx:a_idx + 200]
    b_window = text[b_idx:b_idx + 200]
    assert "checked" in a_window, (
        "EditDualA loan should render pre-checked on the Edit modal"
    )
    assert "checked" in b_window, (
        "EditDualB loan should render pre-checked on the Edit modal"
    )


def test_edit_property_unlinks_unchecked_loans_keeps_checked(app_client):
    """Edit reconciliation: a previously-linked loan that's unchecked
    on save must have its property_slug back-reference cleared, while
    still-checked loans stay linked. Other loans (never linked) stay
    untouched."""
    _seed_mortgage(app_client, slug="ReconA", entity="TestPropertyEntity")
    _seed_mortgage(app_client, slug="ReconB", entity="TestPropertyEntity")
    _seed_mortgage(app_client, slug="ReconUnrelated", entity="TestPropertyEntity")

    # Create the property linking A + B.
    app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "ReconHouse",
            "slug": "ReconHouse",
            "property_type": "house",
            "entity_slug": "TestPropertyEntity",
            "linked_loan_slug": ["ReconA", "ReconB"],
        },
        follow_redirects=False,
    )

    # Edit: keep A, drop B (B is unchecked). ReconUnrelated never
    # linked.
    r = app_client.post(
        "/setup/properties/ReconHouse/edit",
        data={
            "display_name": "ReconHouse",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            "linked_loan_slug": "ReconA",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    rows = {
        r["slug"]: r["property_slug"] for r in db.execute(
            "SELECT slug, property_slug FROM loans "
            " WHERE slug IN ('ReconA', 'ReconB', 'ReconUnrelated')",
        ).fetchall()
    }
    db.close()
    assert rows["ReconA"] == "ReconHouse", "still-checked loan should stay linked"
    assert rows["ReconB"] is None, "unchecked loan should be unlinked"
    assert rows["ReconUnrelated"] is None, (
        "unrelated loan that was never linked should stay untouched"
    )


def test_edit_property_with_zero_links_clears_all(app_client):
    """Submitting Edit with NO linked_loan_slug values clears every
    previously-linked loan — the user is saying 'this property is
    owned outright now.'"""
    _seed_mortgage(app_client, slug="ClearAllLoan", entity="TestPropertyEntity")

    app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "ClearAllHouse",
            "slug": "ClearAllHouse",
            "property_type": "house",
            "entity_slug": "TestPropertyEntity",
            "linked_loan_slug": "ClearAllLoan",
        },
        follow_redirects=False,
    )

    # Edit: zero linked_loan_slug values posted.
    r = app_client.post(
        "/setup/properties/ClearAllHouse/edit",
        data={
            "display_name": "ClearAllHouse",
            "entity_slug": "TestPropertyEntity",
            "property_type": "house",
            # no linked_loan_slug at all
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    row = db.execute(
        "SELECT property_slug FROM loans WHERE slug = 'ClearAllLoan'",
    ).fetchone()
    db.close()
    assert row[0] is None, "all linked loans should be cleared on zero-link save"


def test_add_property_rejects_one_cross_entity_link_in_multi_set(app_client):
    """If ANY of the multiple linked loans fails entity-match, the
    whole save is rejected with the standard mixed-entity message.
    No partial linking happens."""
    _seed_entity(app_client, "TestPropertyEntity")
    _seed_entity(app_client, "OtherEntity")
    _seed_mortgage(
        app_client, slug="GoodLoan", entity="TestPropertyEntity",
    )
    _seed_mortgage(
        app_client, slug="BadEntityLoan", entity="OtherEntity",
    )

    r = app_client.post(
        "/setup/properties/add",
        data={
            "display_name": "MixedEntityHouse",
            "slug": "MixedEntityHouse",
            "property_type": "house",
            "entity_slug": "TestPropertyEntity",
            "linked_loan_slug": ["GoodLoan", "BadEntityLoan"],
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "Mixed-entity links" in r.text

    # Verify NEITHER loan got linked — atomic-on-failure.
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    rows = db.execute(
        "SELECT slug, property_slug FROM loans "
        " WHERE slug IN ('GoodLoan', 'BadEntityLoan')",
    ).fetchall()
    db.close()
    for slug, link in rows:
        assert link is None, (
            f"loan {slug!r} got linked despite cross-entity rejection: "
            f"property_slug={link!r}"
        )
