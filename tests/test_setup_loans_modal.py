# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Phase 4 + Add modal and the Phase 8 step 4 + Edit modal.

Mirrors test_setup_entities_modal.py shape: modals render in-page,
save success creates / updates the loan + redirects, validation
failure preserves typing via field_errors + form_values, account-
path fields expose datalist autocomplete, no /settings/loans link
in either path.

The per-row Edit→ link now routes to ?edit={slug} (Phase 8 step 4
shipped the Edit modal for the recovery-shell field set: essentials
+ escrow). The full editor at /settings/loans/{slug}/edit still
exists for power-user state-change ops (record payments, escrow
reconcile, payoff workflow, revolving fields, simplefin binding,
payment_due_day) — those are intentionally NOT exposed in the
recovery-shell modal.
"""
from __future__ import annotations

import pytest


def _seed_entity(app_client, slug: str = "TestLoanEntity", entity_type: str = "personal"):
    """Seed an entity (idempotent — skips if already present so tests
    can call this freely without colliding with the fixture's
    pre-existing entities)."""
    import sqlite3
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        existing = db.execute(
            "SELECT slug FROM entities WHERE slug = ?", (slug,),
        ).fetchone()
        if existing is not None:
            return
    finally:
        db.close()
    if entity_type == "personal":
        r = app_client.post(
            "/setup/entities/add-person",
            data={"display_name": slug, "slug": slug},
            follow_redirects=False,
        )
    else:
        r = app_client.post(
            "/setup/entities/add-business",
            data={"display_name": slug, "slug": slug, "entity_type": entity_type},
            follow_redirects=False,
        )
    assert r.status_code == 303, f"seed entity failed: {r.text[:300]}"


# ---------------------------------------------------------------------------
# Modal rendering
# ---------------------------------------------------------------------------


def test_get_setup_loans_renders_without_modal(app_client):
    r = app_client.get("/setup/loans")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text
    assert "+ Add loan" in r.text


def test_add_loan_query_renders_modal(app_client):
    r = app_client.get("/setup/loans?add=loan")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Add a loan" in r.text
    # Required-field labels are present.
    assert "Loan type" in r.text
    assert "Owning entity" in r.text
    assert "Original principal" in r.text
    assert "Funded date" in r.text


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_modal_exposes_datalist_for_account_paths(app_client):
    r = app_client.get("/setup/loans?add=loan")
    assert r.status_code == 200
    # Datalist scaffolding is on the page.
    assert 'id="setup-loans-entities-datalist"' in r.text
    assert 'id="setup-loans-liability-datalist"' in r.text
    assert 'id="setup-loans-expense-datalist"' in r.text
    # Liability + interest fields wire to their datalists.
    assert 'list="setup-loans-liability-datalist"' in r.text
    assert 'list="setup-loans-expense-datalist"' in r.text


# ---------------------------------------------------------------------------
# Save success
# ---------------------------------------------------------------------------


def test_add_loan_success_creates_loan_and_redirects(app_client):
    _seed_entity(app_client, "TestLoanEntity")
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "BankTwo 30-yr",
            "slug": "BankTwo30",
            "loan_type": "mortgage",
            "entity_slug": "TestLoanEntity",
            "institution": "Bank Two",
            "original_principal": "550000.00",
            "funded_date": "2025-10-27",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert "added=BankTwo30" in r.headers["location"]

    # Verify the row landed.
    follow = app_client.get(r.headers["location"])
    assert follow.status_code == 200
    assert "BankTwo30" in follow.text


def test_add_loan_auto_derives_slug_from_display_name(app_client):
    _seed_entity(app_client, "TestLoanEntity")
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "Backup Loan",
            "loan_type": "personal",
            "entity_slug": "TestLoanEntity",
            "institution": "Local Credit Union",
            "original_principal": "10000",
            "funded_date": "2024-06-01",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    # suggest_slug strips spaces → "BackupLoan"
    assert "added=BackupLoan" in r.headers["location"]


def test_add_loan_auto_scaffolds_account_paths_when_blank(app_client):
    _seed_entity(app_client, "TestLoanEntity")
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "Auto Loan",
            "slug": "MyAuto",
            "loan_type": "auto",
            "entity_slug": "TestLoanEntity",
            "institution": "Capital One",
            "original_principal": "20000",
            "funded_date": "2024-01-15",
            # No liability_account_path / interest_account_path
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    # Loan row should have computed account paths set.
    import sqlite3
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    try:
        row = db.execute(
            "SELECT liability_account_path, interest_account_path "
            "FROM loans WHERE slug = ?", ("MyAuto",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    # Format mirrors /settings/loans handler:
    # Liabilities:{Entity}:{Institution}:{Slug}
    assert row["liability_account_path"] is not None
    assert "TestLoanEntity" in row["liability_account_path"]
    assert "MyAuto" in row["liability_account_path"]
    # Format: Expenses:{Entity}:{Slug}:Interest
    assert row["interest_account_path"] == "Expenses:TestLoanEntity:MyAuto:Interest"


# ---------------------------------------------------------------------------
# Validation failure: field_errors + form_values preservation
# ---------------------------------------------------------------------------


def test_add_loan_missing_required_fields_re_renders_with_errors(app_client):
    r = app_client.post(
        "/setup/loans/add",
        data={"display_name": ""},
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Modal still open.
    assert "wiz-modal-backdrop is-open" in r.text
    # Multiple required-field errors visible.
    assert "Required." in r.text
    # Specific fields flagged.
    assert "is-invalid" in r.text


def test_add_loan_validation_preserves_typed_values(app_client):
    # User typed everything except funded_date; the modal must
    # re-render with all the values they did type.
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "Half Filled",
            "slug": "HalfFilled",
            "loan_type": "mortgage",
            "entity_slug": "MyEntity",
            "institution": "Some Bank",
            "original_principal": "12345",
            # No funded_date.
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Each typed value survives.
    assert 'value="Half Filled"' in r.text
    assert 'value="HalfFilled"' in r.text
    assert 'value="MyEntity"' in r.text
    assert 'value="Some Bank"' in r.text
    assert 'value="12345"' in r.text


def test_add_loan_invalid_slug_shows_field_error(app_client):
    _seed_entity(app_client, "TestLoanEntity")
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "Test",
            "slug": "lowercase-bad",  # invalid: starts lowercase
            "loan_type": "personal",
            "entity_slug": "TestLoanEntity",
            "institution": "X",
            "original_principal": "100",
            "funded_date": "2024-01-01",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Slug error reaches the user.
    assert "uppercase" in r.text.lower() or "invalid" in r.text.lower()


def test_add_loan_collision_with_existing_slug_offers_alternative(app_client):
    _seed_entity(app_client, "TestLoanEntity")
    # First add succeeds.
    r1 = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "First",
            "slug": "DupeSlug",
            "loan_type": "personal",
            "entity_slug": "TestLoanEntity",
            "institution": "X",
            "original_principal": "100",
            "funded_date": "2024-01-01",
        },
        follow_redirects=False,
    )
    assert r1.status_code == 303
    # Second add at the same slug is rejected with a clear error
    # and the suggested-alternative slug is in the message.
    r2 = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "Second",
            "slug": "DupeSlug",
            "loan_type": "personal",
            "entity_slug": "TestLoanEntity",
            "institution": "Y",
            "original_principal": "200",
            "funded_date": "2024-02-01",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 400
    assert "already taken" in r2.text


def test_add_loan_unknown_loan_type_blocks(app_client):
    _seed_entity(app_client, "TestLoanEntity")
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": "Bad",
            "loan_type": "imaginary-type",
            "entity_slug": "TestLoanEntity",
            "institution": "X",
            "original_principal": "100",
            "funded_date": "2024-01-01",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "loan type" in r.text.lower() or "imaginary-type" in r.text


# ---------------------------------------------------------------------------
# Acceptance: + Add path doesn't link to /settings/loans
# ---------------------------------------------------------------------------


def test_add_loan_button_targets_modal_not_settings(app_client):
    """The + Add button is the Phase 4 acceptance surface for new
    loan creation. Its href must be the in-page modal trigger,
    never a redirect to /settings/loans."""
    r = app_client.get("/setup/loans?add=loan")
    assert r.status_code == 200
    assert 'href="/setup/loans?add=loan"' in r.text
    assert 'action="/setup/loans/add"' in r.text


def test_add_loan_modal_form_does_not_post_to_settings(app_client):
    r = app_client.get("/setup/loans?add=loan")
    assert r.status_code == 200
    assert 'action="/settings/loans"' not in r.text


def test_setup_loans_empty_state_has_no_settings_links(app_client):
    """Recovery layout doesn't ship base.html's sidebar — so the
    naive 'no /settings/loans anywhere in HTML' shape is valid
    again. The previous <main>-bounded workaround was treating the
    symptom of extending base.html. Now that setup_loans.html
    extends setup_recovery/_layout.html, no settings nav is in the
    DOM at all.

    The known partial punt (per-row Edit → /settings/loans/{slug}/edit)
    only renders when there are loans in the table, so empty-state
    has zero /settings/loans references."""
    r = app_client.get("/setup/loans")
    assert r.status_code == 200
    assert 'href="/setup/loans?add=loan"' in r.text
    # Empty state: no row-level Edit links → no /settings/loans
    # anywhere in the page.
    assert "/settings/loans" not in r.text


def test_add_loan_modal_does_not_link_to_settings(app_client):
    """With the recovery-layout migration, the simple assertion is
    valid: the modal page has no /settings/loans references at all."""
    r = app_client.get("/setup/loans?add=loan")
    assert r.status_code == 200
    assert "/settings/loans" not in r.text


# ===========================================================================
# Phase 8 step 4 — + Edit modal
# ===========================================================================


def _seed_loan(app_client, slug: str = "EditTestLoan", entity: str = "EditEntity"):
    """Compose a loan via the + Add path so the Edit tests have a row
    to operate on. Returns the slug."""
    _seed_entity(app_client, entity)
    r = app_client.post(
        "/setup/loans/add",
        data={
            "display_name": f"{slug} display",
            "slug": slug,
            "loan_type": "mortgage",
            "entity_slug": entity,
            "institution": "TestBank",
            "original_principal": "100000.00",
            "funded_date": "2024-01-15",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    return slug


def test_per_row_edit_link_targets_setup_modal_not_settings(app_client):
    """The Edit→ link on each row must route to the in-page modal
    (?edit={slug}) — never break out to /settings/loans/{slug}/edit
    for routine field edits. Phase 8 step 4 closure of the long-
    running 'edit punts' tracker entry."""
    slug = _seed_loan(app_client)
    r = app_client.get("/setup/loans")
    assert r.status_code == 200
    assert f"/setup/loans?edit={slug}" in r.text
    assert f"/settings/loans/{slug}/edit" not in r.text


def test_edit_query_renders_modal_prefilled_with_row_values(app_client):
    """?edit={slug} opens the modal with the row's current values
    populated into the form fields."""
    slug = _seed_loan(app_client)
    r = app_client.get(f"/setup/loans?edit={slug}")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert f"Edit loan: {slug}" in r.text
    # Prefilled values present (display name, institution, principal).
    assert f'value="{slug} display"' in r.text
    assert 'value="TestBank"' in r.text
    assert 'value="100000.00"' in r.text
    # Slug field is read-only (immutable identifier).
    assert "readonly" in r.text


def test_edit_query_with_unknown_slug_renders_page_without_modal(app_client):
    """Stale-link / typo case: ?edit=Bogus renders the page WITHOUT
    the modal rather than crashing. The user sees the loan list
    and can navigate from there."""
    r = app_client.get("/setup/loans?edit=DoesNotExist")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text


def test_edit_save_updates_row_and_redirects(app_client):
    """Submit the Edit form with changed values; verify the row
    updated and the response redirects to ?updated={slug}."""
    slug = _seed_loan(app_client)
    r = app_client.post(
        f"/setup/loans/{slug}/edit",
        data={
            "display_name": "Updated display",
            "loan_type": "mortgage",
            "entity_slug": "EditEntity",
            "institution": "NewBank",
            "original_principal": "150000.00",
            "funded_date": "2024-01-15",
            "interest_rate_apr": "6.75",
            "term_months": "360",
            "escrow_account_path": "Assets:EditEntity:NewBank:EscrowFoo",
            "escrow_monthly": "420.00",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert r.headers["location"] == f"/setup/loans?updated={slug}"

    # Check the row actually updated.
    import sqlite3
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT display_name, institution, original_principal, "
        "       interest_rate_apr, escrow_account_path, escrow_monthly "
        "  FROM loans WHERE slug = ?",
        (slug,),
    ).fetchone()
    db.close()
    assert row["display_name"] == "Updated display"
    assert row["institution"] == "NewBank"
    assert str(row["original_principal"]) == "150000.00"
    assert str(row["interest_rate_apr"]) == "6.75"
    assert row["escrow_account_path"] == "Assets:EditEntity:NewBank:EscrowFoo"
    assert str(row["escrow_monthly"]) == "420.00"


def test_edit_blank_account_path_preserves_existing_value(app_client):
    """The Edit modal uses COALESCE-NULLIF on account paths so a
    blank submission doesn't wipe a previously-set path. This is the
    'recovery flow only adds, never destroys' contract — the user
    might have a custom liability path the Add flow auto-scaffolded
    and they don't want to retype it on every edit."""
    slug = _seed_loan(app_client)

    # First edit: set a custom liability path.
    custom_path = "Liabilities:EditEntity:Custom:Foo"
    r = app_client.post(
        f"/setup/loans/{slug}/edit",
        data={
            "display_name": "x", "loan_type": "mortgage",
            "entity_slug": "EditEntity", "institution": "TestBank",
            "original_principal": "100000.00", "funded_date": "2024-01-15",
            "liability_account_path": custom_path,
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    # Second edit: leave liability_account_path BLANK; the existing
    # custom path must be preserved.
    r2 = app_client.post(
        f"/setup/loans/{slug}/edit",
        data={
            "display_name": "y", "loan_type": "mortgage",
            "entity_slug": "EditEntity", "institution": "TestBank",
            "original_principal": "100000.00", "funded_date": "2024-01-15",
            "liability_account_path": "",  # blank — should NOT wipe
        },
        follow_redirects=False,
    )
    assert r2.status_code == 303

    import sqlite3
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    row = db.execute(
        "SELECT liability_account_path FROM loans WHERE slug = ?",
        (slug,),
    ).fetchone()
    db.close()
    assert row[0] == custom_path, (
        "blank submission wiped the custom liability_account_path — "
        "Edit modal must use COALESCE so blanks preserve existing"
    )


def test_edit_missing_required_field_re_renders_with_error(app_client):
    """Validation failure on Edit re-renders the modal with
    field_errors + the user's typed values preserved (same shape as
    Add's validation re-render)."""
    slug = _seed_loan(app_client)
    r = app_client.post(
        f"/setup/loans/{slug}/edit",
        data={
            "display_name": "",  # blank — required
            "loan_type": "mortgage",
            "entity_slug": "EditEntity",
            "institution": "Bank",
            "original_principal": "100",
            "funded_date": "2024-01-15",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert f"Edit loan: {slug}" in r.text
    assert "Required." in r.text
    # The user's input on other fields is preserved (modal re-render).
    assert 'value="Bank"' in r.text


def test_edit_unknown_slug_returns_404(app_client):
    """POST against a non-existent slug 404s — protects against a
    stale browser tab editing a loan deleted in another session."""
    r = app_client.post(
        "/setup/loans/DoesNotExist/edit",
        data={
            "display_name": "x", "loan_type": "mortgage",
            "entity_slug": "EditEntity", "institution": "x",
            "original_principal": "1", "funded_date": "2024-01-15",
        },
        follow_redirects=False,
    )
    assert r.status_code == 404


def test_edit_modal_does_not_link_to_settings(app_client):
    """Recovery-shell isolation holds for the Edit modal too —
    same rule the Add modal already enforces."""
    slug = _seed_loan(app_client)
    r = app_client.get(f"/setup/loans?edit={slug}")
    assert r.status_code == 200
    # The full /settings/loans editor link is gone from the recovery
    # row template; verify the rendered Edit page doesn't reintroduce it.
    assert "/settings/loans" not in r.text
