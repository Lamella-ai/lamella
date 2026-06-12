# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Phase 4 accounts + Add modal pattern.

Accounts is the editor with the most concurrent complexity:
  - Modal coexists with label-needed inline forms (two surfaces,
    two jobs).
  - with_bean_snapshot() wraps the multi-write chain (the locked
    Phase 4 spec — only accounts gets the snapshot envelope).
  - Sibling-inference re-run on save so the new row renders with
    the ★ suggested badge applied.
  - Auto-heal-at-classify pipeline must run on both the inline
    label-needed form path AND the modal path — same shared
    helper, no divergence.
  - Companion scaffolding fires when kind + entity are both set.
"""
from __future__ import annotations

import sqlite3

import pytest


def _seed_entity(app_client, slug: str = "TestAcctEntity"):
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


# ---------------------------------------------------------------------------
# Modal rendering
# ---------------------------------------------------------------------------


def test_get_setup_accounts_renders_without_modal(app_client):
    r = app_client.get("/setup/accounts")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" not in r.text
    assert "+ Add account" in r.text


def test_add_account_query_renders_modal(app_client):
    r = app_client.get("/setup/accounts?add=account")
    assert r.status_code == 200
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Add an account" in r.text
    # Required-field labels are present.
    assert "Account path" in r.text
    assert "Kind" in r.text
    assert "Owning entity" in r.text


def test_modal_coexists_with_label_needed_table(app_client):
    """Locked spec: + Add modal coexists with the label-needed
    inline forms — two surfaces, two jobs. Don't unify them."""
    r = app_client.get("/setup/accounts")
    assert r.status_code == 200
    # Both the + Add affordance AND the inline-form table render.
    # (Empty fixture = no rows, but the table scaffolding/empty-state
    # is part of the page.)
    assert "+ Add account" in r.text
    # The shared entity datalist is on the page (used by both modal
    # and inline rows).
    assert 'id="setup-accounts-entities-datalist"' in r.text


def test_modal_kind_dropdown_lists_all_account_kinds(app_client):
    r = app_client.get("/setup/accounts?add=account")
    assert r.status_code == 200
    # Phase 1 added tax_liability; the modal dropdown must include it
    # alongside the standard kinds.
    for kind in ("checking", "savings", "credit_card", "line_of_credit",
                 "loan", "tax_liability", "brokerage", "cash"):
        assert f'value="{kind}"' in r.text


# ---------------------------------------------------------------------------
# Save success
# ---------------------------------------------------------------------------


def test_add_account_success_creates_row_and_redirects(app_client):
    _seed_entity(app_client, "TestAcctEntity")
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:TestAcctEntity:BankOne:NewChecking",
            "kind": "checking",
            "entity_slug": "TestAcctEntity",
            "institution": "Bank One",
            "last_four": "1234",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text[:300]
    assert "added=Assets" in r.headers["location"]


def test_add_account_writes_open_directive(app_client, tmp_path):
    """The save handler scaffolds the Open directive in
    connector_accounts.bean alongside the accounts_meta row."""
    _seed_entity(app_client, "TestAcctEntity")
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:TestAcctEntity:Cash",
            "kind": "cash",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    ledger_dir = app_client.app.state.settings.ledger_dir
    connector = ledger_dir / "connector_accounts.bean"
    assert connector.exists()
    text = connector.read_text(encoding="utf-8")
    assert "Assets:TestAcctEntity:Cash" in text
    assert "open Assets:TestAcctEntity:Cash" in text


def test_add_account_persists_kind_via_shared_helper(app_client):
    """Locked spec: the modal save path funnels through the SAME
    helper as the inline label-needed forms. Verify the helper
    is engaged by checking that the row's kind landed correctly
    AND the post-save accounts_meta state mirrors what the inline
    form would produce."""
    _seed_entity(app_client, "TestAcctEntity")
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Liabilities:TestAcctEntity:Visa",
            "kind": "credit_card",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    try:
        row = db.execute(
            "SELECT kind, kind_source, entity_slug, seeded_from_ledger "
            "FROM accounts_meta WHERE account_path = ?",
            ("Liabilities:TestAcctEntity:Visa",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row["kind"] == "credit_card"
    # User-confirmed at creation, no sibling derivation marker.
    assert row["kind_source"] is None
    assert row["entity_slug"] == "TestAcctEntity"
    # Modal-created → seeded_from_ledger = 0 (not boot-discovered).
    assert row["seeded_from_ledger"] == 0


def test_add_account_runs_sibling_inference_post_save(app_client):
    """Phase 2 contract: the post-save handler runs
    infer_kinds_by_sibling so neighboring NULL-kind accounts
    pick up the new row's kind as a sibling-derived suggestion.

    Set up: a NULL-kind row already exists at
    Liabilities:TestAcctEntity:BankOne:Future. We add a
    keyword-derivable peer (Visa). After the modal save, sibling
    inference should propagate credit_card to the Future row.
    """
    _seed_entity(app_client, "TestAcctEntity")
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        db.execute(
            "INSERT OR REPLACE INTO accounts_meta "
            "(account_path, display_name, entity_slug, kind, kind_source, "
            " seeded_from_ledger) "
            "VALUES (?, ?, ?, NULL, NULL, 1)",
            ("Liabilities:TestAcctEntity:BankOne:Future", "Future",
             "TestAcctEntity"),
        )
        db.commit()
    finally:
        db.close()

    # Add a keyword-derivable Visa peer via the modal.
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Liabilities:TestAcctEntity:BankOne:Visa",
            "kind": "credit_card",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    # Sibling inference should have propagated credit_card to the
    # NULL-kind peer.
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    db.row_factory = sqlite3.Row
    try:
        row = db.execute(
            "SELECT kind, kind_source FROM accounts_meta WHERE account_path = ?",
            ("Liabilities:TestAcctEntity:BankOne:Future",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row["kind"] == "credit_card"
    assert row["kind_source"] == "sibling"


# ---------------------------------------------------------------------------
# Validation failure
# ---------------------------------------------------------------------------


def test_add_account_missing_required_re_renders_modal(app_client):
    r = app_client.post(
        "/setup/accounts/add",
        data={"account_path": ""},
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "wiz-modal-backdrop is-open" in r.text
    assert "Required." in r.text


def test_add_account_validation_preserves_typed_values(app_client):
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:GhostEntity:Test",
            "kind": "checking",
            "institution": "TypedBank",
            "last_four": "9999",
            # No entity_slug — and GhostEntity isn't registered.
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert 'value="Assets:GhostEntity:Test"' in r.text
    assert 'value="TypedBank"' in r.text
    assert 'value="9999"' in r.text


def test_add_account_invalid_path_blocks(app_client):
    _seed_entity(app_client, "TestAcctEntity")
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "lowercase:bad:path",
            "kind": "checking",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "valid" in r.text.lower() or "invalid" in r.text.lower()


def test_add_account_too_short_path_blocks(app_client):
    _seed_entity(app_client, "TestAcctEntity")
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:Cash",  # only 2 segments
            "kind": "cash",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "3 segment" in r.text or "Root:Entity:Leaf" in r.text


def test_add_account_wrong_root_blocks(app_client):
    _seed_entity(app_client, "TestAcctEntity")
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Expenses:TestAcctEntity:Misc",
            "kind": "checking",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "Assets" in r.text or "Liabilities" in r.text


def test_add_account_unknown_entity_blocks(app_client):
    r = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:GhostCo:Cash",
            "kind": "cash",
            "entity_slug": "GhostCo",  # not registered
        },
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "GhostCo" in r.text


def test_add_account_collision_blocks(app_client):
    _seed_entity(app_client, "TestAcctEntity")
    # First add succeeds.
    r1 = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:TestAcctEntity:DupeAccount",
            "kind": "checking",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r1.status_code == 303
    # Second add at the same path is rejected.
    r2 = app_client.post(
        "/setup/accounts/add",
        data={
            "account_path": "Assets:TestAcctEntity:DupeAccount",
            "kind": "savings",
            "entity_slug": "TestAcctEntity",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 400
    assert "already exists" in r2.text


# ---------------------------------------------------------------------------
# Acceptance
# ---------------------------------------------------------------------------


def test_setup_accounts_does_not_link_to_settings(app_client):
    r = app_client.get("/setup/accounts")
    assert r.status_code == 200
    assert "/settings/accounts" not in r.text


def test_add_account_modal_does_not_link_to_settings(app_client):
    r = app_client.get("/setup/accounts?add=account")
    assert r.status_code == 200
    assert "/settings/accounts" not in r.text


def test_setup_progress_zero_accounts_punt_targets_setup_not_settings(app_client):
    """Locked acceptance: the empty-accounts case used to route the
    user to /settings/accounts#add-account. Phase 4 retired that
    punt — fix_url should now point at the recovery surface."""
    import sqlite3
    from lamella.features.setup.setup_progress import _check_account_labels
    from lamella.core.db import migrate
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    try:
        step = _check_account_labels(db)
        assert step.fix_url is not None
        assert step.fix_url.startswith("/setup/accounts")
        assert "/settings/" not in step.fix_url
    finally:
        db.close()
