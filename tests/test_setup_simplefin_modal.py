# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Phase 4 SimpleFIN recovery wrapper.

Structurally different from CRUD editors — tests pin the
state machine (unconnected/connected_no_accounts/
connected_unbound/connected_bound), the token security rules
(never echoed, never logged, never round-tripped), the
skip-with-dismissed-at suppression, and the no-href-breakout
acceptance criterion (zero bare /simplefin/* hrefs in rendered
output).
"""
from __future__ import annotations

import sqlite3

import pytest


# ---------------------------------------------------------------------------
# State machine: unconnected
# ---------------------------------------------------------------------------


def test_unconnected_state_renders_paste_token_form(app_client):
    r = app_client.get("/setup/simplefin")
    assert r.status_code == 200
    # Token paste form is the unconnected-state UI.
    assert "Paste a one-time setup token" in r.text or "setup token" in r.text.lower()
    assert 'name="simplefin_token"' in r.text
    # Skip button is required, not optional.
    assert 'action="/setup/simplefin/skip"' in r.text


def test_unconnected_token_field_is_password_input(app_client):
    """Locked constraint: token paste field is treated as a
    credential. type=password keeps it from being shoulder-surfed
    and signals 'sensitive value' to autofill / password managers."""
    r = app_client.get("/setup/simplefin")
    assert r.status_code == 200
    # Should be a password-type input, not plain text.
    assert 'type="password"' in r.text
    # autocomplete=off prevents browser stash.
    assert 'autocomplete="off"' in r.text


def test_unconnected_does_not_render_a_value_attribute_for_token(app_client):
    """No round-trip — the field is type-only. Even on initial render,
    no `value="..."` should be set on the simplefin_token input."""
    r = app_client.get("/setup/simplefin")
    assert r.status_code == 200
    # Look at the snippet around the token input. Any `value=` on
    # that input would be a regression — this assertion catches it.
    import re
    match = re.search(
        r'<input[^>]*name="simplefin_token"[^>]*>', r.text,
    )
    assert match is not None
    assert "value=" not in match.group(0), (
        "simplefin_token input must not carry a value attribute — "
        "tokens are credentials, not round-trippable form state"
    )


# ---------------------------------------------------------------------------
# Token security on save failure
# ---------------------------------------------------------------------------


def test_connect_with_empty_token_re_renders_with_generic_error(app_client):
    r = app_client.post(
        "/setup/simplefin/connect",
        data={"simplefin_token": ""},
        follow_redirects=False,
    )
    assert r.status_code == 400
    # Error visible on the page.
    assert "Paste" in r.text or "token" in r.text.lower()
    # No value attribute carried back even with an empty token.
    import re
    match = re.search(
        r'<input[^>]*name="simplefin_token"[^>]*>', r.text,
    )
    assert match is not None
    assert "value=" not in match.group(0)


def test_failed_connect_does_not_echo_token(app_client, monkeypatch):
    """The locked rule: even when the user typed a (failed) token,
    the re-render must NOT carry that token back in the form. Type-
    only, never display.

    To trigger the failure path deterministically we monkeypatch
    claim_setup_token to raise — actual bridge calls obviously
    aren't valid in tests."""
    from lamella.adapters.simplefin import client as sf_client

    def _fake_claim(token: str) -> str:
        raise sf_client.SimpleFINAuthError("synthetic auth failure")

    monkeypatch.setattr(sf_client, "claim_setup_token", _fake_claim)
    # Also patch the routes/setup.py-imported alias if it was bound.
    import lamella.web.routes.setup as _setup_routes
    if hasattr(_setup_routes, "claim_setup_token"):
        monkeypatch.setattr(
            _setup_routes, "claim_setup_token", _fake_claim, raising=False,
        )

    secret = "TOKEN-secretbearscannerinvalidforthebridge-XYZ"
    r = app_client.post(
        "/setup/simplefin/connect",
        data={"simplefin_token": secret},
        follow_redirects=False,
    )
    assert r.status_code in (200, 400)
    # The secret must not appear anywhere in the rendered output.
    assert secret not in r.text, (
        "failed-connect re-render leaked the token back to the user"
    )
    # The secret's distinctive substring also must not appear.
    assert "secretbearscannerinvalidforthebridge" not in r.text


# ---------------------------------------------------------------------------
# Skip flow
# ---------------------------------------------------------------------------


def test_skip_redirects_to_recovery_with_skipped_marker(app_client):
    """Phase 7 URL rename: /setup/progress → /setup/recovery."""
    r = app_client.post(
        "/setup/simplefin/skip", follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"].startswith("/setup/recovery")
    assert "skipped=simplefin" in r.headers["location"]


def test_skip_persists_dismissed_at_timestamp(app_client):
    app_client.post("/setup/simplefin/skip", follow_redirects=False)
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        row = db.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            ("simplefin_dismissed_at",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[0]  # non-empty ISO timestamp
    # ISO 8601 format with 'T' separator.
    assert "T" in row[0]


def test_progress_check_suppresses_simplefin_finding_after_skip(app_client):
    """Locked constraint: skip stamps a dismissed_at that suppresses
    the recovery-progress finding for 7 days. Recovery doesn't nag
    users who don't use SimpleFIN."""
    from datetime import datetime, timezone, timedelta
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        # Stamp a dismissed_at as if the user just clicked Skip.
        db.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
            (
                "simplefin_dismissed_at",
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            ),
        )
        db.commit()
        from lamella.features.setup.setup_progress import _check_simplefin
        db.row_factory = sqlite3.Row
        step = _check_simplefin(db)
    finally:
        db.close()
    # Suppressed → marked complete + summary mentions skip.
    assert step.is_complete is True
    assert "skipped" in step.summary.lower() or "suppress" in step.summary.lower()


def test_progress_check_re_engages_after_dismissal_expires(app_client):
    """Eight days after a dismiss, the finding re-surfaces."""
    from datetime import datetime, timezone, timedelta
    expired = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat(timespec="seconds")
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        db.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
            ("simplefin_dismissed_at", expired),
        )
        db.commit()
        from lamella.features.setup.setup_progress import _check_simplefin
        db.row_factory = sqlite3.Row
        step = _check_simplefin(db)
    finally:
        db.close()
    # Expired → finding active again.
    assert step.is_complete is False


# ---------------------------------------------------------------------------
# Acceptance: no /simplefin (bare) href breakouts
# ---------------------------------------------------------------------------


def test_unconnected_page_does_not_break_out_to_simplefin_admin(app_client):
    """Locked acceptance criterion: zero bare /simplefin/* href
    references in any rendered page within /setup/recovery. Internal
    POST-action references like /setup/simplefin/connect are fine
    (those are recovery handlers); /simplefin (admin) hrefs are not."""
    r = app_client.get("/setup/simplefin")
    assert r.status_code == 200
    # No anchor element targets bare /simplefin or /simplefin/*.
    import re
    bare_hrefs = re.findall(
        r'href="(/simplefin(?:/[^"]*)?)"', r.text,
    )
    # /setup/simplefin/* is allowed and does not match this regex
    # (the prefix is /simplefin, not /setup/simplefin).
    assert bare_hrefs == [], (
        f"recovery wrapper leaked bare /simplefin hrefs: {bare_hrefs}"
    )


def test_progress_check_routes_to_recovery_wrapper_not_admin(app_client):
    """The recovery-progress finding's fix_url must be /setup/simplefin
    (the wrapper), never /simplefin (the admin)."""
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        from lamella.features.setup.setup_progress import _check_simplefin
        db.row_factory = sqlite3.Row
        step = _check_simplefin(db)
    finally:
        db.close()
    assert step.fix_url == "/setup/simplefin", step.fix_url


# ---------------------------------------------------------------------------
# Connected states (smoke tests with seeded discovered accounts)
# ---------------------------------------------------------------------------


def _seed_simplefin_state(
    app_client, *, has_access: bool, discovered: list[dict] | None = None,
):
    """Seed the SimpleFIN state directly into SQLite + settings,
    bypassing the bridge call. Shape mirrors what
    _upsert_discovered_accounts produces."""
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        if has_access:
            db.execute(
                "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
                ("simplefin_access_url", "https://example.com/sf-access"),
            )
        for acc in (discovered or []):
            db.execute(
                """
                INSERT OR REPLACE INTO simplefin_discovered_accounts
                  (account_id, name, org_name, currency, balance, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    acc["account_id"], acc["name"], acc.get("org_name"),
                    acc.get("currency", "USD"), acc.get("balance", "0.00"),
                    "2026-04-25T12:00:00+00:00",
                ),
            )
        db.commit()
    finally:
        db.close()
    # Refresh the in-memory Settings so the page reads the new access URL.
    if has_access:
        app_client.app.state.settings.apply_kv_overrides({
            "simplefin_access_url": "https://example.com/sf-access",
        })


def test_connected_no_accounts_state_renders_re_fetch_ui(app_client):
    _seed_simplefin_state(app_client, has_access=True, discovered=[])
    r = app_client.get("/setup/simplefin")
    assert r.status_code == 200
    # Connected state: no token paste form.
    assert 'name="simplefin_token"' not in r.text
    # Status banner mentions zero accounts.
    assert "zero accounts" in r.text or "no accounts" in r.text.lower() or \
           "0 accounts" in r.text
    # Disconnect option visible.
    assert 'action="/setup/simplefin/disconnect"' in r.text


def test_connected_unbound_state_renders_binding_table(app_client):
    _seed_simplefin_state(
        app_client, has_access=True,
        discovered=[
            {
                "account_id": "ACC_TEST_1",
                "name": "Bank One Checking",
                "org_name": "Bank One",
                "currency": "USD",
                "balance": "1234.56",
            },
        ],
    )
    r = app_client.get("/setup/simplefin")
    assert r.status_code == 200
    # Discovered account surfaces.
    assert "Bank One Checking" in r.text
    assert "ACC_TEST_1" in r.text
    # Bind form per row.
    assert 'action="/setup/simplefin/bind"' in r.text
    # Datalist for ledger account paths.
    assert 'id="setup-simplefin-paths-datalist"' in r.text


def test_bind_clears_existing_binding_on_explicit_empty_path(app_client):
    """Binding with an empty account_path releases any prior claim."""
    _seed_simplefin_state(
        app_client, has_access=True,
        discovered=[{
            "account_id": "ACC_BIND_TEST",
            "name": "Bound Account",
            "currency": "USD",
            "balance": "0",
        }],
    )
    # First, bind the SimpleFIN id to an account_path that exists in
    # the fixture ledger's accounts_meta. We can't predict which
    # paths exist, but we can prime accounts_meta directly.
    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        db.execute(
            "INSERT OR IGNORE INTO accounts_meta "
            "  (account_path, display_name, simplefin_account_id) "
            "VALUES (?, ?, ?)",
            ("Assets:Personal:Test:Sf", "Test SF", None),
        )
        db.commit()
    finally:
        db.close()

    r = app_client.post(
        "/setup/simplefin/bind",
        data={
            "simplefin_id": "ACC_BIND_TEST",
            "account_path": "Assets:Personal:Test:Sf",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    # Now clear by submitting an empty path.
    r2 = app_client.post(
        "/setup/simplefin/bind",
        data={
            "simplefin_id": "ACC_BIND_TEST",
            "account_path": "",
        },
        follow_redirects=False,
    )
    assert r2.status_code == 303

    db = sqlite3.connect(app_client.app.state.settings.db_path)
    try:
        row = db.execute(
            "SELECT simplefin_account_id FROM accounts_meta "
            "WHERE account_path = ?", ("Assets:Personal:Test:Sf",),
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[0] is None
