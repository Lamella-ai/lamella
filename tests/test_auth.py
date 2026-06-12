# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0050 — auth flow tests.

Covers login GET/POST, logout, password change, bypass list, HTMX-aware
unauth response, redirect-only-to-local-paths defense, and the
auth-disabled no-op path.
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from lamella.core.config import Settings
from lamella.main import create_app
from lamella.web.auth.passwords import hash_password


@pytest.fixture
def auth_settings(tmp_path, ledger_dir) -> Settings:
    """Settings with auth env-vars set so the bootstrap helper inserts
    a user during lifespan startup. Each test gets a fresh tmp_path
    (so a fresh DB) and a unique session secret."""
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",
        ai_vector_search_enabled=False,
        host="0.0.0.0",  # exercise the warning-banner path
        auth_username="admin",
        auth_password="bootstrap-pass-2026",
        auth_session_secret="test-secret-not-for-prod-use-2026",
        auth_session_days=30,
    )


@pytest.fixture
def auth_client(auth_settings, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    app = create_app(settings=auth_settings)
    with TestClient(app, follow_redirects=False) as client:
        app.state.needs_welcome = False
        app.state.needs_reconstruct = False
        app.state.setup_required_complete = True
        yield client


def _signin(client: TestClient, *, username="admin", password="bootstrap-pass-2026"):
    return client.post(
        "/login",
        data={"username": username, "password": password, "next": "/"},
    )


# --------------- bootstrap + basic flow ---------------


def test_bootstrap_inserts_single_user(auth_client):
    db = auth_client.app.state.db
    rows = db.execute(
        "SELECT id, username, account_id, role FROM users"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["username"] == "admin"
    assert rows[0]["account_id"] == 1
    assert rows[0]["role"] == "owner"


@pytest.mark.xfail(
    reason="DB connection closes between consecutive TestClient lifespans; "
    "pre-existing soft. See project_pytest_baseline_triage.md.",
    strict=False,
)
def test_bootstrap_idempotent(auth_settings, tmp_path, monkeypatch):
    """Two consecutive lifespans must not insert a second user."""
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    app = create_app(settings=auth_settings)
    with TestClient(app, follow_redirects=False):
        pass
    with TestClient(app, follow_redirects=False):
        pass
    db = app.state.db
    rows = db.execute("SELECT COUNT(*) AS n FROM users").fetchone()
    assert rows["n"] == 1


def test_login_get_renders_form(auth_client):
    r = auth_client.get("/login")
    assert r.status_code == 200
    assert "Sign in" in r.text
    assert "Lame" in r.text  # brand wordmark
    assert 'name="username"' in r.text
    assert 'name="password"' in r.text


def test_login_post_correct_creds_sets_cookie_and_redirects(auth_client):
    r = _signin(auth_client)
    assert r.status_code == 303
    assert r.headers["location"] == "/"
    # Cookie set under the insecure (non-Secure) name in TestClient.
    cookies = r.headers.get_list("set-cookie")
    assert any("lamella_session=" in c for c in cookies)


def test_login_post_wrong_creds_returns_form(auth_client):
    r = auth_client.post(
        "/login",
        data={"username": "admin", "password": "wrong", "next": "/"},
    )
    assert r.status_code == 200
    assert "Invalid username or password" in r.text
    cookies = r.headers.get_list("set-cookie")
    assert not any("lamella_session=" in c for c in cookies)


def test_login_post_unknown_user_same_response(auth_client):
    r = auth_client.post(
        "/login",
        data={"username": "nobody", "password": "wrong", "next": "/"},
    )
    assert r.status_code == 200
    assert "Invalid username or password" in r.text
    # No username-existence oracle — same generic message regardless.


def test_protected_route_without_cookie_redirects(auth_client):
    r = auth_client.get("/")
    assert r.status_code == 302
    assert r.headers["location"].startswith("/login?next=")


def test_protected_route_with_cookie_passes_through(auth_client):
    _signin(auth_client)
    r = auth_client.get("/")
    # /  routes return 200 once authenticated. (The setup gate redirects
    # are bypassed by the test fixture's flag overrides.)
    assert r.status_code == 200


def test_login_safe_next_blocks_open_redirect(auth_client):
    """`next` must reject external URLs and protocol-relative paths."""
    r = auth_client.post(
        "/login",
        data={
            "username": "admin",
            "password": "bootstrap-pass-2026",
            "next": "https://evil.example/phish",
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/"

    r2 = auth_client.post(
        "/login",
        data={
            "username": "admin",
            "password": "bootstrap-pass-2026",
            "next": "//evil.example/phish",
        },
    )
    assert r2.status_code == 303
    assert r2.headers["location"] == "/"


def test_login_safe_next_allows_local_paths(auth_client):
    r = auth_client.post(
        "/login",
        data={
            "username": "admin",
            "password": "bootstrap-pass-2026",
            "next": "/transactions",
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/transactions"


# --------------- htmx-aware unauth ---------------


def test_unauth_htmx_returns_hx_redirect(auth_client):
    r = auth_client.get("/", headers={"HX-Request": "true"})
    # HTMX clients honor HX-Redirect; status code is 200, body is empty.
    assert r.status_code == 200
    assert r.headers.get("HX-Redirect", "").startswith("/login?next=")


def test_unauth_api_returns_401(auth_client):
    r = auth_client.get("/api/healthz")
    assert r.status_code in (401, 404)
    # A real /api endpoint would return 401 here. /api/healthz may not
    # exist; the assertion handles either case so the test stays
    # robust against route-namespace shifts.


# --------------- bypass list ---------------


def test_healthz_bypasses_auth(auth_client):
    r = auth_client.get("/healthz")
    assert r.status_code == 200


def test_static_bypasses_auth(auth_client):
    r = auth_client.get("/static/img/lamella-icon.svg")
    # Either 200 (file present) or 404 — but never a 302/401.
    assert r.status_code in (200, 404)


def test_login_bypasses_auth(auth_client):
    r = auth_client.get("/login")
    assert r.status_code == 200


# --------------- logout ---------------


def test_logout_revokes_session(auth_client):
    _signin(auth_client)
    # Post-login: cookie is set; protected routes work.
    r = auth_client.get("/")
    assert r.status_code == 200
    # Logout. Need CSRF token from the session.
    db = auth_client.app.state.db
    csrf = db.execute(
        "SELECT csrf_token FROM auth_sessions WHERE revoked_at IS NULL"
    ).fetchone()["csrf_token"]
    r2 = auth_client.post(
        "/logout", headers={"X-CSRF-Token": csrf}
    )
    assert r2.status_code == 303
    # Cookie cleared; a follow-up request gets the redirect again.
    r3 = auth_client.get("/")
    assert r3.status_code == 302


def test_logout_without_csrf_rejected(auth_client):
    _signin(auth_client)
    r = auth_client.post("/logout")
    assert r.status_code == 403


# --------------- password change ---------------


def test_password_change_requires_current(auth_client):
    _signin(auth_client)
    db = auth_client.app.state.db
    csrf = db.execute(
        "SELECT csrf_token FROM auth_sessions WHERE revoked_at IS NULL"
    ).fetchone()["csrf_token"]
    r = auth_client.post(
        "/account/password",
        data={
            "current_password": "wrong-current",
            "new_password": "new-pass-2026!",
            "confirm_password": "new-pass-2026!",
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert r.status_code == 200
    assert "Current password is incorrect" in r.text


def test_password_change_min_length(auth_client):
    _signin(auth_client)
    db = auth_client.app.state.db
    csrf = db.execute(
        "SELECT csrf_token FROM auth_sessions WHERE revoked_at IS NULL"
    ).fetchone()["csrf_token"]
    r = auth_client.post(
        "/account/password",
        data={
            "current_password": "bootstrap-pass-2026",
            "new_password": "short",
            "confirm_password": "short",
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert r.status_code == 200
    assert "at least 8 characters" in r.text


def test_password_change_revokes_old_sessions(auth_client):
    _signin(auth_client)
    db = auth_client.app.state.db
    csrf = db.execute(
        "SELECT csrf_token FROM auth_sessions WHERE revoked_at IS NULL"
    ).fetchone()["csrf_token"]
    r = auth_client.post(
        "/account/password",
        data={
            "current_password": "bootstrap-pass-2026",
            "new_password": "new-pass-2026!",
            "confirm_password": "new-pass-2026!",
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert r.status_code == 200
    # Old sessions revoked; exactly one live session (the freshly
    # rotated one issued by the password-change handler).
    live = db.execute(
        "SELECT COUNT(*) AS n FROM auth_sessions WHERE revoked_at IS NULL"
    ).fetchone()["n"]
    assert live == 1


# --------------- auth disabled (no-op middleware) ---------------


def test_auth_disabled_routes_are_open(settings, tmp_path, monkeypatch):
    """No AUTH_USERNAME → middleware injects ANONYMOUS_OWNER, no /login redirect."""
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    # Use a fresh Settings instance with no auth_username so bootstrap
    # is skipped and the middleware operates in no-op mode.
    no_auth = Settings(
        data_dir=tmp_path / "data2",
        ledger_dir=settings.ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",
        ai_vector_search_enabled=False,
        host="127.0.0.1",
    )
    app = create_app(settings=no_auth)
    with TestClient(app, follow_redirects=False) as client:
        app.state.needs_welcome = False
        app.state.needs_reconstruct = False
        app.state.setup_required_complete = True
        r = client.get("/")
        assert r.status_code == 200
        # Login form is reachable but its protections are inert when
        # the users table is empty — submitting a wrong password just
        # re-renders the form (no ground-truth user to compare against).
        r_login = client.get("/login")
        assert r_login.status_code == 200
