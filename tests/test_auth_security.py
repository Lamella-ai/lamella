# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0050 — auth security regressions.

Covers password-hash properties, session signature integrity, lockout,
CSRF cookie shape, and the response-header surface.
"""

from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from lamella.core.config import Settings
from lamella.main import create_app
from lamella.web.auth import lockout, passwords, sessions


# --------------- argon2 hash properties ---------------


def test_hash_password_emits_argon2id():
    h = passwords.hash_password("test-password-2026")
    assert h.startswith("$argon2id$")


def test_verify_password_round_trip():
    h = passwords.hash_password("alpha-beta-gamma")
    assert passwords.verify_password(h, "alpha-beta-gamma") is True
    assert passwords.verify_password(h, "wrong-pass") is False


def test_verify_dummy_hash_matches_no_real_password():
    """The DUMMY_HASH is verified against on the unknown-user path. It
    must not accidentally match any common short string a typo might
    produce."""
    for candidate in ("", "password", "admin", "test", "1234"):
        assert passwords.verify_password(passwords.DUMMY_HASH, candidate) is False


def test_constant_time_compare_unicode_safe():
    assert passwords.constant_time_compare("café", "café")
    assert not passwords.constant_time_compare("café", "cafe")


def test_hash_empty_password_rejected():
    with pytest.raises(ValueError):
        passwords.hash_password("")


# --------------- session signing / unsigning ---------------


def test_sign_unsign_roundtrip():
    secret = "test-secret-2026"
    signed = sessions.sign_session_id("session-id-abc", secret)
    assert sessions.unsign_session_id(signed, secret) == "session-id-abc"


def test_unsign_with_wrong_secret_fails():
    signed = sessions.sign_session_id("session-id-abc", "secret-a")
    assert sessions.unsign_session_id(signed, "secret-b") is None


def test_unsign_tampered_payload_fails():
    secret = "test-secret-2026"
    signed = sessions.sign_session_id("session-id-abc", secret)
    tampered = signed[:-1] + ("A" if signed[-1] != "A" else "B")
    assert sessions.unsign_session_id(tampered, secret) is None


def test_unsign_garbage_input_fails():
    assert sessions.unsign_session_id("not-a-real-token", "any-secret") is None
    assert sessions.unsign_session_id("", "any-secret") is None


# --------------- session DB operations ---------------


@pytest.fixture
def authdb(tmp_path):
    """Fresh SQLite with only the auth tables applied."""
    conn = sqlite3.connect(tmp_path / "auth.sqlite")
    conn.row_factory = sqlite3.Row
    with open("migrations/060_auth_tables.sql") as f:
        conn.executescript(f.read())
    # Insert a user so session ops have a foreign-key target.
    conn.execute(
        "INSERT INTO users (account_id, username, password_hash) VALUES (?, ?, ?)",
        (1, "admin", passwords.hash_password("test-pass")),
    )
    conn.commit()
    yield conn
    conn.close()


def test_create_and_load_session(authdb):
    rec = sessions.create_session(
        authdb, user_id=1, account_id=1, expires_in_days=30,
        ua="test-agent", ip="127.0.0.1",
    )
    loaded = sessions.load_session(authdb, rec.session_id)
    assert loaded is not None
    assert loaded.user_id == 1
    assert loaded.account_id == 1
    assert loaded.csrf_token == rec.csrf_token


def test_revoked_session_does_not_load(authdb):
    rec = sessions.create_session(
        authdb, user_id=1, account_id=1, expires_in_days=30,
        ua="test-agent", ip="127.0.0.1",
    )
    sessions.revoke_session(authdb, rec.session_id)
    assert sessions.load_session(authdb, rec.session_id) is None


def test_revoke_all_for_user_kills_every_session(authdb):
    sessions.create_session(authdb, user_id=1, account_id=1, expires_in_days=30, ua="a", ip="1")
    sessions.create_session(authdb, user_id=1, account_id=1, expires_in_days=30, ua="b", ip="2")
    sessions.create_session(authdb, user_id=1, account_id=1, expires_in_days=30, ua="c", ip="3")
    n = sessions.revoke_all_for_user(authdb, 1)
    assert n == 3
    live = authdb.execute(
        "SELECT COUNT(*) AS n FROM auth_sessions WHERE revoked_at IS NULL"
    ).fetchone()["n"]
    assert live == 0


# --------------- lockout ---------------


def test_lockout_after_threshold(authdb):
    user_id = 1
    for _ in range(4):
        tripped = lockout.register_failure(
            authdb, user_id,
            threshold=5, window_minutes=15, duration_minutes=15,
        )
        assert tripped is False
        assert lockout.is_locked(authdb, user_id) is False
    tripped = lockout.register_failure(
        authdb, user_id,
        threshold=5, window_minutes=15, duration_minutes=15,
    )
    assert tripped is True
    assert lockout.is_locked(authdb, user_id) is True


def test_lockout_resets_on_success(authdb):
    user_id = 1
    for _ in range(3):
        lockout.register_failure(
            authdb, user_id, threshold=5, window_minutes=15, duration_minutes=15,
        )
    lockout.register_success(authdb, user_id)
    row = authdb.execute(
        "SELECT failed_login_count, locked_until FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    assert row["failed_login_count"] == 0
    assert row["locked_until"] is None


# --------------- response headers ---------------


@pytest.fixture
def auth_client(tmp_path, ledger_dir, monkeypatch):
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=ledger_dir,
        paperless_url="https://paperless.test",
        paperless_api_token="token-test",
        ai_vector_search_enabled=False,
        auth_username="admin",
        auth_password="bootstrap-pass-2026",
        auth_session_secret="test-secret-not-for-prod-2026",
    )
    app = create_app(settings=settings)
    with TestClient(app, follow_redirects=False) as client:
        app.state.needs_welcome = False
        app.state.needs_reconstruct = False
        app.state.setup_required_complete = True
        yield client


def test_security_headers_present(auth_client):
    r = auth_client.get("/login")
    assert r.headers.get("X-Content-Type-Options") == "nosniff"
    assert "Referrer-Policy" in r.headers
    # /login gets the strict CSP + X-Frame-Options DENY
    assert r.headers.get("X-Frame-Options") == "DENY"
    csp = r.headers.get("Content-Security-Policy", "")
    assert "default-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp


def test_login_cookie_flags(auth_client):
    r = auth_client.post(
        "/login",
        data={"username": "admin", "password": "bootstrap-pass-2026", "next": "/"},
    )
    assert r.status_code == 303
    set_cookie = next(
        (c for c in r.headers.get_list("set-cookie") if "lamella_session=" in c),
        "",
    )
    assert set_cookie, "session cookie not set"
    assert "HttpOnly" in set_cookie
    assert "SameSite=lax" in set_cookie.lower() or "samesite=lax" in set_cookie.lower()
