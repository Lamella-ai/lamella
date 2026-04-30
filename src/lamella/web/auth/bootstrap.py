# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Bootstrap path — env-var → first user, ADR-0050.

If AUTH_USERNAME is set AND no user rows exist yet, insert one user
with the env-supplied password (hashed). Subsequent runs: the row
exists, env vars are ignored.

Also resolves the session-signing secret: from AUTH_SESSION_SECRET
when set; otherwise auto-generates and persists `data_dir/.session-secret`
on first start so existing cookies keep validating across restarts.
"""

from __future__ import annotations

import logging
import secrets
import sqlite3
from pathlib import Path
from typing import Optional

from lamella.core.config import Settings
from lamella.web.auth.events import EVENT_BOOTSTRAP, record_event
from lamella.web.auth.passwords import hash_password


log = logging.getLogger("lamella.auth.bootstrap")


def has_users(db: sqlite3.Connection) -> bool:
    row = db.execute("SELECT 1 FROM users LIMIT 1").fetchone()
    return row is not None


def bootstrap_user(db: sqlite3.Connection, settings: Settings) -> Optional[int]:
    """Insert the bootstrap user when the table is empty AND env-var
    creds are configured. Returns the new user_id or None when no
    bootstrap was needed / possible.

    Idempotent: a second call with the same env vars after the first
    bootstrap returns None because the row already exists.
    """
    if has_users(db):
        return None
    if not settings.auth_username:
        return None
    username = settings.auth_username.strip()
    if not username:
        return None

    if settings.auth_password_hash:
        # Pre-hashed bootstrap. Trusted to be a valid argon2 / bcrypt
        # encoded hash; argon2.verify will tell us at first login if
        # it isn't.
        hashed = settings.auth_password_hash.get_secret_value()
    elif settings.auth_password:
        hashed = hash_password(settings.auth_password.get_secret_value())
    else:
        log.warning(
            "auth bootstrap skipped: AUTH_USERNAME=%s set but neither "
            "AUTH_PASSWORD nor AUTH_PASSWORD_HASH provided",
            username,
        )
        return None

    cur = db.execute(
        """
        INSERT INTO users (account_id, username, password_hash, role)
        VALUES (?, ?, ?, ?)
        """,
        (1, username, hashed, "owner"),
    )
    db.commit()
    user_id = cur.lastrowid
    log.info(
        "auth bootstrap: inserted user %s (id=%s, account_id=1, role=owner)",
        username, user_id,
    )
    record_event(
        db,
        event_type=EVENT_BOOTSTRAP,
        user_id=user_id,
        account_id=1,
        success=True,
        detail=f"username={username}",
    )
    return user_id


def resolve_session_secret(settings: Settings) -> str:
    """Return the configured AUTH_SESSION_SECRET, or auto-generate +
    persist `data_dir/.session-secret` and return that.

    The on-disk file is created with mode 0600 — readable only by the
    process owner. This works on both Linux containers and the
    Windows-via-WSL dev path.
    """
    if settings.auth_session_secret:
        v = settings.auth_session_secret.get_secret_value()
        if v:
            return v
    path: Path = settings.session_secret_path
    if path.exists():
        try:
            return path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            log.warning(
                "session secret at %s exists but unreadable (%s); regenerating",
                path, exc,
            )
    secret = secrets.token_urlsafe(64)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(secret, encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        # Some filesystems (Windows / FAT) reject chmod; not fatal.
        pass
    log.info("auth bootstrap: generated new session secret at %s", path)
    return secret


def emit_exposure_warning_banner(settings: Settings) -> None:
    """Loud startup warning when bind is non-loopback AND auth is unset.
    The operator may have a reason; we don't refuse to start. We do
    make sure the choice was visible in the logs."""
    host = (settings.host or "").strip()
    looks_loopback = host in ("127.0.0.1", "localhost", "::1", "")
    if looks_loopback:
        return
    if settings.auth_enabled:
        return
    banner = (
        "\n"
        "  ###################################################################\n"
        "  #                                                                 #\n"
        "  #  WARNING — Lamella is bound to a non-loopback address with NO   #\n"
        "  #  authentication configured.                                     #\n"
        "  #                                                                 #\n"
        "  #    HOST = %-55s  #\n"
        "  #                                                                 #\n"
        "  #  Anyone who can reach this address has full access to your      #\n"
        "  #  ledger, receipts, and bank-feed credentials.                   #\n"
        "  #                                                                 #\n"
        "  #  Either:                                                        #\n"
        "  #    * set HOST=127.0.0.1 (default for non-Docker)                #\n"
        "  #    * put it behind a reverse proxy that authenticates           #\n"
        "  #      (Cloudflare Tunnel, Tailscale, nginx basic auth, etc.)     #\n"
        "  #    * set AUTH_USERNAME + AUTH_PASSWORD on first start           #\n"
        "  #                                                                 #\n"
        "  ###################################################################\n"
    ) % host
    log.warning(banner)
