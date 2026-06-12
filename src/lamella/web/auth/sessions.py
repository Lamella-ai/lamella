# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""DB-backed session store — ADR-0050.

Sessions live in `auth_sessions`; the cookie carries only a signed
session_id. The signature is `itsdangerous` HMAC; tampered cookies
fail the unsign step before any DB lookup. Server-side state buys:

  - logout that actually revokes (cookie cleared AND row revoked)
  - password change kills all sessions
  - session rotation on login (anti session-fixation)
  - active-sessions UI surface (data exists today, UI later)

Cookie name uses the `__Host-` prefix when Secure is set: it binds
the cookie to the current host with no Domain attribute, so a
subdomain cannot inject a forged session.
"""

from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from itsdangerous import BadSignature, URLSafeSerializer

# Cookie names. The `__Host-` prefix requires Secure + Path=/ + no
# Domain; we set those when HTTPS is detected. Without HTTPS we use
# the unprefixed name — older browsers and dev workflows still work.
COOKIE_SECURE = "__Host-lamella_session"
COOKIE_INSECURE = "lamella_session"
CSRF_COOKIE_SECURE = "__Host-lamella_csrf"
CSRF_COOKIE_INSECURE = "lamella_csrf"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SessionRecord:
    session_id: str
    user_id: int
    account_id: int
    csrf_token: str
    expires_at: datetime
    revoked_at: Optional[datetime]


def _serializer(secret: str) -> URLSafeSerializer:
    return URLSafeSerializer(secret, salt="lamella.session.v1")


def sign_session_id(session_id: str, secret: str) -> str:
    return _serializer(secret).dumps(session_id)


def unsign_session_id(signed: str, secret: str) -> Optional[str]:
    """Returns the session_id on success, None on tamper / malformed."""
    try:
        value = _serializer(secret).loads(signed)
    except BadSignature:
        return None
    if not isinstance(value, str):
        return None
    return value


def create_session(
    db: sqlite3.Connection,
    *,
    user_id: int,
    account_id: int,
    expires_in_days: int,
    ua: str | None,
    ip: str | None,
) -> SessionRecord:
    """Insert a new session row and return the record. Caller signs
    the session_id and sets the cookie."""
    session_id = secrets.token_urlsafe(32)
    csrf_token = secrets.token_urlsafe(32)
    expires_at = _utcnow() + timedelta(days=expires_in_days)
    db.execute(
        """
        INSERT INTO auth_sessions
            (session_id, user_id, account_id, csrf_token, expires_at, ua, ip)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            user_id,
            account_id,
            csrf_token,
            expires_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z",
            (ua or "")[:512],
            (ip or "")[:64],
        ),
    )
    db.commit()
    return SessionRecord(
        session_id=session_id,
        user_id=user_id,
        account_id=account_id,
        csrf_token=csrf_token,
        expires_at=expires_at,
        revoked_at=None,
    )


def load_session(
    db: sqlite3.Connection, session_id: str
) -> Optional[SessionRecord]:
    """Returns a live session record; None when missing, expired, or revoked."""
    row = db.execute(
        """
        SELECT session_id, user_id, account_id, csrf_token, expires_at, revoked_at
          FROM auth_sessions
         WHERE session_id = ?
        """,
        (session_id,),
    ).fetchone()
    if row is None:
        return None
    revoked_at_raw = row["revoked_at"]
    if revoked_at_raw is not None:
        return None
    expires_at = _parse_iso(row["expires_at"])
    if expires_at is None or expires_at <= _utcnow():
        return None
    return SessionRecord(
        session_id=row["session_id"],
        user_id=row["user_id"],
        account_id=row["account_id"],
        csrf_token=row["csrf_token"],
        expires_at=expires_at,
        revoked_at=None,
    )


def touch_session(db: sqlite3.Connection, session_id: str) -> None:
    """Bump last_seen on every authenticated request."""
    db.execute(
        "UPDATE auth_sessions SET last_seen = ? WHERE session_id = ?",
        (_utcnow_iso(), session_id),
    )
    db.commit()


def revoke_session(db: sqlite3.Connection, session_id: str) -> None:
    """Logout — sets revoked_at; subsequent load_session returns None."""
    db.execute(
        "UPDATE auth_sessions SET revoked_at = ? WHERE session_id = ? AND revoked_at IS NULL",
        (_utcnow_iso(), session_id),
    )
    db.commit()


def revoke_all_for_user(db: sqlite3.Connection, user_id: int) -> int:
    """Revoke every live session for the user. Returns the row count.
    Called from /account/password after a successful change."""
    cur = db.execute(
        "UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
        (_utcnow_iso(), user_id),
    )
    db.commit()
    return cur.rowcount or 0


def _parse_iso(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        # SQLite stores `YYYY-MM-DDTHH:MM:SS.fffZ`; fromisoformat in 3.12
        # accepts the trailing Z when stripped.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None
