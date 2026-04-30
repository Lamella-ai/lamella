# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Auth event log — append-only audit trail per ADR-0050.

Every credential touch is recorded: login_success, login_failure,
logout, password_change, lockout, bootstrap_user_created.

Per ADR-0025 (logs identify entities, never expose values) we record
user_id (nullable for failed-unknown-user), not the attempted
username, and never the attempted password.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import Request


# Canonical event-type vocabulary. Any change to this set must update
# the audit dashboard query in lamella.web.routes.audit too.
EVENT_LOGIN_SUCCESS = "login_success"
EVENT_LOGIN_FAILURE = "login_failure"
EVENT_LOGOUT = "logout"
EVENT_PASSWORD_CHANGE = "password_change"
EVENT_LOCKOUT = "lockout"
EVENT_BOOTSTRAP = "bootstrap_user_created"
EVENT_CSRF_REJECTED = "csrf_rejected"
EVENT_DISCLAIMER_ACK = "disclaimer_acknowledged"


def _client_ip(request: Request | None) -> str:
    if request is None:
        return ""
    fwd = request.headers.get("x-forwarded-for", "") if hasattr(request, "headers") else ""
    if fwd:
        # First entry of XFF is the originating client; the rest are
        # proxies. Trim aggressively.
        return fwd.split(",")[0].strip()[:64]
    client = getattr(request, "client", None)
    if client and getattr(client, "host", None):
        return str(client.host)[:64]
    return ""


def _client_ua(request: Request | None) -> str:
    if request is None:
        return ""
    return (request.headers.get("user-agent", "") if hasattr(request, "headers") else "")[:512]


def record_event(
    db: sqlite3.Connection,
    *,
    event_type: str,
    user_id: Optional[int],
    account_id: Optional[int],
    success: bool,
    request: Request | None = None,
    detail: str | None = None,
) -> None:
    """Append an auth event row. Best-effort: a write failure here
    must not break the request flow."""
    try:
        db.execute(
            """
            INSERT INTO auth_events
                (user_id, account_id, event_type, success, ip, ua, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                account_id,
                event_type,
                1 if success else 0,
                _client_ip(request),
                _client_ua(request),
                (detail or "")[:1024],
            ),
        )
        db.commit()
    except sqlite3.Error:
        # Logging failures are not authentication failures.
        pass
