# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Per-user lockout — depth-in-defense, ADR-0050.

This is intentionally conservative: it makes credential-stuffing slow
and noisy. The proxy layer (Cloudflare, Tailscale, fail2ban) is the
real defense for an internet-facing deployment.

Rule:
    threshold failures inside `window_minutes` minutes locks the
    account for `duration_minutes` minutes. Success during the lock
    window is still rejected. Successful login outside the window
    resets the counter.

Counter state lives on the users row (failed_login_count,
last_failed_at, locked_until) so it survives process restart.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


def _parse(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def is_locked(db: sqlite3.Connection, user_id: int) -> bool:
    """Returns True when the account is in a current lock window."""
    row = db.execute(
        "SELECT locked_until FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    if row is None:
        return False
    locked_until = _parse(row["locked_until"])
    if locked_until is None:
        return False
    return locked_until > _utcnow()


def register_failure(
    db: sqlite3.Connection,
    user_id: int,
    *,
    threshold: int,
    window_minutes: int,
    duration_minutes: int,
) -> bool:
    """Record one failed-login. Returns True when this failure tripped
    the lockout (caller logs `lockout` event in that case).

    Logic:
      - If last_failed_at is older than window_minutes, reset the counter
        before incrementing (we only count failures inside the window).
      - Increment.
      - If new count >= threshold, set locked_until = now + duration.
    """
    now = _utcnow()
    row = db.execute(
        "SELECT failed_login_count, last_failed_at FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if row is None:
        return False
    last_failed = _parse(row["last_failed_at"])
    count = row["failed_login_count"] or 0
    if last_failed is None or now - last_failed > timedelta(minutes=window_minutes):
        count = 0
    new_count = count + 1
    locked_until_iso: str | None = None
    tripped = False
    if new_count >= threshold:
        locked_until_iso = _iso(now + timedelta(minutes=duration_minutes))
        tripped = True
    db.execute(
        "UPDATE users SET failed_login_count = ?, last_failed_at = ?, locked_until = ? WHERE id = ?",
        (new_count, _iso(now), locked_until_iso, user_id),
    )
    db.commit()
    return tripped


def register_success(db: sqlite3.Connection, user_id: int) -> None:
    """Reset failure counter and clear any active lock on successful login."""
    db.execute(
        "UPDATE users SET failed_login_count = 0, last_failed_at = NULL, "
        "locked_until = NULL, last_login_at = ? WHERE id = ?",
        (_iso(_utcnow()), user_id),
    )
    db.commit()
