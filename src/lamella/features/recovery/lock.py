# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``setup_recovery_lock`` — durable in-flight latch for /setup/recovery/apply.

Resolves gap §11.7 of RECOVERY_SYSTEM.md. The pre-existing JobRunner
``runner.active()`` check inside ``recovery_apply`` is a process-local
guard: it works for the steady-state two-tabs-same-process case but
falls apart across server restarts, in the brief window between
request arrival and ``runner.submit``, and for future entry points
(CLI, scheduled scan) that bypass the route layer.

This module is the durable answer: a single-row latch in
``setup_recovery_lock`` keyed on ``"current"`` (mirrors
``setup_repair_state``'s session-id convention). Acquire is atomic
via SQLite's ``INSERT ... ON CONFLICT DO NOTHING`` — the rowcount
tells us whether we got the lock without a separate read. Release
is unconditional ``DELETE`` — a crashed worker that never reaches
the release branch leaves the row behind, which a future operator
clears manually with ``DELETE FROM setup_recovery_lock``.

Usage from the route layer:

    state = acquire_recovery_lock(conn, holder=f"job:{job_id}")
    if state is not None:
        # Already held — render friendly error with state.holder /
        # state.acquired_at.
        ...

The job worker pairs the acquire with a try/finally release:

    try:
        ...do work...
    finally:
        release_recovery_lock(conn)
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


__all__ = [
    "DEFAULT_SESSION_ID",
    "RecoveryLockState",
    "acquire_recovery_lock",
    "current_lock_state",
    "release_recovery_lock",
]


DEFAULT_SESSION_ID = "current"
"""Mirror :data:`lamella.features.recovery.repair_state.DEFAULT_SESSION_ID`
— v1 is single-session. A future Phase 7+ multi-session expansion
introduces real session ids without a schema change."""


@dataclass(frozen=True)
class RecoveryLockState:
    """Snapshot of who currently holds the lock, surfaced to the
    route layer so the friendly "another tab is running recovery"
    error can identify the holder + acquisition time."""

    session_id: str
    holder: str
    """Free-form holder identifier. The route layer passes
    ``f"job:{job_id}"`` so a lock held by an in-flight apply job
    is traceable back through the JobRunner; tests pass synthetic
    strings."""

    acquired_at: str
    """ISO-8601 UTC string from SQLite's ``CURRENT_TIMESTAMP``.
    Surfaced verbatim — the UI renders it relative ("acquired 12s
    ago") rather than parsing it back into a datetime."""


def acquire_recovery_lock(
    conn: sqlite3.Connection,
    *,
    holder: str,
    session_id: str = DEFAULT_SESSION_ID,
) -> RecoveryLockState | None:
    """Try to take the in-flight lock for ``session_id``.

    Returns ``None`` on success — the lock is now held by ``holder``
    and the caller MUST pair this with a ``release_recovery_lock``
    call in a try/finally so a worker crash doesn't strand the row.

    Returns a :class:`RecoveryLockState` describing the existing
    holder when the lock is already held. The caller renders this
    as a friendly "recovery already in progress in another tab"
    error and does NOT proceed.

    Atomicity: SQLite's ``INSERT ... ON CONFLICT DO NOTHING`` is a
    single statement under ``BEGIN IMMEDIATE`` semantics, so there's
    no race between two callers checking + inserting. The rowcount
    after the insert tells us whether we won the race without a
    separate ``SELECT``. Commit immediately so a sibling caller in
    a different connection sees the row.
    """
    if not holder:
        raise ValueError("acquire_recovery_lock: holder must be non-empty")

    cur = conn.execute(
        """
        INSERT INTO setup_recovery_lock (session_id, holder)
        VALUES (?, ?)
        ON CONFLICT(session_id) DO NOTHING
        """,
        (session_id, holder),
    )
    conn.commit()
    if cur.rowcount == 1:
        # We got the lock.
        return None

    # Someone else holds it — read the existing row so the caller
    # can render a useful error. The read is a separate statement
    # because ``RETURNING`` on conflict-skipped inserts isn't portable
    # across SQLite versions in our deploy matrix.
    return current_lock_state(conn, session_id=session_id)


def release_recovery_lock(
    conn: sqlite3.Connection,
    *,
    session_id: str = DEFAULT_SESSION_ID,
) -> None:
    """Drop the lock row for ``session_id``. Idempotent — silent
    if no row exists. Always safe to call from the worker's
    ``finally`` branch even when the acquire failed mid-flight."""
    conn.execute(
        "DELETE FROM setup_recovery_lock WHERE session_id = ?",
        (session_id,),
    )
    conn.commit()


def current_lock_state(
    conn: sqlite3.Connection,
    *,
    session_id: str = DEFAULT_SESSION_ID,
) -> RecoveryLockState | None:
    """Return the current lock state, or ``None`` if unheld.
    Read-only — never modifies the row. Used by both the acquire
    helper (to describe the conflicting holder) and the route's
    page-render path (to surface a banner if a stale lock from a
    crashed worker is still present)."""
    row = conn.execute(
        "SELECT session_id, holder, acquired_at "
        "FROM setup_recovery_lock WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return RecoveryLockState(
            session_id=row["session_id"],
            holder=row["holder"],
            acquired_at=str(row["acquired_at"]),
        )
    # Fallback for tuple-shaped rows (test fixtures sometimes
    # bypass the row_factory).
    return RecoveryLockState(
        session_id=row[0],
        holder=row[1],
        acquired_at=str(row[2]),
    )
