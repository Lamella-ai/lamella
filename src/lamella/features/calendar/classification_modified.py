# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cache of per-txn override-write timestamps.

The ledger's override blocks in `connector_overrides.bean` are the
authoritative source for "when was this txn last reclassified": each
block carries a `lamella-modified-at` metadata key stamped by
`OverrideWriter.append*()`. This module maintains a SQLite cache
(`txn_classification_modified`) so the calendar's dirty-check query
is a single indexed join rather than an in-memory ledger walk.

The cache rule: `bump()` at every successful override write (called
by `OverrideWriter` when constructed with a connection), and
`rebuild_from_entries()` from boot-time and reconstruct paths.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, time
from typing import Any, Iterable

log = logging.getLogger(__name__)


def bump(
    conn: sqlite3.Connection,
    *,
    txn_hash: str,
    txn_date: date,
    modified_at: datetime,
) -> None:
    """Upsert one cache row. Keeps the max modified_at seen.

    Using REPLACE (not INSERT OR IGNORE) is deliberate — the row
    represents 'most recent override write for this hash', so a
    re-override must update, not be skipped. An override-then-undo
    flow (not yet hooked in) would call a different helper to
    drop the row.
    """
    iso = modified_at.isoformat(sep=" ", timespec="seconds")
    conn.execute(
        """
        INSERT INTO txn_classification_modified (txn_hash, txn_date, modified_at)
        VALUES (?, ?, ?)
        ON CONFLICT(txn_hash) DO UPDATE SET
            txn_date = excluded.txn_date,
            modified_at = MAX(modified_at, excluded.modified_at)
        """,
        (txn_hash, txn_date.isoformat(), iso),
    )


def rebuild_from_entries(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
    *,
    tz_for_fallback,
) -> int:
    """Rebuild the cache from override Transaction entries.

    For each Transaction carrying `lamella-override-of` metadata:
      * If `lamella-modified-at` is present, parse it as the modified_at.
      * Otherwise fall back to the transaction's own date at local
        midnight under `tz_for_fallback` (a `ZoneInfo`). This
        backfill default is deliberately in the past — it must not
        flip any freshly-reviewed day to dirty on feature first-ship.

    Returns the number of rows upserted. Does not wipe rows for
    override blocks that no longer exist — callers that want a true
    rebuild (reconstruct) wipe beforehand.
    """
    from beancount.core.data import Transaction

    written = 0
    # Track per-hash the best (max) modified_at we've seen so
    # successive writes for the same hash converge on the latest.
    best: dict[str, tuple[date, datetime]] = {}

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        meta = getattr(entry, "meta", None) or {}
        txn_hash = meta.get("lamella-override-of")
        if not txn_hash or not isinstance(txn_hash, str):
            continue

        raw_ts = meta.get("lamella-modified-at")
        ts: datetime | None = None
        if isinstance(raw_ts, datetime):
            ts = raw_ts
        elif isinstance(raw_ts, str) and raw_ts.strip():
            try:
                ts = datetime.fromisoformat(raw_ts.strip().replace("Z", "+00:00"))
            except ValueError:
                ts = None

        if ts is None:
            # Fallback: entry.date at midnight in the configured tz.
            # `datetime.combine` with a tz makes this an aware datetime
            # whose wall-clock equivalent is "the morning of txn_date".
            ts = datetime.combine(entry.date, time(0, 0), tzinfo=tz_for_fallback)

        prev = best.get(txn_hash)
        if prev is None or ts > prev[1]:
            best[txn_hash] = (entry.date, ts)

    for txn_hash, (txn_date, ts) in best.items():
        bump(conn, txn_hash=txn_hash, txn_date=txn_date, modified_at=ts)
        written += 1
    return written
