# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Back-filled-mileage audit cache.

When the user records mileage for a date that's meaningfully older
than "today" — importing years-old paper logs, catching up on a
sparse vehicle, etc. — any transaction on that date that was AI-
classified *before* the back-fill happened was decided without the
benefit of that mileage context. The decision may still be right,
but it deserves a human look.

This module maintains a per-date cache of back-fill events so a UI
page can render the audit list cheaply without rescanning the full
mileage log. The cache is pure projection of ``mileage_entries``
columns (``entry_date`` vs ``created_at``) — wipe it, call
``rebuild_mileage_backfill_audit``, get an identical table.

The "is this a back-fill?" threshold is ``BACKFILL_THRESHOLD_DAYS``
(default 2). Same-day or next-day data entry doesn't count — users
who log yesterday's trips this morning aren't back-filling, they're
just a day behind.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date

log = logging.getLogger(__name__)

BACKFILL_THRESHOLD_DAYS = 2


@dataclass(frozen=True)
class BackfillAuditRow:
    """One date that had back-filled mileage."""
    entry_date: date
    backfill_latest_at: str
    backfill_entry_count: int
    gap_days_max: int


def rebuild_mileage_backfill_audit(
    conn: sqlite3.Connection,
    *,
    threshold_days: int = BACKFILL_THRESHOLD_DAYS,
) -> int:
    """Wipe + repopulate ``mileage_backfill_audit`` from
    ``mileage_entries``. Returns the number of rows written.

    Idempotent. Safe to call at startup, from reconstruct, or after
    any bulk mileage write (though incremental ``record_backfill``
    is cheaper for single-row writes).
    """
    try:
        conn.execute("DELETE FROM mileage_backfill_audit")
    except sqlite3.OperationalError:
        # Migration 042 not applied yet; non-fatal.
        log.debug("mileage_backfill_audit table missing; skipping rebuild")
        return 0

    # Aggregate back-filled rows (gap >= threshold) by entry_date in
    # a single pass. julianday() returns a real number of days; we
    # cast to int which truncates toward zero — a row logged exactly
    # 2 days late gives gap=2, at the threshold. Rows logged same
    # day or next day are excluded.
    cursor = conn.execute(
        """
        INSERT INTO mileage_backfill_audit
            (entry_date, backfill_latest_at, backfill_entry_count,
             gap_days_max, refreshed_at)
        SELECT entry_date,
               MAX(created_at),
               COUNT(*),
               CAST(MAX(julianday(created_at) - julianday(entry_date))
                    AS INTEGER),
               datetime('now')
          FROM mileage_entries
         WHERE created_at IS NOT NULL
           AND (julianday(created_at) - julianday(entry_date)) >= ?
         GROUP BY entry_date
        """,
        (int(threshold_days),),
    )
    return int(cursor.rowcount or 0)


def record_backfill(
    conn: sqlite3.Connection,
    *,
    entry_date: date | str,
    created_at: str | None = None,
    threshold_days: int = BACKFILL_THRESHOLD_DAYS,
) -> bool:
    """Incremental maintenance — called after a single mileage insert.

    If the row counts as a back-fill (gap >= threshold_days) based on
    the DB's stored ``created_at`` vs ``entry_date``, upsert into
    ``mileage_backfill_audit``. Returns True iff the audit table was
    touched.

    ``created_at`` may be passed through when the caller already has
    it; when None, the function reads the row back from
    ``mileage_entries`` using ``entry_date`` to compute the
    aggregate. This keeps the one-SQL-per-write cost low.
    """
    iso = entry_date if isinstance(entry_date, str) else entry_date.isoformat()
    try:
        # Re-aggregate JUST this entry_date — cheaper than a full
        # rebuild, still fully correct because the aggregate is a
        # function of the rows, not of prior audit state.
        cursor = conn.execute(
            """
            INSERT OR REPLACE INTO mileage_backfill_audit
                (entry_date, backfill_latest_at, backfill_entry_count,
                 gap_days_max, refreshed_at)
            SELECT entry_date,
                   MAX(created_at),
                   COUNT(*),
                   CAST(MAX(julianday(created_at) - julianday(entry_date))
                        AS INTEGER),
                   datetime('now')
              FROM mileage_entries
             WHERE entry_date = ?
               AND created_at IS NOT NULL
               AND (julianday(created_at) - julianday(entry_date)) >= ?
             GROUP BY entry_date
            """,
            (iso, int(threshold_days)),
        )
        wrote = (cursor.rowcount or 0) > 0
        if not wrote:
            # No back-filled rows remain for this date; purge any
            # stale audit row. Happens when the only back-filled row
            # on a date was just deleted, or when a same-day edit
            # updated created_at to pull it below the threshold.
            conn.execute(
                "DELETE FROM mileage_backfill_audit WHERE entry_date = ?",
                (iso,),
            )
        return bool(wrote)
    except sqlite3.OperationalError:
        log.debug("mileage_backfill_audit table missing; skipping record")
        return False


def list_backfill_dates(
    conn: sqlite3.Connection,
    *,
    limit: int = 500,
) -> list[BackfillAuditRow]:
    """Return all known back-fill dates, most-recently-back-filled
    first. Caller renders the audit page from this list."""
    try:
        rows = conn.execute(
            """
            SELECT entry_date, backfill_latest_at,
                   backfill_entry_count, gap_days_max
              FROM mileage_backfill_audit
          ORDER BY backfill_latest_at DESC, entry_date DESC
             LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    out: list[BackfillAuditRow] = []
    for r in rows:
        try:
            d = date.fromisoformat(str(r["entry_date"])[:10])
        except ValueError:
            continue
        out.append(
            BackfillAuditRow(
                entry_date=d,
                backfill_latest_at=str(r["backfill_latest_at"]),
                backfill_entry_count=int(r["backfill_entry_count"] or 0),
                gap_days_max=int(r["gap_days_max"] or 0),
            )
        )
    return out
