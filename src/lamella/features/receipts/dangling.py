# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Dangling-receipt-link detection.

A *dangling* link is a row in ``receipt_links`` whose ``paperless_id``
points at a Paperless document that no longer exists. The user might
have deleted it intentionally, in which case Lamella should surface
the dead link so they can clean up.

The danger we have to design around is the OPPOSITE case: a transient
Paperless outage (5xx, network error, restart, DNS hiccup, etc.) must
NOT make Lamella conclude the document is gone and unlink it. A single
404 from one sweep is also not enough evidence — Paperless's bulk
endpoints occasionally drop individual docs from a paged response on
busy systems.

Safety guards (all required for a link to surface as dangling):

1. **Multiple consecutive 404 responses.** Each sweep increments a
   counter; a single 200 OK resets it to 0. The default threshold is
   3 consecutive 404s.
2. **Cooldown period.** ``first_404_at`` is set on the first 404 in
   the current consecutive run; the link only surfaces after that
   timestamp is at least 7 days old. A user who deleted on purpose
   has a week to notice; a transient outage with three 404s in a
   row over 30 minutes is filtered out.
3. **Transport errors are not evidence.** ``httpx`` connect errors,
   timeouts, and 5xx responses leave the row unchanged. Only a
   confirmed 404 (Paperless said "this document does not exist") is
   counted.
4. **No auto-unlink.** Surfacing a link as dangling is informational.
   The /reports/dangling-receipts page lists them with a manual unlink
   button; user click is required. (A future opt-in setting may add
   automatic cleanup, behind a separate gate.)

This module exposes two operations:

- ``sweep_paperless_link_health(conn, client)`` — walk the distinct
  paperless_ids in receipt_links, probe each, update counters.
- ``list_dangling_links(conn, ...)`` — return the rows that have
  crossed the threshold and cooldown, joined with txn_hash + receipt
  context for the report page.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lamella.adapters.paperless.client import PaperlessClient

log = logging.getLogger(__name__)

# A link is *dangling* once we've seen this many 404s in a row.
DEFAULT_CONSECUTIVE_404_THRESHOLD = 3

# ...and only after this much wall-clock time has passed since the
# first 404 in the current run. Defends against rapid transient
# outages that briefly return 404 from individual document fetches.
DEFAULT_COOLDOWN_DAYS = 7


@dataclass(frozen=True)
class SweepResult:
    """Per-sweep telemetry. The route renders these counts to the
    user so they know the sweep ran cleanly."""
    checked: int = 0
    seen: int = 0          # 200 OK responses (link still healthy)
    not_found: int = 0     # confirmed 404 (counter incremented)
    transport_errors: int = 0  # network / 5xx — row left untouched
    crossed_threshold: int = 0  # links that JUST became dangling this sweep


@dataclass(frozen=True)
class DanglingLink:
    """One dangling-link row for the report page."""
    paperless_id: int
    txn_hash: str
    consecutive_404s: int
    first_404_at: str
    last_check_at: str
    # Best-effort tax-relevant context pulled from receipt_links so
    # the user can see what the now-dead link was supposedly attached
    # to.
    txn_amount: str | None = None
    txn_date: str | None = None
    paperless_url: str | None = None


def _is_404(exc: Exception) -> bool:
    """The Paperless adapter raises ``PaperlessError`` with the HTTP
    status in the message string. We don't want to depend on inner
    exception attributes that don't exist; the substring check is
    boring but correct."""
    msg = str(exc).lower()
    return "returned 404" in msg or " 404 " in msg or msg.endswith(" 404")


async def sweep_paperless_link_health(
    conn: sqlite3.Connection,
    client: "PaperlessClient",
) -> SweepResult:
    """Probe every distinct paperless_id in receipt_links. Update
    paperless_link_health counters per the rules in the module
    docstring.

    Idempotent: re-running the sweep on a stable system increments
    counters where docs are gone, resets them where docs are healthy.
    """
    rows = conn.execute(
        "SELECT DISTINCT paperless_id FROM receipt_links "
        "WHERE paperless_id IS NOT NULL"
    ).fetchall()

    checked = 0
    seen = 0
    not_found = 0
    transport_errors = 0
    crossed_threshold = 0

    for row in rows:
        pid = int(row["paperless_id"])
        checked += 1
        try:
            await client.get_document(pid)
        except Exception as exc:  # noqa: BLE001 — branch on type below
            if _is_404(exc):
                not_found += 1
                # Increment consecutive_404s; set first_404_at on the
                # first transition (NULL → not-NULL).
                cur = conn.execute(
                    "SELECT consecutive_404s, first_404_at "
                    "FROM paperless_link_health WHERE paperless_id = ?",
                    (pid,),
                ).fetchone()
                prev_count = int(cur["consecutive_404s"]) if cur else 0
                new_count = prev_count + 1
                if cur is None:
                    conn.execute(
                        "INSERT INTO paperless_link_health "
                        "(paperless_id, last_404_at, first_404_at, "
                        " last_check_at, consecutive_404s) "
                        "VALUES (?, datetime('now'), datetime('now'), "
                        "datetime('now'), 1)",
                        (pid,),
                    )
                else:
                    first_404_at = cur["first_404_at"] or "datetime('now')"
                    if cur["first_404_at"]:
                        conn.execute(
                            "UPDATE paperless_link_health SET "
                            "last_404_at = datetime('now'), "
                            "last_check_at = datetime('now'), "
                            "consecutive_404s = consecutive_404s + 1 "
                            "WHERE paperless_id = ?",
                            (pid,),
                        )
                    else:
                        conn.execute(
                            "UPDATE paperless_link_health SET "
                            "last_404_at = datetime('now'), "
                            "last_check_at = datetime('now'), "
                            "first_404_at = datetime('now'), "
                            "consecutive_404s = 1 "
                            "WHERE paperless_id = ?",
                            (pid,),
                        )
                if (
                    prev_count < DEFAULT_CONSECUTIVE_404_THRESHOLD
                    and new_count >= DEFAULT_CONSECUTIVE_404_THRESHOLD
                ):
                    crossed_threshold += 1
                    log.info(
                        "dangling-link: paperless_id=%d crossed "
                        "consecutive_404s threshold (%d) — will surface "
                        "in report after cooldown.",
                        pid, DEFAULT_CONSECUTIVE_404_THRESHOLD,
                    )
            else:
                # Transport error or 5xx — DO NOT update counters.
                # A network blip must not be evidence of deletion.
                transport_errors += 1
                log.info(
                    "dangling-link: paperless_id=%d sweep skipped "
                    "(transport / 5xx — counter untouched): %s",
                    pid, exc,
                )
            continue
        # 200 OK — reset counter, update last_seen_at.
        seen += 1
        conn.execute(
            "INSERT INTO paperless_link_health "
            "(paperless_id, last_seen_at, last_check_at, "
            " consecutive_404s, first_404_at) "
            "VALUES (?, datetime('now'), datetime('now'), 0, NULL) "
            "ON CONFLICT(paperless_id) DO UPDATE SET "
            "last_seen_at = datetime('now'), "
            "last_check_at = datetime('now'), "
            "consecutive_404s = 0, "
            "first_404_at = NULL",
            (pid,),
        )

    conn.commit()
    return SweepResult(
        checked=checked,
        seen=seen,
        not_found=not_found,
        transport_errors=transport_errors,
        crossed_threshold=crossed_threshold,
    )


def list_dangling_links(
    conn: sqlite3.Connection,
    *,
    threshold: int = DEFAULT_CONSECUTIVE_404_THRESHOLD,
    cooldown_days: int = DEFAULT_COOLDOWN_DAYS,
) -> list[DanglingLink]:
    """Return every receipt_links row whose paperless_id has crossed
    the (consecutive 404 + cooldown) gates. The report page renders
    these for manual cleanup.

    Columns pulled match the actual receipt_links schema (per
    migrations/001_init.sql): id / paperless_id / txn_hash /
    txn_date / txn_amount / match_method / match_confidence /
    linked_at. ``paperless_url`` is constructed by the route from
    ``settings.paperless_url`` + the doc id; not stored on the row.
    """
    rows = conn.execute(
        """
        SELECT plh.paperless_id,
               plh.consecutive_404s,
               plh.first_404_at,
               plh.last_check_at,
               rl.txn_hash,
               rl.txn_amount,
               rl.txn_date
          FROM paperless_link_health plh
          JOIN receipt_links rl
            ON rl.paperless_id = plh.paperless_id
         WHERE plh.consecutive_404s >= ?
           AND plh.first_404_at IS NOT NULL
           AND datetime(plh.first_404_at) <= datetime('now', ?)
         ORDER BY plh.first_404_at ASC
        """,
        (threshold, f"-{cooldown_days} days"),
    ).fetchall()
    return [
        DanglingLink(
            paperless_id=int(r["paperless_id"]),
            txn_hash=str(r["txn_hash"]),
            consecutive_404s=int(r["consecutive_404s"]),
            first_404_at=str(r["first_404_at"]),
            last_check_at=str(r["last_check_at"] or ""),
            txn_amount=str(r["txn_amount"]) if r["txn_amount"] is not None else None,
            txn_date=str(r["txn_date"]) if r["txn_date"] is not None else None,
            paperless_url=None,
        )
        for r in rows
    ]


def link_health_status(conn: sqlite3.Connection) -> dict[str, int]:
    """Quick summary for the report page header. Returns counts of
    healthy / pending-confirmation / dangling links.
    """
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE consecutive_404s = 0 AND last_seen_at IS NOT NULL
            ) AS healthy,
            COUNT(*) FILTER (
                WHERE consecutive_404s BETWEEN 1 AND ?
            ) AS pending,
            COUNT(*) FILTER (
                WHERE consecutive_404s >= ?
                  AND first_404_at IS NOT NULL
                  AND datetime(first_404_at) <= datetime('now', ?)
            ) AS dangling
          FROM paperless_link_health
        """,
        (
            DEFAULT_CONSECUTIVE_404_THRESHOLD - 1,
            DEFAULT_CONSECUTIVE_404_THRESHOLD,
            f"-{DEFAULT_COOLDOWN_DAYS} days",
        ),
    ).fetchone()
    return {
        "healthy": int(row["healthy"] or 0),
        "pending": int(row["pending"] or 0),
        "dangling": int(row["dangling"] or 0),
    }
