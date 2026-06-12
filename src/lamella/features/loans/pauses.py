# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Loan pause (forbearance) service (WP12).

Wraps the ledger writer + SQLite cache so callers (route handlers,
tests) get one entry point per operation. The ledger directive is the
source of truth; SQLite is a cache reconstruct rebuilds via
``read_loan_pauses``. Each public method writes the ledger first and
the cache second, so a bean-check rejection on the ledger never
leaves SQLite ahead.

Three operations:

- ``create_pause(...)`` — open or closed pause (end_date optional).
- ``end_pause(pause_id, end_date)`` — set an end_date on an open
  pause. Internally re-writes the directive with the new end so the
  ledger reflects current state; the original directive is left in
  place (last-seen-wins per `read_loan_pauses`).
- ``delete_pause(pause_id)`` — tombstones in the ledger via
  ``loan-pause-revoked`` and removes the SQLite row.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from lamella.features.loans.writer import (
    append_loan_pause,
    append_loan_pause_revoked,
)
from lamella.core.ledger_writer import BeanCheckError

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pause:
    id: int
    loan_slug: str
    start_date: date
    end_date: date | None
    reason: str | None
    notes: str | None
    accrued_interest: Decimal | None


class PauseError(Exception):
    """Surface-level error for HTTP route translation. Message is safe
    to show to the user; an HTTP status hint is attached."""
    def __init__(self, message: str, *, status: int = 400):
        super().__init__(message)
        self.status = status


# --------------------------------------------------------------------- helpers


def _to_date(v: Any) -> date | None:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


def _row_to_pause(row: sqlite3.Row | dict) -> Pause:
    d = dict(row)
    return Pause(
        id=int(d["id"]),
        loan_slug=d["loan_slug"],
        start_date=_to_date(d["start_date"]) or date.min,
        end_date=_to_date(d.get("end_date")),
        reason=d.get("reason"),
        notes=d.get("notes"),
        accrued_interest=(
            Decimal(d["accrued_interest"])
            if d.get("accrued_interest") else None
        ),
    )


# --------------------------------------------------------------------- read


def list_pauses(conn: sqlite3.Connection, loan_slug: str) -> list[Pause]:
    """All pauses for a loan, ordered by start_date ascending."""
    rows = conn.execute(
        "SELECT * FROM loan_pauses "
        "WHERE loan_slug = ? "
        "ORDER BY start_date ASC",
        (loan_slug,),
    ).fetchall()
    return [_row_to_pause(r) for r in rows]


def get_pause(conn: sqlite3.Connection, pause_id: int) -> Pause | None:
    row = conn.execute(
        "SELECT * FROM loan_pauses WHERE id = ?", (pause_id,),
    ).fetchone()
    return _row_to_pause(row) if row else None


# --------------------------------------------------------------------- write


def create_pause(
    conn: sqlite3.Connection,
    *,
    settings: Any,
    loan_slug: str,
    start_date: date,
    end_date: date | None = None,
    reason: str | None = None,
    notes: str | None = None,
    accrued_interest: Decimal | None = None,
) -> Pause:
    """Write the ledger directive + SQLite cache row.

    Raises PauseError on (a) UNIQUE collision (loan_slug, start_date)
    or (b) bean-check rejection of the ledger write.
    """
    # SQLite-side pre-check: UNIQUE collision is much easier to
    # report at this layer than after the ledger write.
    existing = conn.execute(
        "SELECT id FROM loan_pauses "
        "WHERE loan_slug = ? AND start_date = ?",
        (loan_slug, start_date.isoformat()),
    ).fetchone()
    if existing is not None:
        raise PauseError(
            f"a pause already exists for {loan_slug} starting "
            f"{start_date.isoformat()}; end or delete it before "
            f"creating a new one.",
            status=409,
        )

    try:
        append_loan_pause(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            loan_slug=loan_slug,
            start_date=start_date,
            end_date=end_date,
            reason=reason,
            notes=notes,
            accrued_interest=(
                str(accrued_interest) if accrued_interest is not None else None
            ),
        )
    except BeanCheckError as exc:
        raise PauseError(
            f"bean-check rejected the pause directive: {exc}",
            status=500,
        ) from exc

    conn.execute(
        """
        INSERT INTO loan_pauses
            (loan_slug, start_date, end_date, reason, notes, accrued_interest)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            loan_slug,
            start_date.isoformat(),
            end_date.isoformat() if end_date else None,
            reason, notes,
            str(accrued_interest) if accrued_interest is not None else None,
        ),
    )
    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    pause = get_pause(conn, int(new_id))
    assert pause is not None
    return pause


def end_pause(
    conn: sqlite3.Connection,
    *,
    settings: Any,
    pause_id: int,
    end_date: date,
) -> Pause:
    """Set ``end_date`` on an existing pause.

    Implementation: re-write the original directive with the new
    lamella-pause-end. ``read_loan_pauses`` keeps last-seen-wins per
    (slug, start_date), so the new directive supersedes the prior
    one without us needing to revoke it.
    """
    pause = get_pause(conn, pause_id)
    if pause is None:
        raise PauseError(f"pause {pause_id} not found", status=404)
    if pause.end_date is not None:
        raise PauseError(
            f"pause {pause_id} already ended on {pause.end_date.isoformat()}",
            status=409,
        )
    if end_date < pause.start_date:
        raise PauseError(
            f"end_date {end_date.isoformat()} is before start_date "
            f"{pause.start_date.isoformat()}",
            status=400,
        )
    try:
        append_loan_pause(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            loan_slug=pause.loan_slug,
            start_date=pause.start_date,
            end_date=end_date,
            reason=pause.reason,
            notes=pause.notes,
            accrued_interest=(
                str(pause.accrued_interest)
                if pause.accrued_interest is not None else None
            ),
        )
    except BeanCheckError as exc:
        raise PauseError(
            f"bean-check rejected the pause-end rewrite: {exc}",
            status=500,
        ) from exc

    conn.execute(
        "UPDATE loan_pauses SET end_date = ? WHERE id = ?",
        (end_date.isoformat(), pause_id),
    )
    updated = get_pause(conn, pause_id)
    assert updated is not None
    return updated


def delete_pause(
    conn: sqlite3.Connection,
    *,
    settings: Any,
    pause_id: int,
) -> None:
    """Tombstone the pause in the ledger and remove the SQLite row.

    The tombstone directive carries the original pause's start_date
    in ``lamella-pause-start`` so reconstruct knows which historic pause
    the revoke applies to.
    """
    pause = get_pause(conn, pause_id)
    if pause is None:
        raise PauseError(f"pause {pause_id} not found", status=404)
    try:
        append_loan_pause_revoked(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            loan_slug=pause.loan_slug,
            pause_start=pause.start_date,
        )
    except BeanCheckError as exc:
        raise PauseError(
            f"bean-check rejected the pause tombstone: {exc}",
            status=500,
        ) from exc

    conn.execute("DELETE FROM loan_pauses WHERE id = ?", (pause_id,))
