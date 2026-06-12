# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Balance-anchor CRUD + segment drift computation.

Segment drift is the audit surface the user cares about:

    For each pair of consecutive anchors on the same account, compute
    (next.balance − prev.balance) vs (sum of postings to this account
    dated in (prev.date, next.date]). The difference is the
    unexplained movement. Non-zero drift = a reconciliation bug in
    the ledger for that segment.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date as date_t, datetime
from decimal import Decimal
from typing import Any, Iterable

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AnchorRow:
    id: int
    account_path: str
    as_of_date: date_t
    balance: Decimal
    currency: str
    source: str | None
    notes: str | None


@dataclass(frozen=True)
class Segment:
    """One segment between two consecutive anchors."""
    start_anchor: AnchorRow
    end_anchor: AnchorRow
    asserted_delta: Decimal       # end − start
    ledger_delta: Decimal         # sum of postings in (start_date, end_date]
    drift: Decimal                # asserted − ledger (0 = reconciled)
    posting_count: int


@dataclass(frozen=True)
class AccountAudit:
    account_path: str
    currency: str
    anchors: list[AnchorRow]
    segments: list[Segment]
    # Pre-anchor activity: sum of postings before the earliest anchor,
    # to indicate how much history predates the first known balance.
    pre_first_postings_count: int
    pre_first_postings_sum: Decimal


def _to_decimal(s: Any) -> Decimal:
    if s is None or s == "":
        return Decimal("0")
    if isinstance(s, Decimal):
        return s
    try:
        return Decimal(str(s).replace(",", "").replace("$", "").strip())
    except Exception:  # noqa: BLE001
        return Decimal("0")


def _to_date(s: Any) -> date_t:
    if isinstance(s, date_t) and not isinstance(s, datetime):
        return s
    if isinstance(s, datetime):
        return s.date()
    return date_t.fromisoformat(str(s)[:10])


def list_anchors(
    conn: sqlite3.Connection, account_path: str,
) -> list[AnchorRow]:
    rows = conn.execute(
        "SELECT id, account_path, as_of_date, balance, currency, source, notes "
        "FROM account_balance_anchors WHERE account_path = ? "
        "ORDER BY as_of_date ASC, id ASC",
        (account_path,),
    ).fetchall()
    return [
        AnchorRow(
            id=int(r["id"]),
            account_path=r["account_path"],
            as_of_date=_to_date(r["as_of_date"]),
            balance=_to_decimal(r["balance"]),
            currency=(r["currency"] or "USD"),
            source=r["source"],
            notes=r["notes"],
        )
        for r in rows
    ]


def list_all_anchors(
    conn: sqlite3.Connection,
) -> list[AnchorRow]:
    rows = conn.execute(
        "SELECT id, account_path, as_of_date, balance, currency, source, notes "
        "FROM account_balance_anchors ORDER BY account_path, as_of_date ASC"
    ).fetchall()
    return [
        AnchorRow(
            id=int(r["id"]),
            account_path=r["account_path"],
            as_of_date=_to_date(r["as_of_date"]),
            balance=_to_decimal(r["balance"]),
            currency=(r["currency"] or "USD"),
            source=r["source"],
            notes=r["notes"],
        )
        for r in rows
    ]


def upsert_anchor(
    conn: sqlite3.Connection,
    *,
    account_path: str,
    as_of_date: str | date_t,
    balance: str,
    currency: str = "USD",
    source: str | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO account_balance_anchors
            (account_path, as_of_date, balance, currency, source, notes)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (account_path, as_of_date) DO UPDATE SET
            balance  = excluded.balance,
            currency = excluded.currency,
            source   = excluded.source,
            notes    = excluded.notes
        """,
        (
            account_path,
            as_of_date.isoformat() if isinstance(as_of_date, date_t) else str(as_of_date),
            str(balance), currency, source, notes,
        ),
    )


def delete_anchor(conn: sqlite3.Connection, anchor_id: int) -> tuple[str, str] | None:
    """Delete by id. Returns (account_path, as_of_date) so callers can
    emit the revoke directive. Returns None if the row didn't exist."""
    row = conn.execute(
        "SELECT account_path, as_of_date FROM account_balance_anchors WHERE id = ?",
        (anchor_id,),
    ).fetchone()
    if row is None:
        return None
    conn.execute("DELETE FROM account_balance_anchors WHERE id = ?", (anchor_id,))
    return (row["account_path"], row["as_of_date"])


def compute_account_audit(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
    account_path: str,
) -> AccountAudit:
    """Main audit entry point. Returns per-segment drift for the given
    account, plus a pre-first-anchor summary so the user can see how
    much history predates their earliest known balance."""
    from beancount.core.data import Transaction

    anchors = list_anchors(conn, account_path)

    # Collect postings on account_path with (date, decimal-amount).
    postings_by_date: list[tuple[date_t, Decimal]] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        for p in e.postings:
            if p.account != account_path:
                continue
            if p.units is None or p.units.number is None:
                continue
            postings_by_date.append((e.date, Decimal(p.units.number)))

    # Sort once so segment sums can use a two-pointer walk.
    postings_by_date.sort(key=lambda t: t[0])

    if not anchors:
        return AccountAudit(
            account_path=account_path,
            currency="USD",
            anchors=[],
            segments=[],
            pre_first_postings_count=0,
            pre_first_postings_sum=Decimal("0"),
        )

    currency = anchors[0].currency

    # Pre-first postings: everything dated strictly before the earliest
    # anchor (so the anchor represents a snapshot that includes those).
    first_date = anchors[0].as_of_date
    pre_count = 0
    pre_sum = Decimal("0")
    for d, amt in postings_by_date:
        if d < first_date:
            pre_count += 1
            pre_sum += amt
        else:
            break

    # Segments.
    segments: list[Segment] = []
    for i in range(len(anchors) - 1):
        prev = anchors[i]
        nxt = anchors[i + 1]
        asserted = nxt.balance - prev.balance
        ledger = Decimal("0")
        count = 0
        for d, amt in postings_by_date:
            # Half-open: (prev.date, nxt.date] so a posting on the
            # anchor date counts toward reaching that anchor.
            if prev.as_of_date < d <= nxt.as_of_date:
                ledger += amt
                count += 1
            elif d > nxt.as_of_date:
                break
        drift = asserted - ledger
        segments.append(Segment(
            start_anchor=prev,
            end_anchor=nxt,
            asserted_delta=asserted,
            ledger_delta=ledger,
            drift=drift,
            posting_count=count,
        ))

    return AccountAudit(
        account_path=account_path,
        currency=currency,
        anchors=anchors,
        segments=segments,
        pre_first_postings_count=pre_count,
        pre_first_postings_sum=pre_sum,
    )


def compute_portfolio_audit(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
) -> list[AccountAudit]:
    """Run compute_account_audit for every account that has at least
    one anchor. Used on /reports/balance-audit."""
    paths = [
        r["account_path"] for r in conn.execute(
            "SELECT DISTINCT account_path FROM account_balance_anchors "
            "ORDER BY account_path"
        ).fetchall()
    ]
    entries_list = list(entries)  # single scan per call
    return [compute_account_audit(conn, entries_list, p) for p in paths]
