# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Find transactions that are duplicates by content fingerprint.

Two transactions are "the same event" when they match on:
  - date
  - primary (non-FIXME) Assets/Liabilities account
  - absolute amount
  - narration (normalized: lowercased, whitespace-collapsed)
  - payee (when present)

We group by that fingerprint and call any group with >= 2 entries a
duplicate group. Per-entry we capture the SimpleFIN id (for tombstone
& physical removal), the source filename/line (for "where in the
ledger does this live"), and a short preview snippet.

The scanner is pure (no I/O beyond reading the already-loaded
ledger entries). It's safe to call on any /settings page.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction


_WS_RE = re.compile(r"\s+")


def _norm(text: str | None) -> str:
    if not text:
        return ""
    return _WS_RE.sub(" ", text).strip().lower()


def _primary_account(txn: Transaction) -> str | None:
    """First non-FIXME Assets/Liabilities posting — the bank-side leg."""
    for p in txn.postings or ():
        acct = p.account or ""
        if not acct.startswith(("Assets:", "Liabilities:")):
            continue
        if acct.split(":")[-1].upper() == "FIXME":
            continue
        return acct
    return None


def _abs_amount(txn: Transaction) -> Decimal | None:
    """|amount| of the primary posting. Used as part of the fingerprint;
    sign is ignored so inflows and outflows with the same magnitude
    still group together when everything else matches."""
    for p in txn.postings or ():
        if p.units is None or p.units.number is None:
            continue
        return Decimal(p.units.number).copy_abs()
    return None


def _sfid(txn: Transaction) -> str | None:
    from lamella.core.identity import find_source_reference
    ref = find_source_reference(txn, "simplefin")
    return str(ref) if ref else None


@dataclass(frozen=True)
class DuplicateTxn:
    txn_hash: str
    # Immutable UUIDv7 lineage id used for /txn/{id} link-building.
    lamella_txn_id: str | None
    simplefin_id: str | None
    date: str
    amount: Decimal
    primary_account: str
    narration: str
    payee: str | None
    filename: str | None
    lineno: int | None


@dataclass
class DuplicateGroup:
    # Fingerprint (what makes these "the same event"):
    date: str
    amount: Decimal
    primary_account: str
    narration: str
    # Ledger-level rows:
    entries: list[DuplicateTxn] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.entries)

    @property
    def key(self) -> str:
        """Stable id for the group so the UI can reference it."""
        return (
            f"{self.date}|{self.primary_account}|{self.amount:.2f}|"
            f"{self.narration[:40]}"
        )


def scan_duplicates(
    entries: Iterable,
    *,
    require_simplefin_id: bool = True,
) -> list[DuplicateGroup]:
    """Walk ``entries`` once, return all duplicate groups (>= 2).

    ``require_simplefin_id`` = only duplicates where every member has a
    lamella-simplefin-id — that limits cleanup to rows we actually own
    (user-authored transactions stay off-limits). Flip to False if
    you want to detect content duplicates across sources (reboot
    flow). Default True because the cleanup action rewrites
    simplefin_transactions.bean.
    """
    from lamella.core.beancount_io.txn_hash import txn_hash

    buckets: dict[tuple, list[DuplicateTxn]] = {}
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        sfid = _sfid(e)
        if require_simplefin_id and not sfid:
            continue
        primary = _primary_account(e)
        amt = _abs_amount(e)
        if primary is None or amt is None:
            continue
        key = (
            str(e.date),
            primary,
            f"{amt:.2f}",
            _norm(e.narration),
            _norm(getattr(e, "payee", None)),
        )
        meta = getattr(e, "meta", None) or {}
        row = DuplicateTxn(
            txn_hash=txn_hash(e),
            lamella_txn_id=get_txn_id(e),
            simplefin_id=sfid,
            date=str(e.date),
            amount=amt,
            primary_account=primary,
            narration=e.narration or "",
            payee=getattr(e, "payee", None),
            filename=meta.get("filename"),
            lineno=meta.get("lineno"),
        )
        buckets.setdefault(key, []).append(row)

    groups: list[DuplicateGroup] = []
    for key, rows in buckets.items():
        if len(rows) < 2:
            continue
        d, acct, amt_s, narr, _payee = key
        # Stable order: oldest-by-lineno first so "keep" defaults to
        # the original.
        rows.sort(key=lambda r: (r.filename or "", r.lineno or 0))
        groups.append(DuplicateGroup(
            date=d,
            amount=Decimal(amt_s),
            primary_account=acct,
            narration=narr,
            entries=rows,
        ))
    # Stable order for display: most-duplicated first, then newest.
    groups.sort(key=lambda g: (-g.count, g.date), reverse=False)
    groups.sort(key=lambda g: (-g.count, g.date))
    return groups
