# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Detect transfer pairs among FIXME transactions.

**Retirement status (NEXTGEN Phase C2c).** This module is the
legacy per-ledger FIXME pair detector. The cross-source
transfer matcher in ``staging/matcher.py`` is the canonical
path for new data — SimpleFIN ingest no longer emits FIXME
entries (defer-FIXME default, shipped with B2 full swing), so
this detector is only invoked against pre-existing FIXMEs still
present in the ledger from before that change.

Kept in use by ``/review`` and ``/card`` for those legacy
FIXMEs. Full retirement (delete this module) requires:
  * A successful reboot pass that rewrites every ledger FIXME
    to its categorized target via the Phase E2/E3 reboot writer.
  * Migration of any remaining ``detect_pairs(entries)`` callers
    to a ledger-aware variant of the unified matcher.

Do NOT add new callers. New pair-detection work should flow
through ``staging/matcher.py``.

The SimpleFIN importer historically wrote one transaction per
bank-feed line. A transfer between two of your own accounts
therefore shows up as TWO transactions, each with
``Expenses:FIXME`` on the unknown side. This module finds those
pairs so the legacy review UI can offer a single "Mark as
transfer" action.

Pair signal, in order of preference:
  1. Both narrations contain the same reference number (e.g.
     "REF #IB0XP2PX8W"). High confidence.
  2. Same absolute amount, opposite-sign FIXME postings, dated
     within ±1 day, different non-FIXME accounts. Weaker but
     catches most PayPal / internal transfers without REFs.

A pair implies each transaction's FIXME amount should route to
the OTHER half's non-FIXME account (money came from A → ended
up in B, so A's FIXME → Assets:B, and B's FIXME → Assets:A).
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.features.rules.scanner import _is_fixme


_REF_RE = re.compile(r"(?:REF\s*#?\s*|#\s*)([A-Z0-9]{6,20})", re.IGNORECASE)


@dataclass(frozen=True)
class PairInfo:
    partner_hash: str
    partner_account: str       # the non-FIXME account on the partner's side
    ref: str | None
    strength: str              # "ref" | "amount_date"


def _extract_ref(text: str) -> str | None:
    if not text:
        return None
    m = _REF_RE.search(text)
    return m.group(1).upper() if m else None


def _non_fixme_account(txn: Transaction) -> str | None:
    """Prefer Assets/Liabilities accounts as the transfer counterpart. We
    skip Income / Equity / Expenses because a transfer should move money
    between balance-sheet accounts."""
    for p in txn.postings:
        acct = p.account or ""
        if _is_fixme(acct):
            continue
        if acct.startswith(("Assets:", "Liabilities:")):
            return acct
    return None


def _fixme_amount_signed(txn: Transaction) -> Decimal | None:
    for p in txn.postings:
        if _is_fixme(p.account) and p.units and p.units.number is not None:
            return Decimal(p.units.number)
    return None


@dataclass(frozen=True)
class _Candidate:
    hash: str
    date: date
    amount_signed: Decimal
    non_fixme: str
    ref: str | None


def detect_pairs(entries: Iterable) -> dict[str, PairInfo]:
    """Return a mapping `{txn_hash: PairInfo}` for every FIXME transaction
    that has a plausible transfer partner among the other FIXMEs.

    Paired transactions appear TWICE in the result (once under each half's
    hash), which lets the route handler pick the "primary" for rendering
    and skip the other.
    """
    candidates: list[_Candidate] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if not any(_is_fixme(p.account) for p in entry.postings):
            continue
        non_fixme = _non_fixme_account(entry)
        if non_fixme is None:
            continue
        amt = _fixme_amount_signed(entry)
        if amt is None:
            continue
        ref = _extract_ref(entry.narration or "")
        candidates.append(
            _Candidate(
                hash=txn_hash(entry),
                date=entry.date,
                amount_signed=amt,
                non_fixme=non_fixme,
                ref=ref,
            )
        )

    pairs: dict[str, PairInfo] = {}
    used: set[str] = set()

    # Pass 1: by shared REF number.
    by_ref: dict[str, list[_Candidate]] = defaultdict(list)
    for c in candidates:
        if c.ref:
            by_ref[c.ref].append(c)
    for ref, group in by_ref.items():
        if len(group) != 2:
            continue
        a, b = group
        if a.amount_signed + b.amount_signed != Decimal("0"):
            continue
        if a.non_fixme == b.non_fixme:
            continue
        pairs[a.hash] = PairInfo(partner_hash=b.hash, partner_account=b.non_fixme, ref=ref, strength="ref")
        pairs[b.hash] = PairInfo(partner_hash=a.hash, partner_account=a.non_fixme, ref=ref, strength="ref")
        used.add(a.hash)
        used.add(b.hash)

    # Pass 2: by matching absolute amount + same/adjacent date.
    remaining = [c for c in candidates if c.hash not in used]
    by_abs: dict[Decimal, list[_Candidate]] = defaultdict(list)
    for c in remaining:
        by_abs[abs(c.amount_signed)].append(c)
    for amt, group in by_abs.items():
        if len(group) < 2:
            continue
        for i, a in enumerate(group):
            if a.hash in used:
                continue
            for b in group[i + 1:]:
                if b.hash in used:
                    continue
                if a.amount_signed + b.amount_signed != Decimal("0"):
                    continue
                if abs((a.date - b.date).days) > 1:
                    continue
                if a.non_fixme == b.non_fixme:
                    continue
                pairs[a.hash] = PairInfo(
                    partner_hash=b.hash, partner_account=b.non_fixme,
                    ref=None, strength="amount_date",
                )
                pairs[b.hash] = PairInfo(
                    partner_hash=a.hash, partner_account=a.non_fixme,
                    ref=None, strength="amount_date",
                )
                used.add(a.hash)
                used.add(b.hash)
                break

    return pairs
