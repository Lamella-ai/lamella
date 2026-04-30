# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash


@dataclass(frozen=True)
class MatchCandidate:
    txn: Transaction
    txn_hash: str
    amount: Decimal
    date: date
    day_delta: int  # abs(txn.date - receipt_date) in days


# Every account root that can carry a receipt-relevant amount.
# Historically named `_EXPENSE_ROOTS` — kept Expenses/Assets/Liabilities
# only, which meant the matcher silently refused to link receipts to
# Income attributions (ATM deposit slips), Equity moves (owner
# reimbursements), or any txn without an Expenses leg. Widened for
# AI-AGENT.md Phase 2. The matcher is amount-based regardless of root
# now; the noise gate lives upstream in `needs_queue._is_non_receipt`.
_RECEIPT_TARGET_ROOTS = (
    "Expenses", "Income", "Liabilities", "Equity", "Assets",
)


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _coerce_amount(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _txn_signed_totals(txn: Transaction) -> list[Decimal]:
    """Amounts that could plausibly represent a receipt total.

    A receipt represents a purchase: most of the time that's a positive
    Expenses posting. For credits against a card (Liabilities) the same
    total may appear negative on Assets/Liabilities. Return the absolute
    value of every USD posting as a candidate — matching is amount-based,
    sign is handled by abs().
    """
    totals: list[Decimal] = []
    for posting in txn.postings:
        units = posting.units
        if units is None or units.number is None:
            continue
        if units.currency and units.currency != "USD":
            continue
        root = posting.account.split(":", 1)[0]
        if root not in _RECEIPT_TARGET_ROOTS:
            continue
        totals.append(Decimal(units.number).copy_abs())
    return totals


def _posting_has_last_four(txn: Transaction, last_four: str) -> bool:
    target = last_four.strip()
    if not target:
        return True
    for posting in txn.postings:
        account = posting.account or ""
        if target in account:
            return True
        meta = posting.meta or {}
        for value in meta.values():
            if isinstance(value, str) and target in value:
                return True
    meta = txn.meta or {}
    for value in meta.values():
        if isinstance(value, str) and target in value:
            return True
    return False


def find_candidates(
    entries: Iterable,
    *,
    receipt_total: Decimal | float | str | None,
    receipt_date: date | str | None,
    last_four: str | None = None,
    date_window_days: int = 1,
) -> list[MatchCandidate]:
    """Return transactions whose amount matches `receipt_total` and whose
    date falls within ±`date_window_days` of `receipt_date`. If `last_four`
    is provided, results are filtered to transactions that reference it.
    Sorted by |day_delta|, then by narration for determinism."""
    total = _coerce_amount(receipt_total)
    rdate = _coerce_date(receipt_date)
    if total is None or rdate is None:
        return []

    window = timedelta(days=max(0, date_window_days))
    lo = rdate - window
    hi = rdate + window

    matches: list[MatchCandidate] = []
    seen_hashes: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < lo or entry.date > hi:
            continue
        totals = _txn_signed_totals(entry)
        if not any(t == total.copy_abs() for t in totals):
            continue
        if last_four and not _posting_has_last_four(entry, last_four):
            continue
        h = txn_hash(entry)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        matches.append(
            MatchCandidate(
                txn=entry,
                txn_hash=h,
                amount=total.copy_abs(),
                date=entry.date,
                day_delta=abs((entry.date - rdate).days),
            )
        )
    matches.sort(key=lambda m: (m.day_delta, m.txn.narration or ""))
    return matches
