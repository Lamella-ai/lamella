# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Refund detection — match a positive bank-side amount on a card or
checking account against a recently-classified expense in the ledger.

A refund is a positive amount on a bank or credit-card account that
reverses a prior expense. Bank-sync's deposit-skip path treats every
positive-on-account row as money-in; this helper lets the deposit panel
offer "refund of <original-merchant>" candidates so the user can re-route
the refund against the original expense's category in one click instead
of picking ``Income:Refunds:*`` and losing the categorical link.

Scoring is additive (additive boolean signals → score ∈ [0, 1]):

    * merchant match              + 0.40
    * amount within 5%            + 0.30
    * amount within 20% (instead) + 0.10
    * posted within window_days
      AND BEFORE refund date      + 0.20
    * same payment source_account + 0.10

Threshold for inclusion: ``score >= 0.50``. Top 5 returned, ranked
descending by score; ties broken by recency (closer to refund_date wins).

Ranking helpers are pure-Python so the function is callable from any
context (tests, web routes, future bulk-classifier passes) without
needing the FastAPI request scope.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from difflib import SequenceMatcher

from beancount.core.data import Transaction

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as _txn_hash
from lamella.core.identity import get_txn_id

log = logging.getLogger(__name__)


# Threshold below which a candidate is dropped. Raise to demand stricter
# matches (fewer false positives, more "no candidate" misses); lower to
# loosen (more candidates surface, more user-side filtering needed).
MIN_SCORE = 0.50

# Maximum candidates returned. The deposit-skip modal renders these
# inline as one-click buttons; more than 5 turns into a list the user
# has to scan. Tune in concert with ``MIN_SCORE``.
MAX_CANDIDATES = 5


@dataclass(frozen=True)
class RefundCandidate:
    """One ranked match between a refund and a candidate original expense.

    ``lamella_txn_id`` is the canonical pointer; ``txn_hash`` is the
    fallback for legacy entries that haven't been on-touch-normalized
    to lineage yet. The classify path prefers ``lamella_txn_id`` when
    present and falls back to ``txn_hash`` only for refund-stamping
    callers that need a pointer for legacy ledger content.
    """

    lamella_txn_id: str
    txn_hash: str
    date: date
    merchant: str | None
    amount: Decimal  # original's bank-side (signed — negative = expense)
    target_account: str  # the original's expense leg account (where to re-route)
    score: float
    match_reasons: list[str] = field(default_factory=list)
    # Display-alias-resolved label for the target_account (ADR-0041 —
    # never render raw colon-separated paths to the user). Populated
    # by the worker that builds the modal context via
    # ``lamella.core.registry.alias.account_label``. Falls back to the
    # raw path if the alias lookup fails or isn't populated.
    target_account_display: str | None = None


def _normalize_merchant(value: str | None) -> str:
    """Lowercase + strip + collapse whitespace. Cheap canonicalisation
    so "ACME.COM*1234" and "acme.com*1234" compare equal without
    over-engineering. The fuzzy ratio handles the rest of the variation
    bank statements introduce ("ACME MKTPLC" vs "ACME.COM*ABC")."""
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def _merchant_match(refund_merchant: str, original_merchant: str) -> bool:
    """True if either string contains the other (substring match) or
    SequenceMatcher.ratio is >= 0.7. Bank statements rarely use exactly
    the same payee text on the original charge and the refund — the
    refund usually has a "REFUND" or "CREDIT" prefix, sometimes the
    chargeback id appended, sometimes the merchant abbreviated. Both
    branches catch the common shapes; the threshold of 0.7 keeps
    name-near-name matches without firing on unrelated merchants.
    """
    if not refund_merchant or not original_merchant:
        return False
    a = _normalize_merchant(refund_merchant)
    b = _normalize_merchant(original_merchant)
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    ratio = SequenceMatcher(None, a, b).ratio()
    return ratio >= 0.7


def _amount_window_score(
    refund_amount: Decimal, original_amount: Decimal,
) -> tuple[float, str | None]:
    """Return (score-contribution, reason-string).

    Compares ``|refund_amount|`` to ``|original_amount|``. The 5% window
    earns +0.30; the 20% window (when 5% misses) earns +0.10. Outside
    20% earns 0 — a $200 refund against a $50 original is almost always
    a different transaction.

    Both inputs are coerced to Decimal so callers can pass plain
    float / int / str without surprise."""
    refund_abs = abs(Decimal(refund_amount))
    original_abs = abs(Decimal(original_amount))
    if refund_abs == 0 or original_abs == 0:
        return 0.0, None
    diff_pct = abs(refund_abs - original_abs) / original_abs
    if diff_pct <= Decimal("0.05"):
        return 0.30, f"amount within 5% (orig {original_abs:.2f})"
    if diff_pct <= Decimal("0.20"):
        return 0.10, f"amount within 20% (orig {original_abs:.2f})"
    return 0.0, None


def _date_window_score(
    refund_date: date, original_date: date, window_days: int,
) -> tuple[float, str | None]:
    """+0.20 only when the original was posted BEFORE the refund and
    within ``window_days``. Future-dated "originals" are nonsensical
    here (you can't refund something not yet charged), so we exclude
    them — protects against a same-day double-charge being mis-matched
    as its own refund."""
    if original_date >= refund_date:
        return 0.0, None
    delta_days = (refund_date - original_date).days
    if 0 < delta_days <= window_days:
        return 0.20, f"posted {delta_days} day{'s' if delta_days != 1 else ''} ago"
    return 0.0, None


def _bank_leg(
    txn: Transaction,
) -> tuple[str | None, Decimal | None]:
    """Return (account, signed_amount) of the bank/card-side leg of a
    classified expense — defined as the negative-amount Assets:* /
    Liabilities:* posting (the money-out leg). For a normal expense:
    Assets:Checking -42.17 / Expenses:Foo +42.17 → returns
    ("Assets:Checking", Decimal("-42.17"))."""
    for p in txn.postings or ():
        acct = p.account or ""
        if not (acct.startswith("Assets:") or acct.startswith("Liabilities:")):
            continue
        if p.units is None or p.units.number is None:
            continue
        amt = Decimal(p.units.number)
        if amt < 0:
            return acct, amt
    return None, None


def _expense_leg(
    txn: Transaction,
) -> tuple[str | None, Decimal | None]:
    """Return (account, signed_amount) of the expense leg — the
    NON-bank, NON-FIXME, positive-amount posting. Skip FIXME because
    a FIXME-bearing txn is unclassified; a refund-against-FIXME match
    is meaningless (we'd be re-routing the refund to FIXME)."""
    for p in txn.postings or ():
        acct = p.account or ""
        if "FIXME" in acct.upper():
            continue
        if acct.startswith("Assets:") or acct.startswith("Liabilities:"):
            continue
        if p.units is None or p.units.number is None:
            continue
        amt = Decimal(p.units.number)
        if amt > 0:
            return acct, amt
    return None, None


def find_refund_candidates(
    conn: sqlite3.Connection,
    reader: LedgerReader,
    *,
    refund_amount: Decimal,
    refund_date: date,
    merchant: str | None,
    narration: str | None,
    source_account: str | None,
    window_days: int = 60,
) -> list[RefundCandidate]:
    """Find recently-classified expense ledger txns that look like the
    original outflow this refund is reversing. Returns up to 5 candidates
    ranked by match score (highest first).

    See module docstring for the scoring rubric. The function is read-only
    against the ledger — it does not mutate any txn. Callers stamp the
    refund-of meta on the refund-side txn at write time using the chosen
    candidate's ``lamella_txn_id``.

    Notes:
      * ``conn`` is accepted for API symmetry with other detector helpers
        and to leave room for future cache-driven candidate pre-filters
        (e.g. recent classifications by entity). Today the candidate set
        comes entirely from walking the ledger.
      * ``narration`` is used as a fallback merchant signal when the
        bank row's payee is None — helpful for paste/CSV imports that
        don't always populate payee.
    """
    refund_amount = Decimal(refund_amount)
    if refund_amount <= 0:
        # Defensive: a refund must be money-in. Calling with a negative
        # amount is a sign of an upstream sign-flip bug; bail loudly.
        return []

    # Source merchant signal — payee preferred, narration as fallback.
    refund_merchant = (merchant or "").strip() or (narration or "").strip()

    candidates: list[RefundCandidate] = []
    try:
        loaded = reader.load()
    except Exception as exc:  # noqa: BLE001
        log.warning("find_refund_candidates: ledger load failed: %s", exc)
        return []

    for entry in loaded.entries:
        if not isinstance(entry, Transaction):
            continue
        # Skip the refund-side txn itself if it's already in the ledger
        # (e.g. user re-opens the modal for an already-refunded row).
        # We can't exactly identify "the refund" here, but we drop any
        # txn dated AFTER the refund — refunds chronologically follow.
        if entry.date >= refund_date:
            # Allow same-day matches only when the original is strictly
            # before — handled by _date_window_score's strict <.
            pass

        # Need an expense leg to be a refund target.
        expense_acct, expense_amt = _expense_leg(entry)
        if expense_acct is None or expense_amt is None:
            continue

        # Need a bank leg to compute amount + source-account match.
        bank_acct, bank_amt = _bank_leg(entry)
        if bank_acct is None or bank_amt is None:
            continue

        score = 0.0
        reasons: list[str] = []

        # Merchant signal.
        original_merchant = (
            getattr(entry, "payee", None) or entry.narration or ""
        )
        if _merchant_match(refund_merchant, original_merchant):
            score += 0.40
            label = original_merchant.strip() or "(no merchant)"
            reasons.append(f"merchant matched '{label[:32]}'")

        # Amount signal (5% / 20% / nothing).
        amt_score, amt_reason = _amount_window_score(
            refund_amount, expense_amt,
        )
        if amt_score > 0:
            score += amt_score
            assert amt_reason is not None
            reasons.append(amt_reason)

        # Date-window signal (+0.20 only when original is strictly before
        # the refund within window_days).
        date_score, date_reason = _date_window_score(
            refund_date, entry.date, window_days,
        )
        if date_score > 0:
            score += date_score
            assert date_reason is not None
            reasons.append(date_reason)

        # Same payment source-account signal.
        if source_account and bank_acct == source_account:
            score += 0.10
            reasons.append("same payment account")

        if score < MIN_SCORE:
            continue

        lineage = get_txn_id(entry)
        ledger_hash = _txn_hash(entry)
        candidates.append(RefundCandidate(
            lamella_txn_id=lineage or ledger_hash,
            txn_hash=ledger_hash,
            date=entry.date,
            merchant=(original_merchant.strip() or None),
            amount=bank_amt,
            target_account=expense_acct,
            score=round(score, 3),
            match_reasons=reasons,
        ))

    # Rank: score desc, then recency (closer to refund_date wins) so
    # ties between two equal-score candidates surface the more recent
    # one — bank statements occasionally repeat charges, the user
    # almost always means the latest one. ``(refund_date - c.date).days``
    # is a positive day-count for any candidate strictly before the
    # refund; a smaller delta means more recent → wins on ascending sort.
    candidates.sort(
        key=lambda c: (
            -c.score,
            (refund_date - c.date).days
            if c.date < refund_date
            else 9999,  # future/same-day candidates rank last
        ),
    )
    return candidates[:MAX_CANDIDATES]
