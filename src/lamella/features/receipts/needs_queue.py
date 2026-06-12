# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Build the 'Expenses needing finalization' queue.

Rule: a transaction is in scope iff it has at least one Expenses:* posting,
isn't already linked to a receipt (via document_links), and isn't marked in
document_dismissals as "no receipt expected".

Non-receipt narration patterns (transfers, fees, interest, cashback,
credit-card payments, ATM) are excluded up front — those never have
receipts even when fully categorized.

Each queued item surfaces the full transaction context so the UI can render
date, amount, payee, from→to accounts, IRS badge (required if ≥ threshold
USD, else preferred), and eventually the paperless candidates.
"""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash


# Case-insensitive substring patterns. A match on narration OR payee removes
# the transaction from the queue entirely. Covers the common bank-feed junk
# that never has receipts: account-to-account transfers, credit-card payments,
# fees, interest, rewards, cash movement. Designed to be forgiving — false
# positives here are dismissals the user can't easily reverse. Tune via
# config (future work) if legit expenses get swept up.
NON_RECEIPT_PATTERNS: tuple[str, ...] = (
    # Transfers (both sides of self-transfers)
    "online transfer",
    "transfer ref",
    "wire transfer",
    "paypal transfer",
    "transfer in",
    "transfer out",
    # Credit-card / loan payments (both sides)
    "online payment thank you",
    "online payment, thank you",
    "payment thank you",
    "payment authorized",
    "card online payment",
    "credit crd epay",
    "card epay",
    "chase credit crd",
    "citi card online payment",
    "american express ach pmt",
    "mobile payment",
    "automatic daily payment",
    "daily spending",
    "credit account payment",
    # Brokerage / intra-institution moves
    "moneyline",
    "fid bkg svc",
    # Bank fees, interest, rewards (never receipted)
    "monthly service fee",
    "annual fee",
    "interest charge",
    "interest charged",
    "interest payment",
    "finance charge",
    "currency conv fee",
    "currency conversion fee",
    "atm access fee",
    "rewards",
    "cashback",
    "cash back",
    # Cash movement (no merchant receipt)
    "atm withdrawal",
    "atm cash deposit",
    "cash withdrawal",
    "cash ewithdrawal",
    "mobile deposit",
    "edeposit",
    "cash deposit",
    "online advance",
    "cash advance",
    # Statement / adjustment lines
    "last statement bal from",
    "statement balance",
    "currency conv fee adjust",
)


_NON_RECEIPT_RE = re.compile(
    "|".join(re.escape(p) for p in NON_RECEIPT_PATTERNS),
    re.IGNORECASE,
)


def _is_non_receipt(narration: str, payee: str | None) -> bool:
    """True if this transaction's narration/payee matches a known pattern
    that never has a receipt (transfers, fees, etc.)."""
    text = " ".join(filter(None, (narration or "", payee or ""))).strip()
    if not text:
        return False
    return bool(_NON_RECEIPT_RE.search(text))


@dataclass(frozen=True)
class NeedsDocumentItem:
    txn_hash: str
    # Immutable UUIDv7 lineage id; UI link-builders must use this for
    # /txn/{id} so the URL survives ledger edits that change the
    # content-hash. ``txn_hash`` is retained because POSTs to
    # /receipts/needed/{txn_hash}/link still key off the content hash
    # (matches the document_links / document_dismissals schema).
    lamella_txn_id: str | None
    txn_date: date
    payee: str | None
    narration: str
    max_expense_amount: Decimal
    currency: str
    expense_accounts: tuple[str, ...]
    other_accounts: tuple[str, ...]
    last_four: str | None
    is_fixme: bool
    filename: str | None
    lineno: int | None

    @property
    def is_required(self) -> bool:
        # set by caller with threshold context via required_for(...)
        return False  # placeholder, computed outside

    @property
    def priority(self) -> float:
        return float(self.max_expense_amount)


def _is_expense(acct: str | None) -> bool:
    return bool(acct) and acct.startswith("Expenses:")


# AI-AGENT.md Phase 2: receipts attach to more than just expense
# txns. An ATM deposit slip belongs to the Income posting; a
# reimbursement confirmation belongs to the Equity posting; a
# mortgage statement belongs to the Liabilities:Mortgage posting.
# The `NON_RECEIPT_PATTERNS` narration filter still catches the
# true noise (CC payments, transfers, fees, interest) so the
# queue isn't flooded.
_RECEIPT_TARGET_ROOTS = ("Expenses", "Income", "Liabilities", "Equity")


def _is_receipt_target(acct: str | None) -> bool:
    if not acct:
        return False
    root = acct.split(":", 1)[0]
    return root in _RECEIPT_TARGET_ROOTS


def _is_fixme(acct: str | None) -> bool:
    if not acct:
        return False
    return acct.split(":")[-1].upper() == "FIXME"


def _linked_hashes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT DISTINCT txn_hash FROM document_links").fetchall()
    return {r["txn_hash"] for r in rows}


def _dismissed_hashes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT txn_hash FROM document_dismissals").fetchall()
    return {r["txn_hash"] for r in rows}


def find_orphan_dismissals(
    entries: Iterable, conn: sqlite3.Connection
) -> list[dict]:
    """Return dismissals whose ``txn_hash`` no longer matches any
    transaction in the ledger. Intended cause: the user edited the
    transaction since dismissing, so its hash changed.

    Surfaces these so the UI can show "previously dismissed on <date>
    — transaction has since been edited" with a one-click re-dismiss.
    Without that visibility, users see phantom re-appearances of items
    they thought they'd handled and lose trust in dismissal.

    Returned rows mirror the ``document_dismissals`` schema plus the
    dismissal directive's own date for display.
    """
    current_hashes: set[str] = set()
    for entry in entries:
        if isinstance(entry, Transaction):
            current_hashes.add(txn_hash(entry))
    rows = conn.execute(
        "SELECT txn_hash, reason, dismissed_by, dismissed_at "
        "FROM document_dismissals"
    ).fetchall()
    out: list[dict] = []
    for row in rows:
        if row["txn_hash"] in current_hashes:
            continue
        out.append(
            {
                "txn_hash": row["txn_hash"],
                "reason": row["reason"],
                "dismissed_by": row["dismissed_by"],
                "dismissed_at": row["dismissed_at"],
            }
        )
    return out


def _extract_last_four(txn: Transaction) -> str | None:
    """Look for a four-digit card tail in posting accounts or meta.

    Common patterns: Liabilities:Chase:CC:4242 → "4242"; posting meta with
    last_four=1234. This is a best-effort; the matcher will skip the
    last-four boost when absent.
    """
    rx = re.compile(r"(?<!\d)(\d{4})(?!\d)")
    for posting in txn.postings:
        acct = posting.account or ""
        if "Liabilities:" in acct or "Assets:" in acct:
            m = rx.search(acct)
            if m:
                return m.group(1)
    meta = getattr(txn, "meta", None) or {}
    for value in meta.values():
        if isinstance(value, str):
            m = rx.search(value)
            if m:
                return m.group(1)
    return None


@dataclass(frozen=True)
class QueueStats:
    scanned: int = 0
    excluded_non_receipt: int = 0
    excluded_linked: int = 0
    excluded_dismissed: int = 0
    excluded_no_expense: int = 0
    excluded_out_of_window: int = 0
    kept: int = 0


def build_queue(
    *,
    entries: Iterable,
    conn: sqlite3.Connection,
    threshold_usd: float,
    lookback_days: int = 90,
    include_linked: bool = False,
    include_dismissed: bool = False,
    include_non_receipt: bool = False,
    query: str | None = None,
    account_filter: str | None = None,
    min_amount: Decimal | None = None,
    max_amount: Decimal | None = None,
) -> tuple[list[tuple[NeedsDocumentItem, bool]], QueueStats]:
    """Returns ([(item, is_required), ...], stats).

    `is_required` captures the IRS ≥ threshold badge distinct from the item
    itself so the UI can filter/sort on it without mutating the dataclass.
    `stats` lets the caller show what was filtered so the empty state is
    explained, not mysterious.

    `query`, `account_filter`, `min_amount`, `max_amount` post-filter the
    built items in-memory: cheap on a 90-day window, and avoids touching the
    Beancount load loop. Empty / None values disable the filter.
    """
    since = date.today() - timedelta(days=max(0, lookback_days))
    linked = set() if include_linked else _linked_hashes(conn)
    dismissed = set() if include_dismissed else _dismissed_hashes(conn)

    stats_scanned = 0
    stats_non_receipt = 0
    stats_linked = 0
    stats_dismissed = 0
    stats_no_expense = 0
    stats_oow = 0

    out: list[tuple[NeedsDocumentItem, bool]] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        stats_scanned += 1
        if entry.date < since:
            stats_oow += 1
            continue

        # Scope: any txn that touches an Expenses / Income /
        # Liabilities / Equity posting — AI-AGENT.md Phase 2 widens
        # beyond Expenses-only. Pure Asset↔Asset transfers still
        # fall out here (no receipt-target leg). The narration
        # filter below catches the remaining noise.
        target_postings = [
            p for p in entry.postings if _is_receipt_target(p.account)
        ]
        if not target_postings:
            stats_no_expense += 1
            continue

        # Drop transfers, fees, credit-card payments, cashback, interest, etc.
        narration = entry.narration or ""
        payee = getattr(entry, "payee", None)
        if not include_non_receipt and _is_non_receipt(narration, payee):
            stats_non_receipt += 1
            continue

        h = txn_hash(entry)
        if h in linked:
            stats_linked += 1
            continue
        if h in dismissed:
            stats_dismissed += 1
            continue

        # Pick the primary receipt-target amount: the largest |amount|
        # across receipt-target postings. For an expense txn this is
        # the expense leg; for an income deposit the Income leg; for
        # a CC payment the Liabilities destination.
        max_amt = Decimal("0")
        currency = "USD"
        for p in target_postings:
            if p.units and p.units.number is not None:
                val = abs(Decimal(p.units.number))
                if val > max_amt:
                    max_amt = val
                    if p.units.currency:
                        currency = p.units.currency
        # Fallback: target postings had no number (interpolated) —
        # take any other posting's amount.
        if max_amt == 0:
            for p in entry.postings:
                if p.units and p.units.number is not None:
                    val = abs(Decimal(p.units.number))
                    if val > max_amt:
                        max_amt = val
                        if p.units.currency:
                            currency = p.units.currency

        expense_accounts = tuple(sorted({p.account for p in target_postings if p.account}))
        other_accounts = tuple(
            sorted(
                {
                    p.account
                    for p in entry.postings
                    if p.account and not _is_receipt_target(p.account)
                }
            )
        )

        is_fixme = any(_is_fixme(p.account) for p in entry.postings)
        meta = getattr(entry, "meta", None) or {}
        filename = meta.get("filename") if isinstance(meta, dict) else None
        lineno = meta.get("lineno") if isinstance(meta, dict) else None
        filename_short = filename.rsplit("/", 1)[-1] if isinstance(filename, str) else None

        from lamella.core.identity import get_txn_id
        item = NeedsDocumentItem(
            txn_hash=h,
            lamella_txn_id=get_txn_id(entry),
            txn_date=entry.date,
            payee=getattr(entry, "payee", None),
            narration=entry.narration or "",
            max_expense_amount=max_amt,
            currency=currency,
            expense_accounts=expense_accounts,
            other_accounts=other_accounts,
            last_four=_extract_last_four(entry),
            is_fixme=is_fixme,
            filename=filename_short,
            lineno=lineno,
        )
        required = float(max_amt) >= threshold_usd
        out.append((item, required))

    if query or account_filter or min_amount is not None or max_amount is not None:
        q_lower = (query or "").strip().lower() or None
        acct_lower = (account_filter or "").strip().lower() or None
        filtered: list[tuple[NeedsDocumentItem, bool]] = []
        for item, required in out:
            if min_amount is not None and item.max_expense_amount < min_amount:
                continue
            if max_amount is not None and item.max_expense_amount > max_amount:
                continue
            if acct_lower:
                hay = " ".join(item.expense_accounts + item.other_accounts).lower()
                if acct_lower not in hay:
                    continue
            if q_lower:
                hay = " ".join(filter(None, (
                    item.payee, item.narration,
                    " ".join(item.expense_accounts),
                    " ".join(item.other_accounts),
                ))).lower()
                if q_lower not in hay:
                    continue
            filtered.append((item, required))
        out = filtered

    out.sort(key=lambda t: (t[0].max_expense_amount, t[0].txn_date), reverse=True)
    stats = QueueStats(
        scanned=stats_scanned,
        excluded_non_receipt=stats_non_receipt,
        excluded_linked=stats_linked,
        excluded_dismissed=stats_dismissed,
        excluded_no_expense=stats_no_expense,
        excluded_out_of_window=stats_oow,
        kept=len(out),
    )
    return out, stats
