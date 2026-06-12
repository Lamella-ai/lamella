# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP13 — revolving loan helpers.

A revolving loan changes balance via two move types:

* **Draws** — money the user pulled from the line. Liability balance
  becomes more negative (Beancount convention: liabilities are
  conventionally negative). The posting on the liability account
  has amount < 0.
* **Payments** — money the user paid back. Liability balance
  becomes less negative. The posting on the liability account has
  amount > 0.

The auto-classify path (``ClaimKind.DRAW`` / ``REVOLVING_SKIP`` in
``loans/claim.py``) preempts AI; the user hand-categorizes via
record-payment for payments, and via the categorize-draw endpoint
for draws. This module gives the panel a way to surface
recent uncategorized draws.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Iterable

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.identity import get_txn_id


def _D(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:  # noqa: BLE001
        return Decimal("0")


def is_draw_txn(txn: Transaction, liability_path: str) -> bool:
    """True when the transaction has a posting to ``liability_path``
    with amount < 0 (i.e., draws more debt) AND a sibling FIXME
    posting indicating it hasn't been categorized.

    A categorized draw (no FIXME) is no longer surfaced as needing
    action — it already has a destination account on the other leg.
    """
    has_negative_liability = False
    has_fixme = False
    for p in txn.postings or []:
        acct = getattr(p, "account", None) or ""
        units = getattr(p, "units", None)
        if units is None or getattr(units, "number", None) is None:
            continue
        amt = _D(units.number)
        if acct == liability_path and amt < 0:
            has_negative_liability = True
        if acct.split(":")[-1].upper() == "FIXME":
            has_fixme = True
    return has_negative_liability and has_fixme


@dataclass
class DrawRow:
    """One uncategorized draw surfaced on the revolving panel."""
    txn_hash: str
    # Immutable UUIDv7 lineage id — UI link-builders use this for
    # /txn/{id}; the categorize-draw POST still keys off ``txn_hash``
    # because it has to identify the exact ledger entry to rewrite.
    lamella_txn_id: str | None
    txn_date: date
    payee: str | None
    narration: str
    amount: Decimal               # positive — the draw size
    fixme_account: str            # the actual FIXME account path on this txn


def recent_draws(
    *,
    loan: dict,
    entries: Iterable[Any],
    limit: int = 10,
) -> list[DrawRow]:
    """Return the most recent uncategorized draws on this revolving
    loan, newest first.

    Skipped silently when the loan has no liability_account_path
    configured — without it there's nothing to detect.
    """
    liability = loan.get("liability_account_path")
    if not liability:
        return []

    out: list[DrawRow] = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if not is_draw_txn(entry, liability):
            continue
        # Find the FIXME account + the magnitude of the draw.
        draw_amount = Decimal("0")
        fixme_path = ""
        for p in entry.postings or []:
            acct = getattr(p, "account", None) or ""
            units = getattr(p, "units", None)
            if units is None or getattr(units, "number", None) is None:
                continue
            if acct == liability:
                # Draw size = magnitude of the (negative) liability leg.
                draw_amount = _D(units.number).copy_abs()
            if acct.split(":")[-1].upper() == "FIXME":
                fixme_path = acct
        if draw_amount <= 0 or not fixme_path:
            continue
        out.append(DrawRow(
            txn_hash=txn_hash(entry),
            lamella_txn_id=get_txn_id(entry),
            txn_date=entry.date,
            payee=getattr(entry, "payee", None),
            narration=entry.narration or "",
            amount=draw_amount,
            fixme_account=fixme_path,
        ))

    out.sort(key=lambda r: (r.txn_date, r.txn_hash), reverse=True)
    return out[:limit]
