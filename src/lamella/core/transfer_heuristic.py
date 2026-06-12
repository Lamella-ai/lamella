# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shared transfer-suspect heuristic.

Three places in the codebase had nearly-identical regex constants
to detect "this row's narration looks like a transfer":

  * web/routes/staging_review.py — surfaces the "Looks like a
    transfer" hint band on /review and /card.
  * features/ai_cascade/enricher.py — suppresses low-confidence
    Expenses proposals on transfer-suspect rows during background
    enrichment.
  * web/routes/api_txn.py — same suppression for synchronous Ask AI
    runs (staged + ledger paths).

This module consolidates the regex into a single source of truth so
broadening the patterns is a one-place change. Direct user feedback
("Payments also need to be seen as transfers. Because when it comes
to credit cards, loans, lines of credit... a payment basically *has*
to have had a corresponding transfer") motivated widening the
match beyond `transfer|xfer` to cover liability-payment language.

The heuristic is intentionally conservative — false positives here
just add a confirm prompt and a "Looks like transfers" warning band,
but false negatives let the AI confidently misclassify a transfer as
an Expense (the user-visible bug).
"""
from __future__ import annotations

import re
from typing import Iterable

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# Original literal-transfer keywords. Match anywhere as whole words.
_TRANSFER_HINT_RE = re.compile(
    r"\b(transfer|xfer)\b",
    flags=re.IGNORECASE,
)

# Liability-payment patterns. A credit card / loan / mortgage / line
# of credit is structurally "the other half of a transfer" — those
# accounts cannot hold cash, so a payment to them MUST come from
# an Assets:Checking / Savings outflow. Bank narrations vary widely
# but cluster around a small set of phrasings:
#
#   - "Credit Card Payment", "CC Payment", "Card Payment"
#   - "Credit Account Payment" (Mercury, Marcus, etc.)
#   - "Loan Payment", "Mortgage Payment", "LOC Payment"
#   - "Online PMT", "Online Payment", "ACH PMT"
#   - "Payment Thank You", "PMT Thank You" (CC issuer side of the same
#     transfer — the bank-side narration on the receiving leg)
#   - "Payment to <Bank> Credit", "To <X> Credit Account Payment"
#     (Mercury's exact wording in the user-reported case)
#   - "Bill Pay" / "Billpay" / "Auto Pay" — typically pre-scheduled
#     transfers to a billable liability
#
# The pattern is a single alternation regex; OR'ing into the existing
# transfer regex keeps the call sites simple (one boolean check).
_LIABILITY_PAYMENT_RE = re.compile(
    r"\b("
    r"credit\s+card\s+payment|"
    r"credit\s+account\s+payment|"
    r"card\s+payment|"
    r"cc\s+payment|"
    r"loan\s+payment|"
    r"mortgage\s+payment|"
    r"line\s+of\s+credit\s+payment|"
    r"loc\s+payment|"
    r"heloc\s+payment|"
    r"online\s+pmt|"
    r"online\s+payment|"
    r"ach\s+pmt|"
    r"ach\s+payment|"
    r"pmt\s+thank\s+you|"
    r"payment\s+thank\s+you|"
    r"bill\s*pay|"
    r"auto\s*pay|"
    r"autopay"
    r")\b",
    flags=re.IGNORECASE,
)

# Two-step variant for narrations like "To Mercury Credit Account
# Payment" or "Payment to Chase Credit" where the keyword "payment"
# sits adjacent to a credit/card/loan/mortgage marker. The single-
# regex above doesn't catch this because "Mercury" / "Chase" sit
# between "to" and the marker word.
_PAYMENT_TO_LIABILITY_RE = re.compile(
    r"\b("
    r"(?:to|from)\s+\w+(?:\s+\w+){0,3}\s+"
    r"(?:credit|card|loan|mortgage|heloc)"
    r"|"
    r"(?:credit|card|loan|mortgage|heloc)"
    r"\s+(?:account\s+)?(?:payment|pmt)"
    r")\b",
    flags=re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def looks_like_transfer_text(text: str | None) -> bool:
    """Pure-text version of the transfer-suspect heuristic.

    Returns True when ``text`` matches either the literal-transfer
    regex (``transfer`` / ``xfer``) OR a liability-payment regex
    (credit card payment, online pmt, etc.). False on empty / None.

    Used by every code path that has access to a narration string
    but not the full row object — AI suppression, writer pre-emit
    detection, enricher gating.
    """
    if not text:
        return False
    if _TRANSFER_HINT_RE.search(text):
        return True
    if _LIABILITY_PAYMENT_RE.search(text):
        return True
    if _PAYMENT_TO_LIABILITY_RE.search(text):
        return True
    return False


def looks_like_transfer_item(
    item, card_kind: str | None = None,
) -> bool:
    """Row-level transfer-suspect heuristic.

    Combines the text-based check with two structural signals:
      * ``pair_id`` truthy → already paired, NOT a single-leg
        transfer-suspect; return False.
      * Source account kind is a known liability (credit card / loan /
        line of credit / mortgage) AND the amount is in the
        debt-reducing direction → True (single-leg liability payments
        are ALWAYS half of a transfer by construction).

    Mirrors the long-standing _looks_like_transfer in
    web/routes/staging_review.py — kept there as a thin shim that
    delegates to this helper for backward compat.
    """
    if getattr(item, "pair_id", None):
        return False
    pieces: list[str] = []
    for attr in ("payee", "description", "narration", "memo"):
        val = getattr(item, attr, None)
        if val:
            pieces.append(str(val))
    text = " ".join(pieces)
    if looks_like_transfer_text(text):
        return True
    # Liability-source structural signal — even narration-less rows
    # ("ONLINE PMT THANK YOU", typical CC issuer-side text) flag.
    if card_kind in _LIABILITY_KINDS:
        try:
            from decimal import Decimal as _D
            amount = _D(str(getattr(item, "amount", "0")))
        except Exception:  # noqa: BLE001
            return False
        if amount > 0:
            # Payment-direction sign: liability balance reduced
            # (positive in our staging amount convention). Negative
            # on a CC is a normal purchase, not a payment.
            return True
    return False


# Account kinds where ANY incoming amount is structurally a transfer
# leg. Public so other callers can check directly when they have a
# kind hint but no row object (e.g. classify-time pre-flight).
_LIABILITY_KINDS = frozenset({
    "credit_card",
    "line_of_credit",
    "loan",
    "mortgage",
    "heloc",
})


def is_liability_kind(kind: str | None) -> bool:
    """True when ``kind`` is one of the liability classes that
    structurally cannot hold cash (credit cards, loans, lines of
    credit, mortgages). Used by the classify-time guard to decide
    whether the user's chosen target deserves the synthetic-
    counterpart treatment per ADR-0046."""
    return (kind or "").lower() in _LIABILITY_KINDS


__all__ = [
    "looks_like_transfer_text",
    "looks_like_transfer_item",
    "is_liability_kind",
]
