# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Amortization schedule calculator.

Used by the loan detail page and by the card UX's split pre-fill when a
payment-sized charge lands on a loan's tracked account.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP


@dataclass(frozen=True)
class Payment:
    payment_number: int
    principal: Decimal
    interest: Decimal
    total: Decimal
    remaining: Decimal


def monthly_payment(principal: Decimal, apr: Decimal, term_months: int) -> Decimal:
    """P · r / (1 - (1+r)^-n), where r = monthly rate."""
    if term_months <= 0:
        return Decimal("0")
    r = apr / Decimal("100") / Decimal("12")
    if r == 0:
        return (principal / Decimal(term_months)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    one = Decimal("1")
    factor = (one + r) ** term_months
    pmt = principal * r * factor / (factor - one)
    return pmt.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def amortization_schedule(
    principal: Decimal, apr: Decimal, term_months: int
) -> list[Payment]:
    """Full amortization schedule, one Payment per month.

    DEFERRED-WP13-PHASE2: interest-only schedules. Some HELOCs (and
    construction loans) carry an interest-only window before
    transitioning to a fully-amortizing tail. Today every schedule
    is fully-amortizing from month 1; an `interest_only_months`
    parameter would split the loop into two phases — the first
    posts ``interest = balance · r`` with ``principal = 0``, the
    second runs the existing loop on the remaining balance over
    ``term_months - interest_only_months``. Plumbing also needs a
    new column on ``loans`` (``interest_only_months INTEGER``) and
    a corresponding ``lamella-loan-interest-only-months`` meta key.
    """
    if term_months <= 0:
        return []
    pmt = monthly_payment(principal, apr, term_months)
    r = apr / Decimal("100") / Decimal("12")
    balance = principal
    out: list[Payment] = []
    for n in range(1, term_months + 1):
        interest = (balance * r).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        principal_pmt = (pmt - interest).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        # Last payment: adjust to zero the balance exactly.
        if n == term_months:
            principal_pmt = balance.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            pmt_n = (principal_pmt + interest).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            pmt_n = pmt
        balance -= principal_pmt
        out.append(Payment(
            payment_number=n,
            principal=principal_pmt,
            interest=interest,
            total=pmt_n,
            remaining=max(balance, Decimal("0")),
        ))
    return out


def split_for_payment_number(
    principal: Decimal,
    apr: Decimal,
    term_months: int,
    payment_number: int,
    escrow_monthly: Decimal | None = None,
) -> dict[str, Decimal]:
    """Return {'principal': ..., 'interest': ..., 'escrow': ..., 'total': ...}
    for a given payment in the schedule."""
    schedule = amortization_schedule(principal, apr, term_months)
    if not schedule:
        return {"principal": Decimal("0"), "interest": Decimal("0"),
                "escrow": escrow_monthly or Decimal("0"), "total": Decimal("0")}
    idx = max(1, min(payment_number, len(schedule)))
    row = schedule[idx - 1]
    escrow = escrow_monthly or Decimal("0")
    return {
        "principal": row.principal,
        "interest": row.interest,
        "escrow": escrow,
        "total": (row.total + escrow).quantize(Decimal("0.01")),
    }


def payment_number_on(
    first_payment_date: date, as_of: date, term_months: int
) -> int:
    """Given the first payment date and a date, return which payment
    number `as_of` falls closest to. Uses month-count arithmetic."""
    if as_of < first_payment_date:
        return 0
    years = as_of.year - first_payment_date.year
    months = as_of.month - first_payment_date.month
    n = years * 12 + months + 1
    return max(1, min(n, term_months))
