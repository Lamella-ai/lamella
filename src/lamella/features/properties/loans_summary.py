# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Aggregation of loans encumbering a property.

A property may carry several debts at once: a first mortgage, a HELOC,
maybe a refi-in-progress before the old loan closes out. The property
detail page wants to surface them as a unit — total monthly payment,
total current balance, available headroom on revolving lines —
without each row having to walk the ledger itself.

This helper does the walk once, returns rows enriched with current
balance + revolving headroom, and a `combined` summary with the
roll-ups.

Why this lives under `properties/` and not `loans/`: the property is
the entity asking the question. Loans don't aggregate themselves;
properties aggregate the loans that encumber them. Same reasoning
that keeps `loans_for_property` SQL inside the property route.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from beancount.core.data import Transaction


def _D(v: Any, default: str = "0") -> Decimal:
    if v is None or v == "":
        return Decimal(default)
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError):
        return Decimal(default)


@dataclass
class LoanSummaryRow:
    """One loan as seen from the property side."""
    slug: str
    display_name: str | None
    loan_type: str
    institution: str | None
    is_revolving: bool
    is_active: bool
    liability_account_path: str | None
    original_principal: Decimal
    monthly_payment_estimate: Decimal
    credit_limit: Decimal | None
    current_balance: Decimal
    available_headroom: Decimal | None


@dataclass
class LoansSummary:
    """Roll-up across every loan against the property."""
    loans: list[LoanSummaryRow] = field(default_factory=list)
    combined_monthly: Decimal = Decimal("0")
    combined_balance: Decimal = Decimal("0")
    combined_credit_limit: Decimal = Decimal("0")
    combined_available_headroom: Decimal = Decimal("0")

    @property
    def has_revolving(self) -> bool:
        return any(l.is_revolving for l in self.loans)


def loans_for_property(
    *,
    property_slug: str,
    conn,
    entries: Iterable[Any],
    include_inactive: bool = False,
) -> LoansSummary:
    """Aggregate every loan referencing a property.

    Returns a LoansSummary with per-loan enriched rows and a combined
    roll-up. Each row's ``current_balance`` is the magnitude of the
    sum of postings against the loan's liability account in the
    ledger — i.e. how much is currently owed. For revolving lines,
    ``available_headroom`` is ``credit_limit - current_balance`` (or
    ``None`` when no credit limit is configured).

    The ``entries`` iterable is walked once even if there are many
    loans; we collect every relevant liability account up front and
    accumulate balances by path.
    """
    where = "property_slug = ?"
    args: list = [property_slug]
    if not include_inactive:
        where += " AND is_active = 1"
    rows = [
        dict(r) for r in conn.execute(
            f"""
            SELECT slug, display_name, loan_type, institution,
                   original_principal, liability_account_path,
                   monthly_payment_estimate, is_active,
                   is_revolving, credit_limit
            FROM loans WHERE {where}
            ORDER BY is_active DESC, is_revolving ASC, display_name
            """,
            args,
        ).fetchall()
    ]
    if not rows:
        return LoansSummary()

    paths_to_track: set[str] = {
        r["liability_account_path"]
        for r in rows
        if r.get("liability_account_path")
    }
    balances: dict[str, Decimal] = {p: Decimal("0") for p in paths_to_track}

    if paths_to_track:
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            for p in entry.postings:
                if p.account in balances and p.units and p.units.number is not None:
                    balances[p.account] += _D(p.units.number)

    summary = LoansSummary()
    for r in rows:
        path = r.get("liability_account_path")
        # Liability balances are negative on the ledger; the "current
        # debt" is the magnitude.
        balance = abs(balances.get(path, Decimal("0"))) if path else Decimal("0")
        is_revolving = bool(r.get("is_revolving"))
        credit_limit = _D(r.get("credit_limit")) if r.get("credit_limit") else None
        headroom: Decimal | None = None
        if is_revolving and credit_limit is not None:
            headroom = credit_limit - balance
        monthly = _D(r.get("monthly_payment_estimate"))

        summary.loans.append(LoanSummaryRow(
            slug=r["slug"],
            display_name=r.get("display_name"),
            loan_type=r.get("loan_type") or "other",
            institution=r.get("institution"),
            is_revolving=is_revolving,
            is_active=bool(r.get("is_active", 1)),
            liability_account_path=path,
            original_principal=_D(r.get("original_principal")),
            monthly_payment_estimate=monthly,
            credit_limit=credit_limit,
            current_balance=balance,
            available_headroom=headroom,
        ))
        # Roll up. Skip revolving rows from the monthly-payment total —
        # there's no fixed amortized payment for a HELOC; the user
        # services it ad-hoc.
        if not is_revolving:
            summary.combined_monthly += monthly
        summary.combined_balance += balance
        if credit_limit is not None:
            summary.combined_credit_limit += credit_limit
        if headroom is not None:
            summary.combined_available_headroom += headroom

    return summary
