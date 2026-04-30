# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Prepayment / payoff projection (WP9).

Pure math over the loan's remaining balance. Given a starting
balance, APR, monthly payment, and optional extras (extra per
month, one-time lump sum), project the payoff curve and surface
the money-and-time delta vs the no-extras baseline.

Two entry points:

- `resolve_starting_balance(loan, anchors, payments, as_of)`
  decides which "as of today" balance to feed the projector.
  Prefers anchor-walked-forward through ledger payments when an
  anchor exists at or before `as_of`; falls back to the
  amortization-model projection when no anchor is available.
  This mirrors the existing detail page's "current balance
  (anchored)" computation — the projector should not use a number
  the rest of the UI disagrees with.
- `project(starting_balance, apr, monthly_payment, *,
  extra_monthly=0, lump_sum=0, as_of=today)` → `ProjectionResult`.
  Pure.

Deliberately out of scope for WP9: biweekly payment strategies,
amortization recasts, round-up schemes. Extra-per-month and
one-time lump sum cover the 90% of real questions per the brief.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from lamella.features.loans.amortization import (
    amortization_schedule,
    payment_number_on,
)


# Hard cap on the payoff-simulation loop so a pathological input
# (zero payment, zero APR, positive balance) can't hang. 2400 months
# = 200 years; any real loan payoff well under that.
_MAX_MONTHS = 2400


@dataclass(frozen=True)
class ProjectionResult:
    starting_balance: Decimal
    monthly_payment: Decimal
    apr: Decimal

    # Baseline = no extras.
    baseline_payoff_date: date | None
    baseline_total_interest: Decimal
    baseline_months: int

    # Scenario = with extras applied.
    scenario_payoff_date: date | None
    scenario_total_interest: Decimal
    scenario_months: int

    # Deltas — positive = scenario better (shorter, less interest).
    months_saved: int
    interest_saved: Decimal

    # Chart points — scenario only. `(date, balance_after_month)` per
    # month up to payoff. Caller decides sampling; we emit every
    # month.
    monthly_points: list[tuple[date, Decimal]]


# --------------------------------------------------------------------- helpers


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _today(as_of: date | None) -> date:
    return as_of or datetime.now(timezone.utc).date()


def _add_month(d: date) -> date:
    """Advance by one calendar month, clamping end-of-month."""
    if d.month == 12:
        year, month = d.year + 1, 1
    else:
        year, month = d.year, d.month + 1
    # Clamp day to target month's length.
    if month == 12:
        next_first = date(year + 1, 1, 1)
    else:
        next_first = date(year, month + 1, 1)
    last = (next_first - timedelta(days=1)).day
    return date(year, month, min(d.day, last))


# -------------------------------------------------- starting balance resolver


def resolve_starting_balance(
    loan: dict,
    anchors: Sequence[Any],
    payments: Sequence[Any],
    as_of: date | None = None,
) -> Decimal:
    """Decide which number to project from.

    Strategy (matches the existing detail-page anchored-balance path):

    1. If any anchor has `as_of_date ≤ as_of`, pick the most recent
       such anchor and walk forward through every subsequent
       principal payment in the ledger. This handles the
       "18-month-old anchor + 18 months of payments" case correctly
       — the walk-forward is authoritative.
    2. Otherwise, fall back to the amortization model's projected
       remaining balance at the current payment number.
    3. If neither is available (no anchors + no term configured),
       return the original principal unchanged — at least the
       projection starts from a known number rather than zero.

    `anchors` is an iterable of dicts (or any object with
    `as_of_date` / `balance` attrs). `payments` is an iterable of
    objects with `.date` and `.principal_leg` (matches
    coverage.ActualPayment).
    """
    today = _today(as_of)

    # Step 1 — anchor-walked-forward.
    past: list[tuple[date, Decimal]] = []
    for a in anchors:
        if isinstance(a, dict):
            anchor_date = _as_date(a.get("as_of_date"))
            anchor_bal = _as_decimal(a.get("balance"))
        else:
            anchor_date = _as_date(getattr(a, "as_of_date", None))
            anchor_bal = _as_decimal(getattr(a, "balance", None))
        if anchor_date and anchor_bal is not None and anchor_date <= today:
            past.append((anchor_date, anchor_bal))
    if past:
        past.sort(key=lambda t: t[0], reverse=True)
        anchor_date, anchor_balance = past[0]
        principal_since = Decimal("0")
        for p in payments:
            p_date = _as_date(getattr(p, "date", None))
            if p_date and p_date > anchor_date:
                principal_since += _as_decimal(
                    getattr(p, "principal_leg", 0)
                ) or Decimal("0")
        resolved = anchor_balance - principal_since
        # Clamp at zero — the walk-forward shouldn't produce a
        # negative balance in practice, but a mis-split payment
        # could theoretically do it.
        return max(resolved, Decimal("0"))

    # Step 2 — model projection.
    principal = _as_decimal(loan.get("original_principal")) or Decimal("0")
    apr = _as_decimal(loan.get("interest_rate_apr")) or Decimal("0")
    term = int(loan.get("term_months") or 0)
    first = _as_date(loan.get("first_payment_date"))
    if principal > 0 and term > 0 and first:
        schedule = amortization_schedule(principal, apr, term)
        n = payment_number_on(first, today, term)
        if n <= 0:
            return principal
        if n - 1 < len(schedule):
            return schedule[n - 1].remaining
        return Decimal("0")  # loan is past term

    # Step 3 — no data; return original principal.
    return principal


# --------------------------------------------------------------- the projector


def _simulate(
    starting_balance: Decimal,
    monthly_rate: Decimal,
    monthly_payment: Decimal,
    *,
    extra_monthly: Decimal = Decimal("0"),
    lump_sum: Decimal = Decimal("0"),
    start_date: date,
) -> tuple[int, Decimal, list[tuple[date, Decimal]]]:
    """Month-by-month payoff sim. Returns (months, total_interest, points).

    Applies `lump_sum` to the balance at month 1 before computing that
    month's interest. `extra_monthly` adds to the principal paid each
    month in perpetuity (until payoff). Stops when balance hits 0
    (or after `_MAX_MONTHS` to guard against infinite loops).
    """
    balance = starting_balance - lump_sum
    if balance <= 0:
        # Lump sum was ≥ remaining balance; paid off at month 0.
        return 0, Decimal("0"), [(start_date, Decimal("0"))]

    total_interest = Decimal("0")
    points: list[tuple[date, Decimal]] = []
    d = start_date
    for month in range(1, _MAX_MONTHS + 1):
        interest = (balance * monthly_rate).quantize(Decimal("0.01"))
        principal_portion = monthly_payment - interest + extra_monthly
        if principal_portion <= 0:
            # Degenerate: payment doesn't cover interest. Treat as
            # "can't pay off" — return early so the caller can surface
            # the config issue without hanging.
            points.append((d, balance))
            return month, total_interest, points
        # Don't overpay on the final month.
        if principal_portion >= balance:
            total_interest += interest
            balance = Decimal("0")
            points.append((d, balance))
            return month, total_interest, points
        balance -= principal_portion
        total_interest += interest
        points.append((d, balance))
        d = _add_month(d)
    # Hit max months — rare safety valve.
    return _MAX_MONTHS, total_interest, points


def project(
    starting_balance: Decimal,
    apr: Decimal,
    monthly_payment: Decimal,
    *,
    extra_monthly: Decimal = Decimal("0"),
    lump_sum: Decimal = Decimal("0"),
    as_of: date | None = None,
) -> ProjectionResult:
    """Project payoff, compare to no-extras baseline."""
    today = _today(as_of)
    monthly_rate = apr / Decimal("100") / Decimal("12") if apr else Decimal("0")

    # Baseline: same payment, no extras.
    baseline_months, baseline_interest, _ = _simulate(
        starting_balance, monthly_rate, monthly_payment,
        extra_monthly=Decimal("0"), lump_sum=Decimal("0"),
        start_date=today,
    )

    # Scenario: with user-provided extras.
    scenario_months, scenario_interest, points = _simulate(
        starting_balance, monthly_rate, monthly_payment,
        extra_monthly=extra_monthly, lump_sum=lump_sum,
        start_date=today,
    )

    def _payoff_date(months: int) -> date | None:
        if months <= 0:
            return None
        d = today
        for _ in range(months - 1):
            d = _add_month(d)
        return d

    return ProjectionResult(
        starting_balance=starting_balance,
        monthly_payment=monthly_payment,
        apr=apr,
        baseline_payoff_date=_payoff_date(baseline_months),
        baseline_total_interest=baseline_interest,
        baseline_months=baseline_months,
        scenario_payoff_date=_payoff_date(scenario_months),
        scenario_total_interest=scenario_interest,
        scenario_months=scenario_months,
        months_saved=max(0, baseline_months - scenario_months),
        interest_saved=max(
            Decimal("0"), baseline_interest - scenario_interest,
        ),
        monthly_points=points,
    )
