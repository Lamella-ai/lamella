# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP9 — prepayment / payoff projection.

Pure tests over `project` and `resolve_starting_balance`. Validates
the math against hand-computed expectations on small loans so a
typo or sign flip in the simulator shows up as a failing test
rather than a quietly-wrong payoff date.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from lamella.features.loans.projection import (
    ProjectionResult,
    project,
    resolve_starting_balance,
)


# --------------------------------------------------------------- fixtures


@dataclass
class _Payment:
    """Minimal stand-in for coverage.ActualPayment."""

    date: date
    principal_leg: Decimal


def _loan(**overrides) -> dict:
    base = {
        "slug": "M",
        "original_principal": "100000.00",
        "interest_rate_apr": "6.0",
        "term_months": 360,
        "first_payment_date": "2020-01-01",
        "monthly_payment_estimate": "599.55",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------- resolve_starting_balance


def test_resolve_prefers_anchor_walked_forward_over_model():
    """When an anchor exists, walk forward through principal payments
    rather than using the static model projection."""
    loan = _loan()
    anchor = {"as_of_date": "2024-01-01", "balance": "90000.00"}
    payments = [
        _Payment(date(2024, 2, 1), Decimal("100")),
        _Payment(date(2024, 3, 1), Decimal("101")),
        _Payment(date(2024, 4, 1), Decimal("102")),
    ]
    # Starting balance should be 90000 - (100 + 101 + 102) = 89697.
    resolved = resolve_starting_balance(
        loan, [anchor], payments, as_of=date(2024, 5, 1),
    )
    assert resolved == Decimal("89697")


def test_resolve_handles_18_month_old_anchor_with_full_walkforward():
    """The scenario the user flagged: an anchor from 18 months ago
    is still authoritative IF we walk forward through every payment
    since. The resolver must handle that without falling back to the
    model."""
    loan = _loan()
    anchor = {"as_of_date": "2023-01-01", "balance": "85000.00"}
    payments = [
        _Payment(date(2023, 1, 1) + _months(i), Decimal("50"))
        for i in range(1, 19)  # 18 payments
    ]
    # 85000 - (18 × 50) = 84100.
    resolved = resolve_starting_balance(
        loan, [anchor], payments, as_of=date(2024, 7, 1),
    )
    assert resolved == Decimal("84100")


def test_resolve_ignores_future_anchors():
    """An anchor dated after `as_of` is not usable — haven't gotten
    there yet."""
    loan = _loan()
    future_anchor = {"as_of_date": "2030-01-01", "balance": "10000.00"}
    # No past anchors → should fall back to model projection.
    resolved = resolve_starting_balance(
        loan, [future_anchor], [], as_of=date(2024, 1, 1),
    )
    # Model projection at payment ~49 of a 360-month 100k/6% loan.
    # Just verify it's reasonable — not the anchor value.
    assert resolved != Decimal("10000.00")
    assert Decimal("80000") < resolved < Decimal("100000")


def test_resolve_falls_back_to_model_when_no_anchors():
    loan = _loan()
    resolved = resolve_starting_balance(
        loan, [], [], as_of=date(2020, 7, 1),  # 6 payments in
    )
    # Should be slightly below 100000 — 6 months of amortization.
    assert resolved < Decimal("100000")
    assert resolved > Decimal("98000")


def test_resolve_uses_most_recent_anchor_when_multiple_exist():
    loan = _loan()
    anchors = [
        {"as_of_date": "2024-01-01", "balance": "95000.00"},
        {"as_of_date": "2024-06-01", "balance": "92000.00"},  # most recent
        {"as_of_date": "2023-01-01", "balance": "98000.00"},
    ]
    resolved = resolve_starting_balance(
        loan, anchors, [], as_of=date(2024, 7, 1),
    )
    assert resolved == Decimal("92000.00")


def test_resolve_handles_accessor_object_anchors():
    """Not just dicts — accept any object with .as_of_date / .balance."""
    class _A:
        def __init__(self, d, b):
            self.as_of_date = d
            self.balance = b

    loan = _loan()
    resolved = resolve_starting_balance(
        loan, [_A("2024-01-01", "80000.00")], [],
        as_of=date(2024, 2, 1),
    )
    assert resolved == Decimal("80000.00")


def test_resolve_clamps_negative_walk_forward_to_zero():
    """If mis-split payments would drive the walk-forward negative,
    clamp to 0 rather than a confusing negative balance."""
    loan = _loan()
    anchor = {"as_of_date": "2024-01-01", "balance": "1000.00"}
    # 10 payments of $200 each = $2000 in principal — over-pays the
    # $1000 anchor by 2x.
    payments = [
        _Payment(date(2024, 2, 1) + _months(i), Decimal("200"))
        for i in range(10)
    ]
    resolved = resolve_starting_balance(
        loan, [anchor], payments, as_of=date(2024, 12, 1),
    )
    assert resolved == Decimal("0")


# ---------------------------------------------------------------- project()


def test_project_baseline_no_extras_matches_term():
    """With extras at 0, the scenario should match the baseline
    exactly — same months, same total interest."""
    result = project(
        Decimal("100000"), Decimal("6.0"), Decimal("599.55"),
        as_of=date(2024, 1, 1),
    )
    assert result.baseline_months == result.scenario_months
    assert result.baseline_total_interest == result.scenario_total_interest
    assert result.months_saved == 0
    assert result.interest_saved == Decimal("0")


def test_project_extra_monthly_shortens_payoff():
    baseline = project(
        Decimal("100000"), Decimal("6.0"), Decimal("599.55"),
        as_of=date(2024, 1, 1),
    )
    scenario = project(
        Decimal("100000"), Decimal("6.0"), Decimal("599.55"),
        extra_monthly=Decimal("200"),
        as_of=date(2024, 1, 1),
    )
    assert scenario.scenario_months < baseline.baseline_months
    assert scenario.interest_saved > Decimal("0")
    assert scenario.months_saved > 0


def test_project_lump_sum_shortens_payoff():
    scenario = project(
        Decimal("100000"), Decimal("6.0"), Decimal("599.55"),
        lump_sum=Decimal("10000"),
        as_of=date(2024, 1, 1),
    )
    # Should pay off noticeably earlier than 360 months.
    assert scenario.scenario_months < 360
    assert scenario.months_saved > 0


def test_project_lump_sum_covering_full_balance_pays_off_immediately():
    result = project(
        Decimal("1000"), Decimal("6.0"), Decimal("100"),
        lump_sum=Decimal("1000"),
        as_of=date(2024, 1, 1),
    )
    assert result.scenario_months == 0
    assert result.scenario_payoff_date is None
    assert result.scenario_total_interest == Decimal("0")


def test_project_degenerate_payment_below_interest_terminates():
    """If monthly_payment doesn't cover monthly interest, the
    simulator must not hang. It returns early with the current
    month count so the UI can surface the config issue."""
    # $100 balance, 120% APR (10%/mo), $5/mo payment.
    # Interest = $10/mo, payment = $5 — balance grows.
    result = project(
        Decimal("100"), Decimal("120"), Decimal("5"),
        as_of=date(2024, 1, 1),
    )
    # Must have terminated (not hit _MAX_MONTHS).
    assert result.scenario_months < 2400


def test_project_monthly_points_correspond_to_scenario_months():
    result = project(
        Decimal("10000"), Decimal("6.0"), Decimal("200"),
        as_of=date(2024, 1, 1),
    )
    # points should have roughly one entry per scenario month.
    assert len(result.monthly_points) == result.scenario_months
    # Final point balance should be <= 0 (or close to it).
    final_balance = result.monthly_points[-1][1]
    assert final_balance == Decimal("0")


def test_project_points_dates_advance_monthly():
    result = project(
        Decimal("10000"), Decimal("6.0"), Decimal("500"),
        as_of=date(2024, 1, 15),
    )
    # First three points should be on 15th of successive months.
    assert result.monthly_points[0][0] == date(2024, 1, 15)
    assert result.monthly_points[1][0] == date(2024, 2, 15)
    assert result.monthly_points[2][0] == date(2024, 3, 15)


def test_project_zero_apr_still_works():
    """Zero-APR loan: payment = principal / n. Shouldn't hit div-by-zero."""
    result = project(
        Decimal("1200"), Decimal("0"), Decimal("100"),
        as_of=date(2024, 1, 1),
    )
    assert result.scenario_months == 12  # $100 × 12 = $1200 exactly


# --------------------------------------------------------------- helpers


def _months(n: int):
    """Quick month-offset helper for test fixtures (not a real
    calendar arithmetic — good enough for fixture dates)."""
    from datetime import timedelta
    return timedelta(days=30 * n)
