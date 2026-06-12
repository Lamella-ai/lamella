# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP8 — anomaly detection.

Pure tests over each detector + integration tests confirming that
health.assess folds anomalies into the next-actions list and that
the drift baseline resets on config change (option (a) from the
plan review, not user-dismissible flags).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from lamella.features.loans.anomalies import (
    Anomaly,
    _detect_anchor_disagreement,
    _detect_apr_mismatch,
    _detect_double_post,
    _detect_interest_high_vs_model,
    _detect_payment_drift,
    _detect_principal_low_vs_model,
    _most_recent_config_reset_date,
    detect,
)


# -------------------------------------------------------------- fixtures


@dataclass
class _Actual:
    """Minimal stand-in for coverage.ActualPayment."""

    date: date
    total: Decimal = Decimal("0")
    principal_leg: Decimal = Decimal("0")
    interest_leg: Decimal = Decimal("0")
    escrow_leg: Decimal = Decimal("0")
    auto_classified: bool = False


def _loan(**overrides) -> dict:
    base = {
        "slug": "M",
        "original_principal": "100000.00",
        "interest_rate_apr": "6.0",
        "term_months": 360,
        "first_payment_date": "2024-01-01",
        "monthly_payment_estimate": "599.55",
        "escrow_monthly": None,
        "property_tax_monthly": None,
        "insurance_monthly": None,
    }
    base.update(overrides)
    return base


# -------------------------------------- config-reset: baseline drift control


def test_reset_date_none_with_empty_history():
    assert _most_recent_config_reset_date([]) is None


def test_reset_date_none_with_single_directive():
    """One directive = no 'prior' to compare against."""
    history = [
        {"directive_date": date(2024, 1, 1),
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
    ]
    assert _most_recent_config_reset_date(history) is None


def test_reset_date_identifies_config_change():
    """Two directives, escrow changed — reset date = latest."""
    history = [
        {"directive_date": date(2024, 1, 1),
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
        {"directive_date": date(2024, 6, 15),
         "escrow_monthly": "150",  # ← changed
         "property_tax_monthly": None, "insurance_monthly": None,
         "monthly_payment_estimate": "500"},
    ]
    assert _most_recent_config_reset_date(history) == date(2024, 6, 15)


def test_reset_date_ignores_unchanged_monthly_tuple():
    """Name-only edits (display_name change) don't reset drift baseline."""
    history = [
        {"directive_date": date(2024, 1, 1),
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
        {"directive_date": date(2024, 3, 1),
         # Same monthly tuple — only display_name/institution/etc changed.
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
    ]
    assert _most_recent_config_reset_date(history) is None


def test_reset_date_picks_most_recent_when_multiple_changes():
    history = [
        {"directive_date": date(2024, 1, 1),
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
        {"directive_date": date(2024, 3, 1),
         "escrow_monthly": "125", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
        {"directive_date": date(2024, 6, 1),
         "escrow_monthly": "150", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
    ]
    assert _most_recent_config_reset_date(history) == date(2024, 6, 1)


# ------------------------------------------------------------ payment-drift


def _mk_actuals(totals_and_dates: list[tuple[Decimal, date]]) -> list[_Actual]:
    return [_Actual(date=d, total=t) for t, d in totals_and_dates]


def test_drift_insufficient_signal_below_three_actuals():
    """Per §3.8: skip drift detection when fewer than 3 payments
    since the last reset. Early-phase new loans shouldn't trip."""
    actuals = _mk_actuals([
        (Decimal("500"), date(2024, 1, 1)),
        (Decimal("500"), date(2024, 2, 1)),
    ])
    assert _detect_payment_drift(actuals, history=[]) == []


def test_drift_fires_on_significant_deviation():
    actuals = _mk_actuals([
        (Decimal("500"), date(2024, 1, 1)),
        (Decimal("500"), date(2024, 2, 1)),
        (Decimal("500"), date(2024, 3, 1)),
        (Decimal("500"), date(2024, 4, 1)),
        (Decimal("500"), date(2024, 5, 1)),
        # 15% higher — trip the 10% threshold.
        (Decimal("575"), date(2024, 6, 1)),
    ])
    anomalies = _detect_payment_drift(actuals, history=[])
    assert len(anomalies) == 1
    assert anomalies[0].kind == "payment-drift"
    # Direction + median should be in the detail.
    assert "higher" in anomalies[0].detail


def test_drift_does_not_fire_on_small_deviations():
    actuals = _mk_actuals([
        (Decimal("500"), date(2024, 1, 1)),
        (Decimal("500"), date(2024, 2, 1)),
        (Decimal("500"), date(2024, 3, 1)),
        (Decimal("500"), date(2024, 4, 1)),
        (Decimal("500"), date(2024, 5, 1)),
        # 5% higher — below threshold.
        (Decimal("525"), date(2024, 6, 1)),
    ])
    assert _detect_payment_drift(actuals, history=[]) == []


def test_drift_baseline_resets_on_config_change():
    """The critical behavior: after escrow recalc at June 1, drift
    detector should compute median only from post-June payments —
    not drag pre-June values forward.

    Pre-June: 500/mo (5 payments). Post-June config change: 700/mo
    is now the new normal (4 payments). Without reset, rolling median
    would be ~550 and every 700 payment would trip drift. WITH reset,
    median is 700 and the 700 payments don't trip."""
    history = [
        {"directive_date": date(2024, 1, 1),
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
        {"directive_date": date(2024, 6, 1),
         "escrow_monthly": "300",  # ← changed, bumps monthly to ~700
         "property_tax_monthly": None, "insurance_monthly": None,
         "monthly_payment_estimate": "500"},
    ]
    actuals = _mk_actuals([
        # Pre-reset (should be ignored).
        (Decimal("500"), date(2024, 1, 15)),
        (Decimal("500"), date(2024, 2, 15)),
        (Decimal("500"), date(2024, 3, 15)),
        (Decimal("500"), date(2024, 4, 15)),
        (Decimal("500"), date(2024, 5, 15)),
        # Post-reset new normal.
        (Decimal("700"), date(2024, 6, 15)),
        (Decimal("700"), date(2024, 7, 15)),
        (Decimal("700"), date(2024, 8, 15)),
        (Decimal("700"), date(2024, 9, 15)),
    ])
    # With reset: median = 700, latest = 700 → 0% deviation → NO anomaly.
    assert _detect_payment_drift(actuals, history) == []


def test_drift_reset_does_not_fire_when_only_one_post_reset_payment():
    """Right after reset: if there's < 3 post-reset payments, no drift
    detection runs (insufficient signal for new baseline)."""
    history = [
        {"directive_date": date(2024, 1, 1),
         "escrow_monthly": "100", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
        {"directive_date": date(2024, 6, 1),
         "escrow_monthly": "300", "property_tax_monthly": None,
         "insurance_monthly": None, "monthly_payment_estimate": "500"},
    ]
    actuals = _mk_actuals([
        (Decimal("500"), date(2024, 1, 15)),
        (Decimal("500"), date(2024, 2, 15)),
        (Decimal("500"), date(2024, 3, 15)),
        (Decimal("700"), date(2024, 6, 15)),  # only one post-reset
    ])
    assert _detect_payment_drift(actuals, history) == []


# ---------------------------------------------- interest-high / principal-low


def test_interest_high_fires_when_anchored_balance_above_model():
    loan = _loan()
    out = _detect_interest_high_vs_model(
        loan, actuals=[],
        anchored_balance=Decimal("95000"),
        model_remaining=Decimal("90000"),
    )
    assert len(out) == 1
    assert out[0].kind == "interest-high-vs-model"


def test_interest_high_quiet_on_small_gap():
    loan = _loan()  # principal = 100k, threshold 2% = 2000
    out = _detect_interest_high_vs_model(
        loan, actuals=[],
        anchored_balance=Decimal("90500"),
        model_remaining=Decimal("90000"),  # gap is $500 = 0.5%
    )
    assert out == []


def test_principal_low_fires_on_inverse():
    loan = _loan()
    out = _detect_principal_low_vs_model(
        loan,
        anchored_balance=Decimal("85000"),
        model_remaining=Decimal("90000"),
    )
    assert len(out) == 1
    assert out[0].kind == "principal-low-vs-model"


# ------------------------------------------------------------- apr-mismatch


def test_apr_mismatch_fires_on_wrong_configured_rate():
    """Loan configured at 6% APR. Effective rate derived from recent
    payments is 10% (interest >> expected). Detector should fire."""
    loan = _loan(interest_rate_apr="6.0", original_principal="100000")
    # ~10%/12 ≈ 0.00833 monthly rate
    actuals = [
        _Actual(date=date(2024, 1, 1),
                interest_leg=Decimal("833"), principal_leg=Decimal("100")),
        _Actual(date=date(2024, 2, 1),
                interest_leg=Decimal("832"), principal_leg=Decimal("100")),
        _Actual(date=date(2024, 3, 1),
                interest_leg=Decimal("831"), principal_leg=Decimal("100")),
    ]
    out = _detect_apr_mismatch(loan, actuals)
    assert len(out) == 1
    assert out[0].kind == "apr-mismatch"


def test_apr_mismatch_quiet_when_rates_align():
    """Configured 6% + derived 6% → no anomaly."""
    loan = _loan(interest_rate_apr="6.0", original_principal="100000")
    # 6%/12 ≈ 0.005 monthly rate → interest of 500 on 100k balance
    actuals = [
        _Actual(date=date(2024, 1, 1),
                interest_leg=Decimal("500"), principal_leg=Decimal("100")),
        _Actual(date=date(2024, 2, 1),
                interest_leg=Decimal("499"), principal_leg=Decimal("100")),
        _Actual(date=date(2024, 3, 1),
                interest_leg=Decimal("498"), principal_leg=Decimal("100")),
    ]
    assert _detect_apr_mismatch(loan, actuals) == []


# ------------------------------------------------------- anchor-disagreement


def test_anchor_disagreement_fires_on_mismatch():
    """Two anchors; walk-forward gives a balance that disagrees with
    the later anchor by > 1% of principal."""
    anchors = [
        {"as_of_date": "2024-01-01", "balance": "90000"},
        {"as_of_date": "2024-06-01", "balance": "80000"},  # <-- large gap
    ]
    # Only $500 of principal paid between anchors, so walk-forward
    # expects 89500. Later anchor says 80000 — disagreement of 9500.
    actuals = [
        _Actual(date=date(2024, 3, 1), principal_leg=Decimal("500")),
    ]
    out = _detect_anchor_disagreement(anchors, actuals, Decimal("100000"))
    assert len(out) == 1
    assert out[0].kind == "anchor-disagreement"


def test_anchor_disagreement_quiet_when_aligned():
    anchors = [
        {"as_of_date": "2024-01-01", "balance": "90000"},
        {"as_of_date": "2024-06-01", "balance": "87500"},
    ]
    # 2500 of principal paid between anchors matches the delta.
    actuals = [
        _Actual(date=date(2024, 2, 1), principal_leg=Decimal("500")),
        _Actual(date=date(2024, 3, 1), principal_leg=Decimal("500")),
        _Actual(date=date(2024, 4, 1), principal_leg=Decimal("500")),
        _Actual(date=date(2024, 5, 1), principal_leg=Decimal("500")),
        _Actual(date=date(2024, 6, 1), principal_leg=Decimal("500")),
    ]
    assert _detect_anchor_disagreement(anchors, actuals, Decimal("100000")) == []


def test_anchor_disagreement_needs_two_anchors():
    """One anchor can't produce a disagreement — needs a pair."""
    anchors = [{"as_of_date": "2024-01-01", "balance": "90000"}]
    assert _detect_anchor_disagreement(anchors, [], Decimal("100000")) == []


# ------------------------------------------------------------ double-post


def test_double_post_surfaces_extras_as_info_anomaly():
    class _Expected:
        n = 3
        expected_date = date(2024, 3, 1)

    class _Row:
        def __init__(self, extras):
            self.expected = _Expected()
            self.actual = None
            self.extras = extras

    class _Coverage:
        pass

    coverage = _Coverage()
    coverage.rows = [_Row(extras=[
        _Actual(date=date(2024, 3, 2), total=Decimal("500")),
    ])]
    anomalies = _detect_double_post(coverage)
    assert len(anomalies) == 1
    assert anomalies[0].kind == "double-post"
    assert anomalies[0].severity == "info"


# --------------------------------------------------------- detect() entry


def test_detect_combines_all_detectors():
    """Smoke test: detect() aggregates from every detector."""
    loan = _loan(interest_rate_apr="6.0")
    actuals = [
        _Actual(date=date(2024, 1, 1), total=Decimal("500"),
                interest_leg=Decimal("500"), principal_leg=Decimal("100")),
        _Actual(date=date(2024, 2, 1), total=Decimal("500"),
                interest_leg=Decimal("500"), principal_leg=Decimal("100")),
        _Actual(date=date(2024, 3, 1), total=Decimal("500"),
                interest_leg=Decimal("500"), principal_leg=Decimal("100")),
    ]
    # All detectors evaluate but none should fire on this clean data.
    out = detect(
        loan, coverage=None, anchors=[], actuals=actuals, history=[],
        anchored_balance=Decimal("90000"),
        model_remaining=Decimal("90000"),
    )
    assert out == []


def test_detect_returns_anomaly_when_drift_fires():
    loan = _loan()
    actuals = [
        _Actual(date=date(2024, 1, 1), total=Decimal("500")),
        _Actual(date=date(2024, 2, 1), total=Decimal("500")),
        _Actual(date=date(2024, 3, 1), total=Decimal("500")),
        _Actual(date=date(2024, 4, 1), total=Decimal("500")),
        _Actual(date=date(2024, 5, 1), total=Decimal("500")),
        _Actual(date=date(2024, 6, 1), total=Decimal("600")),  # 20% higher
    ]
    out = detect(
        loan, coverage=None, anchors=[], actuals=actuals, history=[],
        anchored_balance=None, model_remaining=None,
    )
    kinds = [a.kind for a in out]
    assert "payment-drift" in kinds


# ------------------------------------------------- health integration hook


def test_health_folds_anomaly_into_next_actions():
    """When assess() receives an anomaly with no recommended_action,
    it synthesizes a kind='anomaly' NextAction linking back to the
    loan's detail page."""
    from lamella.features.loans.health import assess, ScaffoldingReport
    from beancount.core.data import Transaction, Posting
    from beancount.core.amount import Amount

    class _FakeConn:
        def execute(self, sql, params=()):
            class _C:
                def fetchone(self_inner):
                    return None

                def fetchall(self_inner):
                    return []
            return _C()

    loan = _loan()
    funding = Transaction(
        meta={"lamella-loan-slug": "M", "filename": "x", "lineno": 1},
        date=date(2024, 1, 1), flag="*", payee=None,
        narration="Loan funding — M",
        tags={"lamella-loan-funding"}, links=set(),
        postings=[
            Posting(account="Liabilities:Personal:Bank:M",
                    units=Amount(Decimal("-100000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Equity:Personal:OpeningBalances",
                    units=Amount(Decimal("100000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )
    manual_anomaly = Anomaly(
        kind="payment-drift", severity="attention",
        detail="test drift",
    )
    h = assess(
        loan, [funding], _FakeConn(), settings=None,
        as_of=date(2024, 6, 15),
        scaffolding=ScaffoldingReport(issues=[]),
        coverage=None,
        anomalies=[manual_anomaly],
    )
    anomaly_actions = [a for a in h.next_actions if a.kind == "anomaly"]
    assert len(anomaly_actions) == 1
    assert "payment-drift" in anomaly_actions[0].title
