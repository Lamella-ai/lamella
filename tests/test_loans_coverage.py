# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP3 — payment coverage engine.

Pure tests over `build_schedule`, `extract_actuals`, `match`, and
`coverage_for` plus graceful-degradation integration tests against
the health model.

Specific test coverage called out by the plan author (user):

- 1-missing happy path.
- 3-missing consecutive boundary (graceful-degradation trigger).
- 10-missing consecutive (must not emit 10 blocking next-actions).
- Late-but-matched.
- Double-post → extras.
- 6-month-in with 4 payments → 2 missing (impl-doc acceptance).
- 6-month-in with 7 payments → 0 missing, 1 extra (impl-doc acceptance).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction

from lamella.features.loans.coverage import (
    ActualPayment,
    CoverageReport,
    CoverageRow,
    ExpectedPayment,
    _add_months,
    _snap_to_due_day,
    build_schedule,
    coverage_for,
    extract_actuals,
    match,
)


# --------------------------------------------------------------- fixtures


def _loan(**overrides) -> dict:
    base = {
        "slug": "M",
        "display_name": "Test",
        "loan_type": "mortgage",
        "entity_slug": "Personal",
        "institution": "Bank",
        "original_principal": "100000.00",
        "funded_date": "2025-01-01",
        "first_payment_date": "2025-02-01",
        "payment_due_day": 1,
        "term_months": 360,
        "interest_rate_apr": "5.0",
        "escrow_monthly": None,
        "property_tax_monthly": None,
        "insurance_monthly": None,
        "liability_account_path": "Liabilities:Personal:Bank:M",
        "interest_account_path": "Expenses:Personal:M:Interest",
        "escrow_account_path": None,
        "is_active": 1,
    }
    base.update(overrides)
    return base


def _payment_txn(
    d: date,
    *,
    principal: Decimal,
    interest: Decimal,
    from_account: str = "Assets:Checking",
    liability_path: str = "Liabilities:Personal:Bank:M",
    interest_path: str = "Expenses:Personal:M:Interest",
    auto: bool = False,
) -> Transaction:
    meta = {"filename": "x", "lineno": 1}
    if auto:
        meta["lamella-loan-autoclass-tier"] = "exact"
    return Transaction(
        meta=meta,
        date=d, flag="*", payee=None, narration="Mortgage payment",
        tags=set(), links=set(),
        postings=[
            Posting(account=liability_path,
                    units=Amount(principal, "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account=interest_path,
                    units=Amount(interest, "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account=from_account,
                    units=Amount(-(principal + interest), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )


# --------------------------------------------------------------- utilities


def test_add_months_clamps_end_of_month():
    # Jan 31 + 1 month -> Feb 28 (non-leap).
    assert _add_months(date(2025, 1, 31), 1) == date(2025, 2, 28)
    # Jan 31 + 1 month -> Feb 29 (leap).
    assert _add_months(date(2024, 1, 31), 1) == date(2024, 2, 29)
    # Dec 15 + 3 months -> Mar 15 (year rollover).
    assert _add_months(date(2025, 12, 15), 3) == date(2026, 3, 15)


def test_snap_to_due_day_respects_month_length():
    # Due day 31 in Feb clamps to 28/29.
    assert _snap_to_due_day(date(2025, 2, 10), 31) == date(2025, 2, 28)
    # Due day 1 from any starting day.
    assert _snap_to_due_day(date(2025, 3, 27), 1) == date(2025, 3, 1)
    # Due day None = no snap.
    assert _snap_to_due_day(date(2025, 3, 27), None) == date(2025, 3, 27)


# ---------------------------------------------------------- build_schedule


def test_build_schedule_produces_one_row_per_month_to_today():
    # First payment 2025-02-01, as_of 2025-06-15 → 5 expected (Feb-Jun).
    loan = _loan()
    sched = build_schedule(loan, as_of=date(2025, 6, 15))
    assert len(sched) == 5
    assert [r.n for r in sched] == [1, 2, 3, 4, 5]
    dates = [r.expected_date for r in sched]
    assert dates == [
        date(2025, 2, 1), date(2025, 3, 1), date(2025, 4, 1),
        date(2025, 5, 1), date(2025, 6, 1),
    ]


def test_build_schedule_empty_without_term_or_first_payment():
    assert build_schedule(_loan(term_months=None),
                          as_of=date(2025, 6, 15)) == []
    assert build_schedule(_loan(first_payment_date=None),
                          as_of=date(2025, 6, 15)) == []


def test_build_schedule_includes_escrow_in_total():
    loan = _loan(
        escrow_monthly="100.00",
        property_tax_monthly="50.00",
        insurance_monthly="25.00",
    )
    sched = build_schedule(loan, as_of=date(2025, 3, 15))
    row = sched[0]
    assert row.escrow == Decimal("100.00")
    assert row.tax == Decimal("50.00")
    assert row.insurance == Decimal("25.00")
    assert row.total > row.principal + row.interest  # escrow + tax + insurance added


def test_build_schedule_respects_pauses():
    class _Pause:
        def __init__(self, s, e):
            self.start_date = s
            self.end_date = e

    loan = _loan()
    # Pause Mar 1 - Apr 30 2025 (should skip n=2 and n=3).
    pauses = [_Pause(date(2025, 3, 1), date(2025, 4, 30))]
    sched = build_schedule(loan, pauses=pauses, as_of=date(2025, 6, 15))
    ns = [r.n for r in sched]
    assert 2 not in ns
    assert 3 not in ns
    assert 1 in ns
    assert 5 in ns


# ---------------------------------------------------------- extract_actuals


def test_extract_actuals_sums_legs_from_override_shape():
    loan = _loan()
    entries = [
        _payment_txn(date(2025, 2, 1),
                     principal=Decimal("100"), interest=Decimal("400")),
        _payment_txn(date(2025, 3, 1),
                     principal=Decimal("101"), interest=Decimal("399")),
    ]
    actuals = extract_actuals(loan, entries)
    assert len(actuals) == 2
    assert actuals[0].principal_leg == Decimal("100")
    assert actuals[0].interest_leg == Decimal("400")
    assert actuals[0].total == Decimal("500")


def test_extract_actuals_marks_auto_classified_from_meta():
    loan = _loan()
    entries = [
        _payment_txn(date(2025, 2, 1),
                     principal=Decimal("100"), interest=Decimal("400"),
                     auto=True),
    ]
    actuals = extract_actuals(loan, entries)
    assert actuals[0].auto_classified is True


def test_extract_actuals_skips_draws_on_revolving():
    """A draw posts NEGATIVE principal on the liability (balance
    increases). Not a payment — should not appear in actuals."""
    loan = _loan()
    draw = Transaction(
        meta={"filename": "x", "lineno": 1},
        date=date(2025, 2, 1), flag="*", payee=None,
        narration="HELOC draw", tags=set(), links=set(),
        postings=[
            Posting(account="Liabilities:Personal:Bank:M",
                    units=Amount(Decimal("-5000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Assets:Checking",
                    units=Amount(Decimal("5000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )
    actuals = extract_actuals(loan, [draw])
    assert actuals == []


# ------------------------------------------------------------------- match


def test_six_in_with_four_payments_produces_two_missing_rows():
    """Impl-doc acceptance: 6-months-in with 4 payments → 2 missing."""
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 7, 15))  # n=6
    assert len(schedule) == 6
    # Pay on Feb/Mar/Apr only — May and beyond missing.
    actuals = [
        ActualPayment(txn_hash=f"h{i}", date=d,
                      principal_leg=Decimal("100"),
                      interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False)
        for i, d in enumerate([date(2025, 2, 1), date(2025, 3, 1),
                                date(2025, 4, 1), date(2025, 5, 1)])
    ]
    report = match(schedule, actuals)
    missing = [r for r in report.rows if r.status == "missing"]
    assert len(missing) == 2
    assert [r.expected.expected_date for r in missing] == [
        date(2025, 6, 1), date(2025, 7, 1),
    ]


def test_six_in_with_seven_payments_produces_zero_missing_and_one_extra():
    """Impl-doc acceptance: 6 in, 7 paid → 0 missing, 1 extra."""
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 7, 15))  # n=6
    actuals = [
        ActualPayment(txn_hash=f"h{i}", date=d,
                      principal_leg=Decimal("100"),
                      interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False)
        for i, d in enumerate([
            date(2025, 2, 1), date(2025, 3, 1), date(2025, 4, 1),
            date(2025, 5, 1), date(2025, 6, 1), date(2025, 7, 1),
            # Extra: a second payment in March.
            date(2025, 7, 14),
        ])
    ]
    report = match(schedule, actuals)
    missing = [r for r in report.rows if r.status == "missing"]
    assert missing == []
    # Either the extra appears in the report-level `extras` OR as
    # a per-row `extras` entry on the last match (within ±3 days).
    total_extras = len(report.extras) + sum(len(r.extras) for r in report.rows)
    assert total_extras >= 1


def test_payment_twenty_days_late_is_late_not_missing():
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 3, 15))  # n=2
    # Jan/Feb expected; the Feb payment lands on Feb 22 (21 days after).
    actuals = [
        ActualPayment(txn_hash="h1", date=date(2025, 2, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
        ActualPayment(txn_hash="h2", date=date(2025, 3, 22),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
    ]
    report = match(schedule, actuals)
    # Row for expected 2025-03-01 should match the 2025-03-22 actual as "late".
    march_row = next(r for r in report.rows if r.expected.expected_date == date(2025, 3, 1))
    assert march_row.status == "late"
    assert march_row.actual is not None
    assert report.late_count == 1


def test_double_post_appears_as_extras():
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 2, 15))  # n=1
    # Two payments 1 day apart, both look like the February payment.
    actuals = [
        ActualPayment(txn_hash="h1", date=date(2025, 2, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
        ActualPayment(txn_hash="h2", date=date(2025, 2, 2),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
    ]
    report = match(schedule, actuals)
    assert report.matched_count == 1
    # The duplicate shows up as either a per-row extra or a
    # report-level extra.
    total_extras = len(report.extras) + sum(len(r.extras) for r in report.rows)
    assert total_extras >= 1


# --------------------------------------- graceful degradation (long gap)


def test_one_missing_does_not_trigger_long_gap():
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 4, 15))  # n=3
    # Miss Feb, pay Mar + Apr.
    actuals = [
        ActualPayment(txn_hash="h1", date=date(2025, 3, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
        ActualPayment(txn_hash="h2", date=date(2025, 4, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
    ]
    report = match(schedule, actuals)
    assert report.missing_count == 1
    assert report.long_gap_detected is False
    assert report.long_gap_count == 0


def test_two_missing_does_not_trigger_long_gap():
    """Boundary: threshold is 3. Two consecutive missing payments
    still emit individual attention items."""
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 5, 15))  # n=4
    # Miss Feb & Mar, pay Apr + May.
    actuals = [
        ActualPayment(txn_hash="h1", date=date(2025, 4, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
        ActualPayment(txn_hash="h2", date=date(2025, 5, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
    ]
    report = match(schedule, actuals)
    assert report.missing_count == 2
    assert report.long_gap_detected is False


def test_three_missing_triggers_long_gap():
    """The critical 3-missing boundary. The user explicitly called
    out that this test must exist."""
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 6, 15))  # n=5
    # Miss Feb/Mar/Apr (three consecutive), pay May.
    actuals = [
        ActualPayment(txn_hash="h1", date=date(2025, 5, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
        ActualPayment(txn_hash="h2", date=date(2025, 6, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
    ]
    report = match(schedule, actuals)
    assert report.missing_count == 3
    assert report.long_gap_detected is True
    assert report.long_gap_count == 3
    assert report.long_gap_start == date(2025, 2, 1)
    assert report.long_gap_end == date(2025, 4, 1)


def test_ten_missing_triggers_long_gap_not_ten_actions():
    """The degenerate case. 10 months no payments since first —
    the report carries long_gap_detected=True with the whole span,
    NOT ten individual missing rows that the UI would render as
    ten blockers."""
    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 12, 15))  # n=11
    # No payments at all.
    report = match(schedule, [])
    # Schedule itself has 11 rows; all missing.
    assert report.missing_count == 11
    assert report.long_gap_detected is True
    # The long run spans the whole schedule.
    assert report.long_gap_count == 11
    assert report.long_gap_start == date(2025, 2, 1)
    assert report.long_gap_end == date(2025, 12, 1)


# ------------------------------------------------------- health integration


def test_long_gap_collapses_to_one_next_action_in_health():
    """When coverage reports long_gap_detected=True, health.assess
    emits exactly ONE long-payment-gap next-action and ZERO
    missing-payment actions — even if 10 rows are missing."""
    from lamella.features.loans.health import assess
    from lamella.features.loans.scaffolding import Issue  # noqa: F401

    class _FakeConn:
        def execute(self, sql, params=()):
            class _C:
                def fetchone(self_inner):
                    return None

                def fetchall(self_inner):
                    return []
            return _C()

    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 12, 15))
    coverage = match(schedule, [])  # all missing

    # Funded transaction so the fund-initial blocker doesn't show up
    # and crowd the assertions.
    funding = Transaction(
        meta={"lamella-loan-slug": "M", "filename": "x", "lineno": 1},
        date=date(2025, 1, 1), flag="*", payee=None,
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

    h = assess(
        loan, [funding], _FakeConn(), settings=None,
        as_of=date(2025, 12, 15),
        coverage=coverage,
        scaffolding=None,  # disable auto-check
    )

    missing_actions = [a for a in h.next_actions if a.kind == "missing-payment"]
    long_gap_actions = [a for a in h.next_actions if a.kind == "long-payment-gap"]
    assert missing_actions == []
    assert len(long_gap_actions) == 1
    assert "11" in long_gap_actions[0].title  # "Long gap in payments (11 months)"


def test_short_gap_emits_individual_missing_actions_in_health():
    """Converse: 2 missing (below threshold) → 2 individual missing-payment
    actions, no long-payment-gap."""
    from lamella.features.loans.health import assess

    class _FakeConn:
        def execute(self, sql, params=()):
            class _C:
                def fetchone(self_inner):
                    return None

                def fetchall(self_inner):
                    return []
            return _C()

    loan = _loan()
    schedule = build_schedule(loan, as_of=date(2025, 5, 15))  # n=4
    actuals = [
        ActualPayment(txn_hash="h1", date=date(2025, 4, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
        ActualPayment(txn_hash="h2", date=date(2025, 5, 1),
                      principal_leg=Decimal("100"), interest_leg=Decimal("400"),
                      escrow_leg=Decimal("0"),
                      total=Decimal("500"), auto_classified=False),
    ]
    coverage = match(schedule, actuals)
    assert coverage.long_gap_detected is False
    funding = Transaction(
        meta={"lamella-loan-slug": "M", "filename": "x", "lineno": 1},
        date=date(2025, 1, 1), flag="*", payee=None,
        narration="fund", tags={"lamella-loan-funding"}, links=set(),
        postings=[
            Posting(account="Liabilities:Personal:Bank:M",
                    units=Amount(Decimal("-100000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Equity:Personal:OpeningBalances",
                    units=Amount(Decimal("100000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )

    h = assess(
        loan, [funding], _FakeConn(), settings=None,
        as_of=date(2025, 5, 15), coverage=coverage, scaffolding=None,
    )
    missing_actions = [a for a in h.next_actions if a.kind == "missing-payment"]
    long_gap_actions = [a for a in h.next_actions if a.kind == "long-payment-gap"]
    assert len(missing_actions) == 2
    assert long_gap_actions == []


# ------------------------------------------------------------ coverage_for


def test_coverage_for_returns_none_without_term():
    loan = _loan(term_months=None)
    result = coverage_for(loan, [], as_of=date(2025, 6, 15))
    assert result is None


def test_coverage_for_chains_build_extract_match():
    loan = _loan()
    # First payment 2025-02-01; as_of 2025-04-15 → n=3.
    # Pay Feb and Mar only.
    entries = [
        _payment_txn(date(2025, 2, 1),
                     principal=Decimal("100"), interest=Decimal("400")),
        _payment_txn(date(2025, 3, 1),
                     principal=Decimal("100"), interest=Decimal("400")),
    ]
    report = coverage_for(loan, entries, as_of=date(2025, 4, 15))
    assert report is not None
    assert report.expected_count == 3
    assert report.matched_count == 2
    assert report.missing_count == 1
    assert report.long_gap_detected is False
