# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP1 — loan health model.

Purity, sub-reports, and next-action ordering. Zero I/O: every fixture
is a dict + a synthesized list of Beancount entries built with
beancount.core.data helpers, so the tests run on the pure function
directly with no server, no temp ledger, no AI.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from beancount.core.data import Posting, Transaction
from beancount.core.amount import Amount

from lamella.features.loans.health import (
    LoanHealth,
    NextAction,
    ScaffoldingReport,
    _sort_actions,
    assess as _assess_full,
)


# WP1 tests stay focused on WP1's own logic. By default assess() will
# auto-call scaffolding.check() (WP4), which needs a conn that handles
# more SQL shapes than WP1's fake. Pass an empty ScaffoldingReport to
# isolate the tests that aren't about scaffolding.
_EMPTY_SCAFFOLDING = ScaffoldingReport(issues=[])


def assess(loan, entries, conn, settings=None, *, as_of=None,
           scaffolding=None, coverage=None, anomalies=None):
    """Local wrapper that defaults scaffolding to empty. Tests that
    exercise scaffolding integration pass one explicitly."""
    if scaffolding is None:
        scaffolding = _EMPTY_SCAFFOLDING
    return _assess_full(
        loan, entries, conn, settings, as_of=as_of,
        scaffolding=scaffolding, coverage=coverage, anomalies=anomalies,
    )


# ------------------------------------------------------------------ fixtures


def _loan(**overrides) -> dict:
    base = {
        "slug": "MainResidenceMortgage",
        "display_name": "Main Residence Mortgage",
        "loan_type": "mortgage",
        "entity_slug": "Personal",
        "institution": "BankTwo",
        "original_principal": "550000.00",
        "funded_date": "2025-10-27",
        "first_payment_date": "2025-11-01",
        "payment_due_day": 1,
        "term_months": 360,
        "interest_rate_apr": "6.625",
        "monthly_payment_estimate": "3521.64",
        "escrow_monthly": "850.00",
        "property_tax_monthly": "600.00",
        "insurance_monthly": "200.00",
        "liability_account_path": "Liabilities:Personal:BankTwo:MainResidenceMortgage",
        "interest_account_path": "Expenses:Personal:MainResidenceMortgage:Interest",
        "escrow_account_path": "Assets:Personal:BankTwo:MainResidenceMortgage:Escrow",
        "property_slug": "MainResidence",
        "is_active": 1,
    }
    base.update(overrides)
    return base


def _funding_txn(slug: str, d: date) -> Transaction:
    return Transaction(
        meta={"lamella-loan-slug": slug, "filename": "x", "lineno": 1},
        date=d, flag="*", payee=None,
        narration=f"Loan funding — {slug}",
        tags={"lamella-loan-funding"}, links=set(),
        postings=[
            Posting(
                account="Liabilities:Personal:BankTwo:MainResidenceMortgage",
                units=Amount(Decimal("-550000.00"), "USD"),
                cost=None, price=None, flag=None, meta={},
            ),
            Posting(
                account="Assets:Personal:Property:MainResidence:CostBasis",
                units=Amount(Decimal("550000.00"), "USD"),
                cost=None, price=None, flag=None, meta={},
            ),
        ],
    )


class _FakeConn:
    """Minimal sqlite-ish connection for tests that only need
    ``loan_balance_anchors`` reads."""

    def __init__(self, anchors: list[tuple[str, str]] | None = None):
        # (loan_slug, as_of_date ISO)
        self.anchors = anchors or []

    def execute(self, sql: str, params: tuple):
        slug = params[0] if params else ""
        matched = [a for a in self.anchors if a[0] == slug]
        # DESC by as_of_date
        matched.sort(key=lambda x: x[1], reverse=True)
        rows = [(a[1],) for a in matched]

        class _Cursor:
            def __init__(self, rs):
                self._rs = rs

            def fetchall(self):
                return self._rs

        return _Cursor(rows)


# ------------------------------------------------------------------ assess()


def test_assess_is_pure_deterministic():
    loan = _loan()
    entries: list = []
    conn = _FakeConn()

    h1 = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))
    h2 = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h1 == h2, "same inputs must yield byte-equal health records"


def test_unfunded_loan_surfaces_blocking_fund_initial():
    loan = _loan()
    entries: list = []
    conn = _FakeConn()

    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.summary_badge == "blocking"
    assert h.funding.is_funded is False
    # fund-initial is the first blocker since no scaffolding issues
    # exist (WP1 ships empty scaffolding).
    assert h.next_actions[0].kind == "fund-initial"
    assert h.next_actions[0].severity == "blocking"


def test_funded_loan_no_anchors_surfaces_add_anchor():
    loan = _loan()
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn()

    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.funding.is_funded is True
    assert h.funding.funding_date == date(2025, 10, 27)
    assert h.funding.offset_account == "Assets:Personal:Property:MainResidence:CostBasis"
    kinds = [a.kind for a in h.next_actions]
    assert "fund-initial" not in kinds
    assert "add-anchor" in kinds


def test_stale_anchor_surfaces_refresh_action():
    loan = _loan()
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn(anchors=[("MainResidenceMortgage", "2025-11-01")])
    # 2026-04-24 is ~174 days after 2025-11-01 — well past the 90d threshold.
    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.anchor_freshness.anchor_count == 1
    assert h.anchor_freshness.is_stale is True
    kinds = [a.kind for a in h.next_actions]
    assert "stale-anchor" in kinds
    assert "add-anchor" not in kinds  # exclusive-or


def test_fresh_anchor_does_not_stale_flag():
    loan = _loan()
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn(anchors=[("MainResidenceMortgage", "2026-04-01")])
    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.anchor_freshness.is_stale is False
    kinds = [a.kind for a in h.next_actions]
    assert "stale-anchor" not in kinds
    assert "add-anchor" not in kinds


def test_escrow_shortage_projected_when_inflow_below_outflow():
    # 400/mo escrow but 600 tax + 200 insurance = 800/mo outflow. Clear shortage.
    loan = _loan(escrow_monthly="400.00", property_tax_monthly="600.00",
                 insurance_monthly="200.00")
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn(anchors=[("MainResidenceMortgage", "2026-04-01")])
    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.escrow is not None
    assert h.escrow.projected_shortage is True
    kinds = [a.kind for a in h.next_actions]
    assert "escrow-shortage-projected" in kinds


def test_no_escrow_configured_hides_escrow_report():
    loan = _loan(escrow_monthly=None)
    entries: list = []
    conn = _FakeConn()
    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.escrow is None
    kinds = [a.kind for a in h.next_actions]
    assert "escrow-shortage-projected" not in kinds


def test_fully_healthy_loan_reports_ok():
    loan = _loan(escrow_monthly=None, property_tax_monthly=None,
                 insurance_monthly=None)
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn(anchors=[("MainResidenceMortgage", "2026-04-10")])

    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.summary_badge == "ok"
    assert h.next_actions == []


# ------------------------------------------------------- next-action ordering


def _na(kind: str, severity: str = "attention", priority: int = 500,
        **payload) -> NextAction:
    return NextAction(
        kind=kind, severity=severity, title=kind, detail="", action_label="go",
        method="GET", endpoint="/", payload=payload, priority=priority,
    )


def test_sort_places_blocking_before_attention_before_info():
    actions = [
        _na("info-a", severity="info", priority=10),
        _na("attention-a", severity="attention", priority=10),
        _na("blocking-a", severity="blocking", priority=10),
    ]
    sorted_ = _sort_actions(actions)
    assert [a.kind for a in sorted_] == [
        "blocking-a", "attention-a", "info-a",
    ]


def test_sort_stable_key_breaks_kind_ties_by_payload():
    # Two missing-payment actions — expected_date is the stable key.
    actions = [
        _na("missing-payment", expected_date="2026-02-01"),
        _na("missing-payment", expected_date="2026-01-01"),
        _na("missing-payment", expected_date="2026-03-01"),
    ]
    sorted_ = _sort_actions(actions)
    dates = [a.payload["expected_date"] for a in sorted_]
    assert dates == ["2026-01-01", "2026-02-01", "2026-03-01"]


def test_sort_is_stable_for_full_ties():
    # Same severity/priority/kind/stable_key → insertion order preserved.
    actions = [_na("record-payment", txn_hash="h1"),
               _na("record-payment", txn_hash="h1"),
               _na("record-payment", txn_hash="h1")]
    # Attach id() markers to check identity preservation.
    sorted_ = _sort_actions(actions)
    assert [id(a) for a in sorted_] == [id(a) for a in actions]


def test_priority_orders_within_severity_bucket():
    """Two blockers with different priorities — the one with the lower
    priority integer must come first, regardless of insertion order.

    Guards against the regression where sort-by-severity-only lets
    fund-initial (priority 20) jump ahead of scaffolding-open-missing
    (priority 10) just because it was added to the list first.
    """
    from lamella.features.loans.next_action_priorities import priority_for

    # Deliberately add fund-initial FIRST to prove priority wins.
    actions = [
        _na("fund-initial", severity="blocking",
            priority=priority_for("fund-initial")),
        _na("scaffolding-open-missing", severity="blocking",
            priority=priority_for("scaffolding-open-missing")),
    ]
    sorted_ = _sort_actions(actions)
    assert sorted_[0].kind == "scaffolding-open-missing"
    assert sorted_[1].kind == "fund-initial"


def test_coverage_injects_missing_payment_actions():
    """WP3 integration — coverage report with `missing` rows folds
    into individual missing-payment next-actions (as long as the
    long-gap threshold isn't crossed)."""
    from lamella.features.loans.coverage import (
        ActualPayment,
        CoverageReport,
        CoverageRow,
        ExpectedPayment,
    )
    from decimal import Decimal

    def _expected(n: int, d: date) -> ExpectedPayment:
        return ExpectedPayment(
            n=n, expected_date=d,
            principal=Decimal("100"), interest=Decimal("200"),
            escrow=Decimal("0"), tax=Decimal("0"), insurance=Decimal("0"),
            total=Decimal("300"),
        )

    # 2 missing rows with one matched on_time in between — consecutive
    # missing run is only 1 → long-gap NOT detected → individual
    # missing-payment actions.
    coverage = CoverageReport(
        rows=[
            CoverageRow(expected=_expected(1, date(2025, 11, 1)),
                        actual=None, status="missing"),
            CoverageRow(
                expected=_expected(2, date(2025, 12, 1)),
                actual=ActualPayment(
                    txn_hash="h", date=date(2025, 12, 1),
                    principal_leg=Decimal("100"), interest_leg=Decimal("200"),
                    escrow_leg=Decimal("0"), total=Decimal("300"),
                    auto_classified=False,
                ),
                status="on_time",
            ),
            CoverageRow(expected=_expected(3, date(2026, 1, 1)),
                        actual=None, status="missing"),
        ],
        expected_count=3, matched_count=1, missing_count=2,
        late_count=0, extras=[],
        long_gap_detected=False, long_gap_start=None,
        long_gap_end=None, long_gap_count=0,
    )

    loan = _loan()
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn(anchors=[("MainResidenceMortgage", "2026-04-10")])

    h = assess(
        loan, entries, conn, settings=None,
        as_of=date(2026, 4, 24), coverage=coverage,
    )

    missing = [a for a in h.next_actions if a.kind == "missing-payment"]
    assert len(missing) == 2
    # Stable key ensures earlier expected_date sorts first.
    dates = [a.payload["expected_date"] for a in missing]
    assert dates == sorted(dates)
    # long-payment-gap did NOT fire (consecutive run was only 1).
    assert not any(a.kind == "long-payment-gap" for a in h.next_actions)


def test_scaffolding_issues_folded_in_as_blockers():
    """WP4 integration hook — scaffolding issues arrive as objects
    with kind/severity/message/can_autofix/fix_endpoint/path attrs."""
    class _Issue:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    report = ScaffoldingReport(issues=[
        _Issue(
            kind="open-missing", severity="blocking",
            path="Assets:Personal:BankTwo:MainResidenceMortgage:Escrow",
            message="Escrow account has no Open directive",
            can_autofix=True,
            fix_endpoint="/settings/loans/MainResidenceMortgage/autofix",
            fix_payload={"kind": "open-missing",
                         "path": "Assets:Personal:BankTwo:MainResidenceMortgage:Escrow"},
        ),
    ])

    loan = _loan()
    entries = [_funding_txn("MainResidenceMortgage", date(2025, 10, 27))]
    conn = _FakeConn(anchors=[("MainResidenceMortgage", "2026-04-10")])

    h = assess(
        loan, entries, conn, settings=None,
        as_of=date(2026, 4, 24), scaffolding=report,
    )

    assert h.summary_badge == "blocking"
    assert h.next_actions[0].kind == "scaffolding-open-missing"
    assert h.next_actions[0].severity == "blocking"
    assert h.scaffolding.has_blockers is True
