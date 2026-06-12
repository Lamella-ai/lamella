# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP7 — escrow lifecycle helpers."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction

from lamella.features.loans.escrow import (
    EscrowFlow,
    EscrowYTD,
    build_reconciliation_block,
    escrow_flows,
    reconcile,
    running_balance,
    ytd_summary,
)


# -------------------------------------------------------------- fixtures


def _loan(**overrides) -> dict:
    base = {
        "slug": "M",
        "display_name": "Test",
        "entity_slug": "Personal",
        "escrow_account_path": "Assets:Personal:Bank:M:Escrow",
    }
    base.update(overrides)
    return base


def _mk_txn(
    d: date, *,
    escrow_amount: Decimal,
    counterpart: str,
    narration: str = "test",
) -> Transaction:
    return Transaction(
        meta={"filename": "x", "lineno": 1},
        date=d, flag="*", payee=None, narration=narration,
        tags=set(), links=set(),
        postings=[
            Posting(account="Assets:Personal:Bank:M:Escrow",
                    units=Amount(escrow_amount, "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account=counterpart,
                    units=Amount(-escrow_amount, "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )


# --------------------------------------------------------- escrow_flows()


def test_escrow_flows_returns_empty_without_path():
    loan = _loan(escrow_account_path=None)
    assert escrow_flows(loan, []) == []


def test_escrow_flows_classifies_inflow_vs_outflow_by_sign():
    """Positive escrow posting = money deposited (inflow).
    Negative = servicer disbursed (outflow)."""
    loan = _loan()
    entries = [
        # Inflow: payment deposited $250 in escrow.
        _mk_txn(date(2025, 1, 1), escrow_amount=Decimal("250"),
                counterpart="Liabilities:Personal:Bank:M"),
        # Outflow: servicer paid $4000 in property tax.
        _mk_txn(date(2025, 10, 15), escrow_amount=Decimal("-4000"),
                counterpart="Expenses:Personal:M:PropertyTax"),
    ]
    flows = escrow_flows(loan, entries)
    assert len(flows) == 2
    assert flows[0].kind == "inflow"
    assert flows[0].amount == Decimal("250")
    assert flows[1].kind == "outflow"
    assert flows[1].amount == Decimal("4000")


def test_escrow_flows_sorts_chronologically():
    loan = _loan()
    entries = [
        _mk_txn(date(2025, 6, 1), escrow_amount=Decimal("250"),
                counterpart="Liabilities:Personal:Bank:M"),
        _mk_txn(date(2025, 1, 1), escrow_amount=Decimal("250"),
                counterpart="Liabilities:Personal:Bank:M"),
        _mk_txn(date(2025, 3, 1), escrow_amount=Decimal("250"),
                counterpart="Liabilities:Personal:Bank:M"),
    ]
    flows = escrow_flows(loan, entries)
    dates = [f.date for f in flows]
    assert dates == [date(2025, 1, 1), date(2025, 3, 1), date(2025, 6, 1)]


def test_escrow_flows_picks_expense_counterpart_for_outflow():
    """When both an expense and an asset counterpart exist on an
    outflow transaction, prefer the expense — that's what the
    outflow is 'going to'."""
    loan = _loan()
    txn = Transaction(
        meta={"filename": "x", "lineno": 1},
        date=date(2025, 10, 15), flag="*", payee=None,
        narration="Property tax disbursement", tags=set(), links=set(),
        postings=[
            Posting(account="Assets:Personal:Bank:M:Escrow",
                    units=Amount(Decimal("-4000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Expenses:Personal:M:PropertyTax",
                    units=Amount(Decimal("4000"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )
    flows = escrow_flows(loan, [txn])
    assert flows[0].counterpart == "Expenses:Personal:M:PropertyTax"


def test_escrow_flows_ignores_zero_escrow_legs():
    """A transaction with a 0-amount escrow posting isn't a flow."""
    loan = _loan()
    txn = Transaction(
        meta={"filename": "x", "lineno": 1},
        date=date(2025, 1, 1), flag="*", payee=None,
        narration="stub", tags=set(), links=set(),
        postings=[
            Posting(account="Assets:Personal:Bank:M:Escrow",
                    units=Amount(Decimal("0"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )
    assert escrow_flows(loan, [txn]) == []


# ------------------------------------------------------ running_balance()


def test_running_balance_alternates_with_flow_signs():
    flows = [
        EscrowFlow(date(2025, 1, 1), Decimal("250"),
                   "Liabilities:M", "n", "h1", "inflow"),
        EscrowFlow(date(2025, 2, 1), Decimal("250"),
                   "Liabilities:M", "n", "h2", "inflow"),
        EscrowFlow(date(2025, 3, 1), Decimal("100"),
                   "Expenses:Tax", "n", "h3", "outflow"),
    ]
    balances = running_balance(flows)
    assert balances == [
        (date(2025, 1, 1), Decimal("250")),
        (date(2025, 2, 1), Decimal("500")),
        (date(2025, 3, 1), Decimal("400")),
    ]


def test_running_balance_sorts_out_of_order_input():
    flows = [
        EscrowFlow(date(2025, 3, 1), Decimal("100"),
                   "Expenses:Tax", "n", "h3", "outflow"),
        EscrowFlow(date(2025, 1, 1), Decimal("250"),
                   "Liabilities:M", "n", "h1", "inflow"),
    ]
    balances = running_balance(flows)
    # Jan inflow first, then Mar outflow. Order doesn't matter on input.
    assert balances[0][0] == date(2025, 1, 1)
    assert balances[0][1] == Decimal("250")
    assert balances[1][0] == date(2025, 3, 1)
    assert balances[1][1] == Decimal("150")


# --------------------------------------------------------- ytd_summary()


def test_ytd_summary_aggregates_by_year():
    flows = [
        # 2025 inflows
        EscrowFlow(date(2025, 1, 1), Decimal("250"),
                   "L", "n", "h1", "inflow"),
        EscrowFlow(date(2025, 2, 1), Decimal("250"),
                   "L", "n", "h2", "inflow"),
        # 2025 outflow
        EscrowFlow(date(2025, 10, 1), Decimal("3000"),
                   "Expenses:Tax", "n", "h3", "outflow"),
        # 2024 — different year, ignored
        EscrowFlow(date(2024, 6, 1), Decimal("100"),
                   "L", "n", "h4", "inflow"),
    ]
    ytd = ytd_summary(flows, 2025)
    assert ytd.year == 2025
    assert ytd.total_in == Decimal("500")
    assert ytd.total_out == Decimal("3000")
    assert ytd.net == Decimal("-2500")
    assert ytd.biggest_outflow is not None
    assert ytd.biggest_outflow.txn_hash == "h3"


def test_ytd_summary_zero_without_flows_in_year():
    ytd = ytd_summary([], 2025)
    assert ytd.total_in == Decimal("0")
    assert ytd.total_out == Decimal("0")
    assert ytd.biggest_outflow is None


# ----------------------------------------------------------- reconcile()


def test_reconcile_flags_delta_beyond_tolerance():
    flows = [
        EscrowFlow(date(2025, 1, 1), Decimal("250"),
                   "L", "n", "h1", "inflow"),
        EscrowFlow(date(2025, 2, 1), Decimal("250"),
                   "L", "n", "h2", "inflow"),
    ]
    # Ledger balance at Feb 15 is 500; statement says 510 → delta 10.
    result = reconcile(
        flows, Decimal("510"), date(2025, 2, 15),
    )
    assert result.ledger_balance == Decimal("500")
    assert result.statement_balance == Decimal("510")
    assert result.delta == Decimal("10")
    assert result.needs_adjustment is True


def test_reconcile_within_tolerance_no_adjustment():
    flows = [
        EscrowFlow(date(2025, 1, 1), Decimal("250"),
                   "L", "n", "h1", "inflow"),
    ]
    result = reconcile(flows, Decimal("250.50"), date(2025, 1, 15))
    assert result.delta == Decimal("0.50")
    assert result.needs_adjustment is False


def test_reconcile_walks_only_up_to_statement_date():
    """A flow AFTER the statement date shouldn't influence the
    ledger-balance calculation for that date."""
    flows = [
        EscrowFlow(date(2025, 1, 1), Decimal("250"),
                   "L", "n", "h1", "inflow"),
        EscrowFlow(date(2025, 5, 1), Decimal("999"),
                   "L", "n", "h2", "inflow"),  # after statement date
    ]
    result = reconcile(flows, Decimal("250"), date(2025, 3, 1))
    assert result.ledger_balance == Decimal("250")
    assert result.delta == Decimal("0")
    assert result.needs_adjustment is False


# --------------------------------------------- build_reconciliation_block()


def test_build_reconciliation_block_carries_tag_and_meta():
    loan = _loan()
    block = build_reconciliation_block(
        loan,
        statement_date=date(2025, 3, 1),
        delta=Decimal("15.50"),
        offset_account="Expenses:Personal:M:EscrowAdjustment",
    )
    assert "#lamella-loan-escrow-reconcile" in block
    assert 'lamella-loan-slug: "M"' in block
    assert "lamella-loan-escrow-statement-date: 2025-03-01" in block
    # Delta signs: escrow gets +15.50, offset gets -15.50.
    assert "Assets:Personal:Bank:M:Escrow  15.50 USD" in block
    assert "Expenses:Personal:M:EscrowAdjustment  -15.50 USD" in block


def test_build_reconciliation_block_handles_negative_delta():
    loan = _loan()
    block = build_reconciliation_block(
        loan,
        statement_date=date(2025, 3, 1),
        delta=Decimal("-25.00"),
        offset_account="Expenses:Personal:M:EscrowAdjustment",
    )
    # Negative delta = outflow from escrow.
    assert "Assets:Personal:Bank:M:Escrow  -25.00 USD" in block
    assert "Expenses:Personal:M:EscrowAdjustment  25.00 USD" in block
