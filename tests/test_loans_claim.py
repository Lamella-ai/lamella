# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP6 — loan claim detection (principle-3 preemption)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction

from lamella.features.loans.claim import (
    Claim,
    ClaimKind,
    claim_from_simplefin_facts,
    is_claimed_by_loan,
)


# ----------------------------------------------------- fake conn + fixtures


class _FakeConn:
    """Minimal conn — supports PRAGMA table_info and a parameterized
    SELECT so claim.py's column-list-detection path exercises the
    full code path with or without the WP13 columns present."""

    # Simulate a WP6+WP13 schema by default so revolving tests work.
    # Tests that want pre-WP13 schema pass schema_cols=None.
    _DEFAULT_COLS = (
        "slug", "liability_account_path", "interest_account_path",
        "escrow_account_path", "simplefin_account_id",
        "monthly_payment_estimate", "is_active",
        "escrow_monthly", "property_tax_monthly", "insurance_monthly",
        "is_revolving", "auto_classify_enabled",
    )

    def __init__(self, loans: list[dict], schema_cols: tuple | None = None):
        self._loans = loans
        self._cols = schema_cols if schema_cols is not None else self._DEFAULT_COLS

    def execute(self, sql: str, params=()):
        sql = sql.strip()

        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

        if "PRAGMA table_info" in sql:
            # Return (cid, name, type, notnull, dflt, pk) tuples.
            return _Cursor([
                (i, name, "TEXT", 0, None, 0)
                for i, name in enumerate(self._cols)
            ])

        # Parse SELECT to figure out which columns were requested.
        lower = sql.lower()
        if "from loans" in lower:
            # Extract the column list between SELECT and FROM.
            select_start = lower.index("select") + len("select")
            from_start = lower.index("from")
            col_list_raw = sql[select_start:from_start].strip()
            requested = [c.strip() for c in col_list_raw.split(",")]
            loans = [l for l in self._loans if l.get("is_active", 1)]
            rows = [
                tuple(l.get(col) for col in requested)
                for l in loans
            ]
            return _Cursor(rows)

        return _Cursor([])


def _loan(**o) -> dict:
    base = {
        "slug": "M",
        "liability_account_path": "Liabilities:Personal:Bank:M",
        "interest_account_path": "Expenses:Personal:M:Interest",
        "escrow_account_path": "Assets:Personal:Bank:M:Escrow",
        "simplefin_account_id": None,
        "monthly_payment_estimate": "500",
        "is_active": 1,
        "escrow_monthly": None,
        "property_tax_monthly": None,
        "insurance_monthly": None,
    }
    base.update(o)
    return base


def _txn(postings_spec: list[tuple[str, Decimal]], meta=None) -> Transaction:
    postings = [
        Posting(account=acct, units=Amount(amt, "USD"),
                cost=None, price=None, flag=None, meta={})
        for acct, amt in postings_spec
    ]
    return Transaction(
        meta=meta or {"filename": "x", "lineno": 1},
        date=date(2025, 3, 1), flag="*", payee=None, narration="test",
        tags=set(), links=set(), postings=postings,
    )


@dataclass
class _SFTxn:
    id: str
    account_id: str | None = None
    amount: Decimal = Decimal("500")


# ------------------------------------------------- is_claimed_by_loan()


def test_no_loans_means_no_claim():
    conn = _FakeConn(loans=[])
    txn = _txn([("Liabilities:Personal:Bank:M", Decimal("100"))])
    assert is_claimed_by_loan(txn, conn) is None


def test_claim_fires_on_liability_posting():
    conn = _FakeConn(loans=[_loan()])
    txn = _txn([
        ("Liabilities:Personal:Bank:M", Decimal("100")),  # paydown
        ("Expenses:Personal:M:Interest", Decimal("400")),
        ("Assets:Personal:Checking", Decimal("-500")),
    ])
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.kind == ClaimKind.PAYMENT
    assert c.loan_slug == "M"


def test_claim_fires_on_interest_posting_only():
    """A transaction that touches interest but not liability still
    claims (the loan's interest account is part of its tracked set)."""
    conn = _FakeConn(loans=[_loan()])
    txn = _txn([
        ("Expenses:Personal:M:Interest", Decimal("400")),
        ("Assets:Personal:Checking", Decimal("-400")),
    ])
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.loan_slug == "M"


def test_escrow_only_produces_escrow_disbursement():
    """Touches escrow but not liability — servicer paid out of escrow."""
    conn = _FakeConn(loans=[_loan()])
    txn = _txn([
        ("Assets:Personal:Bank:M:Escrow", Decimal("-4000")),
        ("Expenses:Personal:M:PropertyTax", Decimal("4000")),
    ])
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.kind == ClaimKind.ESCROW_DISBURSEMENT


def test_unrelated_transaction_not_claimed():
    conn = _FakeConn(loans=[_loan()])
    txn = _txn([
        ("Assets:Personal:Checking", Decimal("-100")),
        ("Expenses:Groceries", Decimal("100")),
    ])
    assert is_claimed_by_loan(txn, conn) is None


def test_simplefin_id_match_claims_when_accounts_dont():
    """The txn doesn't touch any loan account but carries a meta
    with a matching simplefin-account-id → claimed."""
    conn = _FakeConn(loans=[_loan(simplefin_account_id="sf-1234")])
    txn = _txn(
        [
            ("Assets:Personal:Checking", Decimal("-500")),
            ("Expenses:Personal:FIXME", Decimal("500")),
        ],
        meta={"lamella-simplefin-account-id": "sf-1234", "filename": "x", "lineno": 1},
    )
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.loan_slug == "M"
    assert c.kind == ClaimKind.PAYMENT


def test_tie_break_picks_loan_with_closest_expected_monthly():
    """Two loans share a dual-purpose account. Tie-break by closest
    expected_monthly to the txn amount."""
    loans = [
        _loan(slug="A",
              liability_account_path="Liabilities:Shared:Path",
              monthly_payment_estimate="300"),
        _loan(slug="B",
              liability_account_path="Liabilities:Shared:Path",
              monthly_payment_estimate="1000"),
    ]
    conn = _FakeConn(loans=loans)
    # Amount 950 → closer to B's $1000 than A's $300.
    txn = _txn([
        ("Liabilities:Shared:Path", Decimal("950")),
        ("Assets:Personal:Checking", Decimal("-950")),
    ])
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.loan_slug == "B"


def test_revolving_loan_paydown_is_skip():
    """Revolving loan + positive liability posting (paydown) →
    REVOLVING_SKIP, not PAYMENT."""
    loan = _loan(is_revolving=1)
    conn = _FakeConn(loans=[loan])
    txn = _txn([
        ("Liabilities:Personal:Bank:M", Decimal("100")),
        ("Assets:Personal:Checking", Decimal("-100")),
    ])
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.kind == ClaimKind.REVOLVING_SKIP


def test_revolving_loan_draw_is_draw():
    """Revolving + negative liability posting (balance increases) → DRAW."""
    loan = _loan(is_revolving=1)
    conn = _FakeConn(loans=[loan])
    txn = _txn([
        ("Liabilities:Personal:Bank:M", Decimal("-500")),  # draw
        ("Assets:Personal:Checking", Decimal("500")),
    ])
    c = is_claimed_by_loan(txn, conn)
    assert c is not None
    assert c.kind == ClaimKind.DRAW


def test_inactive_loans_ignored():
    conn = _FakeConn(loans=[_loan(is_active=0)])
    txn = _txn([
        ("Liabilities:Personal:Bank:M", Decimal("100")),
        ("Assets:Personal:Checking", Decimal("-100")),
    ])
    assert is_claimed_by_loan(txn, conn) is None


# ---------------------------------------------- claim_from_simplefin_facts()


def test_simplefin_facts_matches_on_source_account():
    """The ingest flow maps SimpleFIN account → source_account. When
    that path is a loan's liability, claim fires."""
    loans = [_loan(liability_account_path="Liabilities:Personal:Bank:M")]
    conn = _FakeConn(loans=loans)
    sf_txn = _SFTxn(id="sf-1", account_id="external-id-42")
    c = claim_from_simplefin_facts(sf_txn, "Liabilities:Personal:Bank:M", conn)
    assert c is not None
    assert c.loan_slug == "M"
    assert c.kind == ClaimKind.PAYMENT


def test_simplefin_facts_matches_on_account_id():
    loans = [_loan(simplefin_account_id="external-id-42")]
    conn = _FakeConn(loans=loans)
    sf_txn = _SFTxn(id="sf-1", account_id="external-id-42")
    c = claim_from_simplefin_facts(sf_txn, "Assets:Checking", conn)
    assert c is not None
    assert c.loan_slug == "M"


def test_simplefin_facts_no_match_returns_none():
    loans = [_loan(simplefin_account_id="other-id")]
    conn = _FakeConn(loans=loans)
    sf_txn = _SFTxn(id="sf-1", account_id="external-id-42")
    assert claim_from_simplefin_facts(sf_txn, "Assets:Groceries", conn) is None


def test_simplefin_facts_revolving_path():
    loans = [_loan(
        liability_account_path="Liabilities:Personal:HELOC",
        is_revolving=1,
    )]
    conn = _FakeConn(loans=loans)
    sf_txn = _SFTxn(id="sf-1", account_id=None)
    c = claim_from_simplefin_facts(sf_txn, "Liabilities:Personal:HELOC", conn)
    assert c is not None
    assert c.kind == ClaimKind.REVOLVING_SKIP
