# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP13 — properties.loans_summary aggregation helper.

Covers:
  - empty case (property with no loans)
  - single mortgage with current balance walked from postings
  - HELOC with credit_limit + headroom calc
  - mortgage + HELOC combined roll-up
  - inactive loans excluded by default
  - revolving rows skipped from combined_monthly
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction
from beancount.core.number import D as Bn

from lamella.core.db import migrate
from lamella.features.properties.loans_summary import loans_for_property


# --------------------------------------------------------------- helpers


def _conn() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    return db


def _seed_property(db, slug: str = "MainResidence") -> None:
    db.execute(
        "INSERT INTO properties (slug, display_name, property_type, is_active) "
        "VALUES (?, ?, ?, 1)",
        (slug, "Main Residence", "house"),
    )


def _seed_loan(
    db, slug: str, *, property_slug: str = "MainResidence",
    is_revolving: int = 0, credit_limit: str | None = None,
    monthly: str | None = None, principal: str = "100000",
    is_active: int = 1,
    liability_path: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO loans (slug, loan_type, original_principal, funded_date,
                           property_slug, is_active, is_revolving, credit_limit,
                           monthly_payment_estimate, liability_account_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            slug, "mortgage" if not is_revolving else "heloc",
            principal, "2020-01-01", property_slug, is_active,
            is_revolving, credit_limit, monthly,
            liability_path or f"Liabilities:Personal:Bank:{slug}",
        ),
    )


def _txn(d: date, postings: list[tuple[str, str]]) -> Transaction:
    return Transaction(
        meta={"filename": "x.bean", "lineno": 1},
        date=d, flag="*", payee=None, narration="",
        tags=set(), links=set(),
        postings=[
            Posting(account=acct, units=Amount(Bn(amt), "USD"),
                    cost=None, price=None, flag=None, meta={})
            for acct, amt in postings
        ],
    )


# --------------------------------------------------------------- tests


def test_empty_property_returns_empty_summary():
    db = _conn()
    _seed_property(db)
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=[],
    )
    assert summary.loans == []
    assert summary.combined_balance == Decimal("0")
    assert summary.combined_monthly == Decimal("0")
    assert summary.has_revolving is False


def test_single_mortgage_balance_walked():
    db = _conn()
    _seed_property(db)
    _seed_loan(db, "Mort1", monthly="3500", principal="500000",
               liability_path="Liabilities:Personal:Bank:Mort1")
    # Funding (-500k liability) and one payment (+1000 to principal).
    entries = [
        _txn(date(2024, 1, 1), [
            ("Liabilities:Personal:Bank:Mort1", "-500000"),
            ("Equity:OpeningBalances", "500000"),
        ]),
        _txn(date(2024, 2, 1), [
            ("Liabilities:Personal:Bank:Mort1", "1000"),
            ("Assets:Personal:Bank:Checking", "-1000"),
        ]),
    ]
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=entries,
    )
    assert len(summary.loans) == 1
    loan = summary.loans[0]
    assert loan.slug == "Mort1"
    assert loan.is_revolving is False
    # 500k owed - 1k paid = 499k remaining (magnitude).
    assert loan.current_balance == Decimal("499000")
    assert loan.available_headroom is None
    assert summary.combined_balance == Decimal("499000")
    assert summary.combined_monthly == Decimal("3500")


def test_heloc_headroom():
    db = _conn()
    _seed_property(db)
    _seed_loan(db, "HELOC1", is_revolving=1, credit_limit="100000",
               principal="0",
               liability_path="Liabilities:Personal:Bank:HELOC1")
    entries = [
        # Two draws totaling 30k.
        _txn(date(2024, 3, 1), [
            ("Liabilities:Personal:Bank:HELOC1", "-20000"),
            ("Assets:Personal:Bank:Checking", "20000"),
        ]),
        _txn(date(2024, 4, 1), [
            ("Liabilities:Personal:Bank:HELOC1", "-10000"),
            ("Assets:Personal:Bank:Checking", "10000"),
        ]),
    ]
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=entries,
    )
    loan = summary.loans[0]
    assert loan.is_revolving is True
    assert loan.current_balance == Decimal("30000")
    assert loan.credit_limit == Decimal("100000")
    assert loan.available_headroom == Decimal("70000")
    # HELOC has no fixed monthly — combined_monthly stays 0.
    assert summary.combined_monthly == Decimal("0")
    assert summary.has_revolving is True


def test_mortgage_plus_heloc_combined():
    db = _conn()
    _seed_property(db)
    _seed_loan(db, "Mort1", monthly="3500", principal="500000",
               liability_path="Liabilities:Personal:Bank:Mort1")
    _seed_loan(db, "HELOC1", is_revolving=1, credit_limit="100000",
               principal="0",
               liability_path="Liabilities:Personal:Bank:HELOC1")
    entries = [
        _txn(date(2024, 1, 1), [
            ("Liabilities:Personal:Bank:Mort1", "-400000"),
            ("Equity:OpeningBalances", "400000"),
        ]),
        _txn(date(2024, 5, 1), [
            ("Liabilities:Personal:Bank:HELOC1", "-25000"),
            ("Assets:Personal:Bank:Checking", "25000"),
        ]),
    ]
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=entries,
    )
    assert len(summary.loans) == 2
    # combined_balance is sum of magnitudes.
    assert summary.combined_balance == Decimal("425000")
    assert summary.combined_monthly == Decimal("3500")  # mortgage only
    assert summary.combined_credit_limit == Decimal("100000")
    assert summary.combined_available_headroom == Decimal("75000")
    assert summary.has_revolving is True


def test_inactive_loans_excluded_by_default():
    db = _conn()
    _seed_property(db)
    _seed_loan(db, "ActiveMort", monthly="1000",
               liability_path="Liabilities:Personal:Bank:ActiveMort")
    _seed_loan(db, "PaidOffMort", monthly="2000", is_active=0,
               liability_path="Liabilities:Personal:Bank:PaidOffMort")
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=[],
    )
    assert len(summary.loans) == 1
    assert summary.loans[0].slug == "ActiveMort"


def test_inactive_loans_included_when_requested():
    db = _conn()
    _seed_property(db)
    _seed_loan(db, "ActiveMort", monthly="1000",
               liability_path="Liabilities:Personal:Bank:ActiveMort")
    _seed_loan(db, "PaidOffMort", monthly="2000", is_active=0,
               liability_path="Liabilities:Personal:Bank:PaidOffMort")
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=[],
        include_inactive=True,
    )
    assert len(summary.loans) == 2


def test_loan_without_liability_path_balance_is_zero():
    db = _conn()
    _seed_property(db)
    _seed_loan(db, "Orphan", liability_path=None)
    # liability_account_path is None — would be set during scaffolding.
    db.execute(
        "UPDATE loans SET liability_account_path = NULL WHERE slug = 'Orphan'"
    )
    summary = loans_for_property(
        property_slug="MainResidence", conn=db, entries=[],
    )
    assert summary.loans[0].current_balance == Decimal("0")
