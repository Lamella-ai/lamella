# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""AI-AGENT.md Phase 2 — classify pipeline widens beyond Expenses.

Verifies:
- ``valid_accounts_by_root`` returns the right Open'd accounts for
  each root, filters FIXME leaves, respects entity scoping.
- ``build_classify_context`` infers the FIXME root from the txn and
  routes target_roots + whitelist accordingly.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data as bdata
from beancount.core.amount import Amount
from beancount.core.number import D

from lamella.features.ai_cascade.classify import build_classify_context
from lamella.features.ai_cascade.context import (
    valid_accounts_by_root,
    valid_expense_accounts,
)
from lamella.core.db import connect, migrate


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _open(acct: str, d: date = date(2020, 1, 1)) -> bdata.Open:
    return bdata.Open(
        meta={"filename": "main.bean", "lineno": 1},
        date=d, account=acct, currencies=None, booking=None,
    )


def _txn(
    *, d: date, payee: str | None, narration: str | None,
    postings: list[tuple[str, str]],
) -> bdata.Transaction:
    return bdata.Transaction(
        meta={"filename": "main.bean", "lineno": 10},
        date=d, flag="*", payee=payee, narration=narration,
        tags=frozenset(), links=frozenset(),
        postings=[
            bdata.Posting(
                account=acct, units=Amount(D(amt), "USD"),
                cost=None, price=None, flag=None, meta=None,
            )
            for acct, amt in postings
        ],
    )


class TestValidAccountsByRoot:
    def test_expenses_root(self):
        entries = [
            _open("Expenses:Acme:Supplies"),
            _open("Expenses:Personal:Meals"),
            _open("Expenses:Acme:FIXME"),  # excluded
            _open("Income:Acme:Sales"),    # wrong root
        ]
        accts = valid_accounts_by_root(entries, root="Expenses", entity=None)
        assert accts == [
            "Expenses:Acme:Supplies", "Expenses:Personal:Meals",
        ]

    def test_income_root_entity_filter(self):
        entries = [
            _open("Income:Acme:Sales"),
            _open("Income:Acme:Consulting"),
            _open("Income:Personal:Interest"),
            _open("Income:FIXME"),            # FIXME skipped
            _open("Expenses:Acme:Supplies"),  # wrong root
        ]
        accts = valid_accounts_by_root(entries, root="Income", entity="Acme")
        assert accts == ["Income:Acme:Consulting", "Income:Acme:Sales"]

    def test_liabilities_root(self):
        entries = [
            _open("Liabilities:CreditCard:Chase4422"),
            _open("Liabilities:Loans:Mortgage"),
            _open("Liabilities:FIXME"),       # FIXME skipped
        ]
        accts = valid_accounts_by_root(
            entries, root="Liabilities", entity=None,
        )
        assert accts == [
            "Liabilities:CreditCard:Chase4422",
            "Liabilities:Loans:Mortgage",
        ]

    def test_expense_compat_alias(self):
        """valid_expense_accounts still works (delegates to
        valid_accounts_by_root)."""
        entries = [_open("Expenses:Acme:Supplies")]
        assert (
            valid_expense_accounts(entries, entity=None)
            == valid_accounts_by_root(entries, root="Expenses", entity=None)
        )


class TestClassifyTypeInference:
    """build_classify_context infers the FIXME root and uses the
    right whitelist + vector-query scope."""

    def _opens(self):
        return [
            _open("Assets:Personal:Checking"),
            _open("Assets:Acme:Checking"),
            _open("Liabilities:Acme:Card:0123"),
            _open("Liabilities:Personal:CreditCard:Chase4422"),
            _open("Expenses:Acme:Supplies"),
            _open("Expenses:Personal:Meals"),
            _open("Income:Acme:Sales"),
            _open("Income:Personal:Consulting"),
        ]

    def test_expense_fixme_picks_expense_whitelist(self, conn):
        txn = _txn(
            d=date(2026, 4, 1), payee="Hardware Store", narration="lumber",
            postings=[
                ("Liabilities:Acme:Card:0123", "-50"),
                ("Expenses:Acme:FIXME", "50"),
            ],
        )
        entries = self._opens() + [txn]
        view, similar, accounts, *_ = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
        assert view is not None
        assert all(a.startswith("Expenses:") for a in accounts)
        assert "Expenses:Acme:Supplies" in accounts

    def test_income_fixme_picks_income_whitelist(self, conn):
        # Source account is Acme-entity-labeled, so the entity
        # resolver picks Acme, and Income whitelist filters to
        # Income:Acme:*.
        txn = _txn(
            d=date(2026, 4, 14), payee="ATM", narration="cash deposit",
            postings=[
                ("Assets:Acme:Checking", "800"),
                ("Income:FIXME", "-800"),
            ],
        )
        entries = self._opens() + [txn]
        view, similar, accounts, *_ = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
        assert view is not None
        assert view.fixme_account == "Income:FIXME"
        assert all(a.startswith("Income:Acme:") for a in accounts)
        assert "Income:Acme:Sales" in accounts

    def test_liabilities_fixme_picks_liability_whitelist(self, conn):
        # Source = Personal checking → entity=Personal → whitelist
        # filters to Liabilities:Personal:*.
        txn = _txn(
            d=date(2026, 4, 15), payee="Chase",
            narration="PAYMENT THANK YOU",
            postings=[
                ("Assets:Personal:Checking", "-500"),
                ("Liabilities:FIXME", "500"),
            ],
        )
        entries = self._opens() + [txn]
        view, similar, accounts, *_ = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
        assert view is not None
        assert view.fixme_account == "Liabilities:FIXME"
        assert all(a.startswith("Liabilities:Personal:") for a in accounts)
        assert "Liabilities:Personal:CreditCard:Chase4422" in accounts

    def test_no_fixme_returns_none(self, conn):
        """Txn with no FIXME leg falls through — build_classify_context
        returns the empty tuple. Unchanged by Phase 2."""
        txn = _txn(
            d=date(2026, 4, 1), payee="Hardware Store", narration="lumber",
            postings=[
                ("Liabilities:Acme:Card:0123", "-50"),
                ("Expenses:Acme:Supplies", "50"),
            ],
        )
        entries = self._opens() + [txn]
        view, *_ = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
        assert view is None
