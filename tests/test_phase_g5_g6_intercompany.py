# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Phase G5 (four-leg override) + G6 (settlement report)."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data as bdata
from beancount.core.amount import Amount
from beancount.core.number import D

from lamella.features.reports.intercompany import build_intercompany_report
from lamella.features.rules.overrides import (
    OverrideWriter,
    WriteError,
    _intercompany_override_block,
)


class TestInterCompanyBlock:
    def test_four_legs_balanced_and_signed_correctly(self):
        block = _intercompany_override_block(
            txn_date=date(2026, 4, 20),
            txn_hash="abc123",
            paying_entity="Acme",
            owning_entity="WidgetCo",
            card_account="Liabilities:Acme:Card:0123",
            expense_account="Expenses:WidgetCo:Supplies",
            amount=Decimal("100.00"),
        )
        # Card side negative (money leaving the card).
        assert "Liabilities:Acme:Card:0123  -100.00 USD" in block
        # DueFrom positive on paying entity's side.
        assert "Assets:Acme:DueFrom:WidgetCo  100.00 USD" in block
        # Expense positive on owning entity's side.
        assert "Expenses:WidgetCo:Supplies  100.00 USD" in block
        # DueTo negative on owning entity's side.
        assert "Liabilities:WidgetCo:DueTo:Acme  -100.00 USD" in block

    def test_metadata_and_tags_present(self):
        block = _intercompany_override_block(
            txn_date=date(2026, 4, 20),
            txn_hash="abc123",
            paying_entity="Acme",
            owning_entity="WidgetCo",
            card_account="Liabilities:Acme:Card:0123",
            expense_account="Expenses:WidgetCo:Supplies",
            amount=Decimal("100.00"),
        )
        assert "#lamella-override" in block
        assert "#lamella-intercompany" in block
        assert 'lamella-override-of:     "abc123"' in block
        assert "lamella-intercompany:    TRUE" in block
        assert 'lamella-paying-entity:   "Acme"' in block
        assert 'lamella-owning-entity:   "WidgetCo"' in block


class TestAppendIntercompany:
    def test_refuses_when_paying_equals_owning(self, tmp_path: Path):
        main = tmp_path / "main.bean"
        main.write_text('option "operating_currency" "USD"\n', encoding="utf-8")
        ov = tmp_path / "connector_overrides.bean"
        w = OverrideWriter(main_bean=main, overrides=ov, run_check=False)
        with pytest.raises(WriteError, match="paying_entity equals owning_entity"):
            w.append_intercompany(
                txn_date=date(2026, 4, 20),
                txn_hash="h",
                paying_entity="Acme",
                owning_entity="Acme",
                card_account="Liabilities:Acme:Card:1",
                expense_account="Expenses:Acme:Supplies",
                amount=Decimal("10"),
            )

    def test_writes_four_leg_block(self, tmp_path: Path, monkeypatch):
        main = tmp_path / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            '2020-01-01 open Liabilities:Acme:Card:0123 USD\n'
            '2020-01-01 open Assets:Acme:DueFrom:WidgetCo USD\n'
            '2020-01-01 open Expenses:WidgetCo:Supplies USD\n'
            '2020-01-01 open Liabilities:WidgetCo:DueTo:Acme USD\n'
            'include "connector_overrides.bean"\n',
            encoding="utf-8",
        )
        ov = tmp_path / "connector_overrides.bean"
        # Stub bean-check to skip the external call.
        monkeypatch.setattr(
            "lamella.features.rules.overrides.capture_bean_check",
            lambda _m: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.features.rules.overrides.run_bean_check_vs_baseline",
            lambda _m, _b: None,
        )
        w = OverrideWriter(main_bean=main, overrides=ov, run_check=True)
        w.append_intercompany(
            txn_date=date(2026, 4, 20),
            txn_hash="abc",
            paying_entity="Acme",
            owning_entity="WidgetCo",
            card_account="Liabilities:Acme:Card:0123",
            expense_account="Expenses:WidgetCo:Supplies",
            amount=Decimal("100"),
        )
        text = ov.read_text(encoding="utf-8")
        assert "#lamella-intercompany" in text
        assert "Assets:Acme:DueFrom:WidgetCo" in text
        assert "Liabilities:WidgetCo:DueTo:Acme" in text


# --- G6 settlement report -----------------------------------------------


def _txn(
    *, d: date, narration: str, postings: list[tuple[str, str]],
    tags: frozenset[str] = frozenset(),
) -> bdata.Transaction:
    return bdata.Transaction(
        meta={}, date=d, flag="*", payee=None, narration=narration,
        tags=tags, links=frozenset(),
        postings=[
            bdata.Posting(
                account=acct, units=Amount(D(amt), "USD"),
                cost=None, price=None, flag=None, meta=None,
            )
            for acct, amt in postings
        ],
    )


class TestIntercompanyReport:
    def test_single_outstanding_pair(self):
        """Acme paid a 100 WidgetCo expense. The receivable is
        on Acme; the payable on WidgetCo; net outstanding 100."""
        entries = [
            _txn(
                d=date(2026, 4, 20),
                narration="wrong-card",
                postings=[
                    ("Liabilities:Acme:Card:0123", "-100"),
                    ("Assets:Acme:DueFrom:WidgetCo", "100"),
                    ("Expenses:WidgetCo:Supplies", "100"),
                    ("Liabilities:WidgetCo:DueTo:Acme", "-100"),
                ],
                tags=frozenset({"lamella-intercompany"}),
            ),
        ]
        report = build_intercompany_report(entries, as_of=date(2099, 1, 1))
        assert len(report.balances) == 1
        pair = report.balances[0]
        assert pair.paying_entity == "Acme"
        assert pair.owing_entity == "WidgetCo"
        assert pair.outstanding == Decimal("100")
        assert not pair.is_settled
        assert len(report.outstanding_pairs) == 1
        assert len(report.settled_pairs) == 0

    def test_settlement_clears_balance(self):
        """After the owing entity settles, DueFrom drops to zero
        and the pair moves to settled."""
        entries = [
            _txn(
                d=date(2026, 4, 20),
                narration="wrong-card",
                postings=[
                    ("Liabilities:Acme:Card:0123", "-100"),
                    ("Assets:Acme:DueFrom:WidgetCo", "100"),
                    ("Expenses:WidgetCo:Supplies", "100"),
                    ("Liabilities:WidgetCo:DueTo:Acme", "-100"),
                ],
                tags=frozenset({"lamella-intercompany"}),
            ),
            _txn(
                d=date(2026, 5, 1),
                narration="intercompany settlement",
                postings=[
                    ("Liabilities:WidgetCo:DueTo:Acme", "100"),   # clears payable
                    ("Assets:Acme:DueFrom:WidgetCo", "-100"),     # clears receivable
                ],
                tags=frozenset({"lamella-intercompany-settlement"}),
            ),
        ]
        report = build_intercompany_report(entries, as_of=date(2099, 1, 1))
        assert len(report.balances) == 1
        pair = report.balances[0]
        assert pair.outstanding == Decimal("0")
        assert pair.is_settled
        assert len(report.settled_pairs) == 1

    def test_multiple_pairs_sorted_by_magnitude(self):
        """Largest-outstanding first so the report surfaces the
        most-urgent balance on top."""
        entries = [
            _txn(
                d=date(2026, 4, 1),
                narration="small",
                postings=[
                    ("Liabilities:Acme:Card:0001", "-10"),
                    ("Assets:Acme:DueFrom:WidgetCo", "10"),
                    ("Expenses:WidgetCo:X", "10"),
                    ("Liabilities:WidgetCo:DueTo:Acme", "-10"),
                ],
                tags=frozenset({"lamella-intercompany"}),
            ),
            _txn(
                d=date(2026, 4, 2),
                narration="big",
                postings=[
                    ("Liabilities:WidgetCo:Card:0002", "-500"),
                    ("Assets:WidgetCo:DueFrom:Personal", "500"),
                    ("Expenses:Personal:X", "500"),
                    ("Liabilities:Personal:DueTo:WidgetCo", "-500"),
                ],
                tags=frozenset({"lamella-intercompany"}),
            ),
        ]
        report = build_intercompany_report(entries, as_of=date(2099, 1, 1))
        assert len(report.balances) == 2
        assert report.balances[0].outstanding == Decimal("500")
        assert report.balances[1].outstanding == Decimal("10")

    def test_as_of_cutoff_excludes_later_transactions(self):
        """Settlement lands on May 1 but report as-of April 25
        should still show the 100 outstanding."""
        entries = [
            _txn(
                d=date(2026, 4, 20),
                narration="wrong-card",
                postings=[
                    ("Liabilities:Acme:Card:0001", "-100"),
                    ("Assets:Acme:DueFrom:WidgetCo", "100"),
                    ("Expenses:WidgetCo:X", "100"),
                    ("Liabilities:WidgetCo:DueTo:Acme", "-100"),
                ],
                tags=frozenset({"lamella-intercompany"}),
            ),
            _txn(
                d=date(2026, 5, 1),
                narration="settlement",
                postings=[
                    ("Liabilities:WidgetCo:DueTo:Acme", "100"),
                    ("Assets:Acme:DueFrom:WidgetCo", "-100"),
                ],
                tags=frozenset({"lamella-intercompany-settlement"}),
            ),
        ]
        report = build_intercompany_report(entries, as_of=date(2026, 4, 25))
        assert report.balances[0].outstanding == Decimal("100")
