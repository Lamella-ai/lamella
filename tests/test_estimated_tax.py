# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.features.reports.estimated_tax import compute_entity_rows


def _ledger(tmp_path: Path) -> LedgerReader:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Acme:Checking USD\n"
        "2023-01-01 open Income:Acme:Sales USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n"
        "2023-01-01 open Assets:Personal:Checking USD\n"
        "2023-01-01 open Income:Personal:Wages USD\n"
        "2023-01-01 open Expenses:Personal:Groceries USD\n"
        "2023-01-01 open Equity:Acme:Opening-Balances USD\n"
        "2023-01-01 open Equity:Personal:Opening-Balances USD\n",
        encoding="utf-8",
    )
    main.write_text(
        'option "title" "x"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2026-02-01 * "Customer" "Sale"\n'
        "  Assets:Acme:Checking 5000.00 USD\n"
        "  Income:Acme:Sales -5000.00 USD\n\n"
        '2026-02-15 * "Vendor" "Supplies"\n'
        "  Assets:Acme:Checking -1000.00 USD\n"
        "  Expenses:Acme:Supplies 1000.00 USD\n\n"
        '2026-03-15 * "Employer" "Wages"\n'
        "  Assets:Personal:Checking 3000.00 USD\n"
        "  Income:Personal:Wages -3000.00 USD\n\n"
        '2026-03-20 * "Grocery Store" "Groceries"\n'
        "  Assets:Personal:Checking -200.00 USD\n"
        "  Expenses:Personal:Groceries 200.00 USD\n",
        encoding="utf-8",
    )
    return LedgerReader(main)


def test_q1_two_entities(tmp_path: Path):
    reader = _ledger(tmp_path)
    rows = compute_entity_rows(
        year=2026, quarter=1, rate=Decimal("0.25"), entries=reader.load().entries,
    )
    by_ent = {r.entity: r for r in rows}
    assert by_ent["Acme"].income == Decimal("5000.00")
    assert by_ent["Acme"].expenses == Decimal("1000.00")
    assert by_ent["Acme"].net == Decimal("4000.00")
    assert by_ent["Acme"].estimated_tax == Decimal("1000.00")
    assert by_ent["Personal"].net == Decimal("2800.00")
    assert by_ent["Personal"].estimated_tax == Decimal("700.00")


def test_invalid_quarter_raises(tmp_path: Path):
    reader = _ledger(tmp_path)
    with pytest.raises(ValueError):
        compute_entity_rows(year=2026, quarter=5, rate=Decimal("0.25"), entries=reader.load().entries)


def test_negative_net_owes_zero(tmp_path: Path):
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Personal:Checking USD\n"
        "2023-01-01 open Income:Personal:Wages USD\n"
        "2023-01-01 open Expenses:Personal:Rent USD\n"
        "2023-01-01 open Equity:Personal:Opening-Balances USD\n",
        encoding="utf-8",
    )
    main.write_text(
        'option "title" "x"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2026-02-01 * "Employer" "Wages"\n'
        "  Assets:Personal:Checking 100.00 USD\n"
        "  Income:Personal:Wages -100.00 USD\n\n"
        '2026-02-15 * "Landlord" "Rent"\n'
        "  Assets:Personal:Checking -1000.00 USD\n"
        "  Expenses:Personal:Rent 1000.00 USD\n",
        encoding="utf-8",
    )
    reader = LedgerReader(main)
    rows = compute_entity_rows(year=2026, quarter=1, rate=Decimal("0.25"), entries=reader.load().entries)
    by_ent = {r.entity: r for r in rows}
    assert by_ent["Personal"].net == Decimal("-900.00")
    assert by_ent["Personal"].estimated_tax == Decimal("0.00")
