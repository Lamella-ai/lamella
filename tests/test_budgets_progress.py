# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.features.budgets.models import Budget, BudgetPeriod
from lamella.features.budgets.progress import period_window, progress_for_budget


def _budget(*, period: BudgetPeriod = BudgetPeriod.MONTHLY, amount: float = 500.0) -> Budget:
    return Budget(
        id=1,
        label="Supplies",
        entity="Acme",
        account_pattern=r"Expenses:Acme:Supplies",
        period=period,
        amount=Decimal(str(amount)),
        alert_threshold=0.8,
    )


def _ledger(tmp_path: Path, txns_text: str) -> Path:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Liabilities:Acme:Card USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n"
        "2023-01-01 open Expenses:Acme:Other USD\n",
        encoding="utf-8",
    )
    main.write_text(
        'option "title" "x"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        + txns_text,
        encoding="utf-8",
    )
    return main


def test_period_window_monthly():
    start, end = period_window(BudgetPeriod.MONTHLY, today=date(2026, 4, 20))
    assert start == date(2026, 4, 1)
    assert end == date(2026, 5, 1)


def test_period_window_quarterly_q2():
    start, end = period_window(BudgetPeriod.QUARTERLY, today=date(2026, 5, 20))
    assert start == date(2026, 4, 1)
    assert end == date(2026, 7, 1)


def test_period_window_annual():
    start, end = period_window(BudgetPeriod.ANNUAL, today=date(2026, 7, 1))
    assert start == date(2026, 1, 1)
    assert end == date(2027, 1, 1)


def test_progress_excludes_other_entity_and_other_account(tmp_path: Path):
    main = _ledger(
        tmp_path,
        '2026-04-05 * "Acme" "supplies"\n'
        "  Liabilities:Acme:Card -120.00 USD\n"
        "  Expenses:Acme:Supplies 120.00 USD\n\n"
        '2026-04-10 * "Other" "shipping"\n'
        "  Liabilities:Acme:Card -50.00 USD\n"
        "  Expenses:Acme:Other 50.00 USD\n",
    )
    reader = LedgerReader(main)
    progress = progress_for_budget(_budget(), reader.load().entries, today=date(2026, 4, 20))
    assert progress.spent == Decimal("120.00")
    assert pytest.approx(progress.ratio, rel=1e-6) == 120.0 / 500.0
    assert progress.band() == "green"


def test_progress_yellow_at_threshold(tmp_path: Path):
    main = _ledger(
        tmp_path,
        '2026-04-05 * "Acme" "supplies"\n'
        "  Liabilities:Acme:Card -420.00 USD\n"
        "  Expenses:Acme:Supplies 420.00 USD\n",
    )
    reader = LedgerReader(main)
    progress = progress_for_budget(_budget(amount=500), reader.load().entries, today=date(2026, 4, 20))
    assert progress.band() == "yellow"


def test_progress_red_when_over(tmp_path: Path):
    main = _ledger(
        tmp_path,
        '2026-04-05 * "Acme" "supplies"\n'
        "  Liabilities:Acme:Card -600.00 USD\n"
        "  Expenses:Acme:Supplies 600.00 USD\n",
    )
    reader = LedgerReader(main)
    progress = progress_for_budget(_budget(amount=500), reader.load().entries, today=date(2026, 4, 20))
    assert progress.band() == "red"
    assert progress.ratio > 1.0
