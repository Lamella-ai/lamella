# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import pytest

from lamella.features.budgets.models import BudgetPeriod, BudgetValidationError
from lamella.features.budgets.service import BudgetService


def _open_accounts() -> list[str]:
    return [
        "Expenses:Acme:Supplies",
        "Expenses:Acme:Shipping",
        "Assets:Acme:Checking",
    ]


def test_create_round_trip(db):
    service = BudgetService(db)
    b = service.create(
        label="Acme Supplies",
        entity="Acme",
        account_pattern=r"Expenses:Acme:Supplies",
        period="monthly",
        amount=500,
        open_accounts=_open_accounts(),
    )
    assert b.id > 0
    assert b.period == BudgetPeriod.MONTHLY
    fetched = service.get(b.id)
    assert fetched is not None
    assert fetched.label == "Acme Supplies"


def test_invalid_regex_rejects(db):
    service = BudgetService(db)
    with pytest.raises(BudgetValidationError, match="invalid regex"):
        service.create(
            label="bad", entity="Acme",
            account_pattern="Expenses:[unterminated",
            period="monthly", amount=100,
            open_accounts=_open_accounts(),
        )


def test_pattern_with_no_match_rejects(db):
    service = BudgetService(db)
    with pytest.raises(BudgetValidationError, match="matches no open"):
        service.create(
            label="typo",
            entity="Acme",
            account_pattern="Expenses:Acme:Supples",  # typo
            period="monthly",
            amount=100,
            open_accounts=_open_accounts(),
        )


def test_zero_amount_rejects(db):
    service = BudgetService(db)
    with pytest.raises(BudgetValidationError, match="amount"):
        service.create(
            label="zero", entity="Acme",
            account_pattern="Expenses:Acme:Supplies",
            period="monthly", amount=0,
            open_accounts=_open_accounts(),
        )


def test_unknown_period_rejects(db):
    service = BudgetService(db)
    with pytest.raises(BudgetValidationError, match="period"):
        service.create(
            label="weird", entity="Acme",
            account_pattern="Expenses:Acme:Supplies",
            period="hourly", amount=100,
            open_accounts=_open_accounts(),
        )


def test_update_and_delete(db):
    service = BudgetService(db)
    b = service.create(
        label="A", entity="Acme",
        account_pattern="Expenses:Acme:Supplies",
        period="monthly", amount=100,
        open_accounts=_open_accounts(),
    )
    updated = service.update(b.id, label="B", amount=200)
    assert updated.label == "B"
    assert float(updated.amount) == 200.0
    assert service.delete(b.id) is True
    assert service.get(b.id) is None
