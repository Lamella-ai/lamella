# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.features.budgets.models import Budget, BudgetPeriod, BudgetProgress


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


def period_window(period: BudgetPeriod, *, today: date) -> tuple[date, date]:
    """Return ``(start_inclusive, end_exclusive)`` for the calendar period
    containing ``today``."""
    if period == BudgetPeriod.ANNUAL:
        return date(today.year, 1, 1), date(today.year + 1, 1, 1)
    if period == BudgetPeriod.QUARTERLY:
        q_index = (today.month - 1) // 3  # 0..3
        start_month = q_index * 3 + 1
        start = date(today.year, start_month, 1)
        end_month = start_month + 3
        end_year = today.year
        if end_month > 12:
            end_month -= 12
            end_year += 1
        end = date(end_year, end_month, 1)
        return start, end
    # monthly
    start = date(today.year, today.month, 1)
    if today.month == 12:
        end = date(today.year + 1, 1, 1)
    else:
        end = date(today.year, today.month + 1, 1)
    return start, end


def progress_for_budget(
    budget: Budget,
    entries: Iterable,
    *,
    today: date | None = None,
) -> BudgetProgress:
    today = today or date.today()
    start, end = period_window(budget.period, today=today)
    pattern = re.compile(budget.account_pattern)
    spent = Decimal("0")
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date < start or entry.date >= end:
            continue
        for posting in entry.postings:
            if not posting.account.startswith("Expenses:"):
                continue
            if _entity_of(posting.account) != budget.entity:
                continue
            if not pattern.search(posting.account):
                continue
            units = posting.units
            if units is None or units.number is None:
                continue
            if units.currency and units.currency != "USD":
                continue
            spent += Decimal(units.number)
    ratio = float(spent / budget.amount) if budget.amount > 0 else 0.0
    return BudgetProgress(
        budget=budget,
        period_start=start,
        period_end=end,
        spent=spent,
        ratio=ratio,
    )
