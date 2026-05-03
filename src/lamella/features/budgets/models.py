# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Literal


class BudgetPeriod(str, Enum):
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class BudgetValidationError(ValueError):
    """Raised for user-input problems (bad regex, no matching open account,
    nonpositive amount). Routes surface these inline without 500ing."""


@dataclass(frozen=True)
class Budget:
    id: int
    label: str
    entity: str
    account_pattern: str
    period: BudgetPeriod
    amount: Decimal
    alert_threshold: float
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class BudgetProgress:
    budget: Budget
    period_start: date
    period_end: date  # exclusive
    spent: Decimal
    ratio: float  # spent / amount, can exceed 1.0

    @property
    def remaining(self) -> Decimal:
        return self.budget.amount - self.spent

    def band(self) -> Literal["green", "yellow", "red"]:
        """Red once we're at or over 100%. Yellow once we're at/above the
        alert threshold. Otherwise green. The dashboard progress bar uses
        this; the threshold-crossing alerter uses a different signal (the
        before/after ratios across a write)."""
        if self.ratio >= 1.0:
            return "red"
        if self.ratio >= self.budget.alert_threshold:
            return "yellow"
        return "green"
