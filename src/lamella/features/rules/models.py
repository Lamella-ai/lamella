# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class PatternType(str, Enum):
    MERCHANT_EXACT = "merchant_exact"
    MERCHANT_CONTAINS = "merchant_contains"
    AMOUNT_RANGE = "amount_range"
    REGEX = "regex"


PATTERN_TYPES = frozenset(p.value for p in PatternType)


@dataclass(frozen=True)
class RuleRow:
    id: int
    pattern_type: str
    pattern_value: str
    card_account: str | None
    target_account: str
    confidence: float
    hit_count: int
    last_used: datetime | None
    created_by: str


@dataclass(frozen=True)
class TxnFacts:
    """Minimal view of a Beancount transaction for rule evaluation.

    Keeping the engine independent of `beancount.core.data.Transaction` makes
    it trivially unit-testable without constructing full parse trees.
    """

    payee: str | None
    narration: str | None
    amount: Decimal | None
    card_account: str | None

    @property
    def merchant_text(self) -> str:
        return " ".join(filter(None, [self.payee, self.narration])).strip()


@dataclass(frozen=True)
class RuleMatch:
    rule: RuleRow
    tier: int  # lower = higher priority (1..6)

    @property
    def target_account(self) -> str:
        return self.rule.target_account
