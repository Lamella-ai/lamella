# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Open, Transaction


@dataclass(frozen=True)
class EntityBalance:
    entity: str
    assets: Decimal
    liabilities: Decimal
    equity: Decimal
    income: Decimal
    expenses: Decimal

    @property
    def net_worth(self) -> Decimal:
        return self.assets - self.liabilities

    @property
    def net_income(self) -> Decimal:
        return self.income + self.expenses  # income amounts are stored negative


def _entity_of(account: str) -> str | None:
    # "Expenses:Acme:Supplies" -> "Acme". "Assets:Cash" (no entity) -> None.
    # System slugs (OpeningBalances, Clearing, Retained, etc.) are excluded
    # so they never surface as "entities" on the dashboard balance cards.
    from lamella.core.registry.discovery import EXCLUDED_ENTITY_SEGMENTS
    parts = account.split(":")
    if len(parts) < 2:
        return None
    head = parts[0]
    if head not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    slug = parts[1]
    if slug in EXCLUDED_ENTITY_SEGMENTS:
        return None
    return slug


def _root(account: str) -> str:
    return account.split(":", 1)[0]


def entity_balances(entries: Iterable) -> list[EntityBalance]:
    """Sum USD-denominated postings per entity across the known root types.
    Non-USD and positions with no number are ignored for Phase 1."""
    sums: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {"Assets": Decimal("0"), "Liabilities": Decimal("0"),
                 "Equity": Decimal("0"), "Income": Decimal("0"), "Expenses": Decimal("0")}
    )
    opened: set[str] = set()
    for entry in entries:
        if isinstance(entry, Open):
            opened.add(entry.account)
        elif isinstance(entry, Transaction):
            for posting in entry.postings:
                units = posting.units
                if units is None or units.number is None:
                    continue
                if units.currency and units.currency != "USD":
                    continue
                entity = _entity_of(posting.account)
                if not entity:
                    continue
                root = _root(posting.account)
                if root not in sums[entity]:
                    continue
                sums[entity][root] += Decimal(units.number)

    # Surface entities that only appear via Open directives too.
    for account in opened:
        entity = _entity_of(account)
        if entity:
            _ = sums[entity]

    result = []
    for entity, buckets in sorted(sums.items()):
        result.append(
            EntityBalance(
                entity=entity,
                assets=buckets["Assets"],
                liabilities=buckets["Liabilities"],
                equity=buckets["Equity"],
                income=buckets["Income"],
                expenses=buckets["Expenses"],
            )
        )
    return result
