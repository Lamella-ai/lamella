# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Intercompany settlement report — NEXTGEN.md Phase G6.

Pure read-side: scans the current ledger for
``Assets:<Entity>:DueFrom:<OtherEntity>`` and
``Liabilities:<Entity>:DueTo:<OtherEntity>`` balances and
aggregates them into an "as-of-today, who owes whom" view.

Produces one record per *entity pair* with the net outstanding
balance and the list of contributing transactions. When a
corresponding settlement transaction exists (clearing both the
DueFrom and DueTo accounts), that pair shows a net zero and
surfaces under "Settled." Everything else is "Outstanding."

Discipline:
* Reads, never writes. Recording an actual settlement payment
  is a separate writer call (Phase G6 UI action) that posts
  a clearing transaction and the real money movement.
* Entity names come directly from the account paths — no
  accounts_meta lookup needed; the account convention IS the
  entity binding.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

log = logging.getLogger(__name__)

__all__ = [
    "EntityPairBalance",
    "IntercompanyReport",
    "build_intercompany_report",
]


_DUE_FROM = re.compile(r"^Assets:([A-Za-z0-9_\-]+):DueFrom:([A-Za-z0-9_\-]+)$")
_DUE_TO = re.compile(r"^Liabilities:([A-Za-z0-9_\-]+):DueTo:([A-Za-z0-9_\-]+)$")


@dataclass(frozen=True)
class PairTxnRef:
    """One historical transaction touching this pair of accounts."""
    txn_date: date
    narration: str | None
    amount: Decimal          # abs value — absolute magnitude
    is_settlement: bool      # True when the txn is a clearing entry


@dataclass
class EntityPairBalance:
    """Aggregated balance between two entities.

    ``paying_entity`` held the DueFrom account (is owed the balance).
    ``owing_entity`` held the DueTo account (owes the balance).
    A positive ``outstanding`` means ``owing_entity`` still owes
    ``paying_entity`` that amount.
    """
    paying_entity: str
    owing_entity: str
    due_from_balance: Decimal = Decimal("0")
    due_to_balance: Decimal = Decimal("0")
    transactions: list[PairTxnRef] = field(default_factory=list)

    @property
    def outstanding(self) -> Decimal:
        """Canonical "still-owed" number. DueFrom balance is the
        receivable on the paying entity's side; settlements reduce
        it. We take that as truth; DueTo should match in absolute
        value but we surface both so discrepancies are visible."""
        return self.due_from_balance

    @property
    def is_settled(self) -> bool:
        return self.outstanding.copy_abs() < Decimal("0.01")


@dataclass
class IntercompanyReport:
    """Top-level report output."""
    as_of: date
    balances: list[EntityPairBalance] = field(default_factory=list)

    @property
    def outstanding_pairs(self) -> list[EntityPairBalance]:
        return [p for p in self.balances if not p.is_settled]

    @property
    def settled_pairs(self) -> list[EntityPairBalance]:
        return [p for p in self.balances if p.is_settled]


def build_intercompany_report(
    entries: Iterable,
    *,
    as_of: date | None = None,
) -> IntercompanyReport:
    """Walk the ledger and build the intercompany settlement view.

    For each transaction, scan its postings for DueFrom/DueTo
    accounts matching the §6.5 convention. Aggregate running
    balances per (paying, owing) pair. Settlement transactions
    show up as negative postings on DueFrom + positive on DueTo
    — the same algorithm handles them.
    """
    cutoff = as_of or date.today()
    pairs: dict[tuple[str, str], EntityPairBalance] = {}

    def _pair(paying: str, owing: str) -> EntityPairBalance:
        key = (paying, owing)
        if key not in pairs:
            pairs[key] = EntityPairBalance(
                paying_entity=paying, owing_entity=owing,
            )
        return pairs[key]

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date > cutoff:
            continue
        tags = set(getattr(entry, "tags", frozenset()) or ())
        is_settlement = "lamella-intercompany-settlement" in tags
        for posting in entry.postings or []:
            account = posting.account or ""
            if not posting.units or posting.units.number is None:
                continue
            amount = Decimal(posting.units.number)
            m_from = _DUE_FROM.match(account)
            m_to = _DUE_TO.match(account)
            if m_from:
                paying, owing = m_from.group(1), m_from.group(2)
                p = _pair(paying, owing)
                p.due_from_balance += amount
                p.transactions.append(
                    PairTxnRef(
                        txn_date=entry.date,
                        narration=entry.narration,
                        amount=amount.copy_abs(),
                        is_settlement=is_settlement,
                    )
                )
            elif m_to:
                # DueTo accounts are on the owing side; the first
                # regex group is the owing entity.
                owing, paying = m_to.group(1), m_to.group(2)
                p = _pair(paying, owing)
                p.due_to_balance += amount

    # Sort for stable output: largest outstanding first, then by
    # entity-pair name.
    balances = sorted(
        pairs.values(),
        key=lambda b: (-b.outstanding.copy_abs(), b.paying_entity, b.owing_entity),
    )
    return IntercompanyReport(as_of=cutoff, balances=balances)
