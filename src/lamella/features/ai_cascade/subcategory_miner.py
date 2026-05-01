# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Emergent sub-categorization miner.

Given a catchall account (Expenses:Personal:Food, 800 txns
spanning groceries + restaurants + mcdonalds), cluster its
merchants and propose a sub-account hierarchy that carves the
sprawl into cleaner categories. User reviews proposals, accepts
individual clusters, and the existing add-subcategory + audit-
by-account flow handles account creation + retrospective
reclassification.

The miner is proposal-only. It never creates accounts or moves
txns by itself — that path goes through user approval via the
normal account-settings + audit workflows.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable

from pydantic import BaseModel, Field

from beancount.core.data import Transaction

from lamella.adapters.openrouter.client import AIError, OpenRouterClient
from lamella.features.ai_cascade.decisions import DecisionType as _DT  # noqa: F401
from lamella.features.ai_cascade.service import AIService

log = logging.getLogger(__name__)


@dataclass
class MerchantSample:
    merchant: str
    txn_count: int
    total_amount: Decimal = Decimal("0")


@dataclass
class MinerInput:
    account_path: str
    entity_slug: str | None
    total_txns: int
    merchants: list[MerchantSample] = field(default_factory=list)


class ProposedCluster(BaseModel):
    """One proposed sub-account. Name is a proposed LEAF
    (appended to the parent path); rationale is a one-liner the
    user can sanity-check. Merchant list is which merchants from
    the input rolled up into this cluster."""
    proposed_leaf: str = Field(min_length=1, max_length=60)
    rationale: str = ""
    example_merchants: list[str] = Field(default_factory=list)
    estimated_txn_count: int = 0


class MinerProposal(BaseModel):
    clusters: list[ProposedCluster] = Field(default_factory=list)
    unclassifiable: list[str] = Field(default_factory=list)
    reasoning: str = ""


SYSTEM = (
    "You analyze a single expense account's merchant patterns and "
    "propose a small number (2-6) of sub-categories that would "
    "cleanly split the account. Every cluster name is a single "
    "CapitalCase word or short phrase suitable as a Beancount "
    "leaf (Groceries, Restaurants, FastFood, Utilities). Every "
    "merchant you cluster MUST appear in the input list — never "
    "invent merchants. A cluster should have at least 3 distinct "
    "merchants OR account for >10% of the total txns; otherwise "
    "leave those merchants in `unclassifiable` so the user doesn't "
    "fragment the account into too-narrow buckets. If the account "
    "already looks well-split (no clear clusters), return an "
    "empty `clusters` list and explain in `reasoning`."
)


def build_miner_input(
    entries: Iterable,
    *,
    account_path: str,
    entity_slug: str | None,
) -> MinerInput:
    """Roll up an account's txns into a merchant histogram."""
    merchant_counts: Counter = Counter()
    merchant_amounts: dict[str, Decimal] = {}
    txn_count = 0
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        touched = False
        largest_amt = Decimal(0)
        for p in e.postings or []:
            if p.account != account_path:
                continue
            if p.units is None or p.units.number is None:
                continue
            touched = True
            amt = abs(Decimal(p.units.number))
            if amt > largest_amt:
                largest_amt = amt
        if not touched:
            continue
        txn_count += 1
        merchant = (e.payee or e.narration or "").strip()[:80]
        if merchant:
            merchant_counts[merchant] += 1
            merchant_amounts[merchant] = (
                merchant_amounts.get(merchant, Decimal(0)) + largest_amt
            )
    return MinerInput(
        account_path=account_path,
        entity_slug=entity_slug,
        total_txns=txn_count,
        merchants=[
            MerchantSample(
                merchant=m,
                txn_count=int(merchant_counts[m]),
                total_amount=merchant_amounts.get(m, Decimal(0)),
            )
            for m, _ in merchant_counts.most_common(60)
        ],
    )


def _prompt(inp: MinerInput) -> str:
    lines: list[str] = []
    lines.append(f"Account: {inp.account_path}")
    if inp.entity_slug:
        lines.append(f"Entity: {inp.entity_slug}")
    lines.append(f"Total transactions: {inp.total_txns}")
    lines.append("")
    lines.append("Top merchants (merchant · count · total):")
    for m in inp.merchants:
        lines.append(f"  {m.merchant} · {m.txn_count} · ${m.total_amount:.2f}")
    lines.append("")
    lines.append(
        "Propose sub-categories that would meaningfully split this "
        "account. Return JSON with fields: clusters "
        "([{proposed_leaf, rationale, example_merchants[], "
        "estimated_txn_count}]), unclassifiable (list of merchants "
        "not in any cluster), reasoning."
    )
    return "\n".join(lines)


async def propose_subcategories(
    *, ai: AIService, miner_input: MinerInput,
) -> MinerProposal | None:
    """Call the AI to generate a cluster proposal. Returns None
    when AI is disabled / over cap / errored."""
    if not ai.enabled or ai.spend_cap_reached():
        return None
    if miner_input.total_txns < 10:
        # Tiny accounts don't produce useful clusters.
        return MinerProposal(
            clusters=[], unclassifiable=[],
            reasoning="Account has too few transactions to mine "
                     "meaningfully — skipped.",
        )
    client = ai.new_client()
    if client is None:
        return None
    # Use the fallback model (Opus by default) — clustering is a
    # high-context reasoning task where quality matters.
    model = ai.fallback_model_for("classify_txn") or ai.model_for("classify_txn")
    try:
        result = await client.chat(
            decision_type="draft_description",  # reusing the draft slot
            input_ref=f"miner:{miner_input.account_path}",
            system=SYSTEM,
            user=_prompt(miner_input),
            schema=MinerProposal,
            model=model,
        )
    except AIError as exc:
        log.warning(
            "subcategory miner failed for %s: %s",
            miner_input.account_path, exc,
        )
        return None
    finally:
        await client.aclose()
    return result.data
