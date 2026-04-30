# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Work-backwards draft descriptions.

Given a target (entity slug or account path), gather its ledger
history + ai_decisions corrections, summarize into a proposed
plain-English description, return as a draft the user reviews
and edits.

Core insight: don't ask users to write descriptions on a blank
page. Use what the ledger already says about spending patterns
and let the user correct. If the generated description matches
their mental model, understanding is validated. If it doesn't,
surprises force judgment calls that improve the classifier.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable, Literal

from pydantic import BaseModel, Field

from beancount.core.data import Transaction

from lamella.adapters.openrouter.client import AIError, OpenRouterClient
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader

log = logging.getLogger(__name__)


@dataclass
class AccountRollup:
    account: str
    txn_count: int
    total_amount: Decimal = Decimal("0")


@dataclass
class MerchantRollup:
    merchant: str
    txn_count: int
    total_amount: Decimal = Decimal("0")


@dataclass
class DraftSummaryStats:
    """The numeric backing we hand to the model so it isn't
    inventing percentages from thin air."""
    kind: Literal["entity", "account"]
    target: str
    txn_count: int
    date_range: str
    total_expenses: Decimal
    total_income: Decimal
    top_accounts: list[AccountRollup] = field(default_factory=list)
    top_merchants: list[MerchantRollup] = field(default_factory=list)
    avg_monthly_expense: Decimal | None = None


class DraftDescriptionResponse(BaseModel):
    description: str = Field(min_length=20)
    confidence: float = Field(ge=0.0, le=1.0)
    surprises: list[str] = Field(default_factory=list)
    # Short flags the user should sanity-check — e.g.,
    # "47 charges at Fabric Warehouse under Acme — expected?"
    reasoning: str = ""


SYSTEM = (
    "You are a meticulous bookkeeper's analyst. Given a rollup of "
    "a user's ledger history for a specific entity or account, "
    "write a concise plain-English paragraph describing what the "
    "entity does or what the account is used for. Base every "
    "claim on the numbers you were given — never invent merchants "
    "or categories that aren't in the rollup. Then list 0–5 "
    "SURPRISES: specific patterns that look unusual enough the "
    "user should confirm (e.g., 'large spend at merchant X under "
    "entity Y — expected?', 'account used for both rent AND "
    "equipment — should these be split?'). Keep the description "
    "under 1000 characters; keep surprises short."
)


def build_entity_stats(
    entries: Iterable, entity_slug: str, *, lookback_days: int = 730,
) -> DraftSummaryStats:
    """Roll up an entity's ledger history: expense/income totals,
    top accounts, top merchants."""
    cutoff = date.today() - timedelta(days=lookback_days)
    prefix = f":{entity_slug}:"
    expense_accounts: Counter = Counter()
    expense_amounts: dict[str, Decimal] = {}
    income_accounts: Counter = Counter()
    income_amounts: dict[str, Decimal] = {}
    merchant_counts: Counter = Counter()
    merchant_amounts: dict[str, Decimal] = {}
    txn_ids: set[str] = set()
    earliest: date | None = None
    latest: date | None = None
    total_expense = Decimal(0)
    total_income = Decimal(0)

    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if e.date < cutoff:
            continue
        touched = False
        for p in e.postings or []:
            acct = p.account or ""
            if prefix not in f":{acct}:":
                continue
            touched = True
            if p.units is None or p.units.number is None:
                continue
            amt = Decimal(p.units.number)
            if acct.startswith("Expenses:"):
                expense_accounts[acct] += 1
                expense_amounts[acct] = expense_amounts.get(acct, Decimal(0)) + abs(amt)
                total_expense += abs(amt)
            elif acct.startswith("Income:"):
                income_accounts[acct] += 1
                income_amounts[acct] = income_amounts.get(acct, Decimal(0)) + abs(amt)
                total_income += abs(amt)
        if touched:
            merchant = (e.payee or e.narration or "").strip()[:80]
            if merchant:
                merchant_counts[merchant] += 1
                # Pick the largest Expenses leg for this txn to
                # attribute merchant spend.
                amt = max(
                    (abs(Decimal(p.units.number))
                     for p in e.postings or []
                     if p.units and p.units.number is not None
                     and (p.account or "").startswith("Expenses:")),
                    default=Decimal(0),
                )
                merchant_amounts[merchant] = merchant_amounts.get(merchant, Decimal(0)) + amt
            txn_ids.add(id(e))
            earliest = e.date if earliest is None or e.date < earliest else earliest
            latest = e.date if latest is None or e.date > latest else latest

    top_accounts = [
        AccountRollup(
            account=a,
            txn_count=int(expense_accounts[a]),
            total_amount=expense_amounts[a],
        )
        for a, _ in expense_accounts.most_common(12)
    ]
    top_merchants = [
        MerchantRollup(
            merchant=m,
            txn_count=int(merchant_counts[m]),
            total_amount=merchant_amounts.get(m, Decimal(0)),
        )
        for m, _ in merchant_counts.most_common(12)
    ]
    date_range = (
        f"{earliest} → {latest}" if earliest and latest else "no dated txns"
    )
    months = 1
    if earliest and latest:
        delta_days = max(1, (latest - earliest).days)
        months = max(1, delta_days // 30)
    avg = (total_expense / months) if total_expense and months else None

    return DraftSummaryStats(
        kind="entity",
        target=entity_slug,
        txn_count=len(txn_ids),
        date_range=date_range,
        total_expenses=total_expense,
        total_income=total_income,
        top_accounts=top_accounts,
        top_merchants=top_merchants,
        avg_monthly_expense=avg,
    )


def build_account_stats(
    entries: Iterable, account_path: str, *, lookback_days: int = 730,
) -> DraftSummaryStats:
    """Roll up a specific account's ledger history."""
    cutoff = date.today() - timedelta(days=lookback_days)
    merchant_counts: Counter = Counter()
    merchant_amounts: dict[str, Decimal] = {}
    txn_count = 0
    total = Decimal(0)
    earliest: date | None = None
    latest: date | None = None

    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if e.date < cutoff:
            continue
        touched = False
        for p in e.postings or []:
            if p.account != account_path:
                continue
            if p.units is None or p.units.number is None:
                continue
            touched = True
            amt = abs(Decimal(p.units.number))
            total += amt
            merchant = (e.payee or e.narration or "").strip()[:80]
            if merchant:
                merchant_counts[merchant] += 1
                merchant_amounts[merchant] = merchant_amounts.get(merchant, Decimal(0)) + amt
        if touched:
            txn_count += 1
            earliest = e.date if earliest is None or e.date < earliest else earliest
            latest = e.date if latest is None or e.date > latest else latest

    top_merchants = [
        MerchantRollup(
            merchant=m,
            txn_count=int(merchant_counts[m]),
            total_amount=merchant_amounts.get(m, Decimal(0)),
        )
        for m, _ in merchant_counts.most_common(15)
    ]
    date_range = (
        f"{earliest} → {latest}" if earliest and latest else "no dated txns"
    )
    months = 1
    if earliest and latest:
        months = max(1, (latest - earliest).days // 30)
    avg = (total / months) if total and months else None

    return DraftSummaryStats(
        kind="account",
        target=account_path,
        txn_count=txn_count,
        date_range=date_range,
        total_expenses=total,
        total_income=Decimal(0),
        top_accounts=[],
        top_merchants=top_merchants,
        avg_monthly_expense=avg,
    )


def _stats_prompt(stats: DraftSummaryStats) -> str:
    lines: list[str] = []
    lines.append(f"Target: {stats.kind.upper()} {stats.target!r}")
    lines.append(f"Date range: {stats.date_range}")
    lines.append(f"Transactions: {stats.txn_count}")
    lines.append(f"Total expenses: ${stats.total_expenses:.2f}")
    if stats.total_income:
        lines.append(f"Total income: ${stats.total_income:.2f}")
    if stats.avg_monthly_expense:
        lines.append(
            f"Avg monthly expense: ${stats.avg_monthly_expense:.2f}"
        )
    if stats.top_accounts:
        lines.append("")
        lines.append("Top expense accounts (account · count · total):")
        for a in stats.top_accounts:
            lines.append(f"  {a.account} · {a.txn_count} · ${a.total_amount:.2f}")
    if stats.top_merchants:
        lines.append("")
        lines.append("Top merchants (merchant · count · total):")
        for m in stats.top_merchants:
            lines.append(f"  {m.merchant} · {m.txn_count} · ${m.total_amount:.2f}")
    return "\n".join(lines)


async def generate_entity_description(
    *,
    ai: AIService,
    entries: Iterable,
    entity_slug: str,
) -> DraftDescriptionResponse | None:
    """Produce a proposed description paragraph for an entity."""
    stats = build_entity_stats(entries, entity_slug)
    if stats.txn_count == 0:
        return None
    return await _call_draft_model(ai=ai, stats=stats)


async def generate_account_description(
    *,
    ai: AIService,
    entries: Iterable,
    account_path: str,
) -> DraftDescriptionResponse | None:
    stats = build_account_stats(entries, account_path)
    if stats.txn_count == 0:
        return None
    return await _call_draft_model(ai=ai, stats=stats)


async def _call_draft_model(
    *, ai: AIService, stats: DraftSummaryStats,
) -> DraftDescriptionResponse | None:
    if not ai.enabled or ai.spend_cap_reached():
        return None
    client = ai.new_client()
    if client is None:
        return None
    prompt = _stats_prompt(stats)
    # Prefer the fallback model (Opus by default) — draft
    # descriptions are multi-txn summary calls where quality
    # matters more than cost. Callers can override by adjusting
    # ai.settings_store.
    model = ai.fallback_model_for("classify_txn") or ai.model_for("classify_txn")
    try:
        result = await client.chat(
            decision_type="draft_description",
            input_ref=f"{stats.kind}:{stats.target}",
            system=SYSTEM,
            user=prompt,
            schema=DraftDescriptionResponse,
            model=model,
        )
    except AIError as exc:
        log.warning(
            "draft description failed for %s %s: %s",
            stats.kind, stats.target, exc,
        )
        return None
    finally:
        await client.aclose()
    return result.data
