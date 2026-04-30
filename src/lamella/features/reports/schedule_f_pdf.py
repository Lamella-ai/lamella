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

from beancount.core.data import Transaction

from lamella.features.reports._pdf import render_html, render_pdf
from lamella.features.reports.line_map import LineMap
from lamella.features.reports.schedule_f import build_schedule_f


@dataclass(frozen=True)
class IncomeRow:
    account: str
    amount: Decimal


@dataclass(frozen=True)
class ScheduleFContext:
    entity: str
    year: int
    summary: tuple
    income_rows: list
    gross_receipts: Decimal
    total_expenses: Decimal
    net: Decimal


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


def _income_rows(entity: str, year: int, entries: Iterable) -> list[IncomeRow]:
    sums: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date.year != year:
            continue
        for posting in entry.postings:
            if not posting.account.startswith("Income:"):
                continue
            if _entity_of(posting.account) != entity:
                continue
            units = posting.units
            if units is None or units.number is None:
                continue
            if units.currency and units.currency != "USD":
                continue
            sums[posting.account] += -Decimal(units.number)
    return [
        IncomeRow(account=a, amount=v) for a, v in sorted(sums.items()) if v != 0
    ]


def build_context(
    *,
    entity: str,
    year: int,
    entries: Iterable,
    line_map: LineMap,
) -> ScheduleFContext:
    materialized = list(entries)
    report = build_schedule_f(
        entity=entity, year=year, entries=materialized, line_map=line_map,
    )
    income_rows = _income_rows(entity, year, materialized)
    gross = sum((r.amount for r in income_rows), Decimal("0"))
    total_expenses = sum((row.amount for row in report.summary), Decimal("0"))
    net = gross - total_expenses
    return ScheduleFContext(
        entity=entity,
        year=year,
        summary=report.summary,
        income_rows=income_rows,
        gross_receipts=gross,
        total_expenses=total_expenses,
        net=net,
    )


def render_schedule_f_html(ctx: ScheduleFContext) -> str:
    return render_html(
        "schedule_f.html",
        entity=ctx.entity,
        year=ctx.year,
        summary=ctx.summary,
        income_rows=ctx.income_rows,
        gross_receipts=ctx.gross_receipts,
        total_expenses=ctx.total_expenses,
        net=ctx.net,
    )


def render_schedule_f_pdf(ctx: ScheduleFContext) -> bytes:
    return render_pdf(render_schedule_f_html(ctx))
