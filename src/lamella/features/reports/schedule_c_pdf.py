# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.features.mileage.service import MileageService
from lamella.features.reports._pdf import render_html, render_pdf
from lamella.features.reports.line_map import LineMap
from lamella.features.reports.schedule_c import ReportData, build_schedule_c


@dataclass(frozen=True)
class ScheduleCContext:
    entity: str
    year: int
    summary: tuple
    gross_receipts: Decimal
    cogs: Decimal
    gross_income: Decimal
    total_expenses: Decimal
    net: Decimal
    mileage_rows: list


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


def _gross_receipts(entity: str, year: int, entries: Iterable) -> Decimal:
    """Sum Income:<entity>:* postings for the year (income amounts are
    negative on the credit side; the sign-flip yields gross receipts)."""
    total = Decimal("0")
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
            total += -Decimal(units.number)
    return total


def _cogs(entity: str, year: int, entries: Iterable) -> Decimal:
    """Sum Expenses:<entity>:COGS:* postings. Returns 0 if the entity has
    no COGS subaccounts opened."""
    total = Decimal("0")
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date.year != year:
            continue
        for posting in entry.postings:
            if not posting.account.startswith(f"Expenses:{entity}:COGS"):
                continue
            units = posting.units
            if units is None or units.number is None:
                continue
            if units.currency and units.currency != "USD":
                continue
            total += Decimal(units.number)
    return total


def build_context(
    *,
    entity: str,
    year: int,
    entries: Iterable,
    line_map: LineMap,
    conn: sqlite3.Connection | None = None,
    mileage_csv_path=None,
    mileage_rate: float = 0.67,
) -> ScheduleCContext:
    materialized = list(entries)
    report = build_schedule_c(
        entity=entity, year=year, entries=materialized, line_map=line_map,
    )
    gross = _gross_receipts(entity, year, materialized)
    cogs = _cogs(entity, year, materialized)
    gross_income = gross - cogs
    total_expenses = sum((row.amount for row in report.summary), Decimal("0"))
    net = gross_income - total_expenses

    mileage_rows: list = []
    if conn is not None and mileage_csv_path is not None:
        try:
            mileage = MileageService(conn=conn, csv_path=mileage_csv_path)
            mileage_rows = [
                m for m in mileage.yearly_summary(year, rate_per_mile=mileage_rate)
                if m.entity == entity
            ]
        except Exception:  # noqa: BLE001
            mileage_rows = []

    return ScheduleCContext(
        entity=entity,
        year=year,
        summary=report.summary,
        gross_receipts=gross,
        cogs=cogs,
        gross_income=gross_income,
        total_expenses=total_expenses,
        net=net,
        mileage_rows=mileage_rows,
    )


def render_schedule_c_html(ctx: ScheduleCContext) -> str:
    return render_html(
        "schedule_c.html",
        entity=ctx.entity,
        year=ctx.year,
        summary=ctx.summary,
        gross_receipts=ctx.gross_receipts,
        cogs=ctx.cogs,
        gross_income=ctx.gross_income,
        total_expenses=ctx.total_expenses,
        net=ctx.net,
        mileage_rows=ctx.mileage_rows,
    )


def render_schedule_c_pdf(ctx: ScheduleCContext) -> bytes:
    return render_pdf(render_schedule_c_html(ctx))
