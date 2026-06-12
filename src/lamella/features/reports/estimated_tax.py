# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.features.reports._pdf import render_html, render_pdf


QUARTER_RANGES = {
    1: ((1, 1), (4, 1)),
    2: ((1, 1), (7, 1)),  # YTD through Q2
    3: ((1, 1), (10, 1)),
    4: ((1, 1), (12, 31)),  # full year
}


@dataclass(frozen=True)
class EntityTaxRow:
    entity: str
    income: Decimal
    expenses: Decimal
    net: Decimal
    estimated_tax: Decimal


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


def _ytd_window(year: int, quarter: int) -> tuple[date, date]:
    if quarter not in QUARTER_RANGES:
        raise ValueError(f"quarter must be 1..4; got {quarter}")
    start_t, end_t = QUARTER_RANGES[quarter]
    start = date(year, *start_t)
    if quarter == 4:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, *end_t)
    return start, end


def compute_entity_rows(
    *,
    year: int,
    quarter: int,
    rate: Decimal,
    entries: Iterable,
) -> list[EntityTaxRow]:
    start, end = _ytd_window(year, quarter)
    income: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    expenses: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if not (start <= entry.date < end):
            continue
        for posting in entry.postings:
            units = posting.units
            if units is None or units.number is None:
                continue
            if units.currency and units.currency != "USD":
                continue
            entity = _entity_of(posting.account)
            if not entity:
                continue
            if posting.account.startswith("Income:"):
                income[entity] += -Decimal(units.number)
            elif posting.account.startswith("Expenses:"):
                expenses[entity] += Decimal(units.number)
    entities = sorted(set(income) | set(expenses))
    out: list[EntityTaxRow] = []
    for ent in entities:
        net = income.get(ent, Decimal("0")) - expenses.get(ent, Decimal("0"))
        # Only positive nets owe tax in this simple model.
        owed = (net * rate).quantize(Decimal("0.01")) if net > 0 else Decimal("0")
        out.append(
            EntityTaxRow(
                entity=ent,
                income=income.get(ent, Decimal("0")),
                expenses=expenses.get(ent, Decimal("0")),
                net=net,
                estimated_tax=owed,
            )
        )
    return out


def render_estimated_tax_html(
    *,
    year: int,
    quarter: int,
    rate: Decimal,
    rows: list[EntityTaxRow],
) -> str:
    total = sum((r.estimated_tax for r in rows), Decimal("0"))
    return render_html(
        "estimated_tax.html",
        year=year,
        quarter=quarter,
        rate=rate,
        entity_rows=rows,
        total=total,
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def render_estimated_tax_pdf(
    *,
    year: int,
    quarter: int,
    rate: Decimal,
    entries: Iterable,
) -> bytes:
    rows = compute_entity_rows(year=year, quarter=quarter, rate=rate, entries=entries)
    html = render_estimated_tax_html(year=year, quarter=quarter, rate=rate, rows=rows)
    return render_pdf(html)


def stream_estimated_tax_csv(
    *,
    year: int,
    quarter: int,
    rate: Decimal,
    entries: Iterable,
):
    """CSV export for the worksheet — used by the /reports CSV button."""
    import csv
    import io

    rows = compute_entity_rows(year=year, quarter=quarter, rate=rate, entries=entries)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["entity", "income", "expenses", "net", "estimated_tax_at_%g" % rate])
    yield _flush(buf)
    for row in rows:
        writer.writerow([
            row.entity,
            f"{row.income:.2f}",
            f"{row.expenses:.2f}",
            f"{row.net:.2f}",
            f"{row.estimated_tax:.2f}",
        ])
        yield _flush(buf)


def _flush(buf):
    value = buf.getvalue()
    buf.seek(0)
    buf.truncate(0)
    return value
