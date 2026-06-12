# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import csv
import io
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable, Iterator

from beancount.core.data import Transaction

from lamella.features.reports.line_map import LineMap, LineMapEntry


@dataclass(frozen=True)
class LineTotal:
    line: int | str
    description: str
    amount: Decimal
    txn_count: int


@dataclass(frozen=True)
class DetailRow:
    date: date
    narration: str
    account: str
    amount: Decimal
    line: int | str


@dataclass(frozen=True)
class ReportData:
    entity: str
    year: int
    summary: tuple[LineTotal, ...]
    detail: tuple[DetailRow, ...]


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


def build_report(
    *,
    entity: str,
    year: int,
    entries: Iterable,
    line_map: LineMap,
) -> ReportData:
    """Aggregate USD expense postings for `entity` in `year` against `line_map`.

    Only postings rooted in `Expenses:<entity>:...` are considered — Schedule
    C / F line totals are expense totals. Signs are preserved so refunds
    (negative expense postings) correctly reduce a line's total.
    """
    sums: dict[tuple[int | str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    counts: dict[tuple[int | str, str], int] = defaultdict(int)
    detail: list[DetailRow] = []

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if entry.date.year != year:
            continue
        for posting in entry.postings:
            if not posting.account.startswith("Expenses:"):
                continue
            if _entity_of(posting.account) != entity:
                continue
            units = posting.units
            if units is None or units.number is None:
                continue
            if units.currency and units.currency != "USD":
                continue
            line_entry: LineMapEntry | None = line_map.classify(posting.account)
            if line_entry is None:
                continue
            amount = Decimal(units.number)
            key = (line_entry.line, line_entry.description)
            sums[key] += amount
            counts[key] += 1
            detail.append(
                DetailRow(
                    date=entry.date,
                    narration=entry.narration or "",
                    account=posting.account,
                    amount=amount,
                    line=line_entry.line,
                )
            )

    summary = tuple(
        LineTotal(line=line, description=desc, amount=total, txn_count=counts[(line, desc)])
        for (line, desc), total in sorted(sums.items(), key=lambda kv: _sort_key(kv[0][0]))
        if total != 0
    )
    detail_sorted = tuple(
        sorted(detail, key=lambda d: (_sort_key(d.line), d.date, d.account))
    )
    return ReportData(entity=entity, year=year, summary=summary, detail=detail_sorted)


def _sort_key(line: int | str) -> tuple[int, int | str]:
    # Numeric lines first (ascending), then alpha lines.
    if isinstance(line, int):
        return (0, line)
    try:
        return (0, int(str(line).strip()))
    except ValueError:
        return (1, str(line))


def stream_summary_csv(report: ReportData) -> Iterator[str]:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["line_number", "description", "amount", "txn_count"])
    yield _flush(buffer)
    for row in report.summary:
        writer.writerow([row.line, row.description, _fmt(row.amount), row.txn_count])
        yield _flush(buffer)


def stream_detail_csv(report: ReportData) -> Iterator[str]:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["date", "narration", "account", "amount", "line_number"])
    yield _flush(buffer)
    for row in report.detail:
        writer.writerow(
            [row.date.isoformat(), row.narration, row.account, _fmt(row.amount), row.line]
        )
        yield _flush(buffer)


def _flush(buffer: io.StringIO) -> str:
    value = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    return value


def _fmt(value: Decimal) -> str:
    return f"{Decimal(value):.2f}"


def build_schedule_c(
    *, entity: str, year: int, entries: Iterable, line_map: LineMap
) -> ReportData:
    return build_report(entity=entity, year=year, entries=entries, line_map=line_map)
