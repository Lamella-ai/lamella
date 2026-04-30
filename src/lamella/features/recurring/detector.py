# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import re
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.features.recurring.service import RecurringService

log = logging.getLogger(__name__)


_WORD_RE = re.compile(r"[A-Za-z0-9]+")


# Cadence windows (median inter-arrival days, stddev cap).
MONTHLY = (27, 33, 5)
QUARTERLY = (85, 95, 8)
ANNUAL = (355, 375, 15)


def _canonical_merchant(payee: str | None, narration: str | None) -> str:
    src = (payee or "").strip()
    if not src:
        src = (narration or "")[:40]
    tokens = _WORD_RE.findall(src.lower())
    while tokens and tokens[-1].isdigit():
        tokens.pop()
    return " ".join(tokens).strip()


def _entity_of(account: str) -> str | None:
    parts = account.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in {"Assets", "Liabilities", "Income", "Expenses", "Equity"}:
        return None
    return parts[1]


@dataclass
class _Group:
    label: str
    source_account: str
    entity: str
    dates: list[date]
    amounts: list[Decimal]


def _classify_cadence(median: float, stddev: float) -> str | None:
    for low, high, max_std in (MONTHLY, QUARTERLY, ANNUAL):
        if low <= median <= high and stddev <= max_std:
            if (low, high, max_std) == MONTHLY:
                return "monthly"
            if (low, high, max_std) == QUARTERLY:
                return "quarterly"
            return "annual"
    return None


def _intervals(dates: list[date]) -> list[int]:
    sorted_d = sorted(dates)
    return [(sorted_d[i + 1] - sorted_d[i]).days for i in range(len(sorted_d) - 1)]


@dataclass(frozen=True)
class DetectionResult:
    candidates_found: int
    new_proposals: int
    updates: int
    skipped: int


class RecurringDetector:
    """Group ledger transactions by canonical merchant + source account,
    classify the inter-arrival cadence, and upsert into recurring_expenses.
    Replaces Phase 5's heuristic in-memory predictor."""

    def __init__(
        self,
        *,
        scan_window_days: int = 540,
        min_occurrences: int = 3,
    ):
        self.scan_window_days = scan_window_days
        self.min_occurrences = min_occurrences

    def candidates(
        self,
        entries: Iterable,
        *,
        today: date,
    ) -> list[tuple[_Group, str, Decimal, int]]:
        """Returns (group, cadence, expected_amount, expected_day) tuples
        for each detected recurring candidate."""
        cutoff = today - timedelta(days=self.scan_window_days)
        groups: dict[tuple[str, str], _Group] = {}
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            if entry.date < cutoff:
                continue
            label = _canonical_merchant(entry.payee, entry.narration)
            if not label:
                continue
            for posting in entry.postings:
                units = posting.units
                if units is None or units.number is None:
                    continue
                if units.currency and units.currency != "USD":
                    continue
                root = posting.account.split(":", 1)[0]
                if root not in {"Assets", "Liabilities"}:
                    continue
                key = (label, posting.account)
                group = groups.get(key)
                if group is None:
                    group = _Group(
                        label=label,
                        source_account=posting.account,
                        entity=_entity_of(posting.account) or "",
                        dates=[],
                        amounts=[],
                    )
                    groups[key] = group
                group.dates.append(entry.date)
                group.amounts.append(Decimal(units.number))

        out: list[tuple[_Group, str, Decimal, int]] = []
        for group in groups.values():
            if len(group.dates) < self.min_occurrences:
                continue
            ivs = _intervals(group.dates)
            if not ivs:
                continue
            try:
                median = float(statistics.median(ivs))
                stdev = float(statistics.pstdev(ivs)) if len(ivs) > 1 else 0.0
            except statistics.StatisticsError:
                continue
            cadence = _classify_cadence(median, stdev)
            if cadence is None:
                continue
            amount = abs(_median_amount(group.amounts))
            day = int(statistics.median([d.day for d in group.dates]))
            out.append((group, cadence, amount, day))
        return out

    def run(
        self,
        *,
        conn: sqlite3.Connection,
        entries: Iterable,
        today: date | None = None,
    ) -> DetectionResult:
        today = today or date.today()
        service = RecurringService(conn)
        candidates = self.candidates(entries, today=today)
        new_proposals = 0
        updates = 0
        skipped = 0
        materialized_today = today
        for group, cadence, amount, expected_day in candidates:
            last_seen = max(group.dates)
            interval = _expected_interval_days(cadence)
            next_expected = last_seen + timedelta(days=interval)
            merchant_pattern = _pattern_from_label(group.label)
            _row, action = service.upsert(
                label=group.label.title() or group.label,
                entity=group.entity,
                expected_amount=amount,
                expected_day=expected_day,
                source_account=group.source_account,
                merchant_pattern=merchant_pattern,
                cadence=cadence,
                last_seen=last_seen,
                next_expected=next_expected,
            )
            if action == "inserted":
                new_proposals += 1
            elif action == "updated":
                updates += 1
            else:
                skipped += 1
        return DetectionResult(
            candidates_found=len(candidates),
            new_proposals=new_proposals,
            updates=updates,
            skipped=skipped,
        )


def _median_amount(amounts: list[Decimal]) -> Decimal:
    if not amounts:
        return Decimal("0")
    med = statistics.median(sorted(amounts))
    return Decimal(str(med)).quantize(Decimal("0.01"))


def _expected_interval_days(cadence: str) -> int:
    if cadence == "monthly":
        return 30
    if cadence == "quarterly":
        return 91
    return 365


def _pattern_from_label(label: str) -> str:
    """Build a stable, simple regex anchored at word boundaries from the
    canonical label. Non-word chars are escaped; whitespace becomes a
    flexible match. We use this to rematch incoming SimpleFIN txns."""
    tokens = label.split()
    if not tokens:
        return ""
    escaped = [re.escape(t) for t in tokens]
    return r"(?i)\b" + r"\W+".join(escaped) + r"\b"


def run_detection(
    *,
    conn: sqlite3.Connection,
    entries: Iterable,
    scan_window_days: int = 540,
    min_occurrences: int = 3,
    today: date | None = None,
) -> DetectionResult:
    """Convenience function — used by the scheduler job. Records the
    detection-cycle row in recurring_detections."""
    detector = RecurringDetector(
        scan_window_days=scan_window_days,
        min_occurrences=min_occurrences,
    )
    cur = conn.execute(
        "INSERT INTO recurring_detections (scan_window_days) VALUES (?)",
        (scan_window_days,),
    )
    detection_id = int(cur.lastrowid)
    try:
        result = detector.run(conn=conn, entries=entries, today=today)
    except Exception as exc:  # noqa: BLE001
        conn.execute(
            "UPDATE recurring_detections SET error = ? WHERE id = ?",
            (f"{type(exc).__name__}: {exc}", detection_id),
        )
        raise
    conn.execute(
        """
        UPDATE recurring_detections
           SET candidates_found = ?, new_proposals = ?, updates = ?
         WHERE id = ?
        """,
        (result.candidates_found, result.new_proposals, result.updates, detection_id),
    )
    return result
