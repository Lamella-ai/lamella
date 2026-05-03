# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from lamella.features.budgets.models import Budget, BudgetProgress
from lamella.features.budgets.progress import progress_for_budget
from lamella.features.budgets.service import BudgetService
from lamella.ports.notification import (
    Channel,
    NotificationEvent,
    Priority,
)
from lamella.features.notifications.dispatcher import Dispatcher

log = logging.getLogger(__name__)


# Threshold buckets ordered low → high. We fire at most once per bucket
# per (budget, period_start). The "alert" bucket uses the budget's own
# alert_threshold (typically 0.8); the "over" bucket is fixed at 1.0.
@dataclass(frozen=True)
class _Bucket:
    name: str
    threshold: float  # progress ratio ≥ this triggers the bucket
    priority: Priority


def _buckets_for(budget: Budget) -> list[_Bucket]:
    return [
        _Bucket("alert", float(budget.alert_threshold), Priority.WARN),
        _Bucket("over", 1.0, Priority.URGENT),
    ]


def _crossed(progress: BudgetProgress) -> list[_Bucket]:
    """Return the buckets the progress now meets, in ascending order."""
    out: list[_Bucket] = []
    for bucket in _buckets_for(progress.budget):
        if progress.ratio >= bucket.threshold:
            out.append(bucket)
    return out


def _channels_from_setting(value: str | None) -> set[Channel] | None:
    """Empty/None → use whatever the dispatcher decides. Otherwise, the
    explicit set restricts fan-out per the operator setting."""
    if not value:
        return None
    out: set[Channel] = set()
    for raw in value.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        try:
            out.add(Channel(token))
        except ValueError:
            log.warning("budget alert channel %r is not a known channel", token)
    return out or None


def _build_event(
    *,
    budget: Budget,
    progress: BudgetProgress,
    bucket: _Bucket,
) -> NotificationEvent:
    pct = int(round(progress.ratio * 100))
    body = (
        f"{budget.entity}/{budget.label}: ${progress.spent:.2f} of ${budget.amount:.2f} "
        f"({pct}% — bucket {bucket.name}) for the {budget.period.value} period "
        f"starting {progress.period_start.isoformat()}."
    )
    return NotificationEvent(
        dedup_key=f"budget:{budget.id}:{progress.period_start.isoformat()}:{bucket.name}",
        priority=bucket.priority,
        title=f"Budget {bucket.name}: {budget.label}",
        body=body,
        url="/budgets",
    )


async def evaluate_and_alert(
    *,
    conn: sqlite3.Connection,
    dispatcher: Dispatcher | None,
    entries: Iterable,
    today: date | None = None,
    channels: set[Channel] | None = None,
) -> list[BudgetProgress]:
    """Compute progress for every budget and dispatch one notification per
    newly-crossed threshold bucket. Returns the progress objects so the
    dashboard can reuse them in a single pass."""
    service = BudgetService(conn)
    budgets = service.list()
    materialized = list(entries)
    progresses: list[BudgetProgress] = [
        progress_for_budget(b, materialized, today=today) for b in budgets
    ]

    if dispatcher is None:
        return progresses

    for progress in progresses:
        for bucket in _crossed(progress):
            event = _build_event(
                budget=progress.budget, progress=progress, bucket=bucket,
            )
            # Use channel_hint when an operator restriction is set.
            if channels:
                # Send once per channel; the dispatcher's dedup ensures we
                # don't double-send across channels for the same bucket.
                for ch in channels:
                    pinned = NotificationEvent(
                        dedup_key=event.dedup_key,
                        priority=event.priority,
                        title=event.title,
                        body=event.body,
                        url=event.url,
                        channel_hint=ch,
                    )
                    await dispatcher.send(pinned)
            else:
                await dispatcher.send(event)
    return progresses
