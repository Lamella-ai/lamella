# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from beancount.core.data import Transaction

from lamella.ports.notification import NotificationEvent, Priority
from lamella.features.notifications.dispatcher import Dispatcher
from lamella.features.recurring.detector import _expected_interval_days
from lamella.features.recurring.service import (
    RecurringExpense,
    RecurringService,
    RecurringStatus,
)

log = logging.getLogger(__name__)


WINDOW_DAYS = 3  # how close to next_expected counts as "on schedule"


@dataclass(frozen=True)
class MonitorOutcome:
    on_schedule: list[RecurringExpense]
    overdue: list[RecurringExpense]


def _matches(expense: RecurringExpense, txn: Transaction) -> bool:
    """Return True if ``txn`` looks like the next occurrence of
    ``expense``: any posting on ``source_account`` plus a payee/narration
    matching ``merchant_pattern``."""
    if expense.merchant_pattern == "":
        return False
    try:
        rx = re.compile(expense.merchant_pattern)
    except re.error:
        return False
    text = " ".join(filter(None, [txn.payee, txn.narration]))
    if not rx.search(text):
        return False
    return any(p.account == expense.source_account for p in txn.postings)


def _build_on_schedule_event(expense: RecurringExpense) -> NotificationEvent:
    return NotificationEvent(
        dedup_key=f"recurring-on-schedule:{expense.id}:{expense.last_seen.isoformat() if expense.last_seen else 'none'}",
        priority=Priority.INFO,
        title=f"On schedule: {expense.label}",
        body=(
            f"{expense.entity}/{expense.label} hit on schedule "
            f"(${expense.expected_amount:.2f}, next ~{expense.next_expected.isoformat() if expense.next_expected else 'unknown'})."
        ),
        url="/recurring",
    )


def _build_overdue_event(expense: RecurringExpense, *, today: date) -> NotificationEvent:
    return NotificationEvent(
        dedup_key=f"recurring-overdue:{expense.id}:{today.isoformat()}",
        priority=Priority.WARN,
        title=f"Overdue: {expense.label}",
        body=(
            f"{expense.entity}/{expense.label} was expected by "
            f"{expense.next_expected.isoformat() if expense.next_expected else 'unknown'} "
            f"and has not been seen yet."
        ),
        url="/recurring",
    )


async def monitor_after_ingest(
    *,
    conn: sqlite3.Connection,
    new_transactions: Iterable[Transaction],
    dispatcher: Dispatcher | None = None,
    today: date | None = None,
) -> MonitorOutcome:
    """Walk confirmed recurring expenses against the freshly-ingested set
    of transactions. Returns the on-schedule and overdue rows so the
    caller can also reflect them in the UI without re-querying."""
    today = today or date.today()
    service = RecurringService(conn)
    confirmed = service.list(status=RecurringStatus.CONFIRMED.value)
    on_schedule: list[RecurringExpense] = []
    overdue: list[RecurringExpense] = []
    txn_list = list(new_transactions)
    for expense in confirmed:
        match = _find_match(expense, txn_list, today=today)
        if match is not None:
            interval = _expected_interval_days(expense.cadence)
            new_next = match.date + timedelta(days=interval)
            service.mark_seen(expense.id, last_seen=match.date, next_expected=new_next)
            updated = service.get(expense.id)
            if updated is not None:
                on_schedule.append(updated)
                if dispatcher is not None:
                    await dispatcher.send(_build_on_schedule_event(updated))
            continue
        # No match in this ingest. If next_expected + WINDOW_DAYS is past,
        # raise an overdue alert exactly once per (expense, today).
        if expense.next_expected and today > expense.next_expected + timedelta(days=WINDOW_DAYS):
            overdue.append(expense)
            if dispatcher is not None:
                await dispatcher.send(_build_overdue_event(expense, today=today))
    return MonitorOutcome(on_schedule=on_schedule, overdue=overdue)


def _find_match(
    expense: RecurringExpense,
    txns: list[Transaction],
    *,
    today: date,
) -> Transaction | None:
    if expense.next_expected is None:
        return None
    window_low = expense.next_expected - timedelta(days=WINDOW_DAYS)
    window_high = expense.next_expected + timedelta(days=WINDOW_DAYS)
    for txn in txns:
        if not (window_low <= txn.date <= window_high):
            continue
        if _matches(expense, txn):
            return txn
    return None
