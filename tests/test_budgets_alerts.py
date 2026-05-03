# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.features.budgets.alerts import evaluate_and_alert
from lamella.features.budgets.service import BudgetService
from lamella.ports.notification import Channel, Notifier, NotifierResult
from lamella.features.notifications.dispatcher import Dispatcher


class _FakeNotifier(Notifier):
    channel = Channel.NTFY

    def __init__(self):
        self.sent = []

    def enabled(self) -> bool:
        return True

    async def send(self, event):
        self.sent.append(event)
        return NotifierResult(ok=True)


def _ledger(tmp_path: Path, supplies_amount: float) -> LedgerReader:
    main = tmp_path / "main.bean"
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Liabilities:Acme:Card USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n",
        encoding="utf-8",
    )
    today_iso = date.today().replace(day=5).isoformat()
    main.write_text(
        'option "title" "x"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        f'{today_iso} * "Acme" "supplies"\n'
        f"  Liabilities:Acme:Card -{supplies_amount:.2f} USD\n"
        f"  Expenses:Acme:Supplies {supplies_amount:.2f} USD\n",
        encoding="utf-8",
    )
    return LedgerReader(main)


async def test_alert_fires_at_threshold_then_dedupes(db, tmp_path: Path):
    BudgetService(db).create(
        label="Supplies", entity="Acme",
        account_pattern=r"Expenses:Acme:Supplies",
        period="monthly", amount=500, alert_threshold=0.8,
        open_accounts=["Expenses:Acme:Supplies"],
    )
    notifier = _FakeNotifier()
    disp = Dispatcher(conn=db, notifiers=[notifier])

    # First pass: spent $420 -> ratio 0.84 -> hits the alert bucket.
    reader = _ledger(tmp_path, 420)
    await evaluate_and_alert(conn=db, dispatcher=disp, entries=reader.load().entries)
    assert len(notifier.sent) == 1
    assert "alert" in notifier.sent[0].title.lower()

    # Second pass at 0.85 -> still alert bucket, dedup drops the second.
    notifier.sent.clear()
    reader2 = _ledger(tmp_path, 425)
    await evaluate_and_alert(conn=db, dispatcher=disp, entries=reader2.load().entries)
    assert notifier.sent == []  # dedup window absorbs the duplicate

    # Third pass at $510 -> over 1.0 -> a new bucket, fires once.
    reader3 = _ledger(tmp_path, 510)
    await evaluate_and_alert(conn=db, dispatcher=disp, entries=reader3.load().entries)
    over_titles = [e.title for e in notifier.sent]
    assert any("over" in t.lower() for t in over_titles)


async def test_no_dispatcher_returns_progress_only(db, tmp_path: Path):
    BudgetService(db).create(
        label="A", entity="Acme",
        account_pattern=r"Expenses:Acme:Supplies",
        period="monthly", amount=500,
        open_accounts=["Expenses:Acme:Supplies"],
    )
    reader = _ledger(tmp_path, 100)
    out = await evaluate_and_alert(conn=db, dispatcher=None, entries=reader.load().entries)
    assert len(out) == 1
    assert out[0].spent == Decimal("100.00")
