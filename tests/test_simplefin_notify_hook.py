# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke test: ingest tags large FIXMEs and the notify hook dispatches
them. Checks the seam between simplefin.ingest and notify.dispatcher."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
import respx

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.ports.notification import Channel, Notifier, NotifierResult
from lamella.features.notifications.dispatcher import Dispatcher
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.notify_hook import dispatch_large_fixmes
from lamella.features.bank_sync.writer import SimpleFINWriter


FIXTURES = Path(__file__).parent / "fixtures" / "simplefin"


class _FakeNotifier(Notifier):
    channel = Channel.NTFY

    def __init__(self):
        self.sent: list = []

    def enabled(self) -> bool:
        return True

    async def send(self, event):
        self.sent.append(event)
        return NotifierResult(ok=True)


def _settings_with(base: Settings, **overrides) -> Settings:
    return base.model_copy(update=overrides)


async def test_large_fixme_triggers_notification(
    db, ledger_dir: Path, settings: Settings, monkeypatch,
):
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    # Threshold well below the largest txn (~99.99) so it fires.
    s = _settings_with(settings, simplefin_mode="active", notify_min_fixme_usd=50.0)
    account_map = {
        "account-acme-card-a": "Liabilities:Acme:Card:CardA1234",
        "account-personal-card-b": "Liabilities:Personal:Card:CardB9876",
    }
    ingest = SimpleFINIngest(
        conn=db, settings=s, reader=reader, rules=rules, reviews=reviews,
        writer=writer, ai=None, account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    payload = json.loads((FIXTURES / "two_accounts_ten_txns.json").read_text(encoding="utf-8"))
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=payload)
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    # Two txns clear $50: 99.99 (FedEx) and 65.40 (Warehouse Club).
    assert len(result.large_fixmes) >= 2
    for fx in result.large_fixmes:
        assert fx.amount >= Decimal("50")

    notifier = _FakeNotifier()
    disp = Dispatcher(conn=db, notifiers=[notifier])
    n = await dispatch_large_fixmes(dispatcher=disp, result=result)
    assert n == len(result.large_fixmes)
    assert len(notifier.sent) == n
    for event in notifier.sent:
        assert event.dedup_key.startswith("fixme:")
        assert "FIXME" in event.title
