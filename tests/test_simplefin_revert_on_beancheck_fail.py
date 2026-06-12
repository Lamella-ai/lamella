# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.writer import SimpleFINWriter


FIXTURES = Path(__file__).parent / "fixtures" / "simplefin"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


async def test_bean_check_failure_reverts_write_and_records_error(
    db, ledger_dir: Path, settings: Settings, monkeypatch
):
    def _fail(_main_bean):
        raise BeanCheckError("deliberate — test")

    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", _fail
    )

    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    # NEXTGEN Phase B2 full swing: without an auto-applying rule
    # everything defers to staging and no bean write happens, so the
    # bean-check hook never fires. Seed a rule so at least one txn
    # enters the write batch and the monkeypatched bean-check fails.
    rules.create(
        pattern_type="merchant_contains",
        pattern_value="hardware",
        target_account="Expenses:Acme:Supplies",
        confidence=1.0,
        created_by="user",
    )
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = settings.model_copy(update={"simplefin_mode": "active"})
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map={
            "account-acme-card-a": "Liabilities:Acme:Card:CardA1234",
            "account-personal-card-b": "Liabilities:Personal:Card:CardB9876",
        },
    )

    target = ledger_dir / "simplefin_transactions.bean"
    pre_size = target.stat().st_size
    pre_bytes = target.read_bytes()

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    # The run records the failure instead of crashing.
    assert result.error is not None
    assert "bean-check" in result.error.lower()
    assert result.bean_check_ok is False

    # Ledger file is byte-identical to pre-ingest.
    assert target.stat().st_size == pre_size
    assert target.read_bytes() == pre_bytes

    row = db.execute(
        "SELECT error, bean_check_ok FROM simplefin_ingests WHERE id = ?",
        (result.ingest_id,),
    ).fetchone()
    assert row is not None
    assert row["bean_check_ok"] == 0
    assert row["error"] and "bean-check" in row["error"].lower()
