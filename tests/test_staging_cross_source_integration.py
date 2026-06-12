# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Cross-source pairing integration test — NEXTGEN.md Phase C2a.

Drives the actual SimpleFIN ingest flow + a stand-in for the
importer side, puts both sides on the unified staging surface,
then asserts the matcher sweep at the end of SimpleFIN's ingest
produces a cross-source transfer pair in ``staged_pairs``. This
is the motivating "PayPal CSV ↔ Bank One SimpleFIN"
scenario, end to end.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
import respx

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.writer import SimpleFINWriter
from lamella.features.import_.staging import StagingService


FIXTURES = Path(__file__).parent / "fixtures" / "simplefin"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def stub_bean_check(monkeypatch):
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None
    )


def _seed_csv_side_for_transfer_pair(db, *, upload_id: int = 99):
    """Insert a minimal CSV-side staged row AND the importer rows the
    transfer writer needs to resolve the source_account. Returns the
    staged row id."""
    staging = StagingService(db)
    # Importer tables (raw_rows + classifications) back the account-
    # resolution path in transfer_writer._resolve_account_for_side.
    db.execute(
        "INSERT INTO imports (id, filename, content_sha256, stored_path, status) "
        "VALUES (?, ?, ?, ?, ?)",
        (upload_id, "paypal.csv", f"sha{upload_id}", f"/tmp/{upload_id}", "ingested"),
    )
    src = db.execute(
        "INSERT INTO sources "
        "(upload_id, path, sheet_name, sheet_type, source_class) "
        "VALUES (?, ?, ?, ?, ?)",
        (upload_id, "paypal.csv", "PayPal", "primary", "paypal"),
    )
    source_id = int(src.lastrowid)
    raw = db.execute(
        "INSERT INTO raw_rows "
        "(source_id, row_num, date, amount, currency, payee, description, raw_json, hash_key) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            source_id, 7, "2026-04-20", -500.00, "USD",
            "Transfer to Bank One", "BANK ACCT TRANSFER",
            "{}", "hash-csv-side",
        ),
    )
    raw_row_id = int(raw.lastrowid)
    db.execute(
        "INSERT INTO classifications "
        "(raw_row_id, status, source_account) VALUES (?, ?, ?)",
        (raw_row_id, "imported", "Assets:Personal:PayPal"),
    )
    # Stage the row mirroring the importer's source_ref shape.
    paypal_row = staging.stage(
        source="csv",
        source_ref={
            "upload_id": upload_id,
            "sheet_name": "PayPal",
            "row_num": 7,
            "raw_row_id": raw_row_id,
        },
        session_id=str(upload_id),
        posting_date="2026-04-20",
        amount=Decimal("-500.00"),
        payee="Transfer to Bank One",
        description="BANK ACCT TRANSFER",
    )
    staging.record_decision(
        staged_id=paypal_row.id,
        account="Assets:Personal:PayPal",
        confidence="high",
        decided_by="rule",
    )
    return paypal_row


def _seed_wf_accounts_meta(db, *, account_id: str, account_path: str):
    """Seed accounts_meta so transfer_writer can resolve the SimpleFIN
    side's source account via simplefin_account_id."""
    db.execute(
        "INSERT OR REPLACE INTO accounts_meta "
        "(account_path, display_name, simplefin_account_id) VALUES (?, ?, ?)",
        (account_path, account_path, account_id),
    )


async def test_paypal_csv_pairs_with_wf_simplefin(
    db, ledger_dir: Path, settings: Settings, stub_bean_check
):
    """A pending CSV row representing a PayPal outflow is paired
    against an incoming SimpleFIN row on the Bank One account
    after the SimpleFIN ingest runs. The pair lands in
    ``staged_pairs`` as a high-confidence transfer, and both sides
    are advanced to ``status='matched'``."""
    paypal_row = _seed_csv_side_for_transfer_pair(db)

    # Now drive a SimpleFIN fetch with a fixture containing a matching
    # deposit on the WF account.
    account_map = {
        "account-wf-checking": "Assets:Personal:BankOne:Checking",
    }
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
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
        account_map=account_map,
    )

    # Single-account bridge response with one +500 deposit on 2026-04-20.
    bridge_response = {
        "errors": [],
        "accounts": [
            {
                "id": "account-wf-checking",
                "name": "WF Checking",
                "currency": "USD",
                "balance": "12696.83",
                "transactions": [
                    {
                        "id": "sf-wf-deposit-1",
                        "posted": 1776556800,  # 2026-04-20 UTC
                        "amount": "500.00",
                        "description": "Transfer from PayPal",
                        "payee": "PayPal",
                    }
                ],
            }
        ],
    }

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=bridge_response)
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    assert result.error is None

    # The matcher sweep at the end of ingest should have found the pair.
    pairs = db.execute(
        "SELECT kind, confidence, a_staged_id, b_staged_id FROM staged_pairs"
    ).fetchall()
    assert len(pairs) == 1, f"expected 1 paired transfer, got {len(pairs)}"
    p = pairs[0]
    assert p["kind"] == "transfer"
    assert p["confidence"] == "high"
    sides = {int(p["a_staged_id"]), int(p["b_staged_id"])}
    assert paypal_row.id in sides


async def test_paired_transfer_writes_balanced_txn_not_fixmes(
    db, ledger_dir: Path, settings: Settings, stub_bean_check,
    monkeypatch,
):
    """NEXTGEN Phase C2b: when the matcher identifies a cross-source
    transfer pair, the transfer writer emits a single balanced
    transaction to connector_transfers.bean, and the one-sided
    FIXME entry that would otherwise have been written to
    simplefin_transactions.bean is suppressed."""
    # Stub the transfer writer's bean-check too (it uses
    # capture_bean_check + run_bean_check_vs_baseline which shell out).
    monkeypatch.setattr(
        "lamella.features.import_.staging.transfer_writer.capture_bean_check",
        lambda _path: (0, ""),
    )
    monkeypatch.setattr(
        "lamella.features.import_.staging.transfer_writer.run_bean_check_vs_baseline",
        lambda _path, _baseline: None,
    )

    paypal_row = _seed_csv_side_for_transfer_pair(db)
    _seed_wf_accounts_meta(
        db,
        account_id="account-wf-checking",
        account_path="Assets:Personal:BankOne:Checking",
    )

    account_map = {"account-wf-checking": "Assets:Personal:BankOne:Checking"}
    reader = LedgerReader(ledger_dir / "main.bean")
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = settings.model_copy(update={"simplefin_mode": "active"})
    ingest = SimpleFINIngest(
        conn=db, settings=settings_active, reader=reader,
        rules=RuleService(db), reviews=ReviewService(db), writer=writer,
        ai=None, account_map=account_map,
    )

    bridge_response = {
        "errors": [],
        "accounts": [{
            "id": "account-wf-checking", "name": "WF", "currency": "USD",
            "balance": "12696.83",
            "transactions": [{
                "id": "sf-wf-deposit-1", "posted": 1776556800,
                "amount": "500.00",
                "description": "Transfer from PayPal",
                "payee": "PayPal",
            }],
        }],
    }

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=bridge_response)
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    assert result.error is None

    # connector_transfers.bean should contain the balanced two-leg txn.
    transfers_path = ledger_dir / "connector_transfers.bean"
    assert transfers_path.exists(), "connector_transfers.bean should be created"
    transfers_text = transfers_path.read_text(encoding="utf-8")
    assert "Assets:Personal:BankOne:Checking" in transfers_text
    assert "Assets:Personal:PayPal" in transfers_text
    assert "500.00 USD" in transfers_text
    assert "-500.00 USD" in transfers_text
    # Post-Phase-2: SimpleFIN provenance lives on posting meta as a
    # paired indexed source (lamella-source-0/lamella-source-reference-id-0),
    # not on the legacy txn-level lamella-simplefin-id key.
    assert 'lamella-source-0: "simplefin"' in transfers_text
    assert 'lamella-source-reference-id-0: "sf-wf-deposit-1"' in transfers_text

    # The one-sided FIXME entry must NOT have been emitted to
    # simplefin_transactions.bean. If it existed, we'd see sf-wf-deposit-1
    # in both files — a double-write.
    sf_path = ledger_dir / "simplefin_transactions.bean"
    if sf_path.exists():
        sf_text = sf_path.read_text(encoding="utf-8")
        assert "sf-wf-deposit-1" not in sf_text, (
            "paired txn should have been suppressed from simplefin_transactions.bean"
        )

    # Both sides of the pair are now in status='promoted' pointing at
    # connector_transfers.bean.
    rows = db.execute(
        "SELECT id, status, promoted_to_file FROM staged_transactions "
        "WHERE id IN (?, ?)",
        (
            paypal_row.id,
            db.execute(
                "SELECT id FROM staged_transactions "
                "WHERE source = 'simplefin'"
            ).fetchone()["id"],
        ),
    ).fetchall()
    for r in rows:
        assert r["status"] == "promoted"
        assert "connector_transfers.bean" in (r["promoted_to_file"] or "")


async def test_no_pair_when_amounts_differ(
    db, ledger_dir: Path, settings: Settings, stub_bean_check
):
    """Sanity: if the CSV outflow and the SimpleFIN deposit don't
    match on amount, the matcher stays silent."""
    staging = StagingService(db)
    staging.stage(
        source="csv",
        source_ref={"upload_id": 99, "row_num": 1},
        session_id="99",
        posting_date="2026-04-20",
        amount=Decimal("-499.00"),  # off by one
        payee="Transfer to bank",
    )

    account_map = {"account-wf-checking": "Assets:Personal:BankOne:Checking"}
    reader = LedgerReader(ledger_dir / "main.bean")
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = settings.model_copy(update={"simplefin_mode": "active"})
    ingest = SimpleFINIngest(
        conn=db, settings=settings_active, reader=reader,
        rules=RuleService(db), reviews=ReviewService(db), writer=writer,
        ai=None, account_map=account_map,
    )

    bridge_response = {
        "errors": [],
        "accounts": [{
            "id": "account-wf-checking", "name": "WF", "currency": "USD",
            "transactions": [{
                "id": "sf-diff", "posted": 1776556800,
                "amount": "500.00", "description": "Deposit",
            }],
        }],
    }
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=bridge_response)
        await ingest.run(client=client, trigger="manual")
    await client.aclose()

    count = db.execute("SELECT COUNT(*) AS n FROM staged_pairs").fetchone()["n"]
    assert count == 0
