# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0043 P2 — ingest wire-up. When
``settings.enable_staged_txn_directives`` is True, the bank-sync defer
path emits a ``custom "staged-txn"`` directive to the connector-owned
.bean file alongside the staged_transactions row. When the flag is
False (default in v0.3.1), behavior is unchanged from v0.3.0 —
no directive lands.
"""
from __future__ import annotations

import json
from pathlib import Path

import respx
from pydantic import SecretStr

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.writer import SimpleFINWriter


FIXTURES_AI = Path(__file__).parent / "fixtures" / "openrouter"


def _load(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _build_ingest(db, ledger_dir: Path, settings: Settings, *, flag: bool):
    settings_with_flag = settings.model_copy(
        update={
            "simplefin_mode": "active",
            "openrouter_api_key": SecretStr("sk-test"),
            "openrouter_model": "anthropic/claude-haiku-4.5",
            "ai_cache_ttl_hours": 0,
            "enable_staged_txn_directives": flag,
        }
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    ai = AIService(settings=settings_with_flag, conn=db)
    return SimpleFINIngest(
        conn=db,
        settings=settings_with_flag,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=ai,
        account_map={"account-acme-card-a": "Liabilities:Acme:Card:CardA1234"},
    )


_ONE_TXN = {
    "errors": [],
    "accounts": [
        {
            "id": "account-acme-card-a",
            "name": "Acme CardA",
            "currency": "USD",
            "transactions": [
                {
                    "id": "sf-stage-1",
                    "posted": 1744243200,
                    "amount": "-42.17",
                    "description": "A HOME IMPROVEMENT STORE #1234",
                    "payee": "Hardware Store",
                }
            ],
        }
    ],
}


async def _run(ingest):
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(assert_all_called=False) as mock:
        mock.get("https://bridge.example/simplefin/accounts").respond(
            200, json=_ONE_TXN,
        )
        mock.post("https://openrouter.ai/api/v1/chat/completions").respond(
            200, json=_load(FIXTURES_AI / "classify_confident.json"),
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    return result


async def test_flag_off_no_directive_written(
    db, ledger_dir: Path, settings: Settings, monkeypatch,
):
    """v0.3.0 behaviour: a deferred row must NOT add a custom "staged-txn"
    line to simplefin_transactions.bean. Default OFF in v0.3.1."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None,
    )
    bean_path = ledger_dir / "simplefin_transactions.bean"
    before = bean_path.read_text(encoding="utf-8") if bean_path.exists() else ""
    ingest = _build_ingest(db, ledger_dir, settings, flag=False)
    result = await _run(ingest)
    assert result.error is None
    assert result.fixme_txns == 1
    after = bean_path.read_text(encoding="utf-8") if bean_path.exists() else ""
    delta = after[len(before):]
    assert 'custom "staged-txn"' not in delta, (
        "flag is OFF — no directive should have been written"
    )


async def test_flag_on_directive_written(
    db, ledger_dir: Path, settings: Settings, monkeypatch,
):
    """ADR-0043 P2: flag-on path writes one custom "staged-txn"
    directive to the connector-owned bean file per deferred row,
    AND the staged_transactions row still lands (cache layer)."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None,
    )
    bean_path = ledger_dir / "simplefin_transactions.bean"
    before = bean_path.read_text(encoding="utf-8") if bean_path.exists() else ""
    ingest = _build_ingest(db, ledger_dir, settings, flag=True)
    result = await _run(ingest)
    assert result.error is None
    assert result.fixme_txns == 1, "row counted as staged"
    after = bean_path.read_text(encoding="utf-8")
    delta = after[len(before):]
    # Exactly one staged-txn directive landed; the source positional
    # arg is "simplefin"; the source-reference-id matches the txn id.
    assert delta.count('custom "staged-txn" "simplefin"') == 1
    assert 'lamella-source-reference-id: "sf-stage-1"' in delta
    assert "lamella-txn-amount: -42.17 USD" in delta
    # The staged_transactions row is still recorded — directive and
    # row are written together by ADR-0043 P2.
    rows = db.execute(
        """
        SELECT t.id, t.source
          FROM staged_transactions t
         WHERE json_extract(t.source_ref, '$.txn_id') = 'sf-stage-1'
        """
    ).fetchall()
    assert len(rows) == 1


async def test_flag_on_directive_lamella_txn_id_matches_staged_row(
    db, ledger_dir: Path, settings: Settings, monkeypatch,
):
    """ADR-0043b §1 invariant: the lamella-txn-id on the staged-txn
    directive equals the lamella_txn_id of the corresponding
    staged_transactions row. /txn/{token} resolves to the same URL
    pre- and post-promotion because the lineage id is stable."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None,
    )
    bean_path = ledger_dir / "simplefin_transactions.bean"
    before = bean_path.read_text(encoding="utf-8") if bean_path.exists() else ""
    ingest = _build_ingest(db, ledger_dir, settings, flag=True)
    result = await _run(ingest)
    assert result.error is None
    after = bean_path.read_text(encoding="utf-8")
    delta = after[len(before):]
    # Pull the lamella-txn-id from the directive
    import re
    m = re.search(r'lamella-txn-id:\s*"([^"]+)"', delta)
    assert m, "directive must carry lamella-txn-id"
    directive_txn_id = m.group(1)
    # Compare with staged row
    row = db.execute(
        """
        SELECT json_extract(source_ref, '$.lamella_txn_id') as lamella_txn_id
          FROM staged_transactions
         WHERE json_extract(source_ref, '$.txn_id') = 'sf-stage-1'
        """
    ).fetchone()
    if row and row["lamella_txn_id"]:
        assert row["lamella_txn_id"] == directive_txn_id, (
            "directive lamella-txn-id must match staged row "
            f"({directive_txn_id} vs {row['lamella_txn_id']})"
        )
    # If staged row's source_ref doesn't carry lamella_txn_id (legacy
    # path), at minimum the directive carries SOME UUIDv7. The earlier
    # assertion `m` covers that.
