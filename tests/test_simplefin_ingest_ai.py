# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

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


FIXTURES_SF = Path(__file__).parent / "fixtures" / "simplefin"
FIXTURES_AI = Path(__file__).parent / "fixtures" / "openrouter"


def _load(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _ai_settings(base: Settings, *, mode: str) -> Settings:
    return base.model_copy(
        update={
            "simplefin_mode": mode,
            "openrouter_api_key": SecretStr("sk-test"),
            "openrouter_model": "anthropic/claude-haiku-4.5",
            "ai_cache_ttl_hours": 0,
        }
    )


async def test_ingest_without_rule_stages_without_calling_ai(
    db, ledger_dir: Path, settings: Settings, monkeypatch
):
    """Post-workstream-C1: ingest never calls the AI. A row that
    doesn't match a user-rule or a loan claim stages for user touch
    in /review/staged with decided_by='auto' (unresolved). The AI is
    consulted only when the user clicks 'Ask AI' on the staged row."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None
    )

    settings_ai = _ai_settings(settings, mode="active")
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    ai = AIService(settings=settings_ai, conn=db)
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_ai,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=ai,
        account_map={"account-acme-card-a": "Liabilities:Acme:Card:CardA1234"},
    )

    one_txn = {
        "errors": [],
        "accounts": [
            {
                "id": "account-acme-card-a",
                "name": "Acme CardA",
                "currency": "USD",
                "transactions": [
                    {
                        "id": "sf-ai-1",
                        "posted": 1744243200,
                        "amount": "-42.17",
                        "description": "A HOME IMPROVEMENT STORE #1234",
                        "payee": "Hardware Store",
                    }
                ],
            }
        ],
    }

    original_bean_content = (
        (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
        if (ledger_dir / "simplefin_transactions.bean").exists() else ""
    )

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(assert_all_called=False) as mock:
        mock.get("https://bridge.example/simplefin/accounts").respond(200, json=one_txn)
        mock.post("https://openrouter.ai/api/v1/chat/completions").respond(
            200, json=_load(FIXTURES_AI / "classify_confident.json")
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    # new_txns counts bean-file writes; nothing is written on a row
    # with no rule match post-C1. fixme_txns counts staged rows.
    assert result.new_txns == 0, "no bean write when nothing matches"
    assert result.classified_by_ai == 0
    assert result.fixme_txns == 1, "row lands as staged FIXME"

    # The ledger bean file must NOT have been touched — nothing
    # classified this row, so no write happened.
    new_bean = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    delta = new_bean[len(original_bean_content):]
    assert "lamella-ai-classified: TRUE" not in delta
    assert "Expenses:Acme:Supplies" not in delta
    assert "A HOME IMPROVEMENT STORE" not in delta

    # No AI call happened during ingest. Any ai_decisions row would
    # indicate we're still burning tokens on every fetch — the very
    # thing C1 removes.
    ai_rows = db.execute(
        "SELECT * FROM ai_decisions WHERE decision_type = 'classify_txn'"
    ).fetchall()
    assert len(ai_rows) == 0, (
        "ingest must not call the AI classifier; the 'Ask AI' "
        "button is the only on-demand path post-C1"
    )

    # The row is staged with decided_by='auto' (nothing matched) so
    # /review/staged can surface it for user touch or an Ask-AI
    # click.
    staged_rows = db.execute(
        """
        SELECT t.id, t.source, d.account, d.decided_by, d.needs_review
          FROM staged_transactions t
     LEFT JOIN staged_decisions d ON d.staged_id = t.id
         WHERE json_extract(t.source_ref, '$.txn_id') = 'sf-ai-1'
        """
    ).fetchall()
    assert len(staged_rows) == 1
    assert staged_rows[0]["needs_review"] == 1
    assert staged_rows[0]["decided_by"] == "auto"


async def test_ingest_ambiguous_ai_goes_to_fixme(
    db, ledger_dir: Path, settings: Settings, monkeypatch
):
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None
    )

    settings_ai = _ai_settings(settings, mode="active")
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    ai = AIService(settings=settings_ai, conn=db)
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_ai,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=ai,
        account_map={"account-acme-card-a": "Liabilities:Acme:Card:CardA1234"},
    )
    one_txn = {
        "errors": [],
        "accounts": [
            {
                "id": "account-acme-card-a",
                "name": "Acme CardA",
                "currency": "USD",
                "transactions": [
                    {
                        "id": "sf-ai-amb",
                        "posted": 1744243200,
                        "amount": "-42.17",
                        "description": "A HOME IMPROVEMENT STORE #1234",
                        "payee": "Hardware Store",
                    }
                ],
            }
        ],
    }

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(assert_all_called=False) as mock:
        mock.get("https://bridge.example/simplefin/accounts").respond(200, json=one_txn)
        mock.post("https://openrouter.ai/api/v1/chat/completions").respond(
            200, json=_load(FIXTURES_AI / "classify_ambiguous.json")
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    assert result.fixme_txns == 1
    assert result.classified_by_ai == 0  # ambiguous → deferred, not auto-applied

    # NEXTGEN Phase B2 full swing: an ambiguous AI result no longer
    # emits a FIXME leg to the bean file. The row stays in staging
    # with needs_review=1 and the AI's suggestion (low/medium conf)
    # preserved on staged_decisions so /review/staged can show the
    # Accept button.
    contents = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "Expenses:Acme:FIXME" not in contents
    assert "Expenses:FIXME" not in contents
    assert "sf-ai-amb" not in contents  # no bean entry at all

    staged_row = db.execute(
        "SELECT t.id, t.status, d.account, d.confidence, d.needs_review "
        "FROM staged_transactions t "
        "LEFT JOIN staged_decisions d ON d.staged_id = t.id "
        "WHERE t.source = 'simplefin' "
        "  AND json_extract(t.source_ref, '$.txn_id') = 'sf-ai-amb'"
    ).fetchone()
    assert staged_row is not None
    assert staged_row["status"] in ("new", "classified", "matched")
    assert staged_row["needs_review"] == 1
