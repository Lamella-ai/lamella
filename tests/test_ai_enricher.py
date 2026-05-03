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

from lamella.features.ai_cascade.enricher import AIFixmeEnricher
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.scanner import FixmeScanner
from lamella.features.rules.service import RuleService


FIXTURES = Path(__file__).parent / "fixtures" / "openrouter"

_FIXME_APPEND = """
2024-01-01 open Expenses:FIXME USD

2026-04-10 * "Hardware Store" "Supplies for workshop"
  simplefin-id: "sf-2010"
  Liabilities:Acme:Card:CardA1234  -42.17 USD
  Expenses:FIXME                      42.17 USD
"""


def _add_fixme(ledger_dir: Path) -> None:
    path = ledger_dir / "simplefin_transactions.bean"
    path.write_text(path.read_text(encoding="utf-8") + _FIXME_APPEND, encoding="utf-8")


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _wire(ledger_dir: Path, db, settings, *, confident: bool):
    _add_fixme(ledger_dir)
    reader = LedgerReader(ledger_dir / "main.bean")
    reviews = ReviewService(db)
    rules = RuleService(db)
    scanner = FixmeScanner(
        reader=reader, reviews=reviews, rules=rules, override_writer=None
    )
    scanner.scan()

    settings.openrouter_api_key = type(settings.openrouter_api_key or "")("sk-test") if settings.openrouter_api_key else None
    # ensure key present
    from pydantic import SecretStr
    settings.openrouter_api_key = SecretStr("sk-test")
    settings.ai_cache_ttl_hours = 0

    ai = AIService(settings=settings, conn=db)
    enricher = AIFixmeEnricher(
        ai=ai, reader=reader, reviews=reviews, rules=rules,
    )
    return enricher, reviews, reader


@pytest.mark.asyncio
async def test_enricher_records_high_confidence_ai_as_suggestion(
    ledger_dir, db, settings
):
    """Post-workstream-A: the enricher never writes to the ledger. A
    high-confidence AI proposal is recorded on the review row as a
    suggestion; the user's click-accept in the UI is what promotes
    it. See docs/specs/AI-CLASSIFICATION.md tier-2."""
    enricher, reviews, _reader = _wire(ledger_dir, db, settings, confident=True)
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=_load("classify_confident.json"))
        stats = await enricher.run()

    assert stats["auto_applied"] == 0
    assert stats["enriched"] == 1
    open_items = reviews.list_open()
    assert len(open_items) == 1, "fixme stays open for user confirmation"
    payload = json.loads(open_items[0].ai_suggestion)
    assert "ai" in payload
    assert payload["ai"]["target_account"] == "Expenses:Acme:Supplies"
    assert payload["ai"]["confidence"] >= 0.95

    # Ledger overrides file must NOT have been written.
    overrides_path = settings.connector_overrides_path
    if overrides_path.exists():
        assert "Expenses:Acme:Supplies" not in overrides_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_enricher_adds_suggestion_below_threshold(ledger_dir, db, settings):
    enricher, reviews, _reader = _wire(ledger_dir, db, settings, confident=False)
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=_load("classify_ambiguous.json"))
        stats = await enricher.run()

    assert stats["enriched"] == 1
    assert stats["auto_applied"] == 0
    item = reviews.list_open()[0]
    payload = json.loads(item.ai_suggestion)
    assert "ai" in payload
    assert payload["ai"]["target_account"] == "Expenses:Acme:Supplies"
    assert payload["ai"]["confidence"] < 0.95


@pytest.mark.asyncio
async def test_enricher_no_op_when_ai_disabled(ledger_dir, db, settings):
    _add_fixme(ledger_dir)
    reader = LedgerReader(ledger_dir / "main.bean")
    reviews = ReviewService(db)
    rules = RuleService(db)
    FixmeScanner(reader=reader, reviews=reviews, rules=rules).scan()

    settings.openrouter_api_key = None  # AI disabled
    ai = AIService(settings=settings, conn=db)
    enricher = AIFixmeEnricher(
        ai=ai, reader=reader, reviews=reviews, rules=rules,
    )
    stats = await enricher.run()
    assert stats == {"considered": 0, "enriched": 0, "auto_applied": 0, "errors": 0}
