# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import respx

from lamella.adapters.openrouter.client import OpenRouterClient
from lamella.features.ai_cascade.context import CandidateFacts, ReceiptFacts
from lamella.features.ai_cascade.decisions import DecisionsLog
from lamella.features.ai_cascade.match import rank_candidates


FIXTURES = Path(__file__).parent / "fixtures" / "openrouter"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _candidates() -> list[CandidateFacts]:
    return [
        CandidateFacts(
            txn_hash="hash-A",
            date=date(2026, 4, 10),
            amount=Decimal("42.17"),
            payee="Hardware Store",
            narration="supplies",
            card_account="Liabilities:Acme:Card:CardA1234",
            day_delta=0,
        ),
        CandidateFacts(
            txn_hash="hash-B",
            date=date(2026, 4, 12),
            amount=Decimal("42.17"),
            payee="Grocery Store",
            narration="groceries",
            card_account="Liabilities:Personal:Card:CardB9876",
            day_delta=2,
        ),
    ]


async def test_rank_candidates_picks_best(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    payload = _load("match_one_good.json")
    payload["choices"][0]["message"]["content"] = json.dumps(
        {
            "best_match": "hash-A",
            "confidence": 0.93,
            "reasoning": "Hardware Store txn matches.",
            "alternate_date_hypothesis": None,
        }
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=payload)
        ranking = await rank_candidates(
            client,
            paperless_id=99,
            receipt=ReceiptFacts(
                vendor="Hardware Store",
                total=Decimal("42.17"),
                currency="USD",
                date=date(2026, 4, 10),
                last4="1234",
            ),
            candidates=_candidates(),
        )
    await client.aclose()
    assert ranking is not None
    assert ranking.best_match_hash == "hash-A"
    assert ranking.confidence > 0.9
    # runners-up exclude the winner.
    assert all(h != "hash-A" for h, _ in ranking.runners_up)


async def test_rank_candidates_suppresses_off_list_hash(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    payload = _load("match_one_good.json")
    payload["choices"][0]["message"]["content"] = json.dumps(
        {
            "best_match": "hallucinated-hash",
            "confidence": 0.99,
            "reasoning": "invented",
            "alternate_date_hypothesis": None,
        }
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=payload)
        ranking = await rank_candidates(
            client,
            paperless_id=99,
            receipt=ReceiptFacts(
                vendor="Hardware Store",
                total=Decimal("42.17"),
                currency="USD",
                date=date(2026, 4, 10),
                last4=None,
            ),
            candidates=_candidates(),
        )
    await client.aclose()
    assert ranking is not None
    assert ranking.best_match_hash is None


async def test_rank_candidates_no_candidates_skips_network(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1", assert_all_called=False) as mock:
        route = mock.post("/chat/completions").respond(200, json={})
        ranking = await rank_candidates(
            client,
            paperless_id=1,
            receipt=ReceiptFacts(
                vendor="x",
                total=Decimal("1"),
                currency="USD",
                date=date(2026, 4, 10),
                last4=None,
            ),
            candidates=[],
        )
    await client.aclose()
    assert ranking is None
    assert route.call_count == 0
