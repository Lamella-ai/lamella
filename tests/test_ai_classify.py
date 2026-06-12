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

import httpx
import respx

from lamella.features.ai_cascade.classify import propose_account
from lamella.adapters.openrouter.client import OpenRouterClient
from lamella.features.ai_cascade.context import SimilarTxn, TxnForClassify
from lamella.features.ai_cascade.decisions import DecisionsLog


FIXTURES = Path(__file__).parent / "fixtures" / "openrouter"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def httpx_response(body: dict) -> httpx.Response:
    """Helper for queuing multiple respx responses via side_effect."""
    return httpx.Response(200, json=body)


def _txn() -> TxnForClassify:
    return TxnForClassify(
        date=date(2026, 4, 18),
        amount=Decimal("42.17"),
        currency="USD",
        payee="Hardware Store",
        narration="supplies",
        card_account="Liabilities:Acme:Card:CardA1234",
        fixme_account="Expenses:FIXME",
        txn_hash="abc123def456",
    )


async def test_propose_account_confident(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=_load("classify_confident.json"))
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[
                SimilarTxn(
                    date=date(2026, 3, 1),
                    amount=Decimal("37.00"),
                    narration="Hardware Store supplies",
                    target_account="Expenses:Acme:Supplies",
                ),
            ],
            valid_accounts=["Expenses:Acme:Supplies", "Expenses:Acme:Shipping"],
            entity="Acme",
        )
    await client.aclose()

    assert proposal is not None
    assert proposal.target_account == "Expenses:Acme:Supplies"
    assert proposal.confidence > 0.95


async def test_propose_account_off_whitelist_is_suppressed(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    payload = _load("classify_confident.json")
    payload["choices"][0]["message"]["content"] = json.dumps(
        {
            "target_account": "Expenses:Bogus:NotOpened",
            "confidence": 0.98,
            "reasoning": "Invented account.",
        }
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=payload)
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[],
            valid_accounts=["Expenses:Acme:Supplies"],
            entity="Acme",
        )
    await client.aclose()
    assert proposal is None


async def test_propose_account_cascade_escalates_on_low_conf(db):
    """Primary model returns conf=0.45 (below threshold); fallback
    model Opus returns conf=0.88. Returned proposal is from Opus
    and is tagged `escalated_from=<primary>`."""
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        route = mock.post("/chat/completions")
        route.side_effect = [
            httpx_response(_load("classify_low_conf.json")),
            httpx_response(_load("classify_escalated.json")),
        ]
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[],
            valid_accounts=["Expenses:Acme:Supplies", "Expenses:Acme:Shipping"],
            entity="Acme",
            model="anthropic/claude-haiku-4.5",
            fallback_model="anthropic/claude-opus-4.7",
            fallback_threshold=0.60,
        )
    await client.aclose()

    assert proposal is not None
    assert proposal.target_account == "Expenses:Acme:Shipping"
    assert proposal.confidence == 0.88
    assert proposal.escalated_from == "anthropic/claude-haiku-4.5"
    assert route.call_count == 2


async def test_propose_account_no_escalation_when_primary_confident(db):
    """Primary at 0.97 ≥ threshold → no fallback call made."""
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        route = mock.post("/chat/completions").respond(
            200, json=_load("classify_confident.json"),
        )
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[],
            valid_accounts=["Expenses:Acme:Supplies"],
            entity="Acme",
            model="anthropic/claude-haiku-4.5",
            fallback_model="anthropic/claude-opus-4.7",
            fallback_threshold=0.60,
        )
    await client.aclose()

    assert proposal is not None
    assert proposal.escalated_from is None
    assert route.call_count == 1


async def test_propose_account_no_escalation_when_fallback_matches_primary(db):
    """When the configured fallback is the same model as the
    primary, no escalation call is made even on low confidence."""
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        route = mock.post("/chat/completions").respond(
            200, json=_load("classify_low_conf.json"),
        )
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[],
            valid_accounts=["Expenses:Acme:Supplies"],
            entity="Acme",
            model="anthropic/claude-haiku-4.5",
            fallback_model="anthropic/claude-haiku-4.5",
            fallback_threshold=0.99,  # would trigger if models differed
        )
    await client.aclose()

    assert proposal is not None
    assert proposal.escalated_from is None
    assert route.call_count == 1


async def test_propose_account_escalates_when_primary_off_whitelist(db):
    """Primary returns an off-whitelist account (primary proposal
    is None); cascade still triggers and fallback's in-whitelist
    answer wins."""
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    off_whitelist = _load("classify_confident.json")
    off_whitelist["choices"][0]["message"]["content"] = json.dumps({
        "target_account": "Expenses:Bogus:NotOpened",
        "confidence": 0.97,
        "reasoning": "Invented account.",
    })
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        route = mock.post("/chat/completions")
        route.side_effect = [
            httpx_response(off_whitelist),
            httpx_response(_load("classify_escalated.json")),
        ]
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[],
            valid_accounts=["Expenses:Acme:Supplies", "Expenses:Acme:Shipping"],
            entity="Acme",
            model="anthropic/claude-haiku-4.5",
            fallback_model="anthropic/claude-opus-4.7",
            fallback_threshold=0.60,
        )
    await client.aclose()

    assert proposal is not None
    assert proposal.target_account == "Expenses:Acme:Shipping"
    assert proposal.escalated_from == "anthropic/claude-haiku-4.5"
    assert route.call_count == 2


async def test_propose_account_no_accounts_skips_network(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1", assert_all_called=False) as mock:
        route = mock.post("/chat/completions").respond(200, json={})
        proposal = await propose_account(
            client,
            txn=_txn(),
            similar=[],
            valid_accounts=[],
            entity="Acme",
        )
    await client.aclose()
    assert proposal is None
    assert route.call_count == 0
