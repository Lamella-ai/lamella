# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import respx

from lamella.adapters.openrouter.client import OpenRouterClient
from lamella.features.ai_cascade.decisions import DecisionsLog
from lamella.features.ai_cascade.notes import parse_note


FIXTURES = Path(__file__).parent / "fixtures" / "openrouter"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


async def test_parse_note_extracts_hints(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=_load("parse_note_homedepot.json"))
        annotations = await parse_note(
            client,
            note_id=11,
            body="Hardware Store for the Acme warehouse",
            captured_at=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
            entities=["Acme", "Personal"],
        )
    await client.aclose()
    assert annotations is not None
    assert annotations.merchant_hint == "Hardware Store"
    assert annotations.entity_hint == "Acme"
    assert "workshop" in annotations.keywords


async def test_parse_note_enforces_entity_list(db):
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=DecisionsLog(db),
        cache_ttl_hours=0,
    )
    payload = _load("parse_note_homedepot.json")
    payload["choices"][0]["message"]["content"] = json.dumps(
        {
            "merchant_hint": "Hardware Store",
            "entity_hint": "Martian",
            "amount_hint": None,
            "date_hint": None,
            "keywords": ["a", "b"],
        }
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=payload)
        annotations = await parse_note(
            client,
            note_id=12,
            body="...",
            captured_at=datetime.now(timezone.utc),
            entities=["Acme"],
        )
    await client.aclose()
    assert annotations is not None
    assert annotations.entity_hint is None  # off-list hint discarded
