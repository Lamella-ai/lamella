# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from pydantic import BaseModel, Field

from lamella.adapters.openrouter.client import AIError, OpenRouterClient
from lamella.features.ai_cascade.decisions import CACHED_MODEL_SENTINEL, DecisionsLog


FIXTURES = Path(__file__).parent / "fixtures" / "openrouter"


class ClassifyShape(BaseModel):
    target_account: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


async def test_chat_with_images_sends_vision_shape(db):
    """When `images=` is supplied, the outgoing messages must use
    the OpenAI-compatible vision shape: the user message's content
    is a LIST of content blocks with image_url + text, not a plain
    string."""
    captured: dict = {}

    async def _capture(request):
        captured["body"] = request.read()
        return httpx.Response(200, json=_load("classify_confident.json"))

    decisions = DecisionsLog(db)
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-opus-4.7",
        decisions=decisions,
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").mock(side_effect=_capture)
        await client.chat(
            decision_type="receipt_verify",
            input_ref="paperless:42",
            system="sys",
            user="what is on this receipt?",
            schema=ClassifyShape,
            images=[(b"\x89PNG\r\n\x1a\nFAKE", "image/png")],
        )
    await client.aclose()

    body = json.loads(captured["body"])
    user_msg = body["messages"][1]
    assert user_msg["role"] == "user"
    assert isinstance(user_msg["content"], list)
    blocks = user_msg["content"]
    assert blocks[0]["type"] == "image_url"
    assert blocks[0]["image_url"]["url"].startswith("data:image/png;base64,")
    assert blocks[-1]["type"] == "text"
    assert blocks[-1]["text"] == "what is on this receipt?"


async def test_chat_image_bytes_in_prompt_hash(db):
    """Two calls with the same prompt text but different images
    must NOT collide in the decision cache."""
    from lamella.adapters.openrouter.client import _hash_prompt

    h1 = _hash_prompt("m", "s", "u", "S", images=[(b"a", "image/png")])
    h2 = _hash_prompt("m", "s", "u", "S", images=[(b"b", "image/png")])
    h_none = _hash_prompt("m", "s", "u", "S", images=None)
    assert h1 != h2
    assert h1 != h_none


async def test_chat_happy_path_logs_decision(db):
    decisions = DecisionsLog(db)
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=decisions,
        cache_ttl_hours=0,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").respond(200, json=_load("classify_confident.json"))
        result = await client.chat(
            decision_type="classify_txn",
            input_ref="hash:abcd",
            system="sys",
            user="hello",
            schema=ClassifyShape,
        )
    await client.aclose()

    assert result.cached is False
    assert result.prompt_tokens == 420
    assert result.completion_tokens == 48
    assert result.data.target_account == "Expenses:Acme:Supplies"
    assert result.data.confidence > 0.95

    row = db.execute(
        "SELECT * FROM ai_decisions WHERE id = ?", (result.decision_id,)
    ).fetchone()
    assert row is not None
    assert row["decision_type"] == "classify_txn"
    assert row["prompt_tokens"] == 420
    assert row["completion_tokens"] == 48
    assert row["prompt_hash"] is not None


async def test_chat_cache_hit_returns_cached_row(db):
    decisions = DecisionsLog(db)
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=decisions,
        cache_ttl_hours=24,
    )
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        route = mock.post("/chat/completions").respond(
            200, json=_load("classify_confident.json")
        )
        first = await client.chat(
            decision_type="classify_txn",
            input_ref="hash:abcd",
            system="sys",
            user="hello",
            schema=ClassifyShape,
        )
        second = await client.chat(
            decision_type="classify_txn",
            input_ref="hash:abcd",
            system="sys",
            user="hello",
            schema=ClassifyShape,
        )
    await client.aclose()

    assert route.call_count == 1, "cache should suppress the second HTTP call"
    assert first.cached is False
    assert second.cached is True
    assert second.prompt_tokens == 0
    assert second.model == CACHED_MODEL_SENTINEL

    row = db.execute(
        "SELECT * FROM ai_decisions WHERE id = ?", (second.decision_id,)
    ).fetchone()
    assert row["model"] == CACHED_MODEL_SENTINEL
    assert row["prompt_tokens"] == 0


async def test_chat_5xx_retries_then_succeeds(db):
    decisions = DecisionsLog(db)
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=decisions,
        cache_ttl_hours=0,
    )
    call_order = []

    def handler(request):
        call_order.append(True)
        if len(call_order) < 2:
            return httpx.Response(502, json={"error": {"message": "bad gateway"}})
        return httpx.Response(200, json=_load("classify_confident.json"))

    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        mock.post("/chat/completions").mock(side_effect=handler)
        result = await client.chat(
            decision_type="classify_txn",
            input_ref="hash:retry",
            system="sys",
            user="hello",
            schema=ClassifyShape,
        )
    await client.aclose()

    assert len(call_order) >= 2
    assert result.data.confidence > 0.9


async def test_chat_schema_failure_logs_error_and_raises(db):
    decisions = DecisionsLog(db)
    client = OpenRouterClient(
        api_key="sk-test",
        default_model="anthropic/claude-haiku-4.5",
        decisions=decisions,
        cache_ttl_hours=0,
    )
    garbage = {
        "choices": [{"message": {"content": "not-json-at-all"}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }
    with respx.mock(base_url="https://openrouter.ai/api/v1") as mock:
        # Two identical garbage responses; the client should run one repair
        # attempt, then give up and raise.
        mock.post("/chat/completions").respond(200, json=garbage)
        with pytest.raises(AIError):
            await client.chat(
                decision_type="classify_txn",
                input_ref="hash:bad",
                system="sys",
                user="hello",
                schema=ClassifyShape,
            )
    await client.aclose()

    rows = db.execute(
        "SELECT result FROM ai_decisions WHERE input_ref = ?", ("hash:bad",)
    ).fetchall()
    assert rows, "an error row should be logged"
    assert any("error" in json.loads(r["result"]) for r in rows)
