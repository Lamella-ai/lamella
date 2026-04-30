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

FIXTURES = Path(__file__).parent / "fixtures" / "openrouter"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _paperless_mock(mock, doc_id: int, total: str, r_date: str, last_four: str | None):
    fields = [
        {"field": 7, "value": total},
        {"field": 10, "value": r_date},
    ]
    if last_four is not None:
        fields.append({"field": 9, "value": last_four})
    mock.get(f"/api/documents/{doc_id}/").respond(
        200,
        json={
            "id": doc_id,
            "title": f"doc {doc_id}",
            "created": f"{r_date}T00:00:00Z",
            "custom_fields": fields,
        },
    )
    mock.get("/api/custom_fields/").respond(
        200,
        json={
            "next": None,
            "results": [
                {"id": 7, "name": "receipt_total"},
                {"id": 9, "name": "payment_last_four"},
                {"id": 10, "name": "receipt_date"},
            ],
        },
    )


@pytest.fixture
def ai_enabled_client(app_client, settings):
    from pydantic import SecretStr

    settings.openrouter_api_key = SecretStr("sk-test")
    settings.ai_cache_ttl_hours = 0
    return app_client


def test_webhook_ambiguous_attaches_ai_ranking(ai_enabled_client):
    # $42.17 on 2026-04-11 → Chase 4/10 and Amex 4/12 both match.
    with respx.mock() as mock:
        # Paperless calls.
        paperless = mock
        _paperless_mock(paperless, 201, "42.17", "2026-04-11", None)

        # AI rank call — we don't know the hash, so respond with null best_match
        # but a medium confidence; the webhook should attach the payload.
        openrouter = mock
        payload = _load("match_all_bad.json")
        openrouter.post("https://openrouter.ai/api/v1/chat/completions").respond(
            200, json=payload
        )
        resp = ai_enabled_client.post(
            "/webhooks/paperless/new", json={"document_id": 201}
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    assert body["ai"] is True

    row = ai_enabled_client.app.state.db.execute(
        "SELECT ai_suggestion FROM review_queue WHERE source_ref = 'paperless:201'"
    ).fetchone()
    assert row is not None
    sugg = json.loads(row["ai_suggestion"])
    assert "ai" in sugg
    assert sugg["ai"]["best_match_hash"] is None  # AI said "no good match"
