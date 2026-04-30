# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import httpx
import pytest
import respx


def _mock_paperless(doc_id: int, total: str, r_date: str, last_four: str | None):
    mock = respx.mock(base_url="https://paperless.test")
    custom_fields = [
        {"field": 7, "value": total},
        {"field": 10, "value": r_date},
    ]
    if last_four is not None:
        custom_fields.append({"field": 9, "value": last_four})
    mock.get(f"/api/documents/{doc_id}/").respond(
        200,
        json={
            "id": doc_id,
            "title": f"doc {doc_id}",
            "created": f"{r_date}T00:00:00Z",
            "custom_fields": custom_fields,
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
    return mock


def test_webhook_exact_match_links_receipt(app_client):
    with _mock_paperless(100, "42.17", "2026-04-10", "1234"):
        resp = app_client.post(
            "/webhooks/paperless/new",
            json={"document_id": 100},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "linked"
    assert body["match"] == "exact"

    rows = app_client.app.state.db.execute(
        "SELECT * FROM receipt_links WHERE paperless_id = 100"
    ).fetchall()
    assert len(rows) == 1


def test_webhook_ambiguous_enqueues_review(app_client):
    # $42.17 on 2026-04-11, no last_four → matches both CardA1234 (4/10) and
    # CardB9876 (4/12) within ±1 day.
    with _mock_paperless(101, "42.17", "2026-04-11", None):
        resp = app_client.post(
            "/webhooks/paperless/new",
            json={"document_id": 101},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "queued"
    assert body["kind"] == "ambiguous_match"

    rows = app_client.app.state.db.execute(
        "SELECT kind, source_ref FROM review_queue WHERE source_ref = 'paperless:101'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["kind"] == "ambiguous_match"


def test_webhook_no_candidates_enqueues_unmatched(app_client):
    with _mock_paperless(102, "999.99", "2026-04-11", None):
        resp = app_client.post(
            "/webhooks/paperless/new",
            json={"document_id": 102},
        )
    body = resp.json()
    assert body["status"] == "queued"
    assert body["kind"] == "receipt_unmatched"


def test_webhook_requires_paperless_configured(tmp_path):
    from fastapi.testclient import TestClient

    from lamella.core.config import Settings
    from lamella.main import create_app

    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "no-ledger",
        paperless_url=None,
        paperless_api_token=None,
    )
    # No ledger dir exists -> reader.load() returns empty. That's fine.
    settings.ledger_dir.mkdir(parents=True, exist_ok=True)
    app = create_app(settings=settings)
    with TestClient(app) as client:
        resp = client.post("/webhooks/paperless/new", json={"document_id": 1})
    assert resp.status_code == 503
