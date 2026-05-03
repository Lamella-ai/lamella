# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""End-to-end tests for the POST /paperless/{doc_id}/verify and
/enrich routes (Slice B)."""
from __future__ import annotations

import json

import httpx
import respx


def _enable_writeback(conn):
    conn.execute(
        "INSERT OR REPLACE INTO app_settings (key, value) "
        "VALUES ('paperless_writeback_enabled', '1')"
    )


def _bypass_setup_gate(app) -> None:
    """The fixture ledger has raw transactions and no lamella-* markers,
    so ``create_app`` flags the instance as needs_welcome=True and the
    setup-gate middleware redirects every request to /setup/welcome.
    Tests that want to exercise post-setup behaviour on this fixture
    have to flip the flag after app startup."""
    app.state.needs_welcome = False
    app.state.needs_reconstruct = False


def _seed_doc(conn, *, paperless_id=42):
    conn.execute(
        """
        INSERT INTO paperless_doc_index (
            paperless_id, title, vendor, total_amount, document_date,
            content_excerpt, mime_type, tags_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (paperless_id, "Warehouse Club", "Warehouse Club", "58.12", "2064-01-08",
         "Warehouse Club Wholesale\nFuel\n", "image/jpeg", "[1]"),
    )


def test_verify_route_returns_diff_partial(app_client, settings, monkeypatch):
    """POST /paperless/{id}/verify/sync fires a real verify flow
    and returns the HTML partial with the diff. The main
    /paperless/{id}/verify route runs the same flow as a job and
    returns the progress modal; the /sync variant preserves the
    one-shot HTML response path for non-interactive callers."""
    # Need AI enabled AND paperless configured AND writeback on.
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    db = app_client.app.state.db
    _seed_doc(db)
    _enable_writeback(db)
    _bypass_setup_gate(app_client.app)
    # Patch the settings object the app runs with so ai_enabled → True.
    app_client.app.state.settings.openrouter_api_key = (
        __import__("pydantic").SecretStr("sk-test")
    )

    classify_payload = {
        "id": "x", "model": "anthropic/claude-opus-4.7",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": json.dumps({
                    "receipt_date": "2026-04-17",
                    "vendor": "Warehouse Club",
                    "total": "58.12",
                    "confidence": {
                        "receipt_date": 0.95, "vendor": 0.99, "total": 0.98,
                    },
                    "ocr_errors_noted": ["Year OCR'd as 2064"],
                    "reasoning": "Corrected year.",
                }),
            },
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 500, "completion_tokens": 80},
    }

    with respx.mock(base_url="https://paperless.test") as paperless_mock, \
         respx.mock(base_url="https://openrouter.ai/api/v1") as or_mock:
        paperless_mock.get("/api/documents/42/download/").respond(
            200, content=b"fakeimage",
            headers={"content-type": "image/jpeg"},
        )
        paperless_mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        paperless_mock.post("/api/tags/").respond(
            201, json={"id": 99, "name": "Lamella Fixed"},
        )
        paperless_mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [1], "custom_fields": []},
        )
        paperless_mock.patch("/api/documents/42/").respond(
            200, json={"id": 42},
        )
        paperless_mock.post("/api/documents/42/notes/").respond(
            200, json={"id": 7},
        )
        or_mock.post("/chat/completions").respond(200, json=classify_payload)

        resp = app_client.post(
            "/documents/42/verify/sync",
            data={"suspected_date": "2026-04-18", "reason": "year off"},
        )

    assert resp.status_code == 200
    assert "Vision re-extract" in resp.text or "no differences" in resp.text
    # The diff for receipt_date must appear in the rendered HTML.
    assert "2026-04-17" in resp.text
    assert "2064-01-08" in resp.text


def test_enrich_route_returns_success_partial(app_client, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    db = app_client.app.state.db
    _seed_doc(db)
    _enable_writeback(db)
    _bypass_setup_gate(app_client.app)
    app_client.app.state.settings.openrouter_api_key = (
        __import__("pydantic").SecretStr("sk-test")
    )

    with respx.mock(base_url="https://paperless.test") as paperless_mock:
        paperless_mock.post("/api/documents/42/notes/").respond(
            200, json={"id": 1, "note": "x"},
        )
        paperless_mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        paperless_mock.post("/api/tags/").respond(
            201, json={"id": 88, "name": "Lamella Enriched"},
        )
        paperless_mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [1], "custom_fields": []},
        )
        paperless_mock.patch("/api/documents/42/").respond(
            200, json={"id": 42},
        )

        resp = app_client.post(
            "/documents/42/enrich",
            data={
                "vehicle": "2009 Work SUV",
                "entity": "Personal",
                "note_body": "Gas for 2009 Work SUV",
            },
        )
    assert resp.status_code == 200
    assert "Note posted" in resp.text
    assert "Tagged" in resp.text
