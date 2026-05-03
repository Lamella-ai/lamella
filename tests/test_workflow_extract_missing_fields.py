# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0062 — extract_missing_fields rule applies the right tag op
based on the action result."""
from __future__ import annotations

import json
from dataclasses import replace

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.adapters.paperless.schemas import Document
from lamella.features.paperless_bridge.tag_workflow import (
    ActionResult,
    DocumentSelector,
    KIND_WORKFLOW_ACTION,
    KIND_WORKFLOW_ANOMALY,
    RunExtraction,
    TAG_AWAITING_EXTRACTION,
    TAG_EXTRACTED,
    TAG_NEEDS_REVIEW,
    TagOp,
    WorkflowRule,
    extract_missing_fields_rule,
    run_rule,
)


def _seed_doc_index(conn, *, paperless_id: int):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, title, vendor, tags_json)
        VALUES (?, ?, ?, '[]')
        """,
        (paperless_id, f"doc-{paperless_id}", f"Vendor {paperless_id}"),
    )


@pytest.mark.asyncio
async def test_success_outcome_applies_extracted_tag(monkeypatch, db):
    """When the action returns status=success, the on_success tag
    op (Add Lamella:Extracted) must be applied via PATCH."""
    docs_payload = {
        "next": None,
        "results": [
            {
                "id": 100,
                "title": "Doc 100",
                "tags": [],
                "custom_fields": [],
                "created": "2026-04-01T00:00:00Z",
            },
        ],
    }
    _seed_doc_index(db, paperless_id=100)

    patches: list[dict] = []

    def _patch_capture(request):
        patches.append({
            "url": str(request.url),
            "body": json.loads(request.read()),
        })
        return httpx.Response(200, json={})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 5, "name": TAG_EXTRACTED},
                {"id": 6, "name": TAG_NEEDS_REVIEW},
            ]},
        )
        mock.get("/api/documents/").respond(200, json=docs_payload)
        mock.get("/api/documents/100/").respond(
            200, json={"id": 100, "tags": [], "custom_fields": []},
        )
        mock.patch("/api/documents/100/").mock(side_effect=_patch_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            # Stub the action's run() to avoid wiring AIService.
            async def _success_run(self, doc, *, conn, client):
                return ActionResult(
                    status="success", summary="extracted",
                    details={"confidences": {"vendor": 0.95}},
                )
            monkeypatch.setattr(RunExtraction, "run", _success_run)
            report = await run_rule(
                extract_missing_fields_rule, conn=db, paperless_client=client,
            )

    assert report.docs_matched == 1
    assert report.successes == 1
    assert report.anomalies == 0
    # Must have PATCHed to add tag id 5 (Lamella:Extracted) to the doc.
    assert len(patches) == 1
    assert patches[0]["body"]["tags"] == [5]


@pytest.mark.asyncio
async def test_anomaly_outcome_applies_needs_review_tag(monkeypatch, db):
    """When the action returns status=anomaly, the on_anomaly tag op
    (Add Lamella:NeedsReview) must be applied."""
    docs_payload = {
        "next": None,
        "results": [
            {
                "id": 200,
                "tags": [],
                "custom_fields": [],
                "created": "2026-04-01T00:00:00Z",
            },
        ],
    }
    _seed_doc_index(db, paperless_id=200)

    patches: list[dict] = []

    def _patch_capture(request):
        patches.append(json.loads(request.read()))
        return httpx.Response(200, json={})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 5, "name": TAG_EXTRACTED},
                {"id": 6, "name": TAG_NEEDS_REVIEW},
            ]},
        )
        mock.get("/api/documents/").respond(200, json=docs_payload)
        mock.get("/api/documents/200/").respond(
            200, json={"id": 200, "tags": [], "custom_fields": []},
        )
        mock.patch("/api/documents/200/").mock(side_effect=_patch_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            async def _anomaly_run(self, doc, *, conn, client):
                return ActionResult(
                    status="anomaly",
                    summary="vendor below threshold",
                    details={"below_threshold": ["vendor"]},
                )
            monkeypatch.setattr(RunExtraction, "run", _anomaly_run)
            report = await run_rule(
                extract_missing_fields_rule, conn=db, paperless_client=client,
            )

    assert report.docs_matched == 1
    assert report.anomalies == 1
    assert report.successes == 0
    assert len(patches) == 1
    # NeedsReview tag id 6 must be added.
    assert patches[0]["tags"] == [6]


@pytest.mark.asyncio
async def test_audit_log_kind_matches_outcome(monkeypatch, db):
    """anomaly outcome → KIND_WORKFLOW_ANOMALY row in audit log;
    success outcome → KIND_WORKFLOW_ACTION row."""
    docs_payload = {
        "next": None,
        "results": [
            {"id": 300, "tags": [], "custom_fields": [],
             "created": "2026-04-01T00:00:00Z"},
        ],
    }
    _seed_doc_index(db, paperless_id=300)

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 5, "name": TAG_EXTRACTED},
                {"id": 6, "name": TAG_NEEDS_REVIEW},
            ]},
        )
        mock.get("/api/documents/").respond(200, json=docs_payload)
        mock.get("/api/documents/300/").respond(
            200, json={"id": 300, "tags": [], "custom_fields": []},
        )
        mock.patch("/api/documents/300/").respond(200, json={})

        async with PaperlessClient("https://paperless.test", "tok") as client:
            async def _anomaly_run(self, doc, *, conn, client):
                return ActionResult(status="anomaly", summary="low conf")
            monkeypatch.setattr(RunExtraction, "run", _anomaly_run)
            await run_rule(
                extract_missing_fields_rule, conn=db, paperless_client=client,
            )

    rows = db.execute(
        "SELECT kind, payload_json FROM paperless_writeback_log "
        "WHERE paperless_id = 300"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["kind"] == KIND_WORKFLOW_ANOMALY
    payload = json.loads(rows[0]["payload_json"])
    assert payload["rule"] == "extract_missing_fields"
    assert payload["status"] == "anomaly"
