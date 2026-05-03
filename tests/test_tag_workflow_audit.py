# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0062 §7 — every workflow run writes a structured audit row to
paperless_writeback_log capturing input/output state for forensics."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.tag_workflow import (
    ActionResult,
    KIND_WORKFLOW_ACTION,
    KIND_WORKFLOW_ANOMALY,
    KIND_WORKFLOW_ERROR,
    RunDateSanityCheck,
    RunExtraction,
    TAG_EXTRACTED,
    TAG_NEEDS_REVIEW,
    extract_missing_fields_rule,
    run_rule,
)


def _seed_doc_index(conn, *, paperless_id: int, document_date: str | None = None):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, title, vendor, document_date, tags_json)
        VALUES (?, ?, ?, ?, '[]')
        """,
        (paperless_id, f"doc-{paperless_id}", "Vendor",
         document_date),
    )


@pytest.mark.asyncio
async def test_audit_row_payload_captures_before_after_tag_state(monkeypatch, db):
    """The audit row's payload must include the before tag set, the
    after tag set, the rule name, the action summary, and the list
    of tag ops applied."""
    docs_payload = {
        "next": None,
        "results": [
            {"id": 500, "tags": [11, 12], "custom_fields": [],
             "created": "2026-04-01T00:00:00Z"},
        ],
    }
    _seed_doc_index(db, paperless_id=500)

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
        mock.get("/api/documents/500/").respond(
            200, json={"id": 500, "tags": [11, 12], "custom_fields": []},
        )
        mock.patch("/api/documents/500/").respond(200, json={})

        async with PaperlessClient("https://paperless.test", "tok") as client:
            async def _success_run(self, doc, *, conn, client):
                return ActionResult(
                    status="success",
                    summary="all confidences >= 0.6",
                    details={"confidences": {"vendor": 0.9}},
                )
            monkeypatch.setattr(RunExtraction, "run", _success_run)
            await run_rule(
                extract_missing_fields_rule, conn=db, paperless_client=client,
            )

    rows = db.execute(
        "SELECT kind, payload_json FROM paperless_writeback_log "
        "WHERE paperless_id = 500"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["kind"] == KIND_WORKFLOW_ACTION
    payload = json.loads(rows[0]["payload_json"])
    # Required forensics fields per ADR-0062 §7:
    assert payload["rule"] == "extract_missing_fields"
    assert payload["status"] == "success"
    assert payload["action_summary"] == "all confidences >= 0.6"
    assert payload["before_tag_ids"] == [11, 12]
    assert 5 in payload["after_tag_ids"]   # Lamella:Extracted added
    assert 11 in payload["after_tag_ids"]  # original tags preserved
    assert 12 in payload["after_tag_ids"]
    # Tag ops applied: one add for Lamella:Extracted
    ops = payload["tag_ops_applied"]
    assert len(ops) == 1
    assert ops[0] == {"op": "add", "tag_name": TAG_EXTRACTED}
    # The action's details (confidences) are surfaced for forensics.
    assert payload["details"]["confidences"] == {"vendor": 0.9}


@pytest.mark.asyncio
async def test_audit_row_kind_for_error_outcome(monkeypatch, db):
    """When the action raises, the audit row's kind must be
    KIND_WORKFLOW_ERROR."""
    docs_payload = {
        "next": None,
        "results": [
            {"id": 600, "tags": [], "custom_fields": [],
             "created": "2026-04-01T00:00:00Z"},
        ],
    }
    _seed_doc_index(db, paperless_id=600)

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
        mock.get("/api/documents/600/").respond(
            200, json={"id": 600, "tags": [], "custom_fields": []},
        )
        mock.patch("/api/documents/600/").respond(200, json={})

        async with PaperlessClient("https://paperless.test", "tok") as client:
            async def _raise_run(self, doc, *, conn, client):
                raise RuntimeError("simulated extraction failure")
            monkeypatch.setattr(RunExtraction, "run", _raise_run)
            await run_rule(
                extract_missing_fields_rule, conn=db, paperless_client=client,
            )

    rows = db.execute(
        "SELECT kind, payload_json FROM paperless_writeback_log "
        "WHERE paperless_id = 600"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["kind"] == KIND_WORKFLOW_ERROR
    payload = json.loads(rows[0]["payload_json"])
    assert payload["status"] == "error"
    assert "simulated extraction failure" in payload["action_summary"]
