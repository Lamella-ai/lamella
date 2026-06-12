# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0062 — date_sanity_check action flags impossibly-old + future
documents while passing in-bounds documents through."""
from __future__ import annotations

import json
from datetime import date, timedelta

import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.adapters.paperless.schemas import Document
from lamella.features.paperless_bridge.tag_workflow import (
    RunDateSanityCheck,
    date_sanity_check_rule,
    run_rule,
    TAG_DATE_ANOMALY,
    TAG_EXTRACTED,
    TAG_NEEDS_REVIEW,
)


def _seed_doc(conn, *, paperless_id: int, document_date: str | None):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, title, vendor, document_date, tags_json)
        VALUES (?, ?, ?, ?, '[]')
        """,
        (paperless_id, f"doc-{paperless_id}", "Vendor", document_date),
    )


@pytest.mark.asyncio
async def test_date_in_1995_is_flagged_as_anomaly(db):
    action = RunDateSanityCheck(min_year=2000, max_offset_days=0)
    _seed_doc(db, paperless_id=1, document_date="1995-06-15")
    doc = Document(id=1, tags=[])
    async with PaperlessClient("https://paperless.test", "tok") as client:
        result = await action.run(doc, conn=db, client=client)
    assert result.status == "anomaly"
    assert "before" in result.summary.lower() or "min_year" in result.summary.lower()
    assert result.details["reason"] == "before_min_year"


@pytest.mark.asyncio
async def test_date_in_2099_is_flagged_as_anomaly(db):
    action = RunDateSanityCheck(min_year=2000, max_offset_days=0)
    _seed_doc(db, paperless_id=2, document_date="2099-01-01")
    doc = Document(id=2, tags=[])
    async with PaperlessClient("https://paperless.test", "tok") as client:
        result = await action.run(doc, conn=db, client=client)
    assert result.status == "anomaly"
    assert result.details["reason"] == "future_date"


@pytest.mark.asyncio
async def test_date_today_passes_sanity(db):
    today = date.today().isoformat()
    action = RunDateSanityCheck(min_year=2000, max_offset_days=0)
    _seed_doc(db, paperless_id=3, document_date=today)
    doc = Document(id=3, tags=[])
    async with PaperlessClient("https://paperless.test", "tok") as client:
        result = await action.run(doc, conn=db, client=client)
    assert result.status == "success"


@pytest.mark.asyncio
async def test_no_date_returns_skipped(db):
    """Doc with no extractable date — nothing to sanity check."""
    action = RunDateSanityCheck(min_year=2000, max_offset_days=0)
    _seed_doc(db, paperless_id=4, document_date=None)
    doc = Document(id=4, tags=[])
    async with PaperlessClient("https://paperless.test", "tok") as client:
        result = await action.run(doc, conn=db, client=client)
    assert result.status == "skipped"


@pytest.mark.asyncio
async def test_rule_run_only_flags_out_of_bounds_docs(db):
    """End-to-end: feed three docs (1995, today, 2099) through the
    date_sanity_check rule and assert only the out-of-bounds two
    receive the DateAnomaly tag."""
    today = date.today().isoformat()
    _seed_doc(db, paperless_id=10, document_date="1995-06-15")
    _seed_doc(db, paperless_id=11, document_date=today)
    _seed_doc(db, paperless_id=12, document_date="2099-01-01")

    docs_payload = {
        "next": None,
        "results": [
            {"id": 10, "tags": [5], "custom_fields": []},
            {"id": 11, "tags": [5], "custom_fields": []},
            {"id": 12, "tags": [5], "custom_fields": []},
        ],
    }

    patched_ids: list[int] = []

    def _patch_capture(request):
        # URL like /api/documents/12/
        url_str = str(request.url)
        # Extract id from the URL
        seg = url_str.rstrip("/").split("/")[-1]
        try:
            patched_ids.append(int(seg))
        except ValueError:
            pass
        return _resp(200)

    import httpx as _httpx

    def _resp(code, body=None):
        return _httpx.Response(code, json=body or {})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 5, "name": TAG_EXTRACTED},
                {"id": 7, "name": TAG_DATE_ANOMALY},
                {"id": 6, "name": TAG_NEEDS_REVIEW},
            ]},
        )
        mock.get("/api/documents/").respond(200, json=docs_payload)
        for doc_id, tags in [(10, [5]), (11, [5]), (12, [5])]:
            mock.get(f"/api/documents/{doc_id}/").respond(
                200, json={"id": doc_id, "tags": tags, "custom_fields": []},
            )
            mock.patch(f"/api/documents/{doc_id}/").mock(side_effect=_patch_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_rule(
                date_sanity_check_rule, conn=db, paperless_client=client,
            )

    assert report.docs_matched == 3
    assert report.anomalies == 2
    assert report.successes == 1
    # Only 1995 and 2099 docs PATCHed (the today one had no tag op).
    assert sorted(patched_ids) == [10, 12]

    # Assert the audit-log rows reflect the per-doc anomaly statuses.
    rows = db.execute(
        "SELECT paperless_id, kind, payload_json FROM paperless_writeback_log "
        "ORDER BY paperless_id"
    ).fetchall()
    by_id = {r["paperless_id"]: (r["kind"], json.loads(r["payload_json"]))
             for r in rows}
    assert by_id[10][0] == "workflow_anomaly"
    assert by_id[12][0] == "workflow_anomaly"
    # The clean doc still gets an audit row, but with kind=workflow_action
    assert by_id[11][0] == "workflow_action"
