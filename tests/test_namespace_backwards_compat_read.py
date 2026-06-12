# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 — backwards-compat read shim.

When a Paperless instance still has legacy ``Lamella_X`` tags or
custom fields (e.g. the migration hasn't run yet, ran but errored
out, or the user restored a backup), code that asks for the canonical
``Lamella:X`` form should still find the legacy id."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.lamella_namespace import (
    TAG_EXTRACTED,
)


@pytest.mark.asyncio
async def test_ensure_tag_finds_legacy_when_canonical_missing():
    """``ensure_tag('Lamella:Extracted')`` must NOT POST a duplicate
    when ``Lamella_Extracted`` exists. It returns the legacy id."""
    posts: list[dict] = []

    def _post_capture(request):
        posts.append(json.loads(request.read()))
        return httpx.Response(201, json={"id": 999, "name": "should_not_happen"})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 42, "name": "Lamella_Extracted"},  # legacy only
            ]},
        )
        mock.post("/api/tags/").mock(side_effect=_post_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            tag_id = await client.ensure_tag(TAG_EXTRACTED)

    # Returned the legacy id; no POST happened.
    assert tag_id == 42
    assert posts == []


@pytest.mark.asyncio
async def test_ensure_tag_prefers_canonical_when_both_exist():
    """When BOTH forms exist, the canonical form's id wins."""
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 42, "name": "Lamella_Extracted"},
                {"id": 99, "name": "Lamella:Extracted"},
            ]},
        )
        post_route = mock.post("/api/tags/").respond(
            201, json={"id": 999, "name": "should_not_happen"},
        )

        async with PaperlessClient("https://paperless.test", "tok") as client:
            tag_id = await client.ensure_tag(TAG_EXTRACTED)

    assert tag_id == 99  # canonical id
    assert post_route.call_count == 0


@pytest.mark.asyncio
async def test_ensure_lamella_writeback_fields_surfaces_legacy_names():
    """A Paperless instance with legacy ``Lamella_Entity`` and no
    canonical ``Lamella:Entity`` should expose the legacy id under
    the canonical key in the returned dict — callers see a unified
    canonical view regardless of the underlying name."""
    legacy_fields = [
        {"id": 21, "name": "Lamella_Entity", "data_type": "string"},
        {"id": 22, "name": "Lamella_Category", "data_type": "string"},
        {"id": 23, "name": "Lamella_TXN", "data_type": "string"},
        {"id": 24, "name": "Lamella_Account", "data_type": "string"},
    ]
    posts: list[dict] = []

    def _post_capture(request):
        posts.append(json.loads(request.read()))
        return httpx.Response(201, json={
            "id": 99, "name": "should_not_happen", "data_type": "string",
        })

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": legacy_fields},
        )
        mock.post("/api/custom_fields/").mock(side_effect=_post_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            field_ids = await client.ensure_lamella_writeback_fields()

    # Every canonical key should resolve to the legacy id; no POST
    # was made because all four canonical names were satisfied via
    # the backwards-compat shim.
    assert field_ids == {
        "Lamella:Entity": 21,
        "Lamella:Category": 22,
        "Lamella:TXN": 23,
        "Lamella:Account": 24,
    }
    assert posts == []


@pytest.mark.asyncio
async def test_writeback_under_legacy_field_name_input_writes_to_correct_id():
    """Caller passes legacy ``Lamella_Entity`` as the dict key but
    Paperless has the canonical ``Lamella:Entity`` field. The PATCH
    must use the canonical field id."""
    canonical_fields = [
        {"id": 99, "name": "Lamella:Entity", "data_type": "string"},
        {"id": 100, "name": "Lamella:Category", "data_type": "string"},
        {"id": 101, "name": "Lamella:TXN", "data_type": "string"},
        {"id": 102, "name": "Lamella:Account", "data_type": "string"},
    ]
    captured: dict = {}

    def _doc_patch_capture(request):
        captured["body"] = json.loads(request.read())
        return httpx.Response(200, json={})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": canonical_fields},
        )
        mock.get("/api/documents/300/").respond(
            200, json={"id": 300, "tags": [], "custom_fields": []},
        )
        mock.patch("/api/documents/300/").mock(side_effect=_doc_patch_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.writeback_lamella_fields(
                300, values={"Lamella_Entity": "AcmeCoLLC"},
            )

    assert "body" in captured
    body = captured["body"]
    by_id = {cf["field"]: cf["value"] for cf in body["custom_fields"]}
    assert by_id == {99: "AcmeCoLLC"}
