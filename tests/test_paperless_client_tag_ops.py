# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0062 §6 — PaperlessClient.add_tag/remove_tag idempotency +
list_tags() TTL memoization."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient


@pytest.mark.asyncio
async def test_add_tag_idempotent_no_op_when_already_present():
    """add_tag on a doc that already carries the tag should not
    issue a PATCH (the tag list is unchanged)."""
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [5, 6], "custom_fields": []},
        )
        patch_route = mock.patch("/api/documents/42/").respond(200, json={})
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.add_tag(42, 5)
        assert patch_route.call_count == 0


@pytest.mark.asyncio
async def test_add_tag_appends_to_existing_tags():
    """add_tag must read-merge-write: never clobber existing tags."""
    captured: dict = {}

    def _capture(request):
        captured["body"] = json.loads(request.read())
        return httpx.Response(200, json={})

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [5, 6], "custom_fields": []},
        )
        mock.patch("/api/documents/42/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.add_tag(42, 99)
    assert captured["body"] == {"tags": [5, 6, 99]}


@pytest.mark.asyncio
async def test_remove_tag_idempotent_no_op_when_missing():
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [5], "custom_fields": []},
        )
        patch_route = mock.patch("/api/documents/42/").respond(200, json={})
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.remove_tag(42, 999)
        assert patch_route.call_count == 0


@pytest.mark.asyncio
async def test_remove_tag_drops_target_keeps_others():
    captured: dict = {}

    def _capture(request):
        captured["body"] = json.loads(request.read())
        return httpx.Response(200, json={})

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [5, 6, 7], "custom_fields": []},
        )
        mock.patch("/api/documents/42/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.remove_tag(42, 6)
    assert captured["body"] == {"tags": [5, 7]}


@pytest.mark.asyncio
async def test_list_tags_memoizes_within_ttl_window():
    """Repeated list_tags() calls inside the TTL window must hit
    the cache, not the API."""
    with respx.mock(base_url="https://paperless.test") as mock:
        list_route = mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 1, "name": "Foo"}, {"id": 2, "name": "Bar"},
            ]},
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            tags1 = await client.list_tags()
            tags2 = await client.list_tags()
            tags3 = await client.list_tags()
    assert tags1 == {"Foo": 1, "Bar": 2}
    assert tags2 == tags1
    assert tags3 == tags1
    assert list_route.call_count == 1


@pytest.mark.asyncio
async def test_list_tags_force_refresh_bypasses_cache():
    with respx.mock(base_url="https://paperless.test") as mock:
        list_route = mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [{"id": 1, "name": "Foo"}]},
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.list_tags()
            await client.list_tags(force_refresh=True)
    assert list_route.call_count == 2


@pytest.mark.asyncio
async def test_ensure_tag_invalidates_cache_so_new_tag_is_visible():
    """After ensure_tag creates a tag, the next list_tags() call
    must re-fetch (the new id should be visible immediately)."""
    list_call_count = {"n": 0}

    def _list_handler(request):
        list_call_count["n"] += 1
        if list_call_count["n"] == 1:
            return httpx.Response(
                200,
                json={"next": None, "results": []},
            )
        return httpx.Response(
            200,
            json={"next": None, "results": [
                {"id": 99, "name": "Lamella_Brand_New"},
            ]},
        )

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/tags/").mock(side_effect=_list_handler)
        mock.post("/api/tags/").respond(
            201, json={"id": 99, "name": "Lamella_Brand_New"},
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            # First call populates cache (empty list)
            initial = await client.list_tags()
            assert initial == {}
            # Create
            tid = await client.ensure_tag("Lamella_Brand_New")
            assert tid == 99
            # Second list_tags() must re-fetch — cache was invalidated
            after = await client.list_tags()
    assert after == {"Lamella_Brand_New": 99}
    assert list_call_count["n"] == 2
