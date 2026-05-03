# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 — namespace migration: in-place PATCH path.

When Paperless accepts the in-place tag/field rename PATCH, the
migration uses one PATCH per rename — O(num_legacy_names), not
O(num_documents). The fallback copy + remove path runs only when
Paperless refuses the rename."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.namespace_migration import (
    run_namespace_migration,
)


@pytest.mark.asyncio
async def test_in_place_tag_rename_uses_patch_not_copy():
    """Five legacy Lamella_X tags exist; the rename PATCH succeeds.
    Expect 5 PATCHes to /api/tags/<id>/, zero document retags, zero
    DELETEs."""
    legacy_tags = [
        {"id": 11, "name": "Lamella_AwaitingExtraction"},
        {"id": 12, "name": "Lamella_Extracted"},
        {"id": 13, "name": "Lamella_NeedsReview"},
        {"id": 14, "name": "Lamella_DateAnomaly"},
        {"id": 15, "name": "Lamella_Linked"},
    ]
    patches: list[dict] = []

    def _patch_capture(request):
        patches.append({
            "url": str(request.url),
            "body": json.loads(request.read()),
        })
        # Echo body back as a 200 — Paperless's tag PATCH typically
        # returns the updated row.
        body = json.loads(request.read() if False else b"{}")  # already read
        return httpx.Response(200, json={})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        # list_tags returns the five legacy tags only.
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": legacy_tags},
        )
        # No custom fields (covered separately).
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": []},
        )
        # Per-tag rename PATCHes.
        for tag in legacy_tags:
            mock.patch(f"/api/tags/{tag['id']}/").mock(
                side_effect=_patch_capture,
            )
        # If the migration tried iter_documents we'd see this hit;
        # we mock empty so any accidental call returns no docs.
        docs_route = mock.get("/api/documents/").respond(
            200, json={"next": None, "results": []},
        )
        # If the migration tried DELETE we'd see this hit; assert
        # below that count is zero.
        del_route_calls: list[int] = []
        for tag in legacy_tags:
            mock.delete(f"/api/tags/{tag['id']}/").mock(
                side_effect=lambda req, _id=tag["id"]: (
                    del_route_calls.append(_id),
                    httpx.Response(204),
                )[1],
            )

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    # Five renames in place; no errors.
    assert report.tags_renamed_in_place == 5, report
    assert report.tags_migrated_via_copy == 0
    assert report.documents_retagged == 0
    assert report.errors == []
    # No DELETEs were issued (in-place rename keeps the same id).
    assert del_route_calls == []
    # No document iteration was needed.
    assert docs_route.call_count == 0
    # All 5 patches captured.
    assert len(patches) == 5
    # Each PATCH renamed to the canonical Lamella:X form.
    new_names = {p["body"]["name"] for p in patches}
    assert new_names == {
        "Lamella:AwaitingExtraction",
        "Lamella:Extracted",
        "Lamella:NeedsReview",
        "Lamella:DateAnomaly",
        "Lamella:Linked",
    }


@pytest.mark.asyncio
async def test_in_place_field_rename_uses_patch_not_copy():
    """Four legacy Lamella_X custom fields exist; rename PATCH
    succeeds. Expect 4 field PATCHes, zero copies, zero document
    re-write."""
    legacy_fields = [
        {"id": 21, "name": "Lamella_Entity", "data_type": "string"},
        {"id": 22, "name": "Lamella_Category", "data_type": "string"},
        {"id": 23, "name": "Lamella_TXN", "data_type": "string"},
        {"id": 24, "name": "Lamella_Account", "data_type": "string"},
    ]
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
        # No tags (covered separately).
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": legacy_fields},
        )
        for f in legacy_fields:
            mock.patch(f"/api/custom_fields/{f['id']}/").mock(
                side_effect=_patch_capture,
            )

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.fields_renamed_in_place == 4, report
    assert report.fields_migrated_via_copy == 0
    assert report.errors == []
    assert len(patches) == 4
    new_names = {p["body"]["name"] for p in patches}
    assert new_names == {
        "Lamella:Entity",
        "Lamella:Category",
        "Lamella:TXN",
        "Lamella:Account",
    }


@pytest.mark.asyncio
async def test_no_legacy_names_is_complete_no_op():
    """When Paperless already has only canonical names, the
    migration is a no-op — zero PATCHes, zero POSTs, zero DELETEs."""
    canonical_tags = [
        {"id": 1, "name": "Lamella:AwaitingExtraction"},
        {"id": 2, "name": "Lamella:Extracted"},
    ]
    canonical_fields = [
        {"id": 1, "name": "Lamella:Entity", "data_type": "string"},
    ]
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": canonical_tags},
        )
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": canonical_fields},
        )
        # If any PATCH/POST/DELETE fires we'll detect it via call_count.
        patch_route = mock.patch(host__regex=r".*").respond(200, json={})
        post_route = mock.post(host__regex=r".*").respond(201, json={})
        delete_route = mock.delete(host__regex=r".*").respond(204)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.total_writes() == 0
    assert report.errors == []
    assert patch_route.call_count == 0
    assert post_route.call_count == 0
    assert delete_route.call_count == 0
