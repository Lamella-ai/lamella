# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 — namespace migration: copy + remove fallback path.

When Paperless's in-place rename PATCH returns a 4xx (some Paperless
versions reject custom-field renames; some reject tag renames when
the tag is referenced from documents), the migration falls back to:
  1. Create the canonical tag/field
  2. Copy doc taggings / values from legacy id to canonical id
  3. DELETE the legacy tag/field"""
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
async def test_tag_rename_4xx_falls_back_to_copy_and_delete():
    """The PATCH /api/tags/<id>/ returns 400. Migration must
    create the canonical tag, retag every document, then DELETE
    the legacy tag."""
    legacy_tag = {"id": 11, "name": "Lamella_Extracted"}

    posted_bodies: list[dict] = []
    deleted_ids: list[int] = []
    doc_patches: list[dict] = []

    def _post_capture(request):
        body = json.loads(request.read())
        posted_bodies.append(body)
        # Return id=99 for the new canonical tag.
        return httpx.Response(201, json={"id": 99, "name": body["name"]})

    def _doc_patch_capture(request):
        body = json.loads(request.read())
        doc_patches.append({"url": str(request.url), "body": body})
        return httpx.Response(200, json={})

    def _delete_capture(request):
        # /api/tags/11/
        seg = str(request.url).rstrip("/").split("/")[-1]
        try:
            deleted_ids.append(int(seg))
        except ValueError:
            pass
        return httpx.Response(204)

    list_call_count = {"n": 0}

    def _list_tags(request):
        list_call_count["n"] += 1
        # First call: legacy only. After POST creates canonical
        # the cache is invalidated, but we don't need to differentiate
        # because canonical_id is captured from the POST response.
        return httpx.Response(
            200, json={"next": None, "results": [legacy_tag]},
        )

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").mock(side_effect=_list_tags)
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": []},
        )
        # In-place rename PATCH returns 400 to trigger the fallback.
        mock.patch(f"/api/tags/{legacy_tag['id']}/").respond(
            400, json={"detail": "tag name immutable"},
        )
        # Fallback: create canonical via POST.
        mock.post("/api/tags/").mock(side_effect=_post_capture)
        # iter_documents finds two docs tagged with the legacy id.
        mock.get("/api/documents/").respond(
            200, json={
                "next": None,
                "results": [
                    {"id": 100, "tags": [11], "custom_fields": []},
                    {"id": 200, "tags": [11, 5], "custom_fields": []},
                ],
            },
        )
        for doc_id in (100, 200):
            mock.patch(f"/api/documents/{doc_id}/").mock(
                side_effect=_doc_patch_capture,
            )
        mock.delete(f"/api/tags/{legacy_tag['id']}/").mock(
            side_effect=_delete_capture,
        )

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.tags_renamed_in_place == 0
    assert report.tags_migrated_via_copy == 1
    assert report.documents_retagged == 2
    assert report.errors == []
    # Canonical tag was created.
    assert posted_bodies and posted_bodies[0]["name"] == "Lamella:Extracted"
    # Both docs got the new canonical id and lost the legacy id.
    by_url = {p["url"]: p["body"] for p in doc_patches}
    assert any("/api/documents/100/" in u for u in by_url)
    assert any("/api/documents/200/" in u for u in by_url)
    for body in by_url.values():
        assert 11 not in body["tags"], body
        assert 99 in body["tags"], body
    # Legacy tag deleted.
    assert deleted_ids == [11]


@pytest.mark.asyncio
async def test_tag_rename_when_canonical_already_exists():
    """If a Lamella:X tag already exists alongside Lamella_X, the
    migration skips the in-place rename (would 400 on duplicate
    name anyway) and goes straight to copy + delete."""
    tags = [
        {"id": 11, "name": "Lamella_Extracted"},
        # Canonical already there with a different id.
        {"id": 99, "name": "Lamella:Extracted"},
    ]
    doc_patches: list[dict] = []
    deleted_ids: list[int] = []
    in_place_patches: list = []

    def _doc_patch_capture(request):
        doc_patches.append(json.loads(request.read()))
        return httpx.Response(200, json={})

    def _delete_capture(request):
        seg = str(request.url).rstrip("/").split("/")[-1]
        deleted_ids.append(int(seg))
        return httpx.Response(204)

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": tags},
        )
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": []},
        )
        # The in-place PATCH should NOT be called when canonical
        # already exists. Set up a sentinel that records calls.
        in_place_route = mock.patch("/api/tags/11/").mock(
            side_effect=lambda r: (
                in_place_patches.append(r), httpx.Response(400),
            )[1],
        )
        mock.get("/api/documents/").respond(
            200, json={
                "next": None,
                "results": [
                    {"id": 100, "tags": [11], "custom_fields": []},
                ],
            },
        )
        mock.patch("/api/documents/100/").mock(side_effect=_doc_patch_capture)
        mock.delete("/api/tags/11/").mock(side_effect=_delete_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.tags_migrated_via_copy == 1
    assert report.tags_renamed_in_place == 0
    assert report.documents_retagged == 1
    assert report.errors == []
    # The doc was retagged to drop legacy id 11 and pick up canonical id 99.
    assert len(doc_patches) == 1
    assert 11 not in doc_patches[0]["tags"]
    assert 99 in doc_patches[0]["tags"]
    assert deleted_ids == [11]
    # The in-place rename was correctly skipped (no PATCH on /api/tags/11/).
    assert in_place_route.call_count == 0


@pytest.mark.asyncio
async def test_field_rename_4xx_falls_back_to_copy_and_delete():
    """Paperless rejects the custom-field rename. Migration creates
    a canonical field, copies values for every doc that carries the
    legacy field id, then deletes the legacy field."""
    legacy_field = {
        "id": 21, "name": "Lamella_Vendor", "data_type": "string",
    }
    posted_bodies: list[dict] = []
    deleted_ids: list[int] = []
    doc_patches: list[dict] = []
    field_list_call = {"n": 0}

    def _list_fields(request):
        field_list_call["n"] += 1
        # First two listings: legacy only (migration sees no canonical
        # exists, then create_custom_field re-fetches before POST and
        # also sees nothing). Subsequent listings (post-create
        # verification): legacy + freshly-created canonical id=99.
        if field_list_call["n"] <= 2:
            return httpx.Response(
                200, json={"next": None, "results": [legacy_field]},
            )
        return httpx.Response(
            200, json={"next": None, "results": [
                legacy_field,
                {"id": 99, "name": "Lamella:Vendor", "data_type": "string"},
            ]},
        )

    def _post_capture(request):
        body = json.loads(request.read())
        posted_bodies.append(body)
        return httpx.Response(201, json={
            "id": 99, "name": body["name"], "data_type": body["data_type"],
        })

    def _doc_patch_capture(request):
        doc_patches.append({"url": str(request.url), "body": json.loads(request.read())})
        return httpx.Response(200, json={})

    def _delete_capture(request):
        seg = str(request.url).rstrip("/").split("/")[-1]
        deleted_ids.append(int(seg))
        return httpx.Response(204)

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        mock.get("/api/custom_fields/").mock(side_effect=_list_fields)
        mock.patch("/api/custom_fields/21/").respond(
            400, json={"detail": "custom field name immutable"},
        )
        mock.post("/api/custom_fields/").mock(side_effect=_post_capture)
        # Doc 300 carries the legacy field with value "Acme Corp".
        mock.get("/api/documents/").respond(
            200, json={
                "next": None,
                "results": [{
                    "id": 300, "tags": [],
                    "custom_fields": [
                        {"field": 21, "value": "Acme Corp"},
                    ],
                }],
            },
        )
        mock.get("/api/documents/300/").respond(
            200, json={
                "id": 300, "tags": [],
                "custom_fields": [{"field": 21, "value": "Acme Corp"}],
            },
        )
        mock.patch("/api/documents/300/").mock(side_effect=_doc_patch_capture)
        mock.delete("/api/custom_fields/21/").mock(side_effect=_delete_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.fields_renamed_in_place == 0
    assert report.fields_migrated_via_copy == 1
    assert report.errors == []
    # Canonical field was created.
    assert posted_bodies and posted_bodies[0]["name"] == "Lamella:Vendor"
    # Doc was patched to write value under the canonical field id.
    assert len(doc_patches) == 1
    body = doc_patches[0]["body"]
    by_id = {cf["field"]: cf["value"] for cf in body["custom_fields"]}
    assert 99 in by_id and by_id[99] == "Acme Corp"
    assert 21 not in by_id
    # Legacy field deleted.
    assert deleted_ids == [21]
