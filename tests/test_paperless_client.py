# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient, PaperlessError


@pytest.mark.asyncio
async def test_get_document_parses_payload():
    async with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/documents/42/").respond(
            200,
            json={
                "id": 42,
                "title": "Hardware Store Receipt",
                "created": "2026-04-10T10:00:00Z",
                "tags": [1, 2],
                "custom_fields": [
                    {"field": 7, "value": "42.17"},
                    {"field": 8, "value": "Hardware Store"},
                ],
            },
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            doc = await client.get_document(42)
    assert doc.id == 42
    assert doc.title == "Hardware Store Receipt"
    assert len(doc.custom_fields) == 2


@pytest.mark.asyncio
async def test_get_custom_fields_resolves_names():
    async with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/documents/42/").respond(
            200,
            json={
                "id": 42,
                "custom_fields": [
                    {"field": 7, "value": "42.17"},
                    {"field": 8, "value": "Hardware Store"},
                    {"field": 9, "value": "1234"},
                    {"field": 10, "value": "2026-04-10"},
                ],
            },
        )
        mock.get("/api/custom_fields/").respond(
            200,
            json={
                "next": None,
                "results": [
                    {"id": 7, "name": "receipt_total"},
                    {"id": 8, "name": "vendor"},
                    {"id": 9, "name": "payment_last_four"},
                    {"id": 10, "name": "receipt_date"},
                ],
            },
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            doc = await client.get_document(42)
            fields = await client.get_custom_fields(doc)
    assert fields["receipt_total"] == "42.17"
    assert fields["vendor"] == "Hardware Store"
    assert fields["payment_last_four"] == "1234"
    assert fields["receipt_date"] == "2026-04-10"


@pytest.mark.asyncio
async def test_retries_once_on_5xx_then_succeeds():
    async with respx.mock(base_url="https://paperless.test") as mock:
        route = mock.get("/api/documents/42/")
        route.side_effect = [
            httpx.Response(500, text="boom"),
            httpx.Response(200, json={"id": 42, "custom_fields": []}),
        ]
        async with PaperlessClient("https://paperless.test", "tok") as client:
            doc = await client.get_document(42)
        assert doc.id == 42
        assert route.call_count == 2


@pytest.mark.asyncio
async def test_raises_paperless_error_on_4xx():
    async with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/documents/42/").respond(401, text="forbidden")
        async with PaperlessClient("https://paperless.test", "tok") as client:
            with pytest.raises(PaperlessError):
                await client.get_document(42)


@pytest.mark.asyncio
async def test_patch_document_sends_only_supplied_fields():
    """PATCH body must include only the fields the caller passed —
    never clobber unrelated Paperless state with None/empty values."""
    captured: dict = {}
    async with respx.mock(base_url="https://paperless.test") as mock:
        def _capture(request):
            captured["body"] = request.read()
            return httpx.Response(200, json={"id": 42, "title": "new"})
        mock.patch("/api/documents/42/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.patch_document(42, title="new", created="2026-04-18")
    import json
    body = json.loads(captured["body"])
    assert body == {"title": "new", "created": "2026-04-18"}
    # Absent: correspondent, custom_fields, tags — the three fields
    # we explicitly didn't pass.
    assert "tags" not in body
    assert "correspondent" not in body
    assert "custom_fields" not in body


@pytest.mark.asyncio
async def test_ensure_tag_returns_existing_id_without_creating():
    async with respx.mock(base_url="https://paperless.test", assert_all_called=False) as mock:
        mock.get("/api/tags/").respond(
            200, json={
                "next": None,
                "results": [
                    {"id": 5, "name": "Lamella Fixed"},
                    {"id": 6, "name": "Receipts"},
                ],
            },
        )
        create_route = mock.post("/api/tags/")
        async with PaperlessClient("https://paperless.test", "tok") as client:
            tag_id = await client.ensure_tag("Lamella Fixed")
    assert tag_id == 5
    assert create_route.call_count == 0


@pytest.mark.asyncio
async def test_ensure_tag_creates_when_missing():
    async with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [{"id": 6, "name": "Receipts"}]},
        )
        mock.post("/api/tags/").respond(
            201, json={"id": 99, "name": "Lamella Fixed"},
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            tag_id = await client.ensure_tag("Lamella Fixed")
    assert tag_id == 99


@pytest.mark.asyncio
async def test_create_custom_field_posts_and_verifies():
    """POSTs /api/custom_fields/ with name + data_type. After the POST,
    re-fetches the field list and VERIFIES the new field is present —
    guards against Paperless setups where POST silently succeeds
    without persisting (permission middleware, proxy rewrites)."""
    captured: dict = {}
    # Two sequential list responses: first call returns the pre-create
    # state; second call (verification after POST) returns the
    # post-create state with the new field included.
    with respx.mock(base_url="https://paperless.test") as mock:
        list_route = mock.get("/api/custom_fields/")
        list_route.side_effect = [
            httpx.Response(200, json={"next": None, "results": [
                {"id": 7, "name": "Amount", "data_type": "monetary"},
            ]}),
            httpx.Response(200, json={"next": None, "results": [
                {"id": 7, "name": "Amount", "data_type": "monetary"},
                {"id": 42, "name": "Receipt Total", "data_type": "monetary"},
            ]}),
        ]

        def _capture(request):
            captured["body"] = request.read()
            return httpx.Response(
                201,
                json={"id": 42, "name": "Receipt Total", "data_type": "monetary"},
            )
        create_route = mock.post("/api/custom_fields/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            field = await client.create_custom_field(
                name="Receipt Total", data_type="monetary",
            )
    assert field.id == 42
    assert field.name == "Receipt Total"
    import json
    body = json.loads(captured["body"])
    assert body == {"name": "Receipt Total", "data_type": "monetary"}
    assert create_route.call_count == 1
    assert list_route.call_count == 2


@pytest.mark.asyncio
async def test_create_custom_field_raises_when_post_silently_dropped():
    """If Paperless returns a success body but the field isn't in
    the list after a fresh re-fetch, raise PaperlessError instead of
    lying to the caller. This is the bug the user hit: 'Paperless
    accepted my create but nothing shows up in the UI.'"""
    with respx.mock(base_url="https://paperless.test") as mock:
        # Both reads return the SAME pre-create list — the POST's
        # claimed id is never there on verification.
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": [
                {"id": 7, "name": "Amount", "data_type": "monetary"},
            ]},
        )
        mock.post("/api/custom_fields/").respond(
            201, json={"id": 42, "name": "Receipt Total", "data_type": "monetary"},
        )
        async with PaperlessClient("https://paperless.test", "tok") as client:
            with pytest.raises(PaperlessError) as exc_info:
                await client.create_custom_field(
                    name="Receipt Total", data_type="monetary",
                )
    assert "silently dropping" in str(exc_info.value).lower() \
        or "NOT in the custom-fields list" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_custom_field_reuses_existing_by_name():
    """If a field with the chosen name already exists in Paperless, the
    method returns it unchanged rather than POSTing a duplicate."""
    async with respx.mock(base_url="https://paperless.test", assert_all_called=False) as mock:
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": [
                {"id": 7, "name": "Receipt Total", "data_type": "monetary"},
            ]},
        )
        create_route = mock.post("/api/custom_fields/")
        async with PaperlessClient("https://paperless.test", "tok") as client:
            field = await client.create_custom_field(
                name="Receipt Total", data_type="monetary",
            )
    assert field.id == 7
    assert create_route.call_count == 0


@pytest.mark.asyncio
async def test_create_custom_field_rejects_unknown_data_type():
    async with PaperlessClient("https://paperless.test", "tok") as client:
        with pytest.raises(PaperlessError):
            await client.create_custom_field(name="X", data_type="bogus")


@pytest.mark.asyncio
async def test_add_note_posts_body():
    captured: dict = {}
    async with respx.mock(base_url="https://paperless.test") as mock:
        def _capture(request):
            captured["body"] = request.read()
            return httpx.Response(200, json={"id": 1, "note": "x"})
        mock.post("/api/documents/42/notes/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await client.add_note(42, "Gas for 2009 Work SUV")
    import json
    body = json.loads(captured["body"])
    assert body == {"note": "Gas for 2009 Work SUV"}
