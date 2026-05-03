# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0062 — verify the five canonical Lamella: state tags are
ensured at workflow startup. Separator updated by ADR-0064."""
from __future__ import annotations

import pytest
import respx
import httpx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.tag_workflow import (
    CANONICAL_TAGS,
    TAG_AWAITING_EXTRACTION,
    TAG_DATE_ANOMALY,
    TAG_EXTRACTED,
    TAG_LINKED,
    TAG_NEEDS_REVIEW,
    bootstrap_canonical_tags,
)


def test_canonical_tag_names_present_and_namespaced():
    names = [name for name, _color in CANONICAL_TAGS]
    assert names == [
        TAG_AWAITING_EXTRACTION,
        TAG_EXTRACTED,
        TAG_NEEDS_REVIEW,
        TAG_DATE_ANOMALY,
        TAG_LINKED,
    ]
    # ADR-0044 / ADR-0064 namespace: Lamella: prefix on every state tag.
    for name in names:
        assert name.startswith("Lamella:"), name


@pytest.mark.asyncio
async def test_bootstrap_creates_all_missing_tags():
    """Fresh Paperless install — none of the five tags exist yet.
    bootstrap_canonical_tags should POST one create per missing tag."""
    created: list[dict] = []

    def _capture(request):
        body = request.read()
        import json as _json
        created.append(_json.loads(body))
        # Return a unique id per create.
        return httpx.Response(201, json={
            "id": 100 + len(created),
            "name": _json.loads(body)["name"],
        })

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        mock.post("/api/tags/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            ensured = await bootstrap_canonical_tags(client)
    assert len(created) == 5
    created_names = {row["name"] for row in created}
    assert created_names == {name for name, _ in CANONICAL_TAGS}
    # Returned dict has an id for each ensured tag.
    assert set(ensured.keys()) == {name for name, _ in CANONICAL_TAGS}
    for name in ensured:
        assert isinstance(ensured[name], int)


@pytest.mark.asyncio
async def test_bootstrap_is_idempotent_when_all_tags_exist():
    """All five tags already exist in Paperless — bootstrap should
    issue zero POSTs."""
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={
                "next": None,
                "results": [
                    {"id": i, "name": name}
                    for i, (name, _) in enumerate(CANONICAL_TAGS, start=10)
                ],
            },
        )
        post_route = mock.post("/api/tags/")
        async with PaperlessClient("https://paperless.test", "tok") as client:
            ensured = await bootstrap_canonical_tags(client)
    assert post_route.call_count == 0
    assert len(ensured) == 5


@pytest.mark.asyncio
async def test_bootstrap_passes_through_color_per_tag():
    """ensure_tag must be called with the per-tag color from the
    CANONICAL_TAGS table so tags are visually distinguishable in
    the Paperless UI."""
    captured: list[dict] = []

    def _capture(request):
        import json as _json
        body = _json.loads(request.read())
        captured.append(body)
        return httpx.Response(201, json={
            "id": 100 + len(captured),
            "name": body["name"],
            "color": body.get("color"),
        })

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/tags/").respond(200, json={"next": None, "results": []})
        mock.post("/api/tags/").mock(side_effect=_capture)
        async with PaperlessClient("https://paperless.test", "tok") as client:
            await bootstrap_canonical_tags(client)
    assert len(captured) == 5
    # Each call must carry the matching color from CANONICAL_TAGS.
    by_name = {row["name"]: row for row in captured}
    for name, color in CANONICAL_TAGS:
        assert by_name[name]["color"] == color
