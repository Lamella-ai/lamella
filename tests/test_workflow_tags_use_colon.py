# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 — bootstrap_canonical_tags writes the colon-separated form.

The five canonical workflow tags are now ``Lamella:X``. The bootstrap
function must POST that exact name to /api/tags/ when a tag is missing,
not the legacy ``Lamella_X``."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.lamella_namespace import (
    ALL_WORKFLOW_TAGS,
    TAG_AWAITING_EXTRACTION,
    TAG_DATE_ANOMALY,
    TAG_EXTRACTED,
    TAG_LINKED,
    TAG_NEEDS_REVIEW,
)
from lamella.features.paperless_bridge.tag_workflow import (
    CANONICAL_TAGS,
    bootstrap_canonical_tags,
)


def test_canonical_tag_constants_use_colon():
    """All five workflow tag constants carry the colon separator."""
    assert TAG_AWAITING_EXTRACTION == "Lamella:AwaitingExtraction"
    assert TAG_EXTRACTED == "Lamella:Extracted"
    assert TAG_NEEDS_REVIEW == "Lamella:NeedsReview"
    assert TAG_DATE_ANOMALY == "Lamella:DateAnomaly"
    assert TAG_LINKED == "Lamella:Linked"


def test_canonical_tags_table_uses_colon():
    """The CANONICAL_TAGS list (used by bootstrap) is in canonical
    form — none of the entries should carry the legacy underscore."""
    for name, _color in CANONICAL_TAGS:
        assert name.startswith("Lamella:"), (
            f"CANONICAL_TAGS contains a non-canonical name: {name!r}. "
            "Per ADR-0064 the bootstrap MUST write the colon form."
        )
        assert "Lamella_" not in name


def test_all_workflow_tags_uses_colon():
    """The lamella_namespace ALL_WORKFLOW_TAGS tuple matches what
    the engine and bootstrap reference."""
    for name in ALL_WORKFLOW_TAGS:
        assert name.startswith("Lamella:"), name
        assert "Lamella_" not in name


@pytest.mark.asyncio
async def test_bootstrap_posts_colon_names_on_fresh_paperless():
    """A Paperless instance with no Lamella tags at all — bootstrap
    must POST exactly five tags, every name in the colon form."""
    posted: list[dict] = []

    def _capture(request):
        body = json.loads(request.read())
        posted.append(body)
        return httpx.Response(201, json={
            "id": 100 + len(posted), "name": body["name"],
        })

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        mock.post("/api/tags/").mock(side_effect=_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            ensured = await bootstrap_canonical_tags(client)

    posted_names = {p["name"] for p in posted}
    assert posted_names == {
        "Lamella:AwaitingExtraction",
        "Lamella:Extracted",
        "Lamella:NeedsReview",
        "Lamella:DateAnomaly",
        "Lamella:Linked",
    }
    # No legacy underscore names were POSTed.
    for name in posted_names:
        assert "Lamella_" not in name
    # Returned dict keys also use the canonical form.
    assert set(ensured.keys()) == posted_names


@pytest.mark.asyncio
async def test_bootstrap_does_not_create_canonical_when_legacy_present():
    """Backwards-compat: if a legacy ``Lamella_X`` tag exists,
    bootstrap should reuse it (via ensure_tag's fallback) and NOT
    POST a duplicate canonical tag."""
    posted: list[dict] = []

    def _capture(request):
        body = json.loads(request.read())
        posted.append(body)
        return httpx.Response(201, json={"id": 999, "name": body["name"]})

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        # Legacy tags only — no canonical equivalents yet.
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 11, "name": "Lamella_AwaitingExtraction"},
                {"id": 12, "name": "Lamella_Extracted"},
                {"id": 13, "name": "Lamella_NeedsReview"},
                {"id": 14, "name": "Lamella_DateAnomaly"},
                {"id": 15, "name": "Lamella_Linked"},
            ]},
        )
        mock.post("/api/tags/").mock(side_effect=_capture)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            ensured = await bootstrap_canonical_tags(client)

    # No POSTs — every canonical name resolved to the legacy id.
    assert posted == []
    # The returned dict keys are still canonical (callers see the
    # canonical view regardless of underlying id source).
    assert set(ensured.keys()) == set(ALL_WORKFLOW_TAGS)
    # Each id matches the legacy id (the migration will rename
    # them in place when it runs).
    assert ensured[TAG_AWAITING_EXTRACTION] == 11
    assert ensured[TAG_EXTRACTED] == 12
