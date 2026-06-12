# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0064 — namespace migration is fully idempotent.

Once all legacy ``Lamella_X`` tags + fields are gone, a second run
must perform zero writes (no PATCHes, no POSTs, no DELETEs)."""
from __future__ import annotations

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.namespace_migration import (
    run_namespace_migration,
)


@pytest.mark.asyncio
async def test_second_run_after_clean_state_is_zero_writes():
    """Run #1 renames everything. Run #2 must be a complete no-op."""
    # Simulate the post-migration state: only canonical names exist.
    canonical_tags = [
        {"id": 11, "name": "Lamella:AwaitingExtraction"},
        {"id": 12, "name": "Lamella:Extracted"},
        {"id": 13, "name": "Lamella:NeedsReview"},
        {"id": 14, "name": "Lamella:DateAnomaly"},
        {"id": 15, "name": "Lamella:Linked"},
    ]
    canonical_fields = [
        {"id": 21, "name": "Lamella:Entity", "data_type": "string"},
        {"id": 22, "name": "Lamella:Category", "data_type": "string"},
        {"id": 23, "name": "Lamella:TXN", "data_type": "string"},
        {"id": 24, "name": "Lamella:Account", "data_type": "string"},
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
        # Any write would be a bug — these routes catch any straggler.
        all_patches = mock.patch(host__regex=r".*").respond(200, json={})
        all_posts = mock.post(host__regex=r".*").respond(201, json={})
        all_deletes = mock.delete(host__regex=r".*").respond(204)
        # Documents endpoint should never be touched on a no-op pass.
        all_docs = mock.get("/api/documents/").respond(
            200, json={"next": None, "results": []},
        )

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.total_writes() == 0, report
    assert report.errors == []
    assert all_patches.call_count == 0
    assert all_posts.call_count == 0
    assert all_deletes.call_count == 0
    # The migration only paginates documents on the copy/fallback
    # path; with no legacy names there's nothing to paginate.
    assert all_docs.call_count == 0


@pytest.mark.asyncio
async def test_third_run_after_two_runs_still_zero_writes():
    """Belt-and-suspenders: run the migration three times against
    a clean Paperless. Every run must be a zero-write no-op (proves
    the function is genuinely state-free, not just one-shot guarded)."""
    canonical_tags = [
        {"id": 11, "name": "Lamella:Extracted"},
    ]
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": canonical_tags},
        )
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": []},
        )
        all_patches = mock.patch(host__regex=r".*").respond(200, json={})
        all_posts = mock.post(host__regex=r".*").respond(201, json={})
        all_deletes = mock.delete(host__regex=r".*").respond(204)

        async with PaperlessClient("https://paperless.test", "tok") as client:
            for _ in range(3):
                report = await run_namespace_migration(client)
                assert report.total_writes() == 0
                assert report.errors == []

    assert all_patches.call_count == 0
    assert all_posts.call_count == 0
    assert all_deletes.call_count == 0


@pytest.mark.asyncio
async def test_partial_state_with_unrelated_lamella_lookalike_is_ignored():
    """A user-created tag that happens to start with 'Lamella' but
    NOT with the underscore prefix (e.g. 'LamellaCustom') is left
    alone — the migration only touches the explicit Lamella_ prefix
    family."""
    tags = [
        {"id": 11, "name": "LamellaCustom"},  # not Lamella_ prefixed
        {"id": 12, "name": "Lamella:Extracted"},  # already canonical
    ]
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/tags/").respond(
            200, json={"next": None, "results": tags},
        )
        mock.get("/api/custom_fields/").respond(
            200, json={"next": None, "results": []},
        )
        all_writes = mock.patch(host__regex=r".*").respond(200, json={})

        async with PaperlessClient("https://paperless.test", "tok") as client:
            report = await run_namespace_migration(client)

    assert report.total_writes() == 0
    assert all_writes.call_count == 0
