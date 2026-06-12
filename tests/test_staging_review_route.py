# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the /review/staged route — NEXTGEN.md Phase B2 minimal."""
from __future__ import annotations

from decimal import Decimal

from lamella.features.import_.staging import StagingService


def test_staged_review_page_renders_empty(app_client):
    r = app_client.get("/review/staged")
    assert r.status_code == 200
    assert "Staging review" in r.text
    assert "Nothing pending" in r.text


def test_staged_review_page_lists_pending_items(app_client):
    # We need to access the DB through the app's connection.
    # The simplest way in this fixture is to use StagingService via
    # the app state. Here we hit the HTTP surface — POST an intake
    # first so rows exist.
    app_client.post(
        "/intake/stage",
        data={
            "text": (
                "Date,Amount,Description\n"
                "2026-04-20,-12.34,AMAZON.COM\n"
            ),
            "has_header": "1",
        },
    )
    r = app_client.get("/review/staged")
    assert r.status_code == 200
    assert "AMAZON.COM" in r.text
    # Source badge.
    assert ">paste<" in r.text.lower() or "paste" in r.text.lower()


def test_filter_by_source(app_client):
    app_client.post(
        "/intake/stage",
        data={
            "text": "Date,Amount,Description\n2026-04-20,-1.00,X\n",
            "has_header": "1",
        },
    )
    r = app_client.get("/review/staged?source=simplefin")
    assert r.status_code == 200
    # filter=simplefin; no simplefin rows → empty
    assert "Nothing pending" in r.text


def test_staged_review_renders_group_header_for_siblings(app_client):
    """Workstream C2.1 — three rows with the same payee surface as
    one group with a 'N similar rows' header. A singleton does not."""
    # Three AMAZON rows + one WIDGETCO row.
    app_client.post(
        "/intake/stage",
        data={
            "text": (
                "Date,Amount,Description\n"
                "2026-04-20,-12.34,AMAZON.COM\n"
                "2026-04-21,-22.50,amazon.com\n"
                "2026-04-22,-7.10,Amazon.Com\n"
                "2026-04-23,-5.00,WIDGETCO\n"
            ),
            "has_header": "1",
        },
    )
    r = app_client.get("/review/staged")
    assert r.status_code == 200
    # The 3-AMAZON group header carries its size; the singleton
    # WIDGETCO group is rendered with size=1.
    import re
    sizes = [
        int(m.group(1))
        for m in re.finditer(r'class="num">(\d+)</span>\s*<span class="rsg-group-count-label">row', r.text)
    ]
    # Two groups expected: one with 3 AMAZON rows + one singleton
    # WIDGETCO row (size=1).
    assert sorted(sizes) == [1, 3]
    assert "WIDGETCO" in r.text


def test_dismiss_action_removes_from_pending(app_client):
    # Stage a row.
    app_client.post(
        "/intake/stage",
        data={
            "text": "Date,Amount,Description\n2026-04-20,-1.00,Dismissable\n",
            "has_header": "1",
        },
    )
    before = app_client.get("/review/staged")
    assert "Dismissable" in before.text
    # Get the staged_id from the DB side (via direct access is tricky
    # from the TestClient — we scan the v_staged_pending via a raw SQL
    # call through the response body). Simpler: hit the dismiss endpoint
    # for every visible staged_id by extracting hidden inputs.
    import re
    match = re.search(r'name="staged_id" value="(\d+)"', before.text)
    assert match is not None
    staged_id = match.group(1)

    r = app_client.post(
        "/review/staged/dismiss",
        data={"staged_id": staged_id, "reason": "test"},
        follow_redirects=False,
    )
    assert r.status_code == 303

    after = app_client.get("/review/staged")
    assert "Dismissable" not in after.text
