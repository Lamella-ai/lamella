# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke tests for the unified /inbox landing page.

After the v0.3.0 rename (commit 7ca99e1, ADR-0048), /inbox is the
staged-transactions queue (formerly /review). The count-tiles
dashboard moved to /. /review redirects to /inbox for bookmark
stability.
"""
from __future__ import annotations


def test_inbox_returns_200(app_client):
    """The /inbox route resolves cleanly. Empty-state path renders
    review.html via the legacy fallback (per inbox.py); populated
    path renders staging_review.html. Either way: 200."""
    r = app_client.get("/inbox")
    assert r.status_code == 200


def test_inbox_shows_pasted_rows_when_staged(app_client):
    """A pasted intake stages rows; /inbox renders them."""
    app_client.post(
        "/intake/stage",
        data={
            "text": (
                "Date,Amount,Description\n"
                "2026-04-20,-12.34,ACME.COM\n"
                "2026-04-21,-22.50,acme.com\n"
                "2026-04-23,-5.00,WIDGETCO\n"
            ),
            "has_header": "1",
        },
    )
    r = app_client.get("/inbox")
    assert r.status_code == 200
    # Both vendors appear somewhere in the rendered queue.
    assert "acme" in r.text.lower()
    assert "widgetco" in r.text.lower()


def test_review_url_redirects_to_inbox(app_client):
    """The legacy /review URL redirects to /inbox (301) per ADR-0048."""
    r = app_client.get("/review", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers.get("location", "").startswith("/inbox")


def test_inbox_title_is_inbox_not_staging_review(app_client):
    """ADR-0048 + the v0.3.0 inbox-rename: the page must NOT carry
    the legacy 'Staging review' title."""
    r = app_client.get("/inbox")
    assert r.status_code == 200
    assert "Inbox" in r.text
    # The historic "Staging review" title was removed in commit 7ca99e1.
    # Allow it inside source-comments / non-visible text by checking
    # only the <title> tag region.
    title_block = r.text.split("</title>", 1)[0]
    assert "Staging review" not in title_block
