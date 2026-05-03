# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0048: collections are plural. /note is the legacy singular path.
GET /note 301-redirects to /notes; POST /note is kept as a legacy
alias that delegates to the canonical handler so cached HTMX form
actions in old browsers don't break."""
from __future__ import annotations


def test_get_note_redirects_to_notes(app_client):
    r = app_client.get("/note", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "/notes"


def test_get_note_preserves_querystring(app_client):
    r = app_client.get("/note?from=test", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "/notes?from=test"


def test_get_notes_renders(app_client):
    r = app_client.get("/notes")
    assert r.status_code == 200
    # The template uses the new POST action.
    assert 'action="/notes"' in r.text


def test_post_notes_creates_note(app_client):
    r = app_client.post("/notes", data={"body": "hello from /notes"})
    assert r.status_code in {200, 204}


def test_post_note_legacy_alias_still_works(app_client):
    """POST /note delegates to the canonical handler — kept so
    cached form actions or old templates still function."""
    r = app_client.post("/note", data={"body": "hello from legacy /note"})
    assert r.status_code in {200, 204}


def test_sidebar_links_to_notes(app_client):
    r = app_client.get("/")
    assert r.status_code == 200
    assert 'href="/notes"' in r.text
