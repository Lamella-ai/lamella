# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Verify /review returns the partial (not the full page) when HX-Request is set.
Catches the nested-layout bug where the shim's lack of hx-select support
plus a bogus hx-swap modifier caused the entire page body to land inside
#staged-list."""
from __future__ import annotations


def test_review_full_page_includes_shell(app_client):
    r = app_client.get("/review")
    assert r.status_code == 200
    # Full page rendering should include the app shell.
    assert "<aside" in r.text  # sidebar
    assert "topbar" in r.text  # topbar
    assert 'id="staged-list"' in r.text


def test_review_hx_request_returns_partial_only(app_client):
    r = app_client.get("/review", headers={"HX-Request": "true"})
    assert r.status_code == 200
    # Partial-only response: NO outer shell, just the swappable region.
    assert 'id="staged-list"' in r.text
    assert "<aside" not in r.text
    assert "topbar" not in r.text
    assert "<!doctype" not in r.text.lower()


def test_review_staged_redirects_to_review(app_client):
    r = app_client.get("/review/staged", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"].startswith("/review")
