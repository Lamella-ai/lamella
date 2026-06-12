# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0062 — POST /documents/workflows/{rule}/run on-demand trigger
returns the RunReport; unknown rule names return 404."""
from __future__ import annotations

import json

import respx


def _stub_paperless_empty(mock):
    """Stub a Paperless that returns no documents matching anything,
    so the rule run completes quickly with docs_matched=0."""
    mock.get("/api/tags/").respond(
        200, json={"next": None, "results": []},
    )
    mock.get("/api/documents/").respond(
        200, json={"next": None, "results": []},
    )


def test_post_run_returns_report_for_known_rule(app_client):
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        _stub_paperless_empty(mock)
        resp = app_client.post(
            "/documents/workflows/extract_missing_fields/run",
        )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["rule_name"] == "extract_missing_fields"
    assert payload["docs_matched"] == 0
    assert payload["docs_processed"] == 0
    assert "started_at" in payload
    assert "finished_at" in payload


def test_post_run_unknown_rule_returns_404(app_client):
    resp = app_client.post(
        "/documents/workflows/no_such_rule_anywhere/run",
    )
    assert resp.status_code == 404
    body = resp.json()
    assert "no_such_rule_anywhere" in body["detail"]


def test_get_workflows_lists_default_rules(app_client):
    """GET /documents/workflows returns the registered rules."""
    resp = app_client.get("/documents/workflows")
    assert resp.status_code == 200
    payload = resp.json()
    rule_names = [r["name"] for r in payload["rules"]]
    assert "extract_missing_fields" in rule_names
    assert "date_sanity_check" in rule_names
    assert "auto_link" in rule_names


def test_post_run_with_htmx_returns_html_partial(app_client):
    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        _stub_paperless_empty(mock)
        resp = app_client.post(
            "/documents/workflows/date_sanity_check/run",
            headers={"hx-request": "true"},
        )
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "date_sanity_check" in resp.text
