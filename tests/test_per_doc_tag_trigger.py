# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the per-doc tag-trigger button (ADR-0065).

Covers:
  - POST with a valid active-binding tag_name applies tag via PaperlessClient
  - POST with an unknown tag_name returns inline error fragment
  - POST with a disabled binding tag_name returns inline error fragment
  - Success response contains "Run now" link and "run on next scheduler tick"
  - documents.html page renders Process column header
"""
from __future__ import annotations

import json
import unittest.mock as mock

import httpx
import pytest
import respx

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.binding_loader import BindingRow


# ---------------------------------------------------------------------------
# Helper: insert a binding into the test DB
# ---------------------------------------------------------------------------

def _insert_binding(conn, tag_name: str, action_name: str, enabled: bool = True):
    conn.execute(
        """
        INSERT OR REPLACE INTO tag_workflow_bindings
            (tag_name, action_name, enabled, config_json, created_at, updated_at)
        VALUES (?, ?, ?, '', '2026-05-02T00:00:00', '2026-05-02T00:00:00')
        """,
        (tag_name, action_name, 1 if enabled else 0),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# POST /documents/{id}/apply-tag — valid active binding
# ---------------------------------------------------------------------------

def test_apply_tag_valid_binding_calls_paperless(app_client, monkeypatch):
    """POSTing a tag that has an active binding should call
    PaperlessClient.ensure_tag + add_tag and return the success fragment."""
    # Insert a binding into the app's DB
    conn = app_client.app.state.db
    _insert_binding(conn, "Lamella:Process", "extract_fields", enabled=True)

    calls: dict = {"ensure": [], "add": []}

    async def _fake_ensure(self, name, **kwargs):
        calls["ensure"].append(name)
        return 77  # fake tag id

    async def _fake_add(self, doc_id, tag_id):
        calls["add"].append((doc_id, tag_id))

    # Patch PaperlessClient methods via monkeypatch on the class
    monkeypatch.setattr(PaperlessClient, "ensure_tag", _fake_ensure)
    monkeypatch.setattr(PaperlessClient, "add_tag", _fake_add)

    resp = app_client.post(
        "/documents/42/apply-tag",
        data={"tag_name": "Lamella:Process"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200, (
        f"Expected 200; got {resp.status_code}. Body: {resp.text[:300]}"
    )
    assert calls["ensure"] == ["Lamella:Process"], (
        f"ensure_tag should have been called with 'Lamella:Process'; got {calls['ensure']}"
    )
    assert calls["add"] == [(42, 77)], (
        f"add_tag should have been called with (42, 77); got {calls['add']}"
    )


def test_apply_tag_success_fragment_contains_run_now_link(app_client, monkeypatch):
    """The success fragment must contain a 'Run now' link pointing at the
    binding rule via the 'binding:<tag_name>' convention."""
    conn = app_client.app.state.db
    _insert_binding(conn, "Lamella:Process", "extract_fields", enabled=True)

    async def _fake_ensure2(self, name, **kwargs):
        return 99

    async def _fake_add2(self, doc_id, tag_id):
        return None

    monkeypatch.setattr(PaperlessClient, "ensure_tag", _fake_ensure2)
    monkeypatch.setattr(PaperlessClient, "add_tag", _fake_add2)

    resp = app_client.post(
        "/documents/42/apply-tag",
        data={"tag_name": "Lamella:Process"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    body = resp.text
    assert "scheduler tick" in body.lower() or "next" in body.lower(), (
        f"Success message should mention scheduler; got: {body[:200]}"
    )
    assert "Run now" in body, f"Must include 'Run now' link; got: {body[:200]}"
    assert "binding:Lamella:Process" in body, (
        f"Run now link must use 'binding:<tag_name>' convention; got: {body[:300]}"
    )


# ---------------------------------------------------------------------------
# POST /documents/{id}/apply-tag — unknown tag_name
# ---------------------------------------------------------------------------

def test_apply_tag_unknown_tag_returns_inline_error(app_client):
    """POSTing a tag_name that has no active binding should return a 400
    inline error fragment — not a 500 or redirect."""
    resp = app_client.post(
        "/documents/42/apply-tag",
        data={"tag_name": "Some:RandomTag"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 400, (
        f"Expected 400 inline error; got {resp.status_code}. Body: {resp.text[:200]}"
    )
    assert "Some:RandomTag" in resp.text or "No active binding" in resp.text, (
        f"Error should mention the invalid tag; got: {resp.text[:200]}"
    )


# ---------------------------------------------------------------------------
# POST /documents/{id}/apply-tag — disabled binding rejected
# ---------------------------------------------------------------------------

def test_apply_tag_disabled_binding_returns_inline_error(app_client):
    """A disabled binding (enabled=0) must be rejected — only active
    bindings are valid trigger targets."""
    conn = app_client.app.state.db
    _insert_binding(conn, "Lamella:DateCheck", "date_sanity_check", enabled=False)

    resp = app_client.post(
        "/documents/42/apply-tag",
        data={"tag_name": "Lamella:DateCheck"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 400, (
        f"Disabled binding should be rejected with 400; got {resp.status_code}. "
        f"Body: {resp.text[:200]}"
    )


# ---------------------------------------------------------------------------
# documents.html Process column header renders
# ---------------------------------------------------------------------------

def test_documents_page_has_process_column_header(app_client):
    """The Process column header must appear on the /documents page table
    after the ADR-0065 changes. Requires rows to be present (table only
    renders when rows is non-empty)."""
    conn = app_client.app.state.db
    conn.execute(
        """
        INSERT OR REPLACE INTO paperless_doc_index
            (paperless_id, title, document_date, created_date, total_amount,
             subtotal_amount, payment_last_four, document_type_id,
             document_type_name, vendor, correspondent_name, content_excerpt)
        VALUES
            (201, 'Process Test Doc', date('now'), date('now'), '5.00',
             NULL, NULL, NULL, NULL, 'Acme Co.', 'Acme Co.', '')
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO paperless_sync_state (id, doc_count) VALUES (1, 1)"
    )
    conn.commit()

    resp = app_client.get("/documents?link_status=all&lookback_days=9999")
    assert resp.status_code == 200
    assert "<th>Process</th>" in resp.text, (
        "documents.html table should have a 'Process' column header"
    )


# ---------------------------------------------------------------------------
# documents.html empty bindings — "No bindings" placeholder shown in rows
# ---------------------------------------------------------------------------

def test_documents_page_no_bindings_placeholder(app_client, monkeypatch):
    """When no bindings exist, the row partial should show a 'No bindings'
    placeholder with a link to /settings/paperless-workflows."""
    # Ensure the DB has no rows (fresh install)
    conn = app_client.app.state.db
    conn.execute("DELETE FROM tag_workflow_bindings")
    conn.commit()

    # Seed a document row so the table renders (otherwise `rows` is empty
    # and the table is not rendered at all)
    conn.execute(
        """
        INSERT OR REPLACE INTO paperless_doc_index
            (paperless_id, title, document_date, created_date, total_amount,
             subtotal_amount, payment_last_four, document_type_id,
             document_type_name, vendor, correspondent_name, content_excerpt)
        VALUES
            (99, 'Test Receipt', date('now'), date('now'), '12.34',
             NULL, NULL, NULL, NULL, 'Acme Co.', 'Acme Co.', '')
        """
    )
    conn.commit()

    # Patch paperless_sync_state so empty_index=False (otherwise the table
    # doesn't render because the index appears empty)
    conn.execute(
        "INSERT OR REPLACE INTO paperless_sync_state (id, doc_count) VALUES (1, 1)"
    )
    conn.commit()

    resp = app_client.get("/documents?link_status=all&lookback_days=9999")
    assert resp.status_code == 200
    body = resp.text
    # The row partial should render the "No bindings" placeholder
    assert "No bindings" in body or "Configure" in body, (
        f"Row should show 'No bindings' placeholder when no bindings exist; "
        f"body snippet: {body[-500:]}"
    )


