# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the bulk Ask AI silent-failure fix (ADR-0065).

Covers:
  - Empty selection POST returns 200 + inline error fragment (NOT 303)
  - Non-HTMX empty selection still 303s (browser fallback preserved)
  - HTMX 503 condition (Paperless/AI not configured) returns inline error
  - Success path returns job modal partial (not a 303)
  - The form in documents.html targets #job-modal-slot, not body
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Empty selection — HTMX path must return inline error, not 303
# ---------------------------------------------------------------------------

def test_empty_selection_htmx_returns_inline_error(app_client):
    """POST /documents/verify-selected with no doc_ids and HX-Request header
    must return 400 + an inline error fragment, NOT 303.

    Before the fix: the route returned 303 -> HTMX followed the redirect,
    fetched the full documents page, and silently appended it below the
    fold -- invisible to the user.
    """
    resp = app_client.post(
        "/documents/verify-selected",
        data={},  # no doc_ids
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 400, (
        f"Expected 400 inline error fragment; got {resp.status_code}.\n"
        f"Body: {resp.text[:300]}"
    )
    body = resp.text
    assert "No documents selected" in body, (
        f"Error fragment should mention 'No documents selected'; got: {body[:300]}"
    )
    # Must NOT be a redirect
    assert "Location" not in resp.headers, (
        "HTMX path must not redirect on empty selection"
    )


def test_empty_selection_non_htmx_still_redirects(app_client):
    """Vanilla form submit (no HX-Request header) should still 303 for
    browser fallback. The fix only changes the HTMX path."""
    resp = app_client.post(
        "/documents/verify-selected",
        data={},
        follow_redirects=False,
    )
    assert resp.status_code == 303, (
        f"Non-HTMX empty selection should 303; got {resp.status_code}"
    )
    assert "/documents" in resp.headers.get("location", ""), (
        f"Redirect target should be /documents; got {resp.headers.get('location')}"
    )


# ---------------------------------------------------------------------------
# Error slot id in verify-error-slot
# ---------------------------------------------------------------------------

def test_inline_error_fragment_targets_verify_error_slot(app_client):
    """The error fragment should carry id='verify-error-slot' so HTMX
    can swap it into the dedicated slot rather than body-beforeend."""
    resp = app_client.post(
        "/documents/verify-selected",
        data={},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 400
    assert 'id="verify-error-slot"' in resp.text, (
        "Error fragment must carry id='verify-error-slot' for the slot swap"
    )


# ---------------------------------------------------------------------------
# documents.html targets #job-modal-slot, not body
# ---------------------------------------------------------------------------

def test_documents_page_form_targets_job_modal_slot(app_client):
    """The bulk verify form in documents.html must target #job-modal-slot,
    not body (body-beforeend is unreliable with the HTMX shim).

    Requires rows to render so we seed a doc and use link_status=all.
    """
    # Seed a document so the table renders (the form is inside {% if rows %})
    conn = app_client.app.state.db
    conn.execute(
        """
        INSERT OR REPLACE INTO paperless_doc_index
            (paperless_id, title, document_date, created_date, total_amount,
             subtotal_amount, payment_last_four, document_type_id,
             document_type_name, vendor, correspondent_name, content_excerpt)
        VALUES
            (101, 'Test Doc', date('now'), date('now'), '9.99',
             NULL, NULL, NULL, NULL, 'Acme Co.', 'Acme Co.', '')
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO paperless_sync_state (id, doc_count) VALUES (1, 1)"
    )
    conn.commit()

    resp = app_client.get("/documents?link_status=all&lookback_days=9999")
    assert resp.status_code == 200
    body = resp.text
    assert 'hx-target="#job-modal-slot"' in body, (
        "bulk verify form must use hx-target='#job-modal-slot'; "
        "found: NO hx-target or wrong target. "
        "Check that documents.html form attr was updated."
    )
    assert 'hx-target="body"' not in body, (
        "bulk verify form must NOT use hx-target='body' (causes silent failure)"
    )


def test_documents_page_has_job_modal_slot(app_client):
    """The page must include the dedicated job-modal-slot div."""
    resp = app_client.get("/documents")
    assert resp.status_code == 200
    assert 'id="job-modal-slot"' in resp.text, (
        "documents.html must include <div id='job-modal-slot'>"
    )


# ---------------------------------------------------------------------------
# Paperless/AI not configured -- inline error, not 503 HTTPException
# ---------------------------------------------------------------------------

def test_paperless_not_configured_returns_inline_error(app_client, monkeypatch):
    """When Paperless is not configured, verify-selected must return an
    inline error fragment (not raise HTTPException(503)) so HTMX
    displays feedback instead of swapping in a bare error page."""
    monkeypatch.setattr(
        "lamella.core.config.Settings.paperless_configured",
        property(lambda self: False),
    )
    resp = app_client.post(
        "/documents/verify-selected",
        data={"doc_ids": "42"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 400, (
        f"Expected 400 inline error; got {resp.status_code}. Body: {resp.text[:200]}"
    )
    assert "configured" in resp.text.lower(), (
        f"Error should mention configuration; got: {resp.text[:200]}"
    )


# ---------------------------------------------------------------------------
# Success path -- job modal returned (not redirect)
# ---------------------------------------------------------------------------

def test_successful_submit_returns_job_modal(app_client):
    """Non-empty selection with Paperless+AI configured submits a job and
    returns the _job_modal.html partial -- not a redirect."""

    class _FakeRunner:
        def submit(self, *, kind, title, fn, total, return_url, **kwargs):
            return "fake-job-id-001"

    app_client.app.state.job_runner = _FakeRunner()

    resp = app_client.post(
        "/documents/verify-selected",
        data={"doc_ids": "42"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200, (
        f"Success path should return 200 modal; got {resp.status_code}. "
        f"Body: {resp.text[:300]}"
    )
    assert "fake-job-id-001" in resp.text, (
        "Modal should reference the submitted job id"
    )
