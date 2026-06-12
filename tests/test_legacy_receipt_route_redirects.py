# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0061 Phase 5 — legacy /receipts/* and /txn/{token}/receipt-* paths
must 308-redirect to their /documents/* and /txn/{token}/document-*
counterparts.

308 (not 301) is what we use because 308 preserves the request method
and body. HTMX POSTs against /receipts/needed/<hash>/dismiss must keep
working through the redirect.

Each test asserts:
  * status code is 308
  * Location header points at the new path
  * for GET routes with query params, the querystring is preserved
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# /receipts → /documents
# ---------------------------------------------------------------------------

def test_legacy_receipts_list_redirects(app_client):
    """ADR-0061 Phase 5 follow-up: /receipts now lands on the
    receipt-filtered sub-view, not the unfiltered /documents listing —
    the bookmark intent is "show me receipts."""
    resp = app_client.get("/receipts", follow_redirects=False)
    assert resp.status_code == 308, resp.text
    assert resp.headers["location"] == "/documents/receipts"


def test_legacy_receipts_list_preserves_query(app_client):
    """308 forwarding must preserve the original querystring so existing
    bookmarks like /receipts?link_status=linked&linked_since=0 still
    point at the right filter on the new path."""
    resp = app_client.get(
        "/receipts?link_status=linked&linked_since=0",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == "/documents/receipts?link_status=linked&linked_since=0"
    )


def test_legacy_receipts_needed_redirects(app_client):
    resp = app_client.get("/receipts/needed", follow_redirects=False)
    assert resp.status_code == 308
    assert resp.headers["location"] == "/documents/receipts/needed"


def test_legacy_receipts_needed_preserves_query(app_client):
    resp = app_client.get(
        "/receipts/needed?required_only=true&page=2",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == "/documents/receipts/needed?required_only=true&page=2"
    )


def test_legacy_receipts_needed_partial_redirects(app_client):
    resp = app_client.get(
        "/receipts/needed/partial?lookback_days=30",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == "/documents/needed/partial?lookback_days=30"
    )


def test_legacy_receipts_dangling_redirects(app_client):
    resp = app_client.get("/receipts/dangling", follow_redirects=False)
    assert resp.status_code == 308
    assert resp.headers["location"] == "/documents/receipts/dangling"


def test_legacy_receipts_dangling_sweep_redirects_post(app_client):
    """POST /receipts/dangling/sweep must 308 (not 301) so the body +
    method survive."""
    resp = app_client.post(
        "/receipts/dangling/sweep", follow_redirects=False,
    )
    assert resp.status_code == 308
    assert resp.headers["location"] == "/documents/dangling/sweep"


def test_legacy_receipts_verify_selected_redirects_post(app_client):
    resp = app_client.post(
        "/receipts/verify-selected",
        data={"doc_ids": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert resp.headers["location"] == "/documents/verify-selected"


def test_legacy_receipts_doc_link_redirects_post(app_client):
    """POST /receipts/{doc_id}/link with a numeric id."""
    resp = app_client.post(
        "/receipts/42/link",
        data={
            "txn_hash": "deadbeef" * 5,
            "txn_date": "2026-04-20",
            "txn_amount": "12.34",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert resp.headers["location"] == "/documents/42/link"


# ---------------------------------------------------------------------------
# /receipts/needed/{hash}/* — txn-scoped link/dismiss/etc.
# ---------------------------------------------------------------------------

_FAKE_HASH = "deadbeef" * 5  # 40 hex chars — matcher-style content hash


def test_legacy_receipts_needed_link_redirects_post(app_client):
    resp = app_client.post(
        f"/receipts/needed/{_FAKE_HASH}/link",
        data={"paperless_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == f"/documents/needed/{_FAKE_HASH}/link"
    )


def test_legacy_receipts_needed_dismiss_redirects_post(app_client):
    resp = app_client.post(
        f"/receipts/needed/{_FAKE_HASH}/dismiss",
        data={"reason": "cash tip"},
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == f"/documents/needed/{_FAKE_HASH}/dismiss"
    )


def test_legacy_receipts_needed_undismiss_redirects_post(app_client):
    resp = app_client.post(
        f"/receipts/needed/{_FAKE_HASH}/undismiss",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == f"/documents/needed/{_FAKE_HASH}/undismiss"
    )


def test_legacy_receipts_needed_search_redirects_get(app_client):
    resp = app_client.get(
        f"/receipts/needed/{_FAKE_HASH}/search?q=acme",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == f"/documents/needed/{_FAKE_HASH}/search?q=acme"
    )


def test_legacy_receipts_needed_bulk_dismiss_redirects_post(app_client):
    resp = app_client.post(
        "/receipts/needed/bulk/dismiss",
        data={"txn_hash": _FAKE_HASH, "reason": "cash"},
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == "/documents/needed/bulk/dismiss"
    )


# ---------------------------------------------------------------------------
# /txn/{token}/receipt-* → /txn/{token}/document-*
# ---------------------------------------------------------------------------

# Use a UUIDv7-shaped token so the redirect target is realistic; the
# legacy redirect handler only echoes the path segment, it doesn't
# validate the token shape (the destination handler does).
_FAKE_UUID = "01234567-89ab-7cde-89ab-cdef01234567"


def test_legacy_txn_receipt_section_redirects_get(app_client):
    resp = app_client.get(
        f"/txn/{_FAKE_UUID}/receipt-section",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == f"/txn/{_FAKE_UUID}/document-section"
    )


def test_legacy_txn_receipt_search_redirects_get(app_client):
    resp = app_client.get(
        f"/txn/{_FAKE_UUID}/receipt-search?q=acme+co",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == f"/txn/{_FAKE_UUID}/document-search?q=acme+co"
    )


def test_legacy_txn_receipt_link_redirects_post(app_client):
    """POST /txn/{token}/receipt-link → 308 to document-link, body
    preserved by the 308 semantics (Starlette + HTMX can re-issue)."""
    resp = app_client.post(
        f"/txn/{_FAKE_UUID}/receipt-link",
        data={"paperless_doc_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"] == f"/txn/{_FAKE_UUID}/document-link"
    )


def test_legacy_txn_receipt_link_get_also_redirects(app_client):
    """The receipt-link path also exposes a GET shape (some tooling
    pre-flights the URL); we register both so neither emits 405."""
    resp = app_client.get(
        f"/txn/{_FAKE_UUID}/receipt-link",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"] == f"/txn/{_FAKE_UUID}/document-link"
    )


def test_legacy_txn_receipt_unlink_redirects_post(app_client):
    resp = app_client.post(
        f"/txn/{_FAKE_UUID}/receipt-unlink",
        data={"paperless_doc_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"] == f"/txn/{_FAKE_UUID}/document-unlink"
    )


# ---------------------------------------------------------------------------
# Sanity: the new /documents and /documents/needed paths win over the
# legacy redirect entries (since the new routers are registered first).
# ---------------------------------------------------------------------------

def test_new_documents_path_resolves_directly(app_client):
    """Hitting /documents goes to the documents handler, not back
    through the redirect router. A 200/3xx response (not 308) is the
    success signal — anything else means the legacy redirect router
    got included before the new router."""
    resp = app_client.get("/documents", follow_redirects=False)
    assert resp.status_code != 308, (
        f"new /documents path should NOT 308; got {resp.status_code} → "
        f"{resp.headers.get('location')}"
    )


def test_new_documents_needed_resolves_directly(app_client):
    resp = app_client.get(
        "/documents/needed", follow_redirects=False,
    )
    assert resp.status_code != 308, (
        f"new /documents/needed path should NOT 308; got "
        f"{resp.status_code}"
    )


# ---------------------------------------------------------------------------
# /documents/receipts* — receipt-filtered sub-routes that the legacy
# /receipts* paths now redirect to (ADR-0061 Phase 5 follow-up).
# ---------------------------------------------------------------------------

def test_documents_receipts_resolves_directly(app_client):
    """The receipt-filtered listing returns 200 directly — no redirect
    bounce, and the response renders the receipt-only view (the
    template surfaces a "Type: Receipt" filter chip when the route
    locks the type filter)."""
    resp = app_client.get("/documents/receipts", follow_redirects=False)
    assert resp.status_code == 200, resp.text
    body = resp.text
    # The page title and chip are both written by the receipt-locked
    # variant of the documents.html template; either signal is fine,
    # but asserting both keeps the test honest if the chip alone
    # accidentally vanishes during a future refactor.
    assert "Receipts" in body
    assert "Type: Receipt" in body


def test_documents_receipts_dangling_resolves_directly(app_client):
    """The dangling-receipts report returns 200 directly. No dangling
    rows in the test fixture is fine — we're asserting the route
    exists and the template renders, not the row count."""
    resp = app_client.get(
        "/documents/receipts/dangling", follow_redirects=False,
    )
    assert resp.status_code == 200, resp.text


def test_legacy_receipts_preserves_query_into_filtered_target(app_client):
    """End-to-end check: the 308 from /receipts must carry the
    querystring through to the new /documents/receipts target so
    legacy bookmarks like /receipts?q=foo&link_status=linked land on
    a filtered receipt view with their filters intact."""
    resp = app_client.get(
        "/receipts?q=acme&link_status=linked&lookback_days=30",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == "/documents/receipts?q=acme&link_status=linked&lookback_days=30"
    )
