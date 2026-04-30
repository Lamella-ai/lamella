# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the per-/txn receipt-attach action.

Covers the four endpoints under /txn/{token}/receipt-* that let a
user search Paperless and link/unlink documents directly from a
staged-row detail page (or, equivalently, from a ledger-txn detail
page — same routes, same identity).

The routes key off the immutable ``lamella-txn-id`` (UUIDv7) carried
on every staged row, so the existing ``ReceiptLinker`` works
unchanged: we pass the UUIDv7 as ``txn_hash`` and the directive lands
in ``connector_links.bean`` with the same shape as a ledger-txn link.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import respx


def _stage_one(
    db, *, payee: str = "Acme Co.", amount: str = "-12.34",
    source_ref: dict | None = None,
):
    from lamella.features.import_.staging import StagingService
    svc = StagingService(db)
    row = svc.stage(
        source="csv",
        source_ref=source_ref or {"id": "csv-receipt-1"},
        posting_date="2026-04-20",
        amount=Decimal(amount),
        currency="USD",
        payee=payee,
        description="receipt-attach test",
    )
    db.commit()
    return row


def _paperless_doc_payload(*, doc_id: int, title: str) -> dict:
    """Minimal Paperless /api/documents/ row shape that satisfies
    the Document pydantic schema. No correspondent or custom fields
    needed for the search-render path."""
    return {
        "id": doc_id,
        "title": title,
        "correspondent": None,
        "created": "2026-04-20T00:00:00Z",
        "added": "2026-04-20T00:00:00Z",
        "modified": "2026-04-20T00:00:00Z",
        "archive_serial_number": None,
        "original_file_name": f"{title}.pdf",
        "tags": [],
        "custom_fields": [],
        "content": "OCR text snippet for testing.",
    }


def test_receipt_section_renders_for_staged_row(app_client):
    db = app_client.app.state.db
    row = _stage_one(db)

    resp = app_client.get(
        f"/txn/{row.lamella_txn_id}/receipt-section",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.text
    # Section wrapper present so unlink swap-target works.
    assert 'id="staged-receipt-section"' in body
    # No links yet → empty-state copy.
    assert "No receipt linked yet." in body
    # Search box wired with the right hx-get URL.
    assert f'hx-get="/txn/{row.lamella_txn_id}/receipt-search"' in body


def test_receipt_section_rejects_legacy_hex_token(app_client):
    # Legacy hex (non-UUIDv7) tokens were retired in v3.
    resp = app_client.get(
        "/txn/abcdef1234567890abcdef1234567890abcdef12/receipt-section",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 404


def test_receipt_section_404_for_unknown_uuidv7(app_client):
    fake = "00000000-0000-7000-8000-000000000000"
    resp = app_client.get(
        f"/txn/{fake}/receipt-section",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 404


def test_receipt_search_renders_paperless_candidates(app_client):
    db = app_client.app.state.db
    row = _stage_one(db)

    with respx.mock(base_url="https://paperless.test") as mock:
        mock.get("/api/documents/").respond(
            200,
            json={
                "count": 2,
                "next": None,
                "previous": None,
                "results": [
                    _paperless_doc_payload(
                        doc_id=101, title="Acme Receipt April",
                    ),
                    _paperless_doc_payload(
                        doc_id=102, title="Acme Receipt May",
                    ),
                ],
            },
        )
        resp = app_client.get(
            f"/txn/{row.lamella_txn_id}/receipt-search",
            params={"q": "acme"},
            headers={"HX-Request": "true"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "2 matches" in body
    assert "#101" in body and "#102" in body
    assert "Acme Receipt April" in body
    # Each candidate's Link button posts to the right URL.
    assert f'hx-post="/txn/{row.lamella_txn_id}/receipt-link"' in body


def test_receipt_search_empty_query_prompts(app_client):
    db = app_client.app.state.db
    row = _stage_one(db)
    resp = app_client.get(
        f"/txn/{row.lamella_txn_id}/receipt-search",
        params={"q": ""},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200, resp.text
    assert "Type a query" in resp.text


def test_receipt_link_writes_directive_and_db_row(app_client, tmp_path):
    db = app_client.app.state.db
    settings = app_client.app.state.settings
    row = _stage_one(db)

    # Seed paperless_doc_index so the linker can pull a paperless_hash.
    db.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, title, original_checksum)
        VALUES (?, ?, ?)
        """,
        (101, "Acme Receipt April", "md5:abcdef1234567890"),
    )
    db.commit()

    resp = app_client.post(
        f"/txn/{row.lamella_txn_id}/receipt-link",
        data={"paperless_doc_id": "101"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 204, resp.text
    assert resp.headers.get("HX-Refresh") == "true"

    # Directive landed in connector_links.bean.
    text = settings.connector_links_path.read_text(encoding="utf-8")
    assert f'custom "receipt-link" "{row.lamella_txn_id}"' in text
    assert "lamella-paperless-id: 101" in text

    # SQLite cache row mirrors the directive.
    cached = db.execute(
        "SELECT paperless_id, txn_hash, match_method "
        "  FROM receipt_links WHERE paperless_id = ?",
        (101,),
    ).fetchone()
    assert cached is not None
    assert cached["txn_hash"] == row.lamella_txn_id
    assert cached["match_method"] == "user_confirmed"


def test_receipt_section_lists_linked_docs_after_link(app_client):
    db = app_client.app.state.db
    row = _stage_one(db)
    db.execute(
        "INSERT INTO paperless_doc_index "
        "(paperless_id, title, correspondent_name) VALUES (?, ?, ?)",
        (101, "Acme Receipt April", "Acme Co."),
    )
    db.commit()

    link = app_client.post(
        f"/txn/{row.lamella_txn_id}/receipt-link",
        data={"paperless_doc_id": "101"},
        headers={"HX-Request": "true"},
    )
    assert link.status_code == 204

    resp = app_client.get(
        f"/txn/{row.lamella_txn_id}/receipt-section",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "Linked receipts" in body
    assert "#101" in body
    assert "Acme Receipt April" in body
    # And the per-row Unlink form points at the right endpoint.
    assert f'action="/txn/{row.lamella_txn_id}/receipt-unlink"' in body


def test_receipt_unlink_removes_directive_and_db_row(app_client):
    db = app_client.app.state.db
    settings = app_client.app.state.settings
    row = _stage_one(db)

    # Link then unlink — verify both sides flip.
    app_client.post(
        f"/txn/{row.lamella_txn_id}/receipt-link",
        data={"paperless_doc_id": "202"},
        headers={"HX-Request": "true"},
    )
    pre_text = settings.connector_links_path.read_text(encoding="utf-8")
    assert f'custom "receipt-link" "{row.lamella_txn_id}"' in pre_text

    resp = app_client.post(
        f"/txn/{row.lamella_txn_id}/receipt-unlink",
        data={"paperless_doc_id": "202"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 204, resp.text
    assert resp.headers.get("HX-Refresh") == "true"

    post_text = settings.connector_links_path.read_text(encoding="utf-8")
    assert f'custom "receipt-link" "{row.lamella_txn_id}"' not in post_text
    assert "lamella-paperless-id: 202" not in post_text

    # Cache row gone.
    cached = db.execute(
        "SELECT 1 FROM receipt_links WHERE paperless_id = 202 "
        "AND txn_hash = ?",
        (row.lamella_txn_id,),
    ).fetchone()
    assert cached is None


def test_receipt_unlink_404_when_no_matching_directive(app_client):
    db = app_client.app.state.db
    row = _stage_one(db)
    resp = app_client.post(
        f"/txn/{row.lamella_txn_id}/receipt-unlink",
        data={"paperless_doc_id": "999"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 404


def test_receipt_unlink_keeps_other_links_intact(app_client):
    """Removing one receipt-link directive must NOT touch the others —
    the regex needs the (txn_id, paperless_id) pair to match."""
    db = app_client.app.state.db
    settings = app_client.app.state.settings
    row = _stage_one(db)

    for pid in (301, 302, 303):
        app_client.post(
            f"/txn/{row.lamella_txn_id}/receipt-link",
            data={"paperless_doc_id": str(pid)},
            headers={"HX-Request": "true"},
        )

    app_client.post(
        f"/txn/{row.lamella_txn_id}/receipt-unlink",
        data={"paperless_doc_id": "302"},
        headers={"HX-Request": "true"},
    )
    text = settings.connector_links_path.read_text(encoding="utf-8")
    assert "lamella-paperless-id: 301" in text
    assert "lamella-paperless-id: 303" in text
    assert "lamella-paperless-id: 302" not in text


def test_receipt_section_for_legacy_token_404s(app_client):
    """A bare-hex (legacy) token returns 404 from every endpoint —
    v3+ requires a UUIDv7."""
    legacy = "deadbeef" * 5  # 40-char hex, not UUIDv7-shaped
    for path in (
        f"/txn/{legacy}/receipt-section",
        f"/txn/{legacy}/receipt-search?q=x",
    ):
        resp = app_client.get(path, headers={"HX-Request": "true"})
        assert resp.status_code == 404, path

    for path in (
        f"/txn/{legacy}/receipt-link",
        f"/txn/{legacy}/receipt-unlink",
    ):
        resp = app_client.post(
            path,
            data={"paperless_doc_id": "1"},
            headers={"HX-Request": "true"},
        )
        assert resp.status_code == 404, path
