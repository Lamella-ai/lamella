# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Test for the /documents/writebacks audit view."""
from __future__ import annotations

import json


def _seed_writeback(conn, *, paperless_id: int, kind: str,
                    dedup_key: str, payload: dict, ai_decision_id: int | None = None):
    conn.execute(
        """
        INSERT INTO paperless_writeback_log
            (paperless_id, kind, dedup_key, payload_json, ai_decision_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        (paperless_id, kind, dedup_key, json.dumps(payload), ai_decision_id),
    )


def _seed_doc(conn, *, paperless_id: int, vendor: str, title: str):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, title, vendor, tags_json)
        VALUES (?, ?, ?, '[]')
        """,
        (paperless_id, title, vendor),
    )


def test_writebacks_empty_state(app_client):
    resp = app_client.get("/documents/writebacks")
    assert resp.status_code == 200
    assert "Paperless writebacks" in resp.text
    assert "No writebacks yet" in resp.text


def test_writebacks_lists_rows_with_diffs(app_client):
    db = app_client.app.state.db
    _seed_doc(db, paperless_id=42, vendor="Warehouse Club", title="Gas receipt")
    _seed_writeback(
        db, paperless_id=42, kind="verify_correction",
        dedup_key="decision:99",
        payload={
            "diffs": [
                {"field": "receipt_date", "before": "2064-01-08",
                 "after": "2026-04-17", "confidence": 0.95},
            ],
            "patch": {"receipt_date": "2026-04-17"},
        },
        ai_decision_id=99,
    )
    _seed_doc(db, paperless_id=43, vendor="Shell", title="Fuel receipt")
    _seed_writeback(
        db, paperless_id=43, kind="enrichment_note",
        dedup_key="abc123",
        payload={"note": "🤖 Lamella context: Vehicle: 2009 Work SUV"},
    )

    resp = app_client.get("/documents/writebacks")
    assert resp.status_code == 200
    # Both rows present.
    assert "Verify correction" in resp.text
    assert "Enrichment" in resp.text
    # Diff summary is rendered.
    assert "2064-01-08" in resp.text
    assert "2026-04-17" in resp.text
    # Enrichment note body surfaced.
    assert "2009 Work SUV" in resp.text
    # Vendor + doc link.
    assert "Warehouse Club" in resp.text
    assert "Shell" in resp.text
    assert "/paperless/preview/42" in resp.text
    assert "/paperless/preview/43" in resp.text
    # Totals badge.
    assert "Total: <strong>2</strong>" in resp.text
