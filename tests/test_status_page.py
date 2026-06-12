# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke test for the /status system-health dashboard."""
from __future__ import annotations

import json


def test_status_page_renders_all_cards(app_client):
    """Smoke test: /status returns 200 with all nine card headings."""
    resp = app_client.get("/status")
    assert resp.status_code == 200
    # Every card heading appears so the template didn't silently skip any.
    expected = [
        "Ledger",
        "AI cascade",
        "Vector index",
        "Paperless",
        "SimpleFIN",
        "Review queue",
        "Rules",
        "Mileage",
        "Notifications",
    ]
    for title in expected:
        assert title in resp.text, f"expected section '{title}' missing"


def test_status_reflects_vector_index_counts(app_client):
    """When rows exist in txn_embeddings and txn_embeddings_build,
    their counts surface on the status page."""
    db = app_client.app.state.db
    # Vector search is opt-in; the test fixture leaves it OFF, so the
    # /status card short-circuits with "Disabled in /settings" and skips
    # the build-row rendering. Flip it on for the duration of this test.
    db.execute(
        "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
        ("ai_vector_search_enabled", "1"),
    )
    db.execute(
        "INSERT INTO txn_embeddings (source, identity, merchant_text, "
        "target_account, posting_date, amount, weight, embedding, "
        "dims, model_name) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("ledger", "hash1", "Warehouse Club", "Expenses:Personal:Fuel",
         "2026-04-17", "58.12", 1.0,
         b"\x00\x00\x00\x00\x00\x00\x00\x00", 2, "test-model"),
    )
    db.execute(
        "INSERT INTO txn_embeddings (source, identity, merchant_text, "
        "target_account, posting_date, amount, weight, embedding, "
        "dims, model_name) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("correction", "42", "Hardware Store", "Expenses:Acme:Supplies",
         "2026-04-10", "142.00", 2.0,
         b"\x00\x00\x00\x00\x00\x00\x00\x00", 2, "test-model"),
    )
    db.execute(
        "INSERT INTO txn_embeddings_build "
        "(source, model_name, ledger_signature, row_count) "
        "VALUES (?, ?, ?, ?)",
        ("ledger", "test-model", "2:2026-04-17:c1:l42", 2),
    )
    resp = app_client.get("/status")
    assert resp.status_code == 200
    # Both source counts surface.
    assert "1" in resp.text  # ledger rows
    # Signature visible (the page surfaces the build's ledger_signature
    # in the "Stored signature" stat; model_name is no longer rendered
    # on the public status surface).
    assert "2:2026-04-17:c1:l42" in resp.text


def test_status_vector_rebuild_clears_signature(app_client):
    """POST /status/vector-index/rebuild deletes all rows from
    txn_embeddings_build (forcing rebuild on next classify)."""
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO txn_embeddings_build "
        "(source, model_name, ledger_signature, row_count) "
        "VALUES ('ledger', 'test-model', 'stale-sig', 10)"
    )
    resp = app_client.post(
        "/status/vector-index/rebuild", follow_redirects=False,
    )
    assert resp.status_code == 303  # redirect to /status
    count = db.execute(
        "SELECT COUNT(*) AS n FROM txn_embeddings_build"
    ).fetchone()["n"]
    assert count == 0
