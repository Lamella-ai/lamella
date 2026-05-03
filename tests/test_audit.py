# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke tests for /audit."""
from __future__ import annotations


def test_audit_page_empty_state(app_client):
    resp = app_client.get("/audit")
    assert resp.status_code == 200
    assert "Classification audit" in resp.text
    # Empty state message.
    assert "No open disagreements" in resp.text


def test_audit_shows_open_items_from_db(app_client):
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO audit_runs (sample_mode, sample_size) "
        "VALUES ('random', 20)"
    )
    run_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    db.execute(
        """
        INSERT INTO audit_items (
            audit_run_id, txn_hash, txn_date, txn_amount,
            merchant_text, current_account, ai_proposed_account,
            ai_confidence, ai_reasoning
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, "abc123def456" + "0" * 52, "2026-04-17", "58.12",
         "Hardware Store MainResidence", "Expenses:Acme:OtherExpenses",
         "Expenses:Personal:HomeImprovement", 0.85,
         "Receipt items (lumber, concrete mix) match home improvement "
         "retaining-wall project active at MainResidence."),
    )
    resp = app_client.get("/audit")
    assert resp.status_code == 200
    assert "Hardware Store MainResidence" in resp.text
    assert "Expenses:Acme:OtherExpenses" in resp.text
    assert "Expenses:Personal:HomeImprovement" in resp.text
    assert "0.85" in resp.text
    assert "Accept AI" in resp.text


def test_audit_dismiss_silences_pair(app_client):
    db = app_client.app.state.db
    db.execute(
        "INSERT INTO audit_runs (sample_mode, sample_size) "
        "VALUES ('random', 20)"
    )
    run_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    db.execute(
        """
        INSERT INTO audit_items (
            audit_run_id, txn_hash, txn_date, txn_amount,
            merchant_text, current_account, ai_proposed_account,
            ai_confidence, ai_reasoning
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, "feed" + "0" * 60, "2026-04-01", "10.00",
         "Coffee Shop", "Expenses:Personal:Food:Restaurants",
         "Expenses:Personal:Food:Coffee", 0.70, "Coffee signal strong."),
    )
    item_id = db.execute(
        "SELECT id FROM audit_items ORDER BY id DESC LIMIT 1"
    ).fetchone()["id"]

    resp = app_client.post(
        f"/audit/items/{item_id}/dismiss",
        data={"reason": "Keep lumping coffee into restaurants for now"},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    # Item moved to dismissed.
    row = db.execute(
        "SELECT status FROM audit_items WHERE id = ?", (item_id,),
    ).fetchone()
    assert row["status"] == "dismissed"

    # Pair now in dismissals table.
    dismissed = db.execute(
        "SELECT reason FROM audit_dismissals "
        "WHERE merchant_text = ? AND current_account = ?",
        ("Coffee Shop", "Expenses:Personal:Food:Restaurants"),
    ).fetchone()
    assert dismissed is not None
    assert "coffee" in dismissed["reason"].lower()
