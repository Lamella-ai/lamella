# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression — /txn/{token} must surface AI decisions logged
under any of the input_ref shapes a transaction can accumulate
across its lifetime: lineage UUID, Beancount txn_hash, or the
SimpleFIN bridge id from ingest time.

The candidate_refs expansion in routes/search.txn_detail is
**permanent** read-side compat (matches the ``bcg-*`` pattern) —
the user never has to run a migration for /txn AI history to
surface every decision keyed against an entry. Self-healing
flows (on-touch normalization, the /setup/recovery normalize
action) converge legacy entries onto lineage over time, but
absent those the query still finds rows by their original key.
"""
from __future__ import annotations

import json

from beancount.core.data import Transaction

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.identity import get_txn_id


def _hardware_store_token_and_hash(app_client) -> tuple[str, str]:
    """Return the (lamella-txn-id, txn_hash) of the fixture's
    Hardware Store entry. The token is what the URL takes; the hash
    is what ai_decisions.input_ref carries for legacy ledger-side
    decisions logged before lineage existed."""
    entries = app_client.app.state.ledger_reader.load().entries
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        if (e.meta or {}).get("simplefin-id") == "sf-1001":
            tok = get_txn_id(e)
            assert tok, "fixture entry must carry a lamella-txn-id"
            return tok, txn_hash(e)
    raise AssertionError("fixture missing the simplefin-id sf-1001 entry")


def _seed_decision(conn, *, input_ref: str, narration: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO ai_decisions
            (decision_type, input_ref, model, prompt_tokens,
             completion_tokens, result, user_corrected, user_correction)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "classify_txn",
            input_ref,
            "anthropic/claude-haiku-4.5",
            100,
            50,
            json.dumps({
                "account": "Expenses:Acme:Supplies",
                "confidence": 0.92,
                "reasoning": narration,
            }),
            0,
            None,
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def test_txn_detail_surfaces_decisions_logged_under_simplefin_id(app_client):
    """The decision was logged at ingest under input_ref=sf-1001
    (the SimpleFIN bridge id). After promotion the entry carries
    `simplefin-id: "sf-1001"` in metadata. /txn/{hash} pulls the
    decision into ai_history by matching on the bridge id — read-side
    compat that's permanent, not migration scaffolding."""
    token, _ = _hardware_store_token_and_hash(app_client)
    conn = app_client.app.state.db
    decision_id = _seed_decision(
        conn,
        input_ref="sf-1001",
        narration="Hardware purchase classified at ingest time",
    )

    r = app_client.get(f"/txn/{token}")
    assert r.status_code == 200, r.text
    body = r.text

    assert "No AI decisions recorded for this transaction yet." not in body, (
        "ai_decisions row keyed by simplefin-id was not joined into "
        "the per-txn AI history"
    )
    # The decision row should be visible — its model name is a
    # deterministic surface marker.
    assert "claude-haiku-4.5" in body
    assert f"#{decision_id}" in body or "claude-haiku-4.5" in body


def test_txn_detail_still_finds_decisions_logged_under_txn_hash(app_client):
    """Post-promotion calls (bulk_classify, /txn ask-AI) log under
    the real txn_hash. That path must still work."""
    token, target_hash = _hardware_store_token_and_hash(app_client)
    conn = app_client.app.state.db
    _seed_decision(
        conn,
        input_ref=target_hash,
        narration="Re-classified via bulk_classify post-promotion",
    )

    r = app_client.get(f"/txn/{token}")
    assert r.status_code == 200, r.text
    assert "No AI decisions recorded for this transaction yet." not in r.text
    assert "claude-haiku-4.5" in r.text


def test_txn_detail_unrelated_simplefin_id_is_not_pulled_in(app_client):
    """Sanity — only decisions whose input_ref matches the entry's
    own staging ids should appear. A decision logged under a
    DIFFERENT SimpleFIN id must not leak into this txn's history."""
    token, _ = _hardware_store_token_and_hash(app_client)
    conn = app_client.app.state.db
    _seed_decision(
        conn,
        input_ref="sf-9999-not-this-txn",
        narration="belongs to a different transaction",
    )

    r = app_client.get(f"/txn/{token}")
    assert r.status_code == 200, r.text
    assert "No AI decisions recorded for this transaction yet." in r.text, (
        "unrelated decisions should not leak into this txn's history"
    )


def test_txn_detail_surfaces_decisions_logged_under_lamella_txn_id(
    app_client, ledger_dir,
):
    """The canonical post-lineage path. AI decisions log
    ``input_ref = lamella-txn-id`` once the entry has a lineage id;
    the /txn page must include lineage in its candidate-refs lookup
    alongside txn_hash and the staging ids."""
    # Stamp a lineage id on the fixture's Hardware Store entry so
    # the route's get_txn_id() finds it.
    # Fixture already stamps lamella-txn-id (post-v3); use it directly.
    token, _ = _hardware_store_token_and_hash(app_client)
    conn = app_client.app.state.db
    _seed_decision(
        conn,
        input_ref=token,
        narration="Classified post-Phase-3 under stable lineage",
    )

    r = app_client.get(f"/txn/{token}")
    assert r.status_code == 200, r.text
    assert "No AI decisions recorded for this transaction yet." not in r.text, (
        "ai_decisions row keyed by lamella-txn-id was not joined into "
        "the per-txn AI history"
    )
    assert "claude-haiku-4.5" in r.text
