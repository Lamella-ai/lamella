# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Workstream C2.4 — POST /review/staged/ask-ai.

Tier-3: AI is consulted only when the user asks. The proposal
lands on the staged_decisions row so the existing Accept button
promotes it; no ledger write happens in this path.

NOTE: The synchronous ``POST /review/staged/ask-ai`` route was
retired (see staging_review.py:1721 — every UI consumer now uses
``POST /api/txn/staged:<id>/ask-ai``). The replacement is covered
by ``tests/test_api_txn.py``. Tests in this file are kept as
historical guards but xfail'd against the retired endpoint until
they are rewritten against the new shape.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx
from pydantic import SecretStr

pytestmark = pytest.mark.xfail(
    reason=(
        "POST /review/staged/ask-ai retired — replacement route "
        "POST /api/txn/staged:<id>/ask-ai is tested in "
        "tests/test_api_txn.py. See staging_review.py:1721. "
        "Rewrite this file against the new endpoint shape."
    ),
    strict=False,
)


FIXTURES_AI = Path(__file__).parent / "fixtures" / "openrouter"


def _load(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _seed_accounts_meta(db):
    db.execute(
        """
        INSERT INTO accounts_meta (account_path, display_name,
                                   simplefin_account_id)
        VALUES (?, ?, ?)
        ON CONFLICT(account_path) DO UPDATE SET
            simplefin_account_id = excluded.simplefin_account_id
        """,
        ("Liabilities:Acme:Card:CardA1234", "CardA Acme", "sf-card-a"),
    )
    db.commit()


def _stage_one_row(db, payee: str = "Hardware Store"):
    from lamella.features.import_.staging import StagingService
    svc = StagingService(db)
    row = svc.stage(
        source="simplefin",
        source_ref={"account_id": "sf-card-a", "txn_id": "sf-ask-1"},
        posting_date="2026-04-20",
        amount="-42.17",
        currency="USD",
        payee=payee,
        description="HARDWARE STORE #1234",
    )
    db.commit()
    return row.id


def test_ask_ai_requires_ai_enabled(app_client, settings):
    """Without OPENROUTER_API_KEY set, the ask-ai endpoint refuses
    cleanly — it does NOT fall through to a no-op."""
    # The developer may have OPENROUTER_API_KEY in their env; force
    # it off on the app's live settings to exercise the disabled
    # branch deterministically.
    app_client.app.state.settings.openrouter_api_key = None

    db = app_client.app.state.db
    _seed_accounts_meta(db)
    staged_id = _stage_one_row(db)

    r = app_client.post(
        "/review/staged/ask-ai",
        data={"staged_id": staged_id},
        follow_redirects=False,
    )
    assert r.status_code == 503
    assert "ai service disabled" in r.json()["detail"].lower()


def test_ask_ai_writes_proposal_to_staged_decisions(
    app_client, settings, monkeypatch
):
    """Happy path: the AI returns a confident proposal, the handler
    writes it to staged_decisions, and the next render of
    /review/staged shows the proposed account on the row."""
    # Enable AI.
    settings.openrouter_api_key = SecretStr("sk-test")
    settings.ai_cache_ttl_hours = 0

    db = app_client.app.state.db
    _seed_accounts_meta(db)
    staged_id = _stage_one_row(db)

    with respx.mock(assert_all_called=False) as mock:
        mock.post(
            "https://openrouter.ai/api/v1/chat/completions"
        ).respond(200, json=_load(FIXTURES_AI / "classify_confident.json"))
        r = app_client.post(
            "/review/staged/ask-ai",
            data={"staged_id": staged_id},
            follow_redirects=False,
        )

    assert r.status_code == 303, r.text
    loc = r.headers.get("location", "")
    assert "ask_ai_proposed_" in loc

    decision = db.execute(
        "SELECT account, decided_by, needs_review, confidence "
        "  FROM staged_decisions WHERE staged_id = ?",
        (staged_id,),
    ).fetchone()
    assert decision is not None
    assert decision["decided_by"] == "ai"
    assert decision["needs_review"] == 1
    # The fixture returns target Expenses:Acme:Supplies at high
    # confidence. The row should now surface that as the proposal.
    assert decision["account"] == "Expenses:Acme:Supplies"


def test_ask_ai_already_promoted_is_noop(app_client, settings, monkeypatch):
    """If the staged row is already promoted, ask-ai returns a
    redirect with an informational message and does not call the
    AI client."""
    settings.openrouter_api_key = SecretStr("sk-test")
    db = app_client.app.state.db
    _seed_accounts_meta(db)
    staged_id = _stage_one_row(db)

    from lamella.features.import_.staging import StagingService
    StagingService(db).mark_promoted(
        staged_id, promoted_to_file="test.bean",
    )
    db.commit()

    with respx.mock(assert_all_called=False) as mock:
        # Any unexpected call to OpenRouter would blow up here
        # because we're not stubbing /chat/completions.
        r = app_client.post(
            "/review/staged/ask-ai",
            data={"staged_id": staged_id},
            follow_redirects=False,
        )
    assert r.status_code == 303
    assert "already_promoted" in r.headers.get("location", "")
