# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unit tests for the trickle classify gate + neighbor-agreement logic.

The trickle is a scheduled job that runs LLM calls on a tightly
restricted set of FIXMEs. These tests lock the two filters that
control "which rows actually reach the AI":

  - ``_agreeing_target`` — used by both pattern-from-neighbors
    (auto-apply) and the AI gate (qualifies a row for AI).
  - ``_is_context_ripe`` — the direct-evidence gate from
    docs/specs/AI-CLASSIFICATION.md "Scheduling — context-gated trickle."

The full ``run_trickle`` orchestrator is exercised in tandem with
the AI client mocks in test_no_real_external_http; here we lock
the pure-logic primitives.
"""
from __future__ import annotations

from collections import namedtuple
from datetime import date
from decimal import Decimal

from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction

from lamella.features.ai_cascade import trickle_classify as tc


# A minimal stand-in for VectorIndex.VectorMatch — only the
# attributes _agreeing_target reads.
NeighborStub = namedtuple(
    "NeighborStub", ["target_account", "similarity"]
)


def _mk_fixme_txn(
    *,
    narration: str = "",
    payee: str | None = None,
    txn_date: date = date(2024, 5, 10),
) -> Transaction:
    return Transaction(
        meta={"filename": "x", "lineno": 1},
        date=txn_date,
        flag="*",
        payee=payee,
        narration=narration,
        tags=set(),
        links=set(),
        postings=[
            Posting(
                account="Assets:Personal:Checking",
                units=Amount(Decimal("-42.00"), "USD"),
                cost=None, price=None, flag=None, meta={},
            ),
            Posting(
                account="Expenses:FIXME",
                units=Amount(Decimal("42.00"), "USD"),
                cost=None, price=None, flag=None, meta={},
            ),
        ],
    )


# ---------- _agreeing_target ----------


def test_agreeing_target_three_neighbors_agree():
    n = [
        NeighborStub("Expenses:Personal:Coffee", 0.92),
        NeighborStub("Expenses:Personal:Coffee", 0.88),
        NeighborStub("Expenses:Personal:Coffee", 0.86),
    ]
    assert tc._agreeing_target(
        n, min_similarity=0.85, min_count=3,
    ) == "Expenses:Personal:Coffee"


def test_agreeing_target_only_two_dont_qualify_at_three():
    n = [
        NeighborStub("Expenses:Personal:Coffee", 0.92),
        NeighborStub("Expenses:Personal:Coffee", 0.88),
        NeighborStub("Expenses:Personal:Office", 0.91),
    ]
    assert tc._agreeing_target(
        n, min_similarity=0.85, min_count=3,
    ) is None


def test_agreeing_target_below_similarity_dropped():
    n = [
        NeighborStub("Expenses:Personal:Coffee", 0.92),
        NeighborStub("Expenses:Personal:Coffee", 0.40),  # too low
        NeighborStub("Expenses:Personal:Coffee", 0.36),  # too low
    ]
    assert tc._agreeing_target(
        n, min_similarity=0.85, min_count=3,
    ) is None


def test_agreeing_target_skips_fixme_neighbors():
    n = [
        NeighborStub("Expenses:FIXME", 0.99),
        NeighborStub("Expenses:FIXME", 0.99),
        NeighborStub("Expenses:Personal:Coffee", 0.90),
    ]
    # Only one non-FIXME target: shouldn't pass min_count=3.
    assert tc._agreeing_target(
        n, min_similarity=0.85, min_count=3,
    ) is None


def test_agreeing_target_tie_returns_none():
    """Two targets with the same neighbor count → no winner. The
    AI gate explicitly refuses to pick when neighbors disagree."""
    n = [
        NeighborStub("Expenses:Personal:Coffee", 0.95),
        NeighborStub("Expenses:Personal:Coffee", 0.92),
        NeighborStub("Expenses:Personal:Office", 0.95),
        NeighborStub("Expenses:Personal:Office", 0.92),
    ]
    assert tc._agreeing_target(
        n, min_similarity=0.85, min_count=2,
    ) is None


def test_agreeing_target_at_loose_ai_gate():
    """The AI gate uses min_count=2, similarity≥0.55. Two
    moderately-similar neighbors should qualify the row even
    though they wouldn't trigger pattern auto-apply."""
    n = [
        NeighborStub("Expenses:Personal:Coffee", 0.62),
        NeighborStub("Expenses:Personal:Coffee", 0.58),
        NeighborStub("Expenses:Personal:Other", 0.30),
    ]
    assert tc._agreeing_target(
        n,
        min_similarity=tc.AI_GATE_NEIGHBOR_SIMILARITY,
        min_count=tc.AI_GATE_MIN_NEIGHBORS,
    ) == "Expenses:Personal:Coffee"


# ---------- _is_context_ripe ----------


def test_gate_passes_with_linked_receipt(db, monkeypatch):
    """A row with a linked receipt is direct evidence; gate opens."""
    txn = _mk_fixme_txn(narration="Some merchant")
    # Stub _has_linked_receipt to avoid full receipt_links setup.
    monkeypatch.setattr(
        tc, "_has_linked_receipt", lambda h, c: True,
    )
    monkeypatch.setattr(
        tc, "_has_active_project", lambda t, c: False,
    )
    assert tc._is_context_ripe(
        txn=txn, fixme_account="Expenses:FIXME",
        conn=db, neighbors=[],
    )


def test_gate_blocks_bare_txn(db, monkeypatch):
    """No memo, no receipt, no project, no neighbors → off-gate."""
    txn = _mk_fixme_txn(narration="")
    monkeypatch.setattr(tc, "_has_linked_receipt", lambda h, c: False)
    monkeypatch.setattr(tc, "_has_active_project", lambda t, c: False)
    assert not tc._is_context_ripe(
        txn=txn, fixme_account="Expenses:FIXME",
        conn=db, neighbors=[],
    )


def test_gate_passes_memo_plus_active_project(db, monkeypatch):
    txn = _mk_fixme_txn(narration="Vendor")
    monkeypatch.setattr(tc, "_has_linked_receipt", lambda h, c: False)
    monkeypatch.setattr(tc, "_has_active_project", lambda t, c: True)
    assert tc._is_context_ripe(
        txn=txn, fixme_account="Expenses:FIXME",
        conn=db, neighbors=[],
    )


def test_gate_passes_memo_plus_neighbors(db, monkeypatch):
    """Memo + ≥2 agreeing neighbors at the loose AI-gate bar opens
    the gate — this is the 'proximity to a classified group'
    signal the user asked for."""
    txn = _mk_fixme_txn(narration="Recognizable merchant")
    monkeypatch.setattr(tc, "_has_linked_receipt", lambda h, c: False)
    monkeypatch.setattr(tc, "_has_active_project", lambda t, c: False)
    neighbors = [
        NeighborStub("Expenses:Personal:Coffee", 0.62),
        NeighborStub("Expenses:Personal:Coffee", 0.58),
    ]
    assert tc._is_context_ripe(
        txn=txn, fixme_account="Expenses:FIXME",
        conn=db, neighbors=neighbors,
    )


def test_gate_blocks_memo_only_with_no_neighbors(db, monkeypatch):
    """A memo by itself isn't enough — without project, neighbors,
    or receipt the row stays off-gate. This is the philosophy:
    don't burn a token on something the AI can't anchor."""
    txn = _mk_fixme_txn(narration="Mystery purchase")
    monkeypatch.setattr(tc, "_has_linked_receipt", lambda h, c: False)
    monkeypatch.setattr(tc, "_has_active_project", lambda t, c: False)
    assert not tc._is_context_ripe(
        txn=txn, fixme_account="Expenses:FIXME",
        conn=db, neighbors=[],
    )


# ---------- cooldown ----------


def test_cooldown_skips_recent_proposal(db):
    """If we proposed for this txn 1 day ago, the cooldown blocks
    the next trickle from re-paying."""
    db.execute(
        """
        INSERT INTO ai_decisions
            (decision_type, input_ref, model, prompt_hash, result,
             decided_at)
        VALUES (?, ?, ?, ?, ?, datetime('now', '-1 day'))
        """,
        ("classify_txn", "abc123", "model-x", "hash", "{}"),
    )
    assert tc._has_recent_ai_decision(db, "abc123", days=7)


def test_cooldown_lets_old_proposal_retry(db):
    """A proposal from 30 days ago is past the cooldown — let the
    trickle reclassify if context has changed since."""
    db.execute(
        """
        INSERT INTO ai_decisions
            (decision_type, input_ref, model, prompt_hash, result,
             decided_at)
        VALUES (?, ?, ?, ?, ?, datetime('now', '-30 days'))
        """,
        ("classify_txn", "old-hash", "model-x", "hash", "{}"),
    )
    assert not tc._has_recent_ai_decision(db, "old-hash", days=7)


def test_cooldown_other_txn_unaffected(db):
    db.execute(
        """
        INSERT INTO ai_decisions
            (decision_type, input_ref, model, prompt_hash, result,
             decided_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        ("classify_txn", "txn-A", "model-x", "hash", "{}"),
    )
    assert not tc._has_recent_ai_decision(db, "txn-B", days=7)
