# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lamella.features.ai_cascade.decisions import CACHED_MODEL_SENTINEL, DecisionsLog


def test_log_and_get_roundtrip(db):
    dlog = DecisionsLog(db)
    decision_id = dlog.log(
        decision_type="classify_txn",
        input_ref="hash:abc",
        model="anthropic/claude-haiku-4.5",
        result={"target_account": "Expenses:X", "confidence": 0.9},
        prompt_tokens=100,
        completion_tokens=20,
        prompt_hash="deadbeef",
    )
    row = dlog.get(decision_id)
    assert row is not None
    assert row.result["target_account"] == "Expenses:X"
    assert row.prompt_tokens == 100


def test_mark_correction_flips_bool(db):
    dlog = DecisionsLog(db)
    decision_id = dlog.log(
        decision_type="classify_txn",
        input_ref="hash:abc",
        model="m",
        result={"target_account": "Expenses:Wrong"},
    )
    ok = dlog.mark_correction(decision_id, user_correction="user picked Expenses:Right")
    assert ok is True
    row = dlog.get(decision_id)
    assert row.user_corrected is True
    assert "Expenses:Right" in (row.user_correction or "")


def test_cache_lookup_ignores_errors_and_expired(db):
    dlog = DecisionsLog(db)
    dlog.log(
        decision_type="classify_txn",
        input_ref="hash:1",
        model="m",
        result={"error": "boom"},
        prompt_hash="h1",
    )
    hit = dlog.find_cache_hit(prompt_hash="h1", ttl_hours=24, decision_type="classify_txn")
    assert hit is None, "error rows must not satisfy cache"

    good = dlog.log(
        decision_type="classify_txn",
        input_ref="hash:2",
        model="m",
        result={"target_account": "Expenses:OK", "confidence": 0.9},
        prompt_hash="h2",
    )
    hit = dlog.find_cache_hit(prompt_hash="h2", ttl_hours=24, decision_type="classify_txn")
    assert hit is not None
    assert hit.id == good


def test_cache_lookup_respects_ttl_zero(db):
    dlog = DecisionsLog(db)
    dlog.log(
        decision_type="classify_txn",
        input_ref="hash:3",
        model="m",
        result={"target_account": "X", "confidence": 0.9},
        prompt_hash="h3",
    )
    assert dlog.find_cache_hit(prompt_hash="h3", ttl_hours=0) is None


def test_cost_summary_excludes_cache_hits(db):
    dlog = DecisionsLog(db)
    dlog.log(
        decision_type="classify_txn",
        input_ref="a",
        model="real-model",
        result={"x": 1},
        prompt_tokens=1000,
        completion_tokens=200,
    )
    dlog.log(
        decision_type="classify_txn",
        input_ref="b",
        model=CACHED_MODEL_SENTINEL,
        result={"x": 1},
        prompt_tokens=0,
        completion_tokens=0,
    )
    summary = dlog.cost_summary(
        since=datetime.now(timezone.utc) - timedelta(days=1),
        prompt_price_per_1k=0.001,
        completion_price_per_1k=0.005,
    )
    assert summary["calls"] == 1
    assert summary["cache_hits"] == 1
    assert summary["prompt_tokens"] == 1000
    assert summary["completion_tokens"] == 200
    expected = (1000 / 1000.0) * 0.001 + (200 / 1000.0) * 0.005
    assert summary["cost_usd"] == pytest.approx(expected, abs=1e-6)


def test_recent_filters_by_decision_type(db):
    dlog = DecisionsLog(db)
    dlog.log(decision_type="classify_txn", input_ref="a", model="m", result={})
    dlog.log(decision_type="match_receipt", input_ref="b", model="m", result={})
    only_classify = dlog.recent(decision_type="classify_txn")
    assert len(only_classify) == 1
    assert only_classify[0].decision_type == "classify_txn"
