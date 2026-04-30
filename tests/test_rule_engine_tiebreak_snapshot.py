# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Snapshot the current rule-engine tie-break behaviour.

This test locks in the current semantics so any future change to the
engine's ordering (tier → hit_count DESC → id ASC) fails loudly and
forces the author to update the expected behaviour consciously.

Tie-break options we discussed but deferred:
  * most-specific wins (longest pattern match) with recency tiebreak
  * this requires an `added_at` column on classification_rules — now
    present in migration 018, populated by the step-2 reconstruct
    pass — so the engine change becomes a single-file swap when we're
    ready.

This snapshot isn't about asserting the current behaviour is optimal;
it's about refusing to let the behaviour change silently.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from lamella.features.rules.engine import evaluate
from lamella.features.rules.models import RuleRow, TxnFacts


def _rule(
    rule_id: int,
    pattern_type: str,
    pattern_value: str,
    target_account: str,
    *,
    card_account: str | None = None,
    hit_count: int = 0,
) -> RuleRow:
    return RuleRow(
        id=rule_id,
        pattern_type=pattern_type,
        pattern_value=pattern_value,
        card_account=card_account,
        target_account=target_account,
        confidence=1.0,
        hit_count=hit_count,
        last_used=None,
        created_by="user",
    )


def _txn(payee: str, narration: str = "", card: str | None = None, amount: Decimal | None = None) -> TxnFacts:
    return TxnFacts(payee=payee, narration=narration, amount=amount, card_account=card)


def test_tier_ordering_card_scoped_beats_global():
    """Card-scoped exact > global exact. Tier 1 < Tier 2."""
    rules = [
        _rule(1, "merchant_exact", "Grocery Store", "Expenses:A"),
        _rule(
            2, "merchant_exact", "Grocery Store", "Expenses:B",
            card_account="Liabilities:CC",
        ),
    ]
    match = evaluate(_txn("Grocery Store", card="Liabilities:CC"), rules)
    assert match is not None
    assert match.rule.id == 2  # card-scoped wins
    assert match.tier == 1


def test_tier_ordering_exact_beats_contains():
    rules = [
        _rule(1, "merchant_contains", "groc", "Expenses:Contains"),
        _rule(2, "merchant_exact", "Grocery Store", "Expenses:Exact"),
    ]
    match = evaluate(_txn("Grocery Store"), rules)
    assert match is not None
    assert match.rule.id == 2  # exact wins


def test_within_tier_hit_count_wins():
    """Two contains rules both match — higher hit_count wins."""
    rules = [
        _rule(1, "merchant_contains", "groc", "Expenses:Low", hit_count=5),
        _rule(2, "merchant_contains", "grocery", "Expenses:High", hit_count=100),
    ]
    match = evaluate(_txn("Grocery Store"), rules)
    assert match is not None
    assert match.rule.id == 2  # higher hit_count


def test_within_tier_id_tiebreaks_equal_hits():
    """Equal hit_count → lowest id wins (NOT most-recent — that's the
    proposed future behaviour, not current)."""
    rules = [
        _rule(5, "merchant_contains", "grocery", "Expenses:Newer", hit_count=10),
        _rule(3, "merchant_contains", "grocery", "Expenses:Older", hit_count=10),
    ]
    match = evaluate(_txn("Grocery Store"), rules)
    assert match is not None
    assert match.rule.id == 3  # lower id wins under current rules


def test_most_specific_does_NOT_win_under_current_rules():
    """CURRENT behaviour: both are `merchant_contains` (same tier), and
    hit_count is equal, so id wins — not pattern length.

    When the engine switches to most-specific-wins (longest pattern
    match) with recency tiebreak, this test flips — which is exactly
    what we want: the author of the engine change has to consciously
    update the expected behaviour.
    """
    rules = [
        _rule(1, "merchant_contains", "groc", "Expenses:Short"),
        _rule(2, "merchant_contains", "grocery store whole foods", "Expenses:Long"),
    ]
    match = evaluate(_txn("Grocery Store Whole Foods Market"), rules)
    assert match is not None
    # Current: id=1 (lower id, same tier, same hit count).
    assert match.rule.id == 1, (
        "Engine tie-break changed. If the switch to "
        "most-specific-wins (longest-pattern + recency) has landed, "
        "update this test's expected id from 1 to 2."
    )


def test_card_scoped_mismatch_disqualifies_rule():
    """A card-scoped rule whose scope doesn't match the txn's card is
    entirely disqualified, not demoted to a lower tier."""
    rules = [
        _rule(
            1, "merchant_contains", "grocery", "Expenses:Scoped",
            card_account="Liabilities:ChaseCC",
        ),
    ]
    match = evaluate(
        _txn("Grocery Store", card="Liabilities:BankOneCC"),
        rules,
    )
    assert match is None
