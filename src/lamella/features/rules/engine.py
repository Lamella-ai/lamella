# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Iterable

from lamella.features.rules.models import PatternType, RuleMatch, RuleRow, TxnFacts


def _tier(rule: RuleRow, card_scope_matches: bool) -> int | None:
    """Return 1..6 per the briefing's deterministic ordering, or None when
    the rule is disqualified from this txn (e.g. card-scoped rule whose
    scope doesn't match the txn's card account)."""
    pt = rule.pattern_type
    scoped = rule.card_account is not None

    # A card-scoped rule whose scope does NOT match is disqualified entirely
    # — it cannot fall back to a no-scope tier.
    if scoped and not card_scope_matches:
        return None

    if pt == PatternType.MERCHANT_EXACT.value:
        return 1 if scoped else 2
    if pt == PatternType.MERCHANT_CONTAINS.value:
        return 3 if scoped else 4
    if pt == PatternType.REGEX.value:
        return 5
    if pt == PatternType.AMOUNT_RANGE.value:
        return 6
    return None


def _merchant_candidates(txn: TxnFacts) -> list[str]:
    vals: list[str] = []
    if txn.payee:
        vals.append(txn.payee)
    if txn.narration:
        vals.append(txn.narration)
    merged = txn.merchant_text
    if merged and merged not in vals:
        vals.append(merged)
    return vals


def _parse_amount_range(value: str) -> tuple[Decimal, Decimal] | None:
    """`amount_range` pattern_value format: "lo..hi" where both are plain
    decimals, both inclusive. Example: "50.00..250.00"."""
    if ".." not in value:
        return None
    lo_s, hi_s = value.split("..", 1)
    try:
        lo = Decimal(lo_s.strip())
        hi = Decimal(hi_s.strip())
    except (InvalidOperation, ValueError):
        return None
    if hi < lo:
        lo, hi = hi, lo
    return lo, hi


def matches(rule: RuleRow, txn: TxnFacts) -> bool:
    pt = rule.pattern_type
    needle = rule.pattern_value or ""

    if pt == PatternType.MERCHANT_EXACT.value:
        return any(c == needle for c in _merchant_candidates(txn))
    if pt == PatternType.MERCHANT_CONTAINS.value:
        nlow = needle.lower()
        return any(nlow in c.lower() for c in _merchant_candidates(txn))
    if pt == PatternType.REGEX.value:
        try:
            rx = re.compile(needle)
        except re.error:
            return False
        return any(rx.search(c) is not None for c in _merchant_candidates(txn))
    if pt == PatternType.AMOUNT_RANGE.value:
        amt = txn.amount
        if amt is None:
            return False
        bounds = _parse_amount_range(needle)
        if bounds is None:
            return False
        lo, hi = bounds
        abs_amt = abs(Decimal(amt))
        return lo <= abs_amt <= hi
    return False


def evaluate(txn: TxnFacts, rules: Iterable[RuleRow]) -> RuleMatch | None:
    """Return the highest-priority rule match, or None. Deterministic:
    tier first (1..6), then `hit_count DESC`, then `id ASC`. A rule is only
    a *suggestion* — never auto-applied in Phase 2."""
    best: RuleMatch | None = None
    best_key: tuple[int, int, int] | None = None

    for rule in rules:
        card_scope_matches = (
            rule.card_account is None
            or (txn.card_account is not None and rule.card_account == txn.card_account)
        )
        tier = _tier(rule, card_scope_matches)
        if tier is None:
            continue
        if not matches(rule, txn):
            continue

        # Sort key: lower tier wins; within tier, higher hit_count wins;
        # final tiebreak on rule.id for determinism.
        key = (tier, -rule.hit_count, rule.id)
        if best_key is None or key < best_key:
            best = RuleMatch(rule=rule, tier=tier)
            best_key = key

    return best
