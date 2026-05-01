# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from lamella.features.rules.service import (
    AI_DEMOTION_FLOOR,
    AI_INITIAL_CONFIDENCE,
    AI_PROMOTION_THRESHOLD,
    RuleService,
)


def test_ai_created_rule_starts_below_auto_apply(db):
    svc = RuleService(db)
    rid = svc.learn_from_decision(
        matched_rule_id=None,
        user_target_account="Expenses:Acme:Supplies",
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        source="ai",
    )
    rule = svc.get(rid)
    assert rule is not None
    assert rule.created_by == "ai"
    assert rule.confidence == AI_INITIAL_CONFIDENCE
    assert rule.hit_count == 0  # not bumped on creation


def test_ai_rule_promotes_after_threshold(db):
    svc = RuleService(db)
    rid = svc.learn_from_decision(
        matched_rule_id=None,
        user_target_account="Expenses:Acme:Supplies",
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        source="ai",
    )
    for _ in range(AI_PROMOTION_THRESHOLD):
        svc.learn_from_decision(
            matched_rule_id=rid,
            user_target_account="Expenses:Acme:Supplies",
            pattern_value="hardware store",
        )
    rule = svc.get(rid)
    assert rule.created_by == "user"
    assert rule.confidence == 1.0


def test_contradiction_demotes_ai_rule(db):
    svc = RuleService(db)
    rid = svc.create(
        pattern_type="merchant_contains",
        pattern_value="amazon",
        target_account="Expenses:Acme:Supplies",
        confidence=AI_INITIAL_CONFIDENCE,
        created_by="ai",
    )
    svc.learn_from_decision(
        matched_rule_id=rid,
        user_target_account="Expenses:Acme:Subscriptions",
        pattern_type="merchant_contains",
        pattern_value="amazon",
    )
    demoted = svc.get(rid)
    assert demoted.confidence < AI_INITIAL_CONFIDENCE


def test_demotion_never_below_floor(db):
    svc = RuleService(db)
    rid = svc.create(
        pattern_type="merchant_contains",
        pattern_value="amazon",
        target_account="Expenses:Wrong",
        confidence=AI_DEMOTION_FLOOR + 0.01,
        created_by="ai",
    )
    for _ in range(10):
        svc.learn_from_decision(
            matched_rule_id=rid,
            user_target_account="Expenses:Right",
            pattern_type="merchant_contains",
            pattern_value="amazon",
            create_if_missing=False,
        )
    rule = svc.get(rid)
    assert rule.confidence >= AI_DEMOTION_FLOOR


def test_user_rule_unchanged_on_contradiction(db):
    svc = RuleService(db)
    rid = svc.create(
        pattern_type="merchant_contains",
        pattern_value="amazon",
        target_account="Expenses:Acme:Supplies",
        confidence=1.0,
        created_by="user",
    )
    svc.learn_from_decision(
        matched_rule_id=rid,
        user_target_account="Expenses:Acme:Subscriptions",
        pattern_type="merchant_contains",
        pattern_value="amazon",
    )
    rule = svc.get(rid)
    assert rule.confidence == 1.0  # user rules never demote
