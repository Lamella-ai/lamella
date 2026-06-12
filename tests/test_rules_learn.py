# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from lamella.features.rules.service import RuleService


def test_create_and_list(db):
    svc = RuleService(db)
    rid = svc.create(
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        target_account="Expenses:Acme:Supplies",
    )
    rules = svc.list()
    assert len(rules) == 1
    assert rules[0].id == rid
    assert rules[0].hit_count == 0


def test_create_duplicate_returns_same_id(db):
    svc = RuleService(db)
    rid1 = svc.create(
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        target_account="Expenses:Acme:Supplies",
    )
    rid2 = svc.create(
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        target_account="Expenses:Acme:Supplies",
    )
    assert rid1 == rid2
    assert len(svc.list()) == 1


def test_learn_bumps_matching_rule(db):
    svc = RuleService(db)
    rid = svc.create(
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        target_account="Expenses:Acme:Supplies",
    )
    returned = svc.learn_from_decision(
        matched_rule_id=rid,
        user_target_account="Expenses:Acme:Supplies",
        pattern_value="hardware store",
    )
    assert returned == rid
    rule = svc.get(rid)
    assert rule.hit_count == 1
    assert rule.last_used is not None


def test_learn_contradictory_decision_inserts_new_pinned_rule(db):
    svc = RuleService(db)
    existing = svc.create(
        pattern_type="merchant_contains",
        pattern_value="amazon",
        target_account="Expenses:Acme:Supplies",
    )
    new_id = svc.learn_from_decision(
        matched_rule_id=existing,
        user_target_account="Expenses:Acme:Subscriptions",
        pattern_type="merchant_contains",
        pattern_value="amazon",
        create_if_missing=True,
    )
    assert new_id is not None
    assert new_id != existing
    # Original rule's hit_count was NOT bumped.
    orig = svc.get(existing)
    assert orig.hit_count == 0


def test_learn_without_create_is_noop_for_new_pattern(db):
    svc = RuleService(db)
    result = svc.learn_from_decision(
        matched_rule_id=None,
        user_target_account="Expenses:Acme:Supplies",
        pattern_value="hardware store",
        create_if_missing=False,
    )
    assert result is None
    assert svc.list() == []


def test_delete_rule(db):
    svc = RuleService(db)
    rid = svc.create(
        pattern_type="merchant_exact",
        pattern_value="USPS",
        target_account="Expenses:Acme:Shipping",
    )
    assert svc.delete(rid) is True
    assert svc.get(rid) is None
    assert svc.delete(rid) is False


def test_unknown_pattern_type_rejected(db):
    svc = RuleService(db)
    try:
        svc.create(
            pattern_type="bogus",
            pattern_value="x",
            target_account="Expenses:Acme:Supplies",
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError")
