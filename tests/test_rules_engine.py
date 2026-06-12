# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from decimal import Decimal

from lamella.features.rules.engine import evaluate
from lamella.features.rules.models import RuleRow, TxnFacts


def _rule(**kw) -> RuleRow:
    defaults = dict(
        id=1,
        pattern_type="merchant_exact",
        pattern_value="Hardware Store",
        card_account=None,
        target_account="Expenses:Acme:Supplies",
        confidence=1.0,
        hit_count=0,
        last_used=None,
        created_by="user",
    )
    defaults.update(kw)
    return RuleRow(**defaults)


def test_no_rules_returns_none():
    txn = TxnFacts(payee="Hardware Store", narration=None, amount=Decimal("10"), card_account=None)
    assert evaluate(txn, []) is None


def test_merchant_exact_matches_payee():
    rules = [_rule(pattern_type="merchant_exact", pattern_value="Hardware Store")]
    txn = TxnFacts(payee="Hardware Store", narration="Supplies", amount=Decimal("10"), card_account=None)
    match = evaluate(txn, rules)
    assert match is not None
    assert match.target_account == "Expenses:Acme:Supplies"
    assert match.tier == 2


def test_merchant_contains_case_insensitive():
    rules = [_rule(id=1, pattern_type="merchant_contains", pattern_value="home improvement store")]
    txn = TxnFacts(payee="A HOME IMPROVEMENT STORE #12345", narration=None, amount=None, card_account=None)
    match = evaluate(txn, rules)
    assert match is not None


def test_card_scope_wins_over_unscoped():
    scoped = _rule(
        id=1,
        pattern_type="merchant_exact",
        pattern_value="Hardware Store",
        card_account="Liabilities:Acme:Card:CardA1234",
        target_account="Expenses:Acme:Supplies",
    )
    unscoped = _rule(
        id=2,
        pattern_type="merchant_exact",
        pattern_value="Hardware Store",
        card_account=None,
        target_account="Expenses:Personal:Home",
    )
    txn = TxnFacts(
        payee="Hardware Store",
        narration=None,
        amount=Decimal("10"),
        card_account="Liabilities:Acme:Card:CardA1234",
    )
    match = evaluate(txn, [unscoped, scoped])
    assert match is not None
    assert match.tier == 1
    assert match.target_account == "Expenses:Acme:Supplies"


def test_scoped_rule_skipped_when_card_mismatches():
    scoped = _rule(
        id=1,
        pattern_type="merchant_exact",
        pattern_value="Hardware Store",
        card_account="Liabilities:Acme:Card:CardA1234",
        target_account="Expenses:Acme:Supplies",
    )
    unscoped = _rule(
        id=2,
        pattern_type="merchant_exact",
        pattern_value="Hardware Store",
        card_account=None,
        target_account="Expenses:Personal:Home",
    )
    txn = TxnFacts(
        payee="Hardware Store",
        narration=None,
        amount=Decimal("10"),
        card_account="Liabilities:Personal:Card:CardB9876",
    )
    match = evaluate(txn, [scoped, unscoped])
    assert match is not None
    assert match.target_account == "Expenses:Personal:Home"


def test_tier_ordering_exact_before_contains():
    r_exact = _rule(id=1, pattern_type="merchant_exact", pattern_value="USPS",
                    target_account="Expenses:Acme:Shipping")
    r_contains = _rule(id=2, pattern_type="merchant_contains", pattern_value="US",
                       target_account="Expenses:Acme:Office")
    txn = TxnFacts(payee="USPS", narration=None, amount=None, card_account=None)
    match = evaluate(txn, [r_contains, r_exact])
    assert match.target_account == "Expenses:Acme:Shipping"
    assert match.tier == 2


def test_hit_count_breaks_ties_within_tier():
    r_low = _rule(id=1, pattern_type="merchant_contains", pattern_value="office",
                  target_account="Expenses:Acme:Office", hit_count=1)
    r_high = _rule(id=2, pattern_type="merchant_contains", pattern_value="office",
                   target_account="Expenses:Acme:SuppliesOffice", hit_count=50)
    txn = TxnFacts(payee="Office Depot", narration=None, amount=None, card_account=None)
    match = evaluate(txn, [r_low, r_high])
    assert match.target_account == "Expenses:Acme:SuppliesOffice"


def test_regex_pattern():
    rules = [_rule(pattern_type="regex", pattern_value=r"^Amazon.*",
                   target_account="Expenses:Acme:Supplies")]
    txn = TxnFacts(payee="Amazon Marketplace", narration=None, amount=None, card_account=None)
    assert evaluate(txn, rules) is not None


def test_amount_range_weakest_tier():
    rules = [
        _rule(id=1, pattern_type="amount_range", pattern_value="0..100",
              target_account="Expenses:Acme:Office"),
        _rule(id=2, pattern_type="merchant_contains", pattern_value="office",
              target_account="Expenses:Acme:SuppliesOffice"),
    ]
    txn = TxnFacts(payee="Office Store", narration=None, amount=Decimal("42"), card_account=None)
    match = evaluate(txn, rules)
    assert match.target_account == "Expenses:Acme:SuppliesOffice"


def test_amount_range_matches_abs_value():
    rules = [_rule(pattern_type="amount_range", pattern_value="50..100",
                   target_account="Expenses:Acme:Supplies")]
    txn = TxnFacts(payee="x", narration=None, amount=Decimal("-75"), card_account=None)
    assert evaluate(txn, rules) is not None


def test_invalid_regex_does_not_crash():
    rules = [_rule(pattern_type="regex", pattern_value="[invalid",
                   target_account="Expenses:Acme:Supplies")]
    txn = TxnFacts(payee="x", narration=None, amount=None, card_account=None)
    assert evaluate(txn, rules) is None
