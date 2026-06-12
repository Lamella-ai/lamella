# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from lamella.features.ai_cascade.gating import (
    AIProposal,
    ConfidenceGate,
    GateAction,
    MatchRanking,
    RuleProposal,
)


def test_user_rule_at_0_95_auto_applies():
    gate = ConfidenceGate()
    rule = RuleProposal(rule_id=1, target_account="Expenses:Acme:Supplies", confidence=0.96)
    out = gate.decide(rule=rule, ai=None)
    assert out.action == GateAction.AUTO_APPLY_RULE
    assert out.chosen_source == "rule"


def test_ai_rule_at_high_confidence_does_not_auto_apply_as_rule():
    gate = ConfidenceGate()
    rule = RuleProposal(
        rule_id=5,
        target_account="Expenses:Acme:Supplies",
        confidence=0.99,
        created_by="ai",
    )
    out = gate.decide(rule=rule, ai=None)
    # AI-created rule stays in the suggestion band even at confidence 0.99.
    assert out.action == GateAction.REVIEW_WITH_SUGGESTION
    assert out.chosen_source == "rule"


def test_ai_proposal_above_threshold_routes_to_review():
    """Workstream A — AI proposals never self-promote, no matter
    the confidence. The user's click-accept in the review UI is
    what creates a user-rule via learn_from_decision so the next
    N similar rows auto-apply."""
    gate = ConfidenceGate()
    ai = AIProposal(target_account="Expenses:Acme:Supplies", confidence=0.97)
    out = gate.decide(rule=None, ai=ai)
    assert out.action == GateAction.REVIEW_WITH_SUGGESTION
    assert out.chosen_source == "ai"
    assert out.chosen_target == "Expenses:Acme:Supplies"


def test_ai_and_matching_user_rule_rule_wins():
    """AI at 0.99 plus a matching user-rule at 0.95 → the rule
    auto-applies. The AI's presence is irrelevant to the outcome;
    this is not a 'co-decision' path."""
    gate = ConfidenceGate()
    rule = RuleProposal(
        rule_id=7,
        target_account="Expenses:Acme:Supplies",
        confidence=0.95,
        created_by="user",
    )
    ai = AIProposal(target_account="Expenses:Acme:Supplies", confidence=0.99)
    out = gate.decide(rule=rule, ai=ai)
    assert out.action == GateAction.AUTO_APPLY_RULE
    assert out.chosen_source == "rule"


def test_suggest_band_picks_higher_confidence():
    gate = ConfidenceGate()
    rule = RuleProposal(rule_id=2, target_account="Expenses:A", confidence=0.72)
    ai = AIProposal(target_account="Expenses:B", confidence=0.88)
    out = gate.decide(rule=rule, ai=ai)
    assert out.action == GateAction.REVIEW_WITH_SUGGESTION
    assert out.chosen_source == "ai"
    assert out.chosen_target == "Expenses:B"


def test_below_suggest_threshold_is_review_fixme():
    gate = ConfidenceGate()
    ai = AIProposal(target_account="Expenses:X", confidence=0.5)
    out = gate.decide(rule=None, ai=ai)
    assert out.action == GateAction.REVIEW_FIXME


def test_match_gate_auto_links_when_clear():
    gate = ConfidenceGate()
    ranking = MatchRanking(
        best_match_hash="hash-A",
        confidence=0.93,
        runners_up=(("hash-B", 0.4),),
    )
    assert gate.decide_match(ranking=ranking, candidates_present=True) == GateAction.AUTO_LINK


def test_match_gate_ambiguous_when_runnerup_too_high():
    gate = ConfidenceGate()
    ranking = MatchRanking(
        best_match_hash="hash-A",
        confidence=0.92,
        runners_up=(("hash-B", 0.61),),
    )
    assert gate.decide_match(ranking=ranking, candidates_present=True) == GateAction.REVIEW_AMBIGUOUS


def test_match_gate_orphan_when_no_candidates():
    gate = ConfidenceGate()
    assert gate.decide_match(ranking=None, candidates_present=False) == GateAction.REVIEW_ORPHAN


# AI-AGENT.md Phase 2 hard gates — Income never auto-applies.
# These assertions can't silently regress: income misattribution is
# a tax problem (wrong entity pays the tax; unexplained IRS deposits)
# so every income decision must be a human call regardless of
# confidence, source, or rule provenance.


def test_income_target_ai_never_auto_applies():
    gate = ConfidenceGate()
    ai = AIProposal(
        target_account="Income:Acme:Sales", confidence=0.99,
    )
    out = gate.decide(rule=None, ai=ai)
    assert out.action == GateAction.REVIEW_WITH_SUGGESTION
    assert out.chosen_source == "ai"
    assert out.chosen_target == "Income:Acme:Sales"


def test_income_target_user_rule_never_auto_applies():
    """Even a user-created rule at 1.0 confidence stays in review
    when the target is an Income account."""
    gate = ConfidenceGate()
    rule = RuleProposal(
        rule_id=9,
        target_account="Income:Personal:Consulting",
        confidence=1.0,
        created_by="user",
    )
    out = gate.decide(rule=rule, ai=None)
    assert out.action == GateAction.REVIEW_WITH_SUGGESTION
    assert out.chosen_source == "rule"


def test_non_income_ai_above_threshold_still_routes_to_review():
    """Post-workstream-A sanity check: Expenses targets no longer
    auto-apply on AI alone either. The only auto-apply path is a
    user-created rule; the Income gate matters for rule+AI cases,
    but AI-only routes to review across the board."""
    gate = ConfidenceGate()
    ai = AIProposal(
        target_account="Expenses:Acme:Supplies", confidence=0.99,
    )
    out = gate.decide(rule=None, ai=ai)
    assert out.action == GateAction.REVIEW_WITH_SUGGESTION


def test_property_ai_never_auto_applies_regardless_of_inputs():
    """Property-style sweep. Across rule-present/absent × AI
    confidence × intercompany × income target, GateAction.AUTO_APPLY_AI
    is NEVER returned. If a later edit re-adds the branch this
    fails loudly."""
    gate = ConfidenceGate()
    ai_confidences = [0.0, 0.5, 0.9, 0.95, 0.99, 1.0]
    rule_present_options = [None, "user", "ai"]
    intercompany_options = [False, True]
    target_options = [
        "Expenses:Acme:Supplies",
        "Income:Acme:Sales",
    ]

    seen_actions: set[GateAction] = set()
    for ai_conf in ai_confidences:
        for rule_origin in rule_present_options:
            for intercompany in intercompany_options:
                for target in target_options:
                    rule = None
                    if rule_origin is not None:
                        rule = RuleProposal(
                            rule_id=1,
                            target_account=target,
                            confidence=0.99,
                            created_by=rule_origin,
                        )
                    ai = AIProposal(
                        target_account=target,
                        confidence=ai_conf,
                        intercompany_flag=intercompany,
                    )
                    out = gate.decide(rule=rule, ai=ai)
                    assert out.action != GateAction.AUTO_APPLY_AI, (
                        f"AUTO_APPLY_AI returned for "
                        f"ai_conf={ai_conf} rule_origin={rule_origin} "
                        f"intercompany={intercompany} target={target!r}"
                    )
                    seen_actions.add(out.action)

    # Guardrail: the sweep should surface both auto-apply (via a
    # user-rule at 0.99 on a non-Income, non-intercompany target)
    # and review outcomes. If the sweep collapses to one action the
    # test has gone blind.
    assert GateAction.AUTO_APPLY_RULE in seen_actions
    assert GateAction.REVIEW_WITH_SUGGESTION in seen_actions
