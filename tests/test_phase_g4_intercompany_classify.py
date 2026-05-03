# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Phase G4 — intercompany awareness in classification."""
from __future__ import annotations

from lamella.features.ai_cascade.gating import (
    AIProposal,
    ConfidenceGate,
    GateAction,
    RuleProposal,
)


class TestGateNeverAutoAppliesIntercompany:
    def test_high_confidence_ai_with_intercompany_flag_goes_to_review(self):
        """The whole point of G4: a high-confidence AI classification
        that crosses entity boundaries MUST go to human review for
        the four-leg override, never auto-apply."""
        gate = ConfidenceGate()
        ai = AIProposal(
            target_account="Expenses:WidgetCo:Supplies",
            confidence=0.99,                   # well past auto_apply_threshold
            reasoning="merchant always goes to WidgetCo",
            intercompany_flag=True,
            owning_entity="WidgetCo",
        )
        outcome = gate.decide(rule=None, ai=ai)
        assert outcome.action != GateAction.AUTO_APPLY_AI
        # With confidence >= suggest_threshold it surfaces as a
        # suggestion, giving the review UI enough context to render
        # the intercompany prompt and produce the four-leg override.
        assert outcome.action == GateAction.REVIEW_WITH_SUGGESTION

    def test_high_confidence_ai_without_intercompany_routes_to_review(self):
        """Post-workstream-A: a high-confidence AI proposal without
        the intercompany flag still routes to review — the gate no
        longer emits AUTO_APPLY_AI. The non-intercompany case means
        the Phase G4 hard gate doesn't fire, but the outcome is
        REVIEW_WITH_SUGGESTION either way (tier-2 — the user's
        click-accept is what promotes the suggestion)."""
        gate = ConfidenceGate()
        ai = AIProposal(
            target_account="Expenses:Acme:Supplies",
            confidence=0.99,
            reasoning="clean card match",
            intercompany_flag=False,
        )
        outcome = gate.decide(rule=None, ai=ai)
        assert outcome.action == GateAction.REVIEW_WITH_SUGGESTION
        assert outcome.chosen_source == "ai"

    def test_high_confidence_rule_intercompany_still_blocked(self):
        """A user rule auto-apply is also blocked when the AI says
        intercompany — rules don't get to short-circuit a wrong-card
        situation either."""
        gate = ConfidenceGate()
        rule = RuleProposal(
            rule_id=1,
            target_account="Expenses:Acme:Supplies",
            confidence=1.0,
            created_by="user",
        )
        ai = AIProposal(
            target_account="Expenses:WidgetCo:Supplies",
            confidence=0.95,
            intercompany_flag=True,
            owning_entity="WidgetCo",
        )
        outcome = gate.decide(rule=rule, ai=ai)
        assert outcome.action != GateAction.AUTO_APPLY_RULE
        assert outcome.action != GateAction.AUTO_APPLY_AI


class TestCrossEntityWhitelist:
    def test_all_expense_accounts_by_entity_groups_correctly(self):
        """The cross-entity whitelist helper groups Expenses: accounts
        by their second path segment, so the prompt can render them
        under entity headers and let the AI pick across groups."""
        from beancount.core import data as bdata
        from datetime import date
        from lamella.features.ai_cascade.context import all_expense_accounts_by_entity

        entries = [
            bdata.Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:Acme:Supplies",
                currencies=["USD"], booking=None,
            ),
            bdata.Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:Acme:Meals",
                currencies=["USD"], booking=None,
            ),
            bdata.Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:WidgetCo:Supplies",
                currencies=["USD"], booking=None,
            ),
            bdata.Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:Personal:Groceries",
                currencies=["USD"], booking=None,
            ),
            bdata.Open(
                meta={}, date=date(2020, 1, 1),
                account="Expenses:Acme:FIXME",  # must be excluded
                currencies=["USD"], booking=None,
            ),
        ]
        grouped = all_expense_accounts_by_entity(entries)
        assert set(grouped.keys()) == {"Acme", "WidgetCo", "Personal"}
        assert "Expenses:Acme:Supplies" in grouped["Acme"]
        assert "Expenses:Acme:Meals" in grouped["Acme"]
        # FIXME must be filtered.
        assert not any("FIXME" in a for a in grouped["Acme"])
        assert grouped["WidgetCo"] == ["Expenses:WidgetCo:Supplies"]


class TestClassifyResponseSchema:
    def test_defaults_match_legacy_behavior(self):
        """Legacy prompts without the G4 fields still validate — new
        fields default False/None so older cached decisions don't
        break."""
        from lamella.features.ai_cascade.classify import ClassifyResponse
        r = ClassifyResponse(
            target_account="Expenses:Acme:Supplies",
            confidence=0.9,
            reasoning="x",
        )
        assert r.intercompany_flag is False
        assert r.owning_entity is None

    def test_intercompany_fields_parsed(self):
        from lamella.features.ai_cascade.classify import ClassifyResponse
        r = ClassifyResponse.model_validate({
            "target_account": "Expenses:WidgetCo:Supplies",
            "confidence": 0.92,
            "reasoning": "merchant historically on WidgetCo cards",
            "intercompany_flag": True,
            "owning_entity": "WidgetCo",
        })
        assert r.intercompany_flag is True
        assert r.owning_entity == "WidgetCo"
