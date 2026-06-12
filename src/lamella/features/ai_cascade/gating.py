# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


# Briefing thresholds (INSTRUCTIONS.md ~L229-256). Keep these here — this
# is the single source of truth. Don't re-encode them in classify / match.
DEFAULT_AUTO_APPLY_THRESHOLD = 0.95
DEFAULT_SUGGEST_THRESHOLD = 0.70

# Receipt match gate (briefing ~L272-280).
DEFAULT_MATCH_PRIMARY_THRESHOLD = 0.90
DEFAULT_MATCH_RUNNERUP_CEILING = 0.60


class GateAction(str, Enum):
    AUTO_APPLY_RULE = "auto_apply_rule"
    AUTO_APPLY_AI = "auto_apply_ai"
    REVIEW_WITH_SUGGESTION = "review_with_suggestion"
    REVIEW_FIXME = "review_fixme"

    # Receipt-match parallel gate.
    AUTO_LINK = "auto_link"
    REVIEW_AMBIGUOUS = "review_ambiguous"
    REVIEW_ORPHAN = "review_orphan"


@dataclass(frozen=True)
class RuleProposal:
    rule_id: int
    target_account: str
    confidence: float
    source: str = "rule"
    # For `auto_apply` we require a user-created rule; AI-created rules
    # only suggest, even at confidence=1.0. See ConfidenceGate.decide().
    created_by: str = "user"


@dataclass(frozen=True)
class AIProposal:
    target_account: str
    confidence: float
    reasoning: str | None = None
    decision_id: int | None = None
    source: str = "ai"
    # Phase G4 — when the AI sees evidence that this charge is on
    # the wrong entity's card (merchant history skews the other
    # way, an active note declares a card override, narration
    # points at a different entity), it flags intercompany so the
    # gate forces human review regardless of confidence.
    intercompany_flag: bool = False
    owning_entity: str | None = None
    # Two-agent cascade. When the primary model returned a
    # low-confidence answer and the call was retried with a stronger
    # fallback model, `escalated_from` is set to the primary model
    # id. Audit trail + UI can surface "answered by Opus (escalated
    # from Haiku, conf 0.55→0.82)".
    escalated_from: str | None = None


@dataclass(frozen=True)
class GateOutcome:
    action: GateAction
    chosen_target: str | None
    chosen_source: str | None  # "rule" | "ai" | None
    rule: RuleProposal | None
    ai: AIProposal | None


@dataclass(frozen=True)
class MatchRanking:
    """AI's ranking of candidate transactions for a receipt."""

    best_match_hash: str | None
    confidence: float
    runners_up: tuple[tuple[str, float], ...]  # (hash, score)
    reasoning: str | None = None
    alternate_date_hypothesis: str | None = None
    decision_id: int | None = None

    @property
    def top_runner_up(self) -> float:
        return self.runners_up[0][1] if self.runners_up else 0.0


class ConfidenceGate:
    """Pure routing logic — no I/O, no DB. Deterministic given inputs.

    Two gates live here:

    * `decide(rule, ai)` — for classification (FIXME → target account).
    * `decide_match(...)` — for receipt disambiguation.
    """

    def __init__(
        self,
        *,
        auto_apply_threshold: float = DEFAULT_AUTO_APPLY_THRESHOLD,
        suggest_threshold: float = DEFAULT_SUGGEST_THRESHOLD,
        match_primary_threshold: float = DEFAULT_MATCH_PRIMARY_THRESHOLD,
        match_runnerup_ceiling: float = DEFAULT_MATCH_RUNNERUP_CEILING,
    ):
        self.auto_apply_threshold = auto_apply_threshold
        self.suggest_threshold = suggest_threshold
        self.match_primary_threshold = match_primary_threshold
        self.match_runnerup_ceiling = match_runnerup_ceiling

    def decide(
        self,
        *,
        rule: RuleProposal | None,
        ai: AIProposal | None,
    ) -> GateOutcome:
        # Phase G4 — hard gate: when the AI flags intercompany, the
        # outcome NEVER auto-applies, regardless of confidence.
        # Wrong-card situations require a four-leg override via the
        # review UI; a two-leg auto-apply would produce incorrect
        # entity-level books (see §6.5 of LEDGER_LAYOUT.md).
        intercompany = ai is not None and ai.intercompany_flag
        # AI-AGENT.md Phase 2 hard gate — Income targets NEVER
        # auto-apply, regardless of confidence, source (rule or AI),
        # or who created the rule. Misattributed income is a tax
        # problem (wrong entity pays the tax, or the IRS sees
        # unexplained deposits), not a bookkeeping annoyance; the
        # bar has to be a human decision every time. Applies both
        # to rule-based and direct-AI outcomes.
        income_target = _targets_income(ai, rule)
        hard_review_only = intercompany or income_target
        # A user-created rule at ≥ auto_apply can auto-apply. This is
        # the only auto-apply path the gate emits for classification —
        # the tier-2 "pattern-match from prior user confirmations" path
        # docs/specs/AI-CLASSIFICATION.md prescribes. AI proposals never
        # self-promote; they always fall through to the suggestion
        # band below, where the user's click-accept is the event that
        # becomes the next classification's context.
        if (
            rule is not None
            and rule.created_by == "user"
            and rule.confidence >= self.auto_apply_threshold
            and not hard_review_only
        ):
            return GateOutcome(
                action=GateAction.AUTO_APPLY_RULE,
                chosen_target=rule.target_account,
                chosen_source="rule",
                rule=rule,
                ai=ai,
            )

        # Fall through to suggestion band. A high-confidence AI
        # proposal lands here as REVIEW_WITH_SUGGESTION — one click
        # to accept in the review UI, which then creates a user-rule
        # via learn_from_decision so the next N similar rows auto-
        # apply without another LLM call.
        best_source: str | None = None
        best_target: str | None = None
        best_conf = 0.0
        if rule is not None and rule.confidence > best_conf:
            best_conf = rule.confidence
            best_source = "rule"
            best_target = rule.target_account
        if ai is not None and ai.confidence > best_conf:
            best_conf = ai.confidence
            best_source = "ai"
            best_target = ai.target_account

        if best_source is not None and best_conf >= self.suggest_threshold:
            return GateOutcome(
                action=GateAction.REVIEW_WITH_SUGGESTION,
                chosen_target=best_target,
                chosen_source=best_source,
                rule=rule,
                ai=ai,
            )

        return GateOutcome(
            action=GateAction.REVIEW_FIXME,
            chosen_target=None,
            chosen_source=None,
            rule=rule,
            ai=ai,
        )

    def decide_match(
        self,
        *,
        ranking: MatchRanking | None,
        candidates_present: bool,
    ) -> GateAction:
        if not candidates_present:
            return GateAction.REVIEW_ORPHAN
        if ranking is None or ranking.best_match_hash is None:
            return GateAction.REVIEW_AMBIGUOUS
        if (
            ranking.confidence >= self.match_primary_threshold
            and ranking.top_runner_up < self.match_runnerup_ceiling
        ):
            return GateAction.AUTO_LINK
        return GateAction.REVIEW_AMBIGUOUS


def _targets_income(
    ai: AIProposal | None, rule: RuleProposal | None,
) -> bool:
    """True when either proposal points at an ``Income:*`` account.
    Hard gate per AI-AGENT.md Phase 2 — income attribution is a tax
    decision, never auto-applied."""
    for p in (ai, rule):
        if p is None:
            continue
        acct = getattr(p, "target_account", "") or ""
        if acct.split(":", 1)[0] == "Income":
            return True
    return False


def pick_best_ai(
    proposals: Iterable[AIProposal],
) -> AIProposal | None:
    best: AIProposal | None = None
    for p in proposals:
        if best is None or p.confidence > best.confidence:
            best = p
    return best
