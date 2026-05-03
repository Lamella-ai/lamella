# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Rule mining from ledger history — NEXTGEN.md Phase E3.

Scan historical ledger transactions, aggregate by (normalized
payee → target account) pairs, and propose directional rules for
pairs that occur frequently enough. The user reviews each
proposal before it commits to ``connector_rules.bean`` via the
existing ``rule_writer``.

Per the rules-are-directional philosophy
(``feedback_rules_directional.md``), these are **signals the AI
consumes alongside other context**, not hard overrides. The
mining surface exists so the user isn't asked to hand-write
rules for patterns their ledger has already demonstrated.

Algorithm:

1. Walk every ledger ``Transaction`` via ``LedgerReader``.
2. For each txn, pair the normalized payee/description with every
   non-source posting account (i.e., the "where did the money
   end up" accounts: expenses, income, equity). Source-side
   postings (Assets, Liabilities) are the card/account side, not
   the classification target.
3. Aggregate by ``(normalized_payee, target_account)``. Count
   occurrences.
4. For each distinct ``normalized_payee``, compute the dominant
   target account (mode). Emit a proposal when:
   * total occurrences of that payee ≥ ``min_support``
   * the dominant account's share ≥ ``min_confidence``
5. Rank proposals by support descending.

Non-goals:

* AI-assisted pattern expansion (deferred).
* Auto-commit of high-confidence proposals (the user ALWAYS
  reviews; this honors the directional-rules feedback).
"""
from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from beancount.core.data import Transaction

from lamella.core.beancount_io.reader import LedgerReader

log = logging.getLogger(__name__)

__all__ = [
    "MinedRule",
    "mine_rules",
]


# Account roots that represent the "where did the money come from / go to"
# side — classification targets. Liabilities is included for CC-payment
# / loan-servicing pattern mining (AI-AGENT.md Phase 2) — the gate
# never auto-applies Income rules regardless, so including Income is
# safe (it still produces suggestions without unsafe auto-apply).
# Assets deliberately excluded for now: pure Asset↔Asset transfer
# patterns are highly user-specific and would clutter the suggestion
# surface. Revisit if real ledger data shows value.
_TARGET_ROOTS: frozenset[str] = frozenset({
    "Expenses", "Income", "Liabilities", "Equity",
})

# Leaf names that indicate "the user hasn't decided yet" or "catch-
# all bucket I should split." Promoting a rule whose target is one
# of these just hard-codes the undecided state into auto-apply,
# which is the opposite of useful. Filter these out of mined
# proposals.
_UNDECIDED_LEAVES: frozenset[str] = frozenset({
    "FIXME", "UNKNOWN", "UNCATEGORIZED", "UNCLASSIFIED",
    "OTHEREXPENSES",  # optional — treated as a catch-all
    "MISC", "MISCELLANEOUS", "GENERAL",
})


def _is_undecided_target(account_path: str) -> bool:
    if not account_path:
        return True
    leaf = account_path.rsplit(":", 1)[-1].upper()
    return leaf in _UNDECIDED_LEAVES


_WS = re.compile(r"\s+")


def _normalize_payee(text: str | None) -> str:
    if not text:
        return ""
    return _WS.sub(" ", text.lower()).strip()


@dataclass(frozen=True)
class MinedRule:
    """One directional-rule proposal from ledger history.

    ``sample_accounts`` lists the most-common target accounts
    observed for this payee so the user can see alternatives
    alongside the dominant choice. This is the directional-rules
    principle in surface form: the rule is a prior, not an
    override, and showing the runner-ups makes the prior's
    strength visible.
    """
    normalized_payee: str
    proposed_account: str
    support: int              # txns observed
    confidence: float         # share of txns that picked proposed_account
    alternatives: tuple[tuple[str, int], ...] = ()   # (account, count)


def mine_rules(
    reader: LedgerReader,
    *,
    min_support: int = 5,
    min_confidence: float = 0.6,
    max_proposals: int = 100,
) -> list[MinedRule]:
    """Walk the ledger and propose directional rules for
    frequently-repeated (payee → target account) patterns.

    Returns proposals ranked by support descending, confidence
    descending, then alphabetically for determinism.
    """
    loaded = reader.load(force=True)

    # payee → Counter(target_account)
    observed: dict[str, Counter[str]] = defaultdict(Counter)

    for entry in loaded.entries:
        if not isinstance(entry, Transaction):
            continue
        payee_src = entry.payee or entry.narration or ""
        norm = _normalize_payee(payee_src)
        if not norm:
            continue
        for posting in entry.postings or []:
            account = posting.account or ""
            if not account:
                continue
            root = account.split(":", 1)[0]
            if root not in _TARGET_ROOTS:
                continue
            observed[norm][account] += 1

    proposals: list[MinedRule] = []
    for payee, counter in observed.items():
        total = sum(counter.values())
        if total < min_support:
            continue
        # Dominant account + alternatives.
        ranked = counter.most_common()
        dominant_account, dominant_count = ranked[0]
        share = dominant_count / total
        if share < min_confidence:
            continue
        # Skip proposals whose dominant target is an "undecided"
        # or catch-all bucket. Promoting a rule that auto-applies
        # FIXME or Uncategorized hard-codes the undecided state —
        # the opposite of what classify should do. These patterns
        # still show up as priors the AI sees at classify time;
        # we just don't surface them as *promotion* candidates.
        if _is_undecided_target(dominant_account):
            continue
        alternatives = tuple(ranked[1:5])
        proposals.append(
            MinedRule(
                normalized_payee=payee,
                proposed_account=dominant_account,
                support=dominant_count,
                confidence=round(share, 3),
                alternatives=alternatives,
            )
        )

    proposals.sort(
        key=lambda p: (-p.support, -p.confidence, p.normalized_payee),
    )
    return proposals[:max_proposals]
