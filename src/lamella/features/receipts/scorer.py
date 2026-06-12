# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Direction-invariant scorer for document <-> transaction matching.

ADR-0063 extracts the previously-inline scoring cascade from
``txn_matcher.find_document_candidates`` into this module so the
**reverse** direction (document --> ledger transaction) can use the
exact same logic without copy-paste. The forward and reverse paths
share two constants:

  * ``AUTO_LINK_THRESHOLD`` — auto-link gate (0.90).
  * ``REVIEW_THRESHOLD``    — sub-threshold review floor (0.60).

Both are re-exported here as the **single source of truth** for both
directions. ``auto_match.py`` and ``staging_review`` import from this
module; if either threshold needs tuning a future change is one edit.

The scoring formula is **direction-invariant** by design. Comparing
a document's amount against a transaction's amount is the same
operation regardless of which side initiates the search. Tested as
an invariant in ``tests/test_scorer_direction_invariance.py``.

See ``docs/specs/MATCHING_CONFIDENCE.md`` for the model + tuning
guide.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Literal

# ─── Shared thresholds ────────────────────────────────────────────────
#
# These are imported by both forward (``txn_matcher.find_document_candidates``
# -> ``auto_match.sweep_recent``) and reverse (``find_ledger_candidates``
# -> ``auto_link_unlinked_documents``) paths. Per ADR-0063 §2 the two
# directions MUST share a single constant so a tuning change updates
# both atomically.

AUTO_LINK_THRESHOLD: float = 0.90
"""Score >= this value qualifies a candidate for unattended auto-link."""

REVIEW_THRESHOLD: float = 0.60
"""Score >= this value but < AUTO_LINK_THRESHOLD surfaces for human
review on /inbox (the existing sub-threshold review surface)."""


# ─── Tunable knobs ────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScoringSettings:
    """Tunable parameters for the cascade.

    Defaults match the production tuning in ``txn_matcher.py`` so the
    refactor produces no scoring drift. New deployments inherit this
    set; per-deployment overrides are not exposed as a UI yet. See
    ``docs/specs/MATCHING_CONFIDENCE.md`` for which knob to turn for
    which kind of false positive / negative.
    """

    # Date proximity windows (in days) for the cascade stages.
    tight_window_days: int = 3
    wide_window_days: int = 30

    # Amount fuzz allowance for stage 5 (tax rounding / tip variance).
    fuzzy_cents: int = 50

    # Cascade base scores. These are the per-stage starting weights;
    # final score = base + per-adjustment contributions, capped at 1.0.
    base_amount_tight_date: float = 0.90   # exact total + tight date (3d)
    base_amount_wide_date: float = 0.70    # exact total + wide date (30d)
    base_subtotal_tight_date: float = 0.55  # subtotal exact + tight date
    base_amount_any_date: float = 0.45     # exact total, any date
    base_amount_fuzzy_tight_date: float = 0.40  # ±cents + tight date
    base_correspondent_wide_date: float = 0.55  # correspondent + wide date

    # Per-adjustment contributions on top of the cascade base.
    bump_last_four_match: float = 0.10
    bump_amount_in_content: float = 0.08
    bump_merchant_tokens_two_plus: float = 0.10
    bump_merchant_token_one: float = 0.05

    # Date-delta penalties applied during finalization.
    penalty_date_30d: float = 0.03
    penalty_date_over_30d: float = 0.10


# ─── Result types ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScoreResult:
    """Output of :meth:`Scorer.score`.

    ``total`` is the post-adjustment score, clamped to [0.0, 1.0].
    ``breakdown`` exposes each contribution (base stage, adjustments)
    so the UI can render *why* a pair scored where it did.

    ``verdict``:
      * ``auto_link`` — score >= AUTO_LINK_THRESHOLD
      * ``review``    — REVIEW_THRESHOLD <= score < AUTO_LINK_THRESHOLD
      * ``reject``    — score < REVIEW_THRESHOLD
    """

    total: float
    breakdown: dict[str, float] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()
    verdict: Literal["auto_link", "review", "reject"] = "reject"


@dataclass(frozen=True)
class ScoredLedgerCandidate:
    """A ledger transaction scored as a candidate for an unlinked
    document. Returned by ``find_ledger_candidates``.

    Identity carried via ``txn_hash`` so callers can hand it straight
    to :class:`DocumentLinker.link`. ``score`` and ``reasons`` mirror
    the forward-direction ``ScoredCandidate`` shape.
    """

    txn_hash: str
    txn_date: date
    txn_amount: Decimal
    payee: str | None
    narration: str | None
    score: float
    reasons: tuple[str, ...]


# ─── Token-extraction helpers (extracted from txn_matcher) ────────────

_STOPWORDS = frozenset(
    {
        "the", "inc", "llc", "co", "corp", "ltd", "company", "store",
        "payment", "payments", "purchase", "invoice", "receipt", "a", "an",
        "and", "of", "for", "from", "to", "usa", "us", "com", "net", "www",
        "ach", "ref", "ebill", "ebilling", "online", "transfer", "card",
        "acct", "auth", "authorized", "fee", "form", "subscription", "pay",
        "new", "west", "east", "north", "south", "ave", "blvd", "rd", "st",
        "amount", "paid", "thank", "you", "pmts", "pmt", "wf", "sa", "ca",
        "ny", "tx", "il", "co", "az", "nv", "wa", "or", "fl", "ga",
    }
)

_TOKEN_RE = re.compile(r"[A-Za-z0-9]{4,}")


def merchant_tokens(text: str | None) -> set[str]:
    """Extract merchant-signal tokens from arbitrary text.

    Same rules as ``txn_matcher._merchant_tokens``: lowercase, alnum
    runs >= 4 chars, drop common noise. Exposed at module scope so the
    reverse-direction caller (``find_ledger_candidates``) can build
    the same token set the forward direction does.
    """
    if not text:
        return set()
    words = _TOKEN_RE.findall(text.lower())
    return {w for w in words if w not in _STOPWORDS}


# ─── The Scorer ───────────────────────────────────────────────────────


class Scorer:
    """Direction-free pair scorer.

    The :meth:`score` method takes a ``(doc, txn)`` pair regardless of
    which side initiated the search and returns a :class:`ScoreResult`.
    Direction-invariance is a tested invariant: scoring
    ``(doc, txn)`` returns the same total as scoring ``(txn, doc)``
    with the same fields swapped.

    This class is intentionally **stateless** — ``ScoringSettings`` is
    the only carrier of policy. Callers may construct one Scorer per
    request or share a module-level singleton; both are equivalent.
    """

    AUTO_LINK_THRESHOLD: float = AUTO_LINK_THRESHOLD
    REVIEW_THRESHOLD: float = REVIEW_THRESHOLD

    def __init__(self, settings: ScoringSettings | None = None):
        self.settings = settings or ScoringSettings()

    # The thresholds are class-level constants so test code can assert
    # ``Scorer.AUTO_LINK_THRESHOLD is AUTO_LINK_THRESHOLD`` (no
    # duplicate definition).

    def verdict(self, score: float) -> Literal["auto_link", "review", "reject"]:
        if score >= self.AUTO_LINK_THRESHOLD:
            return "auto_link"
        if score >= self.REVIEW_THRESHOLD:
            return "review"
        return "reject"

    def score(
        self,
        *,
        doc_date: date | None,
        doc_total_cents: int | None,
        doc_currency: str | None,
        doc_vendor: str | None,
        doc_doctype: str | None,
        txn_date: date | None,
        txn_amount_cents: int | None,
        txn_currency: str | None,
        txn_payee: str | None,
        txn_description: str | None,
        doc_subtotal_cents: int | None = None,
        doc_content_excerpt: str | None = None,
        doc_correspondent: str | None = None,
        doc_last_four: str | None = None,
        txn_last_four: str | None = None,
    ) -> ScoreResult:
        """Score a single (document, transaction) pair.

        Direction-invariant: swap the doc_* and txn_* arguments and
        the ``total`` is unchanged. The cascade picks the strongest
        applicable stage as the base; per-adjustment contributions
        are additive and clamped at 1.0.

        ``*_cents`` arguments take the absolute value internally so
        the sign convention (debit vs credit) doesn't matter.
        """
        s = self.settings
        breakdown: dict[str, float] = {}
        reasons: list[str] = []

        # Doctype-incompat penalty (statement / tax) is enforced by
        # the SQL-level exclusion in find_*_candidates; if a caller
        # somehow pushes an excluded doc through the scorer directly
        # we still hand back a 0.0.
        if doc_doctype and doc_doctype.strip().lower() in {"statement", "tax"}:
            return ScoreResult(
                total=0.0,
                breakdown={"doctype_excluded": 0.0},
                reasons=("doctype excluded",),
                verdict="reject",
            )

        # Currency mismatch is a hard reject; comparing $5 to €5 is
        # not a 1:1 amount match no matter how clean every other signal.
        if doc_currency and txn_currency and doc_currency != txn_currency:
            return ScoreResult(
                total=0.0,
                breakdown={"currency_mismatch": 0.0},
                reasons=(f"currency mismatch ({doc_currency} vs {txn_currency})",),
                verdict="reject",
            )

        # ─── Stage 1-5: amount cascade ────────────────────────────
        amt_doc = abs(doc_total_cents) if doc_total_cents is not None else None
        amt_txn = abs(txn_amount_cents) if txn_amount_cents is not None else None
        amt_sub = abs(doc_subtotal_cents) if doc_subtotal_cents is not None else None

        date_delta_days: int | None = None
        if doc_date is not None and txn_date is not None:
            date_delta_days = abs((doc_date - txn_date).days)

        base = 0.0
        # Pick the strongest applicable stage (highest base).
        # Stage 1: total exact + tight date.
        if (
            amt_doc is not None and amt_txn is not None and amt_doc == amt_txn
            and date_delta_days is not None
            and date_delta_days <= s.tight_window_days
        ):
            base = max(base, s.base_amount_tight_date)
            breakdown["amount_tight_date"] = s.base_amount_tight_date
            reasons.append("amount + date")
        # Stage 2: total exact + wide date.
        if (
            amt_doc is not None and amt_txn is not None and amt_doc == amt_txn
            and date_delta_days is not None
            and date_delta_days <= s.wide_window_days
        ):
            if s.base_amount_wide_date > base:
                base = s.base_amount_wide_date
            breakdown.setdefault("amount_wide_date", s.base_amount_wide_date)
            if "amount + date" not in reasons and "amount, wide date" not in reasons:
                reasons.append("amount, wide date")
        # Stage 3: subtotal exact + tight date.
        if (
            amt_sub is not None and amt_txn is not None and amt_sub == amt_txn
            and date_delta_days is not None
            and date_delta_days <= s.tight_window_days
        ):
            if s.base_subtotal_tight_date > base:
                base = s.base_subtotal_tight_date
            breakdown.setdefault("subtotal_tight_date", s.base_subtotal_tight_date)
            if "subtotal + date" not in reasons:
                reasons.append("subtotal + date")
        # Stage 4: total exact, any date.
        if (
            amt_doc is not None and amt_txn is not None and amt_doc == amt_txn
        ):
            if s.base_amount_any_date > base:
                base = s.base_amount_any_date
            breakdown.setdefault("amount_any_date", s.base_amount_any_date)
            if "amount, any date" not in reasons and "amount + date" not in reasons \
                    and "amount, wide date" not in reasons:
                reasons.append("amount, any date")
        # Stage 5: amount within fuzzy tolerance + tight date.
        if (
            amt_doc is not None and amt_txn is not None and amt_doc != amt_txn
            and abs(amt_doc - amt_txn) <= s.fuzzy_cents
            and date_delta_days is not None
            and date_delta_days <= s.tight_window_days
        ):
            if s.base_amount_fuzzy_tight_date > base:
                base = s.base_amount_fuzzy_tight_date
            breakdown.setdefault(
                "amount_fuzzy_tight_date", s.base_amount_fuzzy_tight_date
            )
            cents_dollars = Decimal(s.fuzzy_cents) / Decimal(100)
            reasons.append(f"amount ±${cents_dollars:.2f}")

        # Stage 6: correspondent / vendor exact match + wide date.
        merchant_signal = (
            merchant_tokens(txn_payee) | merchant_tokens(txn_description)
        )
        doc_vendor_text = (doc_vendor or "")
        doc_corr_text = (doc_correspondent or "")
        # Correspondent match is a token from the txn appearing in the
        # doc's correspondent_name (Paperless's authoritative vendor
        # attribution). Used only when correspondent_name is populated.
        corr_lower = doc_corr_text.lower()
        corr_hit = False
        if corr_lower:
            for tok in list(merchant_signal)[:4]:
                if tok in corr_lower:
                    corr_hit = True
                    break
        if (
            corr_hit
            and date_delta_days is not None
            and date_delta_days <= s.wide_window_days
        ):
            if s.base_correspondent_wide_date > base:
                base = s.base_correspondent_wide_date
            breakdown.setdefault(
                "correspondent_wide_date", s.base_correspondent_wide_date
            )
            if "correspondent + wide date" not in reasons:
                reasons.append("correspondent + wide date")

        # ─── Per-adjustment contributions ────────────────────────
        # Last-four hint match (either side).
        last_four_match = False
        if (
            doc_last_four
            and txn_last_four
            and doc_last_four.strip() == txn_last_four.strip()
        ):
            last_four_match = True
        elif doc_last_four and txn_description and doc_last_four in txn_description:
            last_four_match = True
        elif (
            txn_last_four
            and doc_content_excerpt
            and txn_last_four in doc_content_excerpt
        ):
            last_four_match = True
        if last_four_match:
            base = min(1.0, base + s.bump_last_four_match)
            breakdown["last_four"] = s.bump_last_four_match
            reasons.append("last-four match")

        # Amount-in-content boost: the txn's dollar amount appears
        # literally in the doc's OCR text. Direction-invariant: we
        # check both sides' content/description for both sides'
        # amounts.
        if amt_txn is not None and doc_content_excerpt:
            dollars = Decimal(amt_txn) / Decimal(100)
            if str(dollars) in doc_content_excerpt:
                base = min(1.0, base + s.bump_amount_in_content)
                breakdown["amount_in_content"] = s.bump_amount_in_content
                if "amount in content" not in reasons:
                    reasons.append("amount in content")
        elif amt_doc is not None and txn_description:
            dollars = Decimal(amt_doc) / Decimal(100)
            if str(dollars) in txn_description:
                base = min(1.0, base + s.bump_amount_in_content)
                breakdown["amount_in_content"] = s.bump_amount_in_content
                if "amount in content" not in reasons:
                    reasons.append("amount in content")

        # Merchant-token corroboration: tokens shared between the
        # txn payee/description and the doc vendor/correspondent/
        # title/content. Two-or-more bumps more than one.
        doc_text_blob = " ".join(
            x for x in (doc_vendor_text, doc_corr_text, doc_content_excerpt) if x
        ).lower()
        if merchant_signal and doc_text_blob:
            merchant_hits = sum(1 for t in merchant_signal if t in doc_text_blob)
            if merchant_hits >= 2:
                base = min(1.0, base + s.bump_merchant_tokens_two_plus)
                breakdown["merchant_tokens_2plus"] = s.bump_merchant_tokens_two_plus
                if "merchant tokens" not in reasons:
                    reasons.append("merchant tokens")
            elif merchant_hits == 1:
                base = min(1.0, base + s.bump_merchant_token_one)
                breakdown["merchant_token_1"] = s.bump_merchant_token_one
                if "merchant token" not in reasons:
                    reasons.append("merchant token")

        # Date-delta penalty (only if we got a base from a stage that
        # didn't already date-bound it tightly).
        if date_delta_days is not None:
            if date_delta_days == 0:
                if "same day" not in reasons:
                    reasons.append("same day")
            elif date_delta_days <= 3:
                reasons.append(f"{date_delta_days}d off")
            elif date_delta_days <= 30:
                reasons.append(f"{date_delta_days}d off")
                base = max(0.0, base - s.penalty_date_30d)
                breakdown["penalty_date_30d"] = -s.penalty_date_30d
            else:
                reasons.append(f"{date_delta_days}d off")
                base = max(0.0, base - s.penalty_date_over_30d)
                breakdown["penalty_date_over_30d"] = -s.penalty_date_over_30d

        # `other` doctype carries a soft -10% penalty per ADR-0063 §6.
        if doc_doctype and doc_doctype.strip().lower() == "other":
            base = max(0.0, base - 0.10)
            breakdown["doctype_other_penalty"] = -0.10

        total = round(min(1.0, max(0.0, base)), 3)
        # Deduplicate reasons preserving order.
        dedup_reasons = tuple(dict.fromkeys(reasons))
        return ScoreResult(
            total=total,
            breakdown=breakdown,
            reasons=dedup_reasons,
            verdict=self.verdict(total),
        )


# Module-level singleton for callers that don't want to instantiate.
default_scorer = Scorer()


__all__ = [
    "AUTO_LINK_THRESHOLD",
    "REVIEW_THRESHOLD",
    "ScoringSettings",
    "ScoreResult",
    "ScoredLedgerCandidate",
    "Scorer",
    "default_scorer",
    "merchant_tokens",
]
