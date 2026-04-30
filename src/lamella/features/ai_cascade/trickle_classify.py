# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Context-gated trickle classify.

Runs on a schedule (twice/day per main.py) and processes a small,
context-ripe slice of pending FIXMEs. Two sub-tiers — both before
any LLM call — handle the cheap cases:

  Sub-tier 1: pattern-from-neighbors. Vector-search for similar
              already-classified transactions. If ≥3 neighbors
              agree on a single target with similarity ≥ 0.85,
              apply that target via in-place rewrite. No AI cost.

  Sub-tier 2: AI classify. Only fires for FIXMEs that pass the
              direct-evidence gate AND didn't pattern-match. The
              gate criteria mirror docs/specs/AI-CLASSIFICATION.md
              "context-gated trickle":

                - has a linked receipt, OR
                - has memo/payee/narration AND an active project
                  on the txn date, OR
                - has memo/payee/narration AND ≥2 vector neighbors
                  classified to the same target (the
                  "proximity-to-an-existing-group" signal).

              Off-gate rows are left alone — they wait for context
              to accumulate or for the user to run bulk classify.

Capped at TRICKLE_LIMIT_PER_RUN AI calls per invocation. Even at
two runs/day this caps daily LLM spend at ~50 classify calls in
the worst case; in practice the gate filters most rows out.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from beancount.core.data import Transaction

from lamella.features.ai_cascade.bulk_classify import (
    AUTO_APPLY_THRESHOLD,
    _classify_one,
    _collect_fixme_txns,
    _existing_override_hashes,
    _has_active_project,
    _has_linked_receipt,
    _has_memo_signal,
    _matching_user_rule,
)
from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_txn_hash
from lamella.core.config import Settings

log = logging.getLogger(__name__)

# How many AI classify calls the trickle is allowed to make per run.
# Two runs/day × 25 = 50 calls/day worst case.
TRICKLE_LIMIT_PER_RUN = 25

# Pattern-from-neighbors: minimum vector similarity for a neighbor
# to count toward the agreement test, and minimum number of agreeing
# neighbors required to short-circuit the AI call.
PATTERN_MIN_SIMILARITY = 0.85
PATTERN_MIN_AGREEMENT = 3

# AI gate: a memo+neighbors gate counts a row as context-ripe when
# this many vector neighbors agree on the same target at this
# similarity. Looser than the pattern-match auto-apply bar — at this
# tier the AI still gets to weigh the evidence.
AI_GATE_MIN_NEIGHBORS = 2
AI_GATE_NEIGHBOR_SIMILARITY = 0.55

# Re-classify cooldown: after the AI has produced a proposal for
# this txn, don't pay for another classify call for this many days.
# A pending proposal is already sitting in /review/staged for the
# user to accept; running again before they look at it just burns
# tokens for the same answer. The cooldown is long enough that
# meaningful context has time to arrive (a receipt being linked, a
# day-note being added) but short enough that a stale proposal
# eventually gets refreshed.
RECLASSIFY_COOLDOWN_DAYS = 7


@dataclass
class TrickleResult:
    scanned: int = 0
    skipped_has_override: int = 0
    skipped_off_gate: int = 0
    skipped_cooldown: int = 0
    pattern_applied: int = 0
    rule_applied: int = 0
    ai_applied: int = 0
    ai_queued: int = 0
    ai_called: int = 0
    errors: list[str] = field(default_factory=list)


def _has_recent_ai_decision(
    conn: sqlite3.Connection,
    txn_hash: str,
    *,
    days: int,
) -> bool:
    """Did we already classify this txn via AI in the last ``days``?
    Used to avoid re-paying for the same proposal on every trickle
    run while the user hasn't yet reviewed it."""
    if conn is None:
        return False
    try:
        row = conn.execute(
            """
            SELECT 1 FROM ai_decisions
             WHERE decision_type = 'classify_txn'
               AND input_ref = ?
               AND decided_at >= datetime('now', ?)
             LIMIT 1
            """,
            (txn_hash, f"-{int(days)} days"),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    return row is not None


def _vector_neighbors(
    *,
    conn: sqlite3.Connection,
    entries: list,
    txn: Transaction,
    settings: Settings,
    fixme_account: str,
):
    """Return up to 8 vector neighbors for this txn. Each neighbor
    is a VectorMatch (raw similarity exposed). Quiet on error —
    trickle never wants to crash on a stale index."""
    if not getattr(settings, "ai_vector_search_enabled", True):
        return []
    try:
        from lamella.features.ai_cascade.decisions import DecisionsLog
        from lamella.features.ai_cascade.vector_index import (
            VectorIndex,
            VectorUnavailable,
        )
    except Exception:  # noqa: BLE001
        return []
    needle_parts = [
        getattr(txn, "payee", None) or "",
        getattr(txn, "narration", None) or "",
    ]
    needle = " ".join(p for p in needle_parts if p).strip()
    if not needle:
        return []
    fixme_root = (fixme_account.split(":", 1) or ["Expenses"])[0]
    try:
        idx = VectorIndex(conn)
        # Don't trigger a build here — trickle is a background task
        # that should not stall on heavy index work. Query against
        # the existing rows; the boot-time refresh task keeps the
        # index fresh.
        ref_date = (
            txn.date if isinstance(txn.date, date)
            else date.fromisoformat(str(txn.date))
        )
        return idx.query(
            needle=needle,
            reference_date=ref_date,
            limit=8,
            target_roots=(fixme_root,),
        )
    except Exception:  # noqa: BLE001
        return []


def _agreeing_target(
    neighbors,
    *,
    min_similarity: float,
    min_count: int,
) -> str | None:
    """If ≥ min_count neighbors with similarity ≥ min_similarity
    share a single target_account (and no other target has > 1
    matching neighbor), return that target. Otherwise None."""
    if not neighbors:
        return None
    counts: Counter[str] = Counter()
    for n in neighbors:
        if n.similarity < min_similarity:
            continue
        target = (n.target_account or "").strip()
        if not target:
            continue
        if target.split(":", 1)[-1].upper() == "FIXME":
            continue
        counts[target] += 1
    if not counts:
        return None
    top, top_n = counts.most_common(1)[0]
    if top_n < min_count:
        return None
    runner_up = counts.most_common(2)[1][1] if len(counts) > 1 else 0
    # Require the leader to dominate — same target wins outright
    # rather than tying with another popular target.
    if runner_up >= top_n:
        return None
    return top


def _is_context_ripe(
    *,
    txn: Transaction,
    fixme_account: str,
    conn: sqlite3.Connection,
    neighbors,
) -> bool:
    """Direct-evidence gate. Returns True iff this row is context-
    ripe enough to spend an AI call on. Mirrors the criteria in
    docs/specs/AI-CLASSIFICATION.md "Scheduling — context-gated trickle."
    """
    target_hash = compute_txn_hash(txn)
    if _has_linked_receipt(target_hash, conn):
        return True
    if _has_memo_signal(txn) and _has_active_project(txn, conn):
        return True
    if _has_memo_signal(txn):
        # "Proximity to a classified group" — at least
        # AI_GATE_MIN_NEIGHBORS neighbors agree on a target at
        # AI_GATE_NEIGHBOR_SIMILARITY+. The agreement target can be
        # the same one pattern-from-neighbors would have promoted,
        # but at a looser bar — pattern-match runs first; if it
        # didn't fire, the AI gets the row with these neighbors as
        # direct anchors.
        agreed = _agreeing_target(
            neighbors,
            min_similarity=AI_GATE_NEIGHBOR_SIMILARITY,
            min_count=AI_GATE_MIN_NEIGHBORS,
        )
        if agreed is not None:
            return True
    return False


def _apply_pattern(
    *,
    txn: Transaction,
    fixme_account: str,
    target_account: str,
    settings: Settings,
    conn: sqlite3.Connection,
) -> tuple[bool, str | None]:
    """Rewrite the FIXME posting in place to ``target_account``.
    Returns (applied, error_message)."""
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.core.rewrite.txn_inplace import (
        InPlaceRewriteError,
        rewrite_fixme_to_account,
    )
    from lamella.features.rules.overrides import OverrideWriter

    target_hash = compute_txn_hash(txn)
    meta = getattr(txn, "meta", None) or {}
    src_file = meta.get("filename")
    src_lineno = meta.get("lineno")
    if not src_file or src_lineno is None:
        return False, "no filename/lineno meta"

    fixme_signed = None
    for p in txn.postings or ():
        if (p.account or "") == fixme_account and p.units \
                and p.units.number is not None:
            fixme_signed = Decimal(p.units.number)
            break
    if fixme_signed is None:
        return False, "no signed amount"

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        try:
            writer.rewrite_without_hash(target_hash)
        except BeanCheckError:
            return False, "override-strip blocked"
        rewrite_fixme_to_account(
            source_file=Path(src_file),
            line_number=int(src_lineno),
            old_account=fixme_account,
            new_account=target_account,
            expected_amount=fixme_signed,
            ledger_dir=settings.ledger_dir,
            main_bean=settings.ledger_main,
        )
    except InPlaceRewriteError as exc:
        return False, f"in-place refused: {exc}"
    except Exception as exc:  # noqa: BLE001
        return False, f"in-place failed: {exc}"
    return True, None


async def run_trickle(
    *,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    settings: Settings,
    ai_service: Any,
    limit_per_run: int = TRICKLE_LIMIT_PER_RUN,
) -> TrickleResult:
    """Schedule entrypoint. See module docstring."""
    from lamella.features.rules.service import RuleService

    result = TrickleResult()
    entries = list(reader.load().entries)
    fixmes = _collect_fixme_txns(entries, conn=conn)
    if not fixmes:
        return result
    existing_overrides = _existing_override_hashes(
        settings.connector_overrides_path
    )
    rules_cache = list(RuleService(conn).iter_active())

    ai_calls_made = 0
    for txn, fixme_acct, amt, currency in fixmes:
        target_hash = compute_txn_hash(txn)
        if target_hash in existing_overrides:
            result.skipped_has_override += 1
            continue
        result.scanned += 1

        # User-rule short-circuit (free, no LLM call).
        rule_match = _matching_user_rule(
            txn=txn, fixme_account=fixme_acct, abs_amount=amt,
            rules=rules_cache,
            auto_apply_threshold=AUTO_APPLY_THRESHOLD,
        )
        if rule_match is not None:
            applied, err = _apply_pattern(
                txn=txn, fixme_account=fixme_acct,
                target_account=rule_match.target_account,
                settings=settings, conn=conn,
            )
            if applied:
                result.rule_applied += 1
                log.info(
                    "trickle: rule auto-apply %s → %s",
                    target_hash[:8], rule_match.target_account,
                )
            else:
                result.errors.append(
                    f"{target_hash[:8]} rule rewrite refused: {err}"
                )
            continue

        # Sub-tier 1: pattern-from-neighbors (free, no LLM call).
        neighbors = _vector_neighbors(
            conn=conn, entries=entries, txn=txn,
            settings=settings, fixme_account=fixme_acct,
        )
        agreed = _agreeing_target(
            neighbors,
            min_similarity=PATTERN_MIN_SIMILARITY,
            min_count=PATTERN_MIN_AGREEMENT,
        )
        if agreed is not None:
            applied, err = _apply_pattern(
                txn=txn, fixme_account=fixme_acct,
                target_account=agreed,
                settings=settings, conn=conn,
            )
            if applied:
                result.pattern_applied += 1
                log.info(
                    "trickle: pattern auto-apply %s → %s "
                    "(neighbors agreed)",
                    target_hash[:8], agreed,
                )
            else:
                result.errors.append(
                    f"{target_hash[:8]} pattern rewrite refused: {err}"
                )
            continue

        # Sub-tier 2 gate: only proceed to AI when context-ripe.
        if not _is_context_ripe(
            txn=txn, fixme_account=fixme_acct,
            conn=conn, neighbors=neighbors,
        ):
            result.skipped_off_gate += 1
            continue

        # Cooldown: if we already proposed for this txn recently,
        # don't pay for another call until the user has had a
        # chance to act on it (or RECLASSIFY_COOLDOWN_DAYS elapses).
        if _has_recent_ai_decision(
            conn, target_hash, days=RECLASSIFY_COOLDOWN_DAYS,
        ):
            result.skipped_cooldown += 1
            continue

        # Sub-tier 2: AI classify. Capped per run.
        if ai_calls_made >= limit_per_run:
            # Exhausted budget — leave the rest for next run.
            result.skipped_off_gate += 1
            continue

        try:
            target, confidence, err = await _classify_one(
                txn=txn, fixme_account=fixme_acct,
                abs_amount=amt, currency=currency,
                entries=entries, conn=conn,
                settings=settings, ai_service=ai_service,
            )
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"{target_hash[:8]} ai error: {exc}")
            ai_calls_made += 1
            continue
        ai_calls_made += 1
        result.ai_called += 1

        if not target:
            # AI declined / errored. Record the proposal (which
            # _classify_one already logged via ai_decisions) but
            # don't write to the ledger. The row stays in the
            # review queue.
            result.ai_queued += 1
            if err:
                log.info(
                    "trickle: AI declined %s: %s", target_hash[:8], err,
                )
            continue

        # Per workstream A — AI proposals never auto-write the
        # ledger. The trickle is intentionally aligned: it logs
        # the proposal (via _classify_one's ai_decisions write,
        # surfaced through review) but does not rewrite. The user
        # accepts via /review/staged or /txn/<hash>.
        result.ai_queued += 1
        log.info(
            "trickle: AI proposal queued %s → %s (conf=%.2f)",
            target_hash[:8], target, confidence,
        )

    log.info(
        "trickle: scanned=%d rule_applied=%d pattern_applied=%d "
        "ai_called=%d ai_queued=%d off_gate=%d cooldown=%d errors=%d",
        result.scanned, result.rule_applied, result.pattern_applied,
        result.ai_called, result.ai_queued, result.skipped_off_gate,
        result.skipped_cooldown, len(result.errors),
    )
    return result
