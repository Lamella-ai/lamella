# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Background job: run the AI classifier over every existing FIXME
transaction in the ledger.

Context: the AI classifier runs automatically during SimpleFIN ingest
for NEW transactions, but there's no code path that re-classifies
FIXMEs that were written before AI was enabled (or that fell through
as "Expenses:FIXME" on a low-confidence ingest). A user who started
with a messy ledger — the thousands-of-FIXMEs case — never benefits
from the AI unless they click "bulk apply" for each merchant by hand.

This module closes that gap. For every FIXME txn:

  1. Skip if an override already exists for its txn_hash.
  2. Build the same classify context the ingest path uses
     (vector similarity, merchant histogram, notes, mileage,
     receipt candidates).
  3. Call OpenRouter with the primary classify model; cascade to
     the fallback model when the primary returns low confidence.
  4. If the final proposal's confidence crosses the auto-apply
     threshold, write an override that routes the FIXME leg to
     the proposed account.
  5. Otherwise record the proposal in ai_decisions / review_queue
     with user_corrected=NULL so it surfaces for review.

The job is cancellable via ``ctx.raise_if_cancelled()`` on each txn
and emits Success / Failure / Not Found / Error events through
``ctx.emit`` so the progress modal tells the user exactly what
happened with each row.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from beancount.core.data import Transaction

from lamella.features.ai_cascade.context import resolve_entity_for_account
from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_txn_hash
from lamella.core.config import Settings
from lamella.core.identity import get_txn_id

log = logging.getLogger(__name__)

# Auto-apply threshold — after workstream A, bulk_classify only
# auto-applies when a matching user-created rule is already at or
# above this confidence. AI-only proposals never self-promote here
# (docs/specs/AI-CLASSIFICATION.md tier-2 — pattern-match from prior
# *user* confirmations); the LLM output always routes to review so
# the user's click-accept is what creates the next-level rule.
# Aligned with ConfidenceGate.DEFAULT_AUTO_APPLY_THRESHOLD so the
# bulk path and the gate path agree on what "auto-apply" means.
AUTO_APPLY_THRESHOLD = 0.95


@dataclass
class BulkClassifyResult:
    scanned: int = 0
    skipped_has_override: int = 0
    skipped_no_context: int = 0
    auto_applied: int = 0
    queued_for_review: int = 0
    ai_errors: int = 0
    bean_check_errors: int = 0
    applied_pairs: list[tuple[str, str, float]] = field(default_factory=list)  # (txn_hash, target, confidence)
    review_suggestions: list[tuple[str, str, float]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _existing_override_hashes(overrides_path: Path) -> set[str]:
    """Every txn_hash that already has an override block."""
    if not overrides_path.exists():
        return set()
    import re
    text = overrides_path.read_text(encoding="utf-8")
    return set(re.findall(r'(?:lamella-)?override-of:\s*"([^"]+)"', text))


def _context_score(
    row: tuple[Transaction, str, Decimal, str],
    conn: sqlite3.Connection | None,
) -> int:
    """Priority score: receipt > memo/narration > project > mileage.

    User-flag (review_queue.priority) is applied as a tie-breaker
    WITHIN a context-richness bucket, not as a top-level override.
    The philosophy ranks signals by 'information content for the
    classifier'; user-flag answers 'what does the user care about'
    — different question. A flagged item with no other context
    still beats an unflagged item with no other context, and
    still sits below any item with a receipt.

    Do not 'fix' this by promoting user-flag to the top of the
    sort — that's an intentional design choice.

    Score shape: context-richness bits in the high half, user-flag
    priority magnitude (clamped to 16 bits) in the low half. Ranks
    receipt > memo > project > mileage by bit weight, with flag as
    a within-bucket tiebreaker.
    """
    txn, fixme_account, _amt, _currency = row
    bits = 0
    txn_hash = compute_txn_hash(txn)
    if _has_linked_receipt(txn_hash, conn):
        bits |= 16
    if _has_memo_signal(txn):
        bits |= 8
    if _has_active_project(txn, conn):
        bits |= 4
    if _has_nearby_mileage(txn, fixme_account, conn):
        bits |= 2
    priority = _user_priority(txn_hash, conn)
    return (bits << 16) | (priority & 0xFFFF)


def _has_linked_receipt(txn_hash: str, conn: sqlite3.Connection | None) -> bool:
    if conn is None:
        return False
    try:
        row = conn.execute(
            "SELECT 1 FROM receipt_links WHERE txn_hash = ? LIMIT 1",
            (txn_hash,),
        ).fetchone()
    except Exception:  # noqa: BLE001 — missing table or schema mismatch in a test
        return False
    return row is not None


def _has_memo_signal(txn: Transaction) -> bool:
    narration = (getattr(txn, "narration", None) or "").strip()
    payee = (getattr(txn, "payee", None) or "").strip()
    return bool(narration) or bool(payee)


def _has_active_project(txn: Transaction, conn: sqlite3.Connection | None) -> bool:
    if conn is None:
        return False
    try:
        txn_date = (
            txn.date if isinstance(txn.date, date)
            else date.fromisoformat(str(txn.date))
        )
        row = conn.execute(
            """
            SELECT 1 FROM projects
             WHERE is_active = 1
               AND start_date <= ?
               AND (end_date IS NULL OR end_date >= ?)
             LIMIT 1
            """,
            (txn_date.isoformat(), txn_date.isoformat()),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    return row is not None


def _has_nearby_mileage(
    txn: Transaction,
    fixme_account: str,
    conn: sqlite3.Connection | None,
    *,
    window_days: int = 3,
) -> bool:
    if conn is None:
        return False
    try:
        txn_date = (
            txn.date if isinstance(txn.date, date)
            else date.fromisoformat(str(txn.date))
        )
    except Exception:  # noqa: BLE001
        return False
    card_account = _pick_card_account(txn, fixme_account)
    entity = resolve_entity_for_account(conn, card_account) if card_account else None
    from datetime import timedelta
    lo = (txn_date - timedelta(days=window_days)).isoformat()
    hi = (txn_date + timedelta(days=window_days)).isoformat()
    try:
        if entity:
            row = conn.execute(
                """
                SELECT 1 FROM mileage_entries
                 WHERE entry_date BETWEEN ? AND ?
                   AND entity = ?
                 LIMIT 1
                """,
                (lo, hi, entity),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT 1 FROM mileage_entries
                 WHERE entry_date BETWEEN ? AND ?
                 LIMIT 1
                """,
                (lo, hi),
            ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    return row is not None


def _user_priority(txn_hash: str, conn: sqlite3.Connection | None) -> int:
    """review_queue.priority already carries magnitude (set when a
    user flags a row for attention). Treat it as a positive int;
    zero when no open row exists or priority is unset."""
    if conn is None:
        return 0
    try:
        row = conn.execute(
            """
            SELECT MAX(priority) AS max_pri
              FROM review_queue
             WHERE source_ref = ?
               AND resolved_at IS NULL
            """,
            (txn_hash,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return 0
    if row is None or row["max_pri"] is None:
        return 0
    try:
        val = int(row["max_pri"])
    except (TypeError, ValueError):
        return 0
    return max(0, val)


def _pick_card_account(txn: Transaction, fixme_account: str) -> str | None:
    """The non-FIXME side of a simple FIXME txn is usually the card
    (or the bank / liability that paid). Pick the first Asset/Liability
    posting that isn't the fixme leg."""
    for p in txn.postings or ():
        acct = p.account or ""
        if acct == fixme_account:
            continue
        if acct.startswith("Assets:") or acct.startswith("Liabilities:"):
            return acct
    return None


def _matching_user_rule(
    *,
    txn: Transaction,
    fixme_account: str,
    abs_amount: Decimal,
    rules: list,
    auto_apply_threshold: float,
):
    """Return the highest-priority user-created rule at or above the
    auto-apply threshold matching this FIXME, or None. This is the
    tier-2 short-circuit that lets bulk_classify skip the LLM call
    entirely when prior user confirmations already resolved the
    question."""
    from lamella.features.rules.engine import evaluate as evaluate_rules
    from lamella.features.rules.models import TxnFacts
    if not rules:
        return None
    card_account = _pick_card_account(txn, fixme_account)
    facts = TxnFacts(
        payee=getattr(txn, "payee", None),
        narration=getattr(txn, "narration", None),
        amount=abs_amount,
        card_account=card_account,
    )
    match = evaluate_rules(facts, rules)
    if match is None:
        return None
    rule = match.rule
    if rule.created_by != "user":
        return None
    if rule.confidence < auto_apply_threshold:
        return None
    return match


def _collect_fixme_txns(
    entries,
    *,
    conn: sqlite3.Connection | None = None,
) -> list[tuple[Transaction, str, Decimal, str]]:
    """Every Transaction with a FIXME posting. Returns
    [(txn, fixme_account, abs_amount, currency), …]. Amount is
    absolute so the caller doesn't have to re-derive it.

    Rows are returned sorted by context-richness (receipt > memo >
    project > mileage) so the AI token budget lands on transactions
    where the classifier actually has the inputs to answer well.
    See `_context_score` for the sort key."""
    out: list[tuple[Transaction, str, Decimal, str]] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        fixme_acct: str | None = None
        amt: Decimal | None = None
        currency = "USD"
        for p in e.postings or ():
            acct = p.account or ""
            if acct.split(":")[-1].upper() == "FIXME":
                if p.units is not None and p.units.number is not None:
                    fixme_acct = acct
                    amt = abs(Decimal(p.units.number))
                    currency = p.units.currency or "USD"
                    break
        if fixme_acct and amt is not None:
            out.append((e, fixme_acct, amt, currency))
    out.sort(key=lambda row: _context_score(row, conn), reverse=True)
    return out


async def _classify_one(
    *,
    txn: Transaction,
    fixme_account: str,
    abs_amount: Decimal,
    currency: str,
    entries: list,
    conn: sqlite3.Connection,
    settings: Settings,
    ai_service,
) -> tuple[str | None, float, str | None]:
    """Run the classifier for one FIXME txn. Returns
    ``(target_account, confidence, error_message)``. target_account is
    None when the AI declined or failed; error_message carries a
    short reason for the event log."""
    from lamella.features.ai_cascade.classify import (
        ClassifyResponse,
        build_classify_context,
        prior_attempts_for_txn,
    )
    from lamella.adapters.openrouter.client import AIError
    from lamella.features.ai_cascade.context import render
    client = ai_service.new_client()
    if client is None:
        return None, 0.0, "AI client unavailable"
    try:
        ctx_tuple = build_classify_context(
            entries=entries, txn=txn, conn=conn,
        )
    except Exception as exc:  # noqa: BLE001
        return None, 0.0, f"context build failed: {exc}"
    view = ctx_tuple[0]
    if view is None:
        return None, 0.0, "no FIXME context"
    similar = ctx_tuple[1]
    valid_accounts = ctx_tuple[2]
    entity = ctx_tuple[3]
    active_notes = ctx_tuple[4]
    card_suspicion = ctx_tuple[5]
    # accounts_by_entity (index 6) is the FULL cross-entity whitelist
    # the AI needs to see so it can flag intercompany situations.
    # Skipping it was the bug that caused a user hint pointing at
    # `Expenses:AJQuick:OfficeExpense` to produce
    # `Expenses:Personal:OfficeExpense` — the AI literally never saw
    # AJQuick's accounts and couldn't select them.
    accounts_by_entity = ctx_tuple[6]
    receipt_ctx = ctx_tuple[7]
    mileage_entries = ctx_tuple[8]
    if not valid_accounts:
        return None, 0.0, f"no valid accounts for entity={entity}"
    # Negative reinforcement: when the user has previously rejected
    # or corrected an attempt at this same txn, surface those prior
    # attempts in the prompt so the AI doesn't re-derive the same
    # answer from the same inputs. Without this, every re-classify
    # is amnesia.
    target_hash = compute_txn_hash(txn)
    # Phase 3 of NORMALIZE_TXN_IDENTITY.md: AI decisions key off
    # ``lamella-txn-id`` (lineage) when the entry carries one — that's
    # what survives the staging→ledger promotion + future ledger edits
    # that change the content hash. Pre-Phase-4 entries don't yet have
    # a lineage id; fall back to ``txn_hash`` so AI history still pins
    # to the entry. The per-txn AI history query in routes/search.py
    # checks both shapes during the transition.
    txn_input_ref = get_txn_id(txn) or target_hash
    prior_attempts = prior_attempts_for_txn(conn, target_hash)
    try:
        prompt = render(
            "classify_txn.j2",
            txn={
                "date": txn.date,
                "amount": abs_amount,
                "currency": currency,
                "payee": getattr(txn, "payee", None),
                "narration": txn.narration or None,
                "card_account": view.card_account,
                "fixme_account": fixme_account,
            },
            similar=similar,
            entity=entity,
            accounts=valid_accounts,
            # Pass the grouped cross-entity map so the prompt's
            # "Whitelist … grouped by entity" branch fires. With this
            # the AI can see AJQuick's accounts alongside Personal's
            # and decide whether to flag intercompany.
            accounts_by_entity=accounts_by_entity,
            active_notes=active_notes,
            card_suspicion=card_suspicion,
            receipt=receipt_ctx,
            mileage_entries=mileage_entries,
            prior_attempts=prior_attempts,
        )
    except Exception as exc:  # noqa: BLE001
        return None, 0.0, f"prompt render failed: {exc}"
    system_prompt = (
        "You are a meticulous bookkeeper. You classify transactions "
        "into a predefined chart of accounts. Never invent accounts."
    )
    primary_model = ai_service.model_for("classify_txn")
    fallback_model = ai_service.fallback_model_for("classify_txn")
    fallback_threshold = ai_service.fallback_threshold()

    # client.chat() returns AIResult[ClassifyResponse]; the parsed
    # response object lives under `.data`. Don't read .confidence off
    # the AIResult directly — that's an older signature that fooled me.
    try:
        primary_wrapper = await client.chat(
            decision_type="classify_txn",
            input_ref=txn_input_ref,
            system=system_prompt,
            user=prompt,
            schema=ClassifyResponse,
            model=primary_model,
        )
    except AIError as exc:
        return None, 0.0, f"AI primary failed: {exc}"

    primary_data: ClassifyResponse | None = (
        getattr(primary_wrapper, "data", None) if primary_wrapper else None
    )
    chosen_data = primary_data
    if (
        fallback_model
        and (primary_data is None or float(getattr(primary_data, "confidence", 0.0) or 0.0) < fallback_threshold)
    ):
        try:
            fallback_wrapper = await client.chat(
                decision_type="classify_txn",
                input_ref=txn_input_ref,
                system=system_prompt,
                user=prompt,
                schema=ClassifyResponse,
                model=fallback_model,
            )
            fb_data = getattr(fallback_wrapper, "data", None) if fallback_wrapper else None
            if fb_data is not None:
                chosen_data = fb_data
        except AIError as exc:
            log.warning(
                "bulk classify: fallback model %s failed for %s: %s",
                fallback_model, target_hash[:8], exc,
            )

    if chosen_data is None:
        return None, 0.0, "AI returned empty"
    target = (getattr(chosen_data, "target_account", "") or "").strip()
    confidence = float(getattr(chosen_data, "confidence", 0.0) or 0.0)
    if not target:
        return None, confidence, "AI did not pick a target"
    # Off-whitelist guard. Check against the full cross-entity set when
    # available, otherwise fall back to the flat entity-scoped list.
    # Allowing the full set is what makes intercompany proposals work
    # (AI picks Expenses:AJQuick:... even though the card is on Personal).
    whitelist = set(valid_accounts)
    if accounts_by_entity:
        for lst in accounts_by_entity.values():
            whitelist.update(lst)
    if target not in whitelist:
        # Off-whitelist — reject. Don't invent accounts.
        return None, confidence, f"target {target!r} not in whitelist"
    return target, confidence, None


def classify_all_fixmes(
    ctx,  # JobContext — has .emit / .advance / .set_total / .raise_if_cancelled
    *,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    settings: Settings,
    ai_service,
    auto_apply_threshold: float = AUTO_APPLY_THRESHOLD,
    limit: int | None = None,
    max_consecutive_errors: int = 3,
) -> dict[str, Any]:
    """Top-level job function. Runs asyncio under the hood so each
    AI call can await its HTTP roundtrip without blocking other work.

    ``limit`` caps the number of FIXMEs actually classified per run
    so a first-time user doesn't watch thousands of AI calls fire
    before they've seen if any are any good. Already-resolved rows
    (existing override) don't count against the limit.

    ``max_consecutive_errors`` aborts the run early when the AI or
    writer keeps failing the same way — no reason to pay for 500
    identical errors. Reset by any successful proposal.
    """
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.features.rules.overrides import OverrideWriter
    from lamella.features.rules.service import RuleService

    result = BulkClassifyResult()

    if not ai_service or not ai_service.enabled:
        ctx.emit(
            "AI service not enabled (OPENROUTER_API_KEY unset). "
            "Set the key in settings and retry.",
            outcome="error",
        )
        return {"error": "ai_disabled"}

    entries = list(reader.load().entries)
    fixmes = _collect_fixme_txns(entries, conn=conn)
    if not fixmes:
        ctx.emit("No FIXME transactions found.", outcome="info")
        return {"scanned": 0, "auto_applied": 0, "queued_for_review": 0}

    existing_overrides = _existing_override_hashes(settings.connector_overrides_path)

    # Informational total = everything pending classification after
    # filtering out already-overridden rows. If a limit was passed,
    # total reflects that cap so the progress bar doesn't promise
    # more than we'll do.
    pending = [
        row for row in fixmes
        if compute_txn_hash(row[0]) not in existing_overrides
    ]
    effective_total = len(pending) if limit is None else min(limit, len(pending))
    ctx.set_total(effective_total)
    limit_note = f"; LIMIT applied: {limit}" if limit is not None else ""
    ctx.emit(
        f"Found {len(fixmes)} FIXME transaction(s); "
        f"{len(existing_overrides)} already have overrides; "
        f"{len(pending)} eligible for classification{limit_note}.",
        outcome="info",
    )

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )

    # Load the active rules once; a user-created rule at or above the
    # auto-apply threshold short-circuits the LLM call for any FIXME
    # it matches (docs/specs/AI-CLASSIFICATION.md tier-2). Small list,
    # evaluated in-memory per txn — no reason to hit SQLite N times.
    _rules_cache = list(RuleService(conn).iter_active())

    # WP6 Site 1 — build the loans snapshot ONCE at the top of the
    # bulk_classify run. Every FIXME gets a claim check against it;
    # without this cache, a 500-FIXME sweep would hit the loans
    # table 500 times. The snapshot is small (one query, N active
    # loans, typically ≤ 10 rows) and stale-within-one-run is fine —
    # bulk_classify itself doesn't edit loans. Looked up lazily so
    # deployments with no loans pay zero cost.
    from lamella.features.loans import auto_classify as _auto_classify
    from lamella.features.loans.claim import (
        is_claimed_by_loan as _is_claimed_by_loan,
        load_loans_snapshot as _load_loans_snapshot,
    )
    _loans_cache = _load_loans_snapshot(conn)
    _loan_rows_by_slug: dict[str, dict] = {}

    def _load_loan(slug: str) -> dict | None:
        """One SQLite read per distinct loan per run. Cache lives on
        the closure; bulk_classify never mutates loans so stale is ok."""
        if slug in _loan_rows_by_slug:
            return _loan_rows_by_slug[slug]
        row = conn.execute(
            "SELECT * FROM loans WHERE slug = ?", (slug,),
        ).fetchone()
        loan = dict(row) if row is not None else None
        if loan is not None:
            _loan_rows_by_slug[slug] = loan
        return loan

    async def _run() -> None:
        consecutive_errors = 0
        processed = 0  # number of txns we actually attempted classification on
        aborted_early = False
        for txn, fixme_acct, amt, currency in fixmes:
            ctx.raise_if_cancelled()
            target_hash = compute_txn_hash(txn)
            if target_hash in existing_overrides:
                result.skipped_has_override += 1
                # Don't count against limit; advance bar too.
                continue

            # WP6 Site 1 — principle-3 preemption. Loan-claimed FIXMEs
            # never reach the AI classifier; auto_classify.process
            # handles them (writes split for tier=exact/over, leaves
            # review-worthy for tier=under/far). Preempted txns don't
            # count against the AI limit since no AI call was made.
            claim = _is_claimed_by_loan(txn, conn, loans=_loans_cache)
            if claim is not None:
                loan = _load_loan(claim.loan_slug)
                if loan is None:
                    result.queued_for_review += 1
                    ctx.emit(
                        f"{target_hash[:8]}… loan:{claim.loan_slug} not found — "
                        f"leaving FIXME for manual review.",
                        outcome="info",
                    )
                    ctx.advance()
                    continue
                try:
                    outcome = _auto_classify.process(
                        claim, txn, loan,
                        settings=settings, reader=reader, conn=conn,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "bulk_classify: auto_classify.process failed "
                        "for %s (slug=%s): %s",
                        target_hash[:12], claim.loan_slug, exc,
                    )
                    result.ai_errors += 1
                    ctx.emit(
                        f"{target_hash[:8]}… loan auto-classify error: {exc}",
                        outcome="error",
                    )
                    ctx.advance()
                    continue
                if outcome.wrote_override:
                    result.auto_applied += 1
                    ctx.emit(
                        f"{target_hash[:8]}… loan:{claim.loan_slug} "
                        f"tier={outcome.tier} → auto-split",
                        outcome="success",
                    )
                else:
                    result.queued_for_review += 1
                    ctx.emit(
                        f"{target_hash[:8]}… loan:{claim.loan_slug} "
                        f"kind={claim.kind.value} → deferred to review "
                        f"({outcome.skip_reason or 'see detail'})",
                        outcome="info",
                    )
                ctx.advance()
                continue

            # Tier-2: a user-created rule at ≥ auto_apply_threshold
            # matching this FIXME auto-applies WITHOUT an LLM call.
            # docs/specs/AI-CLASSIFICATION.md — "pattern-match from prior
            # *user* confirmations" is the free-and-accurate path.
            rule_match = _matching_user_rule(
                txn=txn, fixme_account=fixme_acct, abs_amount=amt,
                rules=_rules_cache,
                auto_apply_threshold=auto_apply_threshold,
            )
            if rule_match is not None:
                try:
                    txn_date_val = (
                        txn.date if isinstance(txn.date, date)
                        else date.fromisoformat(str(txn.date))
                    )
                    # Per CLAUDE.md "in-place rewrites are the
                    # default" — try editing the FIXME posting in
                    # the source file before falling back to the
                    # override-block layer.
                    from lamella.core.rewrite.txn_inplace import (
                        InPlaceRewriteError,
                        rewrite_fixme_to_account,
                    )
                    meta = getattr(txn, "meta", None) or {}
                    src_file = meta.get("filename")
                    src_lineno = meta.get("lineno")
                    fixme_signed = None
                    for _p in txn.postings or ():
                        if (_p.account or "") == fixme_acct and _p.units \
                                and _p.units.number is not None:
                            fixme_signed = Decimal(_p.units.number)
                            break
                    in_place_done = False
                    if src_file and src_lineno is not None:
                        try:
                            try:
                                writer.rewrite_without_hash(target_hash)
                            except BeanCheckError:
                                raise InPlaceRewriteError(
                                    "override-strip blocked"
                                )
                            from pathlib import Path as _P
                            rewrite_fixme_to_account(
                                source_file=_P(src_file),
                                line_number=int(src_lineno),
                                old_account=fixme_acct,
                                new_account=rule_match.target_account,
                                expected_amount=fixme_signed,
                                ledger_dir=settings.ledger_dir,
                                main_bean=settings.ledger_main,
                            )
                            in_place_done = True
                        except InPlaceRewriteError as exc:
                            log.info(
                                "bulk_classify: in-place refused for "
                                "%s: %s — falling back to override",
                                target_hash[:12], exc,
                            )
                    if not in_place_done:
                        writer.append(
                            txn_date=txn_date_val,
                            txn_hash=target_hash,
                            amount=amt,
                            from_account=fixme_acct,
                            to_account=rule_match.target_account,
                            currency=currency,
                            narration=(
                                txn.narration or "rule bulk classify"
                            ),
                        )
                    result.auto_applied += 1
                    result.applied_pairs.append(
                        (target_hash, rule_match.target_account,
                         rule_match.rule.confidence),
                    )
                    ctx.emit(
                        f"{target_hash[:8]}… → {rule_match.target_account} "
                        f"(rule #{rule_match.rule.id} conf "
                        f"{rule_match.rule.confidence:.2f}, "
                        f"{'in-place' if in_place_done else 'override fallback'})",
                        outcome="success",
                    )
                    consecutive_errors = 0
                except BeanCheckError as exc:
                    result.bean_check_errors += 1
                    result.errors.append(
                        f"{target_hash[:8]}: bean-check blocked ({exc})",
                    )
                    ctx.emit(
                        f"{target_hash[:8]}… bean-check blocked: {exc}",
                        outcome="error",
                    )
                    consecutive_errors += 1
                except Exception as exc:  # noqa: BLE001
                    result.bean_check_errors += 1
                    result.errors.append(f"{target_hash[:8]}: {exc}")
                    log.exception(
                        "bulk classify: rule write failed for %s",
                        target_hash[:8],
                    )
                    ctx.emit(
                        f"{target_hash[:8]}… rule write failed: {exc}",
                        outcome="error",
                    )
                    consecutive_errors += 1
                ctx.advance()
                if consecutive_errors >= max_consecutive_errors:
                    ctx.emit(
                        f"{consecutive_errors} consecutive errors — "
                        f"aborting.", outcome="error",
                    )
                    aborted_early = True
                    break
                continue

            # Enforce limit BEFORE we spend an AI call.
            if limit is not None and processed >= limit:
                ctx.emit(
                    f"Reached limit={limit}; stopping. "
                    f"Use a higher limit (or leave it off) to continue.",
                    outcome="info",
                )
                aborted_early = True
                break
            processed += 1
            result.scanned += 1
            target, confidence, error_msg = await _classify_one(
                txn=txn, fixme_account=fixme_acct,
                abs_amount=amt, currency=currency,
                entries=entries, conn=conn,
                settings=settings, ai_service=ai_service,
            )
            if error_msg and not target:
                result.ai_errors += 1
                result.errors.append(f"{target_hash[:8]}: {error_msg}")
                ctx.emit(
                    f"{target_hash[:8]}… {error_msg}", outcome="error",
                )
                consecutive_errors += 1
                ctx.advance()
                if consecutive_errors >= max_consecutive_errors:
                    ctx.emit(
                        f"{consecutive_errors} consecutive errors — "
                        f"aborting to avoid runaway AI spend. Fix the "
                        f"underlying issue and retry.",
                        outcome="error",
                    )
                    aborted_early = True
                    break
                continue
            # Any non-error outcome resets the streak.
            consecutive_errors = 0
            if target is None:
                result.queued_for_review += 1
                ctx.emit(
                    f"{target_hash[:8]}… no target proposed",
                    outcome="info",
                )
                ctx.advance()
                continue
            # Post-workstream-A: AI-only proposals ALWAYS route to
            # review. The user's click-accept in /review is what
            # promotes them (and creates a user-rule via
            # learn_from_decision so the next similar FIXMEs hit the
            # tier-2 path above without another LLM call).
            result.queued_for_review += 1
            result.review_suggestions.append(
                (target_hash, target, confidence)
            )
            ctx.emit(
                f"{target_hash[:8]}… → {target} "
                f"(conf {confidence:.2f}, queued for review)",
                outcome="info",
            )
            ctx.advance()
        result.aborted_early = aborted_early  # type: ignore[attr-defined]

    try:
        asyncio.run(_run())
    except Exception as exc:  # noqa: BLE001
        log.exception("bulk classify: top-level asyncio run failed")
        result.errors.append(f"job-level: {exc}")
        ctx.emit(f"Job failed: {exc}", outcome="error")

    reader.invalidate()
    return {
        "scanned": result.scanned,
        "skipped_has_override": result.skipped_has_override,
        "skipped_no_context": result.skipped_no_context,
        "auto_applied": result.auto_applied,
        "queued_for_review": result.queued_for_review,
        "ai_errors": result.ai_errors,
        "bean_check_errors": result.bean_check_errors,
        "errors_sample": result.errors[:10],
    }
