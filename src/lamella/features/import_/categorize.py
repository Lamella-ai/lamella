# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Categorize every raw_row in an upload into a posting account.

Cascade (first match wins, per Phase 7 plan):
  1. `ann_business_expense='Yes' + ann_expense_category` present  →  confidence=annotated
  2. `payee_rules` LIKE match                                     →  confidence=rule
  3. Phase 2 `classification_rules` match                         →  confidence=rule
  4. AI classification via OpenRouter (if enabled + under spend cap)
      * score >= threshold → confidence=inferred
      * below threshold    → confidence=review  (needs_review=1)
  5. None of the above                                            →  confidence=review
                                                                      (needs_review=1, account='Expenses:Uncategorized')

Every AI classification call logs to `ai_decisions`. Rules-only operation
is supported — if AI is disabled, step 4 is skipped entirely and low-
confidence rows go straight to review.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from lamella.adapters.openrouter.client import AIError
from lamella.features.ai_cascade.service import AIService
from lamella.features.loans.claim import (
    claim_from_csv_row,
    load_loans_snapshot,
)
from lamella.features.rules.engine import evaluate as evaluate_rules
from lamella.features.rules.models import TxnFacts
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)


@dataclass
class CategorizeResult:
    categorized: int = 0
    needs_review: int = 0
    annotated: int = 0
    by_rule: int = 0
    by_classification_rule: int = 0
    by_ai: int = 0
    ai_skipped: int = 0


class AiCategorization(BaseModel):
    account: str = Field(description="Full Beancount account path for the counter leg.")
    entity: str | None = Field(default=None)
    schedule_c_category: str | None = Field(default=None)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reason: str = Field(default="")


_AI_SYSTEM = (
    "You are categorizing a single imported transaction for a bookkeeping "
    "ledger. Return a JSON object with the counter Beancount account "
    "(e.g. `Expenses:Acme:Supplies`), optional entity and Schedule C "
    "category, and a confidence in [0, 1]. Prefer accounts that already "
    "exist in the ledger if possible. If you cannot reasonably classify, "
    "set confidence below 0.5 and suggest `Expenses:Uncategorized`."
)


def _resolve_or_mint_lineage(
    conn: sqlite3.Connection, raw_row_id: int,
) -> str:
    """Return the lineage UUID bound to this raw_row's eventual ledger
    entry. Reuses the existing value on re-categorize so AI decisions
    logged under the original lineage stay matchable; mints a fresh
    UUIDv7 the first time we see a row.

    Stored on ``categorizations.lamella_txn_id`` (added by migration
    055). ``emit.render_transaction`` reads it back and stamps it as
    the entry's ``lamella-txn-id`` so the AI ``input_ref`` and the
    on-disk identity are the same value end-to-end.
    """
    from lamella.core.identity import mint_txn_id
    row = conn.execute(
        "SELECT lamella_txn_id FROM categorizations WHERE raw_row_id = ?",
        (raw_row_id,),
    ).fetchone()
    if row is not None and row["lamella_txn_id"]:
        return str(row["lamella_txn_id"])
    return mint_txn_id()


def _upsert_classification(
    conn: sqlite3.Connection, raw_row_id: int, status: str
) -> None:
    conn.execute(
        """
        INSERT INTO classifications (raw_row_id, status)
        VALUES (?, ?)
        ON CONFLICT(raw_row_id) DO UPDATE SET
            status = excluded.status,
            decided_at = datetime('now')
        """,
        (raw_row_id, status),
    )


def _upsert_categorization(
    conn: sqlite3.Connection,
    *,
    raw_row_id: int,
    account: str,
    confidence: str,
    entity: str | None = None,
    schedule_c_category: str | None = None,
    needs_review: bool = False,
    rule_id: int | None = None,
    reason: str | None = None,
    lamella_txn_id: str | None = None,
) -> None:
    """Upsert one categorization row.

    ``lamella_txn_id`` (post-migration 055) is the lineage UUID we
    mint at categorize time so the AI ``input_ref`` and the eventual
    on-disk ``lamella-txn-id`` are the same value. Reusing the
    existing lineage on a re-categorize keeps the binding stable
    (COALESCE preserves the old value when the upsert doesn't supply
    a new one).
    """
    conn.execute(
        """
        INSERT INTO categorizations
            (raw_row_id, entity, schedule_c_category, account,
             confidence, needs_review, rule_id, reason, lamella_txn_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(raw_row_id) DO UPDATE SET
            entity = excluded.entity,
            schedule_c_category = excluded.schedule_c_category,
            account = excluded.account,
            confidence = excluded.confidence,
            needs_review = excluded.needs_review,
            rule_id = excluded.rule_id,
            reason = excluded.reason,
            lamella_txn_id = COALESCE(
                categorizations.lamella_txn_id, excluded.lamella_txn_id
            ),
            decided_at = datetime('now')
        """,
        (
            raw_row_id,
            entity,
            schedule_c_category,
            account,
            confidence,
            1 if needs_review else 0,
            rule_id,
            reason,
            lamella_txn_id,
        ),
    )
    _mirror_decision_to_staging(
        conn,
        raw_row_id=raw_row_id,
        account=account,
        confidence=confidence,
        needs_review=needs_review,
        rule_id=rule_id,
        reason=reason,
    )


# Map importer confidence tags → unified (confidence_band, decided_by).
# The importer uses free-form strings; staging uses a closed vocabulary
# so the matcher + review UI can reason about confidence uniformly.
_IMPORTER_CONFIDENCE_MAP = {
    "annotated": ("high",   "rule"),
    "rule":      ("high",   "rule"),
    "user":      ("high",   "human"),
    "inferred":  ("medium", "ai"),
    "review":    ("low",    "ai"),
}


def _mirror_decision_to_staging(
    conn: sqlite3.Connection,
    *,
    raw_row_id: int,
    account: str | None,
    confidence: str,
    needs_review: bool,
    rule_id: int | None,
    reason: str | None,
) -> None:
    """Mirror a ``categorizations`` upsert into ``staged_decisions``.

    NEXTGEN.md Phase A: every classification decision on an importer
    row must also land on the unified staging surface so the Phase C
    transfer matcher and the cross-source review UI see it. Silently
    skipped if the staged row for this raw_row_id isn't present (e.g.,
    a row with no date/amount that wasn't mirrored in the first place).
    """
    try:
        from lamella.features.import_.staging.service import StagingService
    except Exception:
        return

    # Look up the staged row paired with this importer raw_row. The
    # mirror stores ``raw_row_id`` inside source_ref's JSON so we key
    # off that via a JSON-extract lookup.
    try:
        row = conn.execute(
            "SELECT id FROM staged_transactions "
            "WHERE source = 'csv' "
            "  AND json_extract(source_ref, '$.raw_row_id') = ?",
            (raw_row_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return
    if row is None:
        return

    band, decided_by = _IMPORTER_CONFIDENCE_MAP.get(
        (confidence or "").lower(), ("low", "ai")
    )
    # Explicit review flag beats the derived band — importer can say
    # "this is rule-matched but needs human confirmation."
    effective_band = "low" if needs_review else band
    try:
        StagingService(conn).record_decision(
            staged_id=int(row["id"]),
            account=account,
            confidence=effective_band,
            decided_by=decided_by,
            rule_id=rule_id,
            rationale=reason,
            needs_review=needs_review,
        )
    except sqlite3.OperationalError:
        return


def _match_payee_rule(conn: sqlite3.Connection, row: sqlite3.Row) -> sqlite3.Row | None:
    """Walk `payee_rules` LIKE patterns at the row level (bundle idiom)."""
    payee = (row["payee"] or "").strip()
    description = (row["description"] or "").strip()
    pm = (row["payment_method"] or "").strip()
    if not payee and not description:
        return None
    candidates = conn.execute(
        """
        SELECT *
          FROM payee_rules
         WHERE enabled = 1
         ORDER BY priority ASC, id ASC
        """,
    ).fetchall()
    for rule in candidates:
        if rule["pattern"] and not _like_match(rule["pattern"], payee):
            if not _like_match(rule["pattern"], description):
                continue
        if rule["description_pattern"] and not _like_match(
            rule["description_pattern"], description
        ):
            continue
        if rule["payment_method_pattern"] and not _like_match(
            rule["payment_method_pattern"], pm
        ):
            continue
        if rule["source_class_filter"] and row["source_class"] != rule["source_class_filter"]:
            continue
        if rule["entity_filter"] and (row["entity"] or "") != rule["entity_filter"]:
            continue
        return rule
    return None


def _like_match(pattern: str, value: str) -> bool:
    """Translate SQL LIKE (`%` wildcard) to Python substring match."""
    if value is None:
        value = ""
    pat = (pattern or "").lower()
    val = value.lower()
    if not pat:
        return False
    if "%" not in pat:
        return pat == val
    # Split on % and require each segment appears in order.
    parts = pat.split("%")
    cursor = 0
    for i, part in enumerate(parts):
        if not part:
            continue
        idx = val.find(part, cursor)
        if idx < 0:
            return False
        cursor = idx + len(part)
    # Anchoring: if pattern doesn't start with %, must match from position 0.
    if not pattern.lower().startswith("%"):
        if not val.startswith(parts[0].lower()):
            return False
    return True


def _row_to_facts(row: sqlite3.Row) -> TxnFacts:
    amt = row["amount"]
    return TxnFacts(
        payee=row["payee"],
        narration=row["description"],
        amount=Decimal(str(amt)) if amt is not None else None,
        card_account=row["payment_method"],
    )


async def categorize_import(
    conn: sqlite3.Connection,
    *,
    import_id: int,
    ai: AIService | None = None,
    rules: RuleService | None = None,
    ai_confidence_threshold: float = 0.7,
) -> CategorizeResult:
    """Walk every raw_row in this import, populate classifications +
    categorizations via the cascade. Returns summary counts.
    """
    rules = rules or RuleService(conn)
    result = CategorizeResult()

    # Preload active classification_rules once; the engine re-evaluates per row.
    classification_rules = list(rules.iter_active())

    # WP11 Site 6: snapshot loans once for the row-level claim check.
    # Loaded outside the row loop because (a) the list is small and
    # (b) doing N round-trips for a 10k-row import would be wasteful.
    # Falls back to [] when no loans exist or the table is missing.
    try:
        loans_snapshot = load_loans_snapshot(conn)
    except Exception:  # noqa: BLE001
        loans_snapshot = []

    rows = conn.execute(
        """
        SELECT rr.id, rr.date, rr.amount, rr.payee, rr.description,
               rr.memo, rr.payment_method, rr.transaction_id,
               rr.ann_business_expense, rr.ann_business,
               rr.ann_expense_category, rr.ann_expense_memo,
               rr.is_deducted_elsewhere,
               s.source_class, s.entity
          FROM raw_rows rr
          JOIN sources s ON s.id = rr.source_id
         WHERE s.upload_id = ?
         ORDER BY rr.id
        """,
        (import_id,),
    ).fetchall()

    ai_client = None
    if ai is not None and ai.enabled:
        ai_client = ai.new_client()

    try:
        for row in rows:
            raw_row_id = int(row["id"])
            # Zero-amount rows: skip outright. raw_rows.amount is TEXT
            # (post-migration 057, ADR-0022) so coerce to Decimal here
            # before the abs() compare — abs() on a str TypeErrors.
            amt_raw = row["amount"]
            amt_decimal = Decimal(amt_raw) if amt_raw is not None else None
            if amt_decimal is None or abs(amt_decimal) < Decimal("0.005"):
                _upsert_classification(conn, raw_row_id, "zero")
                continue

            # Lineage UUID is shared across every classification path
            # AND across categorize→emit. Re-categorize keeps the
            # existing lineage so AI decisions logged under it stay
            # bound; first-time-seen rows mint fresh.
            row_lineage = _resolve_or_mint_lineage(conn, raw_row_id)

            # Step 1 — authoritative annotations.
            is_yes = (row["ann_business_expense"] or "").strip().lower() == "yes"
            if is_yes and (row["ann_expense_category"] or row["ann_business"]):
                entity = row["ann_business"] or row["entity"] or "Personal"
                category = row["ann_expense_category"] or "Uncategorized"
                account = f"Expenses:{entity}:{category}".replace(" ", "")
                _upsert_classification(conn, raw_row_id, "imported")
                _upsert_categorization(
                    conn,
                    raw_row_id=raw_row_id,
                    account=account,
                    confidence="annotated",
                    entity=entity,
                    schedule_c_category=category,
                    reason="Annotated row (Business Expense? = Yes)",
                    lamella_txn_id=row_lineage,
                )
                result.categorized += 1
                result.annotated += 1
                continue

            # Step 2 — payee_rules (row-level LIKE).
            pr = _match_payee_rule(conn, row)
            if pr is not None:
                _upsert_classification(conn, raw_row_id, "imported")
                _upsert_categorization(
                    conn,
                    raw_row_id=raw_row_id,
                    account=pr["account"],
                    confidence="rule",
                    entity=pr["entity"],
                    schedule_c_category=pr["schedule_c_category"],
                    needs_review=bool(pr["needs_review"]),
                    reason=pr["reason"] or f"payee_rule #{pr['id']}",
                    lamella_txn_id=row_lineage,
                )
                result.categorized += 1
                result.by_rule += 1
                if pr["needs_review"]:
                    result.needs_review += 1
                continue

            # Step 3 — classification_rules.
            match = evaluate_rules(_row_to_facts(row), classification_rules)
            if match is not None:
                _upsert_classification(conn, raw_row_id, "imported")
                _upsert_categorization(
                    conn,
                    raw_row_id=raw_row_id,
                    account=match.target_account,
                    confidence="rule",
                    reason=(
                        f"classification_rules #{match.rule.id} "
                        f"({match.rule.pattern_type}={match.rule.pattern_value!r})"
                    ),
                    lamella_txn_id=row_lineage,
                )
                result.categorized += 1
                result.by_classification_rule += 1
                continue

            # WP11 Site 6 — loan-claim preemption (principle 3).
            # If this row looks like a loan payment, the AI classifier
            # has nothing useful to say (it doesn't know amortization
            # or escrow splits). Mark needs_review with a pointer to
            # the backfill flow and skip the AI step entirely.
            #
            # Conservative — runs AFTER explicit rules so a user-
            # configured payee_rule that maps the row to a non-loan
            # account (e.g., a categorization correction) still wins.
            if loans_snapshot:
                csv_claim = claim_from_csv_row(row, conn, loans=loans_snapshot)
                if csv_claim is not None:
                    # Look up the loan's liability path so the user has
                    # the right account starting point in review.
                    matched = next(
                        (l for l in loans_snapshot
                         if l.get("slug") == csv_claim.loan_slug),
                        None,
                    )
                    suggested = (matched.get("liability_account_path")
                                 if matched else None) or "Expenses:Uncategorized"
                    _upsert_classification(conn, raw_row_id, "imported")
                    _upsert_categorization(
                        conn,
                        raw_row_id=raw_row_id,
                        account=suggested,
                        confidence="review",
                        needs_review=True,
                        reason=(
                            f"loan-claimed (WP11 Site 6); use "
                            f"/settings/loans/{csv_claim.loan_slug}/backfill "
                            f"for proper amortization split."
                        ),
                        lamella_txn_id=row_lineage,
                    )
                    result.categorized += 1
                    result.needs_review += 1
                    result.ai_skipped += 1
                    continue

            # Step 4 — AI classification (if available).
            if ai_client is not None:
                # input_ref = the entry's lineage UUID. Mints once at
                # categorize time, stamps on the categorizations row,
                # and emit.render_transaction reads it back to write
                # as lamella-txn-id on the on-disk entry. Same single
                # value end-to-end; AI history on /txn surfaces the
                # decision via the entry's lineage without any
                # composite-PK bridge.
                input_ref = row_lineage
                user_prompt = (
                    "Transaction:\n"
                    f"  date: {row['date']}\n"
                    f"  amount: {row['amount']}\n"
                    f"  payee: {row['payee']!r}\n"
                    f"  description: {row['description']!r}\n"
                    f"  memo: {row['memo']!r}\n"
                    f"  payment_method: {row['payment_method']!r}\n"
                    f"  source_class: {row['source_class']}\n"
                    f"  entity_hint: {row['entity']!r}\n"
                )
                try:
                    ai_result = await ai_client.chat(
                        decision_type="classify_txn",
                        input_ref=input_ref,
                        system=_AI_SYSTEM,
                        user=user_prompt,
                        schema=AiCategorization,
                        model=ai.model_for("classify_txn"),
                    )
                except AIError as exc:
                    log.warning("AI classify failed for %s: %s", input_ref, exc)
                    ai_result = None
                if ai_result is not None:
                    data = ai_result.data
                    is_review = data.confidence < ai_confidence_threshold
                    _upsert_classification(conn, raw_row_id, "imported")
                    _upsert_categorization(
                        conn,
                        raw_row_id=raw_row_id,
                        account=data.account or "Expenses:Uncategorized",
                        confidence="review" if is_review else "inferred",
                        entity=data.entity,
                        schedule_c_category=data.schedule_c_category,
                        needs_review=is_review,
                        reason=(data.reason or "")[:500],
                        lamella_txn_id=row_lineage,
                    )
                    result.categorized += 1
                    result.by_ai += 1
                    if is_review:
                        result.needs_review += 1
                    continue

            # Step 5 — fall-through: needs review.
            _upsert_classification(conn, raw_row_id, "imported")
            _upsert_categorization(
                conn,
                raw_row_id=raw_row_id,
                account="Expenses:Uncategorized",
                confidence="review",
                needs_review=True,
                reason="No rule, annotation, or AI classification available.",
                lamella_txn_id=row_lineage,
            )
            result.categorized += 1
            result.needs_review += 1
            if ai_client is None:
                result.ai_skipped += 1
    finally:
        if ai_client is not None:
            await ai_client.aclose()

    return result
