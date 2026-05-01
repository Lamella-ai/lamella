# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase-2 AI helpers for the calendar feature.

Two callables:

* ``summarize_day()`` — plain-prose narrative for a single day.
  Uses the existing OpenRouter client + the ``summarize_day.j2``
  prompt. Response is validated against a thin pydantic schema
  so the cache + audit-log machinery works, but the only
  meaningful field is ``summary``.

* ``audit_day()`` — per-txn classify re-runs with full day
  context (day note + sibling txns + mileage). Calls
  ``propose_account`` once per FIXME or currently-classified
  expense posting. Returns a list of ``AuditEntry`` dicts. Does
  NOT mutate any ledger or SQLite state.

Both are async. Both respect the spend cap via the caller's
``AIService.new_client()`` contract.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date
from decimal import Decimal
from typing import Any, Iterable

from pydantic import BaseModel, Field

from lamella.adapters.openrouter.client import AIError, AIResult, OpenRouterClient
from lamella.features.ai_cascade.context import TxnForClassify, render

log = logging.getLogger(__name__)


SUMMARY_SYSTEM = (
    "You summarize one business day of a Beancount ledger for a "
    "human reviewer. Be terse, accurate, and prose-only."
)


class _SummaryResponse(BaseModel):
    """Thin envelope — the only meaningful content is ``summary``.

    Using a schema (even a minimal one) keeps the OpenRouter
    response-format + cache + decisions-log path consistent with
    every other call in the app."""

    summary: str = Field(min_length=1, max_length=2000)


@dataclass
class AuditEntry:
    """One per-txn audit result. All fields are plain strings /
    decimals so the dataclass is trivially JSON-serializable for
    the `day_reviews.ai_audit_result` cache column."""

    txn_hash: str
    narration: str
    amount: str  # serialized Decimal
    current_account: str
    proposed_account: str | None
    confidence: float | None
    reasoning: str
    agreed: bool
    skipped_reason: str | None = None
    # Immutable UUIDv7 lineage id for /txn/{id} link-building. Optional
    # because old serialized audits in ``day_reviews.ai_audit_result``
    # don't carry it; consumers should fall back to ``txn_hash`` when
    # this is None... but post-v3 every fresh audit has one.
    lamella_txn_id: str | None = None


async def summarize_day(
    client: OpenRouterClient,
    *,
    day: date,
    day_note: str | None,
    transactions: list,
    mileage: list,
    paperless: list,
    flag_notes: list[str],
    model: str | None = None,
) -> str | None:
    """Render the ``summarize_day.j2`` prompt, call the AI, return
    the narrative text. Returns None on error (logged)."""
    prompt = render(
        "summarize_day.j2",
        day=day,
        day_note=(day_note or "").strip() or None,
        transactions=transactions,
        mileage=mileage,
        paperless=paperless,
        flag_notes=flag_notes,
    )
    input_ref = f"day:{day.isoformat()}"
    try:
        result: AIResult[_SummaryResponse] = await client.chat(
            decision_type="summarize_day",
            input_ref=input_ref,
            system=SUMMARY_SYSTEM,
            user=prompt,
            schema=_SummaryResponse,
            model=model,
        )
    except AIError as exc:
        log.warning("summarize_day failed for %s: %s", day.isoformat(), exc)
        return None
    return result.data.summary.strip()


async def audit_day(
    client: OpenRouterClient,
    *,
    day: date,
    transactions: list,
    active_notes: Iterable,
    mileage_entries: Iterable,
    receipt_by_hash: dict[str, Any] | None = None,
    entity_accounts_by_entity: dict[str, list[str]] | None = None,
    resolve_entity,
    model: str | None = None,
    on_progress=None,
    loan_tracked_paths: set[str] | None = None,
) -> list[AuditEntry]:
    """Re-classify every transaction on ``day`` with full day
    context and return the disagreements + agreements.

    The audit is read-only; it does NOT write overrides, NOT
    decrement merchant memory, NOT touch the review queue. The
    caller is responsible for presenting the result in the UI
    and letting the user decide whether to act on disagreements
    through the normal /card or /review surfaces.

    Each txn becomes its own ``propose_account`` call. Expensive
    but bounded — the day view typically shows < 20 txns.

    WP6 Site 5: same reasoning as Site 4 (ai/audit.py) — skip AI
    for transactions touching a loan's configured accounts because
    the loan module already wrote the correct split with
    deterministic context, and re-running the AI against partial
    accumulated context would produce low-quality flags. Callers
    pass ``loan_tracked_paths`` as the set of configured
    liability/interest/escrow paths across all active loans; each
    day-view txn whose account_summary touches any of those is
    recorded as a skipped entry rather than sent to propose_account.
    """
    from lamella.features.ai_cascade.classify import propose_account
    from lamella.features.ai_cascade.context import resolve_entity_for_account

    out: list[AuditEntry] = []
    total = len(transactions)
    for idx, t in enumerate(transactions):
        if on_progress is not None:
            on_progress(idx, total, t)

        # WP6 Site 5 preemption. account_summary is "from → to";
        # either side matching a loan-tracked path is enough to
        # flag a skip. See Site 4 comment in ai/audit.py for the
        # full "wrong context window" rationale.
        if loan_tracked_paths and t.account_summary and "→" in t.account_summary:
            sides = [s.strip() for s in t.account_summary.split("→", 1)]
            if any(s in loan_tracked_paths for s in sides):
                out.append(
                    AuditEntry(
                        txn_hash=t.txn_hash,
                        lamella_txn_id=getattr(t, "lamella_txn_id", None),
                        narration=t.narration,
                        amount=str(t.amount),
                        current_account=t.account_summary,
                        proposed_account=None,
                        confidence=None,
                        reasoning="",
                        agreed=False,
                        skipped_reason="loan-claimed (WP6 Site 5 preemption)",
                    )
                )
                continue

        if not t.account_summary or "→" not in t.account_summary:
            out.append(
                AuditEntry(
                    txn_hash=t.txn_hash,
                    lamella_txn_id=getattr(t, "lamella_txn_id", None),
                    narration=t.narration,
                    amount=str(t.amount),
                    current_account=t.account_summary,
                    proposed_account=None,
                    confidence=None,
                    reasoning="",
                    agreed=False,
                    skipped_reason="could not determine current account",
                )
            )
            continue

        _, current_to = [s.strip() for s in t.account_summary.split("→", 1)]
        fixme_root = current_to.split(":", 1)[0] if current_to else "Expenses"

        # Card account is the from-side of the summary.
        card_account, _ = [s.strip() for s in t.account_summary.split("→", 1)]

        # Build a TxnForClassify mirroring what the classify route
        # would assemble. We don't have vector-search similar txns
        # wired up here; passing empty list is acceptable since the
        # audit is a point-in-time "does the AI still think X?" check.
        txn_view = TxnForClassify(
            date=t.date,
            amount=Decimal(t.amount),
            currency=t.currency,
            payee=None,
            narration=t.narration,
            card_account=card_account,
            fixme_account=current_to if t.is_fixme else "Expenses:FIXME",
            txn_hash=t.txn_hash,
        )

        entity = resolve_entity(card_account) if resolve_entity else None
        valid = (
            entity_accounts_by_entity.get(entity, [])
            if entity_accounts_by_entity and entity
            else []
        )
        if not valid and entity_accounts_by_entity:
            # Union across every entity — lets the AI pick cross-entity.
            union: list[str] = []
            seen: set[str] = set()
            for accts in entity_accounts_by_entity.values():
                for a in accts:
                    if a not in seen:
                        union.append(a)
                        seen.add(a)
            valid = union

        try:
            proposal = await propose_account(
                client,
                txn=txn_view,
                similar=[],
                valid_accounts=valid,
                entity=entity,
                model=model,
                active_notes=list(active_notes),
                accounts_by_entity=entity_accounts_by_entity or {},
                card_suspicion=None,
                receipt=(receipt_by_hash or {}).get(t.txn_hash),
                mileage_entries=list(mileage_entries),
                fixme_root=fixme_root,
            )
        except AIError as exc:
            log.warning("audit_day AI call failed for %s: %s", t.txn_hash[:12], exc)
            out.append(
                AuditEntry(
                    txn_hash=t.txn_hash,
                    lamella_txn_id=getattr(t, "lamella_txn_id", None),
                    narration=t.narration,
                    amount=str(t.amount),
                    current_account=current_to,
                    proposed_account=None,
                    confidence=None,
                    reasoning="",
                    agreed=False,
                    skipped_reason=f"AI error: {exc}",
                )
            )
            continue

        if proposal is None:
            out.append(
                AuditEntry(
                    txn_hash=t.txn_hash,
                    lamella_txn_id=getattr(t, "lamella_txn_id", None),
                    narration=t.narration,
                    amount=str(t.amount),
                    current_account=current_to,
                    proposed_account=None,
                    confidence=None,
                    reasoning="",
                    agreed=False,
                    skipped_reason="AI declined (off-whitelist or no answer)",
                )
            )
            continue

        agreed = proposal.target_account == current_to
        out.append(
            AuditEntry(
                txn_hash=t.txn_hash,
                lamella_txn_id=getattr(t, "lamella_txn_id", None),
                narration=t.narration,
                amount=str(t.amount),
                current_account=current_to,
                proposed_account=proposal.target_account,
                confidence=proposal.confidence,
                reasoning=proposal.reasoning or "",
                agreed=agreed,
            )
        )
    return out


def audit_entries_to_json(entries: list[AuditEntry]) -> list[dict]:
    return [asdict(e) for e in entries]
