# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Iterable

# Transfer-suspect heuristic — shared module so widening the
# pattern set is a one-place change. See lamella.core.transfer_heuristic
# for the regex catalogue (transfer/xfer + liability-payment
# language). Suppressing low-confidence Expenses proposals on
# transfer-suspect rows matches the user-facing /review hint.
from lamella.core.transfer_heuristic import looks_like_transfer_text


def _looks_like_transfer_view(view) -> bool:
    """True when the AI input has narration/payee that looks like a
    transfer. Used to suppress low-confidence Expenses proposals
    on rows that the user-facing review heuristic has already flagged
    "Looks like a transfer" — the AI itself usually declines verbally
    on these but the enricher still stored the guess as a Proposed
    CTA, which contradicts the hint."""
    pieces = []
    for attr in ("payee", "narration", "description"):
        val = getattr(view, attr, None)
        if val:
            pieces.append(str(val))
    return looks_like_transfer_text(" ".join(pieces))

from beancount.core.data import Transaction

from lamella.features.ai_cascade.classify import (
    build_classify_context,
    load_account_descriptions,
    load_entity_context,
    propose_account,
)
from lamella.core.registry.ai_context import registry_preamble as build_registry_preamble
from lamella.features.ai_cascade.gating import (
    AIProposal,
    RuleProposal,
)
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader, txn_hash
from lamella.features.review_queue.service import ReviewItem, ReviewService
from lamella.features.rules.scanner import combined_suggestion
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)


def _txn_by_hash(entries: Iterable, target: str) -> Transaction | None:
    for entry in entries:
        if isinstance(entry, Transaction) and txn_hash(entry) == target:
            return entry
    return None


def _rule_payload(item: ReviewItem) -> dict | None:
    raw = item.ai_suggestion
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    return data.get("rule")


def _has_ai_entry(item: ReviewItem) -> bool:
    raw = item.ai_suggestion
    if not raw:
        return False
    try:
        data = json.loads(raw)
    except ValueError:
        return False
    return isinstance(data, dict) and "ai" in data


def _rule_proposal_from_payload(payload: dict | None) -> RuleProposal | None:
    if not payload:
        return None
    try:
        return RuleProposal(
            rule_id=int(payload["rule_id"]),
            target_account=str(payload["target_account"]),
            confidence=float(payload.get("confidence") or 0.0),
            created_by=str(payload.get("created_by") or "user"),
        )
    except (KeyError, TypeError, ValueError):
        return None


class AIFixmeEnricher:
    """Async job. For each open `fixme` review item without an AI
    decision, runs `classify.propose_account`, then writes the AI
    suggestion onto the review row so the UI can show it next to any
    rule-based suggestion. Post-workstream-A the enricher never
    writes to the ledger — the gate only produces REVIEW_WITH_*
    outcomes for AI proposals; ledger writes flow through user
    click-accept in the review UI."""

    def __init__(
        self,
        *,
        ai: AIService,
        reader: LedgerReader,
        reviews: ReviewService,
        rules: RuleService,
        paperless_client_factory=None,
    ):
        self.ai = ai
        self.reader = reader
        self.reviews = reviews
        self.rules = rules
        # Callable that returns a fresh `PaperlessClient` or None
        # if Paperless isn't configured. Threaded through so the
        # enricher can kick off verify-and-writeback jobs without
        # taking a hard dep on app state.
        self.paperless_client_factory = paperless_client_factory

    def _writeback_enabled(self) -> bool:
        raw = self.ai.settings_store.get("paperless_writeback_enabled")
        if raw is None:
            return bool(self.ai.settings.paperless_writeback_enabled)
        return str(raw).strip().lower() not in ("0", "false", "no", "off")

    async def run(self, *, limit: int = 25) -> dict[str, int]:
        stats = {"considered": 0, "enriched": 0, "auto_applied": 0, "errors": 0}
        if not self.ai.enabled or self.ai.spend_cap_reached():
            return stats

        items = self.reviews.list_open_by_kind("fixme", limit=limit)
        if not items:
            return stats

        client = self.ai.new_client()
        if client is None:
            return stats

        # Lazily-built Paperless client for writeback (Slice A/C).
        # Held open for the duration of this run; closed in finally.
        paperless_client = None
        verify_service = None
        if (
            self.paperless_client_factory is not None
            and self._writeback_enabled()
        ):
            try:
                paperless_client = self.paperless_client_factory()
            except Exception as exc:  # noqa: BLE001
                log.warning("enricher: paperless client factory failed: %s", exc)
            if paperless_client is not None:
                from lamella.features.paperless_bridge.verify import VerifyService
                verify_service = VerifyService(
                    ai=self.ai,
                    paperless=paperless_client,
                    conn=self.ai.conn,
                )

        try:
            preamble = ""
            conn = getattr(self.reviews, "conn", None)
            try:
                if conn is not None:
                    preamble = build_registry_preamble(conn)
            except Exception as exc:  # noqa: BLE001
                log.warning("registry preamble build failed: %s", exc)

            # WP6 Site 3 — build the loans snapshot once per enricher
            # run so each review-queue item's claim check is O(1) over
            # the cached list. Defaults to running every 15 min via
            # the main.py scheduler; the snapshot is short-lived and
            # the enricher itself doesn't edit loans.
            from lamella.features.loans import auto_classify as _auto_classify
            from lamella.features.loans.claim import (
                is_claimed_by_loan as _is_claimed_by_loan,
                load_loans_snapshot as _load_loans_snapshot,
            )
            _loans_cache = (
                _load_loans_snapshot(conn) if conn is not None else []
            )
            _loan_rows_by_slug: dict[str, dict] = {}

            def _load_loan(slug: str) -> dict | None:
                if slug in _loan_rows_by_slug:
                    return _loan_rows_by_slug[slug]
                if conn is None:
                    return None
                row = conn.execute(
                    "SELECT * FROM loans WHERE slug = ?", (slug,),
                ).fetchone()
                loan = dict(row) if row is not None else None
                if loan is not None:
                    _loan_rows_by_slug[slug] = loan
                return loan

            ledger = self.reader.load()
            ledger_invalidated = False
            for item in items:
                stats["considered"] += 1
                if _has_ai_entry(item):
                    continue
                th = (
                    item.source_ref.split(":", 1)[1]
                    if item.source_ref.startswith("fixme:")
                    else None
                )
                if th is None:
                    continue
                txn = _txn_by_hash(ledger.entries, th)
                if txn is None:
                    continue

                # WP6 Site 3 — principle-3 preemption. A loan-claimed
                # FIXME never reaches propose_account. The loan module
                # writes the split (or defers to review for under/far
                # tiers); on successful write we flip the review row
                # to resolved so the UI doesn't keep surfacing it.
                if _loans_cache and conn is not None:
                    claim = _is_claimed_by_loan(txn, conn, loans=_loans_cache)
                    if claim is not None:
                        loan = _load_loan(claim.loan_slug)
                        if loan is not None:
                            try:
                                outcome = _auto_classify.process(
                                    claim, txn, loan,
                                    settings=self.ai.settings,
                                    reader=self.reader, conn=conn,
                                )
                                if outcome.wrote_override:
                                    self.reviews.resolve(
                                        item.id,
                                        user_decision=(
                                            f"auto_classified_by_loan_module"
                                            f"→{claim.loan_slug}"
                                            f"#{outcome.tier}"
                                        ),
                                    )
                                    stats["auto_applied"] += 1
                                    self.reader.invalidate()
                                    ledger_invalidated = True
                            except Exception as exc:  # noqa: BLE001
                                log.warning(
                                    "enricher: loan auto_classify failed "
                                    "for %s (slug=%s): %s",
                                    th[:12], claim.loan_slug, exc,
                                )
                                stats["errors"] += 1
                        continue  # claim fired — always skip AI
                (
                    view, similar, accounts, entity, active_notes,
                    card_suspicion, accounts_by_entity, receipt,
                    mileage_entries, vehicle_density,
                ) = build_classify_context(
                    entries=ledger.entries, txn=txn, conn=self.ai.conn,
                )
                if view is None:
                    continue
                fixme_root = (
                    (view.fixme_account or "").split(":", 1)[0] or "Expenses"
                )
                if fixme_root not in (
                    "Expenses", "Income", "Liabilities", "Equity", "Assets",
                ):
                    fixme_root = "Expenses"
                try:
                    proposal = await propose_account(
                        client,
                        txn=view,
                        similar=similar,
                        valid_accounts=accounts,
                        entity=entity,
                        model=self.ai.model_for("classify_txn"),
                        registry_preamble=preamble,
                        active_notes=active_notes,
                        card_suspicion=card_suspicion,
                        accounts_by_entity=accounts_by_entity,
                        receipt=receipt,
                        mileage_entries=mileage_entries,
                        vehicle_density=vehicle_density,
                        fallback_model=self.ai.fallback_model_for("classify_txn"),
                        fallback_threshold=self.ai.fallback_threshold(),
                        fixme_root=fixme_root,
                        account_descriptions=load_account_descriptions(
                            self.ai.conn,
                        ),
                        entity_context=load_entity_context(
                            self.ai.conn, entity,
                        ),
                        active_projects=_active_projects_for_view(
                            self.ai.conn, view,
                        ),
                    )
                except Exception as exc:  # defensive: never let one item kill the job
                    log.warning("ai enrich failed for %s: %s", th[:12], exc)
                    stats["errors"] += 1
                    continue
                if proposal is None:
                    continue

                # Transfer-suspect guard: when the row's narration
                # already looks like a transfer (matching the
                # /review "Looks like a transfer" hint heuristic)
                # AND the AI returned a low-confidence Expenses
                # target, drop the suggestion. The AI usually
                # declines verbally on these ("should go to review
                # rather than be classified by guess"); surfacing
                # the low-confidence Expenses guess as a Proposed
                # CTA contradicts the hint and tempts the user to
                # one-click-accept the wrong category.
                # AI history still records the proposal — only the
                # actionable suggestion band is suppressed.
                if (
                    _looks_like_transfer_view(view)
                    and proposal.confidence == "low"
                    and proposal.target_account.startswith("Expenses:")
                ):
                    log.info(
                        "ai enrich: suppressing low-conf Expenses "
                        "proposal on transfer-suspect row "
                        "(txn=%s target=%s)",
                        th[:12], proposal.target_account,
                    )
                    stats["enriched"] += 0  # not a real enrichment
                    stats.setdefault("suppressed_transfer_guess", 0)
                    stats["suppressed_transfer_guess"] += 1
                    continue

                rule_payload = _rule_payload(item)
                rule_prop = _rule_proposal_from_payload(rule_payload)
                outcome = self.ai.gate.decide(rule=rule_prop, ai=proposal)

                # When the cascade escalated, record the fallback
                # model as the "answering" model in the review UI.
                primary_model = self.ai.model_for("classify_txn")
                if proposal.escalated_from:
                    answering_model = (
                        self.ai.fallback_model_for("classify_txn") or primary_model
                    )
                else:
                    answering_model = primary_model
                ai_payload = _ai_payload(proposal, answering_model)
                suggestion = combined_suggestion(rule=rule_payload, ai=ai_payload)
                suggestion_json = json.dumps(suggestion) if suggestion else None

                # Post-workstream-A: the gate no longer emits
                # AUTO_APPLY_AI. High-confidence AI proposals land as
                # REVIEW_WITH_SUGGESTION — recorded on the review row
                # below; the user's click-accept is what promotes them.
                self.reviews.set_suggestion(
                    item.id,
                    ai_suggestion=suggestion_json or "",
                    ai_model=answering_model,
                )
                stats["enriched"] += 1

                # Slice A: fire verify-and-correct on a linked
                # receipt whose OCR looks wrong. Slice C: fire
                # enrichment when classify's context (mileage,
                # notes) points at something concrete we can push
                # back to Paperless. Both are best-effort — a
                # failure here never derails the enrichment loop.
                if verify_service is not None and receipt is not None:
                    await _maybe_writeback_for_receipt(
                        verify_service=verify_service,
                        receipt=receipt,
                        txn_date=view.date,
                        ai_decision_id=proposal.decision_id,
                        target_account=proposal.target_account,
                        mileage_entries=mileage_entries or [],
                        active_notes=active_notes or [],
                        entity=entity,
                        stats=stats,
                    )
            if ledger_invalidated:
                self.reader.invalidate()
        finally:
            await client.aclose()
            if paperless_client is not None:
                try:
                    await paperless_client.aclose()
                except Exception:  # noqa: BLE001
                    pass

        return stats

def _active_projects_for_view(conn, view) -> list:
    """Resolve projects that apply to a TxnForClassify view.
    Uses merchant text from payee+narration as the match target."""
    from lamella.features.projects.service import active_projects_for_txn
    merchant = " ".join(filter(None, [
        (view.payee or "").strip() or None,
        (view.narration or "").strip() or None,
    ])).strip()
    if not merchant:
        return []
    return active_projects_for_txn(
        conn, txn_date=view.date, merchant_text=merchant,
    )


async def _maybe_writeback_for_receipt(
    *,
    verify_service,
    receipt,
    txn_date: date,
    ai_decision_id: int | None,
    target_account: str,
    mileage_entries,
    active_notes,
    entity: str | None,
    stats: dict[str, int],
) -> None:
    """After classify resolves a txn with a linked Paperless
    receipt, push two kinds of improvement back to Paperless:

    1. If the receipt's OCR date looks wrong (date_mismatch_note
       set) we run a vision verify. The caller's hypothesis is
       the txn's date (strong prior: a credit-card-posted txn's
       date is almost always within a day or two of the
       receipt).
    2. If classify drew a conclusion that can be expressed as a
       Paperless note (vehicle pinned by mileage, project pinned
       by an active note), push that as an enrichment.
    """
    from lamella.features.paperless_bridge.verify import (
        EnrichmentContext, VerifyHypothesis,
    )
    paperless_id = getattr(receipt, "paperless_id", None)
    if not paperless_id:
        return

    # Slice A — verify if OCR date looks implausible.
    date_mismatch = getattr(receipt, "date_mismatch_note", None)
    if date_mismatch:
        try:
            result = await verify_service.verify_and_correct(
                int(paperless_id),
                hypothesis=VerifyHypothesis(
                    suspected_date=txn_date,
                    reason=(
                        "Transaction posted on {}; current OCR'd receipt "
                        "date is far from the txn date."
                    ).format(txn_date),
                ),
            )
            if result.changed_anything:
                stats["paperless_corrected"] = stats.get("paperless_corrected", 0) + 1
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "paperless verify (auto) failed for doc %s: %s",
                paperless_id, exc,
            )

    # Slice C — enrich with vehicle/entity context when the
    # classifier drew on mileage or notes. Build a human-readable
    # note body.
    note_body, vehicle = _enrichment_note_from_context(
        target_account=target_account,
        mileage_entries=mileage_entries,
        active_notes=active_notes,
        entity=entity,
        txn_date=txn_date,
    )
    if note_body:
        try:
            context = EnrichmentContext(
                vehicle=vehicle,
                entity=entity,
                note_body=note_body,
            )
            enrich_result = await verify_service.enrich_with_context(
                int(paperless_id),
                context=context,
                ai_decision_id=ai_decision_id,
            )
            if enrich_result.note_added or enrich_result.tag_applied:
                stats["paperless_enriched"] = stats.get("paperless_enriched", 0) + 1
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "paperless enrich (auto) failed for doc %s: %s",
                paperless_id, exc,
            )


def _enrichment_note_from_context(
    *,
    target_account: str,
    mileage_entries,
    active_notes,
    entity: str | None,
    txn_date: date,
) -> tuple[str, str | None]:
    """Compose a Paperless note body when classify's context
    contributed a concrete, reproducible fact. Returns
    (note_body, vehicle_slug) where vehicle_slug is non-None
    when a mileage entry pinned the vehicle."""
    # Mileage — pick the entry closest to the txn date if
    # multiple surfaced. The entity-ranked first result is the
    # strongest signal, but date-proximity beats ranking when
    # available.
    best_mileage = None
    if mileage_entries:
        best_mileage = min(
            mileage_entries,
            key=lambda m: abs((_parse_entry_date(m) - txn_date).days),
        )
    vehicle: str | None = None
    parts: list[str] = []
    if best_mileage is not None:
        vehicle = getattr(best_mileage, "vehicle", None)
        mileage_entity = getattr(best_mileage, "entity", None)
        purpose = getattr(best_mileage, "purpose", None)
        if vehicle:
            parts.append(f"Vehicle: {vehicle}")
        if mileage_entity and mileage_entity != entity:
            parts.append(f"Attributed entity: {mileage_entity}")
        if purpose:
            parts.append(f"Mileage purpose: {purpose}")
    # Active notes with project/trip hints.
    for note in active_notes or []:
        project = getattr(note, "project_hint", None) or ""
        if project:
            parts.append(f"Project: {project}")
            break
    if entity and not any(p.startswith("Attributed entity") for p in parts):
        parts.append(f"Entity: {entity}")
    if not parts:
        return "", None
    target_segment = target_account.split(":", 3)
    target_tail = ":".join(target_segment[-2:]) if len(target_segment) >= 2 else target_account
    note_body = (
        "🤖 Lamella classified this as "
        f"{target_account}.\n"
        + "\n".join(f"  • {p}" for p in parts)
    )
    return note_body, vehicle


def _parse_entry_date(entry) -> date:
    from datetime import date as _date
    value = getattr(entry, "entry_date", None) or getattr(entry, "date", None)
    if isinstance(value, _date):
        return value
    if isinstance(value, str):
        try:
            return _date.fromisoformat(value[:10])
        except ValueError:
            pass
    return _date.today()


def _ai_payload(proposal: AIProposal, model: str) -> dict:
    payload = {
        "target_account": proposal.target_account,
        "confidence": proposal.confidence,
        "reasoning": proposal.reasoning,
        "decision_id": proposal.decision_id,
        "model": model,
    }
    if proposal.escalated_from:
        payload["escalated_from"] = proposal.escalated_from
    return payload
