# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from typing import Iterable

from pydantic import BaseModel, Field, field_validator

from lamella.adapters.openrouter.client import AIError, AIResult, OpenRouterClient
from lamella.features.ai_cascade.context import (
    CardBindingSuspicion,
    SimilarTxn,
    TxnForClassify,
    all_expense_accounts_by_entity,
    entity_from_card,
    extract_fixme_txn,
    render,
    resolve_entity_for_account,
    similar_transactions,
    suspicious_card_binding,
    valid_accounts_by_root,
    valid_expense_accounts,
)
from lamella.features.ai_cascade.gating import AIProposal
from lamella.features.ai_cascade.mileage_context import (
    MileageContextEntry,
    VehicleLogDensity,
    mileage_context_for_txn,
    vehicle_log_density,
)
from lamella.features.ai_cascade.receipt_context import (
    DocumentContext,
    fetch_document_context,
)

log = logging.getLogger(__name__)


def _vector_search_enabled(conn) -> bool:
    """Phase H — default ON. Reads ``app_settings`` for an explicit
    override; only a ``false``-shaped value there turns the feature
    off. A missing row or an unreadable ``app_settings`` table
    defaults to the config default (True), matching
    ``Settings.ai_vector_search_enabled``.
    """
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'ai_vector_search_enabled'"
        ).fetchone()
    except Exception:  # noqa: BLE001
        return True
    if row is None:
        return True
    val = str(row["value"] or "").strip().lower()
    return val not in ("0", "false", "no", "off")


SYSTEM = (
    "You are a meticulous bookkeeper. You classify transactions into a "
    "predefined chart of accounts. Never invent accounts. Prefer the "
    "history of past user decisions when available."
)


class ClassifyResponse(BaseModel):
    target_account: str = Field(min_length=3)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""
    # Phase G4 — intercompany detection on the classifier path.
    # The AI sets these when it believes the charge belongs to a
    # different entity than the card that was physically charged.
    # When intercompany_flag is True, the gate NEVER auto-applies;
    # the outcome always goes to human review regardless of
    # confidence.
    intercompany_flag: bool = False
    owning_entity: str | None = None

    @field_validator("target_account")
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()


async def propose_account(
    client: OpenRouterClient,
    *,
    txn: TxnForClassify,
    similar: Iterable[SimilarTxn],
    valid_accounts: list[str],
    entity: str | None,
    model: str | None = None,
    registry_preamble: str | None = None,
    active_notes: Iterable | None = None,
    accounts_by_entity: dict[str, list[str]] | None = None,
    card_suspicion: CardBindingSuspicion | None = None,
    receipt: DocumentContext | None = None,
    mileage_entries: Iterable | None = None,
    vehicle_density: Iterable | None = None,
    fallback_model: str | None = None,
    fallback_threshold: float = 0.0,
    account_descriptions: dict[str, str] | None = None,
    entity_context: str | None = None,
    active_projects: list | None = None,
    fixme_root: str = "Expenses",
) -> AIProposal | None:
    """Return an `AIProposal` for the FIXME in `txn`, or None on error.

    ``active_notes`` are free-text context notes the user wrote whose
    ``active_from..active_to`` window covers ``txn.date``. They are
    rendered verbatim in the prompt so the model can use them as
    strong contextual priors (e.g. "in Atlanta April 14–20 for ACME
    trade show — charges this week are business travel"). See the
    rules-are-directional feedback memory: notes are a signal the
    AI consumes alongside other context, not a hard override.
    """
    if not valid_accounts:
        log.info("classify: no valid accounts for entity=%r — skipping AI call", entity)
        return None

    # Phase G4 — if the caller supplied a cross-entity grouping
    # (``accounts_by_entity``), render that block. Otherwise fall
    # back to the flat whitelist for the card-binding entity.
    user_prompt = render(
        "classify_txn.j2",
        txn=txn,
        similar=list(similar),
        entity=entity,
        accounts=valid_accounts,
        accounts_by_entity=accounts_by_entity or {},
        registry_preamble=registry_preamble or "",
        active_notes=list(active_notes) if active_notes else [],
        card_suspicion=card_suspicion,
        receipt=receipt,
        mileage_entries=list(mileage_entries) if mileage_entries else [],
        vehicle_density=list(vehicle_density) if vehicle_density else [],
        account_descriptions=account_descriptions or {},
        entity_context=(entity_context or "").strip() or None,
        active_projects=list(active_projects) if active_projects else [],
        fixme_root=fixme_root,
    )

    # Phase G4 — widen the guard. When a cross-entity whitelist was
    # rendered into the prompt, the valid set is the union of every
    # entity's accounts, not just the card-entity's. That lets the
    # AI legitimately pick an out-of-entity account and flag
    # intercompany.
    allowed: set[str] = set(valid_accounts)
    if accounts_by_entity:
        for accts in accounts_by_entity.values():
            allowed.update(accts)

    # Phase 3: prefer the entry's stable lineage id over the content
    # hash. ``txn.lamella_txn_id`` is None for legacy entries (pre-
    # Phase-4 transform); fall back to ``txn_hash`` so AI history
    # still pins to the entry on those.
    classify_input_ref = txn.lamella_txn_id or txn.txn_hash
    try:
        result: AIResult[ClassifyResponse] = await client.chat(
            decision_type="classify_txn",
            input_ref=classify_input_ref,
            system=SYSTEM,
            user=user_prompt,
            schema=ClassifyResponse,
            model=model,
        )
    except AIError as exc:
        log.warning("classify_txn failed for %s: %s", txn.txn_hash[:12], exc)
        return None

    primary = _proposal_from_result(
        result, allowed=allowed, card_suspicion=card_suspicion,
        txn_hash=txn.txn_hash, escalated_from=None,
    )

    # Two-agent cascade. If the primary model came back with low
    # confidence (or got suppressed off-whitelist) and a stronger
    # fallback model is configured, retry once and prefer the
    # fallback's answer. Same prompt — the point is that a more
    # capable model may resolve the ambiguity the cheap one
    # flagged.
    should_escalate = (
        fallback_model
        and fallback_model != model
        and (primary is None or primary.confidence < fallback_threshold)
    )
    if should_escalate:
        try:
            fb_result: AIResult[ClassifyResponse] = await client.chat(
                decision_type="classify_txn",
                input_ref=classify_input_ref,
                system=SYSTEM,
                user=user_prompt,
                schema=ClassifyResponse,
                model=fallback_model,
            )
        except AIError as exc:
            log.warning(
                "classify_txn fallback (%s) failed for %s: %s",
                fallback_model, txn.txn_hash[:12], exc,
            )
        else:
            escalated = _proposal_from_result(
                fb_result, allowed=allowed, card_suspicion=card_suspicion,
                txn_hash=txn.txn_hash,
                escalated_from=model or "primary",
            )
            if escalated is not None:
                return escalated

    return primary


def _proposal_from_result(
    result: AIResult[ClassifyResponse],
    *,
    allowed: set[str],
    card_suspicion: CardBindingSuspicion | None,
    txn_hash: str,
    escalated_from: str | None,
) -> AIProposal | None:
    data = result.data
    if data.target_account not in allowed:
        log.info(
            "classify_txn returned off-whitelist %r for %s — suppressing",
            data.target_account,
            txn_hash[:12],
        )
        return None
    # Phase G3 safety net — if the merchant histogram says this
    # card binding is suspicious, force intercompany_flag=True even
    # if the AI didn't set it. The gate's never-auto-apply rule
    # then keeps the decision on the review queue until the user
    # confirms.
    intercompany_flag = bool(data.intercompany_flag)
    owning_entity = data.owning_entity
    if card_suspicion is not None:
        intercompany_flag = True
        owning_entity = owning_entity or card_suspicion.dominant_entity
    return AIProposal(
        target_account=data.target_account,
        confidence=float(data.confidence),
        reasoning=data.reasoning,
        decision_id=result.decision_id,
        intercompany_flag=intercompany_flag,
        owning_entity=owning_entity,
        escalated_from=escalated_from,
    )


def build_classify_context(
    *,
    entries,
    txn,
    conn=None,
) -> tuple[
    TxnForClassify | None,
    list[SimilarTxn],
    list[str],
    str | None,
    list,
    CardBindingSuspicion | None,
    dict[str, list[str]] | None,
    DocumentContext | None,
    list[MileageContextEntry],
    list[VehicleLogDensity],
]:
    """Assemble the inputs `propose_account` needs from a raw Transaction.

    When ``conn`` is supplied, also pulls active notes via
    ``NoteService.notes_active_on(txn.date, entity=entity,
    card=card_account)`` so the classifier sees user-authored date-
    ranged context. Pass ``conn=None`` (or legacy callers unpack
    only the first four return values) to skip the notes pull.
    """
    view = extract_fixme_txn(txn)
    if view is None:
        return None, [], [], None, [], None, None, None, [], []
    # Phase G2: prefer the registry's entity binding when conn is
    # available; fall back to the string-split heuristic otherwise.
    entity = (
        resolve_entity_for_account(conn, view.card_account)
        if conn is not None
        else entity_from_card(view.card_account)
    )
    # AI-AGENT.md Phase 2 — txn type inference from the FIXME posting
    # root. Today classify only handles expense FIXMEs; with the
    # vector index widened we can now route Income:FIXME to income
    # attribution, Liabilities:FIXME to CC-payment / loan-destination
    # resolution, etc. The root drives (a) which target_roots we
    # query the vector index with and (b) which account whitelist
    # we offer the AI. Falls through to Expenses behavior for every
    # txn the previous pipeline was built around.
    fixme_root = (view.fixme_account or "").split(":", 1)[0] or "Expenses"
    if fixme_root not in ("Expenses", "Income", "Liabilities", "Equity", "Assets"):
        fixme_root = "Expenses"
    # Sign-aware re-routing for already-staged rows. When a row was
    # ingested under the pre-sign-aware code, its FIXME placeholder is
    # always Expenses:{entity}:FIXME — even for deposits. The view's
    # `amount` is the FIXME-leg amount (signed opposite of the
    # bank-side leg per beancount's balanced-txn rule).
    #
    # Universal convention: a NEGATIVE FIXME amount means the bank
    # side was POSITIVE — money IN to the user (deposit on an asset,
    # refund/paydown on a liability). Both belong in Income's box.
    # Verified against actual ledger writes in writer.py:166-172 and
    # real entries (Liabilities:CC -9.49 = charge, +20.95 = refund).
    if (
        fixme_root == "Expenses"
        and view.amount is not None
        and view.amount < 0
    ):
        fixme_root = "Income"
    needle = (view.payee or view.narration or "").strip()
    # Phase H — opt-in vector search. When enabled + conn is present +
    # sentence-transformers is installed, semantic retrieval replaces
    # the 180-day substring window. Falls back to substring on any
    # failure so the classify path never hard-depends on Phase H.
    similar: list[SimilarTxn] = []
    if conn is not None and _vector_search_enabled(conn):
        try:
            from lamella.features.ai_cascade.decisions import DecisionsLog
            from lamella.features.ai_cascade.vector_index import (
                VectorUnavailable,
                similar_transactions_via_vector,
            )
            # Build a cheap ledger signature so the index knows
            # when to rebuild. Using len(entries) + latest date is
            # a coarse but effective fingerprint; a more-specific
            # signature from LedgerReader.mtime_signature would be
            # marginally stricter.
            last_date = ""
            for e in entries:
                d = getattr(e, "date", None)
                if d is not None:
                    iso = d.isoformat()
                    if iso > last_date:
                        last_date = iso
            # Include user-correction count + latest-correction id
            # so corrections to existing ledger rows (which don't
            # change entries length or max_date) still invalidate
            # the index. Otherwise the classify path would keep
            # serving stale embeddings after every review session.
            try:
                correction_row = conn.execute(
                    "SELECT COUNT(*) AS n, MAX(id) AS last_id "
                    "FROM ai_decisions "
                    "WHERE decision_type = 'classify_txn' "
                    "  AND user_corrected = 1"
                ).fetchone()
                correction_n = int(correction_row["n"] or 0) if correction_row else 0
                correction_last = (
                    int(correction_row["last_id"] or 0)
                    if correction_row else 0
                )
            except Exception:  # noqa: BLE001 — cache table optional
                correction_n, correction_last = 0, 0
            sig = (
                f"{len(entries)}:{last_date}"
                f":c{correction_n}:l{correction_last}"
            )
            similar = similar_transactions_via_vector(
                conn, entries, needle=needle,
                reference_date=view.date,
                ai_decisions=DecisionsLog(conn),
                ledger_signature=sig,
                target_roots=(fixme_root,),
            )
        except VectorUnavailable:
            log.info(
                "vector search enabled but sentence-transformers "
                "unavailable; falling back to substring"
            )
            similar = []
        except Exception:  # noqa: BLE001
            log.exception("vector search failed; falling back to substring")
            similar = []
    if not similar:
        similar = similar_transactions(
            entries, needle=needle, reference_date=view.date,
        )
    # Whitelist — pulled from the FIXME root so Income:FIXME gets
    # Income:* suggestions, Liabilities:FIXME gets Liabilities:*, etc.
    # For Expenses this is byte-identical to the prior
    # valid_expense_accounts() call.
    accounts = valid_accounts_by_root(entries, root=fixme_root, entity=entity)
    active_notes: list = []
    if conn is not None:
        try:
            from lamella.features.notes.service import NoteService
            active_notes = NoteService(conn).notes_active_on(
                view.date, entity=entity, card=view.card_account,
                txn_hash=view.txn_hash,
            )
        except Exception:  # noqa: BLE001
            log.warning(
                "classify: notes_active_on failed for txn on %s", view.date,
                exc_info=True,
            )
    # Phase G7 — if an active note declares card_override with a
    # valid entity_hint, swap the working entity to the note's
    # entity and widen the whitelist cross-entity. The prompt's
    # active-notes block already warns the model about the
    # override; switching the entity here ensures the account
    # whitelist reflects the new binding.
    accounts_by_entity: dict[str, list[str]] | None = None
    override_note = next(
        (n for n in active_notes
         if getattr(n, "card_override", False) and n.entity_hint),
        None,
    )
    if override_note is not None:
        entity = override_note.entity_hint
        accounts_by_entity = all_expense_accounts_by_entity(entries)
        # Flatten to populate the legacy flat-list accounts (used by
        # the off-whitelist guard in propose_account's allowed set).
        accounts = sorted({a for lst in accounts_by_entity.values() for a in lst})

    # Cross-entity expansion driven by prior user rejections. If the
    # user has previously rejected this txn and explicitly named an
    # account in a DIFFERENT entity (e.g., card is Personal but user
    # said "Expenses:AJQuick:OfficeExpense"), populate the
    # cross-entity whitelist so the next AI pass can actually pick
    # from the user-named entity. Without this, the AI is stuck in
    # the card's entity and silently maps the user's hint to the
    # closest same-entity account.
    if conn is not None and accounts_by_entity is None:
        try:
            _prior = prior_attempts_for_txn(conn, view.txn_hash)
        except Exception:  # noqa: BLE001
            _prior = []
        if _prior:
            import re as _re_inner
            mentioned_entities: set[str] = set()
            for attempt in _prior:
                # Look at both the user_decision text and any
                # account-paths referenced in it.
                ud = attempt.get("user_decision") or ""
                for m in _re_inner.findall(
                    r"Expenses:([A-Za-z][A-Za-z0-9_]+):", ud,
                ):
                    if m and m != entity:
                        mentioned_entities.add(m)
            if mentioned_entities:
                accounts_by_entity = all_expense_accounts_by_entity(entries)
                accounts = sorted({
                    a for lst in accounts_by_entity.values() for a in lst
                })

    # Phase G3 — compute merchant-entity suspicion if we have a
    # merchant text to look up. The helper returns None when the
    # history supports the card binding, so fresh ledgers and
    # unambiguous cases are no-ops.
    merchant = (view.payee or view.narration or "").strip()
    card_suspicion = suspicious_card_binding(
        entries, merchant=merchant, card_entity=entity,
    ) if merchant else None

    # Paperless receipt context. Tries the linked-receipt path first
    # (when the txn is already in the ledger), falls back to
    # candidate-by-amount+date for new-txn classification. Silent
    # None when the Paperless index isn't populated.
    #
    # Suppress entirely while a Paperless sync is in flight — the
    # index is mid-growth and a candidate-match produces stale
    # answers. Better to classify without receipt context than to
    # attach the wrong doc.
    receipt: DocumentContext | None = None
    if conn is not None:
        try:
            from lamella.features.paperless_bridge.sync import is_paperless_syncing
            if is_paperless_syncing(conn):
                log.info(
                    "classify: skipping receipt context — "
                    "Paperless sync in flight"
                )
            else:
                receipt = fetch_document_context(
                    conn,
                    txn_hash=view.txn_hash or None,
                    posting_date=view.date,
                    amount=view.amount,
                )
        except Exception:  # noqa: BLE001
            log.warning(
                "classify: paperless receipt context failed",
                exc_info=True,
            )

    # Mileage log context. A Warehouse Club-fuel txn on its own can't be
    # attributed to one of several vehicles; a mileage entry the
    # same day "drove to Warehouse Club for gas — Acme Cargo Van" pins
    # it to the right vehicle + entity. Same proximity model as
    # notes_active_on (default ±3 days).
    mileage_entries: list[MileageContextEntry] = []
    if conn is not None:
        try:
            mileage_entries = mileage_context_for_txn(
                conn, txn_date=view.date, entity=entity,
            )
        except Exception:  # noqa: BLE001
            log.warning(
                "classify: mileage_context_for_txn failed",
                exc_info=True,
            )

    # Per-vehicle log density. Ensures the AI treats a vehicle's
    # absence from the day's log conditionally — informative for a
    # densely-logged vehicle, noise for a sparsely-logged one.
    log_density: list[VehicleLogDensity] = []
    if conn is not None:
        try:
            log_density = vehicle_log_density(
                conn, as_of_date=view.date,
            )
        except Exception:  # noqa: BLE001
            log.warning(
                "classify: vehicle_log_density failed",
                exc_info=True,
            )

    return (
        view, similar, accounts, entity, active_notes,
        card_suspicion, accounts_by_entity, receipt, mileage_entries,
        log_density,
    )


def prior_attempts_for_txn(
    conn, txn_hash: str, *, limit: int = 5,
) -> list[dict]:
    """Return prior AI classify attempts on this txn that the user
    has acted on (approved / rejected / corrected). Each row is a
    dict ready for the prompt template:

      {
        "decided_at": "...",
        "model": "...",
        "target": "...",
        "confidence": 0.75,
        "reasoning": "...",
        "user_decision": "rejected: <user's words>",
      }

    Used by the bulk-classify path so the next AI pass on a FIXME
    that was previously rejected sees both its own prior attempt(s)
    AND the user's reasoning. Without this, every re-classify would
    be amnesia: the AI re-derives the same answer from the same
    inputs and the user has to reject again. With it, the prompt
    can explicitly say "the user rejected your previous answer X
    because Y — try a different angle."
    """
    import json as _json
    if conn is None or not txn_hash:
        return []
    try:
        # Exclude decisions that were marked corrected only because a
        # NEWER attempt replaced them (user_correction LIKE
        # "superseded by #N"). Those aren't real user feedback — they're
        # just stale duplicates from re-runs. Feeding them back to the
        # AI as "user said: superseded by #21" would confuse more than
        # help. Only surface explicit approve / reject / corrected
        # decisions as prior attempts.
        rows = conn.execute(
            """
            SELECT id, decided_at, model, result, user_correction
              FROM ai_decisions
             WHERE decision_type = 'classify_txn'
               AND input_ref = ?
               AND user_corrected = 1
               AND (user_correction IS NULL
                    OR user_correction NOT LIKE 'superseded by %')
             ORDER BY decided_at DESC
             LIMIT ?
            """,
            (txn_hash, limit),
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning("prior_attempts_for_txn lookup failed: %s", exc)
        return []
    out: list[dict] = []
    for r in rows:
        try:
            result = _json.loads(r["result"]) if r["result"] else {}
        except Exception:  # noqa: BLE001
            result = {}
        out.append({
            "decided_at": str(r["decided_at"]),
            "model": r["model"] or "(unknown)",
            "target": result.get("target_account") or "",
            "confidence": result.get("confidence"),
            "reasoning": result.get("reasoning") or "",
            "user_decision": r["user_correction"] or "",
        })
    return out


def load_entity_context(conn, entity: str | None) -> str | None:
    """Fetch user-written rich context for an entity —
    "Acme LLC, formed 2008, handles widget merchandise.
    Income via eBay + Shopify. Expenses heavy on shipping."
    Shown as STRONG BACKGROUND to the classifier for every
    charge on that entity's cards. None if missing, column
    doesn't exist yet (pre-migration), or conn is None."""
    if conn is None or not entity:
        return None
    try:
        row = conn.execute(
            "SELECT classify_context FROM entities WHERE slug = ?",
            (entity,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    if row is None:
        return None
    value = (row["classify_context"] or "").strip()
    return value or None


def load_account_descriptions(conn) -> dict[str, str]:
    """Fetch user-written plain-English context for every account
    from ``account_classify_context``. Returns
    {account_path: description}. Classify renders these alongside
    the whitelist so the AI has context even for brand-new,
    history-less accounts. Empty dict on any failure (table
    missing, conn None) — classify falls back to the whitelist
    alone."""
    if conn is None:
        return {}
    try:
        rows = conn.execute(
            "SELECT account_path, description "
            "FROM account_classify_context"
        ).fetchall()
    except Exception:  # noqa: BLE001 — migration optional at first boot
        return {}
    return {
        row["account_path"]: (row["description"] or "").strip()
        for row in rows
        if row["description"] and str(row["description"]).strip()
    }
