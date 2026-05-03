# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Unified transaction-action API.

The app's notion of "a transaction" comes in two flavors:

  * **Staged** — pending in ``staged_transactions`` (SimpleFIN / CSV /
    paste / reboot scan) and not yet in the ledger.
  * **Ledger** — a Beancount Transaction in a ``.bean`` file, possibly
    carrying a FIXME posting that needs categorizing.

Historically every surface (``/review``, ``/card``, ``/txn``, ``/search``,
``/ai/suggestions``, ``/audit``) has its own per-source endpoints for
the same logical operations: classify, dismiss, ask-AI, accept-proposal.
That fragmentation produces buggy edge cases and inconsistent UX.

This module is the start of the unified resource layer:

  POST /api/txn/{ref}/ask-ai     → submit AI re-classify job, return modal
  POST /api/txn/{ref}/classify   → planned: write target_account
  POST /api/txn/{ref}/dismiss    → planned: drop / dismiss
  GET  /api/txn/{ref}            → planned: unified context (date, amount, …)

``ref`` is one of:
  * ``staged:<id>``   for a row in ``staged_transactions``
  * ``ledger:<hash>`` for a Beancount Transaction by its txn_hash

Phase 1 (this commit): only ``ask-ai``. The Ask-AI result modal
(``partials/_ask_ai_result.html``) is generalized so its Accept and
Manual buttons know whether to post into the staged or ledger writer
based on the ref. The ``/ai/suggestions`` reject button uses this
endpoint to give users the "reject + try again" flow that previously
only existed in the staged-row Ask-AI modal.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from decimal import Decimal

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


# ─── Ref parsing ────────────────────────────────────────────────────


@dataclass(frozen=True)
class TxnRef:
    """Parsed ``ref`` — discriminator + id."""
    kind: str       # "staged" or "ledger"
    value: str      # staged_id (as str) or txn_hash

    @property
    def is_staged(self) -> bool:
        return self.kind == "staged"

    @property
    def is_ledger(self) -> bool:
        return self.kind == "ledger"

    @property
    def staged_id(self) -> int:
        if not self.is_staged:
            raise ValueError(f"ref {self} is not staged")
        return int(self.value)

    @property
    def txn_hash(self) -> str:
        if not self.is_ledger:
            raise ValueError(f"ref {self} is not ledger")
        return self.value

    def __str__(self) -> str:
        return f"{self.kind}:{self.value}"


def parse_ref(ref: str) -> TxnRef:
    """Split ``staged:123`` / ``ledger:abc123`` into a TxnRef.
    Raises HTTPException(400) for malformed input."""
    if not ref or ":" not in ref:
        raise HTTPException(
            status_code=400,
            detail=f"invalid txn ref {ref!r} — expected 'staged:<id>' or 'ledger:<hash>'",
        )
    kind, _, value = ref.partition(":")
    kind = kind.strip().lower()
    value = value.strip()
    if kind not in ("staged", "ledger") or not value:
        raise HTTPException(
            status_code=400,
            detail=f"invalid txn ref {ref!r} — expected 'staged:<id>' or 'ledger:<hash>'",
        )
    if kind == "staged":
        try:
            int(value)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"staged ref id must be integer, got {value!r}",
            )
    return TxnRef(kind=kind, value=value)


# ─── /api/txn/{ref}/classify ───────────────────────────────────────
#
# Unified write path. Dispatches by ref:
#   * staged:<id>   → delegates to staged_review_classify (the legacy
#                     handler is still the single source of truth for
#                     staged writes; this endpoint just forwards into it)
#   * ledger:<hash> → writes an Expenses:FIXME → target override via
#                     OverrideWriter, mirroring the search /txn/.../apply
#                     mode=categorize logic
# Both branches return an HX-Refresh response when the request is
# HTMX-driven (so the page reloads in place) or a 303 to return_url
# / a sensible default when called via vanilla form post.


@router.post("/api/txn/{ref}/classify", response_class=HTMLResponse)
async def api_txn_classify(
    ref: str,
    request: Request,
    target_account: str = Form(...),
    accept_proposed: str | None = Form(default=None),
    source: str | None = Form(default=None),
    save_rule: str | None = Form(default=None),
    rule_pattern_value: str | None = Form(default=None),
    return_url: str | None = Form(default=None),
    refund_of_txn_id: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Write a target account against the referenced transaction.

    Staged: delegates into the legacy classify handler so the
    SimpleFINWriter / reboot-override / pre-flight-scaffold logic
    keeps a single source of truth (avoid forking those branches).

    Ledger: writes an Expenses:FIXME → target override via
    OverrideWriter, after stripping any prior override targeting the
    same hash (otherwise two stacked overrides would double up the
    FIXME amount and bean-check rejects)."""
    parsed = parse_ref(ref)
    target_account = (target_account or "").strip()
    if not target_account:
        raise HTTPException(
            status_code=400, detail="target_account required",
        )

    landing = (return_url or "").strip() or "/review"

    if parsed.is_staged:
        # Delegate to the legacy staged classify handler. Its
        # function signature uses FastAPI Form() sentinels for
        # default values — calling the function directly with kwargs
        # bypasses dependency injection cleanly.
        from lamella.web.routes.staging_review import staged_review_classify
        return staged_review_classify(
            request=request,
            conn=conn,
            reader=reader,
            settings=settings,
            staged_id=parsed.staged_id,
            target_account=target_account,
            accept_proposed=accept_proposed,
            source=source,
            next_path=landing,
            refund_of_txn_id=refund_of_txn_id,
        )

    # Ledger path. Mirror /txn/{hash}/apply?mode=categorize but as a
    # standalone path so the unified URL does not need to redirect.
    return _do_ledger_classify(
        request=request,
        conn=conn,
        reader=reader,
        settings=settings,
        txn_hash=parsed.txn_hash,
        target_account=target_account,
        save_rule=save_rule,
        rule_pattern_value=rule_pattern_value,
        landing=landing,
        refund_of_txn_id=refund_of_txn_id,
    )


def _do_ledger_classify(
    *,
    request: Request,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    settings: Settings,
    txn_hash: str,
    target_account: str,
    save_rule: str | None,
    rule_pattern_value: str | None,
    landing: str,
    refund_of_txn_id: str | None = None,
):
    """Override-write path for a ledger Transaction. Strips any
    prior override targeting the same hash before appending the new
    one — without that, two stacked overrides double up the FIXME
    amount and bean-check rejects the second write."""
    from datetime import date as _date_t
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash as _th
    from lamella.core.ledger_writer import BeanCheckError
    from lamella.features.rules.overrides import OverrideWriter
    from lamella.features.rules.scanner import _is_fixme

    entries = list(reader.load().entries)
    target_txn: Transaction | None = None
    for e in entries:
        if isinstance(e, Transaction) and _th(e) == txn_hash:
            target_txn = e
            break
    if target_txn is None:
        raise HTTPException(
            status_code=404,
            detail=f"ledger txn {txn_hash[:16]}… not found",
        )

    fixme_leg = None
    for p in target_txn.postings or ():
        if _is_fixme(p.account or ""):
            if p.units and p.units.number is not None:
                fixme_leg = p
                break
    if fixme_leg is None:
        raise HTTPException(
            status_code=400,
            detail="no FIXME posting on this transaction to override",
        )

    abs_amount = abs(Decimal(fixme_leg.units.number))
    currency = fixme_leg.units.currency or "USD"
    txn_date = (
        target_txn.date if isinstance(target_txn.date, _date_t)
        else _date_t.fromisoformat(str(target_txn.date))
    )

    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        writer.rewrite_without_hash(txn_hash)
    except BeanCheckError:
        # Non-fatal — the append below either succeeds or raises
        # the same error with full context.
        pass
    # Refund-of-expense link (optional). When set, stamps
    # ``lamella-refund-of: "<original-lamella-txn-id>"`` on the override
    # block — bidirectional /txn-page lookup walks the ledger for any
    # txn carrying this meta whose value matches the original's lineage.
    refund_extra_meta: dict | None = None
    refund_pointer = (refund_of_txn_id or "").strip() or None
    if refund_pointer:
        refund_extra_meta = {"lamella-refund-of": refund_pointer}
    try:
        writer.append(
            txn_date=txn_date,
            txn_hash=txn_hash,
            amount=abs_amount,
            from_account=fixme_leg.account,
            to_account=target_account,
            currency=currency,
            narration=(target_txn.narration or "categorize"),
            extra_meta=refund_extra_meta,
        )
    except BeanCheckError as exc:
        log.warning("api_txn ledger classify bean-check: %s", exc)
        raise HTTPException(
            status_code=400, detail=f"bean-check blocked: {exc}",
        )
    reader.invalidate()

    # Stamp any pending ai_decisions targeting this txn as
    # user_corrected so they drop out of /ai/suggestions. Without
    # this the suggestion would sit in the queue forever even though
    # the user already classified the underlying txn. Use the same
    # input_ref ↔ txn_hash bridge the suggestions hydration uses
    # (SimpleFIN decisions store the bank id, not the hash, so a
    # direct WHERE input_ref = ? misses them).
    try:
        candidate_refs = [txn_hash]
        from lamella.core.identity import (
            find_source_reference, get_txn_id,
        )
        # Lineage UUID — what every post-Phase-3 decision keys on.
        _lineage = get_txn_id(target_txn)
        if _lineage and _lineage not in candidate_refs:
            candidate_refs.append(_lineage)
        # SimpleFIN id — what ingest-time decisions keyed on.
        # Reads from posting-level paired source meta; legacy
        # txn-level keys mirror down via _legacy_meta.normalize_entries.
        _sf = find_source_reference(target_txn, "simplefin")
        if _sf and _sf not in candidate_refs:
            candidate_refs.append(_sf)
        placeholders = ",".join("?" * len(candidate_refs))
        conn.execute(
            f"""
            UPDATE ai_decisions
               SET user_corrected = 1,
                   user_correction = COALESCE(user_correction,
                       'classified to ' || ?)
             WHERE decision_type = 'classify_txn'
               AND user_corrected = 0
               AND input_ref IN ({placeholders})
            """,
            (target_account, *candidate_refs),
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "api_txn ledger classify: ai_decisions cleanup failed: %s", exc,
        )

    # Optional rule save — same shape as /txn/{hash}/apply.
    if save_rule == "1" and (rule_pattern_value or "").strip():
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO classification_rules
                    (pattern_type, pattern_value, target_account,
                     card_account, confidence, hit_count, created_by)
                VALUES ('merchant_contains', ?, ?, NULL, 1.0, 0, 'api-txn')
                """,
                (rule_pattern_value.strip(), target_account),
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            log.warning("api_txn ledger classify rule save failed: %s", exc)

    from lamella.web.routes._htmx import is_htmx
    if is_htmx(request):
        # HX-Refresh tells the shim to do a window.location.reload()
        # — the modal's after-request handler is also wired to
        # reload, so either path winning leaves the user on a fresh
        # page reflecting the new ledger state.
        from fastapi.responses import Response
        return Response(status_code=204, headers={"HX-Refresh": "true"})
    from fastapi.responses import RedirectResponse
    sep = "&" if "?" in landing else "?"
    return RedirectResponse(
        f"{landing}{sep}message=classified", status_code=303,
    )


# ─── /api/txn/{ref}/dismiss + /restore ─────────────────────────────
#
# "Dismiss" is the legacy column name; the user-facing word is
# "Ignore." It's a soft state — fully reversible via /restore. The
# row stays in the database forever (never auto-purged) so future
# reconciliation can still see "I told it to skip this."
#
# Staged-only. Ledger refs are rejected because the ledger is the
# source of truth — to remove a misplaced ledger transaction, edit
# the .bean file directly.


# ─── /api/txn/{lamella_txn_id}/reverse-classify ────────────────────
#
# ADR-0046 Phase 4b — undo a classify for a ledger transaction.
#
# "Classify" today either:
#   * Writes an override block in connector_overrides.bean that zeros
#     out the FIXME posting and adds the chosen target account, OR
#   * (per ADR-0002) rewrites the underlying .bean file in place.
#
# This endpoint reverses option (1): every override block targeting
# the named transaction's hash gets removed, putting the txn back in
# its pre-classify FIXME state. It does NOT undo an in-place rewrite —
# those are baked into the source file and require a real ledger edit.
#
# The endpoint takes a ``lamella_txn_id`` (UUIDv7) rather than a
# txn_hash so the URL is stable across edits — the same id continues
# to resolve even if the txn was edited and re-hashed since classify.

@router.post(
    "/api/txn/{lamella_txn_id}/reverse-classify",
    response_class=HTMLResponse,
)
async def api_txn_reverse_classify(
    lamella_txn_id: str,
    request: Request,
    return_url: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Remove every override block targeting the named transaction.

    Resolves ``lamella_txn_id`` → canonical txn → ``txn_hash``, then
    delegates to ``OverrideWriter.rewrite_without_hash``. Idempotent
    against an already-reverted txn (returns 200 with removed=0).
    Records an auth_event of type ``txn_reverse_classify`` for audit
    trail per ADR-0050.
    """
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash as _hash
    from lamella.features.rules.overrides import OverrideWriter, BeanCheckError
    from lamella.web.auth.events import record_event

    if not lamella_txn_id or ":" in lamella_txn_id:
        raise HTTPException(
            status_code=400,
            detail="reverse-classify expects a bare lamella-txn-id "
                   "(no 'staged:' / 'ledger:' prefix)",
        )

    from lamella.core.identity import get_txn_id as _get_txn_id
    target = lamella_txn_id.strip().lower()
    txn: Transaction | None = None
    for entry in reader.load().entries:
        if not isinstance(entry, Transaction):
            continue
        entry_id = _get_txn_id(entry)
        if entry_id and entry_id.lower() == target:
            txn = entry
            break
        meta = getattr(entry, "meta", None) or {}
        for k, v in meta.items():
            if isinstance(k, str) and k.startswith("lamella-txn-id-alias-") and v:
                if str(v).lower() == target:
                    txn = entry
                    break
        if txn is not None:
            break
    if txn is None:
        raise HTTPException(
            status_code=404,
            detail=f"no ledger transaction with lamella-txn-id={lamella_txn_id!r}",
        )

    target_hash = _hash(txn)
    writer = OverrideWriter(
        main_bean=settings.ledger_main,
        overrides=settings.connector_overrides_path,
        conn=conn,
    )
    try:
        removed = writer.rewrite_without_hash(target_hash)
    except BeanCheckError as exc:
        raise HTTPException(
            status_code=500, detail=f"bean-check failed: {exc}",
        )

    # Clear the per-txn dirty-since-reviewed cache so the calendar
    # signal stays honest after the override goes away.
    try:
        conn.execute(
            "DELETE FROM txn_classification_modified WHERE txn_hash = ?",
            (target_hash,),
        )
        conn.commit()
    except sqlite3.Error:
        pass

    reader.invalidate()

    user = getattr(request.state, "user", None)
    record_event(
        conn,
        event_type="txn_reverse_classify",
        user_id=getattr(user, "id", None),
        account_id=getattr(user, "account_id", 1),
        success=True,
        request=request,
        detail=(
            f"lamella_txn_id={lamella_txn_id} txn_hash={target_hash[:12]} "
            f"removed_blocks={removed}"
        ),
    )

    from lamella.web.routes._htmx import is_htmx
    if is_htmx(request):
        from fastapi.responses import Response
        resp = Response(status_code=204)
        resp.headers["HX-Trigger"] = "lamella:txn-classified"
        landing = (return_url or "").strip()
        if landing:
            resp.headers["HX-Redirect"] = landing
        return resp
    landing = (return_url or "").strip() or f"/txn/{lamella_txn_id}"
    sep = "&" if "?" in landing else "?"
    from fastapi.responses import RedirectResponse
    return RedirectResponse(
        f"{landing}{sep}message=reverse_classify_{removed}",
        status_code=303,
    )


@router.post("/api/txn/{ref}/dismiss", response_class=HTMLResponse)
def api_txn_dismiss(
    ref: str,
    request: Request,
    reason: str = Form(default=""),
    return_url: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Mark a staged row as ignored. Reversible via /restore.

    The row stays in the DB indefinitely so the user can always see
    what they previously skipped (useful for reconciliation). Ledger
    refs are rejected — edit the .bean file directly to remove a
    real ledger transaction."""
    parsed = parse_ref(ref)
    if not parsed.is_staged:
        raise HTTPException(
            status_code=400,
            detail="ignore only applies to staged rows; "
                   "edit the .bean file to remove a ledger transaction",
        )

    from lamella.features.import_.staging import StagingService, StagingError
    svc = StagingService(conn)
    # Existence check first — StagingService.dismiss writes a
    # staged_decisions row keyed on staged_id, and a missing row
    # raises an opaque IntegrityError (FK) instead of a clean 404.
    try:
        svc.get(parsed.staged_id)
    except StagingError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    try:
        svc.dismiss(
            parsed.staged_id,
            reason=(reason or "").strip() or "ignored via /api/txn",
        )
    except StagingError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    conn.commit()

    landing = (return_url or "").strip() or "/review"
    from lamella.web.routes._htmx import is_htmx
    if is_htmx(request):
        from fastapi.responses import Response
        return Response(status_code=204, headers={"HX-Refresh": "true"})
    from fastapi.responses import RedirectResponse
    sep = "&" if "?" in landing else "?"
    return RedirectResponse(
        f"{landing}{sep}message=ignored_{parsed.staged_id}",
        status_code=303,
    )


@router.post("/api/txn/{ref}/restore", response_class=HTMLResponse)
def api_txn_restore(
    ref: str,
    request: Request,
    return_url: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Bring an ignored staged row back into the pending queue.

    Reverses :func:`api_txn_dismiss`. Idempotent — calling on a row
    that isn't currently dismissed leaves it alone."""
    parsed = parse_ref(ref)
    if not parsed.is_staged:
        raise HTTPException(
            status_code=400,
            detail="restore only applies to staged rows",
        )
    from lamella.features.import_.staging import StagingService, StagingError
    svc = StagingService(conn)
    try:
        svc.get(parsed.staged_id)
    except StagingError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    try:
        svc.restore(parsed.staged_id)
    except StagingError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    conn.commit()

    landing = (return_url or "").strip() or "/review"
    from lamella.web.routes._htmx import is_htmx
    if is_htmx(request):
        from fastapi.responses import Response
        return Response(status_code=204, headers={"HX-Refresh": "true"})
    from fastapi.responses import RedirectResponse
    sep = "&" if "?" in landing else "?"
    return RedirectResponse(
        f"{landing}{sep}message=restored_{parsed.staged_id}",
        status_code=303,
    )


# ─── /api/txn/{ref}/ask-ai ─────────────────────────────────────────
#
# Submits a job that re-runs the classifier on the referenced txn.
# Optional ``rejection_reason`` is prepended to the prompt context so
# the AI gets the user's "no, that's wrong because…" hint and is
# meaningfully more likely to land on a different answer.
#
# Returns _job_modal.html with the new job_id. The job's terminal
# state renders _ask_ai_result.html, parameterized by ``mode``
# (staged vs ledger) so its Accept and Manual buttons post into the
# right per-source writer.
#
# Note: ``ref`` here is path-encoded — FastAPI handles ":" in path
# segments without special escaping for our local-deploy use case.


@router.post("/api/txn/{ref}/ask-ai", response_class=HTMLResponse)
def api_txn_ask_ai(
    ref: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    rejection_reason: str | None = Form(default=None),
    context_hint: str | None = Form(default=None),
    attempt: int = Form(default=1),
    return_url: str | None = Form(default=None),
):
    """Unified ask-AI endpoint for any txn.

    ``return_url`` is where the modal's Close + post-accept reload
    should land the user (defaults to the page they came from via
    Referer). Lets the modal stay surface-agnostic: the calling page
    provides its own back-pointer rather than the endpoint guessing."""
    parsed = parse_ref(ref)
    runner = getattr(request.app.state, "job_runner", None)
    if runner is None:
        raise HTTPException(status_code=503, detail="job runner not ready")

    templates = request.app.state.templates
    _ASK_AI_MAX_ATTEMPTS = 2
    blocked = attempt > _ASK_AI_MAX_ATTEMPTS

    # Pre-flight: bail out fast with a clear error if AI isn't
    # configured at all. The job-modal would catch this too via
    # ai.enabled in its worker, but only after a 1-second poll —
    # the user clicks AI, sees a modal, then waits for the "AI is
    # disabled" message. The HTTPException path renders an
    # immediate toast at the top of the page (per the new HTMX
    # error handling shipped in fac8651 + 510d15b) so the user
    # gets the answer before the modal even opens.
    _ai_check = AIService(settings=settings, conn=conn)
    if not _ai_check.enabled:
        raise HTTPException(
            status_code=503,
            detail=(
                "AI is not enabled. Configure OPENROUTER_API_KEY in "
                "Settings → AI to use Ask AI."
            ),
        )

    return_to = (return_url or "").strip()
    if not return_to:
        ref_h = (request.headers.get("referer") or "").strip()
        return_to = ref_h or "/review"

    # Per-kind setup. Each branch resolves a label for the modal title
    # and a callable that the worker invokes to actually run the AI.
    if parsed.is_staged:
        from lamella.features.import_.staging import StagingService, StagingError
        svc = StagingService(conn)
        try:
            row = svc.get(parsed.staged_id)
        except StagingError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if row.status == "promoted":
            raise HTTPException(
                status_code=409,
                detail="row already promoted — close the modal and refresh",
            )
        title_label = (row.payee or row.description or f"row #{parsed.staged_id}")[:50]
    else:
        # Resolve the txn upfront so we can fail fast with a clear
        # error when the ref doesn't point to anything in the ledger.
        from beancount.core.data import Transaction
        from lamella.core.beancount_io.txn_hash import txn_hash as _th
        entries = list(reader.load().entries)
        target_txn = None
        for e in entries:
            if isinstance(e, Transaction) and _th(e) == parsed.txn_hash:
                target_txn = e
                break
        if target_txn is None:
            raise HTTPException(
                status_code=404,
                detail=f"ledger txn {parsed.txn_hash[:16]}… not found",
            )
        title_label = (
            getattr(target_txn, "payee", None)
            or (target_txn.narration or f"hash {parsed.txn_hash[:12]}")
        )[:50]

    title = f"Asking AI · {title_label}"
    if attempt > 1:
        title = f"Retry #{attempt - 1} · {title_label}"

    def _render_terminal(
        proposal: dict | None, *, blocked_flag: bool,
        ai_skip_reason: str | None = None,
        refund_candidates: list | None = None,
    ) -> str:
        ctx = {
            "mode": parsed.kind,
            "ref": str(parsed),
            "staged_id": parsed.staged_id if parsed.is_staged else None,
            "txn_hash": parsed.txn_hash if parsed.is_ledger else None,
            "proposal": proposal,
            "attempt": attempt,
            "reason": rejection_reason,
            "blocked": blocked_flag,
            "return_url": return_to,
            "ai_skip_reason": ai_skip_reason,
            # Refund candidates rendered above the manual picker on the
            # deposit-skip panel. None = section absent (the default for
            # any non-deposit terminal); empty list = "we looked but
            # nothing matched". Both render the manual picker; the
            # presence of one or more candidates adds the one-click
            # refund-of buttons.
            "refund_candidates": refund_candidates or [],
            # Legacy field kept so the existing template branches
            # don't break — Phase 1 tolerates both.
            "source": "",
        }
        return templates.get_template(
            "partials/_ask_ai_result.html"
        ).render(ctx)

    def _worker(ctx_job):
        ctx_job.set_total(1)
        if blocked:
            ctx_job.emit(
                "Blocked after 2 failed attempts — surfacing manual fallback.",
                outcome="info",
            )
            ctx_job.advance(1)
            return {"terminal_html": _render_terminal(None, blocked_flag=True)}

        ctx_job.emit("Building classify context …", outcome="info")

        # Architectural shortcut (user directive): deposits don't need
        # AI inference. Money IN — a deposit on an asset, a refund or
        # payment on a credit card — has a deterministic destination
        # category (Income:{Entity}:*, a transfer pair, or a refund
        # link). The AI cannot guess the user's preferred Income
        # subcategory any better than the user themselves can, and
        # the AI bias risk (training-set "user usually deposits to
        # account A" leaking into other-account decisions) is real.
        # Short-circuit; let the user classify manually.
        _is_deposit = False
        try:
            if parsed.is_staged:
                _row = StagingService(conn).get(parsed.staged_id)
                _amt = Decimal(_row.amount) if _row else Decimal("0")
                # Universal sign convention: the staged amount mirrors the
                # bank-side leg as Lamella writes it to the ledger
                # (writer.py:166-172, render_entry). A real ledger inspection
                # confirms the same shape across asset and liability sources:
                #   Liabilities:CC -9.49  → CC charge (money OUT for user)
                #   Liabilities:CC +20.95 → CC refund (money IN for user)
                #   Assets:Checking -50   → withdrawal/expense
                #   Assets:Checking +100  → deposit/income
                # So positive = money IN (deposit-shaped, skip AI), negative
                # = money OUT (expense-shaped, let AI classify) regardless of
                # account kind. Earlier code inverted this for liabilities,
                # which silently misrouted CC charges to the deposit panel
                # and CC refunds to the AI's expense whitelist.
                _is_deposit = _amt > 0
            else:
                # Ledger txn: read the FIXME-leg amount from the parsed
                # entry. The FIXME leg has the opposite sign of the bank
                # side (txn balances to zero), so NEGATIVE FIXME amount
                # = POSITIVE bank-side = money IN = deposit-shaped.
                from lamella.core.beancount_io.txn_hash import (
                    txn_hash as _th,
                )
                from beancount.core.data import Transaction
                for e in reader.load().entries:
                    if not isinstance(e, Transaction):
                        continue
                    if _th(e) != parsed.txn_hash:
                        continue
                    _fixme_amt = None
                    for p in e.postings or ():
                        acct = p.account or ""
                        if "FIXME" in acct.upper() and p.units and p.units.number is not None:
                            _fixme_amt = Decimal(p.units.number)
                            break
                    if _fixme_amt is not None:
                        _is_deposit = _fixme_amt < 0
                    break
        except Exception:  # noqa: BLE001
            _is_deposit = False
        if _is_deposit:
            ctx_job.emit(
                "Deposit detected — skipping AI. Pick an Income:{Entity}:* "
                "subcategory yourself, or recognize as a transfer.",
                outcome="info",
            )
            # Refund-of-expense matching (user directive): a positive
            # amount on a card/checking account that reverses a prior
            # expense should re-route against the original expense's
            # category. Look up candidates so the panel can render them
            # as one-click buttons.
            _refund_candidates: list = []
            try:
                from lamella.features.bank_sync.refund_detect import (
                    find_refund_candidates,
                )
                from datetime import date as _date_t
                # Pull the inputs from whichever side of the parse the
                # row lives on. Same fields, two source shapes.
                if parsed.is_staged:
                    _row = StagingService(conn).get(parsed.staged_id)
                    if _row is not None:
                        _src_acct = None
                        try:
                            from lamella.web.routes.staging_review import (
                                _resolve_account_path as _rap,
                            )
                            _src_acct = _rap(
                                conn, _row.source, _row.source_ref,
                                raw=getattr(_row, "raw", None),
                            )
                        except Exception:  # noqa: BLE001
                            _src_acct = None
                        _refund_candidates = find_refund_candidates(
                            conn, reader,
                            refund_amount=Decimal(_row.amount),
                            refund_date=_date_t.fromisoformat(
                                _row.posting_date[:10]
                            ),
                            merchant=_row.payee,
                            narration=_row.description or _row.memo,
                            source_account=_src_acct,
                        )
                else:
                    # Ledger-side deposit (positive on the bank leg of
                    # a FIXME txn). Read date + bank-side account +
                    # amount off the parsed entry.
                    from lamella.core.beancount_io.txn_hash import (
                        txn_hash as _th,
                    )
                    from beancount.core.data import Transaction as _T
                    _bank_acct = None
                    _bank_amt: Decimal | None = None
                    _txn_date: _date_t | None = None
                    _payee: str | None = None
                    _narration: str | None = None
                    for e in reader.load().entries:
                        if not isinstance(e, _T):
                            continue
                        if _th(e) != parsed.txn_hash:
                            continue
                        _txn_date = e.date if isinstance(e.date, _date_t) else _date_t.fromisoformat(str(e.date))
                        _payee = getattr(e, "payee", None)
                        _narration = e.narration
                        for p in e.postings or ():
                            acct = p.account or ""
                            if "FIXME" in acct.upper():
                                continue
                            if not (acct.startswith("Assets:") or acct.startswith("Liabilities:")):
                                continue
                            if p.units is None or p.units.number is None:
                                continue
                            _amt = Decimal(p.units.number)
                            if _amt > 0:
                                _bank_acct = acct
                                _bank_amt = _amt
                                break
                        break
                    if _bank_acct and _bank_amt is not None and _txn_date is not None:
                        _refund_candidates = find_refund_candidates(
                            conn, reader,
                            refund_amount=_bank_amt,
                            refund_date=_txn_date,
                            merchant=_payee,
                            narration=_narration,
                            source_account=_bank_acct,
                        )
            except Exception as exc:  # noqa: BLE001
                # Detection is best-effort: a failure in candidate
                # lookup must not break the deposit-skip terminal.
                # Surface a soft job-event so a future debugging pass
                # has something to grep, but render the panel without
                # candidates so the manual picker still works.
                log.warning(
                    "refund-candidate lookup failed for %s: %s",
                    parsed, exc,
                )
            if _refund_candidates:
                # ADR-0041: resolve display aliases for the target
                # accounts so the modal renders human-readable labels
                # instead of raw colon-separated paths.
                from lamella.core.registry.alias import account_label
                from dataclasses import replace as _replace
                _refund_candidates = [
                    _replace(
                        c,
                        target_account_display=(
                            account_label(conn, c.target_account) or c.target_account
                        ),
                    )
                    for c in _refund_candidates
                ]
                ctx_job.emit(
                    f"Found {len(_refund_candidates)} possible refund "
                    f"match{'es' if len(_refund_candidates) != 1 else ''}.",
                    outcome="success",
                )
            ctx_job.advance(1)
            return {
                "terminal_html": _render_terminal(
                    None, blocked_flag=False,
                    ai_skip_reason="deposit",
                    refund_candidates=_refund_candidates,
                )
            }

        ai = AIService(settings=settings, conn=conn)
        if not ai.enabled:
            ctx_job.emit(
                "AI is disabled — set OPENROUTER_API_KEY to enable.",
                outcome="error",
            )
            ctx_job.advance(1)
            return {"terminal_html": _render_terminal(None, blocked_flag=False)}

        try:
            if parsed.is_staged:
                proposal = _run_staged_ask_ai(
                    conn=conn, reader=reader, settings=settings, ai=ai,
                    staged_id=parsed.staged_id,
                    rejection_reason=rejection_reason,
                    context_hint=context_hint,
                )
            else:
                proposal = _run_ledger_ask_ai(
                    conn=conn, reader=reader, settings=settings, ai=ai,
                    txn_hash=parsed.txn_hash,
                    rejection_reason=rejection_reason,
                    context_hint=context_hint,
                )
        except Exception as exc:  # noqa: BLE001
            log.exception("api_txn ask-ai worker failed for %s", parsed)
            ctx_job.emit(
                f"Classifier error: {type(exc).__name__}: {exc}",
                outcome="error",
            )
            ctx_job.advance(1)
            return {"terminal_html": _render_terminal(None, blocked_flag=False)}

        if proposal is None:
            ctx_job.emit("AI returned no confident proposal.", outcome="failure")
            ctx_job.advance(1)
            return {"terminal_html": _render_terminal(None, blocked_flag=False)}

        ctx_job.emit(
            f"AI proposed {proposal['target']} ({proposal['confidence']}).",
            outcome="success",
        )
        ctx_job.advance(1)
        return {"terminal_html": _render_terminal(proposal, blocked_flag=False)}

    job_id = runner.submit(
        kind="ask-ai-classify",
        title=title,
        fn=_worker,
        total=1,
        meta={
            "ref": str(parsed),
            "attempt": attempt,
            "return_url": return_to,
        },
    )
    return templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": return_to},
    )


# ─── /api/txn-bulk/ask-ai ─────────────────────────────────────────
#
# Queue one job that asks AI on N referenced transactions in
# sequence. Each ref is dispatched the same way the per-row
# /api/txn/{ref}/ask-ai endpoint does. The job emits one event per
# row (success/failure/error) so the modal's progress bar moves
# row-by-row.
#
# Capped at ASK_AI_BULK_CAP refs to keep budget + time bounded.
# Above that the user should drain in batches.
#
# URL note: this endpoint is at /api/txn-bulk/ (hyphen) rather than
# /api/txn/bulk/ to avoid collision with the per-ref route
# /api/txn/{ref}/ask-ai. Without the rename, FastAPI matches "bulk"
# as `ref` (since {ref} matches any non-slash string) and parse_ref
# returns 400 — the bug surfaced as "Bad request · 400 invalid txn
# ref 'bulk'" when the user clicked Ask All.


_ASK_AI_BULK_CAP = 25


@router.post("/api/txn-bulk/ask-ai", response_class=HTMLResponse)
async def api_txn_bulk_ask_ai(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Bulk Ask-AI across N transactions. Form body carries
    ``refs[]`` (one entry per ``staged:<id>`` or ``ledger:<hash>``)
    plus an optional ``return_url``. Submits one job that iterates
    serially — keeps cost predictable and avoids hammering OpenRouter
    with concurrent requests."""
    # Same pre-flight as the single-row endpoint: fail fast with a
    # clear toast when AI isn't configured, instead of opening the
    # modal and waiting for a worker poll to reveal it.
    _ai_check = AIService(settings=settings, conn=conn)
    if not _ai_check.enabled:
        raise HTTPException(
            status_code=503,
            detail=(
                "AI is not enabled. Configure OPENROUTER_API_KEY in "
                "Settings → AI to use Ask AI."
            ),
        )
    form = await request.form()
    refs_raw = form.getlist("refs") if hasattr(form, "getlist") else (
        form.getall("refs") if hasattr(form, "getall") else []
    )
    if not refs_raw:
        # FastAPI's UploadFile-aware form returns a starlette FormData;
        # iterate in Python form to find every "refs" entry.
        refs_raw = [v for k, v in form.multi_items() if k == "refs"]
    parsed_refs: list[TxnRef] = []
    for r in refs_raw[: _ASK_AI_BULK_CAP]:
        if isinstance(r, str) and r.strip():
            try:
                parsed_refs.append(parse_ref(r.strip()))
            except HTTPException:
                continue
    if not parsed_refs:
        raise HTTPException(
            status_code=400,
            detail=f"no valid refs[] in form (max {_ASK_AI_BULK_CAP})",
        )
    return_url = (form.get("return_url") or "").strip() or "/inbox"

    runner = getattr(request.app.state, "job_runner", None)
    if runner is None:
        raise HTTPException(status_code=503, detail="job runner not ready")
    templates = request.app.state.templates

    title = f"Bulk Ask AI · {len(parsed_refs)} transaction{'s' if len(parsed_refs) != 1 else ''}"

    def _worker(ctx_job):
        ctx_job.set_total(len(parsed_refs))
        ai = AIService(settings=settings, conn=conn)
        if not ai.enabled:
            ctx_job.emit(
                "AI is disabled — set OPENROUTER_API_KEY to enable.",
                outcome="error",
            )
            return {
                "terminal_html":
                    '<div class="ask-ai-result__blocked">'
                    '<h3>AI disabled</h3><p>Set OPENROUTER_API_KEY to enable bulk Ask AI.</p>'
                    '</div>'
            }

        success = 0
        declined = 0
        errors = 0
        for parsed in parsed_refs:
            ctx_job.raise_if_cancelled()
            label = str(parsed)
            try:
                if parsed.is_staged:
                    proposal = _run_staged_ask_ai(
                        conn=conn, reader=reader, settings=settings, ai=ai,
                        staged_id=parsed.staged_id,
                        rejection_reason=None, context_hint=None,
                    )
                else:
                    proposal = _run_ledger_ask_ai(
                        conn=conn, reader=reader, settings=settings, ai=ai,
                        txn_hash=parsed.txn_hash,
                        rejection_reason=None, context_hint=None,
                    )
            except Exception as exc:  # noqa: BLE001
                log.warning("bulk ask-ai failed for %s: %s", label, exc)
                ctx_job.emit(
                    f"{label}: error · {type(exc).__name__}",
                    outcome="error",
                )
                errors += 1
                ctx_job.advance(1)
                continue
            if proposal is None:
                ctx_job.emit(
                    f"{label}: AI declined", outcome="failure",
                )
                declined += 1
            else:
                ctx_job.emit(
                    f"{label}: proposed {proposal['target']} ({proposal['confidence']})",
                    outcome="success",
                )
                success += 1
            ctx_job.advance(1)

        # Render a small terminal summary panel. The user reloads
        # the underlying page to see proposals on each row's
        # Proposed band — staged rows get their staged_decisions
        # row populated; ledger rows get a new ai_decisions entry
        # surfaced in /ai/suggestions.
        # Two CTAs: back to where they came from (typically /review)
        # AND a direct link to /ai/suggestions where the new
        # proposals queue up for one-click Accept. The second link
        # is where the user actually wants to land — finding the
        # just-classified rows on /review by scrolling is painful
        # and was a direct user complaint.
        from html import escape
        terminal = (
            f'<div class="ask-ai-bulk-result">'
            f'  <h3>Bulk Ask AI complete</h3>'
            f'  <ul style="list-style:none; padding:0; margin:0.5rem 0 0">'
            f'    <li>✓ {success} proposal{"" if success == 1 else "s"}</li>'
            f'    <li>· {declined} declined</li>'
            f'    <li>· {errors} error{"" if errors == 1 else "s"}</li>'
            f'  </ul>'
            f'  <div style="display:flex; gap:0.5rem; flex-wrap:wrap; '
            f'        margin-top:0.75rem">'
            f'    <button type="button" class="btn btn-primary" '
            f'      onclick="window.location.href=\'/ai/suggestions\'">'
            f'      Review {success} AI suggestion{"" if success == 1 else "s"} →'
            f'    </button>'
            f'    <button type="button" class="btn" '
            f'      onclick="window.location.href=\'{escape(return_url)}\'">'
            f'      Back to {escape(return_url)}'
            f'    </button>'
            f'  </div>'
            f'</div>'
        )
        return {"terminal_html": terminal}

    job_id = runner.submit(
        kind="ask-ai-bulk",
        title=title,
        fn=_worker,
        total=len(parsed_refs),
        meta={"refs": [str(r) for r in parsed_refs]},
    )
    return templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": return_url},
    )


# ─── Per-kind classifier runners ───────────────────────────────────


def _suppress_transfer_expense_guess(
    *, target_account: str, confidence_bucket: str, narration_text: str,
) -> bool:
    """Return True when the AI returned a low-confidence Expenses
    target on a row whose narration looks like a transfer.

    Mirrors the enricher's transfer-suspect guard. The /review-side
    "Looks like a transfer" hint uses the same shared heuristic
    (transfer/xfer + liability-payment language); surfacing a
    low-conf Expenses guess as a clickable Accept band contradicts
    the hint and tempts a one-click misclassification."""
    if confidence_bucket != "low":
        return False
    if not target_account.startswith("Expenses:"):
        return False
    from lamella.core.transfer_heuristic import looks_like_transfer_text
    return looks_like_transfer_text(narration_text)


def _run_staged_ask_ai(
    *, conn, reader, settings, ai,
    staged_id: int,
    rejection_reason: str | None,
    context_hint: str | None,
) -> dict | None:
    """Reproduce the staged-row classify path used by the existing
    /review/staged/ask-ai-modal worker — same SimpleFINTransaction
    facade, same memo composition. Returns a normalized proposal dict
    or None if the AI declined."""
    from datetime import datetime, timezone
    from lamella.features.ai_cascade.gating import ConfidenceGate
    from lamella.web.routes.staging_review import _resolve_account_path
    from lamella.features.review_queue.service import ReviewService
    from lamella.features.rules.service import RuleService
    from lamella.features.bank_sync.ingest import SimpleFINIngest
    from lamella.adapters.simplefin.schemas import SimpleFINTransaction
    from lamella.features.bank_sync.writer import SimpleFINWriter
    from lamella.features.import_.staging import StagingService

    svc = StagingService(conn)
    row = svc.get(staged_id)
    source_account = _resolve_account_path(
        conn, row.source, row.source_ref,
        raw=getattr(row, "raw", None),
    )
    if not source_account:
        # Soft fallback: we let _maybe_ai_classify run with
        # source_account=None below. Without a card hint, the
        # cross-entity widening already in _maybe_ai_classify covers
        # the row across every entity's whitelist; the existing
        # intercompany_flag mitigation forces the gate to send any
        # high-confidence cross-entity pick to review. Logging the
        # warning so the operator can spot mis-mapped rows in
        # production without the silent return-None that previously
        # surfaced as "No confident proposal" in the modal.
        log.warning(
            "ask_ai: _resolve_account_path returned None for staged "
            "row id=%s source=%s; running classifier without a card "
            "hint.", staged_id, row.source,
        )

    posted_epoch = int(
        datetime.fromisoformat(row.posting_date[:10])
        .replace(tzinfo=timezone.utc).timestamp()
    )
    txn_id = (
        row.source_ref.get("txn_id")
        if isinstance(row.source_ref, dict) else None
    ) or row.source_ref_hash

    memo_parts: list[str] = []
    if rejection_reason and rejection_reason.strip():
        memo_parts.append(
            f"User rejected the previous AI guess. Their reason: "
            f"{rejection_reason.strip()}"
        )
    hint = (context_hint or "").strip()
    if hint:
        memo_parts.append(f"User hint: {hint}")
    if row.memo:
        memo_parts.append(row.memo)
    composed_memo = "\n".join(memo_parts) if memo_parts else None

    sf_txn = SimpleFINTransaction(
        id=str(txn_id),
        posted=posted_epoch,
        amount=Decimal(row.amount),
        description=row.description or "",
        payee=row.payee,
        memo=composed_memo,
    )

    writer = SimpleFINWriter(
        main_bean=settings.ledger_main,
        simplefin_path=settings.simplefin_transactions_path,
    )
    ingest = SimpleFINIngest(
        conn=conn, settings=settings, reader=reader,
        rules=RuleService(conn), reviews=ReviewService(conn),
        writer=writer, ai=ai, gate=ConfidenceGate(),
    )

    import asyncio
    loop = asyncio.new_event_loop()
    try:
        inline = loop.run_until_complete(
            ingest._maybe_ai_classify(  # noqa: SLF001
                txn=sf_txn,
                source_account=source_account,
                # Thread the staged row's lamella-txn-id so the
                # receipt-context lookup hits the linked branch
                # (ADR-0056). Without this the classifier runs with
                # the candidate-by-amount fallback only and misses
                # OCR / line-item data on receipts the user
                # explicitly attached to this staged row.
                lamella_txn_id=row.lamella_txn_id,
            )
        )
    finally:
        loop.close()

    if inline is None or inline.proposal is None:
        return None
    p = inline.proposal
    score = float(p.confidence or 0.0)
    bucket = (
        "high" if score >= 0.90
        else "medium" if score >= 0.50
        else "low"
    )
    # Transfer-suspect guard: AI usually declines verbally on these
    # ("should go to review rather than be classified by guess") but
    # the proposal still carries an Expenses target. Suppress so the
    # user doesn't see a contradictory low-conf Accept CTA next to
    # the "Looks like a transfer" hint.
    narration_text = " ".join(filter(None, [
        getattr(row, "payee", None),
        getattr(row, "description", None),
        getattr(row, "memo", None),
    ]))
    if _suppress_transfer_expense_guess(
        target_account=p.target_account,
        confidence_bucket=bucket,
        narration_text=narration_text,
    ):
        log.info(
            "api_txn ask-ai (staged): suppressing low-conf Expenses "
            "proposal on transfer-suspect row (id=%d target=%s)",
            staged_id, p.target_account,
        )
        return None
    # Mirror the existing handler: record the proposal so the staged
    # row's Proposed band reflects the latest AI attempt even if the
    # user closes the modal without accepting.
    try:
        svc.record_decision(
            staged_id=staged_id,
            account=p.target_account,
            confidence=bucket,
            confidence_score=score,
            decided_by="ai",
            ai_decision_id=p.decision_id,
            rationale=p.reasoning,
            needs_review=True,
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "api_txn ask-ai (staged): record_decision failed for %d: %s",
            staged_id, exc,
        )
    return {
        "target": p.target_account,
        "confidence": bucket,
        "score": score,
        "rationale": p.reasoning,
    }


def _run_ledger_ask_ai(
    *, conn, reader, settings, ai,
    txn_hash: str,
    rejection_reason: str | None,
    context_hint: str | None,
) -> dict | None:
    """Re-run the classifier on a FIXME-in-ledger transaction.
    Builds on bulk_classify._classify_one and prepends the user's
    rejection reason / hint to the txn narration so it lands in the
    prompt's narration block. Returns a normalized proposal dict
    or None when the AI declined."""
    from beancount.core.data import Transaction
    from lamella.features.ai_cascade.bulk_classify import _classify_one
    from lamella.core.beancount_io.txn_hash import txn_hash as _th
    from lamella.features.rules.scanner import _is_fixme

    entries = list(reader.load().entries)
    target_txn: Transaction | None = None
    for e in entries:
        if isinstance(e, Transaction) and _th(e) == txn_hash:
            target_txn = e
            break
    if target_txn is None:
        return None

    fixme_account: str | None = None
    abs_amount = Decimal("0")
    currency = "USD"
    for p in target_txn.postings:
        acct = p.account or ""
        if _is_fixme(acct) and p.units and p.units.number is not None:
            fixme_account = acct
            abs_amount = abs(Decimal(p.units.number))
            currency = p.units.currency or "USD"
            break
    if fixme_account is None:
        return None

    # Compose a "user feedback" addendum to the narration so the
    # classifier sees the rejection reason and / or hint. The prompt
    # template already weighs narration alongside payee, similar
    # history, active notes, and receipt context. _classify_one also
    # picks up `prior_attempts_for_txn` from ai_decisions, so prior
    # rejection reasons in user_correction also reach the model.
    addendum_parts: list[str] = []
    if rejection_reason and rejection_reason.strip():
        addendum_parts.append(
            f"User rejected the previous AI guess. Their reason: "
            f"{rejection_reason.strip()}"
        )
    hint = (context_hint or "").strip()
    if hint:
        addendum_parts.append(f"User hint: {hint}")
    if addendum_parts:
        addendum = "\n".join(addendum_parts)
        original_narration = target_txn.narration or ""
        # Build a shallow copy of the txn with augmented narration.
        # NamedTuple._replace is the safe path here.
        augmented = (
            f"{original_narration}\n[user-feedback]\n{addendum}"
            if original_narration else f"[user-feedback]\n{addendum}"
        )
        target_txn = target_txn._replace(narration=augmented)

    import asyncio
    loop = asyncio.new_event_loop()
    try:
        target, confidence_score, error = loop.run_until_complete(
            _classify_one(
                txn=target_txn,
                fixme_account=fixme_account,
                abs_amount=abs_amount,
                currency=currency,
                entries=entries,
                conn=conn,
                settings=settings,
                ai_service=ai,
            )
        )
    finally:
        loop.close()

    if not target:
        if error:
            log.info(
                "api_txn ledger ask-ai declined: %s (txn=%s)",
                error, txn_hash[:12],
            )
        return None

    score = float(confidence_score or 0.0)
    bucket = (
        "high" if score >= 0.90
        else "medium" if score >= 0.50
        else "low"
    )
    # Transfer-suspect guard (mirrors the staged path).
    ledger_narration = " ".join(filter(None, [
        getattr(target_txn, "payee", None),
        getattr(target_txn, "narration", None),
    ]))
    if _suppress_transfer_expense_guess(
        target_account=target,
        confidence_bucket=bucket,
        narration_text=ledger_narration,
    ):
        log.info(
            "api_txn ask-ai (ledger): suppressing low-conf Expenses "
            "proposal on transfer-suspect txn (hash=%s target=%s)",
            txn_hash[:12], target,
        )
        return None
    return {
        "target": target,
        "confidence": bucket,
        "score": score,
        "rationale": None,
    }
