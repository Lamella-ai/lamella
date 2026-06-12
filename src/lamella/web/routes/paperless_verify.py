# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""User-triggered Paperless verify + enrichment (Slice B).

Routes:

  POST /documents/{doc_id}/verify
      Submit a background job that runs the verify-and-writeback
      flow (Tier 1: OCR text via Haiku; Tier 2: vision). Returns
      the standard job-progress modal so the user sees the
      step-by-step decision cascade, counters, ETA, and — on
      failure — the actual provider error instead of a silently
      dropped HTMX indicator.

  POST /documents/{doc_id}/verify/sync
      Synchronous back-compat endpoint used by tests and
      non-interactive callers that need the final diff partial
      in one response.

  POST /documents/{doc_id}/enrich
      Push a derived-context enrichment (vehicle/entity/project
      note) to a Paperless document. Synchronous — no AI call, so
      the round trip is fast; returns the result partial inline.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import date as date_cls
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.jobs.context import JobCancelled
from lamella.web.deps import (
    get_ai_service,
    get_db,
    get_ledger_reader,
    get_paperless,
    get_settings,
)
from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.verify import (
    EnrichmentContext,
    VerifyHypothesis,
    VerifyService,
)

log = logging.getLogger(__name__)

router = APIRouter()


def _get_verify_service(
    ai: AIService, paperless: PaperlessClient, conn: sqlite3.Connection,
) -> VerifyService:
    return VerifyService(ai=ai, paperless=paperless, conn=conn)


async def _await_with_cancel(
    coro,
    cancel_event,
    *,
    poll_seconds: float = 0.25,
):
    """Run ``coro`` as an asyncio Task and race it against a watcher
    that polls ``cancel_event`` every ``poll_seconds``. If the watcher
    wins (the user clicked Cancel mid-AI-call), the verify task is
    cancelled (httpx raises ``CancelledError``) and we raise
    :class:`JobCancelled` so the job runner stamps the job
    'cancelled'.

    Best-effort: an httpx call already streaming a response may not
    honor cancel instantly — the user usually sees the click 'stick'
    within ~1 second.
    """
    main_task = asyncio.create_task(coro)

    async def _watch() -> None:
        while not cancel_event.is_set():
            await asyncio.sleep(poll_seconds)

    watcher = asyncio.create_task(_watch())
    done, _pending = await asyncio.wait(
        {main_task, watcher}, return_when=asyncio.FIRST_COMPLETED,
    )
    if watcher in done and not main_task.done():
        main_task.cancel()
        try:
            await main_task
        except (asyncio.CancelledError, JobCancelled):
            pass
        except Exception:  # noqa: BLE001
            log.exception("task errored while honoring cancel")
        raise JobCancelled()
    # Main finished first — drain the watcher.
    watcher.cancel()
    try:
        await watcher
    except asyncio.CancelledError:
        pass
    return main_task.result()


def _parse_date(raw: str | None) -> date_cls | None:
    if not raw:
        return None
    try:
        return date_cls.fromisoformat(raw[:10])
    except ValueError:
        return None


def _parse_decimal(raw: str | None) -> Decimal | None:
    if not raw:
        return None
    try:
        return Decimal(raw.strip())
    except (InvalidOperation, ValueError):
        return None


def _build_hypothesis(
    suspected_date: str | None,
    suspected_total: str | None,
    reason: str,
    suspected_vendor: str | None = None,
) -> VerifyHypothesis | None:
    sus_date = _parse_date(suspected_date)
    sus_total = _parse_decimal(suspected_total)
    sus_vendor = (suspected_vendor or "").strip() or None
    if sus_date or sus_total or sus_vendor or (reason and reason.strip()):
        return VerifyHypothesis(
            suspected_date=sus_date,
            suspected_total=sus_total,
            suspected_vendor=sus_vendor,
            reason=(reason or "").strip(),
        )
    return None


def _hypothesis_from_link(
    conn: sqlite3.Connection,
    reader: LedgerReader,
    doc_id: int,
) -> VerifyHypothesis | None:
    """Auto-derive a verify hypothesis from the receipt's link to a
    transaction. The linked txn's payee/date/amount come from the bank
    statement (or the staged-row equivalent) — that's the immutable
    ground truth Paperless's own current fields can't supply.

    Without this, clicking Verify with no form input leaves the AI
    seeing only Paperless's own (potentially wrong) fields. Common
    failure mode: a national-chain receipt where Paperless's
    correspondent extraction picks up the store's city/locality line
    instead of the brand. The bank-statement payee carries the brand
    name; the link supplies it as ground truth so the AI compares
    apples to apples.

    Lookup order for the txn payee: staged_transactions keyed by
    lamella_txn_id (staged-side links store the lamella id in
    txn_hash), then the ledger via the in-memory reader. Returns
    ``None`` if no link or no usable fields.
    """
    row = conn.execute(
        "SELECT txn_hash, txn_date, txn_amount FROM document_links "
        "WHERE paperless_id = ? ORDER BY linked_at DESC LIMIT 1",
        (doc_id,),
    ).fetchone()
    if not row:
        return None
    sus_date = _parse_date(row["txn_date"])
    sus_total = _parse_decimal(row["txn_amount"])
    sus_vendor: str | None = None
    txn_hash = row["txn_hash"]

    # Staged-side: hunt.py keys link.txn_hash to the staged row's
    # lamella_txn_id when the txn isn't in the ledger yet.
    if txn_hash:
        staged = conn.execute(
            "SELECT payee, description FROM staged_transactions "
            "WHERE lamella_txn_id = ? LIMIT 1",
            (txn_hash,),
        ).fetchone()
        if staged:
            cand = (staged["payee"] or staged["description"] or "").strip()
            if cand:
                sus_vendor = cand

    # Ledger-side: txn_hash is the Beancount content hash. Scan the
    # in-memory ledger entries to find the matching payee.
    if sus_vendor is None and txn_hash:
        try:
            from beancount.core.data import Transaction
            from lamella.core.beancount_io.txn_hash import txn_hash as _hash
            for e in reader.load().entries:
                if not isinstance(e, Transaction):
                    continue
                if _hash(e) == txn_hash:
                    cand = (e.payee or e.narration or "").strip()
                    if cand:
                        sus_vendor = cand
                    break
        except Exception as exc:  # noqa: BLE001
            log.info("link-hypothesis ledger lookup failed: %s", exc)

    if not (sus_date or sus_total or sus_vendor):
        return None
    return VerifyHypothesis(
        suspected_date=sus_date,
        suspected_total=sus_total,
        suspected_vendor=sus_vendor,
        reason=(
            "Auto-derived from the receipt's linked transaction — "
            "this is what the bank statement / staged row knows "
            "about the event."
        ),
    )


def _merge_hypothesis(
    primary: VerifyHypothesis | None,
    fallback: VerifyHypothesis | None,
) -> VerifyHypothesis | None:
    """Combine the user's form input (primary) with the link-derived
    hypothesis (fallback). User-supplied values win field-by-field; the
    link fills in only the gaps the user left blank. Reason is the
    user's text when present, else the fallback reason.
    """
    if primary is None:
        return fallback
    if fallback is None:
        return primary
    return VerifyHypothesis(
        suspected_date=primary.suspected_date or fallback.suspected_date,
        suspected_total=primary.suspected_total or fallback.suspected_total,
        suspected_vendor=primary.suspected_vendor or fallback.suspected_vendor,
        reason=primary.reason or fallback.reason,
    )


@router.post("/documents/{doc_id}/verify", response_class=HTMLResponse)
async def verify_document(
    doc_id: int,
    request: Request,
    suspected_date: str | None = Form(default=None),
    suspected_total: str | None = Form(default=None),
    suspected_vendor: str | None = Form(default=None),
    reason: str = Form(default=""),
    dry_run: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    ai: AIService = Depends(get_ai_service),
    paperless: PaperlessClient = Depends(get_paperless),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Kick the verify cascade as a background job and return the
    progress-modal partial. The job emits every step (source
    classification, Tier 1 result, escalation reason, vision call,
    diff summary, writeback) so the user sees what happened — even
    on provider 502s.
    """
    if not settings.paperless_configured:
        raise HTTPException(503, "Paperless not configured")
    if not ai.enabled:
        raise HTTPException(503, "AI is disabled")

    hypothesis = _merge_hypothesis(
        _build_hypothesis(
            suspected_date, suspected_total, reason, suspected_vendor,
        ),
        _hypothesis_from_link(conn, reader, doc_id),
    )
    db_path = settings.db_path
    is_dry_run = bool(dry_run)

    def _work(ctx):
        ctx.set_total(1)
        ctx.emit(
            f"Verifying Paperless document #{doc_id} "
            f"({'dry run' if is_dry_run else 'live writeback'}) …",
            outcome="info",
        )
        # Each verify runs against its own short-lived AIService +
        # client so the worker thread doesn't compete with the
        # request-thread lock on the app's main connection.
        worker_conn = sqlite3.connect(str(db_path), isolation_level=None)
        worker_conn.row_factory = sqlite3.Row
        try:
            from lamella.adapters.paperless.client import PaperlessClient as PC
            from lamella.features.ai_cascade.service import AIService as AS

            worker_ai = AS(settings=settings, conn=worker_conn)
            worker_paperless = PC(
                base_url=settings.paperless_url or "",
                api_token=(
                    settings.paperless_api_token.get_secret_value()
                    if settings.paperless_api_token else ""
                ),
                extra_headers=settings.paperless_extra_headers(),
            )
            service = VerifyService(
                ai=worker_ai, paperless=worker_paperless, conn=worker_conn,
            )

            loop = asyncio.new_event_loop()
            try:
                # Run the verify cascade as a cancellable task and
                # race it against a watcher polling the job's cancel
                # event every ~250ms. When the user clicks Cancel
                # mid-AI-call, the watcher fires task.cancel() so
                # the in-flight httpx call raises CancelledError.
                # ``cancel_check`` also lets the service cooperate
                # at phase boundaries.
                cancel_event = ctx._cancel_event  # noqa: SLF001
                try:
                    outcome = loop.run_until_complete(
                        _await_with_cancel(
                            service.verify_and_correct(
                                doc_id,
                                hypothesis=hypothesis,
                                dry_run=is_dry_run,
                                progress=(
                                    lambda msg, oc, detail=None:
                                    ctx.emit(msg, outcome=oc, detail=detail)
                                ),
                                cancel_check=ctx.raise_if_cancelled,
                            ),
                            cancel_event,
                        )
                    )
                except asyncio.CancelledError as exc:
                    # Edge case: CancelledError leaked past the
                    # helper. Surface as JobCancelled so the runner
                    # stamps the job 'cancelled'.
                    raise JobCancelled() from exc
                # Close the http client that PaperlessClient opened
                # internally so the event loop can shut down cleanly.
                try:
                    loop.run_until_complete(worker_paperless.aclose())
                except Exception:  # noqa: BLE001
                    pass
            finally:
                loop.close()
        finally:
            worker_conn.close()

        ctx.advance(1)
        # Final summary line + structured detail so the terminal
        # modal shows a headline table the user can read at a
        # glance without scrolling the event log.
        if outcome.verified:
            summary = (
                f"Done — source={outcome.source_type}, "
                f"via {outcome.extraction_source}, "
                f"{len(outcome.diffs)} diff(s), "
                f"{outcome.fields_patched} field(s) patched."
            )
            ctx.emit(
                summary,
                outcome="success",
                detail={
                    "kind": "summary",
                    "fields": {
                        "source type": outcome.source_type,
                        "extraction": outcome.extraction_source,
                        "diffs": len(outcome.diffs),
                        "fields patched": outcome.fields_patched,
                        "tag applied": (
                            "yes" if outcome.tag_applied else "no"
                        ),
                        "note added": (
                            "yes" if outcome.note_added else "no"
                        ),
                        "page count": outcome.page_count,
                        "skipped reason": outcome.skipped_reason or "—",
                    },
                },
            )
        elif outcome.skipped_reason:
            ctx.emit(
                f"Skipped: {outcome.skipped_reason}",
                outcome="failure",
                detail={
                    "kind": "summary",
                    "fields": {
                        "source type": outcome.source_type,
                        "skipped reason": outcome.skipped_reason,
                    },
                },
            )
        return {
            "paperless_id": outcome.paperless_id,
            "verified": outcome.verified,
            "diffs": len(outcome.diffs),
            "fields_patched": outcome.fields_patched,
            "skipped_reason": outcome.skipped_reason,
            "extraction_source": outcome.extraction_source,
        }

    runner = request.app.state.job_runner
    return_url = request.headers.get("referer") or "/"
    job_id = runner.submit(
        kind="paperless-verify",
        title=f"Verify Paperless #{doc_id}",
        fn=_work,
        total=1,
        return_url=return_url,
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": return_url},
    )


@router.post("/documents/{doc_id}/verify/sync", response_class=HTMLResponse)
async def verify_document_sync(
    doc_id: int,
    request: Request,
    suspected_date: str | None = Form(default=None),
    suspected_total: str | None = Form(default=None),
    suspected_vendor: str | None = Form(default=None),
    reason: str = Form(default=""),
    dry_run: str | None = Form(default=None),
    settings: Settings = Depends(get_settings),
    ai: AIService = Depends(get_ai_service),
    paperless: PaperlessClient = Depends(get_paperless),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Synchronous back-compat endpoint — runs the verify flow
    inline and returns the HTML diff partial. Used by tests and
    any non-interactive caller that needs the final outcome in
    one response; not reachable from the UI."""
    if not settings.paperless_configured:
        raise HTTPException(503, "Paperless not configured")
    if not ai.enabled:
        raise HTTPException(503, "AI is disabled")

    hypothesis = _merge_hypothesis(
        _build_hypothesis(
            suspected_date, suspected_total, reason, suspected_vendor,
        ),
        _hypothesis_from_link(conn, reader, doc_id),
    )
    service = _get_verify_service(ai, paperless, conn)
    outcome = await service.verify_and_correct(
        doc_id,
        hypothesis=hypothesis,
        dry_run=bool(dry_run),
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/paperless_verify_result.html",
        {"outcome": outcome, "doc_id": doc_id},
    )


@router.post("/documents/{doc_id}/enrich", response_class=HTMLResponse)
async def enrich_document(
    doc_id: int,
    request: Request,
    vehicle: str = Form(default=""),
    entity: str = Form(default=""),
    project: str = Form(default=""),
    note_body: str = Form(default=""),
    settings: Settings = Depends(get_settings),
    ai: AIService = Depends(get_ai_service),
    paperless: PaperlessClient = Depends(get_paperless),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Push a context-enrichment note + tag to the Paperless doc.
    At least one of (vehicle, entity, project, note_body) must be
    non-empty. Safe to call repeatedly — the dedup_key guarantees
    we don't stamp the same enrichment twice."""
    if not settings.paperless_configured:
        raise HTTPException(503, "Paperless not configured")

    context = EnrichmentContext(
        vehicle=(vehicle.strip() or None),
        entity=(entity.strip() or None),
        project=(project.strip() or None),
        note_body=note_body.strip(),
    )
    service = _get_verify_service(ai, paperless, conn)
    outcome = await service.enrich_with_context(doc_id, context=context)
    return request.app.state.templates.TemplateResponse(
        request, "partials/paperless_enrich_result.html",
        {"outcome": outcome, "doc_id": doc_id},
    )
