# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Paperless-first receipts view.

This is the secondary "browse all Paperless docs" page. It reads from the
local paperless_doc_index (fed by the scheduled sync job), so it's fast
whether you have 50 docs or 50,000. The primary workflow is now
/receipts/needed, which is txn-first; this view is here for the times you
want "any unlinked receipts from Warehouse Club?" style browsing.

Linking still works — pick a candidate transaction by (amount, date window)
and one-click-link.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import json
import sqlite3
from typing import Any

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_hash
from lamella.core.config import Settings
from lamella.web.deps import (
    get_app_settings_store,
    get_db,
    get_ai_service,
    get_ledger_reader,
    get_paperless,
    get_settings,
)
from lamella.features.paperless_bridge.lookups import cached_paperless_hash
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.features.receipts.linker import ReceiptLinker
from lamella.features.receipts.matcher import MatchCandidate, find_candidates
from lamella.features.paperless_bridge.verify import VerifyService
from lamella.features.ai_cascade.service import AIService
from lamella.adapters.paperless.client import PaperlessClient
from lamella.core.settings.store import AppSettingsStore
from lamella.web.routes.paperless_verify import _hypothesis_from_link

router = APIRouter()


@dataclass
class DocRow:
    paperless_id: int
    title: str | None
    vendor: str | None
    receipt_date: date | None
    created_date: date | None
    total: Decimal | None
    last_four: str | None
    document_type_name: str | None
    # Each entry is ``{"txn_hash": str, "lamella_txn_id": str | None}``
    # so the template can render the link via the lineage UUID and
    # still show a short hash preview in the label. Keeping ``txn_hash``
    # alongside is necessary because content-hash joins on
    # receipt_links / receipt_dismissals stay keyed off the hash.
    linked_txns: list[dict]
    candidates: list[MatchCandidate]
    paperless_url: str | None


def _linked_hashes(conn, paperless_id: int) -> list[str]:
    """Return non-empty txn_hash strings linked to ``paperless_id``.

    Filters NULL / empty values so an orphaned link row (one whose
    txn_hash got nulled out by a partial migration or a hand edit)
    can't reach the template, where ``h.txn_hash[:8]`` would
    TypeError. Real-world: at least one user hit /receipts?linked=…
    500 because of exactly this state.
    """
    rows = conn.execute(
        "SELECT txn_hash FROM receipt_links "
        "WHERE paperless_id = ? AND txn_hash IS NOT NULL "
        "  AND TRIM(txn_hash) != ''",
        (paperless_id,),
    ).fetchall()
    return [r["txn_hash"] for r in rows]


def _parse_dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


@router.get("/receipts", response_class=HTMLResponse)
def receipts_page(
    request: Request,
    unlinked_only: str | None = None,
    link_status: str | None = None,
    lookback_days: int = 90,
    linked_since: int | None = None,
    q: str | None = None,
    submitted: int | None = None,
    doc_type_scope: str | None = None,
    verify_result: str | None = None,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    store: AppSettingsStore = Depends(get_app_settings_store),
):
    # Tri-state link filter: "unlinked" (default for verifying the
    # receipt-to-txn match queue), "linked" (verifying what's already
    # bound — useful when the user wants to see what got linked
    # overnight), and "all" (browse everything). The legacy
    # `unlinked_only` checkbox still works for backward-compat URLs
    # but `link_status` is the canonical param going forward.
    link_status_clean = (link_status or "").strip().lower()
    if link_status_clean not in ("linked", "unlinked", "all"):
        # Fall back to the old checkbox semantics.
        if submitted is None:
            link_status_clean = "unlinked"
        else:
            unlinked_bool = str(unlinked_only or "").strip().lower() in (
                "true", "1", "on", "yes",
            )
            link_status_clean = "unlinked" if unlinked_bool else "all"
    # Pull from the local index. If it's empty the page shows a hint to run
    # the sync job / wait for the first scheduled pass.
    #
    # ``linked_since=N`` shifts the date filter from the receipt's own date
    # (when the document was issued) to the link row's ``created_at`` (when
    # Lamella attached the receipt to a transaction). The dashboard's
    # "Receipts attached today" tile uses this so its click-through really
    # means "what got linked today" instead of "what receipts have a date
    # within the last day," which is what ``lookback_days=1`` produces.
    clauses: list[str] = []
    params: list[Any] = []
    if linked_since is not None and linked_since >= 0:
        link_since_iso = (
            date.today() - timedelta(days=max(0, int(linked_since)))
        ).isoformat()
        # receipt_links column is `linked_at` (per migration 001 +
        # 057 + 058 retain it), NOT `created_at` — that was a typo
        # that 500'd /receipts the moment a user clicked the
        # dashboard's "Receipts attached today" tile.
        clauses.append(
            "paperless_id IN (SELECT paperless_id FROM receipt_links "
            "WHERE date(linked_at) >= date(?))"
        )
        params.append(link_since_iso)
    else:
        since = (date.today() - timedelta(days=max(1, lookback_days))).isoformat()
        clauses.append(
            "(receipt_date >= ? OR (receipt_date IS NULL AND created_date >= ?))"
        )
        params.extend([since, since])
    if q:
        clauses.append(
            "(LOWER(title) LIKE ? OR LOWER(correspondent_name) LIKE ? OR LOWER(content_excerpt) LIKE ?)"
        )
        like = f"%{q.lower()}%"
        params.extend([like, like, like])
    if link_status_clean == "unlinked":
        clauses.append(
            "paperless_id NOT IN (SELECT paperless_id FROM receipt_links)"
        )
    elif link_status_clean == "linked":
        clauses.append(
            "paperless_id IN (SELECT paperless_id FROM receipt_links)"
        )
    # "all" → no link clause
    doc_scope = (doc_type_scope or "receipts_only").strip().lower()
    type_rows = conn.execute(
        "SELECT DISTINCT document_type_id, document_type_name "
        "FROM paperless_doc_index "
        "WHERE document_type_id IS NOT NULL "
        "ORDER BY LOWER(COALESCE(document_type_name, '')) ASC"
    ).fetchall()
    raw = store.get("paperless_doc_type_roles")
    role_by_type_id: dict[int, str] = {}
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    if str(k).strip().isdigit() and str(v) in {"receipt", "invoice", "ignore"}:
                        role_by_type_id[int(k)] = str(v)
        except Exception:
            role_by_type_id = {}
    receipt_like_ids = {k for k, v in role_by_type_id.items() if v in {"receipt", "invoice"}}
    if doc_scope == "receipts_only":
        if receipt_like_ids:
            placeholders = ",".join("?" for _ in receipt_like_ids)
            clauses.append(
                f"(document_type_id IS NULL OR document_type_id IN ({placeholders}))"
            )
            params.extend(sorted(receipt_like_ids))
        else:
            clauses.append("document_type_id IS NULL")
    elif doc_scope == "non_receipts":
        if receipt_like_ids:
            placeholders = ",".join("?" for _ in receipt_like_ids)
            clauses.append(
                f"(document_type_id IS NOT NULL AND document_type_id NOT IN ({placeholders}))"
            )
            params.extend(sorted(receipt_like_ids))
        else:
            clauses.append("document_type_id IS NOT NULL")

    sql = (
        "SELECT * FROM paperless_doc_index WHERE "
        + " AND ".join(clauses)
        + " ORDER BY COALESCE(receipt_date, created_date) DESC, paperless_id DESC "
        + " LIMIT 200"
    )
    index_rows = conn.execute(sql, params).fetchall()

    base_url = (settings.paperless_url or "").rstrip("/")
    entries = list(reader.load().entries)

    # Build a hash→lamella_txn_id map once so the per-row receipt
    # rendering can link each linked-hash to the immutable /txn/{uuid}
    # URL without re-walking the ledger per row. Also keep enough txn
    # context (date, payee, total amount) on hand so the receipt-row
    # verify form can pre-fill ``suspected_date`` / ``suspected_total``
    # from the LINKED txn instead of asking the user to retype values
    # the system already knows. Per user feedback: "YOU KNOW THOSE
    # THINGS BECAUSE IT IS LINKED TO A TRANSACTION."
    from beancount.core.data import Transaction as _Txn
    from lamella.core.beancount_io.txn_hash import txn_hash as _th
    from lamella.core.identity import get_txn_id as _gtid
    hash_to_lid: dict[str, str | None] = {}
    hash_to_ctx: dict[str, dict] = {}
    for _e in entries:
        if isinstance(_e, _Txn):
            _h = _th(_e)
            hash_to_lid[_h] = _gtid(_e)
            # Pick the largest-magnitude posting as the "primary"
            # amount so a multi-leg txn surfaces a meaningful number.
            _primary: Decimal | None = None
            for _p in _e.postings or []:
                if _p.units and _p.units.number is not None:
                    _amt = abs(Decimal(_p.units.number))
                    if _primary is None or _amt > _primary:
                        _primary = _amt
            hash_to_ctx[_h] = {
                "date": _e.date.isoformat() if _e.date else None,
                "payee": getattr(_e, "payee", None),
                "narration": _e.narration or None,
                "amount": str(_primary) if _primary is not None else None,
            }

    rows: list[DocRow] = []
    import logging as _logging
    _row_log = _logging.getLogger(__name__)
    for idx in index_rows:
        # Per-row try: a single malformed row (NULL hash, weird date,
        # missing column) shouldn't 500 the whole page. Log + skip.
        try:
            pid = int(idx["paperless_id"])
            total = (
                _parse_dec(idx["total_amount"])
                or _parse_dec(idx["subtotal_amount"])
            )
            rdate = (
                _parse_date(idx["receipt_date"])
                or _parse_date(idx["created_date"])
            )

            linked = _linked_hashes(conn, pid)
            linked_txns = [
                {
                    "txn_hash": h,
                    "lamella_txn_id": hash_to_lid.get(h),
                    **(hash_to_ctx.get(h) or {}),
                }
                for h in linked
            ]
            candidates: list[MatchCandidate] = []
            if not linked and total is not None and rdate is not None:
                candidates = find_candidates(
                    entries,
                    receipt_total=total,
                    receipt_date=rdate,
                    last_four=idx["payment_last_four"],
                    date_window_days=3,
                )[:5]

            rows.append(
                DocRow(
                    paperless_id=pid,
                    title=idx["title"],
                    vendor=idx["vendor"] or idx["correspondent_name"],
                    receipt_date=_parse_date(idx["receipt_date"]),
                    created_date=_parse_date(idx["created_date"]),
                    total=total,
                    last_four=idx["payment_last_four"],
                    document_type_name=idx["document_type_name"],
                    linked_txns=linked_txns,
                    candidates=candidates,
                    paperless_url=(
                        f"{base_url}/documents/{pid}/" if base_url else None
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            _row_log.exception(
                "/receipts: skipping malformed row paperless_id=%s: %s",
                idx["paperless_id"] if "paperless_id" in idx.keys() else "?",
                exc,
            )
            continue

    sync_row = conn.execute(
        "SELECT doc_count, last_incremental_sync_at, last_status, last_error "
        "FROM paperless_sync_state WHERE id = 1"
    ).fetchone()
    sync_state = dict(sync_row) if sync_row else {}

    return request.app.state.templates.TemplateResponse(
        request,
        "receipts.html",
        {
            "rows": rows,
            "link_status": link_status_clean,
            # Kept for any template still referencing the old name; the
            # canonical filter going forward is link_status.
            "unlinked_only": link_status_clean == "unlinked",
            "lookback_days": lookback_days,
            "q": q or "",
            "sync_state": sync_state,
            "doc_type_scope": doc_scope,
            "verify_result": verify_result,
            "doc_types": [dict(r) for r in type_rows],
            "receipt_like_ids": receipt_like_ids,
            "empty_index": not sync_state.get("doc_count"),
            "paperless_configured": settings.paperless_configured,
        },
    )


@router.post("/receipts/{doc_id}/link")
async def manual_link(
    doc_id: int,
    request: Request,
    txn_hash: str = Form(...),
    txn_date: str = Form(...),
    txn_amount: str = Form(...),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    try:
        parsed_date = date.fromisoformat(txn_date)
        parsed_amount = Decimal(txn_amount)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"bad input: {exc}")

    ledger = reader.load()
    known = {compute_hash(e) for e in ledger.entries if isinstance(e, Transaction)}
    if txn_hash not in known:
        raise HTTPException(
            status_code=400,
            detail=f"txn_hash {txn_hash!r} not found in ledger",
        )

    linker = ReceiptLinker(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_links=settings.connector_links_path,
    )
    linker.link(
        paperless_id=doc_id,
        txn_hash=txn_hash,
        txn_date=parsed_date,
        txn_amount=parsed_amount,
        match_method="manual",
        match_confidence=1.0,
        paperless_hash=cached_paperless_hash(conn, doc_id),
        paperless_url=paperless_url_for(settings.paperless_url, doc_id),
    )
    reader.invalidate()

    # ADR-0044: write the four canonical Lamella_* fields back to
    # Paperless so the document is searchable by entity / category /
    # txn-id / payment account. Best-effort: never breaks the link.
    try:
        from lamella.features.paperless_bridge.writeback import (
            writeback_after_link,
        )
        await writeback_after_link(
            paperless_id=doc_id,
            txn_hash=txn_hash,
            settings=settings,
            reader=reader,
            conn=conn,
        )
    except Exception:  # noqa: BLE001
        pass

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(
            f'<span class="linked">Linked to {txn_hash[:8]}…</span>'
        )
    return Response(status_code=204)


@router.post("/receipts/verify-selected")
async def verify_selected_docs(
    request: Request,
    doc_ids: list[int] = Form(default=[]),
    reason: str = Form(default=""),
    settings: Settings = Depends(get_settings),
    ai: AIService = Depends(get_ai_service),
    paperless: PaperlessClient = Depends(get_paperless),
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    if not settings.paperless_configured or not ai.enabled:
        raise HTTPException(status_code=503, detail="Paperless + AI must be configured")
    picked = [int(x) for x in doc_ids if str(x).strip().isdigit()]
    if not picked:
        return RedirectResponse("/receipts?verify_result=No+documents+selected", status_code=303)
    chosen = picked[:25]

    def _work(ctx):
        import asyncio
        worker_conn = sqlite3.connect(str(settings.db_path), isolation_level=None)
        worker_conn.row_factory = sqlite3.Row
        try:
            from lamella.features.ai_cascade.service import AIService as _AS
            from lamella.adapters.paperless.client import PaperlessClient as _PC
            worker_ai = _AS(settings=settings, conn=worker_conn)
            worker_paperless = _PC(
                base_url=settings.paperless_url or "",
                api_token=(settings.paperless_api_token.get_secret_value() if settings.paperless_api_token else ""),
                extra_headers=settings.paperless_extra_headers(),
            )
            service = VerifyService(ai=worker_ai, paperless=worker_paperless, conn=worker_conn)
            loop = asyncio.new_event_loop()
            try:
                ctx.set_total(len(chosen))
                for doc_id in chosen:
                    hypo = _hypothesis_from_link(worker_conn, reader, doc_id)
                    if hypo and reason.strip():
                        hypo = hypo.model_copy(update={"reason": reason.strip()})
                    ctx.emit(f"Verifying #{doc_id} …", outcome="info")
                    loop.run_until_complete(
                        service.verify_and_correct(doc_id, hypothesis=hypo, dry_run=False)
                    )
                    ctx.advance(1)
            finally:
                loop.run_until_complete(worker_paperless.aclose())
                loop.close()
        finally:
            worker_conn.close()
        return {"done": len(chosen)}

    runner = request.app.state.job_runner
    return_url = "/receipts"
    job_id = runner.submit(
        kind="paperless-verify-bulk",
        title=f"Verify {len(chosen)} Paperless docs",
        fn=_work,
        total=len(chosen),
        return_url=return_url,
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": return_url},
    )
