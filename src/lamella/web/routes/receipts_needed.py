# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Receipts-needed queue: the txn-first view of expenses awaiting finalization.

Per row: a transaction with at least one Expenses posting, not yet linked to
a receipt and not dismissed as "no receipt expected". Candidates come from
the local paperless_doc_index via the cascading matcher. Actions:

  * Link: attach a Paperless doc to the txn (ReceiptLinker). Optional
    target_account, applied as a FIXME override if the txn has a FIXME
    posting.
  * Dismiss: record in receipt_dismissals so the row stops re-appearing.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, Response

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_hash
from lamella.core.config import Settings
from lamella.web.deps import (
    get_db,
    get_ledger_reader,
    get_settings,
)
from lamella.features.paperless_bridge.lookups import cached_paperless_hash
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.features.receipts.dismissals_writer import (
    append_dismissal,
    append_dismissal_revoke,
)
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.receipts.linker import ReceiptLinker
from lamella.features.receipts.needs_queue import (
    build_queue,
    find_orphan_dismissals,
)
from lamella.features.receipts.txn_matcher import (
    ScoredCandidate,
    find_paperless_candidates,
)
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.scanner import _fixme_amount, _is_fixme

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass
class QueueRow:
    item: Any                          # NeedsReceiptItem
    required: bool
    candidates: list[ScoredCandidate]
    paperless_url: str | None          # link to doc id N/A at row level


def _paperless_base(settings: Settings) -> str:
    return (settings.paperless_url or "").rstrip("/")


def _build_needed_context(
    *,
    settings: Settings,
    conn,
    reader: LedgerReader,
    lookback_days: int,
    include_linked: bool,
    include_dismissed: bool,
    include_non_receipt: bool,
    required_only: bool,
    page: int,
    page_size: int,
    query: str | None = None,
    account_filter: str | None = None,
    min_amount: Decimal | None = None,
    max_amount: Decimal | None = None,
) -> dict:
    """Heavy lifting: load ledger, build queue, score Paperless
    candidates for the page slice. Shared between the main shell
    (which only needs the bind-params to render the filter form)
    and the partial endpoint that actually produces row bodies."""
    entries = list(reader.load().entries)
    raw, stats = build_queue(
        entries=entries,
        conn=conn,
        threshold_usd=settings.receipt_required_threshold_usd,
        lookback_days=lookback_days,
        include_linked=include_linked,
        include_dismissed=include_dismissed,
        include_non_receipt=include_non_receipt,
        query=query,
        account_filter=account_filter,
        min_amount=min_amount,
        max_amount=max_amount,
    )
    if required_only:
        raw = [(item, req) for item, req in raw if req]
    base_url = _paperless_base(settings)

    total_rows = len(raw)
    page = max(1, int(page))
    page_size = max(5, min(100, int(page_size)))
    start = (page - 1) * page_size
    end = start + page_size
    page_slice = raw[start:end]
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 1

    rows: list[QueueRow] = []
    for item, required in page_slice:
        candidates = find_paperless_candidates(
            conn,
            txn_amount=item.max_expense_amount,
            txn_date=item.txn_date,
            narration=item.narration,
            payee=item.payee,
            last_four=item.last_four,
        )
        rows.append(
            QueueRow(
                item=item,
                required=required,
                candidates=candidates,
                paperless_url=base_url or None,
            )
        )

    sync_row = conn.execute(
        "SELECT last_full_sync_at, last_incremental_sync_at, "
        "       last_modified_cursor, doc_count, last_status, last_error "
        "FROM paperless_sync_state WHERE id = 1"
    ).fetchone()
    sync_state = dict(sync_row) if sync_row else {}

    # Orphan dismissals = rows whose txn_hash no longer matches any
    # current ledger transaction. Surfaces "previously dismissed on X —
    # transaction has since been edited" so the user can explain the
    # re-appearance and one-click re-dismiss against the new hash.
    orphans = find_orphan_dismissals(entries, conn)

    return {
        "rows": rows,
        "orphans": orphans,
        "lookback_days": lookback_days,
        "include_linked": include_linked,
        "include_dismissed": include_dismissed,
        "include_non_receipt": include_non_receipt,
        "required_only": required_only,
        "query": query or "",
        "account_filter": account_filter or "",
        "min_amount": str(min_amount) if min_amount is not None else "",
        "max_amount": str(max_amount) if max_amount is not None else "",
        "threshold_usd": settings.receipt_required_threshold_usd,
        "sync_state": sync_state,
        "paperless_base_url": base_url or None,
        "stats": stats,
        "total_rows": total_rows,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "start_index": start + 1 if total_rows else 0,
        "end_index": min(end, total_rows),
    }


def _partial_url_for(
    *,
    lookback_days: int,
    include_linked: bool,
    include_dismissed: bool,
    include_non_receipt: bool,
    required_only: bool,
    page: int,
    page_size: int,
    query: str | None = None,
    account_filter: str | None = None,
    min_amount: str | None = None,
    max_amount: str | None = None,
) -> str:
    from urllib.parse import urlencode
    bits: dict[str, str] = {
        "lookback_days": str(lookback_days),
        "page": str(page),
        "page_size": str(page_size),
    }
    if include_linked:
        bits["include_linked"] = "true"
    if include_dismissed:
        bits["include_dismissed"] = "true"
    if include_non_receipt:
        bits["include_non_receipt"] = "true"
    if required_only:
        bits["required_only"] = "true"
    if query:
        bits["q"] = query
    if account_filter:
        bits["account"] = account_filter
    if min_amount:
        bits["min_amount"] = min_amount
    if max_amount:
        bits["max_amount"] = max_amount
    return "/receipts/needed/partial?" + urlencode(bits)


def _coerce_decimal(raw: str | None) -> Decimal | None:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


def _count_needs_receipt(
    conn,
    *,
    reader: LedgerReader,
    settings: Settings,
    lookback_days: int = 90,
) -> int:
    """Return how many transactions are over the receipt-required
    threshold, not yet linked to a receipt and not dismissed.

    The dashboard's "Receipts needed" KPI tile uses this count. The
    rule mirrors ``build_queue`` with ``required_only=True`` so the
    number on the dashboard matches what the user sees when they click
    through to ``/receipts/needed?required_only=true``.

    Caller supplies the same ``LedgerReader`` and ``Settings`` the
    rest of the dashboard route is using so the cache stays warm and
    the threshold matches the user's config.
    """
    entries = list(reader.load().entries)
    raw, _stats = build_queue(
        entries=entries,
        conn=conn,
        threshold_usd=settings.receipt_required_threshold_usd,
        lookback_days=lookback_days,
        include_linked=False,
        include_dismissed=False,
        include_non_receipt=False,
    )
    return sum(1 for _item, required in raw if required)


@router.get("/receipts/needed", response_class=HTMLResponse)
def needed_page(
    request: Request,
    lookback_days: int = 90,
    include_linked: bool = False,
    include_dismissed: bool = False,
    include_non_receipt: bool = False,
    required_only: bool = False,
    page: int = 1,
    page_size: int = 25,
    q: str = "",
    account: str = "",
    min_amount: str = "",
    max_amount: str = "",
    settings: Settings = Depends(get_settings),
):
    """Shell-only render — returns near-instantly. The row body is
    loaded via hx-get against /receipts/needed/partial so the user
    sees a proper loading state instead of waiting on a blank tab
    while the per-row Paperless candidate queries run."""
    partial_url = _partial_url_for(
        lookback_days=lookback_days,
        include_linked=include_linked,
        include_dismissed=include_dismissed,
        include_non_receipt=include_non_receipt,
        required_only=required_only,
        page=max(1, int(page)),
        page_size=max(5, min(100, int(page_size))),
        query=q.strip() or None,
        account_filter=account.strip() or None,
        min_amount=min_amount.strip() or None,
        max_amount=max_amount.strip() or None,
    )
    ctx = {
        "lookback_days": lookback_days,
        "include_linked": include_linked,
        "include_dismissed": include_dismissed,
        "include_non_receipt": include_non_receipt,
        "required_only": required_only,
        "query": q.strip(),
        "account_filter": account.strip(),
        "min_amount": min_amount.strip(),
        "max_amount": max_amount.strip(),
        "page": page,
        "page_size": page_size,
        "threshold_usd": settings.receipt_required_threshold_usd,
        "partial_url": partial_url,
    }
    return request.app.state.templates.TemplateResponse(
        request, "receipts_needed.html", ctx
    )


@router.get("/receipts/needed/partial", response_class=HTMLResponse)
def needed_partial(
    request: Request,
    lookback_days: int = 90,
    include_linked: bool = False,
    include_dismissed: bool = False,
    include_non_receipt: bool = False,
    required_only: bool = False,
    page: int = 1,
    page_size: int = 25,
    q: str = "",
    account: str = "",
    min_amount: str = "",
    max_amount: str = "",
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Heavy partial — runs the ledger load + queue build + per-row
    Paperless candidate queries. Called by the shell via hx-get on
    page load, and after form submits (which navigate to the shell
    URL, not here)."""
    ctx = _build_needed_context(
        settings=settings,
        conn=conn,
        reader=reader,
        lookback_days=lookback_days,
        include_linked=include_linked,
        include_dismissed=include_dismissed,
        include_non_receipt=include_non_receipt,
        required_only=required_only,
        page=page,
        page_size=page_size,
        query=q.strip() or None,
        account_filter=account.strip() or None,
        min_amount=_coerce_decimal(min_amount),
        max_amount=_coerce_decimal(max_amount),
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/receipts_needed_body.html", ctx
    )


def _find_txn(reader: LedgerReader, target_hash: str) -> Transaction | None:
    for entry in reader.load().entries:
        if isinstance(entry, Transaction) and compute_hash(entry) == target_hash:
            return entry
    return None


def _to_date(value) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


@router.post("/receipts/needed/{txn_hash}/link")
async def link_txn_to_doc(
    txn_hash: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Link one OR many Paperless documents to this transaction.
    Accepts `paperless_id` as a repeated form field so the user can
    check multiple candidates (invoice + receipt, etc.) and commit
    them in one click."""
    form = await request.form()
    raw_ids = [v for (k, v) in form.multi_items() if k == "paperless_id" and v]
    paperless_ids: list[int] = []
    for raw in raw_ids:
        try:
            pid = int(str(raw).strip())
        except ValueError:
            continue
        if pid and pid not in paperless_ids:
            paperless_ids.append(pid)
    if not paperless_ids:
        raise HTTPException(status_code=400, detail="select at least one document")
    target_account = (form.get("target_account") or "").strip() or None
    match_method = (form.get("match_method") or "user_confirmed").strip() or "user_confirmed"
    txn = _find_txn(reader, txn_hash)
    if txn is None:
        raise HTTPException(status_code=404, detail="transaction not found in ledger")

    # Amount: prefer the largest |receipt-target| posting
    # (Expenses / Income / Liabilities / Equity — AI-AGENT.md
    # Phase 2 widened receipt scope); fall back to any posting.
    amount: Decimal | None = None
    _target_roots = ("Expenses", "Income", "Liabilities", "Equity")
    for p in txn.postings:
        acct = p.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root not in _target_roots:
            continue
        if p.units and p.units.number is not None:
            val = abs(Decimal(p.units.number))
            if amount is None or val > amount:
                amount = val
    if amount is None:
        for p in txn.postings:
            if p.units and p.units.number is not None:
                amount = abs(Decimal(p.units.number))
                break
    if amount is None:
        raise HTTPException(status_code=400, detail="no posting with a numeric amount")

    # Optional FIXME override: only meaningful when the txn still has a FIXME
    # leg. A target_account on a non-FIXME txn is a no-op (the txn already
    # has real accounts); we ignore it rather than write a double posting.
    has_fixme = any(_is_fixme(p.account) for p in txn.postings)
    if target_account and has_fixme:
        fixme_amount = _fixme_amount(txn)
        from_account = next(
            (p.account for p in txn.postings if _is_fixme(p.account)), None
        )
        currency = "USD"
        for p in txn.postings:
            if _is_fixme(p.account) and p.units and p.units.currency:
                currency = p.units.currency
                break
        if fixme_amount is None or from_account is None:
            raise HTTPException(
                status_code=400,
                detail="FIXME posting has no numeric amount; cannot override",
            )
        # Per CLAUDE.md "in-place rewrites are the default" —
        # rewrite the FIXME posting line directly. Override
        # fallback only fires on path-safety refusal or missing
        # filename/lineno meta on the txn.
        from pathlib import Path as _P
        from lamella.core.rewrite.txn_inplace import (
            InPlaceRewriteError,
            rewrite_fixme_to_account,
        )
        writer = OverrideWriter(
            main_bean=settings.ledger_main,
            overrides=settings.connector_overrides_path,
            conn=conn,
        )

        in_place_done = False
        meta = getattr(txn, "meta", None) or {}
        src_file = meta.get("filename")
        src_lineno = meta.get("lineno")
        fixme_signed = None
        for _p in txn.postings or ():
            if _is_fixme(_p.account) and _p.units \
                    and _p.units.number is not None:
                fixme_signed = Decimal(_p.units.number)
                break
        if src_file and src_lineno is not None:
            try:
                try:
                    writer.rewrite_without_hash(txn_hash)
                except BeanCheckError:
                    raise InPlaceRewriteError(
                        "override-strip blocked"
                    )
                rewrite_fixme_to_account(
                    source_file=_P(src_file),
                    line_number=int(src_lineno),
                    old_account=from_account,
                    new_account=target_account,
                    expected_amount=fixme_signed,
                    ledger_dir=settings.ledger_dir,
                    main_bean=settings.ledger_main,
                )
                in_place_done = True
            except InPlaceRewriteError as exc:
                log.info(
                    "receipts-needed accept: in-place refused: %s "
                    "— falling back to override", exc,
                )

        if not in_place_done:
            try:
                writer.append(
                    txn_date=_to_date(txn.date),
                    txn_hash=txn_hash,
                    amount=fixme_amount,
                    from_account=from_account,
                    to_account=target_account,
                    currency=currency,
                    narration=(txn.narration or "FIXME override"),
                )
            except BeanCheckError as exc:
                log.error("override rejected by bean-check: %s", exc)
                raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")

    # Link writes receipt_links row + stamp in connector_links.bean; if bean-check
    # fails it reverts both. Does not touch connector_overrides.bean.
    linker = ReceiptLinker(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_links=settings.connector_links_path,
    )
    linked_ids: list[int] = []
    failed: list[tuple[int, str]] = []
    for pid in paperless_ids:
        try:
            linker.link(
                paperless_id=pid,
                txn_hash=txn_hash,
                txn_date=_to_date(txn.date),
                txn_amount=amount,
                match_method=match_method,
                match_confidence=1.0,
                paperless_hash=cached_paperless_hash(conn, pid),
                paperless_url=paperless_url_for(settings.paperless_url, pid),
            )
            linked_ids.append(pid)
        except BeanCheckError as exc:
            log.error("receipt link rejected by bean-check: #%s %s", pid, exc)
            failed.append((pid, str(exc)))

    reader.invalidate()

    # ADR-0044: write the four canonical Lamella_* fields back to
    # Paperless for each successful link. Best-effort: never breaks
    # the link / queue UX.
    if linked_ids:
        try:
            from lamella.features.paperless_bridge.writeback import (
                writeback_after_link,
            )
            for pid in linked_ids:
                await writeback_after_link(
                    paperless_id=pid,
                    txn_hash=txn_hash,
                    settings=settings,
                    reader=reader,
                    conn=conn,
                )
        except Exception:  # noqa: BLE001
            pass

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        if not linked_ids:
            # Every attempt failed — keep the row in the queue so the
            # user can retry, but surface the bean-check error inline.
            err_html = "".join(
                f'<details><summary>#{pid}</summary><pre class="excerpt">{msg}</pre></details>'
                for pid, msg in failed
            )
            return HTMLResponse(
                f'<li id="needs-row-{txn_hash}" class="needs-card needs-card--error">'
                f'<header class="needs-card__head">'
                f'<div class="needs-card__txn">'
                f'<strong>bean-check blocked every link.</strong>'
                f'{err_html}</div></header></li>',
                status_code=200,
            )
        # Success card: keeps the row visible, replaces the candidates
        # form with a "✓ Linked" panel so the user can see WHAT they
        # linked and click through to verify in Paperless. Stays in the
        # DOM (not removed); the user moves on by scrolling.
        linked_bits = "".join(
            f'<a class="linked-receipt-pill" href="/paperless/preview/{pid}" '
            f'target="_blank" rel="noopener">#{pid}</a>'
            for pid in linked_ids
        )
        skipped_bits = (
            f'<span class="muted small">'
            f' · {len(failed)} blocked by bean-check</span>'
            if failed else ""
        )
        suffix = (
            f' <span class="muted small">→ {target_account}</span>'
            if (target_account and has_fixme) else ""
        )
        return HTMLResponse(
            f'<li id="needs-row-{txn_hash}" class="needs-card needs-card--linked">'
            f'<header class="needs-card__head">'
            f'<div class="needs-select-spacer" aria-hidden="true"></div>'
            f'<div class="needs-card__txn">'
            f'<div class="needs-card__linked-status">'
            f'<span class="needs-card__check" aria-hidden="true">✓</span>'
            f'<strong>Linked {len(linked_ids)}'
            f'{"" if len(linked_ids) == 1 else " receipts"}</strong>'
            f'{suffix}{skipped_bits}'
            f'</div>'
            f'<div class="needs-card__linked-pills">{linked_bits}</div>'
            f'</div></header></li>',
            status_code=200,
        )
    if not linked_ids:
        raise HTTPException(status_code=500, detail=f"all links failed: {failed}")
    return Response(status_code=204)


@router.post("/receipts/needed/{txn_hash}/dismiss")
def dismiss_txn(
    txn_hash: str,
    request: Request,
    reason: str | None = Form(default=None),
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    # Verify the txn actually exists so users can't pollute the dismissals
    # table with stale hashes.
    if _find_txn(reader, txn_hash) is None:
        raise HTTPException(status_code=404, detail="transaction not found in ledger")

    reason_clean = (reason or "").strip() or None

    # Dual-write: ledger first (since that's the source of truth for
    # reconstruct), then cache. If the ledger write fails (bean-check
    # regression) the cache update never runs.
    try:
        append_dismissal(
            connector_links=settings.connector_links_path,
            main_bean=settings.ledger_main,
            txn_hash=txn_hash,
            reason=reason_clean,
            dismissed_by="user",
        )
    except BeanCheckError as exc:
        log.error("dismissal ledger write rejected: %s", exc)
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()

    conn.execute(
        "INSERT INTO receipt_dismissals (txn_hash, reason, dismissed_by) "
        "VALUES (?, ?, 'user') "
        "ON CONFLICT (txn_hash) DO UPDATE SET reason = excluded.reason, "
        "  dismissed_at = CURRENT_TIMESTAMP",
        (txn_hash, reason_clean),
    )

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(
            f'<li id="needs-row-{txn_hash}" class="needs-card needs-card--linked">'
            f'<header class="needs-card__head">'
            f'<div class="needs-select-spacer" aria-hidden="true"></div>'
            f'<div class="needs-card__txn">'
            f'<div class="needs-card__linked-status">'
            f'<span class="needs-card__check" aria-hidden="true">✓</span>'
            f'<strong>Dismissed</strong>'
            f'<span class="muted small">no receipt expected</span>'
            f'</div></div></header></li>'
        )
    return Response(status_code=204)


@router.post("/receipts/needed/bulk/dismiss")
async def bulk_dismiss(
    request: Request,
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Dismiss multiple transactions as 'no receipt expected' in one
    request. Form fields:
      txn_hash  — repeated, one per selected row
      reason    — single reason applied to every dismissal
    Behavior mirrors the per-row /dismiss endpoint: ledger write
    first (each in a separate `append_dismissal` call so any
    bean-check failure is isolated to its own row), then DB cache.
    Rows that fail their ledger write are reported in the response;
    successful rows are committed.
    """
    form = await request.form()
    raw_hashes = [v for (k, v) in form.multi_items() if k == "txn_hash" and v]
    hashes: list[str] = []
    for raw in raw_hashes:
        s = str(raw).strip()
        if s and s not in hashes:
            hashes.append(s)
    if not hashes:
        raise HTTPException(status_code=400, detail="select at least one transaction")
    reason_clean = (form.get("reason") or "").strip() or None

    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    # Verify each hash before writing — it's cheap and keeps the
    # ledger from accumulating stale dismissal directives.
    valid_hashes: set[str] = set()
    for entry in reader.load().entries:
        if isinstance(entry, Transaction):
            valid_hashes.add(compute_hash(entry))

    for h in hashes:
        if h not in valid_hashes:
            failed.append((h, "not in ledger"))
            continue
        try:
            append_dismissal(
                connector_links=settings.connector_links_path,
                main_bean=settings.ledger_main,
                txn_hash=h,
                reason=reason_clean,
                dismissed_by="user",
            )
        except BeanCheckError as exc:
            log.error("bulk dismissal ledger write rejected for %s: %s", h, exc)
            failed.append((h, f"bean-check: {exc}"))
            continue
        conn.execute(
            "INSERT INTO receipt_dismissals (txn_hash, reason, dismissed_by) "
            "VALUES (?, ?, 'user') "
            "ON CONFLICT (txn_hash) DO UPDATE SET reason = excluded.reason, "
            "  dismissed_at = CURRENT_TIMESTAMP",
            (h, reason_clean),
        )
        succeeded.append(h)

    if succeeded:
        reader.invalidate()

    is_htmx = "hx-request" in {k.lower() for k in request.headers.keys()}
    if is_htmx:
        # Refresh the page to redraw the queue without the dismissed rows.
        # Done as a header so the user sees state update immediately.
        resp = Response(status_code=204)
        resp.headers["HX-Refresh"] = "true"
        if failed:
            # Attach a one-line summary as a header the client can show.
            resp.headers["X-Lamella-Notice"] = (
                f"Dismissed {len(succeeded)}; {len(failed)} failed"
            )
        return resp

    # Vanilla form post — bounce back to /receipts/needed so the user
    # sees the updated queue.
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/receipts/needed", status_code=303)


@router.get(
    "/receipts/needed/{txn_hash}/search", response_class=HTMLResponse
)
async def search_paperless_for_row(
    txn_hash: str,
    request: Request,
    q: str = "",
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Manual Paperless search scoped to one transaction's row. The user
    types a query (vendor, OCR snippet, doc title) and we return up
    to 12 candidate rows formatted exactly like the auto-matched
    candidates so they plug into the same /link form.

    The candidates partial expects a list of ScoredCandidate-shaped
    objects; we adapt the Paperless Document model into ScoredCandidate
    with score=0.0 and a single "manual search" reason.
    """
    txn = _find_txn(reader, txn_hash)
    if txn is None:
        raise HTTPException(status_code=404, detail="transaction not found in ledger")

    query = q.strip()
    if not query:
        return HTMLResponse(
            '<p class="muted">Type a query to search Paperless.</p>'
        )

    if not settings.paperless_configured:
        return HTMLResponse(
            '<p class="error-inline">Paperless is not configured.</p>'
        )

    from lamella.adapters.paperless.client import PaperlessClient, PaperlessError

    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    docs: list = []
    try:
        # Paperless full-text search via the `query=` param (FTS5
        # against title + content). Page size of 12 is plenty for a
        # manual-pick UI; users can refine the query if not enough.
        async for doc in client.iter_documents({"query": query, "page_size": 12}):
            docs.append(doc)
            if len(docs) >= 12:
                break
    except PaperlessError as exc:
        return HTMLResponse(
            f'<p class="error-inline">Paperless error: {exc}</p>'
        )
    finally:
        await client.aclose()

    # Adapt Document → ScoredCandidate-shape so the existing
    # candidate template renders without modification.
    cands: list[ScoredCandidate] = []
    for d in docs:
        created = d.created
        if isinstance(created, date) and not hasattr(created, "hour"):
            cdate = created
        elif created is not None:
            try:
                cdate = created.date()
            except Exception:
                cdate = None
        else:
            cdate = None
        cands.append(
            ScoredCandidate(
                paperless_id=d.id,
                title=d.title,
                correspondent_name=None,
                created_date=cdate,
                receipt_date=None,
                total_amount=None,
                score=0.0,
                reasons=("manual search",),
            )
        )

    base = _paperless_base(settings)
    ctx = {
        "row": {
            "item": {"txn_hash": txn_hash, "is_fixme": any(
                _is_fixme(p.account) for p in txn.postings
            )},
            "candidates": cands,
            "paperless_url": base or None,
        },
        "txn_hash": txn_hash,
        "paperless_url": base or None,
        "search_query": query,
    }
    return request.app.state.templates.TemplateResponse(
        request, "partials/needs_receipt_search_results.html", ctx
    )


@router.post("/receipts/needed/{txn_hash}/undismiss")
def undismiss_txn(
    txn_hash: str,
    request: Request,
    conn = Depends(get_db),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    # Append-only revoke directive so reconstruct sees the current state
    # without us rewriting the original dismissal block.
    try:
        append_dismissal_revoke(
            connector_links=settings.connector_links_path,
            main_bean=settings.ledger_main,
            txn_hash=txn_hash,
        )
    except BeanCheckError as exc:
        log.error("dismissal revoke ledger write rejected: %s", exc)
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()

    conn.execute("DELETE FROM receipt_dismissals WHERE txn_hash = ?", (txn_hash,))
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("")
    return Response(status_code=204)
