# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Receipt-attach actions for staged + ledger /txn/{token} pages.

For ledger txns the /txn/{token} surface already lists linked receipts
and offers a "link another" flow via :mod:`lamella.web.routes.receipts`
keyed off the Beancount content hash. Staged rows have no content hash
yet, so the existing "Link" button on /receipts/needed isn't reachable
from the staged-side detail page. This module fills the gap.

The directives in ``connector_links.bean`` are keyed by the txn's
``lamella-txn-id`` (UUIDv7) — staged rows already carry that id
(ADR-0046 Phase 4b), so the existing
:class:`lamella.features.receipts.linker.ReceiptLinker` works without
modification: we simply pass the UUIDv7 as ``txn_hash``. The directive
shape is identical pre- and post-promotion, which keeps the receipt
joinable from either side via the same identity.

Endpoints exposed here:

* ``GET  /txn/{token}/receipt-section`` — partial that lists currently
  linked Paperless docs for this transaction plus a search box.
* ``GET  /txn/{token}/receipt-search?q=<query>`` — Paperless full-text
  search; returns a partial of candidate documents with Link buttons.
* ``POST /txn/{token}/receipt-link`` — append a ``custom "receipt-link"``
  block via :class:`ReceiptLinker`. Returns 204 + ``HX-Refresh`` so the
  parent page reloads with the now-linked doc visible.
* ``POST /txn/{token}/receipt-unlink`` — strip the matching
  ``custom "receipt-link"`` block via :func:`remove_receipt_link`.
  204 + ``HX-Refresh`` on success.

All four endpoints accept either a staged row's UUIDv7 or a ledger
txn's UUIDv7 — the linker treats them the same. They reject legacy
hex tokens with 404 (ADR-0019 / v3 cutover).
"""
from __future__ import annotations

import logging
import re
import sqlite3
from datetime import date
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, Response

from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_hash
from lamella.core.config import Settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.import_.staging import StagedRow, StagingService
from lamella.features.paperless_bridge.lookups import cached_paperless_hash
from lamella.features.receipts.linker import ReceiptLinker, remove_receipt_link
from lamella.features.receipts.link_block_writer import append_link_block
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


_UUIDV7_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _is_uuidv7_token(token: str) -> bool:
    return bool(token) and bool(_UUIDV7_RE.match(token))


def _require_uuidv7(token: str) -> None:
    if not _is_uuidv7_token(token):
        raise HTTPException(
            status_code=404,
            detail=(
                "/txn/{token}/receipt-* paths accept only a "
                "lamella-txn-id (UUIDv7); legacy hex was retired in v3."
            ),
        )


def _find_ledger_txn(
    reader: LedgerReader, lamella_id: str,
) -> Transaction | None:
    """Walk the ledger looking for a Transaction whose
    ``lamella-txn-id`` (or any ``lamella-txn-id-alias-N``) matches.
    Mirrors ``search.py::_find_txn_by_lamella_id`` so we don't import
    a private helper across modules."""
    from lamella.core.identity import get_txn_id as _get_txn_id
    target = lamella_id.lower()
    for entry in reader.load().entries:
        if not isinstance(entry, Transaction):
            continue
        primary = _get_txn_id(entry)
        if primary and primary.lower() == target:
            return entry
        meta = getattr(entry, "meta", None) or {}
        for k, v in meta.items():
            if not isinstance(k, str):
                continue
            if k.startswith("lamella-txn-id-alias-") and v:
                if str(v).lower() == target:
                    return entry
    return None


def _resolve_token(
    *, conn: sqlite3.Connection, reader: LedgerReader, token: str,
) -> tuple[StagedRow | None, Transaction | None]:
    """Resolve ``token`` to either a staged row or a ledger Transaction
    (or both, if a row was promoted but the staged record still exists).

    Raises 404 when neither side knows about the id — caller doesn't
    need to re-derive that branch."""
    _require_uuidv7(token)
    staged: StagedRow | None
    try:
        staged = StagingService(conn).get_by_lamella_txn_id(token)
    except Exception:  # noqa: BLE001
        staged = None
    ledger = _find_ledger_txn(reader, token)
    if staged is None and ledger is None:
        raise HTTPException(
            status_code=404,
            detail=f"no staged row or ledger txn with id {token[:12]}…",
        )
    return staged, ledger


def _txn_date_for_link(
    *, staged: StagedRow | None, ledger: Transaction | None,
) -> date:
    """Pick a posting date for the receipt-link directive.
    Prefer the ledger entry's date when we have one (ledger is the
    source of truth); fall back to the staged row's posting_date."""
    if ledger is not None:
        d = ledger.date
        if isinstance(d, date):
            return d
        return date.fromisoformat(str(d))
    assert staged is not None
    return date.fromisoformat(str(staged.posting_date))


def _txn_amount_for_link(
    *, staged: StagedRow | None, ledger: Transaction | None,
) -> Decimal:
    """Pick a representative amount for the receipt-link directive's
    ``lamella-txn-amount`` meta. For a ledger txn we re-use the receipts
    rule (largest |Expenses/Income/Liabilities/Equity| posting). For a
    staged row we have a single signed amount on the row itself."""
    if ledger is not None:
        amount: Decimal | None = None
        for p in ledger.postings:
            acct = p.account or ""
            root = acct.split(":", 1)[0]
            if root not in ("Expenses", "Income", "Liabilities", "Equity"):
                continue
            if p.units and p.units.number is not None:
                v = abs(Decimal(p.units.number))
                if amount is None or v > amount:
                    amount = v
        if amount is None:
            for p in ledger.postings:
                if p.units and p.units.number is not None:
                    amount = abs(Decimal(p.units.number))
                    break
        if amount is not None:
            return amount
    if staged is not None:
        return abs(Decimal(staged.amount))
    return Decimal("0")


def _linked_docs(
    *, conn: sqlite3.Connection, txn_id: str, paperless_base: str,
) -> list[dict[str, Any]]:
    """Return the receipt-links cache rows for this txn id, joined to
    paperless_doc_index for display fields. The cache is refreshed by
    the linker itself and reconstruct paths, so this is the right
    surface to read for "what's linked to this txn right now"."""
    rows = conn.execute(
        """
        SELECT rl.paperless_id,
               rl.match_method,
               rl.match_confidence,
               rl.linked_at,
               pdi.title,
               pdi.correspondent_name,
               pdi.total_amount,
               pdi.receipt_date,
               pdi.created_date
          FROM receipt_links rl
          LEFT JOIN paperless_doc_index pdi
            ON pdi.paperless_id = rl.paperless_id
         WHERE rl.txn_hash = ?
         ORDER BY rl.linked_at DESC
        """,
        (txn_id,),
    ).fetchall()
    base = (paperless_base or "").rstrip("/")
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["paperless_deep_link"] = (
            f"{base}/documents/{d['paperless_id']}/" if base else None
        )
        out.append(d)
    return out


@router.get(
    "/txn/{token}/receipt-section", response_class=HTMLResponse,
)
def receipt_section(
    token: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """HTMX partial — renders the receipt-attach UI for a staged or
    ledger txn. Listed-docs come from the local receipt_links cache so
    the render is a single SELECT; the search box loads candidates
    on demand against the Paperless API."""
    staged, ledger = _resolve_token(conn=conn, reader=reader, token=token)
    paperless_base = (settings.paperless_url or "").rstrip("/")
    linked = _linked_docs(
        conn=conn, txn_id=token, paperless_base=paperless_base,
    )
    is_staged = ledger is None and staged is not None
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_txn_receipt_section.html",
        {
            "token": token,
            "linked": linked,
            "is_staged": is_staged,
            "paperless_configured": bool(settings.paperless_configured),
            "paperless_base_url": paperless_base or None,
        },
    )


@router.get(
    "/txn/{token}/receipt-search", response_class=HTMLResponse,
)
async def receipt_search(
    token: str,
    request: Request,
    q: str = "",
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Paperless full-text search scoped to one /txn/{token} page.
    Returns a partial of candidate documents whose "Link" button posts
    back to ``/txn/{token}/receipt-link``."""
    _resolve_token(conn=conn, reader=reader, token=token)

    query = q.strip()
    if not query:
        return HTMLResponse(
            '<p class="muted small" style="margin:0">'
            'Type a query to search Paperless.</p>'
        )

    if not settings.paperless_configured:
        return HTMLResponse(
            '<p class="error-inline">Paperless is not configured.</p>'
        )

    from lamella.adapters.paperless.client import (
        PaperlessClient,
        PaperlessError,
    )

    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=(
            settings.paperless_api_token.get_secret_value()  # type: ignore[union-attr]
        ),
        extra_headers=settings.paperless_extra_headers(),
    )
    docs: list = []
    try:
        async for doc in client.iter_documents(
            {"query": query, "page_size": 12},
        ):
            docs.append(doc)
            if len(docs) >= 12:
                break
    except PaperlessError as exc:
        return HTMLResponse(
            f'<p class="error-inline">Paperless error: {exc}</p>'
        )
    finally:
        await client.aclose()

    base = (settings.paperless_url or "").rstrip("/")
    # Build a tiny serializable shape so the template doesn't reach
    # into Paperless schema fields by name.
    candidates: list[dict[str, Any]] = []
    for d in docs:
        created = d.created
        if isinstance(created, date) and not hasattr(created, "hour"):
            cdate = created
        elif created is not None:
            try:
                cdate = created.date()
            except Exception:  # noqa: BLE001
                cdate = None
        else:
            cdate = None
        candidates.append({
            "paperless_id": d.id,
            "title": d.title,
            "correspondent_name": None,
            "created_date": cdate,
            "deep_link": (
                f"{base}/documents/{d.id}/" if base else None
            ),
        })
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_txn_receipt_search_results.html",
        {
            "token": token,
            "candidates": candidates,
            "search_query": query,
            "paperless_base_url": base or None,
        },
    )


@router.post("/txn/{token}/receipt-link")
async def receipt_link(
    token: str,
    request: Request,
    paperless_doc_id: int = Form(...),
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Append a ``custom "receipt-link" "<token>"`` directive to
    ``connector_links.bean`` via the existing
    :class:`ReceiptLinker`. The linker keys off
    ``lamella-paperless-id`` and our ``token`` (which it stores
    verbatim under ``txn_hash``), so a staged-row link directive has
    the same shape as a ledger-row link directive — they are joinable
    by identity from either side."""
    staged, ledger = _resolve_token(conn=conn, reader=reader, token=token)
    txn_date = _txn_date_for_link(staged=staged, ledger=ledger)
    txn_amount = _txn_amount_for_link(staged=staged, ledger=ledger)

    linker = ReceiptLinker(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_links=settings.connector_links_path,
    )
    try:
        linker.link(
            paperless_id=paperless_doc_id,
            txn_hash=token,
            txn_date=txn_date,
            txn_amount=txn_amount,
            match_method="user_confirmed",
            match_confidence=1.0,
            paperless_hash=cached_paperless_hash(conn, paperless_doc_id),
            paperless_url=paperless_url_for(
                settings.paperless_url, paperless_doc_id,
            ),
        )
    except BeanCheckError as exc:
        log.error(
            "staged-receipt-link rejected by bean-check: doc=%d txn=%s "
            "err=%s",
            paperless_doc_id, token[:12], exc,
        )
        raise HTTPException(
            status_code=500, detail=f"bean-check failed: {exc}",
        )
    conn.execute(
        "INSERT INTO receipt_link_blocks "
        "(paperless_id, txn_hash, reason) VALUES (?, ?, ?) "
        "ON CONFLICT(paperless_id, txn_hash) DO UPDATE SET "
        "reason = excluded.reason, blocked_at = CURRENT_TIMESTAMP",
        (paperless_doc_id, token, "user_unlink"),
    )
    append_link_block(
        connector_links=settings.connector_links_path,
        main_bean=settings.ledger_main,
        txn_hash=token,
        paperless_id=int(paperless_doc_id),
        reason="user_unlink",
    )
    reader.invalidate()

    # ADR-0044: write the four canonical Lamella_* fields back to
    # Paperless. Only fires for the ledger-row branch (staged rows
    # have no postings yet, so writeback_after_link's lookup just
    # returns {}). Best-effort: failures never break the link.
    try:
        from lamella.features.paperless_bridge.writeback import (
            writeback_after_link,
        )
        await writeback_after_link(
            paperless_id=paperless_doc_id,
            txn_hash=token,
            settings=settings,
            reader=reader,
            conn=conn,
        )
    except Exception:  # noqa: BLE001
        pass

    if request.headers.get("hx-request", "").lower() == "true":
        # Refresh the parent /txn page so the now-linked doc shows up
        # in both the receipts list and any other surface that joins
        # against receipt_links.
        return Response(status_code=204, headers={"HX-Refresh": "true"})
    return Response(status_code=204)


@router.post("/api/paperless/{paperless_id}/unlink-mismatched")
async def paperless_unlink_mismatched(
    paperless_id: int,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Verify-modal entry point. The user clicked "Unlink — mismatch"
    after the receipt-verify AI flagged the linked Paperless document
    as not matching the transaction's hypothesis (wrong vendor / wrong
    document type / hypothesis fields not present in the image).

    Two-phase action — local first, then best-effort Paperless cleanup:

    1. **Local (authoritative).** Look up every ``receipt_links`` row
       for this paperless_id. For each, strip the corresponding
       ``custom "receipt-link"`` block from connector_links.bean and
       delete the SQLite row via :func:`remove_receipt_link`. Bean-
       check vs baseline; on a NEW error, the file is restored and
       this whole call returns 500 — no cleanup attempted.
    2. **Paperless side (best-effort).** PATCH the document to clear
       the four ADR-0044 ``Lamella_*`` custom fields (``Lamella_Entity``,
       ``Lamella_Category``, ``Lamella_TXN``, ``Lamella_Account``)
       so the document doesn't continue advertising the wrong link
       in Paperless searches. PaperlessError is caught and logged —
       the local unlink still stands. Manual cleanup in Paperless
       is documented in the audit log.

    Safety: this endpoint is ONLY user-initiated (clicked from the
    verify modal). There is no auto-unlink based on Paperless 404s,
    sync failures, or transient network issues. A connection issue
    cannot mass-delete receipt links because nothing in this code
    path takes a deletion action without a deliberate POST.
    """
    rows = conn.execute(
        "SELECT txn_hash FROM receipt_links WHERE paperless_id = ?",
        (paperless_id,),
    ).fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"no receipt-link rows for paperless #{paperless_id}",
        )
    removed_count = 0
    failed: list[str] = []
    for row in rows:
        txn_id = row["txn_hash"]
        try:
            removed = remove_receipt_link(
                main_bean=settings.ledger_main,
                connector_links=settings.connector_links_path,
                txn_id=txn_id,
                paperless_id=paperless_id,
                conn=conn,
            )
            if removed:
                removed_count += 1
                conn.execute(
                    "INSERT INTO receipt_link_blocks "
                    "(paperless_id, txn_hash, reason) VALUES (?, ?, ?) "
                    "ON CONFLICT(paperless_id, txn_hash) DO UPDATE SET "
                    "reason = excluded.reason, blocked_at = CURRENT_TIMESTAMP",
                    (paperless_id, str(txn_id), "user_unlink_mismatched"),
                )
                append_link_block(
                    connector_links=settings.connector_links_path,
                    main_bean=settings.ledger_main,
                    txn_hash=str(txn_id),
                    paperless_id=int(paperless_id),
                    reason="user_unlink_mismatched",
                )
        except BeanCheckError as exc:
            log.error(
                "unlink-mismatched: bean-check rejected unlink for "
                "doc=%d txn=%s err=%s",
                paperless_id, str(txn_id)[:12], exc,
            )
            failed.append(str(txn_id)[:12])

    if failed and removed_count == 0:
        raise HTTPException(
            status_code=500,
            detail=f"bean-check failed for: {', '.join(failed)}",
        )

    # Best-effort: clear the four Lamella_* custom fields on the
    # Paperless side. Failure is logged + reported in the response
    # but does NOT undo the local unlink — the link is gone from
    # the ledger regardless.
    paperless_cleared = False
    paperless_error: str | None = None
    try:
        from lamella.adapters.paperless.client import (
            PaperlessClient,
            PaperlessError,
            LAMELLA_WRITEBACK_FIELD_NAMES,
        )
        if settings.paperless_url and settings.paperless_api_token:
            async with PaperlessClient(
                base_url=settings.paperless_url,
                api_token=settings.paperless_api_token.get_secret_value(),
            ) as client:
                cleared_values = {
                    name: "" for name in LAMELLA_WRITEBACK_FIELD_NAMES
                }
                # ensure_fields=True so we don't rely on a populated
                # cache that this short-lived client never had a chance
                # to fill. A 1-call overhead per unlink is fine; the
                # alternative was the cleanup silently no-opping when
                # the cache is empty (which is exactly the bug the user
                # reported: "did NOT clear the lamella specific fields").
                await client.writeback_lamella_fields(
                    paperless_id, values=cleared_values, ensure_fields=True,
                )
                paperless_cleared = True
                log.info(
                    "unlink-mismatched: cleared %d Lamella_* field(s) "
                    "on Paperless doc %d",
                    len(cleared_values), paperless_id,
                )
    except Exception as exc:  # noqa: BLE001 — Paperless cleanup is best-effort
        paperless_error = str(exc)
        log.warning(
            "unlink-mismatched: Paperless field clear failed for "
            "doc=%d: %s",
            paperless_id, exc,
        )

    if request.headers.get("hx-request", "").lower() == "true":
        return Response(
            status_code=204,
            headers={"HX-Refresh": "true"},
        )
    return {
        "paperless_id": paperless_id,
        "links_removed": removed_count,
        "lamella_fields_cleared": paperless_cleared,
        "paperless_error": paperless_error,
    }


@router.post("/txn/{token}/receipt-unlink")
def receipt_unlink(
    token: str,
    request: Request,
    paperless_doc_id: int = Form(...),
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Strip a previously written ``custom "receipt-link"`` block. The
    helper snapshots the file, removes the matching block, and runs
    bean-check vs baseline; on a NEW error it restores the file and
    raises so the caller surfaces the failure."""
    _resolve_token(conn=conn, reader=reader, token=token)
    try:
        removed = remove_receipt_link(
            main_bean=settings.ledger_main,
            connector_links=settings.connector_links_path,
            txn_id=token,
            paperless_id=paperless_doc_id,
            conn=conn,
        )
    except BeanCheckError as exc:
        log.error(
            "staged-receipt-unlink rejected by bean-check: doc=%d "
            "txn=%s err=%s",
            paperless_doc_id, token[:12], exc,
        )
        raise HTTPException(
            status_code=500, detail=f"bean-check failed: {exc}",
        )
    if not removed:
        # Idempotent: nothing to do. Surface a 404 so the UI knows
        # the click didn't change state — the caller can refresh the
        # section to re-sync.
        raise HTTPException(
            status_code=404,
            detail=(
                f"no receipt-link directive found for doc "
                f"#{paperless_doc_id} on txn {token[:12]}…"
            ),
        )
    conn.execute(
        "INSERT INTO receipt_link_blocks "
        "(paperless_id, txn_hash, reason) VALUES (?, ?, ?) "
        "ON CONFLICT(paperless_id, txn_hash) DO UPDATE SET "
        "reason = excluded.reason, blocked_at = CURRENT_TIMESTAMP",
        (paperless_doc_id, token, "user_unlink"),
    )
    append_link_block(
        connector_links=settings.connector_links_path,
        main_bean=settings.ledger_main,
        txn_hash=token,
        paperless_id=int(paperless_doc_id),
        reason="user_unlink",
    )
    reader.invalidate()

    if request.headers.get("hx-request", "").lower() == "true":
        return Response(status_code=204, headers={"HX-Refresh": "true"})
    return Response(status_code=204)
