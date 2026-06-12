# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0061 Phase 5 — legacy /receipts/* and /txn/{token}/receipt-* paths
permanently redirect to their /documents/* and /txn/{token}/document-*
equivalents.

308 Permanent Redirect (RFC 7538) is used instead of 301 because 308
preserves the request method and body. HTMX POSTs and form submits
against the legacy paths therefore continue to work without the browser
silently downgrading them to GET (which 301/302 may do).

These redirects are mounted indefinitely — there is no removal date.
External bookmarks, integrations, and search-engine indexes that point
at the legacy paths must keep working. The cost is one route table
entry per legacy path.
"""
from __future__ import annotations

from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

router = APIRouter()


def _with_query(path: str, request: Request) -> str:
    """Append the original querystring (if any) to the new path. The
    Starlette Request object exposes the raw querystring as ``request.url.query``;
    if it's empty we return the path bare so we don't emit a trailing ``?``."""
    qs = request.url.query
    if qs:
        return f"{path}?{qs}"
    return path


# ---------------------------------------------------------------------------
# /receipts → /documents
# ---------------------------------------------------------------------------

@router.get("/receipts", include_in_schema=False)
def _r_receipts_list(request: Request):
    # ADR-0061 Phase 5 follow-up: /receipts is a category bookmark
    # (the user wanted "receipts," not the unfiltered documents
    # listing), so we land on the receipt-filtered sub-route. The
    # generic /documents listing is reachable from there via the
    # nav.
    return RedirectResponse(
        _with_query("/documents/receipts", request), status_code=308,
    )


@router.get("/receipts/needed", include_in_schema=False)
def _r_receipts_needed(request: Request):
    return RedirectResponse(
        _with_query("/documents/receipts/needed", request),
        status_code=308,
    )


@router.get("/receipts/needed/partial", include_in_schema=False)
def _r_receipts_needed_partial(request: Request):
    return RedirectResponse(
        _with_query("/documents/needed/partial", request), status_code=308,
    )


@router.get("/receipts/dangling", include_in_schema=False)
def _r_receipts_dangling(request: Request):
    return RedirectResponse(
        _with_query("/documents/receipts/dangling", request),
        status_code=308,
    )


# POST /receipts/dangling/sweep — preserve method + body via 308.
@router.post("/receipts/dangling/sweep", include_in_schema=False)
def _r_receipts_dangling_sweep(request: Request):
    return RedirectResponse("/documents/dangling/sweep", status_code=308)


# POST /receipts/verify-selected — bulk verify form post.
@router.post("/receipts/verify-selected", include_in_schema=False)
def _r_receipts_verify_selected(request: Request):
    return RedirectResponse("/documents/verify-selected", status_code=308)


# POST /receipts/{doc_id}/link — manual single-document link form.
@router.post("/receipts/{doc_id}/link", include_in_schema=False)
def _r_receipts_doc_link(doc_id: int, request: Request):
    return RedirectResponse(f"/documents/{doc_id}/link", status_code=308)


# ---------------------------------------------------------------------------
# /receipts/needed/{txn_hash}/* → /documents/needed/{txn_hash}/*
# ---------------------------------------------------------------------------

@router.post(
    "/receipts/needed/{txn_hash}/link", include_in_schema=False,
)
def _r_needs_link(txn_hash: str, request: Request):
    return RedirectResponse(
        f"/documents/needed/{txn_hash}/link", status_code=308,
    )


@router.post(
    "/receipts/needed/{txn_hash}/dismiss", include_in_schema=False,
)
def _r_needs_dismiss(txn_hash: str, request: Request):
    return RedirectResponse(
        f"/documents/needed/{txn_hash}/dismiss", status_code=308,
    )


@router.post(
    "/receipts/needed/{txn_hash}/undismiss", include_in_schema=False,
)
def _r_needs_undismiss(txn_hash: str, request: Request):
    return RedirectResponse(
        f"/documents/needed/{txn_hash}/undismiss", status_code=308,
    )


@router.get(
    "/receipts/needed/{txn_hash}/search", include_in_schema=False,
)
def _r_needs_search(txn_hash: str, request: Request):
    return RedirectResponse(
        _with_query(
            f"/documents/needed/{txn_hash}/search", request,
        ),
        status_code=308,
    )


@router.post(
    "/receipts/needed/bulk/dismiss", include_in_schema=False,
)
def _r_needs_bulk_dismiss(request: Request):
    return RedirectResponse(
        "/documents/needed/bulk/dismiss", status_code=308,
    )


# ---------------------------------------------------------------------------
# /txn/{token}/receipt-* → /txn/{token}/document-*
# ---------------------------------------------------------------------------

@router.get(
    "/txn/{token}/receipt-section", include_in_schema=False,
)
def _r_txn_receipt_section(token: str, request: Request):
    return RedirectResponse(
        _with_query(f"/txn/{token}/document-section", request),
        status_code=308,
    )


@router.get(
    "/txn/{token}/receipt-search", include_in_schema=False,
)
def _r_txn_receipt_search(token: str, request: Request):
    return RedirectResponse(
        _with_query(f"/txn/{token}/document-search", request),
        status_code=308,
    )


@router.get(
    "/txn/{token}/receipt-link", include_in_schema=False,
)
def _r_txn_receipt_link_get(token: str, request: Request):
    return RedirectResponse(
        _with_query(f"/txn/{token}/document-link", request),
        status_code=308,
    )


@router.post(
    "/txn/{token}/receipt-link", include_in_schema=False,
)
def _r_txn_receipt_link_post(token: str, request: Request):
    return RedirectResponse(
        f"/txn/{token}/document-link", status_code=308,
    )


@router.post(
    "/txn/{token}/receipt-unlink", include_in_schema=False,
)
def _r_txn_receipt_unlink(token: str, request: Request):
    return RedirectResponse(
        f"/txn/{token}/document-unlink", status_code=308,
    )


# ---------------------------------------------------------------------------
# /paperless/* user-facing UI → /documents/*
#
# These predate the ADR-0061 documents-abstraction refactor. The
# user-facing surface moved to /documents/*; "Paperless" is now an
# implementation detail of the document store, not a user-facing
# concept. The proxy paths /paperless/thumb/* and /paperless/preview/*
# are NOT moved — those are technical asset URLs referenced by many
# templates as image src, and they reflect the underlying source
# system rather than the user-facing route hierarchy.
# ---------------------------------------------------------------------------

@router.get("/paperless/anomalies", include_in_schema=False)
def _r_paperless_anomalies(request: Request):
    return RedirectResponse(
        _with_query("/documents/anomalies", request), status_code=308,
    )


@router.post(
    "/paperless/anomalies/{paperless_id}/confirm-date",
    include_in_schema=False,
)
def _r_paperless_anomalies_confirm(paperless_id: int, request: Request):
    return RedirectResponse(
        f"/documents/anomalies/{paperless_id}/confirm-date",
        status_code=308,
    )


@router.post(
    "/paperless/anomalies/{paperless_id}/re-extract",
    include_in_schema=False,
)
def _r_paperless_anomalies_reextract(paperless_id: int, request: Request):
    return RedirectResponse(
        f"/documents/anomalies/{paperless_id}/re-extract",
        status_code=308,
    )


@router.get("/paperless/workflows", include_in_schema=False)
def _r_paperless_workflows_list(request: Request):
    return RedirectResponse(
        _with_query("/documents/workflows", request), status_code=308,
    )


@router.post(
    "/paperless/workflows/{rule_name}/run",
    include_in_schema=False,
)
def _r_paperless_workflows_run(rule_name: str, request: Request):
    return RedirectResponse(
        f"/documents/workflows/{rule_name}/run", status_code=308,
    )


@router.get("/paperless/writebacks", include_in_schema=False)
def _r_paperless_writebacks(request: Request):
    return RedirectResponse(
        _with_query("/documents/writebacks", request), status_code=308,
    )


@router.post(
    "/paperless/{doc_id}/verify", include_in_schema=False,
)
def _r_paperless_doc_verify(doc_id: int, request: Request):
    return RedirectResponse(
        f"/documents/{doc_id}/verify", status_code=308,
    )


@router.post(
    "/paperless/{doc_id}/verify/sync", include_in_schema=False,
)
def _r_paperless_doc_verify_sync(doc_id: int, request: Request):
    return RedirectResponse(
        f"/documents/{doc_id}/verify/sync", status_code=308,
    )


@router.post(
    "/paperless/{doc_id}/enrich", include_in_schema=False,
)
def _r_paperless_doc_enrich(doc_id: int, request: Request):
    return RedirectResponse(
        f"/documents/{doc_id}/enrich", status_code=308,
    )
