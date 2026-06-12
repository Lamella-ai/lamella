# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generic path-segment rewrite tool.

Exposes slug_rename at /settings/rewrite so the user can fix
ledger-wide convention issues like `Assets:Property:*` → `Assets:Realty:*`
without editing each .bean file by hand. Always takes an auto-backup
before applying.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.slug_rename import apply_rename, build_preview

log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/settings/rewrite", response_class=HTMLResponse)
def rewrite_page(
    request: Request,
    old: str | None = None,
    new: str | None = None,
    segment_index: int = 1,
):
    preview = None
    if old and new:
        try:
            preview = build_preview(
                ledger_dir=request.app.state.settings.ledger_dir,
                old=old, new=new, segment_index=int(segment_index or 1),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    ctx = {
        "old": old or "",
        "new": new or "",
        "segment_index": int(segment_index or 1),
        "preview": preview,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_rewrite.html", ctx
    )


@router.post("/settings/rewrite", response_class=HTMLResponse)
def rewrite_apply(
    request: Request,
    old: str = Form(...),
    new: str = Form(...),
    segment_index: int = Form(1),
    confirm: str = Form(""),
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Apply a ledger-wide path rewrite — runs as a job so the user
    sees feedback during the multi-file edit + bean-check cycle."""
    if confirm != "yes":
        raise HTTPException(status_code=400, detail="confirmation missing")
    old_val = old.strip()
    new_val = new.strip()
    seg_idx = int(segment_index or 1)

    def _work(ctx):
        ctx.emit(
            f"Rewriting '{old_val}' → '{new_val}' at segment {seg_idx}",
            outcome="info",
        )
        ctx.emit("Running bean-check after rewrite …", outcome="info")
        try:
            preview = apply_rename(
                main_bean=settings.ledger_main,
                ledger_dir=settings.ledger_dir,
                old=old_val, new=new_val,
                segment_index=seg_idx,
                conn=conn,
                data_dir=settings.data_dir,
            )
        except ValueError as exc:
            ctx.emit(f"Rewrite failed: {exc}", outcome="error")
            raise
        except BeanCheckError as exc:
            ctx.emit(f"bean-check blocked: {exc}", outcome="error")
            raise
        reader.invalidate()
        ctx.emit(
            f"Rewrite complete · {preview.file_count} file(s) · "
            f"{preview.line_count} line(s) changed",
            outcome="success",
        )
        return {
            "files": preview.file_count,
            "lines": preview.line_count,
            "old": old_val,
            "new": new_val,
        }

    redirect_url = (
        f"/settings/backups?saved=rewrote-{old_val}-to-{new_val}"
    )
    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="ledger-rewrite",
        title=f"Rewriting {old_val} → {new_val}",
        fn=_work,
        return_url=redirect_url,
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": redirect_url},
    )
