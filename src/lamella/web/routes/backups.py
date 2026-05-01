# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Backup + restore admin routes for the ledger .bean files.

Lists on-disk backups, creates new ones, serves downloads, and performs
restore (always taking a safety snapshot of current state first).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.fs import UnsafePathError, validate_safe_path
from lamella.web.deps import get_ledger_reader, get_settings
from lamella.core.ledger_writer import BeanCheckError, run_bean_check
from lamella.core.registry.backup import (
    create_backup,
    delete_backup,
    list_backups,
    restore_backup,
)

log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/settings/backups", response_class=HTMLResponse)
def backups_page(
    request: Request,
    saved: str | None = None,
    settings: Settings = Depends(get_settings),
):
    backups = list_backups(settings.data_dir)
    ctx = {
        "backups": backups,
        "ledger_dir": str(settings.ledger_dir),
        "saved": saved,
    }
    return request.app.state.templates.TemplateResponse(
        request, "settings_backups.html", ctx
    )


@router.post("/settings/backups/create")
def create_backup_endpoint(
    label: str = Form(""),
    settings: Settings = Depends(get_settings),
):
    info = create_backup(
        ledger_dir=settings.ledger_dir,
        data_dir=settings.data_dir,
        label=(label.strip() or None),
    )
    return RedirectResponse(
        f"/settings/backups?saved=created-{info.filename}", status_code=303
    )


@router.get("/settings/backups/download/{filename}")
def download_backup(
    filename: str,
    settings: Settings = Depends(get_settings),
):
    backups_root = settings.data_dir / "backups" / "ledger"
    # ADR-0030: validate user-supplied filename resolves inside the
    # backups directory before we hand the path to FileResponse. This
    # is a download endpoint (read), but it is reachable directly from
    # the URL; defense-in-depth the path resolution.
    try:
        path = validate_safe_path(filename, allowed_roots=[backups_root])
    except UnsafePathError:
        raise HTTPException(status_code=400, detail="invalid backup name")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="backup not found")
    return FileResponse(
        path=str(path),
        filename=path.name,
        media_type="application/gzip",
    )


@router.post("/settings/backups/delete")
def delete_backup_endpoint(
    filename: str = Form(...),
    settings: Settings = Depends(get_settings),
):
    backups_root = settings.data_dir / "backups" / "ledger"
    # ADR-0030: validate before forwarding to delete_backup so an
    # escape attempt fails the request rather than being absorbed by
    # the function's lower-level guard.
    try:
        validate_safe_path(filename, allowed_roots=[backups_root])
    except UnsafePathError:
        raise HTTPException(status_code=400, detail="invalid backup name")
    ok = delete_backup(settings.data_dir, filename)
    if not ok:
        raise HTTPException(status_code=404, detail="backup not found")
    return RedirectResponse(
        f"/settings/backups?saved=deleted-{filename}", status_code=303
    )


@router.post("/settings/backups/restore")
def restore_backup_endpoint(
    filename: str = Form(...),
    confirm: str = Form(""),
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Restore a backup. Requires `confirm=RESTORE` to proceed so
    accidental clicks don't blow away the current ledger."""
    if confirm != "RESTORE":
        raise HTTPException(
            status_code=400,
            detail="confirmation missing — type RESTORE to proceed",
        )

    backups_root = settings.data_dir / "backups" / "ledger"
    # ADR-0030: validate the user-supplied archive filename before we
    # hand it to the tar extractor.
    try:
        validate_safe_path(filename, allowed_roots=[backups_root])
    except UnsafePathError:
        raise HTTPException(status_code=400, detail="invalid backup name")

    def _check():
        run_bean_check(settings.ledger_main)

    try:
        result = restore_backup(
            ledger_dir=settings.ledger_dir,
            data_dir=settings.data_dir,
            filename=filename,
            bean_check=_check,
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="backup not found")
    except BeanCheckError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"bean-check failed after restore; rolled back. {exc}",
        )
    reader.invalidate()
    return RedirectResponse(
        f"/settings/backups?saved=restored-{filename}-replaced-"
        f"{result['files_replaced']}-added-{result['files_added']}-"
        f"safety-{result['safety_backup']}",
        status_code=303,
    )
