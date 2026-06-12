# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from lamella.web.deps import get_db, get_settings
from lamella.core.config import Settings

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
def readyz(
    request: Request,
    settings: Settings = Depends(get_settings),
    _conn = Depends(get_db),
) -> dict:
    reader = request.app.state.ledger_reader
    try:
        loaded = reader.load()
        ledger_ok = True
        ledger_errors = len(loaded.errors)
    except Exception:
        ledger_ok = False
        ledger_errors = -1
    return {
        "status": "ok" if ledger_ok else "degraded",
        "ledger_ok": ledger_ok,
        "ledger_errors": ledger_errors,
        "paperless_configured": settings.paperless_configured,
    }
