# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Intercompany settlement report — NEXTGEN.md Phase G6 UI.

``GET /reports/intercompany`` renders the outcome of
``build_intercompany_report`` against the current ledger: which
entities owe which other entities, how much, and which pairs
have been settled.

Read-only. The page hints at the settlement writer (for when
the owing entity actually pays back), but the clearing writer
itself is a separate follow-up — today, the user would record
the settlement transaction manually in
``manual_transactions.bean`` using the template the page
describes.
"""
from __future__ import annotations

from datetime import date as _date

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from lamella.core.beancount_io.reader import LedgerReader
from lamella.web.deps import get_ledger_reader
from lamella.features.reports.intercompany import build_intercompany_report

router = APIRouter()


@router.get("/reports/intercompany", response_class=HTMLResponse)
def intercompany_report(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    entries = reader.load(force=False).entries
    report = build_intercompany_report(entries, as_of=_date.today())
    return request.app.state.templates.TemplateResponse(
        request,
        "intercompany_report.html",
        {
            "report": report,
        },
    )
