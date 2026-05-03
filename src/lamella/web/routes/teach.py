# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Teach-a-rule: create a rule for a merchant without going through
the review queue. Fastest path when you already know how a vendor
should be categorized.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.web.deps import get_db, get_rule_service
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/teach", response_class=HTMLResponse)
def teach_page(
    request: Request,
    merchant: str = "",
):
    ctx = {"merchant": merchant}
    return request.app.state.templates.TemplateResponse(
        request, "teach.html", ctx
    )


@router.post("/teach")
def teach_rule(
    request: Request,
    merchant: str = Form(...),
    target_account: str = Form(...),
    card_account: str = Form(""),
    entity: str = Form(""),
    rules: RuleService = Depends(get_rule_service),
    conn = Depends(get_db),
):
    merchant = merchant.strip()
    target = target_account.strip()
    card = (card_account or "").strip() or None
    entity_slug = (entity or "").strip() or None
    if not merchant or not target:
        raise HTTPException(status_code=400, detail="merchant and target_account are required")

    # Use learn_from_decision which handles rule creation + dedup.
    rules.learn_from_decision(
        matched_rule_id=None,
        user_target_account=target,
        pattern_type="merchant_contains",
        pattern_value=merchant,
        card_account=card,
        create_if_missing=True,
        source="user_taught",
    )

    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse(
            f'<div class="toast success">Rule created: "{merchant}" → {target}</div>'
        )
    return RedirectResponse(f"/rules?saved=taught-{merchant[:40]}", status_code=303)
