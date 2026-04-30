# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_ledger_reader, get_rule_service, get_settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.rules.models import PATTERN_TYPES
from lamella.features.rules.rule_writer import append_rule, append_rule_revoke
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)

router = APIRouter()


def _open_accounts(reader: LedgerReader) -> set[str]:
    from beancount.core.data import Open

    return {e.account for e in reader.load().entries if isinstance(e, Open)}


@router.get("/rules", response_class=HTMLResponse)
def list_rules(
    request: Request,
    service: RuleService = Depends(get_rule_service),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    rules = service.list()
    # Mine proposals from ledger history — these are the patterns the
    # AI sees as priors every time it classifies. Surfacing them here
    # makes the "directional-rules" philosophy visible: you can review
    # them, you can promote one to a hard rule (force it to auto-apply
    # at ingest), or you can leave it as a signal and let the AI keep
    # consulting history.
    proposals: list = []
    try:
        from lamella.features.import_.staging import mine_rules
        proposals = mine_rules(reader, min_support=5, min_confidence=0.6)
    except Exception:  # noqa: BLE001
        log.exception("rules page: mine_rules failed")
    # Filter out proposals whose normalized_payee already matches an
    # active rule — they'd double up otherwise.
    active_patterns = {
        (r.pattern_type, (r.pattern_value or "").strip().lower()) for r in rules
    }
    proposals = [
        p for p in proposals
        if ("merchant_contains", p.normalized_payee) not in active_patterns
    ]
    ctx = {
        "rules": rules,
        "pattern_types": sorted(PATTERN_TYPES),
        "mined_proposals": proposals,
    }
    return request.app.state.templates.TemplateResponse(request, "rules.html", ctx)


@router.post("/rules/promote-mined", response_class=HTMLResponse)
def promote_mined(
    request: Request,
    pattern_value: str = Form(...),
    target_account: str = Form(...),
    service: RuleService = Depends(get_rule_service),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Promote a mined proposal into a hard classification rule.

    The proposal's normalized payee becomes a ``merchant_contains``
    pattern; the user is endorsing the target account the miner
    picked. The resulting rule is ``created_by='user'`` and can
    auto-apply at the 0.95 gate threshold — same as a manually-
    created rule. Revocation is via the existing Delete button on
    the rules list.
    """
    pattern_value = (pattern_value or "").strip()
    target_account = (target_account or "").strip()
    if not pattern_value or not target_account:
        raise HTTPException(status_code=400, detail="missing pattern or target")
    valid_accounts = _open_accounts(reader)
    if target_account not in valid_accounts:
        raise HTTPException(
            status_code=400,
            detail=f"target account {target_account!r} is not open in the ledger",
        )
    try:
        service.create(
            pattern_type="merchant_contains",
            pattern_value=pattern_value,
            target_account=target_account,
            confidence=1.0,
            created_by="user",
        )
        append_rule(
            connector_rules=settings.connector_rules_path,
            main_bean=settings.ledger_main,
            pattern_type="merchant_contains",
            pattern_value=pattern_value,
            target_account=target_account,
            created_by="user",
        )
    except BeanCheckError as exc:
        raise HTTPException(status_code=500, detail=f"bean-check: {exc}")
    return RedirectResponse("/rules", status_code=303)


@router.post("/rules", response_class=HTMLResponse)
def create_rule(
    request: Request,
    pattern_type: str = Form(...),
    pattern_value: str = Form(...),
    target_account: str = Form(...),
    card_account: str | None = Form(default=None),
    service: RuleService = Depends(get_rule_service),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    if pattern_type not in PATTERN_TYPES:
        raise HTTPException(status_code=400, detail=f"unknown pattern_type: {pattern_type!r}")

    opened = _open_accounts(reader)
    if target_account not in opened:
        raise HTTPException(
            status_code=400,
            detail=f"target_account {target_account!r} is not opened in the ledger",
        )
    if card_account and card_account not in opened:
        raise HTTPException(
            status_code=400,
            detail=f"card_account {card_account!r} is not opened in the ledger",
        )

    # Dual-write: ledger first (source of truth for reconstruct),
    # then SQLite cache. Bean-check rollback inside append_rule keeps
    # the ledger consistent; if it fails we don't touch the cache.
    settings_obj = None
    try:
        # Inject settings via FastAPI dep instead of constructing here.
        pass
    except Exception:
        pass

    try:
        append_rule(
            connector_rules=_settings_from_request(request).connector_rules_path,
            main_bean=_settings_from_request(request).ledger_main,
            pattern_type=pattern_type,
            pattern_value=pattern_value,
            target_account=target_account,
            card_account=card_account or None,
            created_by="user",
        )
    except BeanCheckError as exc:
        log.error("rule ledger write rejected: %s", exc)
        raise HTTPException(status_code=500, detail=f"bean-check failed: {exc}")
    reader.invalidate()

    rule_id = service.create(
        pattern_type=pattern_type,
        pattern_value=pattern_value,
        target_account=target_account,
        card_account=card_account or None,
        created_by="user",
    )
    rule = service.get(rule_id)
    ctx = {"rule": rule}
    return request.app.state.templates.TemplateResponse(request, "partials/rule_row.html", ctx)


def _settings_from_request(request: Request) -> Settings:
    return request.app.state.settings


def _do_delete_rule(
    rule_id: int,
    request: Request,
    service: RuleService,
    reader: LedgerReader,
) -> tuple[bool, str | None]:
    """Shared delete implementation for the DELETE and POST
    endpoints. Returns (success, error_message)."""
    rule = service.get(rule_id)
    if rule is None:
        return False, "rule not found"
    # Best-effort ledger revoke. If bean-check rejects (often from
    # unrelated pre-existing errors that baseline-subtraction
    # doesn't fully mask), log + continue — the DB delete still
    # removes the rule from active classification. A follow-up
    # revoke can be written later via reconstruct.
    try:
        append_rule_revoke(
            connector_rules=_settings_from_request(request).connector_rules_path,
            main_bean=_settings_from_request(request).ledger_main,
            pattern_type=rule.pattern_type,
            pattern_value=rule.pattern_value,
            target_account=rule.target_account,
            card_account=rule.card_account,
        )
    except BeanCheckError as exc:
        log.warning(
            "rule revoke ledger write failed (rule %d); deleting "
            "from cache anyway: %s", rule_id, exc,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "rule revoke unexpected failure (rule %d): %s",
            rule_id, exc,
        )
    reader.invalidate()
    service.delete(rule_id)
    return True, None


@router.delete("/rules/{rule_id}")
def delete_rule(
    rule_id: int,
    request: Request,
    service: RuleService = Depends(get_rule_service),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """HTMX/API path — HTTP DELETE."""
    ok, err = _do_delete_rule(rule_id, request, service, reader)
    if not ok:
        raise HTTPException(status_code=404, detail=err or "delete failed")
    if "hx-request" in {k.lower() for k in request.headers.keys()}:
        return HTMLResponse("")
    return Response(status_code=204)


@router.post("/rules/{rule_id}/delete")
def delete_rule_post(
    rule_id: int,
    request: Request,
    service: RuleService = Depends(get_rule_service),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Form-POST fallback for HTMX-or-vanilla submission.

    HTMX path returns an empty fragment so the row is removed via the
    form's outerHTML swap (no page-redirect; otherwise the browser
    follows the 303 to /rules and the swap replaces a single <tr>
    with the entire /rules page body — the nested-layout class of
    bug). Vanilla form submits still get the redirect-to-list."""
    ok, err = _do_delete_rule(rule_id, request, service, reader)
    is_htmx = "hx-request" in {k.lower() for k in request.headers.keys()}
    if not ok:
        if is_htmx:
            from html import escape
            return HTMLResponse(
                f'<tr id="rule-row-{rule_id}"><td colspan="9" '
                f'style="color:var(--err); padding:0.5rem 0.75rem">'
                f'Delete failed: {escape(err or "unknown")}</td></tr>',
                status_code=400,
            )
        from urllib.parse import quote
        return RedirectResponse(
            url=f"/rules?delete_failed={quote(err or 'unknown')}",
            status_code=303,
        )
    if is_htmx:
        return HTMLResponse("")
    return RedirectResponse(
        url=f"/rules?deleted={rule_id}", status_code=303,
    )
