# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP10 generic wizard dispatcher.

Four endpoints, all flow-agnostic:

  GET  /settings/loans/wizard/{flow}                     — entry (initial step)
  POST /settings/loans/wizard/{flow}/{step}              — submit current step
  POST /settings/loans/wizard/{flow}/preview             — render the plan
  POST /settings/loans/wizard/{flow}/commit              — execute the plan

Flow modules in ``loans/wizard/`` register themselves via the
``FLOW_REGISTRY`` map below. Each must implement the
``WizardFlow`` Protocol from ``_base.py``.

State storage: form body. Every step's POST carries every prior
step's fields (as hidden inputs in the rendered template). No
URL params for state, no server-side session, no SQLite scratch.
URL only carries ``?step={name}`` for routing, not state — so
refresh on a deep link starts over at the initial step.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.features.loans.wizard._base import (
    WizardCommitError,
    WizardFlow,
)

log = logging.getLogger(__name__)

router = APIRouter()


# Filled by flow modules at import time. Importing this module
# triggers registration via the explicit import in main.py + each
# flow module's top-level ``register_flow(...)`` call.
FLOW_REGISTRY: dict[str, WizardFlow] = {}


def register_flow(flow: WizardFlow) -> None:
    """Each flow module calls this once at import time. Idempotent —
    re-registering replaces the prior instance, which simplifies
    test isolation."""
    FLOW_REGISTRY[flow.name] = flow


def _flow_or_404(name: str) -> WizardFlow:
    flow = FLOW_REGISTRY.get(name)
    if flow is None:
        raise HTTPException(
            status_code=404, detail=f"unknown wizard flow: {name!r}",
        )
    return flow


# Skip these form keys when copying state forward as hidden inputs.
# `step` is the routing slug (URL); the others are CSRF / submit
# button names that vary per request and shouldn't be round-tripped.
_NON_STATE_FORM_KEYS = frozenset(["step", "_action", "_submit"])


def _strip_routing_keys(params: dict) -> dict:
    return {k: v for k, v in params.items() if k not in _NON_STATE_FORM_KEYS}


# --------------------------------------------------------------------- routes


def _flow_context(flow, step_name: str, params: dict, conn) -> dict:
    """Call flow.template_context if the flow exposes one — otherwise
    return an empty dict. Lets flows enrich step renders with
    registry-derived data (existing property slugs, entity options,
    etc.) without hard-coding the dispatcher."""
    fn = getattr(flow, "template_context", None)
    if fn is None:
        return {}
    try:
        return fn(step_name, params, conn) or {}
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "wizard %s.template_context(%s) raised: %s",
            flow.name, step_name, exc,
        )
        return {}


@router.get(
    "/settings/loans/wizard/{flow_name}", response_class=HTMLResponse,
)
def wizard_entry(
    flow_name: str,
    request: Request,
    conn = Depends(get_db),
):
    """Render the initial step. No state needed — fresh start."""
    flow = _flow_or_404(flow_name)
    step_name = flow.initial_step()
    step = flow.steps()[step_name]
    ctx = {
        "flow": flow,
        "flow_name": flow.name,
        "step": step,
        "step_name": step_name,
        "params": {},
        "errors": [],
    }
    ctx.update(_flow_context(flow, step_name, {}, conn))
    return request.app.state.templates.TemplateResponse(
        request, step.template, ctx,
    )


@router.post(
    "/settings/loans/wizard/{flow_name}/step", response_class=HTMLResponse,
)
async def wizard_step(
    flow_name: str,
    request: Request,
    conn = Depends(get_db),
):
    """Submit the current step. Re-validates ALL accumulated state
    up to this step (idempotent), then either re-renders the same
    step on errors OR advances via flow.next_step() to the next.

    Back-edit behavior: if a prior step's edited value is now
    invalid, this validate() catches it and the dispatcher re-renders
    the offending step with errors. If a prior edit revalidates the
    same value with no error, the user just sees the next step as
    expected. No "previously-completed step now broken" surfaces as
    an error — it surfaces as "you're back at that step."
    """
    flow = _flow_or_404(flow_name)
    form = await request.form()
    params = _strip_routing_keys({k: form.get(k) for k in form.keys()})
    current_step = (form.get("step") or flow.initial_step()).strip()

    # Re-validate the current step. The contract is: validate() may
    # examine ALL params (not just current_step's fields) so a flow
    # whose later step depends on an earlier step's coherence (e.g.,
    # "monthly payment is consistent with principal+APR+term") can
    # surface that as a current-step error.
    errors = flow.validate(current_step, params, conn)
    if errors:
        step = flow.steps()[current_step]
        ctx = {
            "flow": flow,
            "flow_name": flow.name,
            "step": step,
            "step_name": current_step,
            "params": params,
            "errors": errors,
        }
        ctx.update(_flow_context(flow, current_step, params, conn))
        return request.app.state.templates.TemplateResponse(
            request, step.template, ctx,
        )

    next_name = flow.next_step(current_step, params, conn)
    if next_name is None:
        # Flow says we're ready — render preview.
        return _render_preview(request, flow, params, conn)

    next_step_obj = flow.steps()[next_name]
    ctx = {
        "flow": flow,
        "flow_name": flow.name,
        "step": next_step_obj,
        "step_name": next_name,
        "params": params,
        "errors": [],
    }
    ctx.update(_flow_context(flow, next_name, params, conn))
    return request.app.state.templates.TemplateResponse(
        request, next_step_obj.template, ctx,
    )


@router.post(
    "/settings/loans/wizard/{flow_name}/preview", response_class=HTMLResponse,
)
async def wizard_preview(
    flow_name: str,
    request: Request,
    conn = Depends(get_db),
):
    """Materialize the write plan and render it for confirmation.

    Splits cleanly from wizard_step so a user can also navigate
    back-and-forth across the preview without re-running the
    next_step state machine.
    """
    flow = _flow_or_404(flow_name)
    form = await request.form()
    params = _strip_routing_keys({k: form.get(k) for k in form.keys()})
    return _render_preview(request, flow, params, conn)


def _render_preview(
    request: Request, flow: WizardFlow, params: dict, conn,
) -> HTMLResponse:
    # Final round of validation across ALL steps before showing the
    # plan. A user who manipulated hidden inputs to skip past a
    # validation failure shouldn't see a clean preview — surface the
    # error and bounce them back.
    final_errors: list = []
    for step_name in flow.steps():
        final_errors.extend(flow.validate(step_name, params, conn))
    if final_errors:
        # Land them on the initial step with the errors visible. A
        # back-edit that invalidated state should never reach preview.
        first = flow.steps()[flow.initial_step()]
        return request.app.state.templates.TemplateResponse(
            request,
            first.template,
            {
                "flow": flow,
                "flow_name": flow.name,
                "step": first,
                "step_name": flow.initial_step(),
                "params": params,
                "errors": final_errors,
            },
        )

    plan = flow.write_plan(params, conn)
    preview_rows = [p.render_preview() for p in plan]

    return request.app.state.templates.TemplateResponse(
        request,
        "loans_wizard_preview.html",
        {
            "flow": flow,
            "flow_name": flow.name,
            "params": params,
            "preview_rows": preview_rows,
        },
    )


@router.post("/settings/loans/wizard/{flow_name}/commit")
async def wizard_commit(
    flow_name: str,
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Execute the plan under a WizardCommitTxn. Redirects to the
    flow's chosen target on success; on bean-check rejection the
    rollback is already complete by the time WizardCommitError fires."""
    flow = _flow_or_404(flow_name)
    form = await request.form()
    params = _strip_routing_keys({k: form.get(k) for k in form.keys()})

    # Defensive re-validate one last time before committing. A user
    # who reached this endpoint via a stale form submission shouldn't
    # be able to bypass validation by going straight to /commit.
    final_errors: list = []
    for step_name in flow.steps():
        final_errors.extend(flow.validate(step_name, params, conn))
    if final_errors:
        raise HTTPException(
            status_code=400,
            detail=(
                "validation failed: "
                + "; ".join(e.message for e in final_errors)
            ),
        )

    try:
        result = flow.commit(params, settings, conn, reader)
    except WizardCommitError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))

    reader.invalidate()
    return RedirectResponse(
        f"{result.redirect_to}?saved={result.saved_message}",
        status_code=303,
    )
