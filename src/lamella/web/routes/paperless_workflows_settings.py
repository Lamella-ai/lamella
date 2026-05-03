# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Settings page for ADR-0065 tag-driven workflow bindings.

Allows users to create, toggle, and delete tag→action bindings. Each
binding maps a Paperless tag name to one of the registered actions
(extract_fields, date_sanity_check, link_to_ledger). Bindings are
persisted to the connector_config.bean ledger file as custom directives
so they survive a DB rebuild (ADR-0001, ADR-0015).
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from lamella.core.config import Settings
from lamella.features.paperless_bridge.binding_loader import (
    list_all_bindings,
    list_known_actions,
)
from lamella.features.paperless_bridge.binding_writer import (
    append_binding,
    append_binding_revoke,
)
from lamella.features.paperless_bridge.lamella_namespace import (
    ALL_WORKFLOW_TAGS,
    TAG_AWAITING_EXTRACTION,
)

# Lamella-managed tags the user CANNOT bind to. Only the queue marker
# is truly off-limits — extract_fields already triggers off
# AwaitingExtraction, so a user binding on the same tag would be
# redundant. The "needs follow-up" signals (DateAnomaly, NeedsReview)
# and "done" markers (Extracted, Linked) are legitimate bind targets:
# the on_success ops remove the trigger tag, so loops can't form, and
# chaining is the whole point ("after Lamella detects a date anomaly,
# auto-run the cheap date-only verify").
NON_BINDABLE_LAMELLA_TAGS: tuple[str, ...] = (TAG_AWAITING_EXTRACTION,)
from lamella.adapters.paperless.client import PaperlessClient
from lamella.web.deps import get_db, get_settings
from lamella.web.routes import _htmx

log = logging.getLogger(__name__)

router = APIRouter()


@router.get("/settings/paperless-workflows", response_class=HTMLResponse)
async def page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Renders the bindings settings page."""
    bindings = list_all_bindings(conn)
    actions = list_known_actions()

    paperless_tags: list[str] = []
    if settings.paperless_configured:
        client = PaperlessClient(
            base_url=settings.paperless_url,  # type: ignore[arg-type]
            api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
            extra_headers=settings.paperless_extra_headers(),
        )
        try:
            tag_map = await client.list_tags()
            paperless_tags = sorted(
                name
                for name in tag_map
                if name not in NON_BINDABLE_LAMELLA_TAGS
            )
        except Exception:  # noqa: BLE001
            log.exception("failed to fetch Paperless tags for autocomplete")
        finally:
            await client.aclose()

    context = {
        "bindings": bindings,
        "actions": actions,
        "paperless_tags": paperless_tags,
        "paperless_configured": settings.paperless_configured,
        "state_tags": list(ALL_WORKFLOW_TAGS),
    }
    return _htmx.render(
        request,
        full="settings_paperless_workflows.html",
        partial="partials/_workflows_settings_body.html",
        context=context,
    )


@router.post("/settings/paperless-workflows/create", response_class=HTMLResponse)
def create_binding(
    request: Request,
    tag_name: str = Form(...),
    action_name: str = Form(...),
    enabled: bool = Form(default=True),
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Create a new tag→action binding."""
    # Validate tag_name
    if not tag_name or not tag_name.strip():
        html = (
            f'<div class="form-error" role="alert">'
            f'Tag name is required.'
            f'</div>'
        )
        return _htmx.error_fragment(html)

    tag_name = tag_name.strip()

    # Reject only the truly-internal queue marker. Other Lamella-stamped
    # tags (DateAnomaly, NeedsReview, Extracted, Linked) are legitimate
    # bind targets — chaining a follow-up action onto a Lamella signal
    # is exactly what makes the workflow engine useful.
    if tag_name in NON_BINDABLE_LAMELLA_TAGS:
        html = (
            f'<div class="form-error" role="alert">'
            f'Cannot create a binding on internal queue marker '
            f'<code>{tag_name}</code>. This tag is the trigger for the '
            f'extract_fields action and binding it again would be '
            f'redundant.'
            f'</div>'
        )
        return _htmx.error_fragment(html)

    # Validate action_name
    known_actions = list_known_actions()
    action_names = {a.name for a in known_actions}
    if action_name not in action_names:
        html = (
            f'<div class="form-error" role="alert">'
            f'Unknown action <code>{action_name}</code>. '
            f'Valid actions: {", ".join(action_names)}'
            f'</div>'
        )
        return _htmx.error_fragment(html)

    # Write to the ledger
    try:
        append_binding(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            tag_name=tag_name,
            action_name=action_name,
            enabled=enabled,
            created_at=datetime.now(timezone.utc).replace(tzinfo=None),
            run_check=True,
        )
    except Exception as exc:
        log.exception("failed to append binding directive")
        html = (
            f'<div class="form-error" role="alert">'
            f'Failed to write binding: {exc}'
            f'</div>'
        )
        return _htmx.error_fragment(html)

    # Re-render the bindings list
    bindings = list_all_bindings(conn)
    actions = list_known_actions()
    context = {
        "bindings": bindings,
        "actions": actions,
    }
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_workflows_settings_body.html",
        context,
    )


@router.post("/settings/paperless-workflows/{tag_name}/toggle", response_class=HTMLResponse)
def toggle_binding(
    request: Request,
    tag_name: str,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Toggle the enabled state of a binding."""
    tag_name = tag_name.strip()

    # Find current binding state
    bindings = list_all_bindings(conn)
    binding = None
    for b in bindings:
        if b.tag_name == tag_name:
            binding = b
            break

    if binding is None:
        return _htmx.error_fragment(
            f'<div class="form-error" role="alert">'
            f'Binding not found for tag <code>{tag_name}</code>.'
            f'</div>'
        )

    # Write updated binding with inverted enabled flag
    try:
        append_binding(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            tag_name=tag_name,
            action_name=binding.action_name,
            enabled=not binding.enabled,
            config_json=binding.config_json,
            created_at=datetime.fromisoformat(binding.created_at),
            run_check=True,
        )
    except Exception as exc:
        log.exception("failed to toggle binding")
        return _htmx.error_fragment(
            f'<div class="form-error" role="alert">'
            f'Failed to toggle binding: {exc}'
            f'</div>'
        )

    # Re-render the bindings list
    bindings = list_all_bindings(conn)
    context = {"bindings": bindings, "actions": list_known_actions()}
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_workflows_settings_body.html",
        context,
    )


@router.post("/settings/paperless-workflows/{tag_name}/delete", response_class=HTMLResponse)
def delete_binding(
    request: Request,
    tag_name: str,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Delete (revoke) a binding."""
    tag_name = tag_name.strip()

    # Write revoke directive
    try:
        append_binding_revoke(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            tag_name=tag_name,
            revoked_at=datetime.now(timezone.utc).replace(tzinfo=None),
            run_check=True,
        )
    except Exception as exc:
        log.exception("failed to revoke binding")
        return _htmx.error_fragment(
            f'<div class="form-error" role="alert">'
            f'Failed to delete binding: {exc}'
            f'</div>'
        )

    # Return empty response so HTMX outerHTML swap removes the row
    return _htmx.empty()


@router.post(
    "/settings/paperless-workflows/oneshot/{action_name}/run",
    response_class=HTMLResponse,
)
async def oneshot_run_action(
    request: Request,
    action_name: str,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Run an action one-shot against its suggested trigger tag,
    without persisting a binding. Powers the "Run now" button on
    each card in the action catalog at the bottom of
    /settings/paperless-workflows.

    Submits the run through the JobRunner so the user gets the
    standard progress modal (per-doc emit log, success/failure
    counters, per-row outcome chips) instead of a blocking await
    that ties up the request and gives no live feedback. ADR-0006.

    The on_success TagOps still fire — RemoveTag(trigger), plus
    AddTag(completion_tag) when set — so a successful one-shot run
    mutates Paperless exactly as a saved binding would. The only
    difference is no row in tag_workflow_bindings.
    """
    if not settings.paperless_configured:
        return _htmx.error_fragment(
            '<div class="form-error" role="alert">'
            'Paperless is not configured.'
            '</div>'
        )
    actions = {a.name: a for a in list_known_actions()}
    meta = actions.get(action_name)
    if meta is None:
        return _htmx.error_fragment(
            f'<div class="form-error" role="alert">'
            f'Unknown action <code>{action_name}</code>.'
            f'</div>'
        )
    if not meta.suggested_trigger_tag:
        return _htmx.error_fragment(
            f'<div class="form-error" role="alert">'
            f'<strong>{meta.display_label}</strong> has no suggested '
            f'trigger tag — it runs as a schedule-driven scan, or '
            f'requires a custom binding to know which docs to target.'
            f'</div>'
        )

    from lamella.features.paperless_bridge.tag_workflow import (
        ACTION_COMPLETION_TAGS,
        _build_action,
    )

    action = _build_action(action_name)
    if action is None:
        return _htmx.error_fragment(
            f'<div class="form-error" role="alert">'
            f'Action factory for <code>{action_name}</code> is not '
            f'registered. This is a bug — report it.'
            f'</div>'
        )

    trigger = meta.suggested_trigger_tag
    completion_tag = ACTION_COMPLETION_TAGS.get(action_name)

    def _work(ctx):
        import asyncio as _asyncio
        from lamella.adapters.paperless.client import PaperlessClient as _PC
        from lamella.features.paperless_bridge.tag_workflow import (
            DocumentSelector,
            TagOp,
            WorkflowRule,
            run_rule,
        )

        on_success: tuple[TagOp, ...] = (TagOp("remove", trigger),)
        if completion_tag:
            on_success = on_success + (TagOp("add", completion_tag),)
        rule = WorkflowRule(
            name=f"oneshot:{trigger}:{action_name}",
            description=(
                f"One-shot run from settings UI: action "
                f"{action_name} against docs carrying {trigger}."
            ),
            selector=DocumentSelector(
                must_have_tags=(trigger,),
                must_not_have_tags=(completion_tag,) if completion_tag else (),
            ),
            action=action,
            on_success=on_success,
        )

        worker_conn = sqlite3.connect(
            str(settings.db_path), isolation_level=None,
        )
        worker_conn.row_factory = sqlite3.Row
        client = _PC(
            base_url=settings.paperless_url or "",
            api_token=(
                settings.paperless_api_token.get_secret_value()
                if settings.paperless_api_token else ""
            ),
            extra_headers=settings.paperless_extra_headers(),
        )
        loop = _asyncio.new_event_loop()
        try:
            ctx.emit(
                f"Running {action_name} against docs carrying "
                f"{trigger} …", outcome="info",
            )
            report = loop.run_until_complete(
                run_rule(rule, conn=worker_conn, paperless_client=client),
            )
            ctx.set_total(max(report.docs_matched, 1))
            for r in report.per_doc:
                ops = ", ".join(
                    f"{op.op}:{op.tag_name}" for op in r.tag_ops_applied
                ) or "—"
                ctx.emit(
                    f"#{r.paperless_id} — {r.status}: {r.summary} "
                    f"[{ops}]",
                    outcome=(
                        "success" if r.status == "success"
                        else "failure" if r.status in ("error", "anomaly")
                        else "info"
                    ),
                )
                ctx.advance(1)
            ctx.emit(
                f"Done — matched {report.docs_matched}, ok "
                f"{report.successes}, anomaly {report.anomalies}, "
                f"err {report.errors}, skipped {report.skipped}.",
                outcome="success",
            )
        finally:
            loop.run_until_complete(client.aclose())
            loop.close()
            worker_conn.close()
        return {
            "matched": report.docs_matched,
            "successes": report.successes,
            "anomalies": report.anomalies,
            "errors": report.errors,
            "skipped": report.skipped,
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="paperless-workflow-oneshot",
        title=f"Run {meta.display_label} ({trigger})",
        fn=_work,
        return_url="/settings/paperless-workflows",
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {
            "job_id": job_id,
            "on_close_url": "/settings/paperless-workflows",
        },
    )
