# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""On-demand triggers for ADR-0062 tag-driven workflow rules.

Surfaces a single endpoint:

  POST /documents/workflows/{rule_name}/run
      Run the named rule once and return the RunReport. Used by the
      buttons in /settings/paperless for ``trigger="on_demand"``
      rules. Responds with JSON for programmatic callers and an
      HTML partial for HTMX swaps.
"""
from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.tag_workflow import (
    DEFAULT_RULES,
    RunReport,
    get_rule_by_name,
    run_rule,
)
from lamella.web.deps import get_db, get_paperless

log = logging.getLogger(__name__)

router = APIRouter()


def _is_htmx(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"


def _render_report_html(report: RunReport) -> str:
    """Compact HTML fragment for HTMX swaps. Mirrors the JSON shape
    so the user can see the run outcome inline without a page hop."""
    rows = []
    for r in report.per_doc:
        ops = ", ".join(
            f"{op.op}:{op.tag_name}" for op in r.tag_ops_applied
        ) or "—"
        rows.append(
            f"<tr><td>#{r.paperless_id}</td><td>{r.status}</td>"
            f"<td>{r.summary}</td><td><code>{ops}</code></td></tr>"
        )
    body = (
        "<table class='tbl'>"
        "<thead><tr><th>Doc</th><th>Status</th>"
        "<th>Summary</th><th>Tag ops</th></tr></thead>"
        f"<tbody>{''.join(rows) if rows else '<tr><td colspan=4>no docs matched</td></tr>'}</tbody>"
        "</table>"
    )
    return (
        f"<div class='workflow-report'>"
        f"<p>Rule <code>{report.rule_name}</code> — "
        f"matched {report.docs_matched}, "
        f"ok {report.successes}, "
        f"anomaly {report.anomalies}, "
        f"err {report.errors}, "
        f"skipped {report.skipped}.</p>"
        f"{body}</div>"
    )


@router.post("/documents/workflows/{rule_name}/run")
async def run_workflow_rule(
    rule_name: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    client: PaperlessClient = Depends(get_paperless),
):
    rule = get_rule_by_name(rule_name)
    if rule is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"unknown workflow rule {rule_name!r}; valid names: "
                f"{[r.name for r in DEFAULT_RULES]}"
            ),
        )
    log.info("on-demand workflow rule run: %s", rule.name)
    report = await run_rule(rule, conn=conn, paperless_client=client)
    if _is_htmx(request):
        return HTMLResponse(_render_report_html(report))
    return JSONResponse(report.to_dict())


@router.get("/documents/workflows")
async def list_workflow_rules(request: Request):
    """List the registered rules. JSON for programmatic callers."""
    payload = {
        "rules": [
            {
                "name": r.name,
                "description": r.description,
                "trigger": r.trigger,
                "must_have_tags": list(r.selector.must_have_tags),
                "must_not_have_tags": list(r.selector.must_not_have_tags),
            }
            for r in DEFAULT_RULES
        ],
    }
    return JSONResponse(payload)
