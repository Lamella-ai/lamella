# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``/setup/recovery/schema`` — Phase 5.5 of /setup/recovery.

Surfaces schema-drift findings (SQLite axis + ledger axis) and runs
the corresponding Migration on confirm. Two-step flow:

- ``GET /setup/recovery/schema`` lists detected drift, one card per
  axis. Each card carries an Apply form pointing at the confirmation
  step.
- ``GET /setup/recovery/schema/confirm?finding_id=…`` runs the
  Migration's dry-run, renders a confirmation page with the preview
  detail. This is the route that lives between the user click and the
  actual write — per the locked spec, recompute migrations require it
  ("Confirmation step beats lying about previewability").
- ``POST /setup/recovery/schema/heal`` applies the chosen Migration.
  Re-runs the detector first to catch race conditions where the drift
  was healed out-of-band between page render and click.

Layout: same recovery-isolated shell as /setup/legacy-paths. No
links to /settings/*, no /accounts, no /simplefin.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from urllib.parse import quote as _q, unquote as _unquote

from lamella.core.beancount_io import LedgerReader
from lamella.features.recovery.findings import detect_schema_drift
from lamella.features.recovery.heal import heal_schema_drift
from lamella.features.recovery.heal.legacy_paths import HealRefused
from lamella.features.recovery.migrations import find_for_finding
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings


log = logging.getLogger(__name__)

router = APIRouter()


# Reuse the Cleanup pill — schema drift gets its own pill in the
# step list. Order: Schema (blockers) before Cleanup (warnings) so a
# user with both sees the more-urgent category first.
_STEP_META = (
    {"id": "schema", "label": "Schema", "url": "/setup/recovery/schema"},
    {"id": "cleanup", "label": "Cleanup", "url": "/setup/legacy-paths"},
)


def _bean_check_runner(path):
    """Same filter as the legacy-paths route."""
    from beancount import loader
    _entries, errors, _opts = loader.load_file(str(path))
    out: list[str] = []
    for e in errors:
        msg = getattr(e, "message", str(e))
        if "Auto-inserted" in msg:
            continue
        source = getattr(e, "source", None)
        filename = ""
        if isinstance(source, dict):
            filename = source.get("filename", "") or ""
        if isinstance(filename, str) and filename.startswith("<"):
            continue
        out.append(msg)
    return out


def _render_findings(
    request: Request,
    *,
    findings: tuple,
    last_result: Any | None = None,
) -> HTMLResponse:
    return request.app.state.templates.TemplateResponse(
        request,
        "setup_recovery/schema_drift.html",
        {
            "findings": findings,
            "last_result": last_result,
            "step_meta": _STEP_META,
            "current_step": "schema",
            "step_index": 0,
        },
    )


@router.get("/setup/recovery/schema", response_class=HTMLResponse)
def schema_drift_page(
    request: Request,
    last: str | None = None,
    last_ok: str | None = None,
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """List detected schema-drift findings."""
    entries = list(reader.load().entries)
    findings = detect_schema_drift(conn, entries)

    last_result = None
    if last:
        ok_flag = (last_ok or "").lower() == "1"
        last_result = type("LastResult", (), {
            "success": ok_flag,
            "message": _unquote(last),
        })

    return _render_findings(
        request, findings=findings, last_result=last_result,
    )


@router.get(
    "/setup/recovery/schema/confirm", response_class=HTMLResponse,
)
def schema_drift_confirm(
    request: Request,
    finding_id: str,
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Render the confirmation screen for a single Migration.

    Calls Migration.dry_run() to compute the preview, then shows a
    final Apply button. The drf-run is recomputed at confirm time
    (not stashed from the listing page) so a user who left the tab
    open doesn't apply a stale dry-run summary.
    """
    entries = list(reader.load().entries)
    findings = detect_schema_drift(conn, entries)
    target = next((f for f in findings if f.id == finding_id), None)

    if target is None:
        return RedirectResponse(
            "/setup/recovery/schema"
            f"?last={_q('Schema drift no longer present.')}"
            "&last_ok=1",
            status_code=303,
        )

    migration = find_for_finding(target)
    if migration is None:
        return RedirectResponse(
            "/setup/recovery/schema"
            f"?last={_q('No migration registered for this drift.')}"
            "&last_ok=0",
            status_code=303,
        )

    try:
        preview = migration.dry_run(conn, settings)
    except Exception as exc:  # noqa: BLE001
        log.exception("schema_drift dry_run failed")
        return RedirectResponse(
            "/setup/recovery/schema"
            f"?last={_q(f'Preview failed: {type(exc).__name__}')}"
            "&last_ok=0",
            status_code=303,
        )

    return request.app.state.templates.TemplateResponse(
        request,
        "setup_recovery/schema_drift_confirm.html",
        {
            "finding": target,
            "preview": preview,
            "supports_dry_run": migration.SUPPORTS_DRY_RUN,
            "step_meta": _STEP_META,
            "current_step": "schema",
            "step_index": 0,
        },
    )


@router.post("/setup/recovery/schema/heal")
async def schema_drift_heal(
    request: Request,
    finding_id: str = Form(...),
    conn=Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Apply one Migration. Re-runs the detector to ground the
    action in the current ledger state — same defense-against-races
    pattern the legacy-paths heal uses."""
    entries = list(reader.load().entries)
    findings = detect_schema_drift(conn, entries)
    target = next((f for f in findings if f.id == finding_id), None)

    if target is None:
        return RedirectResponse(
            "/setup/recovery/schema"
            f"?last={_q('Schema drift no longer present.')}"
            "&last_ok=1",
            status_code=303,
        )

    try:
        result = heal_schema_drift(
            target,
            conn=conn, settings=settings, reader=reader,
            bean_check=_bean_check_runner,
        )
    except HealRefused as exc:
        return RedirectResponse(
            "/setup/recovery/schema"
            f"?last={_q(str(exc))}"
            "&last_ok=0",
            status_code=303,
        )

    return RedirectResponse(
        "/setup/recovery/schema"
        f"?last={_q(result.message)}"
        f"&last_ok={'1' if result.success else '0'}",
        status_code=303,
    )
