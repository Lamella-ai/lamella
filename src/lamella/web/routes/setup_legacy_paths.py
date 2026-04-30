# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``/setup/legacy-paths`` — Phase 3 of /setup/recovery.

Cleanup page for non-canonical chart paths. Detects every legacy
shape via ``detect_legacy_paths`` and offers per-row Close /
Move-and-close actions. Each click runs inside a single
``with_bean_snapshot`` envelope; failures restore the declared file
set and surface an error banner. Per the Phase 3 scope spec, there's
no batch / bulk action — that's Phase 6's job.

Layout: uses the recovery-isolated shell
(``templates/setup_recovery/_layout.html``) which mirrors the
wizard's standalone layout — no app menus, no global JS, no
redirects out to /settings/* mid-flow.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from urllib.parse import quote as _q, unquote as _unquote

from lamella.core.beancount_io import LedgerReader
from lamella.features.recovery.findings import detect_legacy_paths
from lamella.features.recovery.heal import heal_legacy_path
from lamella.features.recovery.heal.legacy_paths import HealRefused
from lamella.features.recovery.models import Finding, fix_payload
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


# Recovery progress steps — Phase 3 ships with one. Phases 4–5 add
# more pills as they introduce new finding categories.
_STEP_META = (
    {"id": "cleanup", "label": "Cleanup", "url": "/setup/legacy-paths"},
)


def _bean_check_runner(path):
    """Same filter shape as setup.py's: strip Auto-inserted noise
    from auto_accounts so a freshly scaffolded ledger doesn't
    look broken."""
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
        "setup_recovery/legacy_paths.html",
        {
            "findings": findings,
            "last_result": last_result,
            "step_meta": _STEP_META,
            "current_step": "cleanup",
            "step_index": 0,
        },
    )


@router.get("/setup/legacy-paths", response_class=HTMLResponse)
def legacy_paths_page(
    request: Request,
    last: str | None = None,
    last_ok: str | None = None,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Render the legacy-paths cleanup page."""
    entries = list(reader.load().entries)
    findings = detect_legacy_paths(conn, entries)

    last_result = None
    if last:
        try:
            decoded = _unquote(last)
            ok_flag = (last_ok or "").lower() == "1"
            last_result = type("LastResult", (), {
                "success": ok_flag,
                "message": decoded,
            })
        except Exception:  # noqa: BLE001
            last_result = None

    return _render_findings(
        request, findings=findings, last_result=last_result,
    )


@router.post("/setup/legacy-paths/heal")
async def legacy_paths_heal(
    request: Request,
    finding_id: str = Form(...),
    action: str = Form(...),
    canonical: str | None = Form(None),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Apply one heal action to one finding.

    Re-runs the detector to find the matching Finding by id, so
    the action's payload is grounded in the current ledger state
    rather than whatever the user clicked at page-render time.
    Re-validation catches the case where someone pushed concurrent
    changes between render and click.
    """
    entries = list(reader.load().entries)
    findings = detect_legacy_paths(conn, entries)

    target = next((f for f in findings if f.id == finding_id), None)
    if target is None:
        # Finding no longer present — heal succeeded out-of-band,
        # or the ledger changed and the path is gone. Either way,
        # nothing to do; redirect with an info banner.
        return RedirectResponse(
            f"/setup/legacy-paths"
            f"?last={_q('Finding no longer present in the ledger.')}"
            f"&last_ok=1",
            status_code=303,
        )

    # The user may have clicked a different alternative than the
    # detector's proposed_fix. Build the effective Finding by
    # swapping in the requested action (still inside the same
    # Finding shape so the heal action validates it the same way).
    if action == target.proposed_fix_dict.get("action") and (
        canonical is None
        or canonical == target.proposed_fix_dict.get("canonical")
    ):
        effective = target
    else:
        # Find the alternative the user clicked.
        match = None
        for alt in target.alternatives_dicts:
            if alt.get("action") != action:
                continue
            if action == "move" and alt.get("canonical") != canonical:
                continue
            match = alt
            break
        if match is None:
            return RedirectResponse(
                f"/setup/legacy-paths"
                f"?last={_q(f'Action {action!r} not offered for this finding.')}"
                f"&last_ok=0",
                status_code=303,
            )
        # Reconstruct the Finding with the chosen action as
        # proposed_fix.
        from dataclasses import replace
        if action == "move":
            new_fix = fix_payload(action="move", canonical=canonical)
        else:
            new_fix = fix_payload(action=action)
        effective = replace(
            target, proposed_fix=new_fix, alternatives=(),
        )

    try:
        result = heal_legacy_path(
            effective,
            conn=conn, settings=settings, reader=reader,
            bean_check=_bean_check_runner,
        )
    except HealRefused as exc:
        return RedirectResponse(
            f"/setup/legacy-paths"
            f"?last={_q(str(exc))}"
            f"&last_ok=0",
            status_code=303,
        )

    return RedirectResponse(
        f"/setup/legacy-paths"
        f"?last={_q(result.message)}"
        f"&last_ok={'1' if result.success else '0'}",
        status_code=303,
    )
