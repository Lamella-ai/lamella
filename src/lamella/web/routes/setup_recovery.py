# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""``/setup/recovery`` — Phase 6 bulk-review page + apply pipeline.

The single landing surface for the recovery flow. Aggregates every
detected finding via :func:`detect_all`, overlays the user's
``setup_repair_state`` drafts, and renders one row per finding with
the per-category edit UX. The ``Apply Repairs`` button submits the
batch to the JobRunner for orchestrated execution.

**Implementation status (Phase 6.1.4):**

- ``6.1.4a`` shipped the GET handler (this module's ``recovery_page``).
- ``6.1.4b`` ships the HTMX per-field draft writers (``draft_dismiss``,
  ``draft_edit``) — the bulk-review surface is now composable.
- ``6.1.4c`` ships the Apply POST → JobRunner worker bridge and the
  live finalizing page consuming ``/jobs/{id}/stream``. The worker
  opens its own SQLite connection (per the wizard finalize pattern)
  and bridges :class:`BatchEvent` instances → ``ctx.emit``.

Layout: shares the recovery-isolated shell at
``templates/setup_recovery/_layout.html``. No links to ``/settings/*``
or other main-app surfaces — recovery happens behind the
setup-completeness gate so the user can't navigate away and back
into a half-fixed install.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.beancount_io import LedgerReader
from lamella.features.recovery.findings import detect_all
from lamella.features.recovery.lock import (
    acquire_recovery_lock,
    release_recovery_lock,
)
from lamella.features.recovery.repair_state import (
    read_repair_state,
    write_repair_state,
)
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.web.routes import _htmx

log = logging.getLogger(__name__)

router = APIRouter()


# Recovery progress steps. Phase 6 collapses Schema + Cleanup into a
# single bulk-review surface; the per-category pages remain reachable
# via the per-finding "Apply individually →" affordance for cases
# where the bulk path isn't applicable (recompute migrations, future
# SimpleFIN bind).
_STEP_META = (
    {"id": "recovery", "label": "Recovery", "url": "/setup/recovery"},
)


# Per-category bulk-applicability. The locked spec scopes
# bulk_applicable as a category-level constant; per-finding
# overrides (e.g. recompute migrations within schema_drift) are
# Phase 6.1.4d follow-up. Today every registered category is
# bulk-applicable because none of the registered Migrations are
# recompute-shape (all SUPPORTS_DRY_RUN = True).
#
# The default for unrecognized categories (see ``_bulk_applicable``
# below) is False — opt-in per category, per gap §11.8 of
# RECOVERY_SYSTEM.md. The earlier default (True) optimized for
# detector authors who wanted new categories to surface bulk
# controls automatically; in practice that meant a new category
# registered without an audited heal action could be batched into
# a multi-write run, with the orchestrator's unknown-category
# dispatch only catching the gap at apply time. False-by-default
# keeps the row visible in the bulk-review surface but funnels the
# user into the per-finding "Apply individually →" path until the
# category author explicitly opts into batching by adding it to
# the map below. Every existing entry is kept at True so behavior
# for the seven registered categories is unchanged.
_BULK_APPLICABLE: dict[str, bool] = {
    "schema_drift":      True,
    "legacy_path":       True,
    "unlabeled_account": True,
    "unset_config":      True,
    "orphan_ref":        True,
    "missing_scaffold":  True,
    "missing_data_file": True,
}


def _bulk_applicable(category: str) -> bool:
    """Default False for unrecognized categories — opt-in per gap
    §11.8. A new category that ships before its bulk-applicability
    is audited renders only the per-finding individual-apply link
    rather than silently joining a multi-write batch. The category
    author opts into batching by adding the category to
    ``_BULK_APPLICABLE`` above with an explicit ``True`` once the
    heal action has been validated against the bulk path."""
    return _BULK_APPLICABLE.get(category, False)


def _finding_is_bulk_applicable(f) -> bool:
    """Combine the category-level _BULK_APPLICABLE map with the
    per-finding ``requires_individual_apply`` override (Phase 6.1.4d).

    A finding renders as bulk-applicable only when BOTH:
      - its category is bulk-applicable (or unrecognized — see
        ``_bulk_applicable`` for the asymmetry rationale), AND
      - it doesn't carry the per-finding individual-apply override.

    Per the locked Phase 8 spec, the override check lives in the
    route layer — never in the orchestrator. The orchestrator sees
    only findings the user composed via drafts; the UI hides the
    draft controls for True-flagged findings, so they never enter
    a batch. The HTMX draft writers reject curl-POSTs against
    True-flagged findings to close the stale-browser-state loophole.
    """
    return _bulk_applicable(f.category) and not f.requires_individual_apply


def _individual_url(category: str, finding_id: str) -> str | None:
    """Per-category URL for the "Apply individually →" link.
    Returns None when the category has no individual-action
    surface (rare — schema_drift and legacy_path both have one)."""
    if category == "schema_drift":
        return f"/setup/recovery/schema/confirm?finding_id={finding_id}"
    if category == "legacy_path":
        return "/setup/legacy-paths"
    return None


def _missing_required_steps(request: Request) -> list[dict]:
    """Compute the set of required setup steps that aren't complete yet.

    Drift detectors only cover schema + legacy paths; the dashboard gate
    keys off ``compute_setup_progress`` which also requires entities,
    account labels, charts, vehicles, and properties. When findings is
    empty but ``setup_required_complete`` is False, the recovery page
    used to render "Your install is healthy" — a lie that strands the
    user (Back to dashboard → setup_gate → /setup → /setup/recovery →
    same green page). Returning the missing required steps here lets
    the template surface them with click-through fix URLs."""
    db = getattr(request.app.state, "db", None)
    reader = getattr(request.app.state, "ledger_reader", None)
    if db is None:
        return []
    try:
        from lamella.features.setup.setup_progress import compute_setup_progress

        settings = request.app.state.settings
        entries = list(reader.load().entries) if reader is not None else []
        progress = compute_setup_progress(
            db, entries,
            imports_dir=settings.import_ledger_output_dir_resolved,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("recovery: setup-progress compute failed: %s", exc)
        return []
    return [
        {
            "id": step.id,
            "label": step.label,
            "summary": step.summary,
            "fix_url": step.fix_url,
        }
        for step in progress.required_steps
        if not step.is_complete
    ]


def _render_recovery(
    request: Request,
    *,
    findings: tuple,
    drafts: dict[str, dict],
    stale_draft_ids: tuple[str, ...],
) -> HTMLResponse:
    """Render the bulk-review page. Splits findings by group so
    the template can render group headings without re-deriving
    the partitioning."""
    from lamella.features.recovery.bulk_apply import GROUPS, categorize

    grouped = categorize(findings)
    # Decorate each finding with its rendered draft state + bulk-
    # applicability so the template stays declarative. The
    # decorated record is a dict, not a mutated Finding (Findings
    # are frozen).
    rows_by_group: dict[str, list[dict]] = {g: [] for g in GROUPS}
    for group_name in GROUPS:
        for f in grouped[group_name]:
            draft = drafts.get(f.id) or {}
            action = draft.get("action", "apply")
            edit_payload = draft.get("edit_payload") or {}
            bulk_ok = _finding_is_bulk_applicable(f)
            # If a finding isn't bulk-applicable, force its action
            # to 'individual' regardless of any draft state — the
            # row UI presents only the individual-action link.
            effective_action = action if bulk_ok else "individual"
            rows_by_group[group_name].append({
                "finding": f,
                "action": effective_action,
                "edit_payload": edit_payload,
                "bulk_applicable": bulk_ok,
                "individual_url": _individual_url(f.category, f.id),
            })

    # Apply button state: enabled iff ≥1 row will actually do work
    # (action == 'apply' or 'edit' AND bulk_applicable).
    has_actionable = any(
        r["action"] in ("apply", "edit") and r["bulk_applicable"]
        for rows in rows_by_group.values()
        for r in rows
    )

    # Counts for the page summary line.
    total = len(findings)
    dismissed = sum(
        1
        for rows in rows_by_group.values()
        for r in rows
        if r["action"] == "dismiss"
    )
    individual_only = sum(
        1
        for rows in rows_by_group.values()
        for r in rows
        if not r["bulk_applicable"]
    )

    missing_steps = _missing_required_steps(request)

    return request.app.state.templates.TemplateResponse(
        request,
        "setup_recovery/index.html",
        {
            "step_meta": _STEP_META,
            "current_step": "recovery",
            "step_index": 0,
            "groups": GROUPS,
            "rows_by_group": rows_by_group,
            "stale_draft_ids": stale_draft_ids,
            "has_actionable": has_actionable,
            "total_findings": total,
            "dismissed_count": dismissed,
            "individual_only_count": individual_only,
            "missing_required_steps": missing_steps,
        },
    )


@router.get("/setup/recovery", response_class=HTMLResponse)
def recovery_page(
    request: Request,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Render the bulk-review page.

    Three rendering states the template handles:

    1. **No findings** — celebrate. Empty-state card, no form.
    2. **Findings, no drafts** — every row defaults to action=apply
       with the detector's proposed_fix. User can dismiss/edit
       before clicking Apply Repairs.
    3. **Findings, drafts present** — the user composed a batch
       previously. Drafts overlay the proposed_fixes; dismissed
       rows render struck-through; edited rows show the user's
       canonical override. Apply Repairs replays this composition.

    Stale-draft case: a draft entry whose finding ID is no longer
    in the current detector output (resolved out-of-band, or the
    detector behavior changed). These IDs are passed to the
    template as ``stale_draft_ids`` so the page can render a
    visible advisory ("3 findings in your draft are no longer
    detected and will be skipped"). The orchestrator's closed-
    world drop already handles them at apply time; the UI surface
    just makes the silent drop visible.
    """
    entries = list(reader.load().entries)
    findings = detect_all(conn, entries)

    state = read_repair_state(conn)
    drafts = state.setdefault("findings", {})

    # Seed default-apply drafts for every finding the user is seeing
    # but hasn't explicitly composed yet. Without this seed, the
    # closed-world batch composition in ``run_bulk_apply`` skips
    # findings whose draft entry is missing — and clicking "Apply
    # Repairs" without first toggling per-row buttons silently 303s
    # back here with no work done. Persisting the visible-default
    # makes the page render and the apply flow agree on what's in
    # the batch.
    seeded = False
    for f in findings:
        if f.id not in drafts:
            drafts[f.id] = {"action": "apply", "edit_payload": None}
            seeded = True
    if seeded:
        write_repair_state(conn, state)

    current_ids = {f.id for f in findings}
    stale_draft_ids = tuple(
        sorted(
            draft_id for draft_id in drafts
            if draft_id not in current_ids
        )
    )

    return _render_recovery(
        request,
        findings=findings,
        drafts=drafts,
        stale_draft_ids=stale_draft_ids,
    )


# ---------------------------------------------------------------------------
# HTMX draft writers (Phase 6.1.4b)
# ---------------------------------------------------------------------------


def _find_finding(
    conn,
    reader: LedgerReader,
    finding_id: str,
):
    """Re-detect and locate the requested Finding by id.

    Returns ``None`` if the finding is no longer present (resolved
    out-of-band, or the detector behavior changed). Callers should
    surface this as a 404."""
    entries = list(reader.load().entries)
    findings = detect_all(conn, entries)
    for f in findings:
        if f.id == finding_id:
            return f, findings
    return None, findings


def _render_row(
    request: Request,
    *,
    finding,
    drafts: dict[str, dict],
    edit_error: str | None = None,
    edit_input: str | None = None,
    findings: tuple = (),
) -> HTMLResponse:
    """Render the row partial for one finding plus the OOB Apply
    button reflecting the post-write actionable state.

    The OOB swap means the page-level Apply Repairs button stays in
    sync with per-row dismiss/edit state without a full page reload.
    HTMX picks up the OOB element by its ``hx-swap-oob="true"`` attr
    and routes it to the ``id="recovery-apply-btn"`` slot."""
    draft = drafts.get(finding.id) or {}
    action = draft.get("action", "apply")
    edit_payload = draft.get("edit_payload") or {}
    bulk_ok = _finding_is_bulk_applicable(finding)
    effective_action = action if bulk_ok else "individual"

    row = {
        "finding": finding,
        "action": effective_action,
        "edit_payload": edit_payload,
        "bulk_applicable": bulk_ok,
        "individual_url": _individual_url(finding.category, finding.id),
        "edit_error": edit_error,
        "edit_input": edit_input,
    }

    # Compute has_actionable across ALL findings (not just this one)
    # so the OOB Apply button reflects the page-level state.
    has_actionable = False
    for f in findings:
        d = drafts.get(f.id) or {}
        a = d.get("action", "apply")
        if _finding_is_bulk_applicable(f) and a in ("apply", "edit"):
            has_actionable = True
            break

    return request.app.state.templates.TemplateResponse(
        request,
        "setup_recovery/partials/_finding_row_with_oob.html",
        {
            "row": row,
            "has_actionable": has_actionable,
        },
    )


@router.post(
    "/setup/recovery/draft/{finding_id}/dismiss",
    response_class=HTMLResponse,
)
def draft_dismiss(
    request: Request,
    finding_id: str,
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Toggle a finding's draft action between 'apply' (or 'edit')
    and 'dismiss'. A single endpoint handles both directions —
    dismiss → apply restores to the default-or-edited state; apply
    or edit → dismiss marks the row as skipped.

    Returns the re-rendered row partial + an OOB Apply button so
    the page-level enabled state stays in sync without a reload.

    404 if the finding ID is no longer detected (e.g. the user
    composed against a stale state). The page should reload to
    pick up the current detected set; the stale-draft advisory at
    GET time will then surface the discrepancy."""
    found, all_findings = _find_finding(conn, reader, finding_id)
    if found is None:
        raise HTTPException(status_code=404, detail="Finding not detected")

    if not _finding_is_bulk_applicable(found):
        # Phase 6.1.4d: per-finding individual-apply override. The UI
        # never renders dismiss/edit controls for these rows; a curl-
        # POST against the endpoint is a stale browser state or an
        # adversarial caller. Refuse with 400 so we never compose a
        # draft the orchestrator wouldn't honor.
        raise HTTPException(
            status_code=400,
            detail=(
                f"finding {finding_id!r} requires individual apply "
                "and cannot be drafted into the bulk batch"
            ),
        )

    state = read_repair_state(conn)
    drafts = state.setdefault("findings", {})
    entry = drafts.setdefault(
        finding_id, {"action": "apply", "edit_payload": None},
    )
    if entry.get("action") == "dismiss":
        # Restore to whichever non-dismiss state the row was in.
        # If an edit_payload exists, treat the row as edited;
        # otherwise apply.
        entry["action"] = "edit" if entry.get("edit_payload") else "apply"
    else:
        entry["action"] = "dismiss"
    write_repair_state(conn, state)
    conn.commit()

    return _render_row(
        request,
        finding=found,
        drafts=drafts,
        findings=all_findings,
    )


@router.post(
    "/setup/recovery/draft/{finding_id}/edit",
    response_class=HTMLResponse,
)
def draft_edit(
    request: Request,
    finding_id: str,
    canonical: str = Form(...),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Set or update the canonical destination for a legacy_path
    finding. Validates against the same ``_passes_destination_guards``
    the detector and heal action use — single source of truth.

    On success: writes ``action='edit'`` + ``edit_payload={canonical}``
    and returns the re-rendered row + OOB Apply button.

    On validation failure: returns the row partial with an inline
    error message and the user's input preserved (matching the
    Phase 4 field-error pattern). Repair state is NOT modified.

    400 if the finding category doesn't support edits (today only
    legacy_path does). 404 if the finding ID is no longer detected.
    """
    found, all_findings = _find_finding(conn, reader, finding_id)
    if found is None:
        raise HTTPException(status_code=404, detail="Finding not detected")

    if not _finding_is_bulk_applicable(found):
        raise HTTPException(
            status_code=400,
            detail=(
                f"finding {finding_id!r} requires individual apply "
                "and cannot be drafted into the bulk batch"
            ),
        )

    if found.category != "legacy_path":
        raise HTTPException(
            status_code=400,
            detail=f"category {found.category!r} does not support edit",
        )

    canonical = canonical.strip()

    # Read-modify-write — preserves any other draft entries that may
    # exist for sibling findings. Replace-not-merge semantics from
    # 6.1.2 mean concurrent writes race at the blob level (last
    # write wins on the whole blob), which is acceptable for
    # single-user single-session recovery.
    state = read_repair_state(conn)
    drafts = state.setdefault("findings", {})

    # Validate using the single-source guard the detector and heal
    # action both consume. Build the opened-account set the same way
    # the heal action does.
    from beancount.core.data import Open as _Open
    from lamella.features.recovery.findings.legacy_paths import (
        _passes_destination_guards,
    )

    entries = list(reader.load().entries)
    opened: set[str] = {
        e.account for e in entries if isinstance(e, _Open)
    }
    if not canonical:
        return _render_row(
            request,
            finding=found,
            drafts=drafts,
            edit_error="Canonical destination is required.",
            edit_input=canonical,
            findings=all_findings,
        )
    if not _passes_destination_guards(canonical, opened):
        return _render_row(
            request,
            finding=found,
            drafts=drafts,
            edit_error=(
                f"Destination {canonical!r} doesn't pass the move-target "
                "guards — its parent path must already be opened in the "
                "ledger. Open the parent first, or pick a different path."
            ),
            edit_input=canonical,
            findings=all_findings,
        )

    drafts[finding_id] = {
        "action": "edit",
        "edit_payload": {"canonical": canonical},
    }
    write_repair_state(conn, state)
    conn.commit()

    return _render_row(
        request,
        finding=found,
        drafts=drafts,
        findings=all_findings,
    )


# ---------------------------------------------------------------------------
# Apply pipeline (Phase 6.1.4c)
# ---------------------------------------------------------------------------


def _bean_check_runner(path):
    """bean-check callable threaded into heal actions through the
    orchestrator. Same filter as ``setup_schema._bean_check_runner``
    and ``setup_legacy_paths._bean_check_runner`` — drops the
    ``Auto-inserted`` advisory from the auto_accounts plugin and the
    pseudo-source synthetic errors so the heal envelope only fails
    on *real* new errors introduced by the write."""
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


def _recovery_apply_worker(*, ctx, settings: Settings) -> dict:
    """JobRunner worker bridging :func:`run_bulk_apply` →
    :class:`JobContext`.

    Opens its own SQLite connection + LedgerReader rather than
    borrowing the request-thread's: same reasoning as the wizard
    finalize worker (commit ``19a82a5``) — keeps the long-running
    pipeline off the main app conn's RLock and survives request-
    scope teardown closing the request conn before the worker
    finishes.

    Each yielded :class:`BatchEvent` is translated through
    ``event.to_emit()`` into a ``ctx.emit(message, outcome, detail)``
    call. The detail blob carries the discriminator ``event`` field
    so the finalizing page's JS can dispatch by event type without
    parsing the human-readable message.

    On terminal completion, writes one ``applied_history`` entry per
    group to ``setup_repair_state`` and clears the ``findings`` blob
    if the batch outcome was ``success`` (so the next page visit
    starts fresh). Partial / failed batches preserve the drafts so
    Resume from /setup/recovery replays the same composition.
    """
    from lamella.core.beancount_io import LedgerReader
    from lamella.features.recovery.bulk_apply import (
        BatchDone,
        BatchStarted,
        FindingApplied,
        FindingFailed,
        GroupCommitted,
        GroupRolledBack,
        GroupStarted,
        run_bulk_apply,
    )
    from lamella.core.db import connect as _db_connect

    conn = _db_connect(settings.db_path)
    try:
        reader = LedgerReader(settings.ledger_main)
        repair_state = read_repair_state(conn)

        # Per-group accumulators for applied_history. GroupCommitted's
        # event payload only carries counts (applied/failed) per the
        # locked SSE schema — the per-finding ids needed for
        # applied_history come from the FindingApplied/FindingFailed
        # stream and are flushed when GroupCommitted closes the group.
        current_group: str | None = None
        current_applied_ids: list[str] = []
        current_failed_ids: list[str] = []
        applied_history_new: list[dict[str, Any]] = []
        outcome_value = "success"

        def _now_iso() -> str:
            return datetime.now(timezone.utc).isoformat(timespec="seconds")

        # Pre-flight failures (Phase 8 step 8) emit FindingFailed
        # events between BatchStarted and BatchDone with no enclosing
        # GroupStarted. Tracked separately so we can write a synthetic
        # "preflight" applied_history entry on BatchDone.
        preflight_failed_ids: list[str] = []
        preflight_failure_messages: list[str] = []

        for event in run_bulk_apply(
            conn=conn,
            settings=settings,
            reader=reader,
            repair_state=repair_state,
            bean_check=_bean_check_runner,
        ):
            message, outcome, detail = event.to_emit()
            ctx.emit(message, outcome=outcome, detail=detail)

            if isinstance(event, BatchStarted):
                ctx.set_total(max(1, event.total_findings))
            elif isinstance(event, GroupStarted):
                current_group = event.group
                current_applied_ids = []
                current_failed_ids = []
            elif isinstance(event, FindingApplied):
                current_applied_ids.append(event.finding_id)
                ctx.advance()
            elif isinstance(event, FindingFailed):
                if current_group is None:
                    # FindingFailed outside any group → pre-flight
                    # validation failure. Don't conflate with the
                    # group-scoped failure accumulator.
                    preflight_failed_ids.append(event.finding_id)
                    preflight_failure_messages.append(event.message)
                else:
                    current_failed_ids.append(event.finding_id)
                ctx.advance()
            elif isinstance(event, GroupCommitted):
                applied_history_new.append({
                    "group": event.group,
                    "committed_at": _now_iso(),
                    "applied_finding_ids": list(current_applied_ids),
                    "failed_finding_ids": list(current_failed_ids),
                    "rolled_back": False,
                })
                current_group = None
            elif isinstance(event, GroupRolledBack):
                # applied_finding_ids stays empty — every preceding
                # FindingApplied was buffered + dropped by the
                # atomic-group runner (the writes were rolled back).
                # failed_finding_ids carries the trigger finding's id
                # if a FindingFailed event preceded the rollback;
                # detection-failure rollbacks have no per-finding
                # failure event, so the list may be empty there.
                applied_history_new.append({
                    "group": event.group,
                    "committed_at": _now_iso(),
                    "applied_finding_ids": [],
                    "failed_finding_ids": list(current_failed_ids),
                    "rolled_back": True,
                    "reason": event.reason,
                })
                current_group = None
            elif isinstance(event, BatchDone):
                outcome_value = event.outcome

        # Pre-flight failure path: synthesize a single applied_history
        # entry with group="preflight" so the user can see, on the
        # next page visit, that the apply attempt was rejected at
        # validation time before any commit. The drafts blob stays
        # populated (no group reached "success"), so Resume after
        # fixing the stale edits replays the same composition.
        if preflight_failed_ids:
            applied_history_new.append({
                "group": "preflight",
                "committed_at": _now_iso(),
                "applied_finding_ids": [],
                "failed_finding_ids": list(preflight_failed_ids),
                "rolled_back": True,
                "reason": (
                    f"pre-flight edit_payload validation rejected "
                    f"{len(preflight_failed_ids)} finding(s) before any "
                    "group ran"
                ),
            })

        # Persist applied_history + (on success) clear drafts. Re-read
        # so we don't clobber concurrent draft edits the user might
        # have made in another tab during the run.
        latest = read_repair_state(conn)
        history = list(latest.get("applied_history", []))
        history.extend(applied_history_new)
        latest["applied_history"] = history
        if outcome_value == "success":
            latest["findings"] = {}
        write_repair_state(conn, latest)
        conn.commit()

        return {
            "redirect_url": "/setup/recovery",
            "outcome": outcome_value,
        }
    finally:
        # Always release the in-flight lock (gap §11.7), even on
        # crash/failure. Acquired by the route layer at submit time;
        # released here so a sibling tab can fire the next batch
        # immediately on completion. A worker that crashes before
        # this branch executes leaves the row stranded — recovery is
        # a manual ``DELETE FROM setup_recovery_lock`` which the
        # ``lock`` module's docstring documents.
        try:
            release_recovery_lock(conn)
        except Exception:  # noqa: BLE001
            pass
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


@router.post("/setup/recovery/apply")
def recovery_apply(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
):
    """Submit ``run_bulk_apply`` as a JobRunner job and redirect to
    the finalizing page.

    The actual ledger writes (re-detect → group iteration → per-
    finding heal) run in the JobRunner so the user lands on the
    progress page within milliseconds and watches real-time status.

    Refuses (303 back to /setup/recovery) when no actionable findings
    remain — the page-level Apply Repairs button is supposed to be
    disabled in that case, but a stale tab or a direct curl could
    still POST. Bouncing back lets the user see the current state
    rather than start an empty job.

    Double-submit guards (defense in depth):

    * Process-local: a ``recovery-apply`` already in
      ``runner.active()`` redirects to *that* job's finalizing page
      instead of submitting a second worker. Catches the steady-
      state two-tabs-same-process case.

    * Durable (gap §11.7): a single-row latch in
      ``setup_recovery_lock`` survives across the brief window
      between request arrival and ``runner.submit``, across server
      restarts, and across future entry points (CLI, scheduled
      scan). Acquired here, released by the worker's ``finally``
      branch. When the lock is held by *something other than* a
      live JobRunner job (stranded after crash, or held by a
      sibling process), we render a friendly HTMX-aware error
      identifying the holder + acquisition time rather than
      stomping on the in-flight write.
    """
    runner = request.app.state.job_runner
    for job in runner.active():
        if job.kind == "recovery-apply":
            return RedirectResponse(
                f"/setup/recovery/finalizing?job={job.id}",
                status_code=303,
            )

    state = read_repair_state(conn)
    drafts = state.get("findings", {})
    has_actionable = any(
        d.get("action") in ("apply", "edit")
        for d in drafts.values()
    )
    if not has_actionable:
        return RedirectResponse("/setup/recovery", status_code=303)

    # Acquire the durable in-flight lock BEFORE submitting the job.
    # Use a placeholder holder string; we'll re-stamp it with the
    # real job id immediately after submit so the row points at a
    # traceable JobRunner entry. The window between acquire and
    # re-stamp is microseconds and stays inside the same request
    # thread, so a race here would require two threads in the same
    # process hitting this route simultaneously, which the
    # ``runner.active()`` check above already serializes.
    held = acquire_recovery_lock(conn, holder="pending")
    if held is not None:
        # Lock is held by something the in-process runner doesn't
        # know about — stranded after a crash, or held by a sibling
        # process. Surface the holder + acquired_at so the user can
        # decide whether to wait or manually clear the row.
        return _render_lock_held_error(request, held=held)

    try:
        settings_snapshot = settings

        def _work(ctx):
            return _recovery_apply_worker(
                ctx=ctx, settings=settings_snapshot,
            )

        job_id = runner.submit(
            kind="recovery-apply",
            title="Applying repairs",
            fn=_work,
            return_url="/setup/recovery",
        )
    except Exception:
        # Submit failed before the worker started — release the lock
        # so a retry isn't blocked by our own stranded row.
        try:
            release_recovery_lock(conn)
        except Exception:  # noqa: BLE001
            pass
        raise

    # Re-stamp the lock holder with the real job id so the next
    # caller's ``current_lock_state`` returns a traceable identifier.
    # Best-effort: an UPDATE failure here doesn't compromise the
    # lock semantics (the worker's finally still releases), it just
    # means the conflict-error UI shows "pending" instead of the
    # job id.
    try:
        conn.execute(
            "UPDATE setup_recovery_lock SET holder = ? "
            "WHERE session_id = ?",
            (f"job:{job_id}", "current"),
        )
        conn.commit()
    except Exception:  # noqa: BLE001
        pass

    return RedirectResponse(
        f"/setup/recovery/finalizing?job={job_id}", status_code=303,
    )


def _render_lock_held_error(
    request: Request,
    *,
    held,
) -> HTMLResponse:
    """Render the friendly "recovery already in progress in another
    tab" error. HTMX-aware per CLAUDE.md "HTMX endpoints return
    partials": for an HX-Request the response is a small inline
    fragment swappable into the form's slot; for a vanilla form
    submit it's a 409 page that links back to /setup/recovery.

    The 409 status is appropriate (RFC 9110: "the request could
    not be completed due to a conflict with the current state of
    the target resource"). The HTMX shim treats non-2xx as a swap
    target like any other response, so the inline error renders
    in place rather than triggering a redirect."""
    msg = (
        f"Recovery is already in progress (holder={held.holder}, "
        f"acquired at {held.acquired_at} UTC). Please wait for the "
        "running batch to finish, or close other tabs running "
        "/setup/recovery before retrying."
    )
    if _htmx.is_htmx(request):
        # Inline error fragment — preserves the form's slot id so
        # subsequent retries from the same tab swap into the same
        # place.
        html = (
            '<div class="recovery-lock-conflict" '
            'role="alert" id="recovery-apply-error">'
            f'<p>{msg}</p>'
            '</div>'
        )
        return _htmx.error_fragment(html, status_code=409)

    # Vanilla submit — render a small standalone page.
    html = (
        '<!doctype html><html><body>'
        f'<h1>Recovery already in progress</h1><p>{msg}</p>'
        '<p><a href="/setup/recovery">Back to recovery</a></p>'
        '</body></html>'
    )
    return HTMLResponse(html, status_code=409)


@router.get("/setup/recovery/finalizing", response_class=HTMLResponse)
def recovery_finalizing(
    request: Request,
    job: str = "",
):
    """Live progress page for the recovery apply pipeline.

    Subscribes to ``/jobs/{job_id}/stream`` and walks the locked
    event vocabulary (``batch_started``, ``group_started``,
    ``finding_applied``, ``finding_failed``, ``group_committed``,
    ``group_rolled_back``, ``batch_done``) to drive the step list +
    counters + progress bar in lock-step with the worker.

    A direct hit (no job id) renders the page with the steps marked
    done — preserves the "see what was set up" view if a user
    bookmarks the URL after a completed run, mirroring the wizard
    finalizing pattern.
    """
    return request.app.state.templates.TemplateResponse(
        request,
        "setup_recovery/finalizing.html",
        {
            "step_meta": _STEP_META,
            "current_step": "recovery",
            "step_index": 0,
            "job_id": job or "",
        },
    )
