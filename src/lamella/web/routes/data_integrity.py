# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Data Integrity / Reboot scan routes — NEXTGEN.md Phase E.

``GET /settings/data-integrity`` shows a status page with a
button to trigger a reboot scan. ``POST .../scan`` parses the
existing ledger, stages every transaction on the unified surface
with ``source='reboot'``, runs the shared content-fingerprint
duplicate detection across all staged rows, and re-renders with
the results.

Non-destructive by design: this route does not touch any ledger
file. File-side reboot (write cleaned copies to ``.reboot/``,
apply with backups, rollback) lands in Phase E2.
"""
from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.config import Settings
from lamella.web.deps import get_db, get_ledger_reader, get_settings
from lamella.features.import_.staging import (
    RebootApplyError,
    RebootService,
    RebootWriter,
    RetrofitError,
    fixme_heavy_payees,
    latest_integrity_report,
    mine_rules,
    noop_cleaner,
    preflight_report_hash,
    retrofit_fingerprint,
    run_integrity_check,
)

log = logging.getLogger(__name__)

router = APIRouter()


def _data_integrity_flags(request: Request) -> dict:
    """Common context flags every data_integrity.html render needs.
    Centralized so the kill-switch flag isn't accidentally dropped on
    a new route — every TemplateResponse now spreads ``**flags``."""
    settings: Settings = request.app.state.settings
    return {
        "reboot_ingest_enabled": bool(
            getattr(settings, "reboot_ingest_enabled", False)
        ),
    }


def _preflight_context(
    conn: sqlite3.Connection, reader: LedgerReader,
) -> dict:
    """Compute the FIXME-heavy pre-flight report and the
    acknowledgment status. Called on every page render so the user
    sees the current state — and re-computed on every reboot-apply
    attempt so the gate can't be bypassed by stale state."""
    report = fixme_heavy_payees(reader.load().entries)
    current_hash = preflight_report_hash(report)
    ack_row = conn.execute(
        "SELECT value FROM app_settings "
        "WHERE key = 'preflight_fixme_ack_hash'",
    ).fetchone()
    ack_hash = ack_row["value"] if ack_row else None
    ack_fresh = bool(ack_hash) and ack_hash == current_hash
    return {
        "preflight_report": report,
        "preflight_hash": current_hash,
        "preflight_ack_fresh": ack_fresh,
    }


def _recent_reboot_sessions(conn: sqlite3.Connection, *, limit: int = 5):
    """Return the most recent reboot scan sessions seen in staging —
    cheap summary for the status page."""
    rows = conn.execute(
        """
        SELECT session_id, MIN(created_at) AS started_at,
               COUNT(*) AS row_count
          FROM staged_transactions
         WHERE source = 'reboot'
           AND session_id IS NOT NULL
         GROUP BY session_id
         ORDER BY started_at DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


@router.get("/settings/data-integrity", response_class=HTMLResponse)
def data_integrity_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    mined_job: str | None = None,
):
    templates = request.app.state.templates
    # If a mine-rules job just finished, pull its proposals in so
    # the page shows them without having to rerun the pass.
    mined_rules = None
    if mined_job:
        try:
            job = request.app.state.job_runner.get(mined_job)
        except Exception:  # noqa: BLE001
            job = None
        if job and job.result:
            mined_rules = job.result.get("proposals")
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": None,
            "mined_rules": mined_rules,
            **_preflight_context(conn, reader),
        },
    )


@router.post("/settings/data-integrity/acknowledge-preflight",
             response_class=HTMLResponse)
def data_integrity_acknowledge_preflight(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Stamp the current FIXME-heavy payee set's hash into app_settings.
    Apply-reboot unlocks as long as the hash hasn't changed since
    acknowledgment."""
    ctx = _preflight_context(conn, reader)
    conn.execute(
        """
        INSERT INTO app_settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        ("preflight_fixme_ack_hash", ctx["preflight_hash"]),
    )
    conn.commit()
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": None,
            **_preflight_context(conn, reader),
        },
    )


def _reboot_kill_switch_response(request: Request) -> HTMLResponse:
    """Centralized 'reboot ingest is disabled' response. Used by every
    destructive reboot route until the operator force-enables via
    settings. The HTMLResponse renders inline so HTMX-targeted forms
    don't dump a stack trace into the page; vanilla forms get a
    legible block."""
    return HTMLResponse(
        "<div class='card card--err' style='padding:1rem'>"
        "<strong>Reboot ingest is disabled.</strong> "
        "<p style='margin-top:.5rem'>This flow scans every transaction "
        "in your ledger and stages it onto the review queue. It can "
        "surface already-classified transactions as if they need "
        "attention, and a follow-on reboot apply rewrites .bean files "
        "in place — both have caused data integrity problems.</p>"
        "<p>Set <code>LAMELLA_REBOOT_INGEST_ENABLED=1</code> in the "
        "environment (or use the force-enable on "
        "<a href='/settings/data-integrity'>/settings/data-integrity</a>) "
        "after taking a manual backup of your ledger directory.</p>"
        "</div>",
        status_code=503,
    )


@router.post("/settings/data-integrity/scan", response_class=HTMLResponse)
def data_integrity_scan(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Run the reboot scan — dispatched as a background job because
    staging every transaction + fingerprint-dedup on a large ledger
    can take 30s+ and previously blocked the browser with no progress.

    Gated on ``settings.reboot_ingest_enabled``. The flow has surfaced
    already-classified ledger transactions as pending review rows
    (the "everything I had classified got duplicated" failure mode),
    so it ships disabled until the safety contract is rebuilt: skip
    classified entries, preserve all metadata, mandatory pre-scan
    backup, recovery-on-failure.
    """
    if not getattr(settings, "reboot_ingest_enabled", False):
        return _reboot_kill_switch_response(request)
    def _work(ctx):
        state = {"prev_done": 0}

        def _on_progress(kind, done, total):
            ctx.raise_if_cancelled()
            if kind == "total":
                ctx.set_total(int(done))
                ctx.emit(
                    f"Scanning {done} transaction(s) on the ledger …",
                    outcome="info",
                )
                return
            pct = f" ({100 * done // total}%)" if total else ""
            ctx.emit(
                f"Scanned {done} of {total} transactions{pct}",
                outcome="info",
            )
            # Advance by the delta since the last tick so the bar
            # tracks exactly, even if the throttle interval doesn't
            # divide evenly into total_txns.
            delta = int(done) - state["prev_done"]
            if delta > 0:
                ctx.advance(delta)
                state["prev_done"] = int(done)

        ctx.emit("Starting data-integrity scan", outcome="info")
        # Skip already-classified entries from the user-triggered scan
        # surface so the review queue doesn't fill up with rows that
        # don't need attention. Programmatic callers (matcher-feed,
        # the integrity check pass) keep the original include-all
        # behavior via the function default.
        result = RebootService(conn).scan_ledger(
            reader, force_reload=True, progress_callback=_on_progress,
            include_classified=False,
        )
        conn.commit()
        # Final reconciliation: progress callbacks only fire on 100-
        # item boundaries, so tail up to the true total.
        if result.total_txns > state["prev_done"]:
            ctx.advance(result.total_txns - state["prev_done"])
        ctx.emit(
            f"Scan complete · staged={result.staged} · "
            f"skipped={result.skipped} · "
            f"duplicate_groups={len(result.duplicate_groups)} · "
            f"files={len(result.files_covered)}",
            outcome="success",
        )
        return {
            "total": result.total_txns,
            "staged": result.staged,
            "skipped": result.skipped,
            "duplicate_groups": len(result.duplicate_groups),
            "files_covered": len(result.files_covered),
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="data-integrity-scan",
        title="Data integrity scan",
        fn=_work,
        return_url="/settings/data-integrity",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/data-integrity"},
    )


@router.post("/settings/data-integrity/retrofit", response_class=HTMLResponse)
def data_integrity_retrofit(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    fingerprint: str = Form(...),
):
    """Stamp ``lamella-source-ref: <fingerprint>`` onto every reboot-source
    staged row's ledger line whose content matches ``fingerprint``.
    After write, re-runs the reboot scan so the UI shows the updated
    picture.
    """
    if not getattr(settings, "reboot_ingest_enabled", False):
        return _reboot_kill_switch_response(request)
    templates = request.app.state.templates
    retrofit_error: str | None = None
    retrofit_result = None
    # Snapshot the ledger to ``$LAMELLA_DATA_DIR/backups/ledger/`` BEFORE
    # writing. Retrofit rewrites .bean files in place to stamp
    # ``lamella-source-ref`` metadata; the operation advertises itself
    # as "additive only" but it still touches the byte content of the
    # files, and a corrupt rewrite has no rollback path without a
    # backup. Other destructive routes here (apply-reboot,
    # duplicates/remove) create backups; retrofit was missing the same
    # guard. The user's recent data loss came through this gap.
    try:
        from lamella.core.registry.backup import create_backup
        info = create_backup(
            ledger_dir=settings.ledger_main.parent,
            data_dir=settings.data_dir,
            label=f"pre-retrofit-{fingerprint[:12]}",
        )
        log.info("retrofit: pre-write backup at %s", info.path)
    except Exception as exc:  # noqa: BLE001
        # If the backup fails we MUST refuse the write — the whole
        # point of the safety contract is "no destructive op without
        # a recoverable snapshot." Without this, a silent backup
        # failure is exactly how the user lost data the first time.
        log.error("retrofit: pre-write backup failed (%s) — refusing", exc)
        return templates.TemplateResponse(
            request,
            "data_integrity.html",
            {
                "result": None,
                "recent_sessions": _recent_reboot_sessions(conn),
                "error": (
                    f"Pre-retrofit backup failed: {exc}. Refusing the "
                    "write so a destructive op can't run without a "
                    "rollback path. Free disk space / fix backup dir "
                    "permissions and retry."
                ),
                "retrofit_result": None,
                "integrity_report": latest_integrity_report(conn),
                "reboot_plan": None,
                "reboot_apply_result": None,
                **_preflight_context(conn, reader),
            },
            status_code=500,
        )
    try:
        retrofit_result = retrofit_fingerprint(
            conn, fingerprint=fingerprint, main_bean=settings.ledger_main,
        )
        conn.commit()
        reader.invalidate()
    except RetrofitError as exc:
        retrofit_error = str(exc)
        log.warning("retrofit failed: %s", exc)
    except Exception as exc:  # noqa: BLE001
        retrofit_error = f"{type(exc).__name__}: {exc}"
        log.exception("retrofit crashed")

    # Always re-scan so the post-retrofit picture is fresh.
    try:
        result = RebootService(conn).scan_ledger(reader, force_reload=True)
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        return templates.TemplateResponse(
            request,
            "data_integrity.html",
            {
                "result": None,
                "recent_sessions": _recent_reboot_sessions(conn),
                "error": f"rescan after retrofit failed: {exc}",
                "retrofit_result": retrofit_result,
                "integrity_report": latest_integrity_report(conn),
                "reboot_plan": None,
                "reboot_apply_result": None,
            },
            status_code=500,
        )

    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": result,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": retrofit_error,
            "retrofit_result": retrofit_result,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": None,
        },
    )


@router.post("/settings/data-integrity/check", response_class=HTMLResponse)
def data_integrity_health_check(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Phase F health check — runs the integrity scan, persists a
    history row, re-renders with the fresh summary."""
    templates = request.app.state.templates
    try:
        report = run_integrity_check(conn, reader)
    except Exception as exc:  # noqa: BLE001
        log.exception("integrity check failed")
        return templates.TemplateResponse(
            request,
            "data_integrity.html",
            {
                "result": None,
                "recent_sessions": _recent_reboot_sessions(conn),
                "error": f"{type(exc).__name__}: {exc}",
                "retrofit_result": None,
                "integrity_report": latest_integrity_report(conn),
                "reboot_plan": None,
                "reboot_apply_result": None,
            },
            status_code=500,
        )
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": report,
            "reboot_plan": None,
            "reboot_apply_result": None,
        },
    )


@router.post("/settings/data-integrity/prepare-reboot", response_class=HTMLResponse)
def data_integrity_prepare_reboot(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Phase E2b — write proposed cleaned copies of every .bean file
    to ``.reboot/`` using the noop baseline cleaner. Shows per-file
    diffs (empty for a clean ledger; non-empty when future cleaners
    produce changes)."""
    if not getattr(settings, "reboot_ingest_enabled", False):
        return _reboot_kill_switch_response(request)
    templates = request.app.state.templates
    rw = RebootWriter(
        ledger_dir=settings.ledger_dir,
        main_bean=settings.ledger_main,
    )
    plan = rw.prepare(noop_cleaner)
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": plan,
            "reboot_apply_result": None,
        },
    )


@router.post("/settings/data-integrity/apply-reboot", response_class=HTMLResponse)
def data_integrity_apply_reboot(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Apply the prepared reboot plan. Backs up to
    ``.pre-reboot-<timestamp>/`` first; on bean-check failure, every
    file is restored.

    Phase E pre-flight gate: if the FIXME-heavy payee report exists
    but hasn't been acknowledged (or the acknowledged hash no
    longer matches — new FIXME patterns accumulated), the apply
    refuses with a 409 and re-renders the page so the warning is
    visible. The gate can't be bypassed by navigating directly to
    this route because the check recomputes the report on every
    call.
    """
    if not getattr(settings, "reboot_ingest_enabled", False):
        return _reboot_kill_switch_response(request)
    templates = request.app.state.templates
    preflight_ctx = _preflight_context(conn, reader)
    report = preflight_ctx["preflight_report"]
    if report.payees and not preflight_ctx["preflight_ack_fresh"]:
        return templates.TemplateResponse(
            request,
            "data_integrity.html",
            {
                "result": None,
                "recent_sessions": _recent_reboot_sessions(conn),
                "error": (
                    f"{len(report.payees)} FIXME-heavy payee(s) need "
                    "review before reboot can be applied. Scroll to the "
                    "pre-flight report and click \"I've reviewed these\"."
                ),
                "retrofit_result": None,
                "integrity_report": latest_integrity_report(conn),
                "reboot_plan": None,
                "reboot_apply_result": None,
                **preflight_ctx,
            },
            status_code=409,
        )
    rw = RebootWriter(
        ledger_dir=settings.ledger_dir,
        main_bean=settings.ledger_main,
    )
    try:
        apply_result = rw.apply()
        reader.invalidate()
    except RebootApplyError as exc:
        return templates.TemplateResponse(
            request,
            "data_integrity.html",
            {
                "result": None,
                "recent_sessions": _recent_reboot_sessions(conn),
                "error": str(exc),
                "retrofit_result": None,
                "integrity_report": latest_integrity_report(conn),
                "reboot_plan": None,
                "reboot_apply_result": None,
                **_preflight_context(conn, reader),
            },
            status_code=400,
        )
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": apply_result,
            **_preflight_context(conn, reader),
        },
    )


@router.post("/settings/data-integrity/mine-rules", response_class=HTMLResponse)
def data_integrity_mine_rules(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Phase E3 — mine directional rules from ledger history.

    Surfaces the top-ranked (payee → account) patterns found in
    the existing ledger. Runs as a job so the user sees the pass
    happen — rule-mining over a large ledger can take 10–30s.
    """

    def _work(ctx):
        ctx.emit(
            "Mining (payee → account) patterns (min_support=5, "
            "min_confidence=0.60) from ledger history …",
            outcome="info",
        )
        try:
            proposals = mine_rules(reader, min_support=5, min_confidence=0.6)
        except Exception as exc:  # noqa: BLE001
            log.exception("rule mining failed")
            ctx.emit(
                f"Rule mining failed: {type(exc).__name__}: {exc}",
                outcome="error",
            )
            raise
        ctx.emit(
            f"Found {len(proposals)} rule proposal(s). Click View results "
            f"to see them.",
            outcome="success",
        )
        # Serialize dataclass proposals so they survive the JSON
        # round-trip and the page GET can re-render them.
        return {
            "proposals": [
                {
                    "normalized_payee": p.normalized_payee,
                    "proposed_account": p.proposed_account,
                    "support": p.support,
                    "confidence": p.confidence,
                    "alternatives": [list(a) for a in p.alternatives],
                }
                for p in proposals
            ],
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="mine-rules",
        title="Mining directional rules from ledger",
        fn=_work,
    )
    # Include job_id in return_url so /settings/data-integrity can
    # re-render the mined proposals from job.result.
    runner.set_return_url(
        job_id,
        f"/settings/data-integrity?mined_job={job_id}",
    )
    return request.app.state.templates.TemplateResponse(
        request, "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/data-integrity"},
    )


@router.get("/settings/data-integrity/duplicates", response_class=HTMLResponse)
def data_integrity_duplicates_page(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """List every (date, account, amount, narration)-matching group of
    SimpleFIN transactions where multiple distinct simplefin ids map
    to the same underlying event. The user picks which one to keep
    per group; the POST endpoint physically removes the others from
    simplefin_transactions.bean.
    """
    from lamella.features.data_integrity import scan_duplicates
    entries = reader.load().entries
    groups = scan_duplicates(entries, require_simplefin_id=True)
    return request.app.state.templates.TemplateResponse(
        request,
        "data_integrity_duplicates.html",
        {
            "groups": groups,
            "total_duplicates": sum(g.count - 1 for g in groups),
        },
    )


@router.post("/settings/data-integrity/duplicates/remove",
             response_class=HTMLResponse)
async def data_integrity_duplicates_remove(
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Remove every selected duplicate SimpleFIN id from
    simplefin_transactions.bean. Runs three safety layers:

      1. Server-side re-scan for duplicate groups; refuses the
         request if any group would have every member removed
         (so a UI bug can't wipe all 3 of a "3 copies" group).
      2. Pre-change archive: copies the target file(s) to
         ``<ledger>/.backups/<timestamp>-dedupe-sfids/`` so
         recovery is a ``cp`` away.
      3. bean-check; reverts on failure.
    """
    from fastapi.responses import RedirectResponse
    from lamella.features.data_integrity import (
        remove_duplicate_sfids,
        scan_duplicates,
        archive_before_change,
        assert_would_keep_one_per_group,
        WouldEmptyGroupError,
    )
    from lamella.core.ledger_writer import BeanCheckError

    form = await request.form()
    sfids = [
        str(v).strip() for (k, v) in form.multi_items()
        if k == "remove_sfid" and str(v).strip()
    ]
    if not sfids:
        return RedirectResponse(
            "/settings/data-integrity/duplicates?error=no-selection",
            status_code=303,
        )

    # Safety 1: server-side group re-scan + keep-one assertion. The
    # UI sends hidden inputs; we do NOT trust that alone.
    entries = reader.load().entries
    groups = scan_duplicates(entries, require_simplefin_id=True)
    group_map: dict[str, list[str]] = {
        g.key: [e.simplefin_id for e in g.entries if e.simplefin_id]
        for g in groups
    }
    try:
        assert_would_keep_one_per_group(
            groups=group_map, remove_ids=sfids,
        )
    except WouldEmptyGroupError as exc:
        log.warning("dedupe refused: %s", exc)
        return RedirectResponse(
            f"/settings/data-integrity/duplicates?error=would-empty-group"
            f"&detail={exc}",
            status_code=303,
        )

    # Compute alias_targets: for each group, the sfid that WASN'T in
    # the remove list becomes the keeper, and every removed sfid in
    # that group aliases to the keeper. This way next SimpleFIN fetch,
    # when the same event re-appears under yet another fresh id,
    # `build_index` pulls the aliases set and dedup catches it —
    # instead of a fresh duplicate landing in the ledger until the
    # event slides out of the bridge's window.
    remove_set = {s for s in sfids}
    alias_targets: dict[str, str] = {}
    for g in groups:
        members = [e.simplefin_id for e in g.entries if e.simplefin_id]
        kept = [m for m in members if m not in remove_set]
        if len(kept) != 1:
            # Multiple kept (user unchecked both) or zero (caught by
            # would-empty-group above). Skip alias-wiring; physical
            # removal still happens but no keeper gets stamped.
            continue
        keeper = kept[0]
        for m in members:
            if m != keeper and m in remove_set:
                alias_targets[m] = keeper

    # Safety 2: backup every file we might touch.
    simplefin_path = settings.ledger_main.parent / "simplefin_transactions.bean"
    preview_path = (
        settings.ledger_main.parent / "simplefin_transactions.connector_preview.bean"
    )
    archive = archive_before_change(
        ledger_dir=settings.ledger_main.parent,
        operation="dedupe-sfids",
        target_files=[simplefin_path, preview_path],
    )
    log.info("dedupe-sfids: backed up to %s", archive.backups)

    # Safety 3: the existing bean-check + snapshot-restore inside
    # remove_duplicate_sfids.
    try:
        result = remove_duplicate_sfids(
            main_bean=settings.ledger_main,
            simplefin_transactions=simplefin_path,
            simplefin_preview=preview_path if preview_path.exists() else None,
            remove_ids=sfids,
            alias_targets=alias_targets,
        )
    except BeanCheckError as exc:
        log.error("duplicate removal bean-check failed: %s", exc)
        return RedirectResponse(
            f"/settings/data-integrity/duplicates?error=bean-check&detail={exc}",
            status_code=303,
        )
    reader.invalidate()
    return RedirectResponse(
        f"/settings/data-integrity/duplicates"
        f"?removed={result.removed_count}"
        f"&skipped={len(result.skipped_ids)}"
        f"&archive={archive.timestamp}",
        status_code=303,
    )


@router.get("/settings/data-integrity/legacy-fees-paths",
            response_class=HTMLResponse)
def data_integrity_legacy_fees_paths(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """List every ``Expenses:{entity}:Fees:{institution}:{name}``
    account still open in the ledger. These are leftovers from the
    pre-fix account scaffolder — the path doesn't match any Schedule
    C line (27a wants ``:Bank:``, 16 wants ``:Interest:``). The page
    shows them alongside the proposed replacement path so the user
    can see what should move.
    """
    from beancount.core.data import Open, Transaction
    entries = list(reader.load().entries)
    # Collect every open path matching the bad pattern.
    legacy: list[dict] = []
    by_usage: dict[str, int] = {}
    # Count postings per account so the user knows which paths are
    # "orphan" (0 postings — safe to rename) vs "active" (postings
    # already in the ledger — needs override rewrites).
    for e in entries:
        if isinstance(e, Transaction):
            for p in e.postings:
                acct = p.account or ""
                if acct:
                    by_usage[acct] = by_usage.get(acct, 0) + 1

    import re
    # Shape we're detecting: Expenses:<Entity>:Fees:<Inst>:<Name>
    # (4+ segments, third segment is literal "Fees")
    pattern = re.compile(r"^Expenses:([^:]+):Fees:([^:]+)(?::(.+))?$")
    for e in entries:
        if not isinstance(e, Open):
            continue
        m = pattern.match(e.account)
        if not m:
            continue
        entity, inst, leaf = m.group(1), m.group(2), m.group(3)
        proposed = f"Expenses:{entity}:Bank:{inst}:Fees"
        legacy.append({
            "path": e.account,
            "entity": entity,
            "institution": inst,
            "leaf": leaf or "",
            "proposed": proposed,
            "usage_count": by_usage.get(e.account, 0),
            "proposed_exists": any(
                isinstance(x, Open) and x.account == proposed
                for x in entries
            ),
        })
    legacy.sort(key=lambda r: (r["entity"], r["institution"], r["path"]))
    return request.app.state.templates.TemplateResponse(
        request,
        "data_integrity_legacy_fees.html",
        {
            "legacy": legacy,
            "orphan_count": sum(1 for r in legacy if r["usage_count"] == 0),
            "active_count": sum(1 for r in legacy if r["usage_count"] > 0),
        },
    )


@router.post("/settings/data-integrity/classify-fixmes",
             response_class=HTMLResponse)
async def data_integrity_classify_fixmes(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Run the AI classifier over every FIXME transaction that doesn't
    already have an override. High-confidence proposals (≥0.90) are
    auto-applied as overrides; lower-confidence ones get logged to
    ai_decisions for manual review. Runs as a background job with
    live progress.

    Form inputs:
      - limit (optional int) — cap the number of txns the AI sees per
        run. Leave blank / omit for unlimited. Defaults applied in the
        UI form so a first-time user doesn't fire thousands of AI
        calls accidentally.
      - max_consecutive_errors (optional int, default 3) — aborts
        the run when errors streak.
    """
    # Setup-completeness gate. The middleware allows /settings/* but
    # we re-check here so even a direct POST (from a curl, or via
    # back-button into a stale form) can't fire the AI when the
    # chart isn't ready.
    if not getattr(request.app.state, "setup_required_complete", False):
        # HTMX-callable: per ADR-0037 + routes/CLAUDE.md, never return
        # a plain 30x from an HTMX-targeted handler. _htmx.redirect
        # does 204 + HX-Redirect for HTMX, plain 303 for vanilla.
        from lamella.web.routes import _htmx
        return _htmx.redirect(
            request, "/setup/recovery?blocked=ai-classify",
        )
    form = await request.form()
    raw_limit = (form.get("limit") or "").strip()
    try:
        limit_val: int | None = int(raw_limit) if raw_limit else None
    except ValueError:
        limit_val = None
    raw_mce = (form.get("max_consecutive_errors") or "").strip()
    try:
        mce_val = int(raw_mce) if raw_mce else 3
    except ValueError:
        mce_val = 3

    def _work(ctx):
        from lamella.features.ai_cascade.bulk_classify import classify_all_fixmes
        from lamella.features.ai_cascade.service import AIService
        # AIService is lightweight (a facade over OpenRouterClient +
        # DecisionsLog); the classifier never holds open connections,
        # so making one per-job is fine.
        ai_service = AIService(settings=settings, conn=conn)
        return classify_all_fixmes(
            ctx,
            conn=conn,
            reader=reader,
            settings=settings,
            ai_service=ai_service,
            limit=limit_val,
            max_consecutive_errors=mce_val,
        )

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="ai-classify-fixmes",
        title="AI bulk-classify FIXME transactions",
        fn=_work,
        # After the job ends the user wants to see what the AI
        # actually proposed — the suggestion review queue, not the
        # data-integrity page they launched from.
        return_url="/ai/suggestions?from=classify-job",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/ai/suggestions?from=classify-job"},
    )


@router.post("/settings/data-integrity/auto-match-receipts",
             response_class=HTMLResponse)
def data_integrity_auto_match_receipts(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Sweep every unlinked transaction in the last N days for
    high-confidence Paperless receipts. Runs as a background job so
    the user doesn't wait on potentially-thousands of candidate
    lookups. Same matcher the SimpleFIN ingest post-hook uses — this
    just lets the user trigger it independently for a backlog.
    """
    from lamella.features.receipts.auto_match import sweep_recent

    def _work(ctx):
        ctx.emit("Scanning recent transactions…", outcome="info")
        sweep = sweep_recent(
            conn=conn, reader=reader, settings=settings,
            window_days=180,  # wider for manual bulk runs
            emit=lambda msg, outcome="info": ctx.emit(msg, outcome=outcome),
        )
        ctx.emit(
            f"Scanned {sweep.scanned}; matched {sweep.matched}; "
            f"already-linked {sweep.already_linked}; "
            f"no-candidate {sweep.no_candidate}; "
            f"low-confidence {sweep.low_confidence}",
            outcome="success" if sweep.matched else "info",
        )
        return {
            "scanned": sweep.scanned,
            "matched": sweep.matched,
            "already_linked": sweep.already_linked,
            "no_candidate": sweep.no_candidate,
            "low_confidence": sweep.low_confidence,
            "errors": len(sweep.errors),
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="auto-match-receipts",
        title="Receipt auto-match sweep",
        fn=_work,
        return_url="/settings/data-integrity",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/settings/data-integrity"},
    )


@router.get("/settings/data-integrity/stacked-overrides", response_class=HTMLResponse)
def data_integrity_stacked_overrides_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Show groups of override blocks pointing at the same source
    txn — the stacked-duplicate symptom that hit users before the
    OverrideWriter gained ``replace_existing=True`` semantics. Each
    group can be collapsed down to a single block; default strategy
    is "keep the newest".
    """
    from lamella.features.data_integrity.stacked_overrides import scan_stacked
    groups = scan_stacked(settings.connector_overrides_path)
    # Hydrate target txns with narration + date for easier recognition.
    from beancount.core.data import Transaction
    from lamella.core.beancount_io.txn_hash import txn_hash
    entries = reader.load().entries
    by_hash: dict[str, Transaction] = {}
    for e in entries:
        if isinstance(e, Transaction):
            by_hash[txn_hash(e)] = e
    display = []
    for g in groups:
        t = by_hash.get(g.target_hash)
        display.append({
            "target_hash": g.target_hash,
            "count": g.count,
            "excess": g.excess,
            "txn_date": str(t.date) if t else "?",
            "narration": (t.narration if t else "") or "",
            "payee": getattr(t, "payee", None) if t else None,
            "blocks": [
                {
                    "modified_at": b.modified_at or "(unknown)",
                    "preview": b.block_text[:400],
                }
                for b in g.blocks
            ],
        })
    return request.app.state.templates.TemplateResponse(
        request,
        "data_integrity_stacked_overrides.html",
        {
            "groups": display,
            "total_excess": sum(g.excess for g in groups),
        },
    )


@router.post("/settings/data-integrity/stacked-overrides/cleanup",
             response_class=HTMLResponse)
async def data_integrity_stacked_overrides_cleanup(
    request: Request,
    settings: Settings = Depends(get_settings),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Collapse every selected stacked-override group to a single
    block (keeping the newest). Runs bean-check; reverts on failure.
    """
    from fastapi.responses import RedirectResponse
    from lamella.features.data_integrity.stacked_overrides import dedupe_stacked
    from lamella.core.ledger_writer import (
        capture_bean_check, run_bean_check_vs_baseline, BeanCheckError,
    )

    form = await request.form()
    hashes = [
        str(v).strip() for (k, v) in form.multi_items()
        if k == "target_hash" and str(v).strip()
    ]
    from lamella.features.data_integrity import archive_before_change

    overrides_path = settings.connector_overrides_path
    if not overrides_path.exists():
        return RedirectResponse(
            "/settings/data-integrity/stacked-overrides?error=overrides-missing",
            status_code=303,
        )

    # Safety: backup before changing anything. dedupe_stacked is
    # hardcoded to keep exactly one block per group, so the design
    # itself refuses to empty a group — but a backup is free
    # defense-in-depth in case a bug slips through.
    archive = archive_before_change(
        ledger_dir=settings.ledger_main.parent,
        operation="dedupe-stacked-overrides",
        target_files=[overrides_path],
    )
    log.info("dedupe-stacked: backed up to %s", archive.backups)

    # Snapshot for rollback if bean-check rejects.
    backup_main = settings.ledger_main.read_bytes()
    backup_ov = overrides_path.read_bytes()
    _, baseline = capture_bean_check(settings.ledger_main)
    try:
        removed = dedupe_stacked(
            overrides_path,
            target_hashes=hashes or None,
            keep_strategy="newest",
        )
        run_bean_check_vs_baseline(settings.ledger_main, baseline)
    except BeanCheckError as exc:
        settings.ledger_main.write_bytes(backup_main)
        overrides_path.write_bytes(backup_ov)
        log.error("stacked-override cleanup bean-check rejected: %s", exc)
        return RedirectResponse(
            f"/settings/data-integrity/stacked-overrides?error=bean-check&detail={exc}",
            status_code=303,
        )
    except Exception as exc:  # noqa: BLE001
        settings.ledger_main.write_bytes(backup_main)
        overrides_path.write_bytes(backup_ov)
        log.exception("stacked-override cleanup failed")
        return RedirectResponse(
            f"/settings/data-integrity/stacked-overrides?error={type(exc).__name__}&detail={exc}",
            status_code=303,
        )
    reader.invalidate()
    return RedirectResponse(
        f"/settings/data-integrity/stacked-overrides?removed={removed}"
        f"&archive={archive.timestamp}",
        status_code=303,
    )


@router.post("/settings/data-integrity/purge-reboot-orphans",
             response_class=HTMLResponse)
def data_integrity_purge_reboot_orphans(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Recovery action — delete every pending source='reboot' staged
    row whose matched ledger transaction is already fully classified
    (no FIXME leg). These are the "duplicates" the user sees on the
    review queue after running a reboot scan against an
    already-classified ledger.

    Read-only on .bean files (only touches ``staged_transactions`` in
    SQLite). Does NOT delete reboot rows that point at unclassified
    ledger entries — those are real work-in-progress items.
    """
    from beancount.core.data import Transaction
    import json
    templates = request.app.state.templates
    entries = list(reader.load().entries)
    by_file_line: dict[tuple[str, int], Transaction] = {}
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        meta = getattr(e, "meta", None) or {}
        f = meta.get("filename")
        ln = meta.get("lineno")
        if isinstance(f, str) and ln is not None:
            try:
                by_file_line[(f, int(ln))] = e
            except (TypeError, ValueError):
                continue

    rows = conn.execute(
        "SELECT id, source_ref FROM staged_transactions "
        "WHERE source = 'reboot' AND status IN ('new', 'classified')"
    ).fetchall()

    purged_ids: list[int] = []
    for r in rows:
        try:
            ref = json.loads(r["source_ref"]) if r["source_ref"] else {}
        except Exception:  # noqa: BLE001
            continue
        f = ref.get("file") if isinstance(ref, dict) else None
        ln = ref.get("lineno") if isinstance(ref, dict) else None
        if not f or ln is None:
            continue
        # Match by suffix because reader-side filenames are absolute
        # paths and source_ref may be relative — look for any txn
        # whose filename ends with the source_ref file path.
        match: Transaction | None = None
        try:
            ln_int = int(ln)
        except (TypeError, ValueError):
            continue
        for (fk, lk), entry in by_file_line.items():
            if lk == ln_int and (fk == f or fk.endswith(f.lstrip("/"))):
                match = entry
                break
        if match is None:
            continue
        # Already classified iff no posting has FIXME at the leaf.
        has_fixme = any(
            (p.account or "").split(":")[-1].upper() == "FIXME"
            for p in (match.postings or [])
        )
        if not has_fixme:
            purged_ids.append(int(r["id"]))

    if purged_ids:
        placeholders = ",".join("?" for _ in purged_ids)
        conn.execute(
            f"DELETE FROM staged_transactions WHERE id IN ({placeholders})",
            purged_ids,
        )
        conn.commit()
        log.info(
            "purge-reboot-orphans: deleted %d already-classified "
            "reboot-source staged row(s)",
            len(purged_ids),
        )
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": None,
            "purge_result": {
                "deleted": len(purged_ids),
                "candidate_pool": len(rows),
            },
            **_preflight_context(conn, reader),
        },
    )


@router.post(
    "/settings/data-integrity/purge-all-reboot-rows",
    response_class=HTMLResponse,
)
def data_integrity_purge_all_reboot_rows(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
):
    """Nuclear recovery — delete EVERY pending source='reboot' staged
    row, regardless of whether the matched ledger entry is classified
    or unclassified.

    The narrower ``purge-reboot-orphans`` only removes rows whose
    ledger counterpart is fully classified. Users who never wanted the
    reboot scan to run at all (default-off kill switch was bypassed,
    or scan ran on a ledger they didn't intend to migrate) need a way
    to undo the full ingest. This action wipes the entire reboot
    surface from ``staged_transactions``. SQLite-only — no .bean files
    are touched, so the ledger remains authoritative and recoverable.
    Promoted rows (status='promoted') are kept because they represent
    work the user has already accepted; only ``new`` / ``classified``
    / ``dismissed`` rows are removed.
    """
    templates = request.app.state.templates
    rows = conn.execute(
        "SELECT id FROM staged_transactions "
        "WHERE source = 'reboot' "
        "AND status IN ('new', 'classified', 'dismissed')"
    ).fetchall()
    deleted = 0
    if rows:
        ids = [int(r["id"]) for r in rows]
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"DELETE FROM staged_transactions WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
        deleted = len(ids)
        log.info(
            "purge-all-reboot-rows: deleted %d non-promoted reboot-source "
            "staged row(s)",
            deleted,
        )
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": None,
            "purge_result": {
                "deleted": deleted,
                "candidate_pool": deleted,
                "scope": "all",
            },
            **_preflight_context(conn, reader),
        },
    )


@router.post("/settings/data-integrity/rollback-reboot", response_class=HTMLResponse)
def data_integrity_rollback_reboot(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
):
    """Restore from the most recent ``.pre-reboot-<timestamp>/`` backup."""
    templates = request.app.state.templates
    rw = RebootWriter(
        ledger_dir=settings.ledger_dir,
        main_bean=settings.ledger_main,
    )
    try:
        apply_result = rw.rollback()
        reader.invalidate()
    except RebootApplyError as exc:
        return templates.TemplateResponse(
            request,
            "data_integrity.html",
            {
                "result": None,
                "recent_sessions": _recent_reboot_sessions(conn),
                "error": str(exc),
                "retrofit_result": None,
                "integrity_report": latest_integrity_report(conn),
                "reboot_plan": None,
                "reboot_apply_result": None,
            },
            status_code=400,
        )
    return templates.TemplateResponse(
        request,
        "data_integrity.html",
        {
            "result": None,
            "recent_sessions": _recent_reboot_sessions(conn),
            "error": None,
            "retrofit_result": None,
            "integrity_report": latest_integrity_report(conn),
            "reboot_plan": None,
            "reboot_apply_result": apply_result,
        },
    )
