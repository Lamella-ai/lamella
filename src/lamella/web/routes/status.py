# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""System status dashboard.

One page answering "is this actually working." Aggregates counts
and freshness signals from every subsystem — ledger, AI cascade,
vector index, Paperless sync, Paperless writeback, SimpleFIN,
mileage, rules, review queue, notifications.

Each card is a thin SQL query or a service call; none of it is
cached. The page is cheap enough to load on demand.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.web.temporal import render_local_ts
from lamella.web.deps import (
    get_ai_service,
    get_db,
    get_ledger_reader,
    get_settings,
)

log = logging.getLogger(__name__)

router = APIRouter()


# ------------------------------------------------------------------
# Data shapes for the template
# ------------------------------------------------------------------


@dataclass
class StatusCard:
    title: str
    health: str = "ok"          # ok | warn | bad | neutral
    summary: str = ""
    stats: list[tuple[str, str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _count(conn: sqlite3.Connection, sql: str, *args) -> int:
    try:
        row = conn.execute(sql, args).fetchone()
    except sqlite3.Error:
        return 0
    if not row:
        return 0
    val = row[0] if not isinstance(row, sqlite3.Row) else row[0]
    try:
        return int(val or 0)
    except (TypeError, ValueError):
        return 0


def _fetchone(conn: sqlite3.Connection, sql: str, *args) -> sqlite3.Row | None:
    try:
        return conn.execute(sql, args).fetchone()
    except sqlite3.Error:
        return None


def _fmt_ts(raw, *, tz_name: str) -> str:
    if raw is None:
        return "never"
    rendered = render_local_ts(raw, tz_name=tz_name, with_seconds=False)
    return rendered or "never"


def _humanize_age(raw) -> str:
    if raw is None:
        return "—"
    try:
        dt = (
            raw if isinstance(raw, datetime)
            else datetime.fromisoformat(str(raw).replace(" ", "T"))
        )
    except ValueError:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    total = int(delta.total_seconds())
    if total < 60:
        return f"{total}s ago"
    if total < 3600:
        return f"{total // 60}m ago"
    if total < 86400:
        return f"{total // 3600}h ago"
    return f"{total // 86400}d ago"


# ------------------------------------------------------------------
# Per-card builders
# ------------------------------------------------------------------


def _ledger_card(conn: sqlite3.Connection, reader: LedgerReader) -> StatusCard:
    """Total txns, FIXME count, last-ingested date. Source of truth
    is the ledger itself — we load it to count."""
    card = StatusCard(title="Ledger")
    try:
        from beancount.core.data import Transaction
        entries = reader.load().entries
    except Exception as exc:  # noqa: BLE001
        card.health = "bad"
        card.summary = f"Couldn't load ledger: {exc}"
        return card

    txn_count = 0
    fixme_count = 0
    unresolved_leaves = {"FIXME", "UNKNOWN", "UNCATEGORIZED", "UNCLASSIFIED"}
    latest_date: date | None = None
    earliest_date: date | None = None
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        txn_count += 1
        if latest_date is None or e.date > latest_date:
            latest_date = e.date
        if earliest_date is None or e.date < earliest_date:
            earliest_date = e.date
        for p in e.postings or []:
            acct = p.account or ""
            leaf = acct.rsplit(":", 1)[-1] if acct else ""
            if leaf in unresolved_leaves:
                fixme_count += 1
                break

    card.summary = f"{txn_count} transactions"
    if earliest_date and latest_date:
        card.summary += f" · {earliest_date} → {latest_date}"
    card.stats = [
        ("Transactions", f"{txn_count:,}"),
        ("Unresolved (FIXME/UNKNOWN/etc.)", f"{fixme_count:,}"),
        ("Latest txn date", str(latest_date) if latest_date else "—"),
    ]
    if fixme_count > 0:
        if fixme_count > 100:
            card.health = "warn"
            card.notes.append(
                f"{fixme_count} unresolved leaves — consider a review "
                f"session or an enricher run."
            )
        else:
            card.health = "ok"
    return card


def _ai_cascade_card(
    conn: sqlite3.Connection, ai: AIService,
) -> StatusCard:
    """Cost + decision counts this month. Primary vs. escalated
    split when we can read it from ai_decisions metadata."""
    card = StatusCard(title="AI cascade")
    if not ai.enabled:
        card.health = "neutral"
        card.summary = "AI disabled (no OPENROUTER_API_KEY)"
        return card

    summary = ai.cost_summary()
    cap = ai.monthly_cap_usd()
    cost = float(summary.get("cost_usd") or 0.0)
    cost_str = f"${cost:.2f}"
    cap_str = f"${cap:.2f}" if cap > 0 else "unlimited"

    # Decision counts this month, split by model.
    month_start = ai.month_start().isoformat(sep=" ", timespec="seconds")
    rows = conn.execute(
        """
        SELECT model, COUNT(*) AS n
          FROM ai_decisions
         WHERE decided_at >= ?
         GROUP BY model
         ORDER BY n DESC
        """,
        (month_start,),
    ).fetchall()
    total_calls = sum(int(r["n"]) for r in rows)
    primary = ai.model_for("classify_txn")
    fallback = ai.fallback_model_for("classify_txn")
    primary_n = next((int(r["n"]) for r in rows if r["model"] == primary), 0)
    fallback_n = (
        next((int(r["n"]) for r in rows if r["model"] == fallback), 0)
        if fallback else 0
    )
    cached_n = next(
        (int(r["n"]) for r in rows if r["model"] == "<cached>"), 0,
    )

    card.summary = f"{total_calls:,} decisions this month · {cost_str}"
    card.stats = [
        ("Total decisions (this month)", f"{total_calls:,}"),
        ("Primary model", primary),
        (f"  → calls on {primary}", f"{primary_n:,}"),
        ("Fallback model", fallback or "(disabled)"),
        (f"  → escalated calls", f"{fallback_n:,}"),
        ("Cache hits", f"{cached_n:,}"),
        ("Cost this month", cost_str),
        ("Monthly cap", cap_str),
    ]
    if cap > 0:
        pct = cost / cap * 100 if cap else 0
        card.stats.append(("Cap used", f"{pct:.0f}%"))
        if cost >= cap:
            card.health = "bad"
            card.notes.append("Cap reached — new classify calls are refused.")
        elif pct > 80:
            card.health = "warn"
            card.notes.append(f"At {pct:.0f}% of monthly cap.")
    return card


def _vector_index_card(
    conn: sqlite3.Connection, settings: Settings,
) -> StatusCard:
    """Embedding count, build freshness. If the build signature
    lags the current ledger signature, the next classify will
    rebuild."""
    card = StatusCard(title="Vector index")
    enabled_row = _fetchone(
        conn, "SELECT value FROM app_settings WHERE key = 'ai_vector_search_enabled'",
    )
    if enabled_row is not None:
        raw = str(enabled_row["value"] or "").strip().lower()
        enabled = raw not in ("0", "false", "no", "off")
    else:
        enabled = settings.ai_vector_search_enabled
    if not enabled:
        card.health = "neutral"
        card.summary = "Disabled in /settings"
        return card

    ledger_count = _count(
        conn, "SELECT COUNT(*) FROM txn_embeddings WHERE source = 'ledger'",
    )
    correction_count = _count(
        conn, "SELECT COUNT(*) FROM txn_embeddings WHERE source = 'correction'",
    )
    total = ledger_count + correction_count

    build = _fetchone(
        conn,
        "SELECT ledger_signature, built_at, row_count, model_name "
        "FROM txn_embeddings_build WHERE source = 'ledger' ORDER BY id DESC LIMIT 1",
    )
    # Is there a build in flight right now? (migration 030 +
    # VectorIndex.build() writes a 'building' row at start, flips
    # to 'complete' at end.)
    building = None
    last_error = None
    try:
        building = conn.execute(
            "SELECT id, started_at, total, processed, trigger "
            "FROM vector_index_runs "
            "WHERE state = 'building' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        # Most recent failure — but ignore it if a successful
        # build finished after it. Otherwise a stale error row
        # from a previous container image sticks on the status
        # page forever even after the user has rebuilt cleanly.
        last_complete = conn.execute(
            "SELECT id FROM vector_index_runs "
            "WHERE state = 'complete' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_complete_id = int(last_complete["id"]) if last_complete else 0
        last_error = conn.execute(
            "SELECT id, started_at, finished_at, error_message, trigger "
            "FROM vector_index_runs "
            "WHERE state = 'error' AND id > ? "
            "ORDER BY id DESC LIMIT 1",
            (last_complete_id,),
        ).fetchone()
    except sqlite3.Error:
        building = None
        last_error = None

    card.stats = [
        ("Total embeddings", f"{total:,}"),
        ("  → ledger rows", f"{ledger_count:,}"),
        ("  → user corrections", f"{correction_count:,}"),
        ("Model", build["model_name"] if build else settings.ai_vector_model_name),
    ]
    # Belt on top of the "ignore errors older than last success"
    # filter: if embeddings actually exist, a build HAS completed
    # successfully at some point — don't flag "last build failed"
    # even if vector_index_runs is missing the success row (e.g.,
    # user cleared txn_embeddings_build via 'Force rebuild on next
    # classify' and we haven't re-embedded yet).
    if total > 0:
        last_error = None

    # Error state takes precedence over "never built" — if the
    # last attempt died, show that so the user doesn't stare at
    # "still building..." for hours while it's actually broken.
    if last_error is not None and building is None:
        card.health = "bad"
        card.summary = f"❌ last build failed ({last_error['trigger'] or 'unknown'})"
        err = (last_error["error_message"] or "").strip()
        if err:
            card.notes.append(f"Last error: {err}")
        err_lower = err.lower()
        if "getpwuid" in err_lower:
            card.notes.append(
                "This error is from the OLD container image — the "
                "container's runtime uid wasn't in /etc/passwd, so "
                "sentence-transformers crashed resolving its cache dir. "
                "The current image monkey-patches pwd.getpwuid to handle "
                "unknown uids. Pull the latest image, restart the "
                "container, then click 'Rebuild now' below."
            )
        else:
            card.notes.append(
                "Click 'Rebuild now' below to retry. Most common cause: "
                "sentence-transformers model download failed (check "
                "container egress to huggingface.co)."
            )
    if building is not None:
        card.health = "warn"
        started_age = _humanize_age(building["started_at"])
        trigger_label = building["trigger"] or "unknown"
        processed = int(building["processed"] or 0)
        total_expected = int(building["total"] or 0)
        if total_expected:
            card.summary = (
                f"🔨 building ({trigger_label}) · "
                f"{processed:,}/{total_expected:,} embedded"
            )
        else:
            card.summary = f"🔨 building ({trigger_label}) · started {started_age}"
        card.notes.append(
            "A rebuild is in flight. Classify calls fall back to "
            "substring matching until it finishes. The rebuild buttons "
            "below are disabled to prevent concurrent rebuilds."
        )
        # Best-effort estimate: 500 txns ≈ 30s on CPU.
        if total_expected and processed:
            remaining = max(0, total_expected - processed)
            est_seconds = int(remaining * (30.0 / 500.0))
            est_mins = est_seconds // 60
            est_s = est_seconds % 60
            eta = f"{est_mins}m {est_s}s" if est_mins else f"{est_s}s"
            card.stats.append(("Estimated remaining", eta))
    if build:
        card.stats.append(("Last built", _fmt_ts(build["built_at"], tz_name=settings.app_tz)))
        card.stats.append(("Last-built age", _humanize_age(build["built_at"])))
        card.stats.append(("Stored signature", build["ledger_signature"] or "—"))
    else:
        if total > 0:
            # Embeddings exist but the signature sentinel is gone —
            # user asked for a forced rebuild on next classify. Don't
            # falsely claim "never built".
            card.stats.append(("Last built", "pending rebuild"))
            if building is None:
                card.notes.append(
                    "Signature cleared — next classify call will "
                    "re-embed the ledger to pick up changes. Existing "
                    f"{total:,} embeddings remain available as a "
                    "fallback until then."
                )
                card.health = "warn"
        else:
            card.stats.append(("Last built", "never"))
            if building is None:
                card.notes.append(
                    "Index has never been built. Next classify call will build "
                    "it (roughly 30s per 500 txns on CPU — a 20k ledger is "
                    "about 20 minutes)."
                )
                card.health = "warn"
    if not card.summary:
        card.summary = f"{total:,} embedded txns"
    return card


def _paperless_card(
    conn: sqlite3.Connection, settings: Settings,
) -> StatusCard:
    card = StatusCard(title="Paperless")
    if not settings.paperless_configured:
        card.health = "neutral"
        card.summary = "Not configured"
        card.notes.append("Set PAPERLESS_URL + PAPERLESS_API_TOKEN to enable.")
        return card

    doc_count = _count(conn, "SELECT COUNT(*) FROM paperless_doc_index")
    linked = _count(conn, "SELECT COUNT(DISTINCT paperless_id) FROM receipt_links")
    orphan = doc_count - linked
    sync_state = _fetchone(
        conn,
        "SELECT last_full_sync_at, last_incremental_sync_at, doc_count, "
        "last_status, last_error FROM paperless_sync_state WHERE id = 1",
    )

    # Mime distribution (useful to see if native-PDF detection is saving tokens).
    mime_rows = conn.execute(
        "SELECT COALESCE(mime_type, '(unknown)') AS mime, COUNT(*) AS n "
        "FROM paperless_doc_index GROUP BY mime ORDER BY n DESC LIMIT 6"
    ).fetchall()
    mime_breakdown = ", ".join(f"{r['mime']}: {int(r['n'])}" for r in mime_rows)

    # Writebacks so far.
    verified = _count(
        conn,
        "SELECT COUNT(*) FROM paperless_writeback_log WHERE kind = 'verify_correction'",
    )
    enriched = _count(
        conn,
        "SELECT COUNT(*) FROM paperless_writeback_log WHERE kind = 'enrichment_note'",
    )

    writeback_enabled_row = _fetchone(
        conn,
        "SELECT value FROM app_settings WHERE key = 'paperless_writeback_enabled'",
    )
    if writeback_enabled_row is not None:
        raw = str(writeback_enabled_row["value"] or "").strip().lower()
        wb_enabled = raw not in ("0", "false", "no", "off")
    else:
        wb_enabled = settings.paperless_writeback_enabled

    card.summary = f"{doc_count:,} docs indexed · {linked:,} linked to txns"
    card.stats = [
        ("Indexed docs", f"{doc_count:,}"),
        ("  → linked to txns", f"{linked:,}"),
        ("  → unlinked (orphans)", f"{orphan:,}"),
        ("Mime breakdown", mime_breakdown or "—"),
        (
            "Last full sync",
            _fmt_ts(sync_state["last_full_sync_at"], tz_name=settings.app_tz) if sync_state else "never",
        ),
        (
            "Last incremental sync",
            _fmt_ts(sync_state["last_incremental_sync_at"], tz_name=settings.app_tz) if sync_state else "never",
        ),
        (
            "Last sync status",
            (sync_state["last_status"] if sync_state else None) or "—",
        ),
        ("Writeback", "ON" if wb_enabled else "OFF"),
        ("  → verify corrections applied", f"{verified:,}"),
        ("  → enrichments pushed", f"{enriched:,}"),
    ]
    if sync_state and sync_state["last_error"]:
        card.health = "bad"
        card.notes.append(f"Last sync error: {sync_state['last_error']}")
    elif sync_state and sync_state["last_incremental_sync_at"]:
        age = _humanize_age(sync_state["last_incremental_sync_at"])
        card.stats.append(("Sync age", age))
    return card


def _simplefin_card(
    conn: sqlite3.Connection, settings: Settings,
) -> StatusCard:
    card = StatusCard(title="SimpleFIN")
    mode = (settings.simplefin_mode or "disabled").lower()
    if mode == "disabled" or not settings.simplefin_access_url:
        card.health = "neutral"
        card.summary = "Disabled"
        return card
    card.summary = f"Mode: {mode}"
    last_ingest = _fetchone(
        conn,
        "SELECT started_at, finished_at, status, summary_json "
        "FROM simplefin_ingest_runs "
        "ORDER BY id DESC LIMIT 1",
    )
    discovered = _count(
        conn, "SELECT COUNT(*) FROM simplefin_discovered_accounts",
    )
    card.stats = [
        ("Mode", mode),
        ("Lookback", f"{settings.simplefin_lookback_days} days"),
        ("Fetch interval", f"{settings.simplefin_fetch_interval_hours}h"),
        ("Discovered accounts", f"{discovered:,}"),
    ]
    if last_ingest:
        card.stats.append(
            ("Last ingest started", _fmt_ts(last_ingest["started_at"], tz_name=settings.app_tz)),
        )
        card.stats.append(
            ("Last ingest status", last_ingest["status"] or "—"),
        )
        card.stats.append(
            ("Last ingest age", _humanize_age(last_ingest["started_at"])),
        )
        if last_ingest["status"] and last_ingest["status"] != "ok":
            card.health = "warn"
    else:
        card.stats.append(("Last ingest", "never"))
    return card


def _mileage_card(conn: sqlite3.Connection) -> StatusCard:
    card = StatusCard(title="Mileage")
    entry_count = _count(conn, "SELECT COUNT(*) FROM mileage_entries")
    if entry_count == 0:
        card.health = "neutral"
        card.summary = "No mileage entries"
        return card
    card.summary = f"{entry_count:,} entries"
    by_entity = conn.execute(
        "SELECT entity, COUNT(*) AS n, SUM(miles) AS miles "
        "FROM mileage_entries GROUP BY entity ORDER BY n DESC"
    ).fetchall()
    card.stats = [("Total entries", f"{entry_count:,}")]
    for r in by_entity[:8]:
        ent = r["entity"] or "(no entity)"
        miles = float(r["miles"] or 0)
        card.stats.append((f"  → {ent}", f"{int(r['n']):,} entries · {miles:,.1f} mi"))
    latest = _fetchone(
        conn, "SELECT MAX(entry_date) AS d FROM mileage_entries",
    )
    if latest:
        card.stats.append(("Latest entry", str(latest["d"]) or "—"))
    return card


def _rules_card(conn: sqlite3.Connection) -> StatusCard:
    card = StatusCard(title="Rules")
    total = _count(conn, "SELECT COUNT(*) FROM classification_rules")
    try:
        by_source = conn.execute(
            "SELECT created_by, COUNT(*) AS n FROM classification_rules "
            "GROUP BY created_by ORDER BY n DESC"
        ).fetchall()
    except sqlite3.Error:
        by_source = []
    hits = _count(
        conn, "SELECT COALESCE(SUM(hit_count), 0) FROM classification_rules",
    )
    card.summary = f"{total:,} active"
    card.stats = [
        ("Total rules", f"{total:,}"),
        ("Lifetime hit count (sum)", f"{hits:,}"),
    ]
    for r in by_source:
        cb = r["created_by"] or "(unknown)"
        card.stats.append((f"  → created_by={cb}", f"{int(r['n']):,}"))
    return card


def _review_card(conn: sqlite3.Connection) -> StatusCard:
    card = StatusCard(title="Review queue")
    open_total = _count(
        conn, "SELECT COUNT(*) FROM review_queue WHERE resolved_at IS NULL",
    )
    by_kind = conn.execute(
        "SELECT kind, COUNT(*) AS n FROM review_queue "
        "WHERE resolved_at IS NULL GROUP BY kind ORDER BY n DESC"
    ).fetchall()
    card.summary = f"{open_total:,} open items"
    card.stats = [("Open items", f"{open_total:,}")]
    for r in by_kind:
        card.stats.append((f"  → {r['kind']}", f"{int(r['n']):,}"))
    if open_total > 50:
        card.health = "warn"
    return card


def _notifications_card(
    conn: sqlite3.Connection, settings: Settings,
) -> StatusCard:
    card = StatusCard(title="Notifications")
    channels: list[str] = []
    if settings.ntfy_enabled:
        channels.append("ntfy")
    if settings.pushover_enabled:
        channels.append("pushover")
    if not channels:
        card.health = "neutral"
        card.summary = "No channels configured"
        return card
    recent = _count(
        conn,
        "SELECT COUNT(*) FROM notifications "
        "WHERE created_at >= datetime('now', '-7 days')",
    )
    card.summary = f"{', '.join(channels)} · {recent:,} recent"
    card.stats = [
        ("Channels", ", ".join(channels)),
        ("Notifications in last 7 days", f"{recent:,}"),
    ]
    return card


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------


@router.get("/status", response_class=HTMLResponse)
def status_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    ai: AIService = Depends(get_ai_service),
):
    cards = [
        _ledger_card(conn, reader),
        _ai_cascade_card(conn, ai),
        _vector_index_card(conn, settings),
        _paperless_card(conn, settings),
        _simplefin_card(conn, settings),
        _review_card(conn),
        _rules_card(conn),
        _mileage_card(conn),
        _notifications_card(conn, settings),
    ]
    return request.app.state.templates.TemplateResponse(
        request, "status.html",
        {
            "cards": cards,
            "generated_at": datetime.now(timezone.utc),
            "app_tz_label": settings.app_tz or "UTC",
        },
    )


@router.post("/status/vector-index/clear-stuck", response_class=HTMLResponse)
def clear_stuck_vector_build(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Mark any stuck 'building' rows as 'error'. Used to recover
    from a crashed build task that didn't clean up its state row
    (silent pod OOM, getpwuid issue before the error handler
    could fire, etc.)."""
    try:
        conn.execute(
            "UPDATE vector_index_runs "
            "SET state = 'error', finished_at = datetime('now'), "
            "    error_message = COALESCE(error_message, 'manually cleared stuck state') "
            "WHERE state = 'building'"
        )
    except sqlite3.Error:
        pass
    return RedirectResponse(url="/status", status_code=303)


@router.post("/status/paperless/full-sync", response_class=HTMLResponse)
async def paperless_full_sync(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Kick a Paperless full sync via JobRunner so the user gets a
    live progress modal. Replaces the old fire-and-forget asyncio
    task whose only feedback was a static "reload to see progress"
    banner."""
    if not settings.paperless_configured:
        return HTMLResponse(
            '<p class="muted">Paperless not configured.</p>',
        )
    import asyncio as _asyncio

    def _work(ctx):
        ctx.emit("Starting Paperless full sync …", outcome="info")
        loop = _asyncio.new_event_loop()
        try:
            from lamella.main import _run_paperless_sync
            loop.run_until_complete(_run_paperless_sync(request.app, full=True))
        finally:
            loop.close()
        ctx.emit("Full sync complete", outcome="success")
        return None

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="paperless-full-sync",
        title="Paperless full sync",
        fn=_work,
        return_url="/status",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/status"},
    )


async def _run_paperless_sync_wrapper(app) -> None:
    """Thin wrapper so the route can import lazily without circular
    dep on main.py."""
    from lamella.main import _run_paperless_sync
    await _run_paperless_sync(app, full=True)


@router.post("/status/vector-index/rebuild", response_class=HTMLResponse)
def rebuild_vector_index(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
    reader: LedgerReader = Depends(get_ledger_reader),
    settings: Settings = Depends(get_settings),
    sync: str = "",
):
    """Rebuild the vector index.

    Default (no sync flag): clear the build-signature row so the
    next classify call does the rebuild inline. Returns a redirect
    to /status.

    With ``?sync=1`` (from the synchronous-rebuild button): run
    the full rebuild inline right now, measure elapsed time, and
    render a small result partial showing "Rebuilt N ledger rows +
    M corrections in X.Ys." Meant for iterating on a review
    session — user sees immediate confirmation rather than
    trusting the next classify.
    """
    # Don't let the user kick a second build while one is already
    # in progress — concurrent builds race on the same rows.
    try:
        in_flight = conn.execute(
            "SELECT id FROM vector_index_runs WHERE state = 'building' LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        in_flight = None
    if in_flight is not None:
        return HTMLResponse(
            '<p class="muted">A rebuild is already in progress. Wait for '
            'it to finish before kicking another one. '
            '<a href="/status">Back to status →</a></p>',
        )

    if not sync:
        conn.execute("DELETE FROM txn_embeddings_build")
        return RedirectResponse(url="/status", status_code=303)

    def _work(ctx):
        import time
        from lamella.features.ai_cascade.decisions import DecisionsLog
        from lamella.features.ai_cascade.vector_index import (
            VectorIndex, VectorUnavailable,
        )
        ctx.emit("Loading ledger …", outcome="info")
        try:
            entries = reader.load().entries
        except Exception as exc:  # noqa: BLE001
            ctx.emit(f"Ledger load failed: {exc}", outcome="error")
            raise
        ctx.emit(
            f"Loaded {len(entries)} entries · building vector index",
            outcome="info",
        )
        try:
            from lamella.main import ensure_hf_home
            ensure_hf_home()
        except Exception:  # noqa: BLE001
            pass
        start = time.monotonic()
        try:
            idx = VectorIndex(conn, model_name=settings.ai_vector_model_name)
            stats = idx.build(
                entries=entries,
                ai_decisions=DecisionsLog(conn),
                ledger_signature="",
                force=True,
                trigger="manual",
            )
        except VectorUnavailable:
            ctx.emit(
                "sentence-transformers not available; falling back to substring path.",
                outcome="failure",
            )
            return {"unavailable": True}
        elapsed = time.monotonic() - start
        ledger_added = int(stats.get("ledger_added", 0) or 0)
        corrections_added = int(stats.get("corrections_added", 0) or 0)
        ctx.emit(
            f"Rebuilt {ledger_added} ledger rows + "
            f"{corrections_added} corrections in {elapsed:.1f}s",
            outcome="success",
        )
        return {
            "ledger_added": ledger_added,
            "corrections_added": corrections_added,
            "elapsed": elapsed,
        }

    runner = request.app.state.job_runner
    job_id = runner.submit(
        kind="vector-index-rebuild",
        title="Vector index rebuild",
        fn=_work,
        return_url="/status",
    )
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": "/status"},
    )
