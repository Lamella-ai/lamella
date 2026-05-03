# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from lamella import __version__
from lamella.utils._legacy_env import apply_env_aliases

# Translate legacy env-var names (CONNECTOR_*, BCG_SKIP_DISCOVERY_GUARD)
# to/from the new LAMELLA_* names BEFORE pydantic Settings reads them.
# Logs a one-shot DeprecationWarning when only the legacy name is set.
apply_env_aliases()

from lamella.features.ai_cascade.service import AIService
from lamella.core.backups.sqlite_dump import run_backup
from lamella.core.beancount_io import LedgerReader
from lamella.core.bootstrap.detection import detect_ledger_state
from lamella.core.config import Settings, get_settings
from lamella.core.db import connect, migrate
from lamella.core.jobs.runner import JobRunner
from lamella.features.review_queue.service import ReviewService
from lamella.features.notifications.dispatcher import Dispatcher
from lamella.adapters.ntfy.client import NtfyNotifier
from lamella.adapters.pushover.client import PushoverNotifier
from lamella.features.notifications.digests import maybe_send_weekly_digest
from lamella.web.routes import (
    accounts as accounts_route,
    accounts_admin as accounts_admin_route,
    accounts_browse as accounts_browse_route,
    api_txn as api_txn_route,
    inbox as inbox_route,
    balances as balances_route,
    ai as ai_route,
    backups as backups_route,
    budgets as budgets_route,
    businesses as businesses_route,
    calendar as calendar_route,
    card as card_route,
    dashboard,
    entities as entities_route,
    health,
    data_integrity as data_integrity_route,
    import_ as import_route,
    intake as intake_route,
    intercompany as intercompany_route,
    jobs as jobs_route,
    loans as loans_route,
    loans_backfill as loans_backfill_route,
    loans_wizard as loans_wizard_route,
    staging_review as staging_review_route,
    review_duplicates as review_duplicates_route,
    imports_archive as imports_archive_route,
    mileage as mileage_route,
    note,
    notifications as notifications_route,
    paperless_anomalies as paperless_anomalies_route,
    paperless_fields as paperless_fields_route,
    paperless_proxy as paperless_proxy_route,
    paperless_verify as paperless_verify_route,
    paperless_workflows as paperless_workflows_route,
    paperless_workflows_settings as paperless_workflows_settings_route,
    paperless_writebacks as paperless_writebacks_route,
    payout_sources as payout_sources_route,
    status as status_route,
    setup_check as setup_check_route,
    account_descriptions as account_descriptions_route,
    audit as audit_route,
    projects as projects_route,
    properties as properties_route,
    documents,
    documents_needed as documents_needed_route,
    recurring as recurring_route,
    reports,
    review,
    rewrite as rewrite_route,
    rules,
    setup as setup_route,
    setup_legacy_paths as setup_legacy_paths_route,
    setup_recovery as setup_recovery_route,
    setup_schema as setup_schema_route,
    setup_wizard as setup_wizard_route,
    slug_api as slug_api_route,
    search as search_route,
    teach as teach_route,
    transactions as transactions_route,
    txn_document as txn_document_route,
    dangling_documents as dangling_documents_route,
    settings as settings_route,
    simplefin as simplefin_route,
    vehicles as vehicles_route,
    webhooks,
)
from lamella.features.budgets.alerts import _channels_from_setting, evaluate_and_alert
from lamella.features.mileage.service import MileageService
from lamella.features.recurring.detector import run_detection
from lamella.features.recurring.confirmations import monitor_after_ingest
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.scanner import FixmeScanner
from lamella.features.rules.service import RuleService
from lamella.core.settings.store import AppSettingsStore
from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.sync import PaperlessSync
from lamella.core.registry.alias import account_label, alias_for, format_money
from lamella.core.registry.discovery import sync_from_ledger
from lamella.adapters.simplefin.client import SimpleFINClient, SimpleFINError
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.schedule import register as register_simplefin
from lamella.features.bank_sync.writer import SimpleFINWriter

log = logging.getLogger(__name__)

PACKAGE_ROOT = Path(__file__).resolve().parent
TEMPLATE_DIR = PACKAGE_ROOT / "web" / "templates"
STATIC_DIR = PACKAGE_ROOT / "web" / "static"


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _migrate_legacy_sqlite_filename(settings: Settings) -> None:
    """Rename the legacy beancounter-glue.sqlite file to lamella.sqlite.

    Pre-rebrand the SQLite file lived at
    ``$LAMELLA_DATA_DIR/beancounter-glue.sqlite``; post-rebrand it
    lives at ``$LAMELLA_DATA_DIR/lamella.sqlite``. We rename
    in-place (preserving ctime, contents, and the ``-wal``/``-shm``
    sidecars) the first time we see the legacy file without the new
    file. If both exist we leave both alone and log a warning — the
    operator picked one to keep current and we shouldn't guess.

    Drop together with the env-var deprecation shim once no
    deployment is on the legacy filename.
    """
    legacy = settings.legacy_db_path
    new = settings.db_path
    if not legacy.exists():
        return
    if new.exists():
        log.warning(
            "sqlite migration: both %s and %s exist — leaving both in "
            "place. Inspect and remove the stale one manually.",
            legacy.name,
            new.name,
        )
        return
    log.info(
        "sqlite migration: renaming %s → %s (one-time)",
        legacy.name,
        new.name,
    )
    legacy.rename(new)
    for suffix in ("-wal", "-shm"):
        side_old = legacy.with_name(legacy.name + suffix)
        side_new = new.with_name(new.name + suffix)
        if side_old.exists() and not side_new.exists():
            side_old.rename(side_new)


def _run_fixme_scan(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    try:
        scanner = FixmeScanner(
            reader=app.state.ledger_reader,
            reviews=ReviewService(app.state.db),
            rules=RuleService(app.state.db),
            override_writer=OverrideWriter(
                main_bean=settings.ledger_main,
                overrides=settings.connector_overrides_path,
                conn=app.state.db,
            ),
        )
        scanner.scan()
    except Exception as exc:  # never let scheduler die from a scan error
        log.warning("FIXME scanner failed: %s", exc)


def _run_sqlite_backup(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    try:
        run_backup(db_path=settings.db_path, backup_dir=settings.backups_dir)
    except Exception as exc:
        log.warning("sqlite backup failed: %s", exc)


def _build_dispatcher(app: FastAPI) -> Dispatcher:
    settings: Settings = app.state.settings
    notifiers = [
        NtfyNotifier(
            base_url=settings.ntfy_base_url,
            topic=settings.ntfy_topic,
            token=(
                settings.ntfy_token.get_secret_value() if settings.ntfy_token else None
            ),
        ),
        PushoverNotifier(
            user_key=(
                settings.pushover_user_key.get_secret_value()
                if settings.pushover_user_key
                else None
            ),
            api_token=(
                settings.pushover_api_token.get_secret_value()
                if settings.pushover_api_token
                else None
            ),
        ),
    ]
    return Dispatcher(conn=app.state.db, notifiers=notifiers)


def _run_recurring_detection(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    try:
        run_detection(
            conn=app.state.db,
            entries=app.state.ledger_reader.load().entries,
            scan_window_days=settings.recurring_scan_window_days,
            min_occurrences=settings.recurring_min_occurrences,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("recurring detection failed: %s", exc)


async def _run_budget_evaluation(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    dispatcher: Dispatcher | None = getattr(app.state, "dispatcher", None)
    try:
        await evaluate_and_alert(
            conn=app.state.db,
            dispatcher=dispatcher,
            entries=app.state.ledger_reader.load().entries,
            channels=_channels_from_setting(settings.budget_alert_channels),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("budget evaluation failed: %s", exc)


def _run_business_cache_warmup(app: FastAPI) -> None:
    """Pre-populate the per-entity dashboard cache for all six period
    labels. The first page load after a ledger write still pays the
    compute cost; this job keeps subsequent loads on the warm path even
    when the user hasn't visited the dashboard recently."""
    try:
        from lamella.features.dashboard import service as dash
        from lamella.core.registry.service import list_entities

        conn = app.state.db
        loaded = app.state.ledger_reader.load()
        entities = list_entities(conn, include_inactive=False)
        for entity in entities:
            for label in dash.PERIOD_LABELS:
                period = dash.resolve_period(label)
                dash.compute_period_kpis(conn, loaded, entity.slug, period)
                dash.compute_expense_composition(conn, loaded, entity.slug, period)
                dash.compute_top_payees(conn, loaded, entity.slug, period)
            dash.compute_monthly_pnl(conn, loaded, entity.slug)
            dash.compute_expense_trend(conn, loaded, entity.slug)
    except Exception as exc:  # noqa: BLE001
        log.warning("business cache warmup failed: %s", exc)


def _run_receipt_auto_sweep(app: FastAPI) -> None:
    """Continuous receipt → transaction matching. No AI call —
    pure deterministic scoring on amount/date/merchant against
    the Paperless index. Runs in-thread; sweep_recent is bounded
    by the 60-day window so a typical run touches ≤ a few hundred
    txns."""
    settings: Settings = app.state.settings
    if not settings.paperless_configured:
        return
    try:
        from lamella.features.receipts.auto_match import sweep_recent
        result = sweep_recent(
            conn=app.state.db,
            reader=app.state.ledger_reader,
            settings=settings,
        )
        if result.matched > 0:
            log.info(
                "receipt auto-sweep: linked %d, scanned %d "
                "(already_linked=%d, no_candidate=%d, low_conf=%d, errors=%d)",
                result.matched, result.scanned, result.already_linked,
                result.no_candidate, result.low_confidence,
                len(result.errors),
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("receipt auto-sweep failed: %s", exc)


async def _run_context_ready_classify(app: FastAPI) -> None:
    """Context-gated trickle classify. Walks pending FIXMEs,
    applies the direct-evidence gate, processes a small budget
    per run.

    Two sub-tiers run before any LLM call:
      1. Pattern-from-neighbors: vector lookup; if ≥3 classified
         neighbors agree on a single target with similarity ≥ 0.85,
         apply that target via in-place rewrite. Free.
      2. AI classify: only for rows that pass the gate AND no
         pattern matched. Hard cap (settings) per run.

    Off-gate rows are left for context to keep accumulating, or
    for the user-triggered bulk classify path. See
    docs/specs/AI-CLASSIFICATION.md "Scheduling — context-gated trickle."
    """
    settings: Settings = app.state.settings
    ai_service = getattr(app.state, "ai_service", None)
    if ai_service is None or not getattr(ai_service, "enabled", False):
        return
    try:
        from lamella.features.ai_cascade.trickle_classify import run_trickle
        await run_trickle(
            conn=app.state.db,
            reader=app.state.ledger_reader,
            settings=settings,
            ai_service=ai_service,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("trickle classify failed: %s", exc)


async def _run_weekly_digest(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    dispatcher: Dispatcher | None = getattr(app.state, "dispatcher", None)
    if dispatcher is None:
        return
    try:
        await maybe_send_weekly_digest(
            dispatcher=dispatcher,
            conn=app.state.db,
            mileage_csv_path=settings.mileage_csv_resolved,
            digest_day=settings.notify_digest_day,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("weekly digest failed: %s", exc)


def ensure_hf_home() -> None:
    """Alias kept at module level for import from other routes
    (status.py sync-rebuild, etc.). Backward-compat with the
    underscored private name used only inside this module."""
    _ensure_hf_home()


_BCG_DIRECTIVE_TYPES = frozenset({
    "loan", "loan-balance-anchor", "loan-deleted",
    "property", "property-valuation", "property-deleted",
    "project", "project-deleted",
    "note", "note-deleted",
    "vehicle", "vehicle-fuel-entry", "mileage-trip-meta",
    "account-description", "entity-context", "audit-dismissed",
    "classification-rule", "budget", "receipt-dismissed",
    "recurring-confirmed", "recurring-ignored", "paperless-field",
    "setting", "setting-unset",
    "vehicle-yearly-mileage", "vehicle-valuation",
    "vehicle-election", "vehicle-credit", "vehicle-renewal",
    "vehicle-trip-template",
    "mileage-attribution", "mileage-attribution-revoked",
    # ADR-0061: both vocabularies recognized; writers emit document-*.
    "document-link", "document-link-hash-backfill",
    "document-dismissed", "document-dismissal-revoked",
    "document-link-blocked", "document-link-block-revoked",
    "receipt-link", "receipt-link-hash-backfill",
    "day-review", "day-review-deleted",
    "account-kind", "account-kind-cleared",
    "balance-anchor", "balance-anchor-revoked",
})


# Same set the per-table wizard checks to decide whether the user has
# ever gone through onboarding. Any populated row = past the setup
# stage; no redirect.
_WITNESS_TABLES = (
    "app_settings",
    "classification_rules",
    "paperless_field_map",
    "loans",
    "notes",
    "mileage_trip_meta",
    "projects",
    "property_valuations",
    "vehicle_yearly_mileage",
    "vehicle_valuations",
    "vehicle_fuel_log",
    "account_classify_context",
    "audit_dismissals",
    "budgets",
    "document_dismissals",
    "recurring_expenses",
)


def _ledger_is_bcg_managed(reader) -> bool:
    """True when ANY entry in the ledger carries a lamella-* metadata key
    OR a known lamella-owned custom directive type. Distinguishes "user
    imported raw Beancount we've never touched" from "user's ledger
    already carries our directives"."""
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return False
    from beancount.core.data import Custom
    for e in entries:
        if isinstance(e, Custom) and e.type in _BCG_DIRECTIVE_TYPES:
            return True
        meta = getattr(e, "meta", None) or {}
        for key in meta:
            if isinstance(key, str) and key.startswith("lamella-"):
                return True
    return False


def _witnesses_empty(db) -> bool:
    """True when none of the "user has onboarded" witness tables have
    any rows. Registry-discovered tables (entities/vehicles/properties
    base rows) intentionally don't count — they auto-populate on every
    boot and would always short-circuit this check."""
    import sqlite3 as _sqlite3
    for table in _WITNESS_TABLES:
        try:
            row = db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        except _sqlite3.OperationalError:
            continue
        if row is not None:
            return False
    return True


def _detect_needs_reconstruct(db, reader) -> bool:
    """True when the ledger carries lamella-* markers we'd normally rebuild
    into state tables, but none of those state tables have been
    populated yet. That's the "fresh DB against previously-managed
    ledger" case; setup_gate routes /* to /setup/reconstruct.

    Returns False when the ledger has no lamella-* markers at all — that's
    the "raw Beancount files, never touched by this software" case,
    which /setup/welcome handles instead.
    """
    if not _witnesses_empty(db):
        return False
    return _ledger_is_bcg_managed(reader)


def _detect_needs_welcome(db, reader) -> bool:
    """True when the ledger has real transactions but no lamella-* markers
    anywhere — i.e., the user imported raw Beancount files and has
    never used our UI. setup_gate routes /* to /setup/welcome so they
    see a guided first-run page.
    """
    if not _witnesses_empty(db):
        return False
    try:
        entries = reader.load().entries
    except Exception:  # noqa: BLE001
        return False
    from beancount.core.data import Transaction
    has_content = any(isinstance(e, Transaction) for e in entries)
    if not has_content:
        return False
    return not _ledger_is_bcg_managed(reader)


def _should_route_to_first_run_wizard(request, detection) -> bool:
    """True when this request should land on /setup/wizard/welcome.

    The wizard is for a TRULY zero-state install. If any of the
    following are true, we route to the maintenance ``/setup``
    instead so the user gets the existing recover / import / fixup
    paths rather than an onboarding flow that would talk past their
    existing data:

      - ledger state is anything other than MISSING / STRUCTURALLY_EMPTY
        (i.e. the ledger is broken, foreign-imported, version-stamped
        with content, etc.)
      - the ledger directory contains any ``*.bean`` files even though
        ``main.bean`` is missing — the user has data we should let them
        import via /setup/import, not silently scaffold past
      - witness tables already have data (notes, settings, rules,
        etc.) — the DB is non-fresh
      - the ledger carries any ``lamella-*`` markers — it was managed by
        a previous install and reconstruct is the right move
      - the wizard has already been completed once — re-running
        onboarding on an existing install would mangle it

    Anything else (clean directory, fresh DB, no prior state) drops
    the user into the wizard.
    """
    try:
        from lamella.core.bootstrap.detection import LedgerState
        from lamella.features.setup.wizard_state import is_wizard_complete
        if detection.state not in {
            LedgerState.MISSING, LedgerState.STRUCTURALLY_EMPTY,
        }:
            return False
        # Ledger directory carries other .bean files? That's a partial
        # ledger the user wants to import — leave them on /setup so
        # the existing import analyzer + reconstruct paths stay
        # discoverable. This is the "I already have beancount files"
        # case the user explicitly called out.
        try:
            settings = getattr(request.app.state, "settings", None)
            if settings is not None:
                ledger_dir = getattr(settings, "ledger_dir", None)
                if ledger_dir is not None and ledger_dir.is_dir():
                    from lamella.core.bootstrap.templates import (
                        CANONICAL_FILES as _CF,
                    )
                    canon = {"main.bean"} | {f.name for f in _CF}
                    other_bean = [
                        p for p in ledger_dir.glob("*.bean")
                        if p.name not in canon
                    ]
                    if other_bean:
                        return False
        except Exception:  # noqa: BLE001
            pass
        db = getattr(request.app.state, "db", None)
        if db is None:
            return False
        if is_wizard_complete(db):
            return False
        if not _witnesses_empty(db):
            return False
        reader = getattr(request.app.state, "ledger_reader", None)
        if reader is not None and _ledger_is_bcg_managed(reader):
            return False
        return True
    except Exception:  # noqa: BLE001
        return False


def _ensure_hf_home() -> None:
    """Belt-and-suspenders: set HOME + HF_HOME + friends if
    unset, so sentence-transformers doesn't call getpwuid() in
    container runtimes where the invoking uid isn't in
    /etc/passwd (which raises 'getpwuid(): uid not found: N' and
    kills the vector build silently). The runtime Dockerfile sets
    these explicitly; this guard covers the case where the
    container image is older than that fix."""
    import os
    defaults = {
        "HOME": "/app",
        "HF_HOME": "/data/huggingface",
        "TRANSFORMERS_CACHE": "/data/huggingface",
        "SENTENCE_TRANSFORMERS_HOME": "/data/huggingface",
        "XDG_CACHE_HOME": "/data/cache",
    }
    for k, v in defaults.items():
        if not os.environ.get(k):
            os.environ[k] = v
    # Ensure the directories exist + are writable. If not, drop
    # back to /tmp so at least the model download works (just not
    # persistent across restarts).
    import pathlib
    for path_str in (
        os.environ["HF_HOME"],
        os.environ["TRANSFORMERS_CACHE"],
        os.environ["SENTENCE_TRANSFORMERS_HOME"],
        os.environ["XDG_CACHE_HOME"],
    ):
        p = pathlib.Path(path_str)
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception:  # noqa: BLE001
            # Can't create it — fall back to tmp.
            tmp = pathlib.Path("/tmp/Lamella-hf")
            tmp.mkdir(parents=True, exist_ok=True)
            os.environ["HF_HOME"] = str(tmp)
            os.environ["TRANSFORMERS_CACHE"] = str(tmp)
            os.environ["SENTENCE_TRANSFORMERS_HOME"] = str(tmp)
            break


async def _vector_index_refresh(app: FastAPI) -> None:
    """Background freshness check for the vector index at app start.

    The underlying ``VectorIndex.build()`` does sentence-transformers
    encode() work synchronously — if called on the event loop, every
    HTTP request waits for encoding to finish (= "the web UI doesn't
    respond for 2 minutes after restart"). Wrap the whole thing in
    ``asyncio.to_thread`` so the event loop stays free for HTTP.

    App.state.vector_index_progress holds a live counter the UI can
    poll to show a spinner with real progress.
    """
    import asyncio as _asyncio
    _ensure_hf_home()
    # Initialize progress state BEFORE launching the worker so a fast
    # request can read it immediately without a KeyError.
    app.state.vector_index_progress = {
        "status": "starting",
        "processed": 0,
        "total": 0,
        "error": None,
        "started_at": datetime.now(UTC).isoformat(),
        "finished_at": None,
    }

    def _work() -> None:
        try:
            settings: Settings = app.state.settings
            reader: LedgerReader = app.state.ledger_reader
            conn = app.state.db
            from lamella.features.ai_cascade.decisions import DecisionsLog
            from lamella.features.ai_cascade.vector_index import (
                VectorIndex,
                VectorUnavailable,
            )

            entries = reader.load().entries
            app.state.vector_index_progress["total"] = len(entries)
            app.state.vector_index_progress["status"] = "running"
            last_date = ""
            for e in entries:
                d = getattr(e, "date", None)
                if d is not None and d.isoformat() > last_date:
                    last_date = d.isoformat()
            try:
                row = conn.execute(
                    "SELECT COUNT(*) AS n, MAX(id) AS last_id "
                    "FROM ai_decisions "
                    "WHERE decision_type = 'classify_txn' "
                    "  AND user_corrected = 1"
                ).fetchone()
                correction_n = int(row["n"] or 0) if row else 0
                correction_last = int(row["last_id"] or 0) if row else 0
            except Exception:  # noqa: BLE001
                correction_n, correction_last = 0, 0
            sig = (
                f"{len(entries)}:{last_date}:c{correction_n}:l{correction_last}"
            )
            try:
                def _progress(processed: int, total: int) -> None:
                    # Mutate the live state dict so polling readers see it.
                    app.state.vector_index_progress["processed"] = processed
                    app.state.vector_index_progress["total"] = total

                idx = VectorIndex(conn, model_name=settings.ai_vector_model_name)
                # The build function publishes per-batch progress via log;
                # we also attach a progress callback if the API supports it.
                kwargs = {
                    "entries": entries,
                    "ai_decisions": DecisionsLog(conn),
                    "ledger_signature": sig,
                    "trigger": "startup",
                }
                try:
                    # Try passing progress_callback — newer API. If the
                    # function doesn't accept it, retry without.
                    stats = idx.build(progress_callback=_progress, **kwargs)
                except TypeError:
                    stats = idx.build(**kwargs)
                app.state.vector_index_progress["status"] = "done"
                if stats.get("ledger_added", 0) or stats.get("corrections_added", 0):
                    log.info(
                        "vector index: restart rebuild added %d ledger rows + "
                        "%d corrections (sig=%s)",
                        stats.get("ledger_added", 0),
                        stats.get("corrections_added", 0),
                        sig,
                    )
                else:
                    log.info("vector index: fresh at startup (sig=%s)", sig)
            except VectorUnavailable:
                app.state.vector_index_progress["status"] = "unavailable"
                log.info(
                    "vector index: sentence-transformers unavailable; "
                    "classify will fall back to substring path"
                )
        except Exception as exc:  # noqa: BLE001 — never kill startup
            app.state.vector_index_progress["status"] = "failed"
            app.state.vector_index_progress["error"] = str(exc)
            log.warning("vector index startup check failed: %s", exc)
        finally:
            app.state.vector_index_progress["finished_at"] = (
                datetime.now(UTC).isoformat()
            )

    # to_thread drops the synchronous encoding work into a worker
    # thread so the asyncio event loop continues serving HTTP.
    try:
        await _asyncio.to_thread(_work)
    except Exception as exc:  # noqa: BLE001
        app.state.vector_index_progress["status"] = "failed"
        app.state.vector_index_progress["error"] = str(exc)
        log.warning("vector index refresh thread failed: %s", exc)


async def _run_doc_tag_workflow(app: FastAPI) -> None:
    """ADR-0062 / ADR-0065 — periodic tick of the tag-driven workflow engine.

    Loads user-defined rules dynamically from ``tag_workflow_bindings``
    via ``load_runtime_rules``. An empty bindings table means the tick
    is a no-op — workflows do nothing until the user creates a binding
    via /settings/paperless-workflows (ADR-0065).

    Per-rule failures are caught so one bad rule cannot kill the
    scheduler thread.
    """
    settings: Settings = app.state.settings
    if not settings.paperless_configured:
        return
    if not getattr(settings, "paperless_workflow_enabled", True):
        return
    # Lazy import keeps the workflow module out of the cold path
    # of every other lifespan task.
    from lamella.features.paperless_bridge.tag_workflow import (
        load_runtime_rules,
        run_rule,
    )
    rules = load_runtime_rules(app.state.db)
    if not rules:
        # No user-defined bindings — nothing to do this tick.
        return
    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    try:
        for rule in rules:
            if rule.trigger != "scheduled":
                continue
            try:
                report = await run_rule(
                    rule, conn=app.state.db, paperless_client=client,
                )
                log.info(
                    "doc_tag_workflow rule=%s matched=%d "
                    "ok=%d anomaly=%d err=%d",
                    rule.name, report.docs_matched,
                    report.successes, report.anomalies, report.errors,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "doc_tag_workflow rule %r crashed: %s",
                    rule.name, exc,
                )
    finally:
        await client.aclose()


async def _run_namespace_migration(app: FastAPI) -> None:
    """ADR-0064 — one-time rename of legacy ``Lamella_X`` tags +
    custom fields in Paperless to canonical ``Lamella:X``. Idempotent
    once all legacy names are gone. Wrapped in try/except so a
    Paperless outage at boot never breaks startup; the next boot
    retries until a clean run, then flips the
    ``paperless_namespace_migration_completed`` setting and stops
    running on subsequent boots."""
    settings: Settings = app.state.settings
    if not settings.paperless_configured:
        return
    if getattr(settings, "paperless_namespace_migration_completed", False):
        return
    from lamella.features.paperless_bridge.namespace_migration import (
        run_namespace_migration,
    )
    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    try:
        report = await run_namespace_migration(client)
        log.info(
            "ADR-0064 migration: tags(in_place=%d, copy=%d), "
            "fields(in_place=%d, copy=%d), docs=%d, errors=%d",
            report.tags_renamed_in_place, report.tags_migrated_via_copy,
            report.fields_renamed_in_place, report.fields_migrated_via_copy,
            report.documents_retagged, len(report.errors),
        )
        if not report.errors:
            # Persist the completion flag so subsequent boots skip
            # the migration entirely. The settings store is the
            # canonical home; the in-process Settings object also
            # gets updated so the rest of the lifespan sees the
            # flipped value without a fresh boot.
            try:
                from lamella.core.settings.store import AppSettingsStore
                store = AppSettingsStore(
                    app.state.db,
                    connector_config_path=getattr(
                        settings, "connector_config_path", None,
                    ),
                    main_bean_path=settings.ledger_main,
                )
                store.set("paperless_namespace_migration_completed", "1")
                app.state.db.commit()
                settings.paperless_namespace_migration_completed = True
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "ADR-0064: migration succeeded but persisting the "
                    "completion flag failed; next boot will retry: %s", exc,
                )
        else:
            log.warning(
                "ADR-0064: migration completed with %d error(s); the "
                "completion flag stays unset and the migration will "
                "retry on next boot. errors=%s",
                len(report.errors), report.errors[:5],
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "ADR-0064: namespace migration crashed (non-fatal): %s", exc,
        )
    finally:
        await client.aclose()


async def _bootstrap_workflow_tags(app: FastAPI) -> None:
    """ADR-0062 — ensure the five canonical Lamella: tags exist in
    Paperless. Best-effort — if Paperless is unreachable at boot
    the next scheduled tick retries."""
    settings: Settings = app.state.settings
    if not settings.paperless_configured:
        return
    if not getattr(settings, "paperless_workflow_enabled", True):
        return
    from lamella.features.paperless_bridge.tag_workflow import (
        bootstrap_canonical_tags,
    )
    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    try:
        ensured = await bootstrap_canonical_tags(client)
        log.info(
            "doc_tag_workflow bootstrap: ensured %d canonical tag(s)",
            len(ensured),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "doc_tag_workflow bootstrap failed (non-fatal): %s", exc,
        )
    finally:
        await client.aclose()


async def _run_paperless_sync(app: FastAPI, *, full: bool = False) -> None:
    settings: Settings = app.state.settings
    if not settings.paperless_configured:
        return
    client = PaperlessClient(
        base_url=settings.paperless_url,  # type: ignore[arg-type]
        api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
        extra_headers=settings.paperless_extra_headers(),
    )
    try:
        sync = PaperlessSync(
            conn=app.state.db,
            client=client,
            lookback_days=settings.paperless_sync_lookback_days,
        )
        result = await sync.sync(full=full)
        if result.error:
            log.warning("paperless sync reported error: %s", result.error)
        else:
            log.info(
                "paperless sync ok (%s): seen=%d written=%d",
                result.mode, result.docs_seen, result.docs_written,
            )
    except Exception as exc:  # noqa: BLE001 — never let the scheduler die.
        log.warning("paperless sync crashed: %s", exc)
    finally:
        await client.aclose()


async def _run_simplefin_fetch(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    if (settings.simplefin_mode or "disabled").lower() == "disabled":
        return
    access = settings.simplefin_access_url
    if not access or not access.get_secret_value():
        log.info("simplefin: no access URL configured — skipping scheduled fetch")
        return
    # Setup-completeness gate. The HTTP middleware blocks AI surfaces
    # when setup_required_complete is False, but the scheduler runs
    # in a separate task that bypasses HTTP entirely. So we re-check
    # here: skip the fetch if entities, accounts, or charts aren't
    # locked down. The user's rule: "we can't classify into broken
    # or missing categories." Burning AI tokens on a misconfigured
    # ledger is exactly that.
    if not getattr(app.state, "setup_required_complete", False):
        log.warning(
            "simplefin: skipping scheduled fetch — setup is incomplete. "
            "Visit /setup/recovery and complete required steps before "
            "scheduled fetches resume."
        )
        return
    client = SimpleFINClient(access_url=access.get_secret_value())
    try:
        writer = SimpleFINWriter(
            main_bean=settings.ledger_main,
            simplefin_path=settings.simplefin_transactions_path,
        )
        ai = AIService(settings=settings, conn=app.state.db)
        ingest = SimpleFINIngest(
            conn=app.state.db,
            settings=settings,
            reader=app.state.ledger_reader,
            rules=RuleService(app.state.db),
            reviews=ReviewService(app.state.db),
            writer=writer,
            ai=ai,
        )
        try:
            ingest_result = await ingest.run(client=client, trigger="scheduled")
        except SimpleFINError as exc:
            log.warning("scheduled simplefin fetch failed: %s", exc)
        else:
            dispatcher: Dispatcher | None = getattr(app.state, "dispatcher", None)
            if dispatcher is not None and ingest_result.large_fixmes:
                from lamella.features.bank_sync.notify_hook import (
                    dispatch_large_fixmes,
                )

                await dispatch_large_fixmes(
                    dispatcher=dispatcher, result=ingest_result,
                )
            # Phase 6 — recurring monitor + budget evaluation after every fetch.
            try:
                from beancount.core.data import Transaction

                txns = [
                    e for e in app.state.ledger_reader.load(force=True).entries
                    if isinstance(e, Transaction)
                ]
                await monitor_after_ingest(
                    conn=app.state.db,
                    new_transactions=txns,
                    dispatcher=dispatcher,
                )
                await evaluate_and_alert(
                    conn=app.state.db,
                    dispatcher=dispatcher,
                    entries=app.state.ledger_reader.load().entries,
                    channels=_channels_from_setting(settings.budget_alert_channels),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("post-ingest budget/recurring hooks failed: %s", exc)
    finally:
        await client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    _configure_logging(settings.log_level)

    settings.data_dir.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_sqlite_filename(settings)
    db = connect(settings.db_path)
    migrate(db)
    app.state.db = db

    # ADR-0050 — auth bootstrap. Resolves the session-signing secret
    # (auto-generates and persists data_dir/.session-secret on first
    # run when AUTH_SESSION_SECRET is unset) and seeds a single user
    # from AUTH_USERNAME + AUTH_PASSWORD when the users table is empty.
    # Subsequent boots see the row already in place and ignore env
    # vars; password changes go through /account/password.
    from lamella.web.auth.bootstrap import (
        bootstrap_user,
        emit_exposure_warning_banner,
        resolve_session_secret,
    )
    app.state.auth_session_secret = resolve_session_secret(settings)
    try:
        bootstrap_user(db, settings)
    except Exception as exc:  # noqa: BLE001
        log.warning("auth bootstrap failed: %s", exc)
    emit_exposure_warning_banner(settings)

    # Generic job runner — every long-running handler in the app goes
    # through this so the browser gets a progress modal instead of a
    # 20-minute hang. One instance per process; short-lived per-worker
    # connections avoid contending with request threads on the main
    # connection's RLock.
    job_runner = JobRunner(db_path=settings.db_path)
    interrupted = job_runner.mark_interrupted_on_startup()
    if interrupted:
        log.info("job runner: marked %d leftover job(s) as interrupted", interrupted)
    app.state.job_runner = job_runner

    # Overlay editable settings (persisted in app_settings) on top of env vars.
    try:
        overrides = AppSettingsStore(db).all()
        if overrides:
            settings.apply_kv_overrides(overrides)
    except Exception as exc:
        log.warning("failed to apply app_settings overrides: %s", exc)

    # First-run detection: classify the ledger before any
    # ledger-consuming bootstrap runs. The middleware gates routes
    # on app.state.ledger_detection.needs_setup so the user is sent
    # to /setup instead of hitting empty or broken UI.
    detection = detect_ledger_state(settings.ledger_main)
    app.state.ledger_detection = detection
    if detection.needs_setup:
        log.warning(
            "ledger needs setup (state=%s); routes will redirect to /setup",
            detection.state.value,
        )

    reader = LedgerReader(settings.ledger_main)
    try:
        reader.load()
    except Exception as exc:  # defensive: bad ledger shouldn't block startup
        log.warning("initial ledger load failed: %s", exc)
    app.state.ledger_reader = reader

    # Registry discovery: additively insert any entity / vehicle / account
    # slugs present in the ledger but missing from our tables. Idempotent.
    try:
        entries = reader.load().entries
        sync_from_ledger(
            db, entries,
            simplefin_map_path=settings.simplefin_account_map_resolved,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("registry discovery sync failed: %s", exc)

    # Re-apply user-chosen `custom "account-kind"` overrides on top of
    # discovery's inferred kinds. The directive is the source of truth;
    # without this step, a DB wipe would revert every manual kind
    # choice to whatever the keyword heuristic guessed.
    try:
        from lamella.core.registry.kind_writer import apply_kind_overrides
        touched = apply_kind_overrides(db, reader.load().entries)
        if touched:
            log.info("accounts_meta: re-applied %d user kind overrides", touched)
    except Exception as exc:  # noqa: BLE001
        log.warning("account-kind override apply failed: %s", exc)

    # Self-heal companion accounts on boot. Every labeled account
    # should have its Schedule-C-compatible Interest / Bank:Fees /
    # Bank:Cashback / OpeningBalances / Transfers:InFlight accounts
    # open in the ledger — so classification never has to invent a
    # bespoke expense path. Runs once per boot, idempotent.
    #
    # Only touches LABELED accounts (kind + entity_slug present) so a
    # raw-imported ledger with no registry metadata doesn't get
    # spammed with pre-scaffold opens. Operation is safe to skip on
    # failure: per-account exceptions are swallowed so one bad row
    # doesn't block boot.
    try:
        from lamella.core.registry.companion_accounts import ensure_companions
        row_iter = db.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE closed_on IS NULL AND kind IS NOT NULL AND kind != '' "
            "AND entity_slug IS NOT NULL AND entity_slug != ''"
        ).fetchall()
        opened_count = 0
        for row in row_iter:
            try:
                opened = ensure_companions(
                    conn=db, settings=settings, reader=reader,
                    account_path=row["account_path"],
                )
                opened_count += len(opened)
            except Exception as per_row_exc:  # noqa: BLE001
                log.warning(
                    "ensure_companions failed for %s: %s",
                    row["account_path"], per_row_exc,
                )
        if opened_count:
            log.info(
                "companion_accounts: boot self-heal opened %d new account(s)",
                opened_count,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("companion-account boot self-heal skipped: %s", exc)

    # First-run / reconstruct detection. Three mutually-exclusive flags
    # on app.state drive the setup_gate middleware:
    #
    #   needs_reconstruct = ledger carries lamella-* markers + witness
    #       tables are empty → wizard at /setup/reconstruct
    #   needs_welcome = ledger has transactions but NO lamella-* markers +
    #       witness tables empty → onboarding at /setup/welcome
    #   (neither) = user is past setup; serve normally
    try:
        app.state.needs_reconstruct = _detect_needs_reconstruct(db, reader)
        app.state.needs_welcome = (
            False if app.state.needs_reconstruct
            else _detect_needs_welcome(db, reader)
        )
        if app.state.needs_reconstruct:
            log.warning(
                "ledger carries lamella-* markers and state tables are empty; "
                "routes will redirect to /setup/reconstruct"
            )
        elif app.state.needs_welcome:
            log.warning(
                "ledger has transactions but no lamella-* markers; "
                "routes will redirect to /setup/welcome"
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("first-run detection failed: %s", exc)
        app.state.needs_reconstruct = False
        app.state.needs_welcome = False

    # Setup-completeness gate: compute the boolean once at boot so the
    # middleware (and the SimpleFIN scheduled fetch, which bypasses
    # HTTP middleware) can short-circuit any AI-burning operations
    # until the user has labeled entities + accounts + scaffolded
    # required charts. SAFE DEFAULT: if the check raises, set to
    # False (gate ON) — better to over-block than burn tokens.
    try:
        from lamella.features.setup.setup_progress import (
            compute_setup_progress,
        )
        entries = list(reader.load().entries) if reader else []
        progress = compute_setup_progress(
            db, entries, imports_dir=settings.import_ledger_output_dir_resolved,
        )
        app.state.setup_required_complete = progress.required_complete
        if not progress.required_complete:
            log.warning(
                "setup-completeness gate ENABLED at boot — "
                "%d/%d required steps incomplete; scheduled SimpleFIN "
                "fetch + AI calls will be skipped until /setup/recovery "
                "shows green",
                progress.required_total - progress.required_done,
                progress.required_total,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "setup-progress compute failed; gate defaulting to ON: %s", exc,
        )
        app.state.setup_required_complete = False

    # Settings drift heal: the dual-write in AppSettingsStore swallows
    # BeanCheckError (correct for UX) so an unlucky save can leave a
    # setting DB-only and invisibly violate the reconstruct guarantee.
    # Every boot, compare SQLite settings against ledger directives and
    # restamp the gap. Failures are logged at WARN so they're loud in
    # the journal even though the page never 500s.
    try:
        from lamella.core.transform.settings_drift import restamp_missing

        restamp_missing(
            db,
            reader,
            connector_config_path=settings.connector_config_path,
            main_bean_path=settings.ledger_main,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("settings drift heal failed: %s", exc)

    # Phase 5 — notifications + upcoming.
    app.state.dispatcher = _build_dispatcher(app)

    def _rebuild_dispatcher() -> None:
        try:
            app.state.dispatcher = _build_dispatcher(app)
        except Exception as exc:  # noqa: BLE001
            log.warning("dispatcher rebuild failed: %s", exc)

    app.state._notify_rebuild = _rebuild_dispatcher

    # One-shot: import rows from the legacy vehicles.csv into
    # mileage_entries if the table is empty. After 032, the DB is
    # the primary store and the CSV is strictly a daily backup —
    # but existing deployments carrying only a CSV on disk still
    # need a way to populate the new table on first boot.
    try:
        _mileage_service = MileageService(
            conn=db, csv_path=settings.mileage_csv_resolved,
        )
        _mileage_service.bootstrap_from_csv_if_empty()
        # Even if the table isn't empty, re-link any entries whose
        # vehicle display name didn't previously match a known slug.
        # Vehicles discovered from the ledger on this boot may now
        # make entries that were unlinked before resolvable.
        linked = _mileage_service.link_unlinked_entries()
        if linked:
            log.info("mileage: linked %d entries to registered vehicle slugs", linked)
    except Exception as exc:  # noqa: BLE001
        log.warning("mileage CSV bootstrap failed: %s", exc)

    # Rebuild back-fill audit cache on every boot. Pure projection
    # from mileage_entries — cheap, one SQL pass, guarantees the
    # cache reflects current reality even if it was dropped /
    # skipped by an older deploy.
    try:
        from lamella.features.mileage.backfill_audit import (
            rebuild_mileage_backfill_audit,
        )
        n = rebuild_mileage_backfill_audit(db)
        if n:
            log.info("mileage backfill audit: %d date(s) cached", n)
    except Exception as exc:  # noqa: BLE001
        log.warning("mileage backfill audit rebuild failed: %s", exc)

    # Calendar: bootstrap the txn_classification_modified cache on
    # first boot after migration 044. Idempotent (the writer upserts
    # with MAX(modified_at)), but only runs when the cache is empty
    # so subsequent boots don't wipe live-bumped rows from writes
    # not yet reflected in the ledger file snapshot we're reading.
    try:
        cache_empty = db.execute(
            "SELECT COUNT(*) FROM txn_classification_modified"
        ).fetchone()[0] == 0
        if cache_empty:
            from lamella.features.calendar.classification_modified import (
                rebuild_from_entries,
            )
            from lamella.features.calendar.tz import app_tz as _app_tz
            n = rebuild_from_entries(
                db,
                reader.load().entries,
                tz_for_fallback=_app_tz(settings),
            )
            if n:
                log.info(
                    "calendar: bootstrapped %d txn_classification_modified row(s) from ledger",
                    n,
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("calendar cache bootstrap failed: %s", exc)

    scheduler = AsyncIOScheduler()
    # Classifier-related schedules:
    #
    # - FixmeScanner is event-driven (boot prime + post-ingest +
    #   post-classify-group). No interval.
    # - Bulk classify (full sweep) is user-triggered only.
    # - Context-gated trickle classify runs twice/day at 04:00 +
    #   16:00 (`_run_context_ready_classify`). Gated on direct
    #   evidence + a per-run cap, with pattern-from-neighbors
    #   short-circuit. See docs/specs/AI-CLASSIFICATION.md "Scheduling
    #   — context-gated trickle" for the gate criteria.
    # - Receipt auto-match sweep runs every 4 hours
    #   (`_run_receipt_auto_sweep`). Deterministic, no AI call.
    scheduler.add_job(
        _run_sqlite_backup,
        CronTrigger(hour=2, minute=0),
        args=[app],
        id="sqlite_backup",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_recurring_detection,
        CronTrigger(day_of_week="sun", hour=3, minute=0),
        args=[app],
        id="recurring_detection",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_budget_evaluation,
        IntervalTrigger(hours=6),
        args=[app],
        id="budget_evaluation",
        replace_existing=True,
        next_run_time=None,
    )
    scheduler.add_job(
        _run_business_cache_warmup,
        IntervalTrigger(minutes=10, jitter=60),
        args=[app],
        id="business_cache_warmup",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        next_run_time=datetime.now(UTC),
    )
    scheduler.add_job(
        _run_weekly_digest,
        CronTrigger(hour=9, minute=0),
        args=[app],
        id="weekly_digest",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_receipt_auto_sweep,
        IntervalTrigger(hours=4, jitter=300),
        args=[app],
        id="receipt_auto_sweep",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.add_job(
        _run_context_ready_classify,
        CronTrigger(hour="4,16", minute=0, jitter=300),
        args=[app],
        id="context_ready_classify",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )

    async def _paperless_sync_job() -> None:
        await _run_paperless_sync(app)

    # IntervalTrigger's default first-fire is `now + interval` —
    # that means a 6-hour interval doesn't sync until 6 hours after
    # boot. Set `next_run_time` to ~5 minutes from now so every
    # restart produces a fresh-ish sync without making the user
    # wait six hours. The startup-side incremental kick below
    # bridges the first 5 minutes.
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    scheduler.add_job(
        _paperless_sync_job,
        IntervalTrigger(hours=max(1, settings.paperless_sync_interval_hours)),
        id="paperless_sync",
        replace_existing=True,
        next_run_time=_dt.now(_tz.utc) + _td(minutes=5),
        coalesce=True,
        max_instances=1,
    )

    # ADR-0062 — tag-driven workflow engine. Scheduler ticks every
    # `paperless_workflow_interval_minutes` minutes (default 60).
    # max_instances=1 + coalesce=True per ADR-0062 §5: a long-running
    # rule cannot stack invocations. Wrapped in try/except so a
    # configuration glitch (e.g. unreachable Paperless at boot) does
    # NOT block app startup; the next tick retries.
    async def _doc_tag_workflow_job() -> None:
        await _run_doc_tag_workflow(app)

    if (
        settings.paperless_configured
        and getattr(settings, "paperless_workflow_enabled", True)
    ):
        try:
            workflow_minutes = max(
                1,
                int(getattr(settings, "paperless_workflow_interval_minutes", 60)),
            )
            scheduler.add_job(
                _doc_tag_workflow_job,
                IntervalTrigger(minutes=workflow_minutes),
                id="doc_tag_workflow",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                # First tick ~10 min after boot so the bootstrap +
                # any catch-up sync land before we evaluate selectors.
                next_run_time=_dt.now(_tz.utc) + _td(minutes=10),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "doc_tag_workflow scheduler registration failed: %s",
                exc,
            )

    async def _simplefin_job() -> None:
        await _run_simplefin_fetch(app)

    def _register_simplefin() -> None:
        register_simplefin(
            scheduler,
            interval_hours=app.state.settings.simplefin_fetch_interval_hours,
            mode=app.state.settings.simplefin_mode,
            callback=_simplefin_job,
        )

    app.state._simplefin_register = _register_simplefin
    _register_simplefin()

    scheduler.start()
    app.state.scheduler = scheduler

    # Prime the queue once at boot so users see FIXMEs immediately.
    _run_fixme_scan(app)

    # Background tasks spawned during startup must be tracked so the
    # lifespan teardown can cancel them. Without this, the test
    # suite under Windows hits an access violation: after a
    # TestClient context exits, its event loop is torn down while
    # vector_index._work() (heavy sentence-transformers + sqlite
    # walk on a worker thread) is still running, then pytest moves
    # to the next test which creates a fresh app + a new
    # connection, and the orphan worker hits freed resources from
    # the previous test. See: tests/test_setup_smoke.py crashes
    # at test ~#11 with a 49-deep merged_lifespan unwind in the
    # main thread + Thread doing `_work` on the prior test's
    # SQLite handle.
    import asyncio as _asyncio
    _bg_tasks: list[_asyncio.Task] = []

    # Kick a Paperless sync at startup whenever Paperless is
    # configured. Was gated on "index is empty" but the user
    # observed scheduled syncs feel non-existent — so we always run
    # an incremental sync at boot to pull anything new since the
    # last process exited. ``full=True`` only when the index is
    # empty (initial seeding); incremental otherwise to keep boot
    # cheap. Non-blocking; fires in the background.
    if settings.paperless_configured:
        try:
            state_row = db.execute(
                "SELECT doc_count FROM paperless_sync_state WHERE id = 1"
            ).fetchone()
            empty = not state_row or int(state_row["doc_count"] or 0) == 0
        except Exception:
            empty = True
        # ADR-0064 — one-time namespace migration must run BEFORE the
        # bootstrap so the bootstrap finds canonical tags and skips
        # creation. Skipped entirely after the first successful run
        # (settings flag flips to True). Best-effort: a Paperless
        # outage at boot leaves the flag unset, so the migration
        # retries on next boot.
        if not getattr(
            settings, "paperless_namespace_migration_completed", False
        ):
            _bg_tasks.append(
                _asyncio.create_task(_run_namespace_migration(app))
            )
        _bg_tasks.append(
            _asyncio.create_task(_run_paperless_sync(app, full=empty))
        )
        # ADR-0062 — fire-and-forget bootstrap of the canonical
        # Lamella: state tags. Idempotent; safe on every boot.
        if getattr(settings, "paperless_workflow_enabled", True):
            _bg_tasks.append(
                _asyncio.create_task(_bootstrap_workflow_tags(app))
            )

    # Vector index: kick a background freshness check at startup.
    # Reads the current ledger, computes the live signature, and
    # if it's stale OR the index has never been built, rebuilds.
    # Runs in a task so it doesn't delay app startup — first
    # classify calls may fall back to substring until the rebuild
    # finishes, and will pick up the fresh index on subsequent
    # calls.
    if settings.ai_vector_search_enabled:
        _bg_tasks.append(
            _asyncio.create_task(_vector_index_refresh(app))
        )

    app.state.last_webhook_at = None

    try:
        yield
    finally:
        # Cancel + await the background tasks first so any worker
        # threads they spawned (asyncio.to_thread for the vector
        # index build) get a chance to wind down before SQLite is
        # closed. Tests reuse the same event loop across cases via
        # pytest-asyncio's session loop, so an orphan task from one
        # test gets re-attached to the next test's loop teardown
        # and crashes when its underlying conn is gone — that's the
        # Windows access-violation we saw in test_setup_smoke.py.
        for task in _bg_tasks:
            if not task.done():
                task.cancel()
        if _bg_tasks:
            try:
                await _asyncio.gather(*_bg_tasks, return_exceptions=True)
            except Exception:  # noqa: BLE001
                log.warning("background task drain failed", exc_info=True)
        scheduler.shutdown(wait=False)
        try:
            job_runner.shutdown()
        except Exception:  # noqa: BLE001
            log.warning("job runner shutdown failed", exc_info=True)
        dispatcher: Dispatcher | None = getattr(app.state, "dispatcher", None)
        if dispatcher is not None:
            try:
                await dispatcher.aclose()
            except Exception:  # noqa: BLE001
                log.warning("dispatcher aclose failed", exc_info=True)
        db.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(
        title="Lamella",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.version = __version__

    templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
    templates.env.globals["app_version"] = __version__
    # Per-process-start cache-bust token for static assets. We never
    # bump __version__ across normal deploys, so using it in static
    # URLs produces permanently-stable cache keys and the browser /
    # service worker happily keep serving yesterday's htmx shim
    # after a code change. Process-start time changes every time
    # the app restarts, which IS the granularity of a deploy —
    # CI builds a new image, the container comes up, new token,
    # cache busts. Cheap to compute, updates automatically.
    templates.env.globals["static_version"] = str(int(datetime.now(UTC).timestamp()))

    # Number formatting helper. Reads settings.number_locale at every
    # call so a runtime locale flip from /settings takes effect on the
    # next page render, no restart needed. Two locales today:
    #   en_US  → 1,234.56
    #   en_EU  → 1.234,56
    def _fmt_amount(value, decimals: int = 2) -> str:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return ""
        loc = getattr(app.state.settings, "number_locale", "en_US") if (
            hasattr(app, "state") and getattr(app.state, "settings", None)
        ) else "en_US"
        s = f"{v:,.{decimals}f}"
        if loc == "en_EU":
            # Swap via temporary placeholders so we don't double-replace.
            s = s.replace(",", "").replace(".", ",").replace("", ".")
        return s
    templates.env.globals["fmt_amount"] = _fmt_amount

    # Sidebar businesses quick-jump global. Registered as a callable so
    # templates pick up freshly-added entities without an app restart.
    # The base template hides the whole group when fewer than 2 active
    # entities exist (single-entity / personal-only mode).
    def _sidebar_entities():
        import hashlib
        from lamella.core.registry.service import list_entities

        conn = getattr(app.state, "db", None)
        if conn is None:
            return []
        try:
            ents = list_entities(conn, include_inactive=False)
        except sqlite3.Error:
            return []
        out = []
        for e in ents:
            display = e.display_name or e.slug
            # Stable hue from slug hash so each entity gets a consistent
            # color across sessions; saturation/lightness chosen to read
            # well on both light and dark sidebar backgrounds.
            h = int(hashlib.md5(e.slug.encode()).hexdigest(), 16) % 360
            entity_type = (e.entity_type or "").strip().lower()
            kind = "personal" if entity_type == "personal" else "business"
            out.append({
                "slug": e.slug,
                "name": display,
                "color": f"hsl({h}, 62%, 52%)",
                "letter": (display[:1] or "?").upper(),
                "entity_type": entity_type,
                "kind": kind,
            })
        return out

    templates.env.globals["sidebar_entities"] = _sidebar_entities

    # Canonical entity URL builder. Returns ``/personal/{slug}`` for
    # entities with entity_type == "personal" and ``/businesses/{slug}``
    # for everything else. Centralized here so templates rendering a
    # link back to an entity's dashboard don't hardcode /businesses/.
    # Handles both dict-shaped (sqlite Row → dict) and object-shaped
    # entities so callers can pass either.
    def _entity_url(entity, suffix: str = ""):
        if entity is None:
            return suffix or "/"
        if isinstance(entity, dict):
            entity_type = entity.get("entity_type")
            slug = entity.get("slug")
        else:
            entity_type = getattr(entity, "entity_type", None)
            slug = getattr(entity, "slug", None)
        is_personal = (entity_type or "").strip().lower() == "personal"
        prefix = "/personal" if is_personal else "/businesses"
        return f"{prefix}/{slug}{suffix}" if slug else prefix

    templates.env.globals["entity_url"] = _entity_url

    # ADR-0058 — duplicate-review queue depth. Surfaces in the sidebar
    # so the user sees "you have N likely duplicates to verify" without
    # having to remember the URL. Cheap (one indexed status filter); the
    # base template hides the nav item entirely when count is 0.
    def _likely_duplicates_count() -> int:
        conn = getattr(app.state, "db", None)
        if conn is None:
            return 0
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM staged_transactions "
                "WHERE status = 'likely_duplicate'"
            ).fetchone()
        except sqlite3.Error:
            return 0
        return int(row["n"]) if row else 0

    templates.env.globals[
        "likely_duplicates_count"
    ] = _likely_duplicates_count

    # Footer health pill + sync labels. Cheap aggregates the base template
    # surfaces in the docked footer so every page shows a quick-glance
    # answer to "is the system healthy and recently fresh?".
    def _ledger_status():
        # 'clean' / 'warn' / 'err' for the pulse badge. We trust the
        # detection that ran at lifespan startup — re-running bean-check
        # on every page render would be expensive on a multi-thousand-txn
        # ledger, and the actual surfaces that mutate the ledger
        # (overrides, reboot ingest, etc.) refresh detection themselves.
        d = getattr(app.state, "ledger_detection", None)
        if d is None:
            return "clean"
        if getattr(d, "parse_errors", None):
            return "err"
        if getattr(d, "needs_setup", False):
            return "warn"
        return "clean"

    def _last_sync_label():
        conn = getattr(app.state, "db", None)
        if conn is None:
            return ""
        try:
            row = conn.execute(
                "SELECT MAX(finished_at) FROM simplefin_ingest_runs WHERE status = 'ok'"
            ).fetchone()
        except sqlite3.Error:
            return ""
        if not row or not row[0]:
            return ""
        # Render as "synced 2h ago" — small enough to fit, big enough
        # to spot a stale fetch at a glance.
        try:
            from datetime import datetime, timezone
            ts = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - ts
            secs = int(delta.total_seconds())
            if secs < 60:
                return "synced just now"
            if secs < 3600:
                return f"synced {secs // 60}m ago"
            if secs < 86400:
                return f"synced {secs // 3600}h ago"
            return f"synced {secs // 86400}d ago"
        except (ValueError, TypeError):
            return ""

    def _receipts_indexed():
        conn = getattr(app.state, "db", None)
        if conn is None:
            return None
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM paperless_doc_index"
            ).fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return None
        n = row[0] or 0
        return f"{n:,}" if n else None

    templates.env.globals["ledger_status"] = _ledger_status
    templates.env.globals["last_sync_label"] = _last_sync_label
    templates.env.globals["receipts_indexed"] = _receipts_indexed

    def _reboot_ingest_enabled() -> bool:
        # Kill switch surfaced via env LAMELLA_REBOOT_INGEST_ENABLED.
        # Used by data_integrity.html to decide whether to render the
        # destructive-warning banner instead of the action buttons.
        s = getattr(app.state, "settings", None)
        return bool(getattr(s, "reboot_ingest_enabled", False)) if s else False

    templates.env.globals["reboot_ingest_enabled"] = _reboot_ingest_enabled

    # Registry filters. We bind to the (not-yet-populated) app.state so the
    # filter reads the live db connection at render time, not at filter
    # registration time. During very early boot (before lifespan has run)
    # the filter falls back to the raw value.
    def _alias_filter(value):
        conn = getattr(app.state, "db", None)
        if conn is None or value is None:
            return "" if value is None else str(value)
        try:
            return alias_for(conn, str(value))
        except sqlite3.Error as exc:
            # A template filter must never 500 the response — the
            # shared-connection lock in db.py should prevent the
            # concurrency races that caused this, but if anything
            # else fails (closed connection during shutdown, disk
            # error, etc.), fall back to the raw path and log.
            log.warning("alias filter failed for %r: %s", value, exc)
            return str(value)

    def _alias_label_filter(value):
        conn = getattr(app.state, "db", None)
        if conn is None or value is None:
            return ("", "")
        try:
            return account_label(conn, str(value))
        except sqlite3.Error as exc:
            log.warning("alias_label filter failed for %r: %s", value, exc)
            return (str(value), "")

    def _money_filter(value, currency: str | None = None):
        # Wrap the formatted string in <span class="money money--{tone}">
        # so refunds (positive-sign reversal of an expense leg) get the
        # money--neg color class and visually distinguish from charges.
        # Without this, a refund like "-$3.49" and a charge like "$54.72"
        # both read as plain dim text. Returning Markup means Jinja
        # won't HTML-escape the span. Empty / unparseable values fall
        # through to the raw formatter and skip the wrap so we don't
        # tag malformed input with a tone class.
        from decimal import Decimal as _D, InvalidOperation as _IO
        from markupsafe import Markup, escape
        formatted = format_money(value, currency)
        if not formatted:
            return formatted
        try:
            d = value if isinstance(value, _D) else _D(str(value))
        except (_IO, ValueError, TypeError):
            return formatted
        tone = 'pos' if d > 0 else ('neg' if d < 0 else 'zero')
        return Markup(f'<span class="money money--{tone} num">{escape(formatted)}</span>')

    def _entity_filter(slug):
        """Resolve an entity slug to its display_name, falling back to
        the slug itself when no display_name is set or the lookup
        fails. Use anywhere a user-facing label needs the human name —
        ``{{ entity_slug | entity }}`` — without forcing the route to
        also pass an ``entity_display_name``."""
        conn = getattr(app.state, "db", None)
        if conn is None or slug is None:
            return "" if slug is None else str(slug)
        try:
            from lamella.core.registry.alias import entity_label
            return entity_label(conn, str(slug))
        except sqlite3.Error as exc:
            log.warning("entity filter failed for %r: %s", slug, exc)
            return str(slug)

    def _local_ts_filter(value, with_seconds: bool = False, fmt: str | None = None):
        from lamella.web.temporal import render_local_ts

        return render_local_ts(
            value,
            tz_name=getattr(app.state.settings, "app_tz", "UTC"),
            with_seconds=with_seconds,
            fmt=fmt,
        )

    templates.env.filters["alias"] = _alias_filter
    templates.env.filters["alias_label"] = _alias_label_filter
    templates.env.filters["money"] = _money_filter
    templates.env.filters["entity"] = _entity_filter
    templates.env.filters["local_ts"] = _local_ts_filter

    # ─── humanize ─────────────────────────────────────────────────
    # Convert internal slug values (snake_case) into pretty
    # display labels. Used everywhere we render account kinds,
    # source names, or any other internal identifier — `credit_card`
    # → `Credit Card`, `line_of_credit` → `Line of Credit`,
    # `tax_liability` → `Tax Liability`. Explicit map for known
    # values (so common short words stay lowercase, acronyms stay
    # uppercase); generic fallback for anything not in the map.
    _HUMANIZE_LABELS = {
        # Account kinds (registry/service.py)
        "checking": "Checking",
        "savings": "Savings",
        "credit_card": "Credit Card",
        "line_of_credit": "Line of Credit",
        "loan": "Loan",
        "tax_liability": "Tax Liability",
        "brokerage": "Brokerage",
        "cash": "Cash",
        "money_market": "Money Market",
        "hsa": "HSA",
        "asset": "Asset",
        "virtual": "Virtual",
        "payout": "Payout",
        # Sources / connectors
        "simplefin": "SimpleFIN",
        "paperless": "Paperless",
        "openrouter": "OpenRouter",
        # Status / state values
        "new": "New",
        "classified": "Classified",
        "matched": "Matched",
        "promoted": "Promoted",
        "dismissed": "Dismissed",
        "failed": "Failed",
        "pending": "Pending",
        # Entity types — ENTITY_TYPES enum in registry/entity_structure.py
        # is internally lowercased/snake_cased; the user-facing form uses
        # the legal-style abbreviations (LLC stays uppercase, S-Corp gets
        # the hyphen). title() over "llc" / "s_corp" produces the wrong
        # shape, so they're explicitly mapped here.
        "llc": "LLC",
        "s_corp": "S-Corp",
        "c_corp": "C-Corp",
        "sole_proprietorship": "Sole Proprietorship",
        "partnership": "Partnership",
        "personal": "Personal",
        "trust": "Trust",
        "estate": "Estate",
        "nonprofit": "Nonprofit",
        "skip": "Skipped",
        # Common short-words that title() would over-capitalize
        # (handled in the fallback path).
    }
    _LOWERCASE_SHORT = frozenset({"of", "and", "or", "the", "a", "an", "in", "on", "to"})

    def _humanize_filter(value):
        if value is None:
            return ""
        s = str(value).strip()
        if not s:
            return ""
        if s in _HUMANIZE_LABELS:
            return _HUMANIZE_LABELS[s]
        # Generic: replace _, title-case, then lowercase short joining
        # words for a more natural reading.
        parts = s.replace("_", " ").split()
        out = []
        for i, w in enumerate(parts):
            if i > 0 and w.lower() in _LOWERCASE_SHORT:
                out.append(w.lower())
            else:
                out.append(w.capitalize())
        return " ".join(out)

    templates.env.filters["humanize"] = _humanize_filter

    # ─── ADR-0041 display-name globals ────────────────────────────
    # Three Jinja globals so templates can call display helpers inline
    # without piping.  All three read from app.state.db at render time
    # (not at registration time) so they see the live connection.

    # 1. humanize_slug — same logic as the |humanize filter, exposed
    #    as a callable global: {{ humanize_slug(value) }}
    templates.env.globals["humanize_slug"] = _humanize_filter

    # 2. display_entity(slug) — entity slug → display_name.
    #    Wraps the existing entity_label helper from alias.py which
    #    already does the DB lookup + slug fallback.
    def _display_entity_global(slug):
        conn = getattr(app.state, "db", None)
        if conn is None or slug is None:
            return "" if slug is None else _humanize_filter(str(slug))
        try:
            from lamella.core.registry.alias import entity_label
            return entity_label(conn, str(slug))
        except sqlite3.Error as exc:
            log.warning("display_entity global failed for %r: %s", slug, exc)
            return _humanize_filter(str(slug))

    templates.env.globals["display_entity"] = _display_entity_global

    # 3. display_account(path) — account path → human label.
    #    Wraps alias_for (alias.py) which checks accounts_meta.display_name
    #    first and falls back to the heuristic camel-split renderer.
    def _display_account_global(path):
        conn = getattr(app.state, "db", None)
        if conn is None or path is None:
            return "" if path is None else str(path)
        try:
            return alias_for(conn, str(path))
        except sqlite3.Error as exc:
            log.warning("display_account global failed for %r: %s", path, exc)
            return str(path)

    templates.env.globals["display_account"] = _display_account_global

    app.state.templates = templates

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # ADR-0050 — auth + security middleware. Mounted FIRST so an
    # unauthenticated request never reaches a setup-detection branch
    # and never sees ledger state on its way to /login. The middleware
    # is a no-op when no users exist (request.state.user falls back to
    # ANONYMOUS_OWNER / account_id=1) so first-run setup still works.
    from lamella.web.auth import middleware as _auth_middleware
    from lamella.web.auth import routes as _auth_routes
    _auth_middleware.install(app)
    app.include_router(_auth_routes.router)

    app.include_router(health.router)
    app.include_router(setup_route.router)
    # First-run wizard — registered alongside the maintenance setup
    # router. Mounting before the setup_gate middleware definition
    # means routes under /setup/wizard/** are exempt from the gate
    # via the existing /setup/* prefix exemption.
    app.include_router(setup_wizard_route.router)
    # Recovery: /setup/legacy-paths (Phase 3 of /setup/recovery).
    # Mounted before the setup_gate middleware definition so routes
    # under /setup/legacy-paths are exempt via the /setup/* prefix.
    app.include_router(setup_legacy_paths_route.router)
    # Recovery: /setup/recovery/schema (Phase 5 of /setup/recovery).
    # Same /setup/* exemption rule — mounted before setup_gate.
    app.include_router(setup_schema_route.router)
    # Recovery: /setup/recovery (Phase 6 bulk-review). Mounted in
    # the same /setup/* exemption window as the per-category pages.
    app.include_router(setup_recovery_route.router)
    app.include_router(dashboard.router)

    @app.middleware("http")
    async def setup_gate(request, call_next):
        """Redirect non-exempt routes to /setup when the ledger needs it.

        Exempt: /setup/**, /static/**, /webhooks/**, /healthz, /readyz.
        Webhooks are machine-to-machine callbacks — they expect a
        proper HTTP status, not an HTML redirect. Everything else is
        gated until detection reports a dashboard-ready ledger.
        The detection result lives on app.state and is refreshed by
        setup_scaffold after a successful write.

        Three redirect conditions, checked in order:
          1. ledger_detection.needs_setup → /setup (missing/broken/empty ledger)
          2. needs_reconstruct → /setup/reconstruct (lamella-managed ledger,
             empty DB — fresh install / deleted DB against our own ledger)
          3. needs_welcome → /setup/welcome (raw Beancount files with
             transactions but no lamella-* markers — user brought in a
             ledger from elsewhere and hasn't onboarded yet)
        """
        path = request.url.path
        exempt = (
            path.startswith("/setup")
            or path.startswith("/static")
            or path.startswith("/webhooks")
            or path in ("/healthz", "/readyz", "/login", "/logout", "/favicon.ico")
        )
        if exempt:
            return await call_next(request)

        # AJAX / polling endpoints must not be redirected to a full
        # HTML page. base.html embeds a globally-polled job dock
        # (#job-dock, hx-get="/jobs/active/dock", hx-trigger="every
        # 5s") on every page that extends it — including /setup and
        # /setup/recovery. If the gate redirects /jobs/active/dock
        # to /setup/wizard/welcome (303), HTMX follows and swaps the
        # wizard HTML into the dock; since base.html itself is
        # served on /setup, the wizard's content keeps appending
        # (the swap target is a child element on a long-lived page).
        # Fix: pass these prefixes through to their real handlers so
        # the dock returns its empty-state partial, not a full page.
        # The setup-completeness gate further down already
        # whitelists these for the post-scaffold setup flow; we
        # mirror that allowlist here for the pre-scaffold case.
        ajax_passthrough = (
            "/jobs",
            "/api",
            "/search/palette.json",
        )
        is_htmx = request.headers.get("hx-request", "").lower() == "true"
        if is_htmx or any(
            path == p or path.startswith(p + "/") for p in ajax_passthrough
        ):
            return await call_next(request)

        detection = getattr(request.app.state, "ledger_detection", None)
        if detection is not None and detection.needs_setup:
            from fastapi.responses import RedirectResponse as _Redirect
            # All redirects funnel through /setup. That page is the
            # single place that decides what to render: the first-run
            # wizard for a truly fresh install, the import analyzer
            # for foreign-ledger imports, the repair flow for broken
            # ledgers, or the maintenance checklist for drift. The
            # middleware doesn't need to know which scenario applies
            # — it just signals "ledger isn't ready, go to /setup."
            return _Redirect("/setup", status_code=303)
        if getattr(request.app.state, "needs_reconstruct", False):
            from fastapi.responses import RedirectResponse as _Redirect
            return _Redirect("/setup/reconstruct", status_code=303)
        if getattr(request.app.state, "needs_welcome", False):
            from fastapi.responses import RedirectResponse as _Redirect
            return _Redirect("/setup/welcome", status_code=303)
        # Setup-completeness gate: the user's stated rule is
        # "we can't classify into broken or missing categories."
        # When required steps (entities labeled, accounts labeled,
        # charts scaffolded) aren't done, block the AI + dashboard +
        # bulk-operation surfaces. Allow the settings pages so the
        # user can fix things, the accounts page so they can see
        # what's unlabeled, the api endpoints that power dropdowns,
        # and the /jobs/* polling a background task might need.
        if not getattr(request.app.state, "setup_required_complete", True):
            allowed_prefixes = (
                "/settings",
                "/accounts",
                "/api",
                "/jobs",
                "/ai/audit",       # safe to inspect prior decisions
                "/ai/decisions",   # same
                "/ai/cost",        # same
                "/simplefin",      # user may be setting it up
                "/settings/entities",
                # /setup/* pages link into these editors (e.g. "Set
                # owning entity" on /setup/vehicles → /vehicles/<slug>/edit).
                # Blocking them here strands the user on /setup/recovery
                # when they're trying to finish setup. Safe to allow:
                # they're editors, not AI or bulk-op surfaces.
                "/vehicles",
                "/vehicle-templates",
                "/mileage",
                "/projects",
                "/budgets",
                "/note",
                "/recurring",
            )
            if not any(path.startswith(p) or path == p for p in allowed_prefixes):
                from fastapi.responses import RedirectResponse as _Redirect
                # Funnel through /setup, not /setup/recovery. /setup
                # is the single decision point — it forwards to the
                # first-run wizard for an unconfigured install
                # (post-scaffold but pre-entity), or to /setup/recovery
                # for drift on an already-configured one. The
                # middleware doesn't need to know which.
                return _Redirect("/setup", status_code=303)
        return await call_next(request)
    app.include_router(businesses_route.router)
    app.include_router(note.router)
    app.include_router(card_route.router)
    app.include_router(api_txn_route.router)
    app.include_router(inbox_route.router)
    app.include_router(review.router)
    app.include_router(documents_needed_route.router)
    app.include_router(documents.router)
    app.include_router(txn_document_route.router)
    app.include_router(dangling_documents_route.router)
    # ADR-0061 §6: legacy /receipts/* and /txn/{token}/receipt-* paths
    # 308-redirect to their /documents/* and /txn/{token}/document-*
    # equivalents. Mounted AFTER the new routers so the new paths win
    # on ambiguity.
    from lamella.web.routes import _legacy_redirects
    app.include_router(_legacy_redirects.router)
    app.include_router(paperless_fields_route.router)
    app.include_router(paperless_proxy_route.router)
    app.include_router(paperless_verify_route.router)
    app.include_router(paperless_writebacks_route.router)
    app.include_router(paperless_anomalies_route.router)
    app.include_router(paperless_workflows_route.router)
    app.include_router(paperless_workflows_settings_route.router)
    app.include_router(payout_sources_route.router)
    app.include_router(status_route.router)
    app.include_router(setup_check_route.router)
    app.include_router(account_descriptions_route.router)
    app.include_router(audit_route.router)
    # Slug-availability API used by add-modal client-side validators.
    app.include_router(slug_api_route.router)
    app.include_router(projects_route.router)
    app.include_router(entities_route.router)
    # balances must come BEFORE accounts_admin: accounts_admin has a
    # @router.post("/settings/accounts/{account_path:path}") catchall
    # that would otherwise greedily match our /…/balances POST.
    app.include_router(balances_route.router)
    # accounts_browse provides the new /accounts browse + detail UX.
    # The /accounts/{path}/balance-chart.json + /accounts/{path}/edit
    # routes are defined BEFORE the catch-all /accounts/{path} in that
    # module so the more-specific ones win.
    app.include_router(accounts_browse_route.router)
    app.include_router(accounts_admin_route.router)
    app.include_router(vehicles_route.router)
    app.include_router(loans_route.router)
    app.include_router(loans_backfill_route.router)
    # Importing the loans_wizard router triggers _flows_ registry
    # population via the flow modules below. The order of these
    # imports doesn't matter; the dispatcher reads FLOW_REGISTRY at
    # request time.
    from lamella.features.loans.wizard import purchase as _purchase_flow_mod  # noqa: F401
    from lamella.features.loans.wizard import import_existing as _import_existing_flow_mod  # noqa: F401
    from lamella.features.loans.wizard import refi as _refi_flow_mod  # noqa: F401
    from lamella.features.loans.wizard import payoff as _payoff_flow_mod  # noqa: F401
    app.include_router(loans_wizard_route.router)
    app.include_router(properties_route.router)
    app.include_router(backups_route.router)
    app.include_router(rewrite_route.router)
    app.include_router(teach_route.router)
    app.include_router(search_route.router)
    app.include_router(transactions_route.router)
    app.include_router(accounts_route.router)
    app.include_router(rules.router)
    # intercompany_route owns explicit `/reports/intercompany`. It must
    # be included BEFORE reports.router because reports.router has a
    # catchall ``/reports/{entity_slug}`` that would otherwise shadow
    # any sibling `/reports/<fixed-name>` declared in another router.
    app.include_router(intercompany_route.router)
    app.include_router(reports.router)
    app.include_router(webhooks.router)
    app.include_router(settings_route.router)
    app.include_router(ai_route.router)
    app.include_router(simplefin_route.router)
    app.include_router(mileage_route.router)
    app.include_router(notifications_route.router)
    app.include_router(budgets_route.router)
    app.include_router(recurring_route.router)
    app.include_router(calendar_route.router)
    app.include_router(import_route.router)
    app.include_router(intake_route.router)
    app.include_router(data_integrity_route.router)
    app.include_router(staging_review_route.router)
    app.include_router(review_duplicates_route.router)
    app.include_router(imports_archive_route.router)
    app.include_router(jobs_route.router)

    # Emergency-mode HTML error pages for browser navigation.
    # JSON / HTMX / API callers keep the default FastAPI response —
    # only HTML page-loads see the styled error.html template.
    # Register on Starlette's HTTPException so unmatched routes (which
    # Starlette raises 404 for, before FastAPI sees them) also use it.
    from starlette.exceptions import HTTPException as _StarHTTPExc
    from fastapi.responses import JSONResponse as _JSONResponse
    from fastapi.requests import Request as _Request

    _STATUS_LABELS = {
        400: "Bad request",
        401: "Authentication required",
        403: "Forbidden",
        404: "Page not found",
        405: "Method not allowed",
        409: "Conflict",
        410: "Gone",
        413: "Payload too large",
        422: "Unprocessable entity",
        429: "Too many requests",
        500: "Server error",
        502: "Bad gateway",
        503: "Service unavailable",
        504: "Gateway timeout",
    }

    def _wants_html(request: _Request) -> bool:
        # HTMX swaps and JSON clients keep their original error shape.
        # Real browsers always send Accept containing text/html for
        # top-level navigation, so an explicit text/html match is the
        # right HTML signal. Empty/star Accept (TestClient defaults,
        # API tooling) goes to JSON so we don't break httpx callers
        # that expect r.json().
        if request.headers.get("HX-Request"):
            return False
        accept = request.headers.get("accept", "")
        return "text/html" in accept

    def _htmx_error_toast(message: str, *, status_code: int) -> HTMLResponse:
        # Toast that lands in #toast-area regardless of the original
        # hx-target. We use the out-of-band swap (`hx-swap-oob`) form
        # because that is universally supported by both real htmx.org
        # and our local shim, whereas HX-Retarget/HX-Reswap response
        # headers are not honored by the shim. Sending the OOB element
        # as the entire response body means the shim's primary-target
        # swap receives nothing visible (the wrapper div) after OOB
        # extraction. Headers kept too as defense-in-depth for any
        # client that does honor them.
        from html import escape as _esc
        from fastapi.responses import HTMLResponse as _HTMLResp
        label = _STATUS_LABELS.get(status_code, "Request failed")
        body = (
            f'<div id="toast-area" hx-swap-oob="innerHTML">'
            f'<div class="toast error" role="alert">'
            f'<strong>{label} · {status_code}</strong>'
            f'<span>{_esc(message)}</span>'
            f'</div>'
            f'</div>'
        )
        return _HTMLResp(
            body,
            status_code=status_code,
            headers={
                "HX-Retarget": "#toast-area",
                "HX-Reswap": "innerHTML",
            },
        )

    @app.exception_handler(_StarHTTPExc)
    async def _http_exc_handler(request: _Request, exc: _StarHTTPExc):  # type: ignore[unused-ignore]
        if request.headers.get("HX-Request"):
            # HTMX clients get a visible toast routed to #toast-area
            # rather than JSON dumped wherever the action was targeted.
            return _htmx_error_toast(
                str(exc.detail or _STATUS_LABELS.get(exc.status_code, "Request failed")),
                status_code=exc.status_code,
            )
        if not _wants_html(request):
            return _JSONResponse({"detail": exc.detail}, status_code=exc.status_code, headers=getattr(exc, "headers", None) or {})
        ctx = {
            "status_code": exc.status_code,
            "short_message": _STATUS_LABELS.get(exc.status_code, "Something went wrong"),
            "detail": exc.detail if exc.detail and str(exc.detail) != _STATUS_LABELS.get(exc.status_code, "") else "",
            "traceback": "",
        }
        return templates.TemplateResponse(request, "error.html", ctx, status_code=exc.status_code)

    @app.exception_handler(Exception)
    async def _unhandled_exc_handler(request: _Request, exc: Exception):
        # Always log the full traceback so the server-side record exists
        # for diagnosis. Previous code only formatted it for verbose mode,
        # which left the operator blind on every 500.
        log.exception(
            "unhandled exception in %s %s: %s",
            request.method, request.url.path, exc,
        )
        import traceback as _tb
        if request.headers.get("HX-Request"):
            return _htmx_error_toast(
                f"{type(exc).__name__}: {exc}"[:200],
                status_code=500,
            )
        if not _wants_html(request):
            return _JSONResponse({"detail": "internal server error"}, status_code=500)
        verbose = (get_settings().log_level or "").upper() == "DEBUG"
        tb = _tb.format_exc() if verbose else ""
        ctx = {
            "status_code": 500,
            "short_message": _STATUS_LABELS[500],
            "detail": str(exc) if verbose else "An unexpected error occurred. Check the server logs for details.",
            "traceback": tb,
        }
        return templates.TemplateResponse(request, "error.html", ctx, status_code=500)

    return app


app = create_app()


def run() -> None:
    import uvicorn

    settings = get_settings()
    # ADR-0050 — bind respects settings.host (defaults to 127.0.0.1 for
    # non-Docker runs; the docker-entrypoint.sh sets HOST=0.0.0.0
    # explicitly because the container's network namespace is isolated).
    uvicorn.run(
        "lamella.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()
