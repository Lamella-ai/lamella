# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from lamella.core.beancount_io import LedgerReader, entity_balances
from lamella.features.budgets.progress import progress_for_budget
from lamella.features.budgets.service import BudgetService
from lamella.core.config import Settings
from lamella.web.deps import (
    get_db,
    get_ledger_reader,
    get_note_service,
    get_review_service,
    get_settings,
)
from lamella.features.import_.service import OPEN_STATES as IMPORT_OPEN_STATES
from lamella.features.import_.staging import count_pending_items
from lamella.features.notes.service import NoteService
from lamella.features.recurring.service import RecurringService, RecurringStatus
from lamella.features.dashboard.service import activity_summary, money_groups
from lamella.features.review_queue.service import ReviewService

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True)
class _UpcomingCard:
    """Read-side projection of a confirmed recurring expense for the
    dashboard. Replaces Phase 5's UpcomingExpense (which the predictor
    used to compute on the fly)."""
    label: str
    source_account: str
    expected_date: date
    expected_amount: Decimal
    cadence: str
    overdue: bool


def _last_simplefin_ingest(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        """
        SELECT started_at, new_txns, duplicate_txns, fixme_txns, bean_check_ok, error
          FROM simplefin_ingests
         ORDER BY id DESC
         LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    started = row["started_at"]
    started_at: datetime | None = None
    if isinstance(started, datetime):
        started_at = started
    elif isinstance(started, str):
        try:
            started_at = datetime.fromisoformat(started)
        except ValueError:
            started_at = None
    return {
        "started_at": started_at,
        "new_txns": int(row["new_txns"] or 0),
        "duplicate_txns": int(row["duplicate_txns"] or 0),
        "fixme_txns": int(row["fixme_txns"] or 0),
        "bean_check_ok": bool(row["bean_check_ok"]),
        "error": row["error"],
    }


def _last_notification(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        """
        SELECT sent_at, channel, priority, title, delivered, error
          FROM notifications
      ORDER BY id DESC
         LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    raw = row["sent_at"]
    sent_at: datetime | None = None
    if isinstance(raw, datetime):
        sent_at = raw
    elif isinstance(raw, str):
        try:
            sent_at = datetime.fromisoformat(raw)
        except ValueError:
            sent_at = None
    return {
        "sent_at": sent_at,
        "channel": row["channel"],
        "priority": row["priority"],
        "title": row["title"],
        "delivered": bool(row["delivered"]),
        "error": row["error"],
    }


def _upcoming_cards(conn: sqlite3.Connection, *, today: date | None = None) -> list[_UpcomingCard]:
    """Read confirmed recurring expenses and project the ones with a
    next_expected within the 14-day horizon. Phase 6 replacement for
    Phase 5's heuristic predictor."""
    today = today or date.today()
    horizon = today + timedelta(days=14)
    service = RecurringService(conn)
    confirmed = service.list(status=RecurringStatus.CONFIRMED.value)
    out: list[_UpcomingCard] = []
    for r in confirmed:
        if r.next_expected is None:
            continue
        if r.next_expected > horizon:
            continue
        out.append(
            _UpcomingCard(
                label=r.label,
                source_account=r.source_account,
                expected_date=r.next_expected,
                expected_amount=r.expected_amount,
                cadence=r.cadence,
                overdue=r.next_expected < today,
            )
        )
    out.sort(key=lambda c: (c.expected_date, c.label))
    return out


@router.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    reader: LedgerReader = Depends(get_ledger_reader),
    reviews: ReviewService = Depends(get_review_service),
    notes: NoteService = Depends(get_note_service),
    conn: sqlite3.Connection = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    ledger = reader.load()
    balances = entity_balances(ledger.entries)
    money = money_groups(conn, ledger.entries)
    activity = activity_summary(conn)
    # Next-up card teaser: the most recent open review item.
    next_up_row = conn.execute(
        "SELECT id, source_ref FROM review_queue "
        "WHERE resolved_at IS NULL ORDER BY priority DESC, id LIMIT 1"
    ).fetchone()
    next_up_id = next_up_row["id"] if next_up_row else None

    # The "Needs categorizing" tile sums two distinct work surfaces
    # (legacy FIXME review_queue + staging). Route the click to the
    # one that actually has items, preferring the legacy FIXME flow
    # if it has work (so the existing /card UX stays intact) and
    # falling back to staged review otherwise. Without this, a user
    # whose work is entirely in staging clicks "38" and lands on
    # /card's empty state.
    legacy_fixme = int(activity.get("needs_categorizing_legacy_fixme") or 0)
    staged_pending = int(activity.get("needs_categorizing_staged") or 0)
    if legacy_fixme > 0 and next_up_id is not None:
        categorize_url = f"/card?item_id={next_up_id}"
    elif staged_pending > 0:
        categorize_url = "/review"
    else:
        categorize_url = "/card"

    budget_service = BudgetService(conn)
    budgets = budget_service.list()
    progresses = [progress_for_budget(b, ledger.entries) for b in budgets]
    placeholders = ",".join("?" * len(IMPORT_OPEN_STATES))
    import_open_row = conn.execute(
        f"SELECT COUNT(*) AS n FROM imports WHERE status IN ({placeholders})",
        tuple(IMPORT_OPEN_STATES),
    ).fetchone()
    import_open = int(import_open_row["n"] if import_open_row else 0)

    # First-run welcome panel: show on the first visit after the
    # wizard finishes, then auto-dismiss once the user has 5+
    # transactions or 7 days have passed. Manual dismiss writes a
    # user_ui_state row.
    show_welcome = False
    welcome_name = ""
    welcome_simplefin = False
    try:
        from lamella.features.setup.wizard_state import load_state
        wiz = load_state(conn)
        if wiz.completed_at:
            dismissed_row = conn.execute(
                "SELECT value FROM user_ui_state "
                "WHERE scope = 'dashboard' AND key = 'welcome_dismissed'",
            ).fetchone()
            already_dismissed = bool(dismissed_row and dismissed_row["value"])
            if not already_dismissed:
                # Auto-dismiss after 5+ transactions or 7 days.
                tx_count_row = conn.execute(
                    "SELECT COUNT(*) AS n FROM simplefin_transactions",
                ).fetchone()
                tx_count = int(tx_count_row["n"] if tx_count_row else 0)
                from datetime import datetime as _dt, timedelta as _td
                seven_days_ago = (_dt.now() - _td(days=7)).isoformat()
                old_enough = wiz.completed_at < seven_days_ago
                if tx_count < 5 and not old_enough:
                    show_welcome = True
                    welcome_name = wiz.name
                    welcome_simplefin = wiz.simplefin_connected
    except Exception:  # noqa: BLE001
        pass

    # Suggestion cards — observed-state nudges (e.g. "eBay looks like
    # a payout source — scaffold an account?"). Built from current
    # ledger + staging state; the card carries its own CTA so the
    # template just renders, never decides.
    try:
        from lamella.features.review_queue.suggestions import build_suggestion_cards
        suggestion_cards = build_suggestion_cards(
            conn, ledger.entries, context="global",
        )
    except Exception:  # noqa: BLE001 — bad data shouldn't kill the dashboard
        log.exception("dashboard: build_suggestion_cards failed")
        suggestion_cards = []

    # Pending-work tiles (formerly /inbox). Cheap counts surfaced on the
     # dashboard so the user has one landing page for "what's waiting on
     # me" instead of bouncing between /inbox + /. See ADR-0047 — the
     # dashboard is the navigation surface; focused workflows live at
     # their own routes.
    inbox_staged_total = count_pending_items(conn) or 0
    inbox_ai_pending = conn.execute(
        "SELECT COUNT(*) FROM ai_decisions "
        "WHERE decision_type = 'classify_txn' AND user_corrected = 0"
    ).fetchone()[0] or 0
    inbox_legacy_review = reviews.count_open()
    # Count of transactions over the receipt-required threshold that
    # don't yet have a receipt attached and weren't dismissed. Matches
    # what /receipts/needed?required_only=true shows. We let unexpected
    # failures bubble — silently zero-ing this hid a real bug for weeks
    # ("Receipts needed: 0" when hundreds were over threshold).
    try:
        from lamella.web.routes.receipts_needed import _count_needs_receipt
        inbox_receipts_needed = _count_needs_receipt(
            conn, reader=reader, settings=settings,
        ) or 0
    except sqlite3.OperationalError:
        # Schema not migrated yet (fresh install) — surface as zero
        # rather than 500ing the dashboard.
        inbox_receipts_needed = 0

    ctx = {
        "money": money,
        "activity": activity,
        "next_up_id": next_up_id,
        "categorize_url": categorize_url,
        "balances": balances,
        "review_count": reviews.count_open(),
        "note_count": notes.count_open(),
        "ledger_errors": ledger.errors,
        # Pending-work tiles for the dashboard's "Inbox" strip. Each is
        # a deep link to the focused page that owns the workflow.
        "inbox_staged_total": inbox_staged_total,
        "inbox_ai_pending": inbox_ai_pending,
        "inbox_legacy_review": inbox_legacy_review,
        "inbox_receipts_needed": inbox_receipts_needed,
        "last_webhook_at": getattr(request.app.state, "last_webhook_at", None),
        "simplefin_mode": settings.simplefin_mode,
        "last_simplefin": _last_simplefin_ingest(conn),
        "last_notification": _last_notification(conn),
        "upcoming": _upcoming_cards(conn),
        "budget_progresses": progresses,
        "import_open": import_open,
        "show_welcome": show_welcome,
        "welcome_name": welcome_name,
        "welcome_simplefin": welcome_simplefin,
        "suggestion_cards": suggestion_cards,
    }
    return request.app.state.templates.TemplateResponse(request, "dashboard.html", ctx)


@router.post("/dashboard/welcome/dismiss", response_class=HTMLResponse)
def dismiss_welcome(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Mark the first-run welcome panel as dismissed.

    Writes user_ui_state(scope='dashboard', key='welcome_dismissed').
    HTMX swaps the panel out via hx-target; non-HTMX falls back to a
    303 redirect home.
    """
    conn.execute(
        """
        INSERT INTO user_ui_state (scope, key, value)
        VALUES ('dashboard', 'welcome_dismissed', '1')
        ON CONFLICT (scope, key) DO UPDATE SET
            value = '1', updated_at = CURRENT_TIMESTAMP
        """,
    )
    conn.commit()
    if request.headers.get("hx-request", "").lower() == "true":
        return HTMLResponse("", status_code=200)
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/", status_code=303)
