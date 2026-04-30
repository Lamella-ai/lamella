# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from lamella.core.config import Settings
from lamella.web.deps import get_db, get_settings
from lamella.ports.notification import Channel, NotificationEvent, Priority
from lamella.features.notifications.dispatcher import Dispatcher

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True)
class NotificationRow:
    id: int
    sent_at: datetime | None
    channel: str
    priority: str
    dedup_key: str
    title: str
    body: str
    delivered: bool
    error: str | None


def _parse_ts(value) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _rows_to_items(rows) -> list[NotificationRow]:
    out: list[NotificationRow] = []
    for r in rows:
        out.append(
            NotificationRow(
                id=int(r["id"]),
                sent_at=_parse_ts(r["sent_at"]),
                channel=r["channel"],
                priority=r["priority"],
                dedup_key=r["dedup_key"],
                title=r["title"],
                body=r["body"],
                delivered=bool(r["delivered"]),
                error=r["error"],
            )
        )
    return out


def recent_notifications(conn: sqlite3.Connection, *, limit: int = 50) -> list[NotificationRow]:
    rows = conn.execute(
        """
        SELECT id, sent_at, channel, priority, dedup_key, title, body, delivered, error
          FROM notifications
      ORDER BY id DESC
         LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return _rows_to_items(rows)


def _dispatcher(request: Request) -> Dispatcher | None:
    return getattr(request.app.state, "dispatcher", None)


def _ctx(request: Request, settings: Settings, conn: sqlite3.Connection, *, message: str | None = None, error: str | None = None):
    dispatcher = _dispatcher(request)
    channels: list[dict] = []
    if dispatcher is not None:
        for notifier in dispatcher.notifiers:
            channels.append(
                {"name": notifier.channel.value, "enabled": notifier.enabled()}
            )
    return {
        "rows": recent_notifications(conn),
        "channels": channels,
        "ntfy_topic": settings.ntfy_topic or "",
        "pushover_configured": settings.pushover_enabled,
        "message": message,
        "error": error,
    }


@router.get("/notifications", response_class=HTMLResponse)
def notifications_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    ctx = _ctx(request, settings, conn)
    return request.app.state.templates.TemplateResponse(
        request, "notifications.html", ctx,
    )


@router.post("/notifications/test")
async def notifications_test(
    request: Request,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    dispatcher = _dispatcher(request)
    if dispatcher is None:
        ctx = _ctx(
            request, settings, conn,
            error="dispatcher is not configured (no channels enabled)",
        )
        return request.app.state.templates.TemplateResponse(
            request, "notifications.html", ctx, status_code=200,
        )
    from time import time as _now

    event = NotificationEvent(
        dedup_key=f"test:{int(_now())}",
        priority=Priority.INFO,
        title="Lamella test",
        body="This is a test notification from /notifications/test.",
        url="/notifications",
    )
    outcomes = await dispatcher.send(event)
    delivered = [o for o in outcomes if o.delivered]
    if delivered:
        message = f"delivered to {', '.join(o.channel.value for o in delivered)}"
    else:
        message = "no channel accepted the test — see rows below"
    ctx = _ctx(request, settings, conn, message=message)
    return request.app.state.templates.TemplateResponse(
        request, "notifications.html", ctx, status_code=200,
    )


@router.post("/notifications/{row_id}/resend")
async def notifications_resend(
    request: Request,
    row_id: int,
    settings: Settings = Depends(get_settings),
    conn: sqlite3.Connection = Depends(get_db),
):
    row = conn.execute(
        "SELECT * FROM notifications WHERE id = ?", (row_id,),
    ).fetchone()
    if row is None:
        return RedirectResponse(url="/notifications", status_code=303)
    dispatcher = _dispatcher(request)
    if dispatcher is None:
        ctx = _ctx(
            request, settings, conn,
            error="dispatcher is not configured (no channels enabled)",
        )
        return request.app.state.templates.TemplateResponse(
            request, "notifications.html", ctx, status_code=200,
        )
    try:
        priority = Priority(row["priority"])
    except ValueError:
        priority = Priority.INFO
    try:
        channel_hint = Channel(row["channel"])
    except ValueError:
        channel_hint = None
    event = NotificationEvent(
        dedup_key=f"resend:{row['id']}",
        priority=priority,
        title=row["title"],
        body=row["body"],
        channel_hint=channel_hint,
    )
    await dispatcher.send(event)
    return RedirectResponse(url="/notifications", status_code=303)
