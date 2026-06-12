# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""HTTP endpoints for the generic job runner.

* ``GET  /jobs/{id}/partial``   → progress-modal body (polled every 1s)
* ``GET  /jobs/{id}``           → full-page wrapper (used for deep links
                                  + after-reload modal reattachment)
* ``POST /jobs/{id}/cancel``    → sets cancel flag; returns refreshed partial
* ``GET  /jobs/active/dock``    → docked active-jobs strip (for base.html)
* ``GET  /jobs/{id}/stream``    → Server-Sent Events (optional real-time)

Any handler that submits a job typically returns a 200 with the modal
partial pointed at the new job id — HTMX swaps it into the current page
and polling starts automatically.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from lamella.core.jobs.models import TERMINAL_STATUSES
from lamella.core.jobs.runner import JobRunner

log = logging.getLogger(__name__)

router = APIRouter()


def _get_runner(request: Request) -> JobRunner:
    runner = getattr(request.app.state, "job_runner", None)
    if runner is None:
        raise HTTPException(status_code=503, detail="job runner not ready")
    return runner


@router.get("/jobs/{job_id}/partial", response_class=HTMLResponse)
def job_partial(job_id: str, request: Request) -> HTMLResponse:
    runner = _get_runner(request)
    job = runner.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    events = runner.tail_events(job_id, limit=40)
    response = request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_progress.html",
        {"job": job, "events": events, "terminal": job.is_terminal},
    )
    # Server-driven polling halt. Once the job is terminal, surface a
    # custom event via HX-Trigger so _job_modal.html can detach the
    # polling loop. This is more reliable than relying on a client-
    # side htmx:afterSwap listener — that path depends on the shim
    # firing the event AND on the listener still being bound. The
    # HX-Trigger path goes through the shim's `handleTrigger` which
    # dispatches on document.body unconditionally.
    if job.is_terminal:
        response.headers["HX-Trigger"] = "lamella:job-terminal"
    return response


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(job_id: str, request: Request) -> HTMLResponse:
    """Standalone page that wraps the modal — used when the browser
    lands on a job URL directly (link sharing, reload, etc.)."""
    runner = _get_runner(request)
    job = runner.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    events = runner.tail_events(job_id, limit=100)
    return request.app.state.templates.TemplateResponse(
        request, "job_detail.html",
        {"job": job, "events": events},
    )


@router.post("/jobs/{job_id}/cancel", response_class=HTMLResponse)
def job_cancel(job_id: str, request: Request) -> HTMLResponse:
    runner = _get_runner(request)
    job = runner.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    runner.cancel(job_id)
    # Return the refreshed partial.
    job = runner.get(job_id) or job
    events = runner.tail_events(job_id, limit=40)
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_progress.html",
        {"job": job, "events": events, "terminal": job.is_terminal},
    )


@router.get("/jobs/active/dock", response_class=HTMLResponse)
def jobs_active_dock(request: Request) -> HTMLResponse:
    runner = _get_runner(request)
    jobs = runner.active()
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_dock.html",
        {"jobs": jobs},
    )


@router.get("/jobs/{job_id}/modal-fragment", response_class=HTMLResponse)
def job_modal_fragment(job_id: str, request: Request) -> HTMLResponse:
    """Return the in-page modal partial for an existing job, so a
    user who minimized the modal can click a dock pill and bring the
    modal back on the current page (instead of navigating to the
    full-page detail view at /jobs/{id}). Mirrors the partial that
    handlers initially returned when the job was submitted, so the
    polling + cancel behavior reattaches identically.
    """
    runner = _get_runner(request)
    job = runner.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return_url = job.return_url or str(request.headers.get("referer") or "/")
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/_job_modal.html",
        {"job_id": job_id, "on_close_url": return_url},
    )


@router.get("/jobs/{job_id}/stream")
async def job_stream(job_id: str, request: Request):
    """Server-Sent Events stream. Emits a ``progress`` event whenever
    new rows appear in ``job_events``, and a ``close`` event once the
    job reaches a terminal status. Not currently required for the
    modal (it polls), but available for any UI that wants lower-
    latency updates."""
    runner = _get_runner(request)
    job = runner.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    async def iter_events() -> AsyncIterator[bytes]:
        after_seq = 0
        while True:
            if await request.is_disconnected():
                return
            current = runner.get(job_id)
            if current is None:
                yield b"event: close\ndata: {}\n\n"
                return
            new_events = runner.events(job_id, after_seq=after_seq)
            for ev in new_events:
                after_seq = max(after_seq, ev.seq)
                payload = {
                    "seq": ev.seq,
                    "message": ev.message,
                    "outcome": ev.outcome,
                    "ts": ev.ts.isoformat(),
                    "detail": ev.detail,
                }
                yield (
                    b"event: progress\ndata: "
                    + json.dumps(payload).encode("utf-8")
                    + b"\n\n"
                )
            status_payload = json.dumps({
                "status": current.status,
                "completed": current.completed,
                "total": current.total,
                "percent": current.percent,
                "success": current.success_count,
                "failure": current.failure_count,
                "not_found": current.not_found_count,
                "error": current.error_count,
            }).encode("utf-8")
            yield b"event: status\ndata: " + status_payload + b"\n\n"
            if current.is_terminal:
                yield b"event: close\ndata: {}\n\n"
                return
            await asyncio.sleep(1.0)

    return StreamingResponse(
        iter_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        },
    )
