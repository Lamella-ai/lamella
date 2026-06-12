# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Awaitable, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

log = logging.getLogger(__name__)

JOB_ID = "simplefin_fetch"


def register(
    scheduler: AsyncIOScheduler,
    *,
    interval_hours: int,
    mode: str,
    callback: Callable[[], Awaitable[None]],
) -> None:
    """Register (or re-register) the SimpleFIN fetch job.

    ``mode`` ``disabled`` removes the job entirely. ``shadow`` and
    ``active`` share the same trigger — the difference lives in the
    ingest pipeline, not the scheduler."""
    try:
        scheduler.remove_job(JOB_ID)
    except Exception:  # noqa: BLE001 — APScheduler raises when the job is absent
        pass

    if mode.strip().lower() == "disabled":
        return

    scheduler.add_job(
        callback,
        IntervalTrigger(hours=max(1, interval_hours), jitter=300),
        id=JOB_ID,
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )


def trigger_manual(scheduler: AsyncIOScheduler, callback: Callable[[], Awaitable[None]]) -> None:
    """Fire a manual fetch that shares the single-instance guard on JOB_ID
    so it cannot overlap a scheduled run."""
    scheduler.add_job(
        callback,
        id=f"{JOB_ID}_manual",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        next_run_time=datetime.now(UTC),
    )
