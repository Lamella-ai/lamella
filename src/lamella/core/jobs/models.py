# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Shapes for the job runner. See __init__.py for the top-level API."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

JobStatus = Literal[
    "queued",
    "running",
    "done",
    "cancelled",
    "error",
    "interrupted",
]

JobOutcome = Literal[
    "success",
    "failure",
    "not_found",
    "error",
    "info",
]

TERMINAL_STATUSES: frozenset[str] = frozenset({
    "done", "cancelled", "error", "interrupted",
})


@dataclass
class Job:
    id: str
    kind: str
    title: str
    status: JobStatus
    total: int | None
    completed: int
    success_count: int
    failure_count: int
    not_found_count: int
    error_count: int
    info_count: int
    cancel_requested: bool
    meta: dict | None
    result: dict | None
    error_message: str | None
    return_url: str | None
    started_at: datetime
    finished_at: datetime | None
    last_progress_at: datetime

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def percent(self) -> float | None:
        """0–100, or None when total isn't known."""
        if self.total is None or self.total <= 0:
            return None
        pct = 100.0 * self.completed / self.total
        return max(0.0, min(100.0, pct))

    @property
    def elapsed_seconds(self) -> float:
        end = self.finished_at or _now()
        start = self.started_at
        return max(0.0, (end - start).total_seconds())

    @property
    def eta_seconds(self) -> float | None:
        """Rough remaining time in seconds, None if unknown."""
        if self.is_terminal:
            return 0.0
        if self.total is None or self.completed <= 0:
            return None
        elapsed = self.elapsed_seconds
        if elapsed <= 0:
            return None
        rate = self.completed / elapsed
        if rate <= 0:
            return None
        remaining = max(0, self.total - self.completed)
        return remaining / rate

    def humanize_eta(self) -> str:
        secs = self.eta_seconds
        if secs is None:
            return "—"
        return _humanize_seconds(secs)

    def humanize_elapsed(self) -> str:
        return _humanize_seconds(self.elapsed_seconds)


@dataclass
class JobEvent:
    id: int
    job_id: str
    seq: int
    ts: datetime
    message: str
    outcome: JobOutcome | None
    detail: dict | None


def _humanize_seconds(secs: float) -> str:
    secs = max(0, int(secs))
    if secs < 60:
        return f"{secs}s"
    mins, s = divmod(secs, 60)
    if mins < 60:
        return f"{mins}m {s:02d}s"
    hrs, m = divmod(mins, 60)
    return f"{hrs}h {m:02d}m"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def row_to_job(row) -> Job:
    """Convert a sqlite3.Row / dict-like into a Job."""
    return Job(
        id=row["id"],
        kind=row["kind"],
        title=row["title"],
        status=row["status"],
        total=row["total"],
        completed=row["completed"] or 0,
        success_count=row["success_count"] or 0,
        failure_count=row["failure_count"] or 0,
        not_found_count=row["not_found_count"] or 0,
        error_count=row["error_count"] or 0,
        info_count=row["info_count"] or 0,
        cancel_requested=bool(row["cancel_requested"]),
        meta=_load_json(row["meta_json"]),
        result=_load_json(row["result_json"]),
        error_message=row["error_message"],
        return_url=row["return_url"],
        started_at=_parse_ts(row["started_at"]),
        finished_at=_parse_ts(row["finished_at"]) if row["finished_at"] else None,
        last_progress_at=_parse_ts(row["last_progress_at"]),
    )


def row_to_event(row) -> JobEvent:
    return JobEvent(
        id=row["id"],
        job_id=row["job_id"],
        seq=row["seq"],
        ts=_parse_ts(row["ts"]),
        message=row["message"],
        outcome=row["outcome"],
        detail=_load_json(row["detail_json"]),
    )


def _load_json(raw) -> dict | None:
    if not raw:
        return None
    try:
        val = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return val if isinstance(val, dict) else None


def _parse_ts(raw) -> datetime:
    if isinstance(raw, datetime):
        return raw
    s = str(raw)
    # SQLite CURRENT_TIMESTAMP → "YYYY-MM-DD HH:MM:SS"
    try:
        return datetime.fromisoformat(s.replace(" ", "T"))
    except ValueError:
        return _now()
