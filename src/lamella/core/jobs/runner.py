# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""JobRunner — the long-running-ops service used across the app.

Lifecycle:

* ``submit(kind, title, fn, total=?, meta=?, return_url=?)`` inserts
  a row in ``jobs``, spins up a threadpool worker, returns the job id
  immediately. The caller can then redirect the browser to a URL that
  includes the job id and renders the progress modal.
* The worker receives a :class:`JobContext` it uses to ``emit``
  events, ``advance`` the progress counter, and check ``should_cancel``
  at each item boundary.
* Terminal status (``done``, ``cancelled``, ``error``) is stamped by
  the runner itself based on whether the worker returned normally,
  raised :class:`JobCancelled`, or raised anything else.
* On process startup ``mark_interrupted_on_startup`` flips any
  leftover ``running`` / ``queued`` rows to ``interrupted`` — the
  worker is gone and cannot resume.

Connection note: the worker's JobContext opens SHORT-LIVED SQLite
connections per write (see context.py) instead of sharing the main
app's locked connection. A 20-minute job holding the app's RLock
across every emit would starve request threads — short-lived
connections in WAL mode are far cheaper and let readers proceed.
"""
from __future__ import annotations

import json
import logging
import secrets
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

from lamella.core.jobs.context import JobCancelled, JobContext
from lamella.core.jobs.models import (
    Job,
    JobEvent,
    TERMINAL_STATUSES,
    row_to_event,
    row_to_job,
)

log = logging.getLogger(__name__)

WorkerFn = Callable[[JobContext], dict | None]


class JobRunner:
    """Threadpool-backed job runner. One instance per app (lives on
    ``app.state.job_runner``)."""

    def __init__(
        self,
        *,
        db_path: Path,
        max_workers: int = 4,
    ) -> None:
        self._db_path = Path(db_path)
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="jobrunner",
        )
        self._cancel_events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    # ---- submission ----

    def submit(
        self,
        *,
        kind: str,
        title: str,
        fn: WorkerFn,
        total: int | None = None,
        meta: dict | None = None,
        return_url: str | None = None,
    ) -> str:
        job_id = f"j_{secrets.token_urlsafe(10)}"
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO jobs
                    (id, kind, title, status, total, meta_json, return_url)
                VALUES (?, ?, ?, 'queued', ?, ?, ?)
                """,
                (
                    job_id, kind, title,
                    int(total) if total is not None else None,
                    json.dumps(meta) if meta else None,
                    return_url,
                ),
            )
        cancel_event = threading.Event()
        with self._lock:
            self._cancel_events[job_id] = cancel_event
        ctx = JobContext(
            job_id=job_id,
            cancel_event=cancel_event,
            db_path=self._db_path,
            runner=self,
        )
        self._pool.submit(self._run, ctx, fn)
        return job_id

    def _run(self, ctx: JobContext, fn: WorkerFn) -> None:
        self._mark_status(ctx.job_id, "running")
        try:
            result = fn(ctx)
        except JobCancelled:
            self._finish(ctx.job_id, status="cancelled")
            return
        except Exception as exc:  # noqa: BLE001
            log.exception("job %s crashed", ctx.job_id)
            self._finish(
                ctx.job_id,
                status="error",
                error_message=f"{type(exc).__name__}: {exc}",
            )
            return
        finally:
            with self._lock:
                self._cancel_events.pop(ctx.job_id, None)
        self._finish(
            ctx.job_id,
            status="done",
            result=result if isinstance(result, dict) else None,
        )

    # ---- control ----

    def cancel(self, job_id: str) -> bool:
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET cancel_requested = 1 WHERE id = ? AND status IN ('queued', 'running')",
                (job_id,),
            )
        with self._lock:
            ev = self._cancel_events.get(job_id)
        if ev is not None:
            ev.set()
            return True
        return False

    def set_return_url(self, job_id: str, url: str) -> None:
        """Patch the stored return_url. Used when the final URL needs
        to embed the generated job_id (which isn't known until after
        ``submit`` runs)."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET return_url = ? WHERE id = ?",
                (url, job_id),
            )

    def mark_interrupted_on_startup(self) -> int:
        """Flip any stale running/queued rows to 'interrupted'.
        Returns the count flipped."""
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'interrupted', "
                "finished_at = CURRENT_TIMESTAMP, "
                "error_message = COALESCE(error_message, 'process restarted mid-job') "
                "WHERE status IN ('queued', 'running')"
            )
            return cur.rowcount or 0

    def shutdown(self) -> None:
        """Best-effort: signal all active cancel events, then drain the pool."""
        with self._lock:
            events = list(self._cancel_events.values())
        for ev in events:
            ev.set()
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ---- reads ----

    def get(self, job_id: str) -> Job | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,),
            ).fetchone()
            return row_to_job(row) if row else None

    def active(self, limit: int = 20) -> list[Job]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued', 'running') "
                "ORDER BY started_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [row_to_job(r) for r in rows]

    def recent(self, *, limit: int = 25) -> list[Job]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY started_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [row_to_job(r) for r in rows]

    def events(
        self,
        job_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ) -> list[JobEvent]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM job_events WHERE job_id = ? AND seq > ? "
                "ORDER BY seq LIMIT ?",
                (job_id, int(after_seq), int(limit)),
            ).fetchall()
            return [row_to_event(r) for r in rows]

    def tail_events(
        self,
        job_id: str,
        *,
        limit: int = 20,
    ) -> list[JobEvent]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM job_events WHERE job_id = ? "
                "ORDER BY seq DESC LIMIT ?",
                (job_id, int(limit)),
            ).fetchall()
            # Return in chronological order.
            return list(reversed([row_to_event(r) for r in rows]))

    # ---- internal ----

    def _conn(self) -> sqlite3.Connection:
        from lamella.core.jobs.context import _Closer

        conn = sqlite3.connect(str(self._db_path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        return _Closer(conn)  # type: ignore[return-value]

    def _mark_status(self, job_id: str, status: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, last_progress_at = CURRENT_TIMESTAMP "
                "WHERE id = ?",
                (status, job_id),
            )

    def _finish(
        self,
        job_id: str,
        *,
        status: str,
        result: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"not a terminal status: {status!r}")
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, finished_at = CURRENT_TIMESTAMP, "
                "last_progress_at = CURRENT_TIMESTAMP, "
                "result_json = COALESCE(?, result_json), "
                "error_message = COALESCE(?, error_message) "
                "WHERE id = ?",
                (
                    status,
                    json.dumps(result) if result else None,
                    error_message,
                    job_id,
                ),
            )
