# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""The handle a worker function uses to report progress + check cancel."""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import TYPE_CHECKING

from lamella.core.jobs.models import JobOutcome

if TYPE_CHECKING:
    from lamella.core.jobs.runner import JobRunner


class JobCancelled(RuntimeError):
    """Raised by JobContext.raise_if_cancelled when the user asks to stop."""


class JobContext:
    """Passed to the worker callable. Thread-owned, not shared.

    The worker typically does::

        def work(ctx: JobContext) -> dict:
            ctx.set_total(len(items))
            for i, item in enumerate(items):
                ctx.raise_if_cancelled()
                ctx.emit(f"Processing {item.name}", outcome="info")
                try:
                    result = do_one(item)
                    ctx.emit(f"Done: {result}", outcome="success")
                except ItemNotFound:
                    ctx.emit(f"Not found: {item.name}", outcome="not_found")
                except Exception as exc:  # noqa: BLE001
                    ctx.emit(f"Error on {item.name}: {exc}", outcome="error")
                ctx.advance()
            return {"processed": len(items)}

    The emit/set_total/advance calls are thread-safe and open their
    own short-lived connection per call so the worker never competes
    with the request-thread lock on the main app connection.
    """

    def __init__(
        self,
        *,
        job_id: str,
        cancel_event: threading.Event,
        db_path,
        runner: "JobRunner | None" = None,
    ) -> None:
        self.job_id = job_id
        self._cancel_event = cancel_event
        self._db_path = db_path
        self._runner = runner
        self._lock = threading.Lock()
        self._seq = 0

    # ---- cancel ----

    @property
    def should_cancel(self) -> bool:
        return self._cancel_event.is_set()

    def raise_if_cancelled(self) -> None:
        if self._cancel_event.is_set():
            raise JobCancelled()

    # ---- progress ----

    def set_return_url(self, url: str) -> None:
        """Update the job's ``return_url`` from inside the worker.
        Useful when the URL depends on what path the worker took
        (e.g. "mapped existing field" vs "created new one" go to
        different result URLs)."""
        if self._runner is not None:
            self._runner.set_return_url(self.job_id, url)

    def set_total(self, total: int) -> None:
        with self._writer() as conn:
            conn.execute(
                "UPDATE jobs SET total = ?, last_progress_at = CURRENT_TIMESTAMP "
                "WHERE id = ?",
                (int(total), self.job_id),
            )

    def advance(self, n: int = 1) -> None:
        with self._writer() as conn:
            conn.execute(
                "UPDATE jobs SET completed = completed + ?, "
                "last_progress_at = CURRENT_TIMESTAMP WHERE id = ?",
                (int(n), self.job_id),
            )

    def emit(
        self,
        message: str,
        *,
        outcome: JobOutcome | None = None,
        detail: dict | None = None,
    ) -> None:
        """Append an event to the stream AND bump the per-outcome counter
        so the modal's counters update live."""
        with self._lock:
            self._seq += 1
            seq = self._seq
        outcome_col = {
            "success": "success_count",
            "failure": "failure_count",
            "not_found": "not_found_count",
            "error": "error_count",
            "info": "info_count",
        }.get(outcome or "")
        detail_json = json.dumps(detail) if detail else None
        with self._writer() as conn:
            conn.execute(
                "INSERT INTO job_events (job_id, seq, message, outcome, detail_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.job_id, seq, message, outcome, detail_json),
            )
            if outcome_col:
                conn.execute(
                    f"UPDATE jobs SET {outcome_col} = {outcome_col} + 1, "
                    "last_progress_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (self.job_id,),
                )
            else:
                conn.execute(
                    "UPDATE jobs SET last_progress_at = CURRENT_TIMESTAMP "
                    "WHERE id = ?",
                    (self.job_id,),
                )

    # ---- internal ----

    def _writer(self) -> sqlite3.Connection:
        """Fresh short-lived connection per write. Autocommit; WAL is
        already on from connect()."""
        conn = sqlite3.connect(str(self._db_path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        return _Closer(conn)


class _Closer:
    """Context-manager wrapper that ensures the connection closes."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self) -> sqlite3.Connection:
        return self._conn

    def __exit__(self, *exc) -> None:
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001
            pass
