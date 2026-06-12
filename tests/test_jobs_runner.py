# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the generic JobRunner — the long-running-ops service.

Covers submission, event streaming, cancel, error propagation,
interrupted-on-restart recovery, and the counter bookkeeping the
progress modal relies on.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.core.jobs import JobCancelled, JobContext, JobRunner


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "jobs.sqlite"
    conn = connect(p)
    migrate(conn)
    conn.close()
    return p


def _wait_for_terminal(runner: JobRunner, job_id: str, *, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = runner.get(job_id)
        if job and job.is_terminal:
            return job
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


def test_submit_runs_to_done_and_emits_events(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=2)
    try:
        def work(ctx: JobContext):
            ctx.set_total(3)
            for i in range(3):
                ctx.emit(f"item {i}", outcome="success")
                ctx.advance()
            return {"processed": 3}

        job_id = runner.submit(kind="test", title="Test job", fn=work)
        job = _wait_for_terminal(runner, job_id)
        assert job.status == "done"
        assert job.completed == 3
        assert job.success_count == 3
        assert job.total == 3
        assert job.result == {"processed": 3}
        events = runner.events(job_id)
        assert [e.message for e in events] == ["item 0", "item 1", "item 2"]
        assert all(e.outcome == "success" for e in events)
    finally:
        runner.shutdown()


def test_outcome_counters_track_each_bucket(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=1)
    try:
        def work(ctx: JobContext):
            ctx.emit("win", outcome="success")
            ctx.emit("miss", outcome="not_found")
            ctx.emit("bust", outcome="failure")
            ctx.emit("crash", outcome="error")
            ctx.emit("note", outcome="info")

        job_id = runner.submit(kind="test", title="counters", fn=work)
        job = _wait_for_terminal(runner, job_id)
        assert job.success_count == 1
        assert job.not_found_count == 1
        assert job.failure_count == 1
        assert job.error_count == 1
        assert job.info_count == 1
        assert job.status == "done"
    finally:
        runner.shutdown()


def test_cancel_raises_in_worker_and_marks_cancelled(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=1)
    try:
        started = threading.Event()
        allowed = threading.Event()

        def work(ctx: JobContext):
            ctx.set_total(5)
            started.set()
            for i in range(5):
                if not allowed.wait(timeout=2.0):
                    # Let the cancel come in while we're looping.
                    pass
                ctx.raise_if_cancelled()
                ctx.emit(f"item {i}", outcome="success")
                ctx.advance()

        job_id = runner.submit(kind="test", title="cancellable", fn=work)
        assert started.wait(timeout=1.5)
        assert runner.cancel(job_id) is True
        allowed.set()
        job = _wait_for_terminal(runner, job_id)
        assert job.status == "cancelled"
        assert job.cancel_requested is True
    finally:
        runner.shutdown()


def test_worker_exception_marks_error(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=1)
    try:
        def work(ctx: JobContext):
            ctx.emit("starting", outcome="info")
            raise ValueError("boom")

        job_id = runner.submit(kind="test", title="error", fn=work)
        job = _wait_for_terminal(runner, job_id)
        assert job.status == "error"
        assert job.error_message and "boom" in job.error_message
    finally:
        runner.shutdown()


def test_mark_interrupted_on_startup_flips_stale_rows(db_path: Path):
    """A row left in 'running' from a killed process is flipped to
    'interrupted' on next boot — the worker is gone and cannot resume."""
    # Simulate a process that died mid-job: insert a row directly.
    runner = JobRunner(db_path=db_path, max_workers=1)
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path), isolation_level=None)
        conn.execute(
            "INSERT INTO jobs (id, kind, title, status) VALUES (?, ?, ?, 'running')",
            ("j_stale", "test", "stale"),
        )
        conn.close()

        flipped = runner.mark_interrupted_on_startup()
        assert flipped == 1
        job = runner.get("j_stale")
        assert job is not None
        assert job.status == "interrupted"
        assert job.finished_at is not None
    finally:
        runner.shutdown()


def test_events_returns_only_new_after_seq(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=1)
    try:
        def work(ctx: JobContext):
            for i in range(4):
                ctx.emit(f"n{i}", outcome="info")

        job_id = runner.submit(kind="test", title="events", fn=work)
        _wait_for_terminal(runner, job_id)
        all_events = runner.events(job_id)
        assert len(all_events) == 4
        after_two = runner.events(job_id, after_seq=2)
        assert [e.seq for e in after_two] == [3, 4]


    finally:
        runner.shutdown()


def test_total_and_percent_and_eta(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=1)
    try:
        def work(ctx: JobContext):
            ctx.set_total(10)
            for i in range(5):
                ctx.advance()

        job_id = runner.submit(kind="test", title="pct", fn=work)
        job = _wait_for_terminal(runner, job_id)
        assert job.total == 10
        assert job.completed == 5
        # Completion halfway → 50%.
        assert job.percent == pytest.approx(50.0)
    finally:
        runner.shutdown()


def test_active_lists_running_only(db_path: Path):
    runner = JobRunner(db_path=db_path, max_workers=2)
    try:
        block = threading.Event()

        def blocking(ctx: JobContext):
            ctx.emit("started", outcome="info")
            block.wait(timeout=2.0)

        def quick(ctx: JobContext):
            ctx.emit("zip", outcome="info")

        blocking_id = runner.submit(kind="block", title="blocking", fn=blocking)
        quick_id = runner.submit(kind="quick", title="quick", fn=quick)
        # Wait for quick to finish; blocking is still stuck.
        _wait_for_terminal(runner, quick_id)
        active = runner.active()
        active_ids = [j.id for j in active]
        assert blocking_id in active_ids
        assert quick_id not in active_ids
        block.set()
        _wait_for_terminal(runner, blocking_id)
    finally:
        runner.shutdown()
