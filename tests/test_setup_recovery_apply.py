# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Integration walk for /setup/recovery/apply — Phase 6.1.4c.

Drives the full pipeline through the route surface:

1. Compose drafts via the HTMX dismiss/edit endpoints.
2. POST /setup/recovery/apply → 303 to /setup/recovery/finalizing.
3. JobRunner worker bridges run_bulk_apply events → ctx.emit; we
   poll the job until terminal and walk the recorded events.
4. Verify post-state: applied_history populated, drafts cleared on
   success, finalizing template renders the recovery shell.
5. Refusal path: POST with no actionable drafts bounces back to
   /setup/recovery rather than spinning up a no-op job.

Heal-action specifics are not under test here — they're covered by
`tests/test_phase_3_legacy_paths.py` and `tests/test_bulk_apply.py`.
The integration walk patches ``heal_legacy_path`` to return success
so we can drive the orchestrator/route bridge without constructing
a real legacy-path scenario in the fixture ledger.
"""
from __future__ import annotations

import json
import time

from lamella.features.recovery.models import (
    Finding,
    HealResult,
    fix_payload,
    make_finding_id,
)


def _legacy_finding(target: str = "Assets:Vehicles:Foo") -> Finding:
    return Finding(
        id=make_finding_id("legacy_path", target),
        category="legacy_path",
        severity="warning",
        target_kind="account",
        target=target,
        summary=f"Move {target}",
        detail=None,
        proposed_fix=fix_payload(
            action="move", canonical="Assets:Personal:Vehicle:Foo",
        ),
        alternatives=(),
        confidence="high",
        source="detect_legacy_paths",
    )


def _wait_for_terminal(runner, job_id: str, *, timeout_s: float = 5.0):
    """Poll the runner until the job hits a terminal status. The
    threadpool worker runs in the background; the test thread has to
    wait without busy-spinning."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        job = runner.get(job_id)
        if job is not None and job.is_terminal:
            return job
        time.sleep(0.05)
    raise AssertionError(
        f"job {job_id} did not reach terminal in {timeout_s}s"
    )


def _patch_detector(monkeypatch, *findings: Finding) -> None:
    """Pin the detector output everywhere the pipeline calls it.

    The route layer's GET / draft / apply flow imports detect_all
    from `lamella.web.routes.setup_recovery`; the orchestrator's default
    re-detect path imports detect_all from
    `lamella.features.recovery.bulk_apply`. Both need to see the
    same synthetic finding set so the closed-world filter doesn't
    drop our drafts when the orchestrator re-detects.
    """
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: findings,
    )
    monkeypatch.setattr(
        "lamella.features.recovery.bulk_apply.detect_all",
        lambda conn, entries: findings,
    )


def _patch_heal_success(monkeypatch) -> None:
    """Replace heal_legacy_path with a success-returning stub. The
    orchestrator imports it inside `_heal_one` from
    `lamella.features.recovery.heal`."""
    def _ok(finding, *, conn, settings, reader, bean_check=None, bulk_context=None):
        return HealResult(
            success=True,
            message=f"applied {finding.id}",
            files_touched=(),
            finding_id=finding.id,
        )
    monkeypatch.setattr(
        "lamella.features.recovery.heal.heal_legacy_path",
        _ok,
    )


# ---------------------------------------------------------------------------
# Refusal path — no actionable drafts
# ---------------------------------------------------------------------------


def test_apply_with_no_drafts_bounces_back(app_client, monkeypatch):
    """A direct POST with no composed drafts is a no-op — bounce
    back to /setup/recovery rather than spin up an empty job."""
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )
    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/setup/recovery"


def test_apply_with_only_dismissed_drafts_bounces_back(app_client, monkeypatch):
    """Drafts present, but every one is dismissed → no actionable
    work. Same bounce behavior as the no-drafts case."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    # Compose: dismiss the only finding.
    app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/setup/recovery"


# ---------------------------------------------------------------------------
# Happy path — full integration walk
# ---------------------------------------------------------------------------


def test_apply_submits_job_and_redirects_to_finalizing(
    app_client, monkeypatch,
):
    """POST /setup/recovery/apply should redirect (303) to
    /setup/recovery/finalizing?job=<id> with a runnable job id."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    _patch_heal_success(monkeypatch)
    # The default-action for a finding without an explicit draft entry
    # is now closed-world drop — write an explicit "apply" draft to
    # include this finding in the batch.
    from lamella.features.recovery.repair_state import write_repair_state
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    assert r.status_code == 303
    location = r.headers["location"]
    assert location.startswith("/setup/recovery/finalizing?job=")
    job_id = location.rsplit("=", 1)[-1]
    assert job_id.startswith("j_")

    # The job must complete (terminal status) — proves the worker
    # doesn't hang.
    runner = app_client.app.state.job_runner
    job = _wait_for_terminal(runner, job_id)
    assert job.status == "done"


def test_apply_emits_locked_event_sequence(app_client, monkeypatch):
    """After the job finishes, the event log must carry the locked
    event vocabulary in the locked order: batch_started,
    group_started, finding_applied, group_committed, batch_done."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    _patch_heal_success(monkeypatch)
    from lamella.features.recovery.repair_state import write_repair_state
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    job_id = r.headers["location"].rsplit("=", 1)[-1]
    runner = app_client.app.state.job_runner
    _wait_for_terminal(runner, job_id)

    events = runner.events(job_id)
    # Pull the event-discriminator off each detail blob.
    discriminators = []
    for ev in events:
        if ev.detail and "event" in ev.detail:
            discriminators.append(ev.detail["event"])
    # The single-finding cleanup-group batch must produce exactly:
    # batch_started → group_started(cleanup) → finding_applied →
    # group_committed → batch_done.
    assert discriminators == [
        "batch_started",
        "group_started",
        "finding_applied",
        "group_committed",
        "batch_done",
    ]
    # The terminal event's outcome must be 'success' (one finding,
    # no failures).
    batch_done = events[-1]
    assert batch_done.detail["outcome"] == "success"


def test_apply_writes_applied_history_and_clears_drafts_on_success(
    app_client, monkeypatch,
):
    """Worker contract: after a successful run, applied_history is
    appended (one entry per group with the per-finding ids) and the
    drafts blob is cleared so the next page visit starts fresh."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    _patch_heal_success(monkeypatch)
    from lamella.features.recovery.repair_state import (
        read_repair_state,
        write_repair_state,
    )
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    job_id = r.headers["location"].rsplit("=", 1)[-1]
    runner = app_client.app.state.job_runner
    _wait_for_terminal(runner, job_id)

    # The worker writes through its own connection; pull state via
    # the request conn.
    state = read_repair_state(db)
    assert state["findings"] == {}, "drafts should be cleared on success"
    assert len(state["applied_history"]) == 1
    history = state["applied_history"][0]
    assert history["group"] == "cleanup"
    assert history["applied_finding_ids"] == [f.id]
    assert history["failed_finding_ids"] == []
    assert history["rolled_back"] is False
    assert "committed_at" in history


def test_apply_rolls_back_atomic_group_on_failure(
    app_client, monkeypatch,
):
    """Phase 6.1.3.5 atomic-group semantics: legacy_path is in the
    cleanup group (atomic). When f1 succeeds and f2 fails, the
    outer envelope rolls back f1's writes too — applied_history
    must reflect that nothing committed for the rolled-back group.
    Drafts persist for Resume."""
    f1 = _legacy_finding("Assets:Vehicles:A")
    f2 = _legacy_finding("Assets:Vehicles:B")
    _patch_detector(monkeypatch, f1, f2)

    # f1 returns success, f2 returns failure — would partial-succeed
    # under best-effort semantics, but cleanup is atomic now.
    def _heal(finding, *, conn, settings, reader, bean_check=None, bulk_context=None):
        if finding.id == f2.id:
            return HealResult(
                success=False, message="contrived failure",
                files_touched=(), finding_id=finding.id,
            )
        return HealResult(
            success=True, message="ok",
            files_touched=(), finding_id=finding.id,
        )
    monkeypatch.setattr(
        "lamella.features.recovery.heal.heal_legacy_path", _heal,
    )

    from lamella.features.recovery.repair_state import (
        read_repair_state,
        write_repair_state,
    )
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {
            f1.id: {"action": "apply", "edit_payload": None},
            f2.id: {"action": "apply", "edit_payload": None},
        },
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    job_id = r.headers["location"].rsplit("=", 1)[-1]
    runner = app_client.app.state.job_runner
    _wait_for_terminal(runner, job_id)

    state = read_repair_state(db)
    # Drafts survive — Resume durability holds in both atomic and
    # best-effort modes.
    assert f1.id in state["findings"]
    assert f2.id in state["findings"]
    # applied_history records the rollback: rolled_back=True,
    # applied_finding_ids empty (the f1 success was reverted),
    # failed_finding_ids carries the trigger finding's id.
    assert len(state["applied_history"]) == 1
    h = state["applied_history"][0]
    assert h["group"] == "cleanup"
    assert h["applied_finding_ids"] == [], (
        "atomic-group rollback should revert successful in-flight "
        "heals — non-empty applied_finding_ids would mean we leaked "
        "a partial-commit into history"
    )
    assert sorted(h["failed_finding_ids"]) == [f2.id]
    assert h["rolled_back"] is True
    assert "contrived failure" in h.get("reason", "")


# ---------------------------------------------------------------------------
# Phase 8 step 8 — pre-flight failure → applied_history "preflight" entry
# ---------------------------------------------------------------------------


def test_apply_preflight_failure_writes_synthetic_history_entry(
    app_client, monkeypatch,
):
    """Pre-flight rejection emits FindingFailed events outside any
    group, then BatchDone(failed). The worker bridge writes a
    synthetic applied_history entry with group="preflight" so the
    user can see on the next page visit that the apply was rejected
    at validation time before any commit. Drafts persist (no
    success → no clearing) for Resume after fixing the stale edits."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    # Force the destination guard to fail so pre-flight rejects.
    monkeypatch.setattr(
        "lamella.features.recovery.findings.legacy_paths."
        "_passes_destination_guards",
        lambda canonical, opened: False,
    )

    from lamella.features.recovery.repair_state import (
        read_repair_state,
        write_repair_state,
    )
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {
            "action": "edit",
            "edit_payload": {"canonical": "Assets:Stale:Path"},
        }},
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    job_id = r.headers["location"].rsplit("=", 1)[-1]
    runner = app_client.app.state.job_runner
    _wait_for_terminal(runner, job_id)

    state = read_repair_state(db)
    # Drafts survive — Resume durability.
    assert f.id in state["findings"]
    # Synthetic preflight entry appended to applied_history.
    assert len(state["applied_history"]) == 1
    h = state["applied_history"][0]
    assert h["group"] == "preflight"
    assert h["applied_finding_ids"] == []
    assert sorted(h["failed_finding_ids"]) == [f.id]
    assert h["rolled_back"] is True
    assert "pre-flight" in h["reason"].lower()


# ---------------------------------------------------------------------------
# Finalizing page surface
# ---------------------------------------------------------------------------


def test_finalizing_page_renders_recovery_shell_with_job_id(app_client):
    """The finalizing page renders inside the recovery shell — no
    /settings/* leaks, no main-app sidebar, and the JS picks up the
    job_id we passed in for SSE subscription."""
    r = app_client.get("/setup/recovery/finalizing?job=j_abc123")
    assert r.status_code == 200
    # Recovery shell isolation: no leaks to /settings/*, /accounts,
    # /simplefin (same invariants the bulk-review page enforces).
    assert "/settings/" not in r.text
    assert 'href="/accounts"' not in r.text
    assert 'href="/simplefin"' not in r.text
    # JS config block carries the job id verbatim.
    assert '"jobId": "j_abc123"' in r.text
    # SSE subscription URL embedded.
    assert "/jobs/" in r.text
    # Recovery progress pill renders (current_step='recovery').
    assert "Recovery" in r.text


def test_finalizing_page_with_no_job_renders_safely(app_client):
    """A direct hit (no ?job=) must not crash; the JS handles the
    empty case by bouncing back to /setup/recovery."""
    r = app_client.get("/setup/recovery/finalizing")
    assert r.status_code == 200
    assert '"jobId": ""' in r.text


# ---------------------------------------------------------------------------
# Audit fix: double-submit guard
# ---------------------------------------------------------------------------


def test_apply_double_submit_redirects_to_existing_job(app_client, monkeypatch):
    """When a recovery-apply job is already in flight, a second POST
    must NOT spawn a parallel worker — that would race ledger writes.
    Instead, the route redirects to the existing job's finalizing
    page so the user sees the live status of the in-flight run."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)

    # Patch heal to block on an event so the worker stays running long
    # enough for us to issue a second POST. We control the unblock so
    # the first job actually completes after the assertion.
    import threading
    block = threading.Event()

    def _slow_heal(finding, *, conn, settings, reader, bean_check=None, bulk_context=None):
        block.wait(timeout=2.0)
        return HealResult(
            success=True, message="ok",
            files_touched=(), finding_id=finding.id,
        )
    monkeypatch.setattr(
        "lamella.features.recovery.heal.heal_legacy_path", _slow_heal,
    )

    from lamella.features.recovery.repair_state import write_repair_state
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    try:
        r1 = app_client.post("/setup/recovery/apply", follow_redirects=False)
        assert r1.status_code == 303
        first_job_id = r1.headers["location"].rsplit("=", 1)[-1]

        # Second POST while the first worker is blocked. Must redirect
        # to the SAME finalizing URL — same job_id — not spawn another.
        r2 = app_client.post("/setup/recovery/apply", follow_redirects=False)
        assert r2.status_code == 303
        second_location = r2.headers["location"]
        assert second_location == f"/setup/recovery/finalizing?job={first_job_id}"
    finally:
        block.set()
        # Drain the worker so it doesn't outlive the test.
        runner = app_client.app.state.job_runner
        _wait_for_terminal(runner, first_job_id)


# ---------------------------------------------------------------------------
# Audit fix: exception message sanitization
# ---------------------------------------------------------------------------


def test_finding_failed_exception_message_is_sanitized(app_client, monkeypatch):
    """When a heal action raises a generic Exception, the message
    embedded in the FindingFailed event must NOT include the raw
    exception ``str()`` — that can leak file paths from OSError /
    sqlite3.OperationalError. The sanitized message names the
    exception type only; the full context lives in the server log."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)

    secret_path = "/etc/sensitive/path/that/should/not/leak.bean"

    def _leaking_heal(finding, *, conn, settings, reader, bean_check=None, bulk_context=None):
        raise OSError(f"could not open {secret_path}: permission denied")
    monkeypatch.setattr(
        "lamella.features.recovery.heal.heal_legacy_path", _leaking_heal,
    )

    from lamella.features.recovery.repair_state import write_repair_state
    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    job_id = r.headers["location"].rsplit("=", 1)[-1]
    runner = app_client.app.state.job_runner
    _wait_for_terminal(runner, job_id)

    events = runner.events(job_id)
    failed_events = [
        ev for ev in events
        if ev.detail and ev.detail.get("event") == "finding_failed"
    ]
    assert len(failed_events) == 1
    msg = failed_events[0].detail.get("message", "")
    # The path MUST NOT appear in the SSE payload.
    assert secret_path not in msg, (
        f"sanitization failed — secret path leaked: {msg!r}"
    )
    # The sanitized message names the exception type for debuggability.
    assert "OSError" in msg


# ---------------------------------------------------------------------------
# Gap §11.7 — durable in-flight lock blocks concurrent applies
# ---------------------------------------------------------------------------


def test_apply_blocks_when_durable_lock_held_by_other_process(
    app_client, monkeypatch,
):
    """A second concurrent /setup/recovery/apply POST is refused with
    a friendly 409 error when the durable ``setup_recovery_lock``
    row is held by something the in-process JobRunner doesn't know
    about — simulating a server restart that lost the in-memory job
    state, or a sibling CLI/scheduler process.

    The pre-existing ``runner.active()`` check covers the steady-
    state two-tabs-same-process case (test_apply_double_submit_…
    above); this test exercises the durable layer added for gap
    §11.7 specifically.
    """
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    _patch_heal_success(monkeypatch)

    from lamella.features.recovery.lock import (
        acquire_recovery_lock,
        release_recovery_lock,
    )
    from lamella.features.recovery.repair_state import write_repair_state

    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    # Pre-acquire the lock under a foreign holder — the JobRunner has
    # no active job, so the route's process-local guard won't fire,
    # but the durable lock must.
    held = acquire_recovery_lock(db, holder="job:stranded-from-prior-boot")
    assert held is None, "test setup: lock should have been free to acquire"

    try:
        r = app_client.post("/setup/recovery/apply", follow_redirects=False)
        assert r.status_code == 409
        # Friendly error names the holder + acquired_at so the user
        # can decide whether to wait or manually clear the row.
        assert "already in progress" in r.text
        assert "stranded-from-prior-boot" in r.text
    finally:
        release_recovery_lock(db)


def test_apply_releases_durable_lock_on_worker_completion(
    app_client, monkeypatch,
):
    """The worker MUST release the durable lock in its finally branch
    so a subsequent apply isn't blocked by a stranded row from the
    prior successful run."""
    f = _legacy_finding()
    _patch_detector(monkeypatch, f)
    _patch_heal_success(monkeypatch)

    from lamella.features.recovery.lock import current_lock_state
    from lamella.features.recovery.repair_state import write_repair_state

    db = app_client.app.state.db
    write_repair_state(db, {
        "findings": {f.id: {"action": "apply", "edit_payload": None}},
        "applied_history": [],
    })

    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    assert r.status_code == 303
    job_id = r.headers["location"].rsplit("=", 1)[-1]

    runner = app_client.app.state.job_runner
    _wait_for_terminal(runner, job_id)

    # Lock must be released — the row should be gone.
    state = current_lock_state(db)
    assert state is None, (
        f"worker did not release the durable lock — row still present: "
        f"holder={state.holder!r} acquired_at={state.acquired_at!r}"
    )


def test_apply_lock_helpers_are_atomic_under_concurrent_acquire():
    """Direct unit test on the lock module: two ``acquire`` calls in
    sequence must produce one success and one conflict-state, never
    two successes. Exercises the SQLite ``INSERT ... ON CONFLICT DO
    NOTHING`` semantics end-to-end (the rowcount return tells us
    whether we won the race)."""
    import sqlite3
    from lamella.features.recovery.lock import (
        acquire_recovery_lock,
        release_recovery_lock,
    )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE setup_recovery_lock ("
        "session_id TEXT NOT NULL PRIMARY KEY, "
        "holder TEXT NOT NULL, "
        "acquired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )

    # First acquire wins.
    held1 = acquire_recovery_lock(conn, holder="EntityA")
    assert held1 is None, "first acquire must succeed"

    # Second acquire (different holder) sees the conflict.
    held2 = acquire_recovery_lock(conn, holder="EntityB")
    assert held2 is not None
    assert held2.holder == "EntityA"

    # Release, then reacquire.
    release_recovery_lock(conn)
    held3 = acquire_recovery_lock(conn, holder="EntityB")
    assert held3 is None, (
        "after release the lock should be free again"
    )

    # Cleanup.
    release_recovery_lock(conn)
    conn.close()
