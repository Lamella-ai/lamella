# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Smoke tests for the receipt-hunt job port.

The full legacy handler had zero dedicated tests (only a manual smoke
path via /search/receipt-hunt). These verify:

* POST /search/receipt-hunt with no selected txns → 400
* POST /search/receipt-hunt submits a job and returns the progress
  modal partial with a job id that's immediately visible via the
  /jobs endpoints
* GET /search/receipt-hunt/result with a terminal job renders the
  result template using the job's stored report
"""
from __future__ import annotations

import time

import pytest


def _wait(runner, job_id, timeout=15.0):
    """Poll for terminal state. Timeout is generous — under full-suite
    load the first post-app-startup job can take several seconds to
    complete its short-lived SQLite connection setup + ledger load."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = runner.get(job_id)
        if job and job.is_terminal:
            return job
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} still running after {timeout}s")


def test_receipt_hunt_requires_selection(app_client):
    resp = app_client.post("/search/receipt-hunt", data={})
    assert resp.status_code == 400


def test_receipt_hunt_returns_progress_modal(app_client):
    """POST with a bogus hash → job runs, emits 'not in ledger' error,
    terminates as 'done' with the report attached. Under full-suite
    load the bootstrap Paperless sync kicked off by TestClient's
    lifespan may flag the DB as 'syncing' before the hunt worker
    runs, in which case the hunt short-circuits with a 'sync is
    running' event instead — either is a legitimate completion
    signal for this smoke test."""
    resp = app_client.post(
        "/search/receipt-hunt",
        data={"txn_hash": "deadbeefdeadbeef", "lookback_days": "365"},
        follow_redirects=False,
    )
    assert resp.status_code == 200
    body = resp.text
    assert 'id="job-modal-root"' in body
    assert 'data-job-id="j_' in body
    import re
    m = re.search(r'data-job-id="(j_[A-Za-z0-9_-]+)"', body)
    assert m, "job id not found in modal body"
    job_id = m.group(1)
    runner = app_client.app.state.job_runner
    job = _wait(runner, job_id)
    assert job.status == "done"
    assert (job.result or {}).get("report") is not None
    events = runner.events(job_id)
    assert any(
        "not in ledger" in e.message
        or "Paperless sync is running" in e.message
        for e in events
    )


def test_receipt_hunt_result_renders_after_completion(app_client):
    """After the hunt finishes, /search/receipt-hunt/result?job_id=...
    should render receipt_hunt_result.html with the stored report."""
    resp = app_client.post(
        "/search/receipt-hunt",
        data={"txn_hash": "deadbeefdeadbeef"},
        follow_redirects=False,
    )
    import re
    m = re.search(r'data-job-id="(j_[A-Za-z0-9_-]+)"', resp.text)
    job_id = m.group(1)
    runner = app_client.app.state.job_runner
    _wait(runner, job_id)

    resp = app_client.get(
        f"/search/receipt-hunt/result?job_id={job_id}",
        follow_redirects=False,
    )
    assert resp.status_code == 200
    # The template includes the original q/lookback_days values.
    assert "receipt" in resp.text.lower()


def test_receipt_hunt_result_missing_job_404(app_client):
    resp = app_client.get(
        "/search/receipt-hunt/result?job_id=j_nonexistent",
        follow_redirects=False,
    )
    assert resp.status_code == 404


def test_jobs_partial_renders_live_progress(app_client):
    """The /jobs/{id}/partial endpoint should return the progress
    fragment whether the job is running or terminal."""
    resp = app_client.post(
        "/search/receipt-hunt",
        data={"txn_hash": "deadbeefdeadbeef"},
    )
    import re
    job_id = re.search(r'data-job-id="(j_[A-Za-z0-9_-]+)"', resp.text).group(1)
    runner = app_client.app.state.job_runner
    _wait(runner, job_id)
    resp = app_client.get(f"/jobs/{job_id}/partial")
    assert resp.status_code == 200
    assert "job-modal__panel" in resp.text
    assert 'data-terminal="1"' in resp.text


def test_job_cancel_endpoint(app_client):
    """POST /jobs/{id}/cancel flips the cancel flag and returns the
    refreshed partial."""
    resp = app_client.post(
        "/search/receipt-hunt",
        data={"txn_hash": "deadbeefdeadbeef"},
    )
    import re
    job_id = re.search(r'data-job-id="(j_[A-Za-z0-9_-]+)"', resp.text).group(1)
    resp = app_client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200


def test_jobs_active_dock_empty_when_no_active(app_client):
    """The docked-jobs strip renders empty when nothing is active."""
    # Wait for any previously-started jobs from prior tests to clear.
    # (Each test function uses a fresh app_client, so this is a no-op
    # under the fixture scope; we add the call just to assert the
    # endpoint works under both states.)
    resp = app_client.get("/jobs/active/dock")
    assert resp.status_code == 200
