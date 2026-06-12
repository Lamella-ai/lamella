# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``/setup/recovery/schema`` — Phase 5.5.

Pins down: (a) GET renders 200 with the recovery layout (no app
sidebar / settings links), (b) when no drift is detected the
"in sync" empty state shows, (c) injected findings render with
axis label + version pair, (d) the confirmation route shows the
Migration's dry-run preview, (e) POST /heal redirects with the
heal result message, (f) no ``/settings/*``, ``/accounts``, or
``/simplefin`` link in any rendered response.
"""
from __future__ import annotations

import pytest

from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)


def _fake_finding(axis: str, from_version, to_version: int) -> Finding:
    target = f"{axis}:{from_version}:{to_version}"
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary=f"{axis.capitalize()} drift v{from_version} → v{to_version}",
        detail=f"Detail for {axis} drift.",
        proposed_fix=fix_payload(
            action="migrate", axis=axis,
            from_version=from_version, to_version=to_version,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


# ---------------------------------------------------------------------------
# Empty / no-drift state
# ---------------------------------------------------------------------------


def test_get_schema_drift_in_sync_renders_empty_state(app_client):
    r = app_client.get("/setup/recovery/schema")
    assert r.status_code == 200
    # With the app_client's default ledger (carries legacy bcg-* stamp,
    # normalized to lamella-* on read) and a fully-migrated SQLite,
    # the detector returns no findings.
    assert "Schema is in sync" in r.text


def test_no_settings_links_in_empty_state(app_client):
    """Locked Phase 4 acceptance carried into Phase 5: no
    ``/settings/*`` link surfaces from a recovery surface, in any
    state."""
    r = app_client.get("/setup/recovery/schema")
    assert r.status_code == 200
    assert "/settings/" not in r.text
    # Bare /accounts and /simplefin (with leading space to avoid
    # matching /accounts_meta etc).
    assert " /accounts" not in r.text
    assert " /simplefin" not in r.text


# ---------------------------------------------------------------------------
# Drift detected — list view
# ---------------------------------------------------------------------------


def test_drift_findings_render_with_axis_and_versions(
    app_client, monkeypatch,
):
    """Inject a SQLite drift finding via monkey-patch and verify the
    list page renders the axis label, version pair, and a link to
    the confirmation step (not a direct heal POST)."""
    sqlite_finding = _fake_finding("sqlite", 50, 53)

    monkeypatch.setattr(
        "lamella.web.routes.setup_schema.detect_schema_drift",
        lambda conn, entries: (sqlite_finding,),
    )

    r = app_client.get("/setup/recovery/schema")
    assert r.status_code == 200
    assert "SQLite axis" in r.text
    assert "v50" in r.text and "v53" in r.text
    # The Apply button is a link to /confirm, NOT a POST form
    # directly to /heal — confirm step is mandatory.
    assert (
        f"/setup/recovery/schema/confirm?finding_id={sqlite_finding.id}"
        in r.text
    )
    # Does not directly post to /heal yet.
    assert (
        "<form" not in r.text or 'action="/setup/recovery/schema/heal"' not in r.text
    )


def test_drift_list_no_settings_links(app_client, monkeypatch):
    sqlite_finding = _fake_finding("sqlite", 50, 53)
    ledger_finding = _fake_finding("ledger", "none", 1)

    monkeypatch.setattr(
        "lamella.web.routes.setup_schema.detect_schema_drift",
        lambda conn, entries: (sqlite_finding, ledger_finding),
    )

    r = app_client.get("/setup/recovery/schema")
    assert r.status_code == 200
    assert "/settings/" not in r.text
    assert " /accounts" not in r.text
    assert " /simplefin" not in r.text


# ---------------------------------------------------------------------------
# Confirmation route
# ---------------------------------------------------------------------------


def test_confirm_renders_dry_run_preview(app_client, monkeypatch):
    """The confirm route runs the Migration's dry_run and renders
    its summary + detail."""
    sqlite_finding = _fake_finding("sqlite", 50, 53)
    monkeypatch.setattr(
        "lamella.web.routes.setup_schema.detect_schema_drift",
        lambda conn, entries: (sqlite_finding,),
    )

    r = app_client.get(
        f"/setup/recovery/schema/confirm?finding_id={sqlite_finding.id}",
    )
    assert r.status_code == 200
    # The CatchUpSqliteMigrations dry_run says "No pending SQLite
    # migrations" because the test DB is already migrated. That
    # IS the preview text — verify the preview surface rendered it.
    assert "No pending SQLite migrations" in r.text
    # Apply button visible.
    assert "Apply migration" in r.text
    # No /settings/, /accounts, /simplefin leak.
    assert "/settings/" not in r.text


def test_confirm_unknown_finding_redirects(app_client, monkeypatch):
    """If the finding_id isn't in the current detector output (drift
    healed out-of-band), redirect to the listing with an info banner."""
    monkeypatch.setattr(
        "lamella.web.routes.setup_schema.detect_schema_drift",
        lambda conn, entries: (),
    )
    r = app_client.get(
        "/setup/recovery/schema/confirm?finding_id=schema_drift:abcdef123456",
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"].startswith("/setup/recovery/schema?last=")


# ---------------------------------------------------------------------------
# POST heal
# ---------------------------------------------------------------------------


def test_post_heal_redirects_with_result_message(app_client, monkeypatch):
    """POST /heal with a known finding_id runs heal_schema_drift and
    redirects with the result message + last_ok flag."""
    sqlite_finding = _fake_finding("sqlite", 50, 53)
    monkeypatch.setattr(
        "lamella.web.routes.setup_schema.detect_schema_drift",
        lambda conn, entries: (sqlite_finding,),
    )

    r = app_client.post(
        "/setup/recovery/schema/heal",
        data={"finding_id": sqlite_finding.id},
        follow_redirects=False,
    )
    assert r.status_code == 303
    location = r.headers["location"]
    assert location.startswith("/setup/recovery/schema?")
    # Carries the success flag (no pending migrations on a fully-
    # migrated test DB → CatchUpSqliteMigrations.apply is a no-op
    # → result.success=True).
    assert "last_ok=1" in location


def test_post_heal_unknown_finding_redirects_info(app_client, monkeypatch):
    """POST with a finding_id that no longer matches detector output
    redirects without raising — same race-condition handling as the
    confirm route."""
    monkeypatch.setattr(
        "lamella.web.routes.setup_schema.detect_schema_drift",
        lambda conn, entries: (),
    )
    r = app_client.post(
        "/setup/recovery/schema/heal",
        data={"finding_id": "schema_drift:abcdef123456"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"].startswith("/setup/recovery/schema?last=")
