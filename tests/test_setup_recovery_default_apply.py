# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression: opening /setup/recovery seeds default-apply drafts for
every finding the user is seeing. Without this seed, clicking the
"Apply Repairs" button on a fresh page (no per-row interaction)
silently 303s back because the orchestrator's closed-world batch
composition skips findings without an explicit draft entry."""
from __future__ import annotations

from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)


def _schema_drift_finding() -> Finding:
    target = "ledger:2:3"
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary="ledger v2 → v3",
        detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="ledger",
            from_version="2", to_version=3,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


def _patch_detector(monkeypatch, *findings: Finding) -> None:
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: findings,
    )
    monkeypatch.setattr(
        "lamella.features.recovery.bulk_apply.detect_all",
        lambda conn, entries: findings,
    )


def test_recovery_get_seeds_default_apply_drafts(app_client, monkeypatch):
    """First load of /setup/recovery should write an action='apply'
    draft for every detected finding so a downstream Apply Repairs
    click — even without per-row interaction — has actionable
    composition."""
    f = _schema_drift_finding()
    _patch_detector(monkeypatch, f)

    # Pre-condition: nothing in repair_state.
    from lamella.features.recovery.repair_state import read_repair_state
    state_before = read_repair_state(app_client.app.state.db)
    assert state_before.get("findings", {}).get(f.id) is None

    # Render the page.
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200

    # Post-condition: the draft now exists with action=apply.
    state_after = read_repair_state(app_client.app.state.db)
    drafts = state_after.get("findings", {})
    assert f.id in drafts, (
        "GET /setup/recovery should seed a default draft for every "
        "finding the user is seeing"
    )
    assert drafts[f.id]["action"] == "apply"


def test_apply_repairs_works_without_per_row_interaction(
    app_client, monkeypatch,
):
    """The original bug: clicking Apply Repairs on a fresh page (no
    per-row toggle clicks) silently bounced back to /setup/recovery
    because the orchestrator filtered to "explicit drafts" and found
    none. After the seed-on-render fix, the click submits a real job."""
    f = _schema_drift_finding()
    _patch_detector(monkeypatch, f)

    # Visit the page so defaults seed.
    app_client.get("/setup/recovery")

    # Submit the bulk apply.
    r = app_client.post("/setup/recovery/apply", follow_redirects=False)
    # Should redirect to the finalizing page (303), not bounce back
    # to /setup/recovery (the no-actionable-drafts case).
    assert r.status_code == 303, (
        "apply should submit a job, not bounce back; got "
        f"{r.status_code} → {r.headers.get('location')}"
    )
    assert r.headers["location"].startswith("/setup/recovery/finalizing")


def test_recovery_get_preserves_existing_user_drafts(app_client, monkeypatch):
    """If the user already composed drafts (e.g. dismissed a row),
    re-rendering should NOT overwrite their choice with default-apply."""
    f = _schema_drift_finding()
    _patch_detector(monkeypatch, f)

    # User dismisses the row.
    r = app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    assert r.status_code in {200, 204}

    # Re-render the page.
    app_client.get("/setup/recovery")

    # The dismiss should have survived the re-render's seeding pass.
    from lamella.features.recovery.repair_state import read_repair_state
    state = read_repair_state(app_client.app.state.db)
    assert state["findings"][f.id]["action"] == "dismiss"
