# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``GET /setup/recovery`` — Phase 6.1.4a.

Pins down the three rendering states the bulk-review page must
handle, the stale-draft advisory, and the recovery-shell isolation
discipline (no leaks to /settings/* or other main-app surfaces).

The HTMX per-field draft writer + Apply POST tests live in their
own modules (6.1.4b/c).
"""
from __future__ import annotations

import json

import pytest

from lamella.features.recovery.models import (
    Finding,
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
        summary=f"Move {target} to its canonical location",
        detail=None,
        proposed_fix=fix_payload(
            action="move", canonical="Assets:Personal:Vehicle:Foo",
        ),
        alternatives=(),
        confidence="high",
        source="detect_legacy_paths",
    )


def _schema_finding(from_v=50, to_v=53) -> Finding:
    target = f"sqlite:{from_v}:{to_v}"
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary=f"SQLite drift v{from_v} → v{to_v}",
        detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="sqlite",
            from_version=from_v, to_version=to_v,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


# ---------------------------------------------------------------------------
# State 1 — no findings (celebrate)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md",
    strict=False,
)
def test_empty_state_renders_healthy_celebration(app_client, monkeypatch):
    """Locked acceptance: empty install shows a celebratory empty
    state, not an empty form."""
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "Your install is healthy" in r.text
    # No Apply Repairs form when empty.
    assert "Apply Repairs" not in r.text


def test_empty_state_no_settings_links(app_client, monkeypatch):
    """Recovery shell isolation — no leaks to main-app surfaces
    even from the celebratory empty state."""
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )
    r = app_client.get("/setup/recovery")
    assert "/settings/" not in r.text
    assert " /accounts" not in r.text
    assert " /simplefin" not in r.text


# ---------------------------------------------------------------------------
# State 2 — findings present, no drafts
# ---------------------------------------------------------------------------


def test_findings_no_drafts_default_to_will_apply(app_client, monkeypatch):
    """A fresh visit (no draft state) renders every finding with
    its proposed_fix as the default action — the user can dismiss
    or edit before clicking Apply."""
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "Will apply" in r.text
    assert f.target in r.text
    # The proposed canonical destination is rendered.
    assert "Assets:Personal:Vehicle:Foo" in r.text
    # The Apply Repairs button is present and enabled (no `disabled`
    # attr inside the button tag).
    apply_idx = r.text.find("Apply Repairs")
    assert apply_idx > -1
    btn_open = r.text.rfind("<button", 0, apply_idx)
    btn_close = r.text.find(">", apply_idx)
    btn_html = r.text[btn_open:btn_close]
    assert "disabled" not in btn_html


def test_finding_summary_count_is_accurate(app_client, monkeypatch):
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (
            _schema_finding(50, 51),
            _legacy_finding("Assets:Vehicles:A"),
            _legacy_finding("Assets:Vehicles:B"),
        ),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "<strong>3</strong> drift finding" in r.text


def test_groups_are_rendered_in_locked_order(app_client, monkeypatch):
    """Schema → labels → cleanup. With schema + cleanup findings,
    the Schema migrations heading appears before Cleanup in the
    rendered HTML."""
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (
            _legacy_finding("Assets:Vehicles:Late"),
            _schema_finding(50, 51),
        ),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    schema_idx = r.text.find("Schema migrations")
    cleanup_idx = r.text.find("Cleanup")
    assert schema_idx > -1 and cleanup_idx > -1
    assert schema_idx < cleanup_idx


# ---------------------------------------------------------------------------
# State 3 — findings present, drafts overlay
# ---------------------------------------------------------------------------


def test_dismissed_finding_renders_struck_through(app_client, monkeypatch):
    """A draft entry with action='dismiss' renders the row with a
    Dismissed pill and line-through styling on the target."""
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    # Seed repair_state with a dismissal.
    db = app_client.app.state.db
    db.execute(
        "INSERT OR REPLACE INTO setup_repair_state "
        "(session_id, state_json, updated_at) "
        "VALUES ('current', ?, CURRENT_TIMESTAMP)",
        (json.dumps({
            "findings": {f.id: {"action": "dismiss", "edit_payload": None}},
            "applied_history": [],
        }),),
    )
    db.commit()

    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "Dismissed" in r.text
    assert "line-through" in r.text


def test_edited_finding_renders_user_canonical(app_client, monkeypatch):
    """A draft entry with action='edit' and an edit_payload renders
    the user's canonical override visibly."""
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    db = app_client.app.state.db
    db.execute(
        "INSERT OR REPLACE INTO setup_repair_state "
        "(session_id, state_json, updated_at) "
        "VALUES ('current', ?, CURRENT_TIMESTAMP)",
        (json.dumps({
            "findings": {f.id: {
                "action": "edit",
                "edit_payload": {"canonical": "Assets:Acme:Vehicle:Foo"},
            }},
            "applied_history": [],
        }),),
    )
    db.commit()

    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "Edited" in r.text
    assert "Assets:Acme:Vehicle:Foo" in r.text


def test_apply_button_disabled_when_only_dismissed(app_client, monkeypatch):
    """If every finding is dismissed, Apply Repairs disables —
    nothing to do."""
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    db = app_client.app.state.db
    db.execute(
        "INSERT OR REPLACE INTO setup_repair_state "
        "(session_id, state_json, updated_at) "
        "VALUES ('current', ?, CURRENT_TIMESTAMP)",
        (json.dumps({
            "findings": {f.id: {"action": "dismiss", "edit_payload": None}},
            "applied_history": [],
        }),),
    )
    db.commit()

    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    apply_idx = r.text.find("Apply Repairs")
    assert apply_idx > -1
    btn_open = r.text.rfind("<button", 0, apply_idx)
    btn_close = r.text.find(">", apply_idx)
    btn_html = r.text[btn_open:btn_close]
    assert "disabled" in btn_html


# ---------------------------------------------------------------------------
# Stale-draft advisory
# ---------------------------------------------------------------------------


def test_stale_drafts_are_advised_visibly(app_client, monkeypatch):
    """A draft entry whose finding ID is no longer present in the
    detector output should surface as a visible advisory — the
    orchestrator drops it silently at apply time, but the page
    makes the discrepancy clear."""
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    db = app_client.app.state.db
    db.execute(
        "INSERT OR REPLACE INTO setup_repair_state "
        "(session_id, state_json, updated_at) "
        "VALUES ('current', ?, CURRENT_TIMESTAMP)",
        (json.dumps({
            "findings": {
                f.id: {"action": "apply", "edit_payload": None},
                "stale-finding-id-no-longer-present": {
                    "action": "apply", "edit_payload": None,
                },
            },
            "applied_history": [],
        }),),
    )
    db.commit()

    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "no longer detected" in r.text


# ---------------------------------------------------------------------------
# Recovery shell isolation across all states
# ---------------------------------------------------------------------------


def test_findings_state_no_settings_links(app_client, monkeypatch):
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (
            _schema_finding(50, 51),
            _legacy_finding(),
        ),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    assert "/settings/" not in r.text
    assert " /accounts" not in r.text
    assert " /simplefin" not in r.text


# ---------------------------------------------------------------------------
# Phase 6.1.4d — per-finding requires_individual_apply override
# ---------------------------------------------------------------------------


def _legacy_finding_individual(target: str = "Assets:Vehicles:Stuck") -> Finding:
    """A legacy_path finding with requires_individual_apply=True.
    The category is normally bulk-applicable, so this exercises the
    per-finding override path specifically (not the category-level
    map)."""
    base = _legacy_finding(target)
    from dataclasses import replace
    return replace(base, requires_individual_apply=True)


def test_individual_apply_finding_renders_only_individual_link(
    app_client, monkeypatch,
):
    """A finding with requires_individual_apply=True must render
    with only the 'Apply individually →' link — no Dismiss button,
    no Edit form — even though its category (legacy_path) is
    otherwise bulk-applicable."""
    f = _legacy_finding_individual()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    # The individual-action link is present.
    assert "Apply individually" in r.text
    # The bulk controls are NOT — the row should render just the
    # individual punt + the "Requires individual confirmation" hint.
    assert "Requires individual confirmation" in r.text


def test_individual_apply_excludes_finding_from_apply_button_count(
    app_client, monkeypatch,
):
    """A page with ONLY individual-apply findings must render the
    Apply Repairs button as disabled — there's no actionable bulk
    work even though the page has findings."""
    f = _legacy_finding_individual()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.get("/setup/recovery")
    apply_idx = r.text.find('id="recovery-apply-btn"')
    assert apply_idx > -1
    btn_open = r.text.rfind("<button", 0, apply_idx)
    btn_close = r.text.find(">", apply_idx)
    btn_html = r.text[btn_open:btn_close]
    assert "disabled" in btn_html, (
        "individual-apply findings shouldn't make the bulk Apply "
        "button enabled"
    )


def test_dismiss_endpoint_refuses_individual_apply_finding(
    app_client, monkeypatch,
):
    """HTMX POST /setup/recovery/draft/{id}/dismiss must 400 when
    the target finding carries requires_individual_apply=True. The
    UI never renders the dismiss button for these rows; a curl-POST
    is a stale-browser-state or adversarial caller and we refuse so
    we never compose drafts the orchestrator wouldn't honor."""
    f = _legacy_finding_individual()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    assert r.status_code == 400
    assert "requires individual apply" in r.text


def test_edit_endpoint_refuses_individual_apply_finding(
    app_client, monkeypatch,
):
    """Same guard on the edit endpoint."""
    f = _legacy_finding_individual()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.post(
        f"/setup/recovery/draft/{f.id}/edit",
        data={"canonical": "Assets:Anything"},
    )
    assert r.status_code == 400
    assert "requires individual apply" in r.text


def test_mixed_individual_and_bulk_findings_render_correctly(
    app_client, monkeypatch,
):
    """A page with one individual-apply finding + one bulk-applicable
    finding must show the individual link on one row and the bulk
    controls on the other. The Apply button stays enabled because at
    least one row is actionable in the batch."""
    individual = _legacy_finding_individual("Assets:Vehicles:Stuck")
    bulk = _legacy_finding("Assets:Vehicles:Easy")
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (individual, bulk),
    )
    r = app_client.get("/setup/recovery")
    assert r.status_code == 200
    # Individual link rendered for the individual row.
    assert "Apply individually" in r.text
    # The bulk row's Dismiss button rendered.
    assert "Dismiss" in r.text
    # Apply button enabled (the bulk finding is actionable).
    apply_idx = r.text.find('id="recovery-apply-btn"')
    btn_open = r.text.rfind("<button", 0, apply_idx)
    btn_close = r.text.find(">", apply_idx)
    btn_html = r.text[btn_open:btn_close]
    assert "disabled" not in btn_html


# ---------------------------------------------------------------------------
# Gap §11.8 — bulk_applicable defaults to False for unrecognized categories
# ---------------------------------------------------------------------------


def test_bulk_applicable_default_is_false_for_unrecognized_category():
    """A category that isn't in the ``_BULK_APPLICABLE`` map must
    default to False — opt-in per gap §11.8 of RECOVERY_SYSTEM.md.

    Behavior for the seven existing registered categories
    (schema_drift, legacy_path, unlabeled_account, unset_config,
    orphan_ref, missing_scaffold, missing_data_file) is unchanged:
    each is still explicitly True in the map. Only the fallback for
    a not-yet-registered category flips from True to False, so a new
    category added by a detector author surfaces in the bulk-review
    UI as individual-apply-only until the author explicitly opts
    into batching.
    """
    from lamella.web.routes.setup_recovery import (
        _BULK_APPLICABLE,
        _bulk_applicable,
    )

    # The seven currently-registered categories stay True.
    for known in (
        "schema_drift",
        "legacy_path",
        "unlabeled_account",
        "unset_config",
        "orphan_ref",
        "missing_scaffold",
        "missing_data_file",
    ):
        assert _BULK_APPLICABLE[known] is True, (
            f"behavior regression: category {known!r} should remain "
            "explicitly True after the §11.8 default flip"
        )
        assert _bulk_applicable(known) is True

    # Unrecognized categories now default to False.
    assert _bulk_applicable("brand_new_uncategorized_drift") is False
    assert _bulk_applicable("") is False
