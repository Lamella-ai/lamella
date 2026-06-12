# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the HTMX draft writers — Phase 6.1.4b.

Eight acceptance cases per the locked sub-freeze:

1. POST dismiss → row re-rendered as dismissed, repair state updated
2. POST dismiss again (toggle) → row re-rendered as apply, repair
   state updated
3. POST edit with valid canonical → row re-rendered with edit
   applied, repair state edit_payload set
4. POST edit with invalid canonical → row re-rendered with inline
   error, user input preserved, repair state unchanged
5. POST edit on non-legacy_path finding → 400
6. POST on unknown finding_id → 404
7. Two writes in sequence both succeed; second sees first's effect
   (read-modify-write correctness)
8. Apply-button OOB swap reflects per-row dismiss state changes
   correctly after HTMX writes
"""
from __future__ import annotations

import json

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
        summary=f"Move {target}",
        detail=None,
        proposed_fix=fix_payload(
            action="move", canonical="Assets:Personal:Vehicle:Foo",
        ),
        alternatives=(),
        confidence="high",
        source="detect_legacy_paths",
    )


def _schema_finding() -> Finding:
    target = "sqlite:50:53"
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary="SQLite drift v50 → v53",
        detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="sqlite",
            from_version=50, to_version=53,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


def _read_state(app_client) -> dict:
    db = app_client.app.state.db
    row = db.execute(
        "SELECT state_json FROM setup_repair_state "
        "WHERE session_id = 'current'"
    ).fetchone()
    if row is None:
        return {"findings": {}, "applied_history": []}
    return json.loads(row[0])


# ---------------------------------------------------------------------------
# Case 1 + 2 — Dismiss toggle
# ---------------------------------------------------------------------------


def test_dismiss_toggles_to_dismissed_and_persists(app_client, monkeypatch):
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    assert r.status_code == 200
    # The row re-renders as Dismissed.
    assert "Dismissed" in r.text
    assert "Restore" in r.text
    # Repair state persisted.
    state = _read_state(app_client)
    assert state["findings"][f.id]["action"] == "dismiss"


def test_dismiss_toggles_back_to_apply(app_client, monkeypatch):
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    # First dismiss.
    app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    # Toggle back.
    r = app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    assert r.status_code == 200
    assert "Will apply" in r.text
    state = _read_state(app_client)
    assert state["findings"][f.id]["action"] == "apply"


# ---------------------------------------------------------------------------
# Case 3 + 4 — Edit canonical
# ---------------------------------------------------------------------------


def test_edit_with_valid_canonical_persists_edit_payload(
    app_client, monkeypatch, tmp_path,
):
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    # The destination guard requires the parent path to be opened
    # in the ledger. Stub it so any canonical is accepted.
    monkeypatch.setattr(
        "lamella.features.recovery.findings.legacy_paths."
        "_passes_destination_guards",
        lambda canonical, opened: True,
    )

    r = app_client.post(
        f"/setup/recovery/draft/{f.id}/edit",
        data={"canonical": "Assets:Acme:Vehicle:Custom"},
    )
    assert r.status_code == 200
    assert "Edited" in r.text
    assert "Assets:Acme:Vehicle:Custom" in r.text
    state = _read_state(app_client)
    assert state["findings"][f.id]["action"] == "edit"
    assert state["findings"][f.id]["edit_payload"] == {
        "canonical": "Assets:Acme:Vehicle:Custom",
    }


def test_edit_with_invalid_canonical_preserves_input_and_errors(
    app_client, monkeypatch,
):
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    # Force the guard to refuse — simulates a path whose parent
    # isn't opened in the ledger.
    monkeypatch.setattr(
        "lamella.features.recovery.findings.legacy_paths."
        "_passes_destination_guards",
        lambda canonical, opened: False,
    )

    r = app_client.post(
        f"/setup/recovery/draft/{f.id}/edit",
        data={"canonical": "Assets:Bogus:Path"},
    )
    assert r.status_code == 200
    # User's input is preserved in the form.
    assert "Assets:Bogus:Path" in r.text
    # An inline error is rendered. (Substring without an apostrophe
    # since Jinja HTML-escapes "doesn't" → "doesn&#39;t".)
    assert "move-target guards" in r.text
    # Repair state was NOT modified.
    state = _read_state(app_client)
    assert f.id not in state["findings"]


def test_edit_empty_canonical_returns_required_error(
    app_client, monkeypatch,
):
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.post(
        f"/setup/recovery/draft/{f.id}/edit",
        data={"canonical": "   "},
    )
    assert r.status_code == 200
    assert "required" in r.text
    state = _read_state(app_client)
    assert f.id not in state["findings"]


# ---------------------------------------------------------------------------
# Case 5 + 6 — Edit on wrong category, unknown ID
# ---------------------------------------------------------------------------


def test_edit_on_non_legacy_path_returns_400(app_client, monkeypatch):
    f = _schema_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.post(
        f"/setup/recovery/draft/{f.id}/edit",
        data={"canonical": "Assets:Anything"},
    )
    assert r.status_code == 400
    assert "does not support edit" in r.text


def test_dismiss_unknown_finding_returns_404(app_client, monkeypatch):
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )
    r = app_client.post("/setup/recovery/draft/legacy_path:abc123/dismiss")
    assert r.status_code == 404


def test_edit_unknown_finding_returns_404(app_client, monkeypatch):
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (),
    )
    r = app_client.post(
        "/setup/recovery/draft/legacy_path:abc123/edit",
        data={"canonical": "Assets:Anything"},
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Case 7 — Two writes in sequence
# ---------------------------------------------------------------------------


def test_two_writes_in_sequence_compose_correctly(app_client, monkeypatch):
    """Read-modify-write correctness: two sequential writes on
    different fields/findings both land. The second sees the first's
    effect rather than clobbering it."""
    f1 = _legacy_finding("Assets:Vehicles:A")
    f2 = _legacy_finding("Assets:Vehicles:B")
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f1, f2),
    )
    monkeypatch.setattr(
        "lamella.features.recovery.findings.legacy_paths."
        "_passes_destination_guards",
        lambda canonical, opened: True,
    )

    # First write: dismiss f1.
    r1 = app_client.post(f"/setup/recovery/draft/{f1.id}/dismiss")
    assert r1.status_code == 200
    # Second write: edit f2.
    r2 = app_client.post(
        f"/setup/recovery/draft/{f2.id}/edit",
        data={"canonical": "Assets:Acme:Vehicle:B"},
    )
    assert r2.status_code == 200

    # Both effects persisted.
    state = _read_state(app_client)
    assert state["findings"][f1.id]["action"] == "dismiss"
    assert state["findings"][f2.id]["action"] == "edit"
    assert state["findings"][f2.id]["edit_payload"] == {
        "canonical": "Assets:Acme:Vehicle:B",
    }


# ---------------------------------------------------------------------------
# Case 8 — Apply-button OOB swap stays in sync
# ---------------------------------------------------------------------------


def test_dismiss_response_includes_oob_apply_button_disabled(
    app_client, monkeypatch,
):
    """When the only finding gets dismissed, the OOB Apply button
    in the response renders disabled — the page-level button
    updates without a reload."""
    f = _legacy_finding()
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f,),
    )
    r = app_client.post(f"/setup/recovery/draft/{f.id}/dismiss")
    assert r.status_code == 200
    # The OOB Apply button is included in the response.
    assert 'id="recovery-apply-btn"' in r.text
    assert 'hx-swap-oob="true"' in r.text
    # And it's disabled because nothing actionable remains.
    apply_idx = r.text.find('id="recovery-apply-btn"')
    btn_open = r.text.rfind("<button", 0, apply_idx)
    btn_close = r.text.find(">", apply_idx)
    btn_html = r.text[btn_open:btn_close]
    assert "disabled" in btn_html


def test_dismiss_response_oob_apply_enabled_when_others_remain(
    app_client, monkeypatch,
):
    """Dismissing one finding when others remain actionable — the
    OOB Apply button stays enabled."""
    f1 = _legacy_finding("Assets:Vehicles:A")
    f2 = _legacy_finding("Assets:Vehicles:B")
    monkeypatch.setattr(
        "lamella.web.routes.setup_recovery.detect_all",
        lambda conn, entries: (f1, f2),
    )
    r = app_client.post(f"/setup/recovery/draft/{f1.id}/dismiss")
    assert r.status_code == 200
    apply_idx = r.text.find('id="recovery-apply-btn"')
    assert apply_idx > -1
    btn_open = r.text.rfind("<button", 0, apply_idx)
    btn_close = r.text.find(">", apply_idx)
    btn_html = r.text[btn_open:btn_close]
    assert "disabled" not in btn_html
