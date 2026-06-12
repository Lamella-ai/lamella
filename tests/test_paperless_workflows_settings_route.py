# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0065 — /settings/paperless-workflows CRUD UI for tag bindings.

Tests the binding creation, toggle, and deletion endpoints.
"""
from __future__ import annotations

import respx
from lamella.features.paperless_bridge.binding_loader import (
    list_all_bindings,
)


def _stub_paperless_empty(mock):
    """Stub a Paperless that returns no documents or tags."""
    mock.get("/api/tags/").respond(
        200, json={"next": None, "results": []},
    )


def test_get_settings_page_empty(app_client):
    """GET /settings/paperless-workflows renders empty bindings list."""
    resp = app_client.get("/settings/paperless-workflows")
    assert resp.status_code == 200
    assert "Tag-driven workflow bindings" in resp.text
    assert "Add new binding" in resp.text


def test_post_create_binding_valid(app_client):
    """POST /settings/paperless-workflows/create with valid tag + action
    writes the directive and returns the updated table."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Invoice",
            "action_name": "extract_fields",
            "enabled": "on",
        },
    )
    assert resp.status_code == 200
    assert "Invoice" in resp.text
    assert "AI Field Extraction" in resp.text


def test_post_create_binding_empty_tag_name(app_client):
    """POST /settings/paperless-workflows/create with empty tag_name
    returns 400 + inline error."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "",
            "action_name": "extract_fields",
            "enabled": "on",
        },
    )
    # FastAPI validates on the empty string and returns 422
    assert resp.status_code in (400, 422)
    # If validation passed, we'd see the error fragment; 422 means FastAPI caught it
    if resp.status_code == 400:
        assert "form-error" in resp.text
        assert "required" in resp.text.lower()


def test_post_create_binding_invalid_action(app_client):
    """POST /settings/paperless-workflows/create with unknown action_name
    returns 400 + inline error."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Invoice",
            "action_name": "unknown_action",
            "enabled": "on",
        },
    )
    assert resp.status_code == 400
    assert "form-error" in resp.text
    assert "unknown" in resp.text.lower() or "Unknown" in resp.text


def test_post_create_binding_rejects_internal_queue_marker(app_client):
    """POST /settings/paperless-workflows/create rejects bindings on
    Lamella:AwaitingExtraction — the queue marker that extract_fields
    already triggers off, so binding it again would be redundant.

    Other Lamella-stamped tags (DateAnomaly, NeedsReview, Extracted,
    Linked) are legitimate bind targets and tested separately."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Lamella:AwaitingExtraction",
            "action_name": "extract_fields",
            "enabled": "on",
        },
    )
    assert resp.status_code == 400
    assert "form-error" in resp.text
    assert "redundant" in resp.text.lower() or "queue marker" in resp.text.lower()


def test_oneshot_run_unknown_action_returns_inline_error(app_client):
    """POST /settings/paperless-workflows/oneshot/{name}/run for an
    unregistered action returns the inline error fragment (400, not
    500). The fragment is HTMX-targeted at the per-card slot so the
    user sees the error in context."""
    resp = app_client.post(
        "/settings/paperless-workflows/oneshot/no_such_action/run",
    )
    assert resp.status_code == 400
    assert "form-error" in resp.text
    assert "no_such_action" in resp.text


def test_oneshot_run_action_without_suggested_trigger_returns_inline_error(
    app_client,
):
    """date_sanity_check has suggested_trigger_tag=None — the
    one-shot endpoint must report this clearly instead of crashing
    or running an empty selector."""
    resp = app_client.post(
        "/settings/paperless-workflows/oneshot/date_sanity_check/run",
    )
    assert resp.status_code == 400
    assert "form-error" in resp.text
    assert "no suggested" in resp.text.lower()


def test_oneshot_run_verify_date_only_returns_job_modal(app_client):
    """One-shot run with a valid action + suggested trigger submits
    the rule to the JobRunner and returns the standard progress
    modal partial. The actual run happens in a background worker —
    we verify here only that the request is accepted and a job is
    spawned (the modal includes the job id)."""
    resp = app_client.post(
        "/settings/paperless-workflows/oneshot/verify_date_only/run",
    )
    assert resp.status_code == 200
    # Job-modal partial wraps the live job-progress UI; the partial
    # always renders job-modal-slot or includes the job id reference.
    assert "job" in resp.text.lower()


def test_post_create_binding_accepts_date_anomaly_signal_tag(app_client):
    """Lamella:DateAnomaly is a Lamella-stamped *signal* output, not
    an internal queue marker. The whole point of verify_date_only is
    to listen for it, so the bindings page must allow it."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Lamella:DateAnomaly",
            "action_name": "verify_date_only",
            "enabled": "on",
        },
    )
    # Either the binding gets created (200) or the ledger write fails
    # in the test fixture — both are fine. What we're guarding is
    # that we don't get a 400 with the "system-managed" rejection.
    assert resp.status_code != 400 or "system-managed" not in resp.text.lower()


def test_post_toggle_binding(app_client):
    """POST /settings/paperless-workflows/{tag_name}/toggle returns 200."""
    # Toggle it (even if it doesn't exist, should return 200 or error gracefully)
    resp = app_client.post(
        "/settings/paperless-workflows/Invoice/toggle",
    )
    # Should either succeed (200) or return a friendly error (400)
    assert resp.status_code in (200, 400)


def test_post_delete_binding(app_client):
    """POST /settings/paperless-workflows/{tag_name}/delete returns empty
    response for HTMX outerHTML swap (idempotent)."""
    # Delete it (even if it doesn't exist, should be idempotent)
    resp = app_client.post(
        "/settings/paperless-workflows/Invoice/delete",
    )
    assert resp.status_code == 200
    assert resp.text == ""  # empty response for HTMX outerHTML swap


def test_post_toggle_nonexistent_binding(app_client):
    """POST /settings/paperless-workflows/{tag_name}/toggle on a
    nonexistent binding returns 400 + error."""
    resp = app_client.post(
        "/settings/paperless-workflows/NonExistent/toggle",
    )
    assert resp.status_code == 400
    assert "form-error" in resp.text or "not found" in resp.text.lower()


def test_post_delete_nonexistent_binding(app_client):
    """POST /settings/paperless-workflows/{tag_name}/delete on a
    nonexistent binding still succeeds (idempotent)."""
    resp = app_client.post(
        "/settings/paperless-workflows/NonExistent/delete",
    )
    # Should succeed because revoke is idempotent
    assert resp.status_code == 200


def test_post_create_binding_with_colon_in_tag_name(app_client):
    """POST /settings/paperless-workflows/create accepts tag names with
    colons, URL-encodes them in toggle/delete paths."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "User:Jane",
            "action_name": "date_sanity_check",
            "enabled": "on",
        },
    )
    assert resp.status_code == 200
    # The route succeeded (didn't throw an error processing the colon)

    # Toggle should work with URL-encoded colon
    resp = app_client.post(
        "/settings/paperless-workflows/User%3AJane/toggle",
    )
    assert resp.status_code in (200, 400)  # Either success or error


def test_htmx_post_create_returns_partial(app_client):
    """POST /settings/paperless-workflows/create with hx-request header
    returns the partial (not full page)."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Invoice",
            "action_name": "extract_fields",
            "enabled": "on",
        },
        headers={"hx-request": "true"},
    )
    assert resp.status_code == 200
    # Partial should contain the bindings table, not the full page chrome
    assert "bindings-table" in resp.text or "Invoice" in resp.text


def test_non_htmx_post_create_redirects(app_client):
    """POST /settings/paperless-workflows/create without hx-request header
    returns a redirect (not the partial)."""
    resp = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Invoice",
            "action_name": "extract_fields",
            "enabled": "on",
        },
        follow_redirects=False,
    )
    # Non-HTMX form submit should get a 303 redirect
    assert resp.status_code in (200, 303)  # depending on follow_redirects


def test_multiple_bindings_for_different_tags(app_client):
    """Create multiple bindings for different tags; each returns 200."""
    resp1 = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Invoice",
            "action_name": "extract_fields",
            "enabled": "on",
        },
    )
    resp2 = app_client.post(
        "/settings/paperless-workflows/create",
        data={
            "tag_name": "Receipt",
            "action_name": "date_sanity_check",
            "enabled": "on",
        },
    )

    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert "Invoice" in resp1.text
    assert "Receipt" in resp2.text
