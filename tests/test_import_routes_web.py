# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression tests for the /import web routes.

Covers the upstream-reported fixes:

* ``IngestSummary.duplicate_rows`` exists — the ingest job's progress
  line reads it, and it used to raise ``AttributeError`` because the
  count only lived in ``summary.transfers["duplicates"]``.
* Classify / preview entity pickers come from the entity registry,
  not a hardcoded demo tuple (Acme, WidgetCo, …).
* Hard delete is a standard form POST (CSRF + confirm modal ride the
  shared form pipeline) instead of a hand-rolled ``fetch`` DELETE
  that silently redirected on failure.
"""
from __future__ import annotations

import re

from lamella.core.registry.service import upsert_entity
from lamella.features.import_.service import IngestSummary
from lamella.web.routes.import_ import _entity_choices


# ─── IngestSummary.duplicate_rows ──────────────────────────────────


def test_ingest_summary_duplicate_rows_defaults_to_zero():
    assert IngestSummary().duplicate_rows == 0


def test_ingest_summary_duplicate_rows_reads_transfer_counts():
    s = IngestSummary(transfers={"duplicates": 3, "transfers": 1})
    assert s.duplicate_rows == 3


# ─── registry-backed entity choices ────────────────────────────────


def test_entity_choices_come_from_registry(db):
    upsert_entity(db, slug="ExampleLLC", display_name="Example LLC")
    upsert_entity(db, slug="Personal")
    upsert_entity(db, slug="Ceased", is_active=0)
    db.commit()
    choices = _entity_choices(db)
    slugs = [c["slug"] for c in choices]
    assert "ExampleLLC" in slugs
    assert "Personal" in slugs
    assert "Ceased" not in slugs
    labels = {c["slug"]: c["label"] for c in choices}
    assert labels["ExampleLLC"] == "Example LLC"
    assert labels["Personal"] == "Personal"


def _upload_csv(app_client) -> int:
    r = app_client.post(
        "/import",
        files={
            "file": (
                "sample.csv",
                b"Date,Amount,Description\n2026-01-05,-12.34,Acme Co. supplies\n",
                "text/csv",
            ),
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text
    m = re.search(r"/import/(\d+)", r.headers["location"])
    assert m, r.headers["location"]
    return int(m.group(1))


def test_classify_page_lists_registry_entities(app_client):
    db = app_client.app.state.db
    upsert_entity(db, slug="ExampleLLC", display_name="Example LLC")
    db.commit()
    import_id = _upload_csv(app_client)
    r = app_client.get(f"/import/{import_id}/classify")
    assert r.status_code == 200
    assert "Example LLC" in r.text
    # The old hardcoded demo tuple must be gone.
    assert "WidgetCo" not in r.text
    assert "ThetaCo" not in r.text


def test_import_detail_json_route_reachable(app_client):
    """Regression: the .json route used to register after /{import_id},
    so '1.json' hit the detail route, failed the int parse, and 422'd."""
    import_id = _upload_csv(app_client)
    r = app_client.get(f"/import/{import_id}.json")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == import_id
    assert body["filename"] == "sample.csv"


# ─── hard delete via form POST ─────────────────────────────────────


def test_hard_delete_via_form_post(app_client):
    import_id = _upload_csv(app_client)
    r = app_client.post(f"/import/{import_id}/cancel", follow_redirects=False)
    assert r.status_code == 303
    r = app_client.post(f"/import/{import_id}/delete", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/import"
    assert app_client.get(f"/import/{import_id}").status_code == 404
    assert app_client.get(f"/import/{import_id}.json").status_code == 404


def test_hard_delete_form_post_refused_for_active_import(app_client):
    import_id = _upload_csv(app_client)
    # Not cancelled / errored — the service refuses, surfaced as 400
    # (previously the broken fetch() handler swallowed this).
    r = app_client.post(f"/import/{import_id}/delete", follow_redirects=False)
    assert r.status_code == 400
    assert app_client.get(f"/import/{import_id}").status_code == 200
