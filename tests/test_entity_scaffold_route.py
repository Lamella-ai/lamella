# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Entity-scaffold page regressions.

Upstream report: "manage →" linked every existing category to
``/accounts/{path}``, which 404s for Expense/Income paths because
``seed_accounts_meta`` deliberately skips those trees; submitting the
form with nothing selected silently redirected to /entities; and the
computed extras list never rendered.
"""
from __future__ import annotations

from lamella.core.registry.service import upsert_entity


def _seed_personal(app_client):
    db = app_client.app.state.db
    upsert_entity(db, slug="Personal")
    db.commit()


def test_scaffold_page_renders_extras_and_safe_manage_links(app_client):
    _seed_personal(app_client)
    r = app_client.get("/settings/entities/Personal/scaffold")
    assert r.status_code == 200, r.text
    # Fixture account that isn't on the personal chart surfaces in
    # the "Not on this chart" extras section.
    assert "Expenses:Personal:Groceries" in r.text
    # Existing-but-unregistered paths must not link to /accounts/{path}
    # — that page only resolves accounts_meta rows and 404s otherwise.
    assert 'href="/accounts/Expenses:Personal' not in r.text
    assert 'href="/accounts/Income:Personal' not in r.text


def test_scaffold_empty_post_redirects_back_with_notice(app_client):
    _seed_personal(app_client)
    r = app_client.post(
        "/settings/entities/Personal/scaffold",
        data={},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"] == (
        "/settings/entities/Personal/scaffold?notice=nothing-selected"
    )
    r2 = app_client.get(r.headers["location"])
    assert r2.status_code == 200
    assert "Nothing to create" in r2.text
