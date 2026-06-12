# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import pytest

from lamella.features.notes.service import NoteService


def test_create_and_list(db):
    svc = NoteService(db)
    note_id = svc.create("bought brushes at hardware store", merchant_hint="Hardware Store")
    assert note_id > 0
    rows = svc.list()
    assert len(rows) == 1
    assert rows[0].body == "bought brushes at hardware store"
    assert rows[0].status == "open"
    assert svc.count_open() == 1


def test_empty_note_rejected(db):
    svc = NoteService(db)
    with pytest.raises(ValueError):
        svc.create("   ")


def test_post_note_via_client(app_client):
    resp = app_client.post("/note", data={"body": "test note from http"})
    assert resp.status_code == 204
    page = app_client.get("/note")
    assert page.status_code == 200
    assert "test note from http" in page.text


def test_htmx_post_returns_toast(app_client):
    resp = app_client.post(
        "/note",
        data={"body": "hx test"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    assert "Saved note" in resp.text
    assert resp.headers.get("HX-Trigger") == "note-saved"
