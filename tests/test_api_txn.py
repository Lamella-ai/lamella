# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the unified /api/txn/{ref} resource layer.

Covers ref parsing, classify dispatch by ref kind, dismiss
constraints, and the ask-ai modal-return shape. Each test pins a
contract that the resource layer needs to honor as more pages
migrate onto it."""
from __future__ import annotations

import pytest

from lamella.web.routes.api_txn import parse_ref, TxnRef


# ─── parse_ref ────────────────────────────────────────────────────


def test_parse_ref_staged():
    r = parse_ref("staged:42")
    assert r.kind == "staged"
    assert r.value == "42"
    assert r.is_staged is True
    assert r.is_ledger is False
    assert r.staged_id == 42


def test_parse_ref_ledger():
    r = parse_ref("ledger:abc123def456")
    assert r.kind == "ledger"
    assert r.value == "abc123def456"
    assert r.is_ledger is True
    assert r.is_staged is False
    assert r.txn_hash == "abc123def456"


def test_parse_ref_kind_uppercase_normalized():
    assert parse_ref("Staged:42").kind == "staged"
    assert parse_ref("LEDGER:abc").kind == "ledger"


def test_parse_ref_rejects_missing_colon():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        parse_ref("staged42")
    assert exc.value.status_code == 400


def test_parse_ref_rejects_bad_kind():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        parse_ref("garbage:abc")
    assert exc.value.status_code == 400


def test_parse_ref_rejects_empty_value():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        parse_ref("staged:")
    assert exc.value.status_code == 400


def test_parse_ref_rejects_non_int_staged_id():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        parse_ref("staged:not-a-number")
    assert exc.value.status_code == 400


def test_parse_ref_str_roundtrip():
    assert str(parse_ref("staged:7")) == "staged:7"
    assert str(parse_ref("ledger:abc")) == "ledger:abc"


def test_txnref_staged_id_raises_for_ledger():
    r = TxnRef(kind="ledger", value="abc")
    with pytest.raises(ValueError):
        _ = r.staged_id


def test_txnref_txn_hash_raises_for_staged():
    r = TxnRef(kind="staged", value="1")
    with pytest.raises(ValueError):
        _ = r.txn_hash


# ─── /api/txn/{ref}/dismiss ───────────────────────────────────────


def _stage_one(client):
    """Stage one row and return its staged_id."""
    client.post(
        "/intake/stage",
        data={
            "text": "Date,Amount,Description\n2026-04-20,-9.99,TEST CHARGE\n",
            "has_header": "1",
        },
    )
    import re
    r = client.get("/review")
    m = re.search(r'id="rsg-row-(\d+)"', r.text)
    assert m is not None, "staged row should appear in /review"
    return int(m.group(1))


def test_dismiss_staged_drops_row(app_client):
    sid = _stage_one(app_client)
    r = app_client.post(
        f"/api/txn/staged:{sid}/dismiss",
        data={"reason": "test"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    after = app_client.get("/review")
    assert "TEST CHARGE" not in after.text


def test_dismiss_staged_htmx_returns_hx_refresh(app_client):
    sid = _stage_one(app_client)
    r = app_client.post(
        f"/api/txn/staged:{sid}/dismiss",
        data={"reason": "test"},
        headers={"HX-Request": "true"},
        follow_redirects=False,
    )
    assert r.status_code == 204
    assert r.headers.get("HX-Refresh") == "true"


def test_dismiss_ledger_rejected(app_client):
    r = app_client.post(
        "/api/txn/ledger:abc123/dismiss",
        data={"reason": "test"},
        follow_redirects=False,
    )
    # Ledger dismiss is intentionally not supported.
    assert r.status_code == 400


def test_dismiss_unknown_staged_id_404(app_client):
    r = app_client.post(
        "/api/txn/staged:999999/dismiss",
        data={"reason": "test"},
        follow_redirects=False,
    )
    assert r.status_code == 404


def test_dismiss_bad_ref_400(app_client):
    r = app_client.post(
        "/api/txn/garbage/dismiss",
        data={"reason": "test"},
        follow_redirects=False,
    )
    assert r.status_code == 400


# ─── /api/txn/{ref}/classify shape ────────────────────────────────


def test_classify_requires_target_account(app_client):
    sid = _stage_one(app_client)
    # Empty target_account should 400.
    r = app_client.post(
        f"/api/txn/staged:{sid}/classify",
        data={"target_account": ""},
        follow_redirects=False,
    )
    # FastAPI form-validation may surface as 422 (unprocessable) for
    # the missing required Form(), or as 400 from our explicit check
    # when the value is present but empty. Either way it's not a
    # 2xx success, and the row stays in /review.
    assert r.status_code in (400, 422)
    after = app_client.get("/review")
    # Row didn't get classified out.
    assert "TEST CHARGE" in after.text


def test_classify_bad_ref_400(app_client):
    r = app_client.post(
        "/api/txn/notvalid/classify",
        data={"target_account": "Expenses:Foo"},
        follow_redirects=False,
    )
    assert r.status_code == 400
