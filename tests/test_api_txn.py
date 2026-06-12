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


# ─── /api/txn/ledger:<hash>/classify — account-open guard ──────────
#
# The ledger classify path runs the same pre-write guards as the
# staged path: a target that deepens an attested branch is
# auto-scaffolded into connector_accounts.bean; a brand-new branch
# gets a clean 400 instead of an opaque post-write bean-check error.


def _mock_bean_checks(monkeypatch):
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    monkeypatch.setattr(
        "lamella.features.rules.overrides.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.features.rules.overrides.capture_bean_check",
        lambda main_bean: (0, ""),
    )
    monkeypatch.setattr(
        "lamella.features.rules.overrides.run_bean_check_vs_baseline",
        lambda main_bean, baseline_output: None,
    )


def _append_fixme_txn(settings) -> str:
    """Append a FIXME-carrying txn to the fixture ledger and return
    its content hash. The reader cache is mtime-keyed, so the route
    picks up the new entry on its next load."""
    from beancount.core.data import Transaction

    from lamella.core.beancount_io.reader import LedgerReader
    from lamella.core.beancount_io.txn_hash import txn_hash

    bean = settings.ledger_dir / "simplefin_transactions.bean"
    bean.write_text(
        bean.read_text(encoding="utf-8")
        + '\n2026-04-20 * "Jane Doe" "Gift received"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-000000002001"\n'
        "  Assets:Personal:Checking      100.00 USD\n"
        "  Expenses:FIXME               -100.00 USD\n",
        encoding="utf-8",
    )
    entries = LedgerReader(settings.ledger_main).load(force=True).entries
    for e in entries:
        if isinstance(e, Transaction) and e.narration == "Gift received":
            return txn_hash(e)
    raise AssertionError("appended fixture txn not found in ledger")


def test_ledger_classify_auto_scaffolds_new_branch(
    app_client, settings, monkeypatch,
):
    """User overrides the AI's pick with a not-yet-open path under an
    attested entity (Income:Personal:GiftReceived) — classify should
    open the account and write the override, not 400/500."""
    _mock_bean_checks(monkeypatch)
    h = _append_fixme_txn(settings)
    r = app_client.post(
        f"/api/txn/ledger:{h}/classify",
        data={"target_account": "Income:Personal:GiftReceived"},
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text
    text = settings.connector_accounts_path.read_text(encoding="utf-8")
    assert "open Income:Personal:GiftReceived" in text


def test_ledger_classify_rejects_unregistered_entity(
    app_client, settings, monkeypatch,
):
    """A target whose second segment isn't a registered entity slug
    is refused by the ADR-0042 entity-first validation — clean 400
    before any write."""
    _mock_bean_checks(monkeypatch)
    h = _append_fixme_txn(settings)
    r = app_client.post(
        f"/api/txn/ledger:{h}/classify",
        data={"target_account": "Expenses:Zebra:Stuff"},
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "ADR-0042" in r.text


def test_ledger_classify_rejects_unattested_branch(
    app_client, settings, monkeypatch,
):
    """A target that passes entity-first validation (Equity is
    exempt) but extends no existing branch is refused by the
    account-open guard — clean 400 before any write."""
    _mock_bean_checks(monkeypatch)
    h = _append_fixme_txn(settings)
    r = app_client.post(
        f"/api/txn/ledger:{h}/classify",
        data={"target_account": "Equity:Zebra:Stuff"},
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "not opened" in r.text or "not part of any existing" in r.text
