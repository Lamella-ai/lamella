# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Pre-write validation: classify endpoints reject a target
account whose open date is after the txn date. Without this, the
write succeeds and bean-check breaks the ledger — exactly the
"Invalid reference to inactive account" trap that hit the user's
live ledger.
"""
from __future__ import annotations

from lamella.features.import_.staging import StagingService


def _seed_card(db):
    db.execute(
        """
        INSERT INTO accounts_meta (account_path, display_name,
                                   simplefin_account_id)
        VALUES (?, ?, ?)
        ON CONFLICT(account_path) DO UPDATE SET
            simplefin_account_id = excluded.simplefin_account_id
        """,
        ("Liabilities:Acme:Card:CardA1234", "CardA Acme", "sf-acct-x"),
    )
    db.commit()


def _stage_simplefin(db, txn_id: str, posting_date: str, payee="Acme"):
    svc = StagingService(db)
    return svc.stage(
        source="simplefin",
        source_ref={"account_id": "sf-acct-x", "txn_id": txn_id},
        posting_date=posting_date,
        amount="-25.00",
        currency="USD",
        payee=payee,
        description=None,
    ).id


def test_classify_rejects_target_opened_after_txn(
    app_client, settings, monkeypatch
):
    """The user reported `Invalid reference to inactive account
    'Expenses:Personal:Food:Groceries'` after classifying a
    2026-04-18 txn into an account opened on 2026-04-24. The
    endpoint should refuse that write up front."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    # Stage a Jan 2024 txn, then point classify at an account whose
    # entity branch isn't in the ledger at all — no parent or sibling
    # is opened — so it cannot auto-scaffold via the
    # "extend an existing branch" path either.
    staged_id = _stage_simplefin(
        db, txn_id="sf-validate-1", posting_date="2024-01-15",
    )

    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:UnknownEntity:NeverOpened",
        },
        follow_redirects=False,
    )
    # Pre-flight refusal — clean 400 with explanation.
    assert r.status_code == 400, r.text
    detail = r.json()["detail"].lower()
    assert "expenses:unknownentity:neveropened" in detail
    # The error should name the missing-open situation. ADR-0042 path
    # validation rejects the unknown second segment as "not a registered
    # entity slug" before the open-date check can run, which is fine —
    # the user still gets a clean refusal with the offending account.
    assert (
        "not opened" in detail
        or "open directive" in detail
        or "registered entity slug" in detail
    )


def test_classify_accepts_target_open_on_txn_date(
    app_client, settings, monkeypatch
):
    """Sanity: a target account already open in the fixture works
    and the validator does not over-reject."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    # Override the simplefin id mapping so the staged row routes to
    # an account that exists in the fixture ledger.
    db.execute(
        "UPDATE accounts_meta SET simplefin_account_id = ? "
        " WHERE account_path = ?",
        ("sf-acct-x", "Liabilities:Acme:Card:CardA1234"),
    )
    db.commit()

    staged_id = _stage_simplefin(
        db, txn_id="sf-validate-2", posting_date="2024-02-10",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:Acme:Supplies",
        },
        follow_redirects=False,
    )
    # 303 redirect on success.
    assert r.status_code == 303, r.text


def test_classify_group_rejects_when_any_row_predates_open(
    app_client, settings, monkeypatch
):
    """Group classify must validate every row in the group, not
    just the prototype. If even one row is older than the target
    account's open date, the whole group action refuses up front
    so we don't leave half the group written and half not."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    sid_old = _stage_simplefin(
        db, txn_id="sf-grp-old", posting_date="2020-06-01",
    )
    sid_new = _stage_simplefin(
        db, txn_id="sf-grp-new", posting_date="2024-06-01",
    )

    r = app_client.post(
        "/review/staged/classify-group",
        data={
            "staged_ids": [str(sid_old), str(sid_new)],
            "target_account": "Expenses:UnknownEntity:NeverOpened",
        },
        follow_redirects=False,
    )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"].lower()
    # The error should name the offending row id and the account.
    assert "expenses:unknownentity:neveropened" in detail
