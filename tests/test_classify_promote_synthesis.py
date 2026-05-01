# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0059 promotion-path narration synthesis.

When a staged row gets classified into a real ledger entry, the
narration written to disk is the output of
``DeterministicNarrationSynthesizer`` (longest description, payee
fallback, then a placeholder), and the entry carries a
``lamella-narration-synthesized: TRUE`` marker at txn meta so future
multi-source sync passes know the line is theirs to rewrite.
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


def _stage(db, *, description: str | None, payee: str | None):
    svc = StagingService(db)
    return svc.stage(
        source="simplefin",
        source_ref={"account_id": "sf-acct-x", "txn_id": "sf-synth-1"},
        posting_date="2024-05-10",
        amount="-12.34",
        currency="USD",
        payee=payee,
        description=description,
    ).id


def test_promote_writes_synthesized_narration_and_marker(
    app_client, settings, monkeypatch,
):
    """Single-row promote: the bean entry's narration is the synthesizer's
    output for the staged row's description, and the entry carries the
    ``lamella-narration-synthesized: TRUE`` marker."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage(
        db, description="ACME COFFEE SHOP DECAF", payee="Acme",
    )
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:Acme:Supplies",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    text = settings.simplefin_transactions_path.read_text(encoding="utf-8")
    # The synthesized narration is the full description (longest one).
    assert '"ACME COFFEE SHOP DECAF"' in text
    # The synthesized marker landed at txn meta.
    assert "lamella-narration-synthesized: TRUE" in text


def test_promote_falls_back_to_payee_when_description_missing(
    app_client, settings, monkeypatch,
):
    """When the staged row has no description, the synthesizer falls
    back to the payee. The marker still gets written."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_card(db)

    staged_id = _stage(db, description=None, payee="Acme Coffee")
    r = app_client.post(
        "/review/staged/classify",
        data={
            "staged_id": staged_id,
            "target_account": "Expenses:Acme:Supplies",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text

    text = settings.simplefin_transactions_path.read_text(encoding="utf-8")
    assert '"Acme Coffee"' in text
    assert "lamella-narration-synthesized: TRUE" in text
