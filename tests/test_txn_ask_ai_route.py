# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""POST /txn/{hash}/ask-ai — on-demand classifier from the txn
detail page. Same shape as /review/staged/ask-ai but the source
is a real ledger txn, not a staged row.

Refusal-path tests only — exercises the 404 / 400 / 503 cases
without making any AI calls. The conftest network safety net
already refuses real OpenRouter calls; the happy path is covered
by the live UI testing the user's doing manually.
"""
from __future__ import annotations


def test_ask_ai_404_for_unknown_token(app_client):
    """Unknown UUIDv7 → 404."""
    r = app_client.post(
        "/txn/01900000-0000-7000-8000-deadbeef0000/ask-ai",
        data={},
        follow_redirects=False,
    )
    assert r.status_code == 404
    assert "no ledger transaction" in r.json()["detail"].lower()


def test_ask_ai_404_for_legacy_hex_token(app_client):
    """Legacy hex form retired in v3 — endpoint must reject."""
    r = app_client.post(
        "/txn/0123456789abcdef0123456789abcdef01234567/ask-ai",
        data={},
        follow_redirects=False,
    )
    assert r.status_code == 404
    assert "retired in v3" in r.json()["detail"].lower()


def test_ask_ai_400_for_already_classified_txn(app_client):
    """The fixture ledger has fully-classified txns (e.g. the
    Hardware Store row at $42.17 → Expenses:Acme:Supplies). They
    have no FIXME leg, so the endpoint refuses up front."""
    from beancount.core.data import Transaction
    from lamella.core.beancount_io import LedgerReader
    from lamella.core.identity import get_txn_id

    settings = app_client.app.state.settings
    reader = LedgerReader(settings.ledger_main)
    classified_token = None
    for e in reader.load().entries:
        if not isinstance(e, Transaction):
            continue
        if any(
            (p.account or "").endswith(":FIXME") for p in (e.postings or ())
        ):
            continue
        if any(
            (p.account or "").startswith("Expenses:")
            for p in (e.postings or ())
        ):
            classified_token = get_txn_id(e)
            break
    assert classified_token is not None, (
        "fixture should have at least one fully-classified expense txn "
        "with a lamella-txn-id"
    )

    r = app_client.post(
        f"/txn/{classified_token}/ask-ai",
        data={},
        follow_redirects=False,
    )
    assert r.status_code == 400
    detail = r.json()["detail"].lower()
    assert "no fixme" in detail


def test_ask_ai_503_when_ai_disabled(app_client):
    """Without an API key the endpoint refuses cleanly even when a
    valid FIXME txn exists. Seeds a FIXME row into the fixture and
    then disables AI."""
    app_client.app.state.settings.openrouter_api_key = None

    settings = app_client.app.state.settings
    sf_path = settings.simplefin_transactions_path
    sf_path.write_text(
        sf_path.read_text(encoding="utf-8")
        + (
            "\n"
            '2024-02-15 * "Some Vendor" "low-context FIXME"\n'
            '  lamella-txn-id: "01900000-0000-7000-8000-fffffffff111"\n'
            '  lamella-simplefin-id: "sf-fixme-test"\n'
            "  Liabilities:Acme:Card:CardA1234  -10.00 USD\n"
            "  Expenses:FIXME                    10.00 USD\n"
        ),
        encoding="utf-8",
    )
    app_client.app.state.ledger_reader.invalidate()

    fixme_token = "01900000-0000-7000-8000-fffffffff111"

    r = app_client.post(
        f"/txn/{fixme_token}/ask-ai",
        data={},
        follow_redirects=False,
    )
    assert r.status_code == 503
    assert "ai service disabled" in r.json()["detail"].lower()
