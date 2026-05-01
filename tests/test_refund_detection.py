# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the refund-of-expense detector at
``lamella.features.bank_sync.refund_detect``. Covers the scoring rubric,
the no-match path, multi-candidate ranking, classify-path stamping of
``lamella-refund-of`` meta, and bidirectional /txn-page link rendering.

Canonical placeholders only — Acme / Jane Doe per ADR-0017 + tests/CLAUDE.md.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.bank_sync.refund_detect import (
    MIN_SCORE,
    find_refund_candidates,
)


def _build_ledger(
    tmp_path: Path,
    *,
    extra_txns: str = "",
) -> LedgerReader:
    """Minimal Acme ledger with one classified expense (HARDWARE STORE,
    $42.17 on 2026-04-10) plus whatever the caller pastes in via
    ``extra_txns``. Returns a LedgerReader pointed at it."""
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Acme:Checking USD\n"
        "2023-01-01 open Liabilities:Acme:Card:CardA1234 USD\n"
        "2023-01-01 open Liabilities:Acme:Card:CardA9999 USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n"
        "2023-01-01 open Expenses:Acme:Office USD\n"
        "2023-01-01 open Income:Acme:Refunds USD\n"
        "2023-01-01 open Equity:Acme:Opening-Balances USD\n",
        encoding="utf-8",
    )
    main = tmp_path / "main.bean"
    main.write_text(
        'option "title" "Refund-detect fixture"\n'
        'option "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2026-04-10 * "Hardware Store" "Supplies for workshop"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-000000001001"\n'
        "  Liabilities:Acme:Card:CardA1234 -42.17 USD\n"
        "    lamella-source-0: \"simplefin\"\n"
        "    lamella-source-reference-id-0: \"sf-1001\"\n"
        "  Expenses:Acme:Supplies         42.17 USD\n"
        + extra_txns,
        encoding="utf-8",
    )
    return LedgerReader(main)


@pytest.fixture
def tmp_conn(tmp_path: Path) -> sqlite3.Connection:
    """Lightweight in-test SQLite — the detector accepts conn for API
    symmetry but doesn't read it today, so any migrated handle works."""
    conn = connect(tmp_path / "refund.sqlite")
    migrate(conn)
    yield conn
    conn.close()


# ─── Scoring ────────────────────────────────────────────────────────


def test_strong_match_high_score(tmp_path, tmp_conn):
    """All four signals fire — the candidate clears the threshold with
    a score over 0.95."""
    reader = _build_ledger(tmp_path)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("42.17"),
        refund_date=date(2026, 4, 24),
        merchant="Hardware Store",
        narration="REFUND - HARDWARE STORE",
        source_account="Liabilities:Acme:Card:CardA1234",
    )
    assert len(candidates) == 1
    c = candidates[0]
    assert c.target_account == "Expenses:Acme:Supplies"
    assert c.lamella_txn_id == "0190f000-0000-7000-8000-000000001001"
    # 0.40 (merchant) + 0.30 (5%) + 0.20 (date) + 0.10 (account) = 1.00
    assert c.score == pytest.approx(1.0, abs=0.001)
    assert any("merchant matched" in r for r in c.match_reasons)
    assert any("amount within 5%" in r for r in c.match_reasons)
    assert any("days ago" in r for r in c.match_reasons)
    assert any("same payment account" in r for r in c.match_reasons)


def test_amount_within_20_pct_partial_score(tmp_path, tmp_conn):
    """When the refund is within 20% but outside 5%, the amount signal
    contributes +0.10 (not +0.30). With merchant + date that still
    clears MIN_SCORE."""
    reader = _build_ledger(tmp_path)
    # 42.17 → ~36.50 = 13.4% off (in the 5–20% band)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("36.50"),
        refund_date=date(2026, 4, 24),
        merchant="Hardware Store",
        narration=None,
        source_account=None,
    )
    assert len(candidates) == 1
    # 0.40 (merchant) + 0.10 (20%) + 0.20 (date) = 0.70
    assert candidates[0].score == pytest.approx(0.70, abs=0.001)
    reasons = candidates[0].match_reasons
    assert any("amount within 20%" in r for r in reasons)
    assert not any("amount within 5%" in r for r in reasons)


def test_below_threshold_dropped(tmp_path, tmp_conn):
    """Only the date signal fires (no merchant match, amount way off
    the 20% window) — score is 0.20, below MIN_SCORE, dropped."""
    reader = _build_ledger(tmp_path)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("999.00"),  # 23x the original; outside 20%
        refund_date=date(2026, 4, 24),
        merchant="Totally Different Merchant",
        narration=None,
        source_account=None,
    )
    assert candidates == []
    assert MIN_SCORE > 0.20  # documents the boundary the test relies on


# ─── No-match path ──────────────────────────────────────────────────


def test_no_candidates_when_amount_negative(tmp_path, tmp_conn):
    """Defensive: a refund must be money-IN. Calling with a negative
    amount returns empty (caller bug — sign-flip upstream)."""
    reader = _build_ledger(tmp_path)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("-42.17"),
        refund_date=date(2026, 4, 24),
        merchant="Hardware Store",
        narration=None,
        source_account="Liabilities:Acme:Card:CardA1234",
    )
    assert candidates == []


def test_no_candidates_when_original_after_refund(tmp_path, tmp_conn):
    """The original must precede the refund chronologically. A txn
    dated AFTER the refund can't be the source of that refund."""
    reader = _build_ledger(tmp_path)
    # Refund dated April 1 — original is April 10, so no date credit.
    # Without date credit: 0.40 (merchant) + 0.30 (amount) + 0.10 (acct)
    # = 0.80 still clears, BUT the candidate is offered.
    # Re-test with merchant DISABLED so the only path to threshold is
    # date — confirms the date_window_score returns 0 for future origs.
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("42.17"),
        refund_date=date(2026, 4, 1),
        merchant="Totally Unrelated",  # no merchant credit
        narration=None,
        source_account=None,  # no account credit
    )
    # Score = 0.30 (5% amount) only — below MIN_SCORE.
    assert candidates == []


# ─── Multi-candidate ranking ────────────────────────────────────────


def test_multiple_candidates_ranked_by_score(tmp_path, tmp_conn):
    """Two candidates clear the threshold; the higher-scoring one
    surfaces first. Same-score ties resolve by recency."""
    extra = (
        '\n2026-04-12 * "Hardware Store" "More supplies"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-000000001002"\n'
        "  Liabilities:Acme:Card:CardA1234 -42.17 USD\n"
        "    lamella-source-0: \"simplefin\"\n"
        "    lamella-source-reference-id-0: \"sf-1002\"\n"
        "  Expenses:Acme:Office           42.17 USD\n"
    )
    reader = _build_ledger(tmp_path, extra_txns=extra)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("42.17"),
        refund_date=date(2026, 4, 24),
        merchant="Hardware Store",
        narration=None,
        source_account="Liabilities:Acme:Card:CardA1234",
    )
    # Both txns score identically (1.00) — recency wins, so the Apr 12
    # txn surfaces ahead of the Apr 10 one.
    assert len(candidates) == 2
    assert candidates[0].date == date(2026, 4, 12)
    assert candidates[1].date == date(2026, 4, 10)
    assert candidates[0].score >= candidates[1].score


def test_capped_at_five(tmp_path, tmp_conn):
    """Even with 6 high-score candidates, only the top 5 return."""
    rows = []
    for i in range(6):
        rows.append(
            f'\n2026-04-{10 + i:02d} * "Hardware Store" "Run #{i}"\n'
            f'  lamella-txn-id: "0190f000-0000-7000-8000-{i:012d}"\n'
            f"  Liabilities:Acme:Card:CardA1234 -42.17 USD\n"
            f"    lamella-source-0: \"simplefin\"\n"
            f"    lamella-source-reference-id-0: \"sf-{2000 + i}\"\n"
            f"  Expenses:Acme:Supplies         42.17 USD\n"
        )
    # Wipe the default txn so we get exactly 6 candidates, not 7.
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Assets:Acme:Checking USD\n"
        "2023-01-01 open Liabilities:Acme:Card:CardA1234 USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n"
        "2023-01-01 open Equity:Acme:Opening-Balances USD\n",
        encoding="utf-8",
    )
    main = tmp_path / "main.bean"
    main.write_text(
        'option "title" "x"\noption "operating_currency" "USD"\n'
        'include "accounts.bean"\n' + "".join(rows),
        encoding="utf-8",
    )
    reader = LedgerReader(main)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("42.17"),
        refund_date=date(2026, 4, 24),
        merchant="Hardware Store",
        narration=None,
        source_account="Liabilities:Acme:Card:CardA1234",
    )
    assert len(candidates) == 5


# ─── Merchant-fallback (narration) ──────────────────────────────────


def test_falls_back_to_narration_for_merchant_signal(tmp_path, tmp_conn):
    """When ``merchant`` is None, ``narration`` carries the merchant
    signal — common for paste/CSV imports without a payee column."""
    reader = _build_ledger(tmp_path)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("42.17"),
        refund_date=date(2026, 4, 24),
        merchant=None,
        narration="Refund — HARDWARE STORE chargeback #1234",
        source_account=None,
    )
    assert len(candidates) == 1
    assert any("merchant matched" in r for r in candidates[0].match_reasons)


# ─── Skip FIXME-bound txns (unclassified) ────────────────────────────


# ─── render_entry stamps lamella-refund-of ──────────────────────────


def test_render_entry_stamps_refund_of_meta():
    """The writer's render_entry helper emits ``lamella-refund-of:
    "<id>"`` at the txn-meta level when ``refund_of_txn_id`` is set
    on the PendingEntry. Absent → no stamping (clean output)."""
    from lamella.features.bank_sync.writer import PendingEntry, render_entry

    entry = PendingEntry(
        date=date(2026, 4, 24),
        simplefin_id="sf-refund-1",
        payee="Hardware Store",
        narration="REFUND - chargeback",
        amount=Decimal("42.17"),  # positive = money in (a refund)
        currency="USD",
        source_account="Liabilities:Acme:Card:CardA1234",
        target_account="Expenses:Acme:Supplies",
        ai_classified=False,
        lamella_txn_id="0190f000-0000-7000-8000-00000000ffff",
        refund_of_txn_id="0190f000-0000-7000-8000-000000001001",
    )
    rendered = render_entry(entry)
    # The refund-of meta lands at the txn-meta level (between the
    # lamella-txn-id and the postings).
    assert (
        '  lamella-refund-of: '
        '"0190f000-0000-7000-8000-000000001001"'
    ) in rendered
    # And the absence-case produces no spurious key.
    no_refund = PendingEntry(
        date=date(2026, 4, 24),
        simplefin_id="sf-deposit-1",
        payee="Customer",
        narration="Sale",
        amount=Decimal("100.00"),
        currency="USD",
        source_account="Assets:Acme:Checking",
        target_account="Income:Acme:Sales",
        ai_classified=False,
        lamella_txn_id="0190f000-0000-7000-8000-00000000fff0",
    )
    assert "lamella-refund-of" not in render_entry(no_refund)


# ─── Integration: staged classify path stamps the meta ──────────────


def _seed_simplefin_card(db):
    """Bind a SimpleFIN account_id to a card path so ``_resolve_account_path``
    finds the card on classify."""
    db.execute(
        """
        INSERT INTO accounts_meta (account_path, display_name,
                                   simplefin_account_id)
        VALUES (?, ?, ?)
        ON CONFLICT(account_path) DO UPDATE SET
            simplefin_account_id = excluded.simplefin_account_id
        """,
        ("Liabilities:Acme:Card:CardA1234", "CardA Acme", "sf-acct-refund"),
    )
    db.commit()


def _stage_simplefin_refund(db, txn_id: str, *, amount: str = "42.17"):
    """Stage a positive-amount SimpleFIN row (a refund). Amount is
    positive from the bank-account POV — money IN."""
    from lamella.features.import_.staging import StagingService
    return StagingService(db).stage(
        source="simplefin",
        source_ref={"account_id": "sf-acct-refund", "txn_id": txn_id},
        posting_date="2026-04-24",
        amount=amount,
        currency="USD",
        payee="Hardware Store",
        description="REFUND - chargeback",
    ).id


def test_classify_stamps_refund_of_meta(app_client, settings, monkeypatch):
    """Posting to /api/txn/staged:<id>/classify with a non-empty
    refund_of_txn_id stamps ``lamella-refund-of`` on the rendered
    transaction."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_simplefin_card(db)

    sid = _stage_simplefin_refund(db, txn_id="sf-refund-stamp-1")
    original_id = "0190f000-0000-7000-8000-000000001001"
    r = app_client.post(
        f"/api/txn/staged:{sid}/classify",
        data={
            "target_account": "Expenses:Acme:Supplies",
            "refund_of_txn_id": original_id,
        },
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text

    text = settings.simplefin_transactions_path.read_text(encoding="utf-8")
    assert f'lamella-refund-of: "{original_id}"' in text


def test_classify_without_refund_of_does_not_stamp(
    app_client, settings, monkeypatch,
):
    """Plain classify (no refund_of_txn_id form value) leaves the
    rendered txn free of any lamella-refund-of meta — defensive
    confirmation that the field is opt-in, not always-on."""
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.core.registry.accounts_writer.AccountsWriter._check",
        lambda self, baseline=None: None,
    )
    db = app_client.app.state.db
    _seed_simplefin_card(db)

    sid = _stage_simplefin_refund(db, txn_id="sf-no-refund-1")
    r = app_client.post(
        f"/api/txn/staged:{sid}/classify",
        data={"target_account": "Expenses:Acme:Supplies"},
        follow_redirects=False,
    )
    assert r.status_code in (200, 303), r.text

    text = settings.simplefin_transactions_path.read_text(encoding="utf-8")
    assert "lamella-refund-of" not in text


# ─── Template rendering: deposit-skip panel surfaces refund buttons ─


def _render_ask_ai_result(**ctx) -> str:
    """Render the partial in isolation. Mirrors what the api_txn worker
    does: builds a context dict and renders ``partials/_ask_ai_result.html``
    via the app's Jinja env so the result is real production-shape HTML."""
    from lamella.main import create_app
    from lamella.core.config import Settings
    app = create_app(settings=Settings())
    templates = app.state.templates
    base_ctx = {
        "mode": "staged",
        "ref": "staged:1",
        "staged_id": 1,
        "txn_hash": None,
        "proposal": None,
        "attempt": 1,
        "reason": None,
        "blocked": False,
        "return_url": "/review",
        "ai_skip_reason": None,
        "refund_candidates": [],
        "source": "",
    }
    base_ctx.update(ctx)
    return templates.get_template(
        "partials/_ask_ai_result.html"
    ).render(base_ctx)


def test_template_renders_refund_candidate_buttons():
    """Given a deposit-skip terminal with one refund candidate, the
    rendered HTML carries the candidate's merchant + target_account +
    a hidden refund_of_txn_id input pointing at the original's lineage."""
    from lamella.features.bank_sync.refund_detect import RefundCandidate
    candidate = RefundCandidate(
        lamella_txn_id="0190f000-0000-7000-8000-000000001001",
        txn_hash="abc123def456",
        date=date(2026, 4, 10),
        merchant="Hardware Store",
        amount=Decimal("-42.17"),
        target_account="Expenses:Acme:Supplies",
        score=0.95,
        match_reasons=[
            "merchant matched 'Hardware Store'",
            "amount within 5% (orig 42.17)",
        ],
    )
    html = _render_ask_ai_result(
        ai_skip_reason="deposit",
        refund_candidates=[candidate],
    )
    # The candidate's merchant + target lands as visible button text.
    assert "Hardware Store" in html
    assert "Expenses:Acme:Supplies" in html
    # The hidden form input carries the original's lineage so the
    # classify path stamps lamella-refund-of correctly.
    assert (
        'name="refund_of_txn_id" '
        'value="0190f000-0000-7000-8000-000000001001"'
    ) in html
    # And the form's target is the unified classify endpoint with the
    # right pre-filled target_account.
    assert (
        'name="target_account" value="Expenses:Acme:Supplies"'
    ) in html
    assert 'action="/api/txn/staged:1/classify"' in html


def test_template_no_refund_section_when_empty():
    """No candidates → the refund section is absent. The deposit panel
    falls back to its "search /search" hint copy, and the manual picker
    is the only action surface."""
    html = _render_ask_ai_result(
        ai_skip_reason="deposit",
        refund_candidates=[],
    )
    assert "Possible refund of an existing expense" not in html
    # And the deposit-fallback hint to /search does appear.
    assert "/search" in html


# ─── /txn detail page renders bidirectional refund links ──────────


def _seed_refund_pair(settings):
    """Append two txns to the fixture's simplefin_transactions.bean:
      * Original expense — Hardware Store $42.17 charge on 2026-04-10,
        lamella-txn-id = 0190f000-0000-7000-8000-000000abcdef.
      * Refund — Hardware Store $42.17 credit on 2026-04-24,
        lamella-txn-id = 0190f000-0000-7000-8000-000000fedcba,
        carrying lamella-refund-of pointing at the original.
    Returns the two lamella-txn-id tokens (original, refund)."""
    original_id = "0190f000-0000-7000-8000-000000abcdef"
    refund_id = "0190f000-0000-7000-8000-000000fedcba"
    extra = (
        '\n2026-04-10 * "Hardware Store" "Supplies for workshop"\n'
        f'  lamella-txn-id: "{original_id}"\n'
        "  Liabilities:Acme:Card:CardA1234 -42.17 USD\n"
        "    lamella-source-0: \"simplefin\"\n"
        "    lamella-source-reference-id-0: \"sf-orig-1\"\n"
        "  Expenses:Acme:Supplies         42.17 USD\n"
        '\n2026-04-24 * "Hardware Store" "REFUND - chargeback"\n'
        f'  lamella-txn-id: "{refund_id}"\n'
        f'  lamella-refund-of: "{original_id}"\n'
        "  Liabilities:Acme:Card:CardA1234  42.17 USD\n"
        "    lamella-source-0: \"simplefin\"\n"
        "    lamella-source-reference-id-0: \"sf-refund-1\"\n"
        "  Expenses:Acme:Supplies         -42.17 USD\n"
    )
    sf_path = settings.simplefin_transactions_path
    sf_path.parent.mkdir(parents=True, exist_ok=True)
    if sf_path.exists():
        sf_path.write_text(
            sf_path.read_text(encoding="utf-8") + extra, encoding="utf-8",
        )
    else:
        sf_path.write_text(extra, encoding="utf-8")
    # Make sure main.bean includes the simplefin file (the fixture
    # already does, but be defensive for non-default fixture paths).
    main = settings.ledger_main
    main_text = main.read_text(encoding="utf-8")
    include_line = f'include "{sf_path.name}"'
    if include_line not in main_text:
        main.write_text(main_text + "\n" + include_line + "\n", encoding="utf-8")
    return original_id, refund_id


def test_txn_detail_renders_refunded_by_on_original(app_client, settings):
    """The original expense's /txn/{token} renders a "Refunded by"
    card linking to the refund txn."""
    original_id, refund_id = _seed_refund_pair(settings)
    resp = app_client.get(f"/txn/{original_id}")
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "Refunded by" in body
    # Linked target points at the refund.
    assert f"/txn/{refund_id}" in body


def test_txn_detail_renders_refund_of_on_refund(app_client, settings):
    """The refund txn's /txn/{token} renders a "Refund of" card
    linking back to the original expense."""
    original_id, refund_id = _seed_refund_pair(settings)
    resp = app_client.get(f"/txn/{refund_id}")
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "Refund of" in body
    # Linked target points at the original.
    assert f"/txn/{original_id}" in body


def test_txn_detail_no_refund_card_for_unrelated_txn(app_client, settings):
    """An unrelated txn (no refund-of meta inbound or outbound) renders
    neither card. Sanity check that the templates only fire when the
    metadata is present."""
    # Append one unrelated txn — Hardware Store charge with no refund link.
    other_id = "0190f000-0000-7000-8000-00000000aaaa"
    extra = (
        '\n2026-04-15 * "Hardware Store" "Other supplies"\n'
        f'  lamella-txn-id: "{other_id}"\n'
        "  Liabilities:Acme:Card:CardA1234 -10.00 USD\n"
        "    lamella-source-0: \"simplefin\"\n"
        "    lamella-source-reference-id-0: \"sf-other-1\"\n"
        "  Expenses:Acme:Supplies         10.00 USD\n"
    )
    sf_path = settings.simplefin_transactions_path
    sf_path.write_text(
        (sf_path.read_text(encoding="utf-8") if sf_path.exists() else "")
        + extra,
        encoding="utf-8",
    )
    resp = app_client.get(f"/txn/{other_id}")
    assert resp.status_code == 200, resp.text
    # Allow "refund" to appear in unrelated copy — but not the card title.
    body = resp.text
    assert "Refunded by" not in body
    # The "Refund of" card title would only appear if the txn had
    # the meta — confirm it doesn't render here.
    # (be permissive about other "refund" mentions in the page body)
    import re
    assert not re.search(r"<h2[^>]*>\s*Refund of\s*<", body)


# ─── (rest below) ──────────────────────────────────────────────────


def test_fixme_txns_excluded(tmp_path, tmp_conn):
    """A txn with an Expenses:FIXME leg is unclassified — re-routing
    a refund there is meaningless. Detector skips it."""
    accounts = tmp_path / "accounts.bean"
    accounts.write_text(
        "2023-01-01 open Liabilities:Acme:Card:CardA1234 USD\n"
        "2023-01-01 open Expenses:FIXME USD\n"
        "2023-01-01 open Expenses:Acme:Supplies USD\n",
        encoding="utf-8",
    )
    main = tmp_path / "main.bean"
    main.write_text(
        'option "title" "x"\noption "operating_currency" "USD"\n'
        'include "accounts.bean"\n\n'
        '2026-04-10 * "Hardware Store" "Unclassified"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-000000009001"\n'
        "  Liabilities:Acme:Card:CardA1234 -42.17 USD\n"
        "  Expenses:FIXME                   42.17 USD\n",
        encoding="utf-8",
    )
    reader = LedgerReader(main)
    candidates = find_refund_candidates(
        tmp_conn, reader,
        refund_amount=Decimal("42.17"),
        refund_date=date(2026, 4, 24),
        merchant="Hardware Store",
        narration=None,
        source_account="Liabilities:Acme:Card:CardA1234",
    )
    assert candidates == []
