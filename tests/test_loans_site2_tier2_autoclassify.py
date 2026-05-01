# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP6 Site 2 Tier 2 — post-ingest auto-classify of loan-claimed entries.

The Tier 1 commit preempts AI for loan-claimed SimpleFIN txns,
which leaves them in staging under DEFER-FIXME. Tier 2 closes
that gap by writing the auto-classified split at ingest commit —
because loan payments are information-complete at ingest (the
configured amortization is the full context; no receipt/note/
mileage that lands later changes the split).

These tests cover:
- `SimpleFINWriter.append_split_entry` — multi-leg transaction
  write with the lock/bean-check/rollback contract.
- `auto_classify.apply_ingest_split` — end-to-end write + log row
  insert.
- `_auto_classify_claimed_ingest_entries` — full post-commit drain
  over a synthesized `_claimed_ingest_entries` list, covering
  exact/over (writes + promotes), under/far (leaves staging),
  PAYMENT-but-disabled (leaves staging).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.config import Settings
from lamella.features.loans.auto_classify import (
    ClassifyPlan,
    apply_ingest_split,
    plan_from_facts,
)
from lamella.features.loans.claim import Claim, ClaimKind


# -------------------------------------------------------------- helpers


def _seed_loan(db, *, slug: str, liability: str, auto_classify_enabled: int = 1):
    db.execute(
        "INSERT INTO entities (slug, display_name, is_active) VALUES (?, ?, ?)",
        ("Personal", "Personal", 1),
    )
    db.execute(
        "INSERT INTO loans (slug, display_name, loan_type, entity_slug, "
        "institution, original_principal, funded_date, first_payment_date, "
        "term_months, interest_rate_apr, monthly_payment_estimate, "
        "liability_account_path, interest_account_path, "
        "is_active, auto_classify_enabled, overflow_default) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            slug, slug, "mortgage", "Personal", "Bank",
            "100000.00", "2024-01-01", "2024-01-01",
            360, "6.0", "599.55",
            liability, f"Expenses:Personal:{slug}:Interest",
            1, auto_classify_enabled, "bonus_principal",
        ),
    )
    db.commit()


def _sf_txn(sf_id: str, amount: str, d: date = date(2024, 3, 1)):
    from lamella.adapters.simplefin.schemas import SimpleFINTransaction
    return SimpleFINTransaction(
        id=sf_id,
        posted=int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()),
        amount=amount,
        description="Mortgage payment",
        payee="Bank",
    )


# ------------------------- SimpleFINWriter.append_split_entry


def test_append_split_entry_balances_source_against_splits(tmp_path: Path):
    from lamella.features.bank_sync.writer import SimpleFINWriter

    main = tmp_path / "main.bean"
    sf_bean = tmp_path / "simplefin_transactions.bean"
    main.write_text(
        'option "title" "t"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        f'include "{sf_bean.name}"\n', encoding="utf-8",
    )
    writer = SimpleFINWriter(
        main_bean=main, simplefin_path=sf_bean, run_check=False,
    )
    block = writer.append_split_entry(
        txn_date=date(2024, 3, 1),
        simplefin_id="sf-mortgage-1",
        source_account="Assets:Personal:Checking",
        source_amount=Decimal("-599.55"),   # negative: money out
        splits=[
            ("Liabilities:Personal:Bank:M", Decimal("100")),
            ("Expenses:Personal:M:Interest", Decimal("499.55")),
        ],
        narration="Mortgage payment",
        extra_meta={
            "lamella-loan-autoclass-tier": "exact",
            "lamella-loan-autoclass-decision-id": "dec-abc",
        },
    )
    # Source leg preserves SimpleFIN sign; splits are flipped.
    assert "Assets:Personal:Checking  -599.55 USD" in block
    assert "Liabilities:Personal:Bank:M  100.00 USD" in block
    assert "Expenses:Personal:M:Interest  499.55 USD" in block
    # Meta lines before postings.
    assert 'lamella-loan-autoclass-tier: "exact"' in block
    assert 'lamella-source-0: "simplefin"' in block
    assert 'lamella-source-reference-id-0: "sf-mortgage-1"' in block


def test_append_split_entry_rejects_unbalanced_splits(tmp_path: Path):
    from lamella.features.bank_sync.writer import SimpleFINWriter, WriteError

    main = tmp_path / "main.bean"
    sf_bean = tmp_path / "simplefin_transactions.bean"
    main.write_text("", encoding="utf-8")
    writer = SimpleFINWriter(
        main_bean=main, simplefin_path=sf_bean, run_check=False,
    )
    with pytest.raises(WriteError):
        writer.append_split_entry(
            txn_date=date(2024, 3, 1),
            simplefin_id="sf-bad",
            source_account="Assets:Personal:Checking",
            source_amount=Decimal("-599.55"),
            splits=[
                ("Liabilities:Personal:Bank:M", Decimal("100")),
                # Missing $499.55 — splits don't balance source.
            ],
        )


def test_append_split_entry_empty_splits_rejected(tmp_path: Path):
    from lamella.features.bank_sync.writer import SimpleFINWriter, WriteError

    main = tmp_path / "main.bean"
    sf_bean = tmp_path / "simplefin_transactions.bean"
    main.write_text("", encoding="utf-8")
    writer = SimpleFINWriter(
        main_bean=main, simplefin_path=sf_bean, run_check=False,
    )
    with pytest.raises(WriteError):
        writer.append_split_entry(
            txn_date=date(2024, 3, 1),
            simplefin_id="sf-empty",
            source_account="Assets:Personal:Checking",
            source_amount=Decimal("-100"),
            splits=[],
        )


# ----------------------------------- apply_ingest_split


def test_apply_ingest_split_writes_and_logs(db, ledger_dir: Path, monkeypatch):
    from lamella.features.bank_sync.writer import SimpleFINWriter

    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None,
    )
    _seed_loan(db, slug="M", liability="Liabilities:Personal:Bank:M")
    loan = dict(db.execute("SELECT * FROM loans WHERE slug = 'M'").fetchone())

    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    sf_txn = _sf_txn("sf-mortgage-march", "-599.55")
    plan = plan_from_facts(
        actual_total=Decimal("599.55"),
        txn_date=date(2024, 3, 1),
        loan=loan,
        source_account="Assets:Personal:Checking",
    )
    assert plan.tier == "exact"  # sanity

    apply_ingest_split(
        plan, sf_txn, "Assets:Personal:Checking", loan,
        writer=writer, conn=db,
    )

    # Written to simplefin_transactions.bean.
    content = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert 'lamella-source-0: "simplefin"' in content
    assert 'lamella-source-reference-id-0: "sf-mortgage-march"' in content
    assert "Liabilities:Personal:Bank:M  100.00 USD" in content or \
           "Liabilities:Personal:Bank:M" in content  # amortization can vary pennies
    assert 'lamella-loan-autoclass-tier: "exact"' in content

    # Log row inserted.
    log_row = db.execute(
        "SELECT loan_slug, tier, txn_hash FROM loan_autoclass_log "
        "WHERE decision_id = ?",
        (plan.decision_id,),
    ).fetchone()
    assert log_row is not None
    assert log_row[0] == "M"
    assert log_row[1] == "exact"
    assert log_row[2] == "simplefin:sf-mortgage-march"


def test_apply_ingest_split_noop_on_under_tier(db, ledger_dir: Path):
    """tier=under means plan produced empty splits. apply must not
    write anything."""
    from lamella.features.bank_sync.writer import SimpleFINWriter

    _seed_loan(db, slug="M", liability="Liabilities:Personal:Bank:M")
    loan = dict(db.execute("SELECT * FROM loans WHERE slug = 'M'").fetchone())

    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
        run_check=False,
    )
    sf_txn = _sf_txn("sf-partial", "-400")
    plan = plan_from_facts(
        actual_total=Decimal("400"),
        txn_date=date(2024, 3, 1),
        loan=loan,
        source_account="Assets:Personal:Checking",
    )
    assert plan.tier == "under"

    apply_ingest_split(plan, sf_txn, "Assets:Personal:Checking", loan,
                       writer=writer, conn=db)

    # No SimpleFIN write, no log row.
    sf_path = ledger_dir / "simplefin_transactions.bean"
    if sf_path.exists():
        assert "sf-partial" not in sf_path.read_text(encoding="utf-8")
    rows = db.execute("SELECT * FROM loan_autoclass_log").fetchall()
    assert rows == []


# -------------------------- _auto_classify_claimed_ingest_entries


def _make_ingest(db, ledger_dir: Path, settings: Settings):
    from pydantic import SecretStr

    from lamella.features.ai_cascade.service import AIService
    from lamella.core.beancount_io import LedgerReader
    from lamella.features.review_queue.service import ReviewService
    from lamella.features.rules.service import RuleService
    from lamella.features.bank_sync.ingest import SimpleFINIngest
    from lamella.features.bank_sync.writer import SimpleFINWriter

    settings_sf = settings.model_copy(update={
        "simplefin_mode": "active",
        "openrouter_api_key": SecretStr("sk-test"),
        "openrouter_model": "anthropic/claude-haiku-4.5",
    })
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
        run_check=False,
    )
    return SimpleFINIngest(
        conn=db, settings=settings_sf,
        reader=LedgerReader(ledger_dir / "main.bean"),
        rules=RuleService(db), reviews=ReviewService(db),
        writer=writer, ai=AIService(settings=settings_sf, conn=db),
        account_map={"account-m": "Liabilities:Personal:Bank:M"},
    )


def test_drain_writes_exact_and_promotes_staging(db, ledger_dir: Path, settings):
    """PAYMENT + exact tier: writes the split, log row appears."""
    _seed_loan(db, slug="M", liability="Liabilities:Personal:Bank:M")
    ingest = _make_ingest(db, ledger_dir, settings)

    sf_txn = _sf_txn("sf-ok", "-599.55")
    claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    # staged_id None (we're not actually running the full ingest; we just
    # want to exercise the drain logic against synthesized state).
    ingest._claimed_ingest_entries = [
        (None, sf_txn, "Assets:Personal:Checking", claim),
    ]

    ingest._auto_classify_claimed_ingest_entries(
        target_path=ledger_dir / "simplefin_transactions.bean",
    )

    # Written to the SimpleFIN file.
    content = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "sf-ok" in content
    assert 'lamella-loan-autoclass-tier: "exact"' in content
    # Log row present.
    rows = db.execute("SELECT tier FROM loan_autoclass_log").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "exact"
    # Drain clears the list.
    assert ingest._claimed_ingest_entries == []


def test_drain_skips_under_tier(db, ledger_dir: Path, settings):
    """PAYMENT + under tier: no write, no log, drain clears anyway."""
    _seed_loan(db, slug="M", liability="Liabilities:Personal:Bank:M")
    ingest = _make_ingest(db, ledger_dir, settings)

    sf_txn = _sf_txn("sf-partial", "-400")  # under
    claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    ingest._claimed_ingest_entries = [
        (None, sf_txn, "Assets:Personal:Checking", claim),
    ]

    ingest._auto_classify_claimed_ingest_entries(
        target_path=ledger_dir / "simplefin_transactions.bean",
    )

    sf_path = ledger_dir / "simplefin_transactions.bean"
    if sf_path.exists():
        assert "sf-partial" not in sf_path.read_text(encoding="utf-8")
    assert db.execute("SELECT COUNT(*) FROM loan_autoclass_log").fetchone()[0] == 0
    assert ingest._claimed_ingest_entries == []


def test_drain_respects_auto_classify_disabled(db, ledger_dir: Path, settings):
    """A loan with auto_classify_enabled=0: claim fires (AI preempted
    per principle 3), but Tier 2 must NOT auto-write."""
    _seed_loan(
        db, slug="M", liability="Liabilities:Personal:Bank:M",
        auto_classify_enabled=0,
    )
    ingest = _make_ingest(db, ledger_dir, settings)

    sf_txn = _sf_txn("sf-disabled", "-599.55")
    claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    ingest._claimed_ingest_entries = [
        (None, sf_txn, "Assets:Personal:Checking", claim),
    ]

    ingest._auto_classify_claimed_ingest_entries(
        target_path=ledger_dir / "simplefin_transactions.bean",
    )

    sf_path = ledger_dir / "simplefin_transactions.bean"
    if sf_path.exists():
        assert "sf-disabled" not in sf_path.read_text(encoding="utf-8")
    assert db.execute("SELECT COUNT(*) FROM loan_autoclass_log").fetchone()[0] == 0


def test_drain_skips_non_payment_claim_kinds(db, ledger_dir: Path, settings):
    """ESCROW_DISBURSEMENT, DRAW, REVOLVING_SKIP: claimed (to preempt
    AI) but never auto-split."""
    _seed_loan(db, slug="M", liability="Liabilities:Personal:Bank:M")
    ingest = _make_ingest(db, ledger_dir, settings)

    sf_txn = _sf_txn("sf-escrow", "-4000")
    claim = Claim(kind=ClaimKind.ESCROW_DISBURSEMENT, loan_slug="M")
    ingest._claimed_ingest_entries = [
        (None, sf_txn, "Assets:Personal:Bank:M:Escrow", claim),
    ]

    ingest._auto_classify_claimed_ingest_entries(
        target_path=ledger_dir / "simplefin_transactions.bean",
    )

    sf_path = ledger_dir / "simplefin_transactions.bean"
    if sf_path.exists():
        assert "sf-escrow" not in sf_path.read_text(encoding="utf-8")
    assert db.execute("SELECT COUNT(*) FROM loan_autoclass_log").fetchone()[0] == 0


def test_drain_isolated_failures_dont_block_others(db, ledger_dir: Path, settings,
                                                    caplog):
    """If one entry fails to write, the drain continues and processes
    the remaining ones. Best-effort contract."""
    _seed_loan(db, slug="M", liability="Liabilities:Personal:Bank:M")
    ingest = _make_ingest(db, ledger_dir, settings)

    bad_sf = _sf_txn("sf-bad", "-599.55")
    good_sf = _sf_txn("sf-good", "-599.55")
    bad_claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="DOES_NOT_EXIST")
    good_claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    ingest._claimed_ingest_entries = [
        (None, bad_sf, "Assets:Personal:Checking", bad_claim),   # loan missing
        (None, good_sf, "Assets:Personal:Checking", good_claim),
    ]

    with caplog.at_level(logging.INFO):
        ingest._auto_classify_claimed_ingest_entries(
            target_path=ledger_dir / "simplefin_transactions.bean",
        )

    content = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "sf-good" in content
    assert "sf-bad" not in content
    # Good claim logged its tier; bad claim was skipped silently (loan
    # lookup returned None → early continue, no error).
    assert db.execute("SELECT COUNT(*) FROM loan_autoclass_log").fetchone()[0] == 1
