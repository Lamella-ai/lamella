# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP6 Site 2 Tier 1 — SimpleFIN ingest preempts AI for loan-claimed txns.

Principle-3 enforcement at the simplefin/ingest classify path. When
the SimpleFIN txn's source_account matches a loan's liability path,
or the SimpleFIN account id matches a loan's configured
simplefin_account_id, the AI classify call is skipped. The txn
stays in staging (DEFER-FIXME) until Tier 2's post-commit pass
writes the auto-classified split.

These tests verify:
- The guard fires on a matching txn.
- The AI `propose_account` call is never made.
- `_claimed_ingest_entries` captures the tuple for Tier 2 to consume.
- The staged row is NOT classified by AI.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from lamella.core.config import Settings


# ---------------------------------------- helpers


def _seed_loan(db, *, slug: str, liability: str, simplefin_id: str | None = None):
    db.execute(
        "INSERT INTO entities (slug, display_name, is_active) "
        "VALUES (?, ?, ?)",
        ("Personal", "Personal", 1),
    )
    db.execute(
        "INSERT INTO loans (slug, display_name, loan_type, entity_slug, "
        "institution, original_principal, funded_date, first_payment_date, "
        "term_months, interest_rate_apr, monthly_payment_estimate, "
        "liability_account_path, interest_account_path, simplefin_account_id, "
        "is_active, auto_classify_enabled, overflow_default) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            slug, slug, "mortgage", "Personal", "Bank",
            "100000.00", "2024-01-01", "2024-01-01",
            360, "6.0", "599.55",
            liability, f"Expenses:Personal:{slug}:Interest",
            simplefin_id, 1, 1, "bonus_principal",
        ),
    )
    db.commit()


def _make_ingest(
    *, db, ledger_dir: Path, settings: Settings, account_map: dict[str, str],
    ai_propose_mock: AsyncMock | None = None,
):
    """Build a SimpleFINIngest with mocked AI so we can verify whether
    propose_account was called."""
    from lamella.features.ai_cascade.service import AIService
    from lamella.core.beancount_io import LedgerReader
    from lamella.features.review_queue.service import ReviewService
    from lamella.features.rules.service import RuleService
    from lamella.features.bank_sync.ingest import SimpleFINIngest
    from lamella.features.bank_sync.writer import SimpleFINWriter
    from pydantic import SecretStr

    settings_with_sf = settings.model_copy(update={
        "simplefin_mode": "active",
        "openrouter_api_key": SecretStr("sk-test"),
        "openrouter_model": "anthropic/claude-haiku-4.5",
        "ai_cache_ttl_hours": 0,
    })
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
        run_check=False,
    )
    ai = AIService(settings=settings_with_sf, conn=db)
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_with_sf,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=ai,
        account_map=account_map,
    )
    return ingest


# ---------------------------------------- tests


async def test_loan_claimed_txn_preempts_ai_classify(
    db, ledger_dir: Path, settings: Settings, monkeypatch,
):
    """When a SimpleFIN txn's source_account matches a loan's
    liability, ingest._classify skips the AI call and tracks the
    claim in _claimed_ingest_entries."""
    from lamella.adapters.simplefin.schemas import SimpleFINTransaction

    _seed_loan(
        db, slug="MainResidenceMortgage",
        liability="Liabilities:Personal:Bank:MainResidenceMortgage",
    )

    ingest = _make_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings,
        account_map={
            "account-mortgage": "Liabilities:Personal:Bank:MainResidenceMortgage",
        },
    )
    # Reset per-run queues since we're calling _classify directly, not run().
    ingest._claimed_ingest_entries = []

    # Track whether propose_account was invoked.
    propose_calls: list = []

    async def _never_called(*args, **kwargs):
        propose_calls.append((args, kwargs))
        raise AssertionError(
            "propose_account must not be called for loan-claimed txns"
        )

    monkeypatch.setattr(
        "lamella.features.ai_cascade.classify.propose_account", _never_called,
    )

    sf_txn = SimpleFINTransaction(
        id="sf-mortgage-march",
        posted=int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp()),
        amount="-599.55",
        description="ACME BANK MORTGAGE PAYMENT",
        payee="Acme Bank",
    )

    from lamella.features.bank_sync.ingest import AccountOutcome, IngestResult

    outcome = AccountOutcome(account_id="account-mortgage", account_name="Mortgage")
    result = IngestResult(trigger="test", mode="active")

    pending = await ingest._classify(
        txn=sf_txn,
        source_account="Liabilities:Personal:Bank:MainResidenceMortgage",
        currency="USD",
        outcome=outcome,
        result=result,
    )

    # AI was never asked.
    assert propose_calls == []

    # The claim was captured for Tier 2.
    assert len(ingest._claimed_ingest_entries) == 1
    staged_id, captured_txn, captured_source, claim = ingest._claimed_ingest_entries[0]
    assert captured_txn.id == "sf-mortgage-march"
    assert captured_source == "Liabilities:Personal:Bank:MainResidenceMortgage"
    assert claim.loan_slug == "MainResidenceMortgage"


async def test_claim_list_is_empty_for_unrelated_txn(
    db, ledger_dir: Path, settings: Settings,
):
    """Sanity: a txn whose source_account doesn't match any loan
    must not add anything to _claimed_ingest_entries. We verify this
    via the pure claim-check (not the full ingest path, which would
    invoke AI). The claim function is tested exhaustively in
    test_loans_claim.py; this is a single-point confirmation that
    the ingest-level integration doesn't accidentally claim
    unrelated transactions."""
    from lamella.features.loans.claim import claim_from_simplefin_facts
    from lamella.adapters.simplefin.schemas import SimpleFINTransaction

    _seed_loan(
        db, slug="M",
        liability="Liabilities:Personal:Bank:M",
    )

    sf_txn = SimpleFINTransaction(
        id="sf-groceries-1",
        posted=int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp()),
        amount="-42.00",
        description="WHOLE FOODS",
        payee="Whole Foods",
    )
    claim = claim_from_simplefin_facts(
        sf_txn, "Assets:Personal:Checking", db,
    )
    assert claim is None
