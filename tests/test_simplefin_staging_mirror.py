# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase B — SimpleFIN onto the unified staging pipeline.

Asserts every SimpleFIN fetch lands in ``staged_transactions`` with
``source='simplefin'``, classification decisions are mirrored to
``staged_decisions``, and lifecycle transitions
(promoted / failed) reflect the writer's actual outcome.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.writer import SimpleFINWriter
from lamella.features.import_.staging import StagingService


FIXTURES = Path(__file__).parent / "fixtures" / "simplefin"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _settings_with_mode(base: Settings, mode: str) -> Settings:
    return base.model_copy(update={"simplefin_mode": mode})


@pytest.fixture
def account_map() -> dict[str, str]:
    return {
        "account-acme-card-a": "Liabilities:Acme:Card:CardA1234",
        "account-personal-card-b": "Liabilities:Personal:Card:CardB9876",
    }


@pytest.fixture
def stub_bean_check(monkeypatch):
    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", lambda main_bean: None
    )


def _build_ingest(
    *, db, ledger_dir: Path, settings: Settings, account_map, ai=None,
    seed_auto_apply_rule: bool = True,
) -> SimpleFINIngest:
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    if seed_auto_apply_rule:
        # NEXTGEN Phase B2 full swing: SimpleFIN now defers un-classified
        # rows to staging instead of emitting FIXMEs. Tests that exercise
        # the writer path need at least one row to auto-apply. Seed a
        # broad rule so every test fixture lands at least a few txns in
        # the ledger write batch.
        rules.create(
            pattern_type="merchant_contains",
            pattern_value="hardware",
            target_account="Expenses:Acme:Supplies",
            confidence=1.0,
            created_by="user",
        )
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    return SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=ai,
        account_map=account_map,
    )


async def test_every_new_txn_lands_in_staged_transactions(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """A SimpleFIN fetch with 10 new txns produces 10 staged_transactions
    rows tagged source='simplefin'."""
    ingest = _build_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings, account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(
            200, json=_load("two_accounts_ten_txns.json"),
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    assert result.error is None

    n = db.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions WHERE source = 'simplefin'"
    ).fetchone()["n"]
    assert n == 10

    # source_ref carries account_id + txn_id so the row is traceable.
    ref_json = db.execute(
        "SELECT source_ref FROM staged_transactions "
        "WHERE source = 'simplefin' LIMIT 1"
    ).fetchone()["source_ref"]
    ref = json.loads(ref_json)
    assert "account_id" in ref and "txn_id" in ref


async def test_rule_auto_apply_records_high_confidence_decision(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """An auto-applied user rule produces a staged_decision with
    confidence='high', decided_by='rule', rule_id populated."""
    rules = RuleService(db)
    rule_id = rules.create(
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        target_account="Expenses:Acme:Supplies",
        confidence=1.0,
        created_by="user",
    )
    ingest = _build_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings, account_map=account_map,
    )
    # Override with the pre-seeded rules service so the ingest sees our rule.
    ingest.rules = rules
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(
            200, json=_load("two_accounts_ten_txns.json"),
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    assert result.error is None
    assert result.classified_by_rule >= 1

    rule_decisions = db.execute(
        """
        SELECT d.account, d.confidence, d.decided_by, d.rule_id
          FROM staged_decisions d
          JOIN staged_transactions t ON t.id = d.staged_id
         WHERE t.source = 'simplefin'
           AND d.decided_by = 'rule'
        """
    ).fetchall()
    assert len(rule_decisions) >= 1
    for r in rule_decisions:
        assert r["confidence"] == "high"
        assert r["rule_id"] is not None


async def test_fixme_txns_mark_needs_review_on_staging(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """Txns that fall to the FIXME path still write to bean (Phase B1
    keeps the FIXME-emit flow), but the staged_decision for them marks
    needs_review=True and confidence in the low/medium/unresolved
    band."""
    ingest = _build_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings, account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(
            200, json=_load("two_accounts_ten_txns.json"),
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    assert result.error is None
    assert result.fixme_txns >= 1

    needs_review_decisions = db.execute(
        """
        SELECT d.account, d.confidence, d.needs_review
          FROM staged_decisions d
          JOIN staged_transactions t ON t.id = d.staged_id
         WHERE t.source = 'simplefin'
           AND d.needs_review = 1
        """
    ).fetchall()
    assert len(needs_review_decisions) >= 1
    for r in needs_review_decisions:
        # FIXME account name always ends with :FIXME (with optional
        # entity prefix).
        assert r["account"].endswith(":FIXME")
        assert r["confidence"] in {"low", "medium", "unresolved"}


async def test_promoted_rows_marked_with_target_file(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """After a successful write, staged rows transition to
    status='promoted' and carry promoted_to_file pointing at the bean
    file actually written (active mode → simplefin_transactions.bean)."""
    ingest = _build_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings, account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(
            200, json=_load("two_accounts_ten_txns.json"),
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    assert result.error is None

    promoted = db.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions "
        "WHERE source = 'simplefin' AND status = 'promoted' "
        "  AND promoted_to_file IS NOT NULL"
    ).fetchone()["n"]
    assert promoted == result.new_txns

    # The target file is the active simplefin path.
    row = db.execute(
        "SELECT promoted_to_file FROM staged_transactions "
        "WHERE source = 'simplefin' AND status = 'promoted' LIMIT 1"
    ).fetchone()
    assert "simplefin_transactions.bean" in row["promoted_to_file"]


async def test_bean_check_failure_marks_staged_rows_failed(
    db, ledger_dir: Path, settings: Settings, account_map, monkeypatch
):
    """If the writer's bean-check rejects the batch, staged rows for
    that batch land in status='failed' — the staging surface never
    claims promotion that didn't happen."""
    # Make bean-check explode exactly once.
    def blow_up(_main_bean):
        raise BeanCheckError("synthetic bean-check failure")

    monkeypatch.setattr(
        "lamella.features.bank_sync.writer.run_bean_check", blow_up
    )

    ingest = _build_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings, account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(
            200, json=_load("two_accounts_ten_txns.json"),
        )
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()
    # The ingest captures the bean-check error into result.error and
    # returns; it does not re-raise up to the caller.
    assert result.error is not None and "bean-check" in result.error.lower()

    # Every staged row that entered the write batch should be 'failed'.
    failed = db.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions "
        "WHERE source = 'simplefin' AND status = 'failed'"
    ).fetchone()["n"]
    assert failed >= 1
    promoted = db.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions "
        "WHERE source = 'simplefin' AND status = 'promoted'"
    ).fetchone()["n"]
    assert promoted == 0


async def test_refetch_same_window_is_idempotent_on_staging(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """Running the ingest twice on the same bridge response must not
    double-stage rows. The (source, source_ref_hash) uniqueness on
    staged_transactions guards against it."""
    ingest = _build_ingest(
        db=db, ledger_dir=ledger_dir, settings=settings, account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(
            200, json=_load("two_accounts_ten_txns.json"),
        )
        await ingest.run(client=client, trigger="manual")
        # Second pass: same response, should land zero new staged rows.
        await ingest.run(client=client, trigger="manual")
    await client.aclose()

    # Still exactly 10 staged SimpleFIN rows after two passes.
    n = db.execute(
        "SELECT COUNT(*) AS n FROM staged_transactions WHERE source = 'simplefin'"
    ).fetchone()["n"]
    assert n == 10
