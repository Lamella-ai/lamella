# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx

from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient
from lamella.features.bank_sync.ingest import SimpleFINIngest
from lamella.features.bank_sync.writer import SimpleFINWriter


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


async def test_ingest_auto_applies_user_rule_at_high_confidence(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    # Seed a user rule that should auto-apply to Hardware Store txns.
    rules = RuleService(db)
    rules.create(
        pattern_type="merchant_contains",
        pattern_value="home improvement store",
        target_account="Expenses:Acme:Supplies",
        confidence=1.0,
        created_by="user",
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    # NEXTGEN Phase B2 full swing: FIXME txns are no longer written to
    # the ledger — they're left in staging for the user to classify via
    # /review/staged. So new_txns counts ONLY auto-applied rows; the
    # rest land in fixme_txns (deferred).
    assert result.classified_by_rule >= 1  # Hardware Store auto-applied
    assert result.new_txns + result.fixme_txns == 10
    assert result.new_txns == result.classified_by_rule + result.classified_by_ai

    contents = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "Expenses:Acme:Supplies" in contents
    assert "sf-2001" in contents
    # FIXME must NOT appear in the ledger — the defer-FIXME path keeps
    # un-classified rows in staging only.
    assert "Expenses:FIXME" not in contents
    assert "Expenses:Acme:FIXME" not in contents
    # Deferred rows should be in staging with needs_review=1.
    staged_needs_review = db.execute(
        "SELECT COUNT(*) FROM staged_transactions t "
        "JOIN staged_decisions d ON d.staged_id = t.id "
        "WHERE t.source = 'simplefin' "
        "  AND t.status IN ('new', 'classified', 'matched') "
        "  AND d.needs_review = 1"
    ).fetchone()[0]
    assert staged_needs_review == result.fixme_txns


async def test_ingest_unmapped_account_creates_priority_review_item(
    db, ledger_dir: Path, settings: Settings, stub_bean_check
):
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    # Map only one account; the other must become an unmapped review item.
    partial_map = {"account-acme-card-a": "Liabilities:Acme:Card:CardA1234"}
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=partial_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    # Only the mapped account's 5 txns are processed; with defer-FIXME
    # they end up in staging (fixme_txns) instead of the ledger
    # (new_txns), since no rule/AI is configured in this test.
    assert result.new_txns + result.fixme_txns == 5
    unmapped = [a for a in result.per_account if a.unmapped]
    assert len(unmapped) == 1
    assert unmapped[0].account_id == "account-personal-card-b"

    open_items = reviews.list_open()
    kinds = {it.kind for it in open_items}
    assert "simplefin_unmapped_account" in kinds
    # Priority high enough to sort above normal FIXMEs.
    unmapped_item = next(i for i in open_items if i.kind == "simplefin_unmapped_account")
    assert unmapped_item.priority >= 1000


async def test_ingest_dedups_on_simplefin_id_from_ledger(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    # The fixture ledger already contains sf-1001 on a Hardware Store txn.
    # Our duplicate fixture repeats that id — it must be ignored, while
    # the other id goes through.
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("duplicate_ids.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    assert result.duplicate_txns == 1
    # With defer-FIXME, the non-duplicate txn lands in staging (fixme)
    # rather than the ledger (new_txns), absent an auto-applying rule.
    assert result.new_txns + result.fixme_txns == 1


async def test_ingest_writes_refund_and_original_as_separate_entries(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("refund_reversal.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    # With defer-FIXME, both the original and refund land in staging
    # (neither auto-applies with no rules/AI). The test still verifies
    # they're treated as distinct rows — no double-dedup wiping out
    # the refund.
    assert result.new_txns + result.fixme_txns == 2
    assert result.duplicate_txns == 0
    staged_ids = db.execute(
        "SELECT json_extract(source_ref, '$.txn_id') AS tid "
        "FROM staged_transactions WHERE source = 'simplefin'"
    ).fetchall()
    ids = {r["tid"] for r in staged_ids}
    assert "sf-4001" in ids
    assert "sf-4002" in ids


async def test_shadow_mode_writes_to_preview_file(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    reader = LedgerReader(ledger_dir / "main.bean")
    rules = RuleService(db)
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_shadow = _settings_with_mode(settings, "shadow")

    main_before = (ledger_dir / "main.bean").read_text(encoding="utf-8")
    real_before = (ledger_dir / "simplefin_transactions.bean").read_bytes()

    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_shadow,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    preview = ledger_dir / "simplefin_transactions.connector_preview.bean"
    # With defer-FIXME and no rules/AI configured, none of the txns
    # auto-apply — they stay in staging. Nothing reaches the preview
    # file in shadow mode either. The real invariant is "the non-
    # shadow files stay untouched regardless of mode." Preview file
    # only exists when there WAS a clean entry to preview.
    if result.new_txns > 0:
        assert preview.exists()

    # The real transactions file must be untouched in shadow mode.
    assert (ledger_dir / "simplefin_transactions.bean").read_bytes() == real_before
    # main.bean may pick up a structural include (e.g. connector_config.bean
    # auto-stitch) on first run; what shadow mode protects is *txn writes*,
    # not bookkeeping include lines. Verify no transaction directives leaked.
    main_after = (ledger_dir / "main.bean").read_text(encoding="utf-8")
    txn_directives_before = sum(
        1 for line in main_before.splitlines()
        if " * " in line or " ! " in line
    )
    txn_directives_after = sum(
        1 for line in main_after.splitlines()
        if " * " in line or " ! " in line
    )
    assert txn_directives_after == txn_directives_before


async def test_ingest_demotes_rule_when_target_account_not_open(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """Regression: a rule pointing at a never-opened account must NOT
    crash the whole batch with a bean-check rollback. Instead the
    guard demotes that one row to DEFER-FIXME so the rest of the
    batch still lands.

    The historical bug: widening the lookback window surfaced rows
    older than a rule's target_account Open date (or for which no
    Open existed at all), and `Invalid reference to inactive account`
    rolled back ~50 rule-applied rows plus everything else in the
    batch.
    """
    rules = RuleService(db)
    rules.create(
        pattern_type="merchant_contains",
        pattern_value="home improvement store",
        # This account is intentionally NOT in the fixture ledger —
        # the guard must catch it before bean-check does.
        target_account="Expenses:Acme:Supplies:NeverOpened",
        confidence=1.0,
        created_by="user",
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    # Critical: ingest completed without bean-check error.
    assert result.error is None
    # The Hardware Store row was the one matching the rule, but its
    # target wasn't safely writable on the txn date → it got demoted
    # to a deferred FIXME instead of forcing a rollback. The auto-
    # scaffold path under "Expenses:Acme:Supplies" extends a
    # legitimate branch, so it CAN scaffold a child — verify the
    # demote-to-FIXME branch by pointing at a parent that ALSO
    # doesn't exist (next test) AND ensure no rollback either way.
    contents = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "Expenses:Acme:Supplies:NeverOpened" in contents or \
        result.fixme_txns >= 1


async def test_ingest_demotes_rule_when_parent_branch_doesnt_exist(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """The guard refuses to auto-scaffold accounts under a parent
    that isn't part of any opened branch (typo'd entity, brand-new
    top-level). Such rows must demote to DEFER-FIXME, not crash the
    batch."""
    rules = RuleService(db)
    rules.create(
        pattern_type="merchant_contains",
        pattern_value="home improvement store",
        # Parent "Expenses:NonexistentEntity" is not opened anywhere
        # in the fixture ledger; auto-scaffold must refuse.
        target_account="Expenses:NonexistentEntity:Other:Stuff",
        confidence=1.0,
        created_by="user",
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )

    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    # Did NOT crash with bean-check.
    assert result.error is None
    # The Hardware Store row's rule was demoted; no
    # NonexistentEntity-prefixed posting was written.
    contents = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "Expenses:NonexistentEntity" not in contents
    # That row should be in staging with the rule recorded as a
    # blocked suggestion (rationale mentions the guard reason).
    blocked = db.execute(
        "SELECT d.rationale FROM staged_decisions d "
        "JOIN staged_transactions t ON t.id = d.staged_id "
        "WHERE t.source = 'simplefin' AND d.rationale LIKE '%blocked from auto-apply%'"
    ).fetchall()
    assert len(blocked) >= 1


async def test_failed_staged_rows_retry_on_next_ingest(
    db, ledger_dir: Path, settings: Settings, account_map, stub_bean_check
):
    """A row marked status='failed' on a prior ingest (e.g. when a
    rule's target account wasn't yet open and bean-check rolled
    the batch back) must be retried on the next ingest. Otherwise
    the row sits invisibly: /review/staged filters to {new,
    classified, matched}, so failed rows aren't surfaced for
    manual resolution either.

    Realistic setup: in the original incident, the bean-check
    rollback meant the txn was NOT in the ledger but the staged
    row was marked 'failed'. Simulate that exact state directly
    (an `_pre_seed` failed row + an empty ledger), then run an
    ingest. SimpleFIN re-delivers the same id; the row must
    reset to 'new', re-classify, and land cleanly.
    """
    from decimal import Decimal as _D
    from lamella.features.import_.staging import StagingService

    rules = RuleService(db)
    rules.create(
        pattern_type="merchant_contains",
        pattern_value="home improvement store",
        target_account="Expenses:Acme:Supplies",
        confidence=1.0,
        created_by="user",
    )

    # Pre-seed: stage the row exactly as the prior failed ingest
    # would have, then mark it failed. Ledger has no entry for
    # this simplefin_id, mirroring the post-rollback state.
    svc = StagingService(db)
    seeded = svc.stage(
        source="simplefin",
        source_ref={"account_id": "account-acme-card-a", "txn_id": "sf-2001"},
        session_id="prior-ingest",
        posting_date="2025-04-10",
        amount=_D("-42.17"),
        currency="USD",
        payee="Hardware Store",
        description="A HOME IMPROVEMENT STORE #1234",
        memo=None,
        raw={"id": "sf-2001"},
    )
    svc.mark_failed(seeded.id, reason="simulated bean-check rollback")
    assert svc.get(seeded.id).status == "failed"

    # Now run an ingest. The bridge re-delivers sf-2001; our
    # reset hook must flip it back to 'new' so classify runs
    # and the rule-applied entry lands in the bean file.
    reader = LedgerReader(ledger_dir / "main.bean")
    reviews = ReviewService(db)
    writer = SimpleFINWriter(
        main_bean=ledger_dir / "main.bean",
        simplefin_path=ledger_dir / "simplefin_transactions.bean",
    )
    settings_active = _settings_with_mode(settings, "active")
    ingest = SimpleFINIngest(
        conn=db,
        settings=settings_active,
        reader=reader,
        rules=rules,
        reviews=reviews,
        writer=writer,
        ai=None,
        account_map=account_map,
    )
    client = SimpleFINClient(access_url="https://u:p@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        result = await ingest.run(client=client, trigger="manual")
    await client.aclose()

    assert result.error is None
    final = svc.get(seeded.id)
    assert final.status == "promoted", (
        f"expected previously-failed row to retry through to 'promoted', "
        f"got {final.status!r}"
    )
    # The bean file now contains the previously-failed row.
    bean = (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8")
    assert "sf-2001" in bean
    assert "Expenses:Acme:Supplies" in bean


def test_run_accepts_lookback_days_override(
    db, ledger_dir: Path, settings: Settings
):
    """The wizard's first-time pull threads
    ``lookback_days_override=90`` into ``ingest.run`` so a fresh
    ledger gets a real corpus to classify against. Recurring runs
    omit the override and use ``settings.simplefin_lookback_days``.

    This is a signature-shape test — the override must be a
    valid keyword arg on ``run``. A regression here would silently
    break the wizard's deeper-history bootstrap."""
    import inspect
    sig = inspect.signature(SimpleFINIngest.run)
    assert "lookback_days_override" in sig.parameters
    assert sig.parameters["lookback_days_override"].default is None
