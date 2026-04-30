# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0043 P6 — bulk-rewrite legacy FIXME postings to
``custom "staged-txn"`` directives.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from beancount import loader
from beancount.core.data import Custom, Transaction

from lamella.features.bank_sync.migrate_fixme_to_staged_txn import (
    migrate_fixme_to_staged_txn,
)


_PRELUDE = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
    '1900-01-01 open Assets:Personal:Bank:Checking USD\n'
    '1900-01-01 open Expenses:Personal:FIXME USD\n'
    '1900-01-01 open Liabilities:Personal:Loan USD\n'
    'include "simplefin_transactions.bean"\n'
)


# A balanced txn with a FIXME leg + a SimpleFIN source — eligible.
_LEGACY_FIXME_TXN = """
2026-04-15 * "Coffee Shop" "Morning coffee"
  lamella-txn-id: "01900000-0000-7000-8000-aaaaaaaaaaa1"
  Assets:Personal:Bank:Checking  -4.50 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "sf-coffee-1"
  Expenses:Personal:FIXME  4.50 USD
"""

# A balanced txn whose FIXME leg is part of a loan group — INELIGIBLE.
_LOAN_FIXME_TXN = """
2026-04-15 * "Lender Co" "Loan payment"
  lamella-txn-id: "01900000-0000-7000-8000-bbbbbbbbbbb1"
  lamella-loan-group-id: "loan-group-1"
  Assets:Personal:Bank:Checking  -100.00 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "sf-loan-1"
  Liabilities:Personal:Loan  100.00 USD
  Expenses:Personal:FIXME  0.00 USD
"""

# A balanced txn with a FIXME leg but NO source meta — INELIGIBLE
# (might be a hand-edited or imported-without-provenance row).
_NO_SOURCE_FIXME_TXN = """
2026-04-15 * "Mystery" "no source"
  lamella-txn-id: "01900000-0000-7000-8000-ccccccccccc1"
  Assets:Personal:Bank:Checking  -10.00 USD
  Expenses:Personal:FIXME  10.00 USD
"""


@pytest.fixture
def ledger_with_fixmes(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Build a tmp ledger with one eligible + two ineligible FIXME
    txns. Returns (ledger_dir, main_bean, simplefin_bean)."""
    ledger = tmp_path / "ledger"
    ledger.mkdir()
    main = ledger / "main.bean"
    sf = ledger / "simplefin_transactions.bean"
    main.write_text(_PRELUDE, encoding="utf-8")
    sf.write_text(
        _LEGACY_FIXME_TXN + _LOAN_FIXME_TXN + _NO_SOURCE_FIXME_TXN,
        encoding="utf-8",
    )
    return ledger, main, sf


class TestEligibility:
    def test_dry_run_counts_only_eligible(self, ledger_with_fixmes):
        ledger, main, sf = ledger_with_fixmes
        report = migrate_fixme_to_staged_txn(
            ledger_dir=ledger,
            connector_files=[sf],
            main_bean=main,
            dry_run=True,
        )
        assert report.dry_run is True
        # One eligible (the coffee txn) — loan + no-source are skipped.
        assert report.txns_migrated == 1
        assert report.files_scanned == 1
        assert report.files_modified == 0  # dry-run never writes
        # Snapshot only on apply.
        assert report.snapshot_dir is None

    def test_dry_run_does_not_modify_files(self, ledger_with_fixmes):
        _, _, sf = ledger_with_fixmes
        before = sf.read_bytes()
        migrate_fixme_to_staged_txn(
            ledger_dir=sf.parent,
            connector_files=[sf],
            main_bean=sf.parent / "main.bean",
            dry_run=True,
        )
        assert sf.read_bytes() == before


class TestApply:
    def test_apply_replaces_eligible_txn_with_directive(
        self, ledger_with_fixmes, monkeypatch,
    ):
        ledger, main, sf = ledger_with_fixmes
        # Skip real bean-check for the unit test.
        monkeypatch.setattr(
            "lamella.features.bank_sync.migrate_fixme_to_staged_txn.run_bean_check",
            lambda main_bean: None,
        )
        report = migrate_fixme_to_staged_txn(
            ledger_dir=ledger,
            connector_files=[sf],
            main_bean=main,
            dry_run=False,
        )
        assert report.dry_run is False
        assert report.txns_migrated == 1
        assert report.files_modified == 1
        assert report.snapshot_dir is not None and report.snapshot_dir.exists()

        # The bean file should now contain a custom "staged-txn"
        # directive in place of the eligible txn, AND the loan + no-
        # source txns should be untouched.
        body = sf.read_text(encoding="utf-8")
        assert 'custom "staged-txn"' in body
        assert "01900000-0000-7000-8000-aaaaaaaaaaa1" in body
        # The loan txn lamella-txn-id is still present (unmodified)
        assert "01900000-0000-7000-8000-bbbbbbbbbbb1" in body
        # The no-source txn is also still present
        assert "01900000-0000-7000-8000-ccccccccccc1" in body

    def test_directive_carries_correct_amount_and_source(
        self, ledger_with_fixmes, monkeypatch,
    ):
        ledger, main, sf = ledger_with_fixmes
        monkeypatch.setattr(
            "lamella.features.bank_sync.migrate_fixme_to_staged_txn.run_bean_check",
            lambda main_bean: None,
        )
        migrate_fixme_to_staged_txn(
            ledger_dir=ledger,
            connector_files=[sf],
            main_bean=main,
            dry_run=False,
        )
        # Re-parse and verify shape.
        entries, errors, _ = loader.load_file(str(main))
        assert errors == []
        custom_entries = [
            e for e in entries
            if isinstance(e, Custom) and e.type == "staged-txn"
        ]
        assert len(custom_entries) == 1
        c = custom_entries[0]
        assert c.meta["lamella-source"] == "simplefin"
        assert c.meta["lamella-source-reference-id"] == "sf-coffee-1"
        # Amount sign matches the original bank-side posting POV
        # (-4.50 — money leaving Assets:Personal:Bank:Checking).
        assert "-4.50" in str(c.meta["lamella-txn-amount"])

    def test_apply_idempotent_second_run_is_noop(
        self, ledger_with_fixmes, monkeypatch,
    ):
        ledger, main, sf = ledger_with_fixmes
        monkeypatch.setattr(
            "lamella.features.bank_sync.migrate_fixme_to_staged_txn.run_bean_check",
            lambda main_bean: None,
        )
        first = migrate_fixme_to_staged_txn(
            ledger_dir=ledger, connector_files=[sf], main_bean=main,
            dry_run=False,
        )
        assert first.txns_migrated == 1
        # Second run finds nothing to migrate (no FIXME-leg txns left).
        second = migrate_fixme_to_staged_txn(
            ledger_dir=ledger, connector_files=[sf], main_bean=main,
            dry_run=False,
        )
        assert second.txns_migrated == 0
        # No new modifications either.
        assert second.files_modified == 0


class TestEmptyFiles:
    def test_empty_connector_file_no_error(self, tmp_path: Path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        main = ledger / "main.bean"
        sf = ledger / "simplefin_transactions.bean"
        main.write_text(_PRELUDE, encoding="utf-8")
        sf.write_text("", encoding="utf-8")
        report = migrate_fixme_to_staged_txn(
            ledger_dir=ledger, connector_files=[sf], main_bean=main,
            dry_run=True,
        )
        assert report.txns_migrated == 0
        assert report.files_scanned == 1

    def test_missing_connector_file_skipped(self, tmp_path: Path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        main = ledger / "main.bean"
        main.write_text(_PRELUDE, encoding="utf-8")
        report = migrate_fixme_to_staged_txn(
            ledger_dir=ledger,
            connector_files=[ledger / "does_not_exist.bean"],
            main_bean=main,
            dry_run=True,
        )
        assert report.files_scanned == 0
        assert report.txns_migrated == 0
