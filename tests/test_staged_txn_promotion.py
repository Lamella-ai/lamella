# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0043 P3 — staged-txn promotion writer.

Covers the atomic two-part write that turns a `custom "staged-txn"`
directive into:
  1. A `custom "staged-txn-promoted"` directive (audit anchor) carrying
     the original payload + promotion meta.
  2. A real balanced transaction with the user-picked target account.

Both edits land in one bean-check pass under the writer lock.
Failures roll the file back byte-for-byte.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount import loader
from beancount.core.data import Custom, Transaction

from lamella.features.bank_sync.writer import (
    BeanCheckError,
    PendingEntry,
    SimpleFINWriter,
    StagedDirectiveNotFoundError,
    WriteError,
    render_staged_txn_directive,
)


_LEDGER_PRELUDE = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
    '1900-01-01 open Assets:Personal:Bank:Checking USD\n'
    '1900-01-01 open Expenses:Personal:Food USD\n'
    '1900-01-01 open Expenses:Personal:FIXME USD\n'
    'include "simplefin_transactions.bean"\n'
)


def _entry(**overrides) -> PendingEntry:
    base = dict(
        date=date(2026, 4, 29),
        simplefin_id="ABC123",
        payee="Coffee Shop",
        narration="Test purchase",
        amount=Decimal("-4.50"),
        currency="USD",
        source_account="Assets:Personal:Bank:Checking",
        target_account="Expenses:Personal:Food",
        lamella_txn_id="01900000-0000-7000-8000-000000000001",
    )
    base.update(overrides)
    return PendingEntry(**base)


@pytest.fixture
def writer_with_directive(tmp_path: Path):
    """Set up a ledger with two staged-txn directives ready for
    promotion. Returns (writer, sf_path, entries) — the directives
    correspond to ``entries[0]`` and ``entries[1]``."""
    main = tmp_path / "main.bean"
    sf = tmp_path / "simplefin_transactions.bean"
    main.write_text(_LEDGER_PRELUDE, encoding="utf-8")
    e1 = _entry(simplefin_id="A1", lamella_txn_id="01900000-0000-7000-8000-000000000001")
    e2 = _entry(simplefin_id="B2", lamella_txn_id="01900000-0000-7000-8000-000000000002")
    sf.write_text(
        render_staged_txn_directive(e1) + render_staged_txn_directive(e2),
        encoding="utf-8",
    )
    writer = SimpleFINWriter(
        main_bean=main, simplefin_path=sf, run_check=True,
    )
    return writer, sf, (e1, e2)


class TestPromoteStagedTxn:
    def test_promote_replaces_directive_and_appends_balanced_txn(
        self, writer_with_directive,
    ):
        writer, sf, (e1, e2) = writer_with_directive
        result = writer.promote_staged_txn(
            promoted_entry=e1,
            promoted_by="manual",
        )
        assert result == e1.lamella_txn_id

        # Parse the resulting ledger and check shape.
        entries, errors, _ = loader.load_file(str(writer.main_bean))
        assert errors == [], f"unexpected parse errors: {errors}"
        custom_types = [
            (e.type, e.meta.get("lamella-txn-id"))
            for e in entries if isinstance(e, Custom)
        ]
        # e1's directive is now staged-txn-promoted; e2's is unchanged.
        assert ("staged-txn-promoted", e1.lamella_txn_id) in custom_types
        assert ("staged-txn", e2.lamella_txn_id) in custom_types
        assert ("staged-txn", e1.lamella_txn_id) not in custom_types

        # And there is exactly one balanced transaction with e1's
        # txn_id and the picked target account.
        txns = [e for e in entries if isinstance(e, Transaction)]
        e1_txns = [
            t for t in txns
            if t.meta.get("lamella-txn-id") == e1.lamella_txn_id
        ]
        assert len(e1_txns) == 1
        target_accts = [p.account for p in e1_txns[0].postings]
        assert "Expenses:Personal:Food" in target_accts
        assert "Assets:Personal:Bank:Checking" in target_accts

    def test_unknown_lamella_txn_id_raises(self, writer_with_directive):
        writer, _, _ = writer_with_directive
        ghost = _entry(lamella_txn_id="01900000-0000-7000-8000-99999999dead")
        with pytest.raises(StagedDirectiveNotFoundError):
            writer.promote_staged_txn(
                promoted_entry=ghost, promoted_by="manual",
            )

    def test_promotion_is_not_idempotent_second_call_raises(
        self, writer_with_directive,
    ):
        writer, _, (e1, _) = writer_with_directive
        writer.promote_staged_txn(promoted_entry=e1, promoted_by="manual")
        # The directive is now staged-txn-promoted; a second promote
        # call cannot find a staged-txn directive with that id.
        with pytest.raises(StagedDirectiveNotFoundError):
            writer.promote_staged_txn(promoted_entry=e1, promoted_by="manual")

    def test_invalid_promoted_by_rejected(self, writer_with_directive):
        writer, sf, (e1, _) = writer_with_directive
        before = sf.read_bytes()
        with pytest.raises(WriteError):
            writer.promote_staged_txn(
                promoted_entry=e1, promoted_by="garbage",
            )
        # File untouched on validation failure.
        assert sf.read_bytes() == before

    def test_rule_promotion_carries_rule_id(self, writer_with_directive):
        writer, sf, (e1, _) = writer_with_directive
        writer.promote_staged_txn(
            promoted_entry=e1,
            promoted_by="rule",
            promoted_rule_id="rule-7",
        )
        body = sf.read_text(encoding="utf-8")
        assert 'lamella-promoted-rule-id: "rule-7"' in body

    def test_ai_promotion_carries_model(self, writer_with_directive):
        writer, sf, (e1, _) = writer_with_directive
        writer.promote_staged_txn(
            promoted_entry=e1,
            promoted_by="ai",
            promoted_ai_model="claude-haiku-4-5",
        )
        body = sf.read_text(encoding="utf-8")
        assert 'lamella-promoted-ai-model: "claude-haiku-4-5"' in body

    def test_lamella_txn_id_lineage_preserved_through_promotion(
        self, writer_with_directive,
    ):
        """ADR-0043b §1: the same UUIDv7 the staging directive carried
        is on the promoted directive AND on the balanced txn — the
        /txn/{token} URL is stable across the bridge."""
        writer, _, (e1, _) = writer_with_directive
        writer.promote_staged_txn(promoted_entry=e1, promoted_by="manual")
        entries, _, _ = loader.load_file(str(writer.main_bean))
        # Promoted directive
        promoted_directives = [
            e for e in entries
            if isinstance(e, Custom) and e.type == "staged-txn-promoted"
        ]
        assert len(promoted_directives) == 1
        assert promoted_directives[0].meta["lamella-txn-id"] == e1.lamella_txn_id
        # Balanced txn
        txns = [
            e for e in entries
            if isinstance(e, Transaction)
            and e.meta.get("lamella-txn-id") == e1.lamella_txn_id
        ]
        assert len(txns) == 1


class TestPromoteAtomicity:
    """If the post-write bean-check fails, both files must roll back
    byte-for-byte. The two edits (directive replacement + balanced
    txn append) are coupled — never half-applied."""

    def test_bean_check_failure_rolls_both_files_back(self, tmp_path: Path):
        # The promotion writer shells out to `bean-check` to validate;
        # without it on PATH there is no failure to recover from. Skip
        # rather than misreport a regression on a clean dev machine.
        import shutil
        if shutil.which("bean-check") is None:
            pytest.skip("bean-check binary not on PATH")
        main = tmp_path / "main.bean"
        sf = tmp_path / "simplefin_transactions.bean"
        # Prelude that's missing the target account Open — bean-check
        # will reject the promotion's balanced txn as "inactive
        # account at txn date."
        main.write_text(
            'option "title" "Test"\n'
            'option "operating_currency" "USD"\n'
            '1900-01-01 open Assets:Personal:Bank:Checking USD\n'
            '1900-01-01 open Expenses:Personal:FIXME USD\n'
            'include "simplefin_transactions.bean"\n',
            encoding="utf-8",
        )
        e = _entry(target_account="Expenses:Personal:UnopenedAccount")
        sf.write_text(render_staged_txn_directive(e), encoding="utf-8")
        writer = SimpleFINWriter(
            main_bean=main, simplefin_path=sf, run_check=True,
        )
        sf_before = sf.read_bytes()
        main_before = main.read_bytes()
        with pytest.raises(BeanCheckError):
            writer.promote_staged_txn(
                promoted_entry=e, promoted_by="manual",
            )
        # Both files restored byte-for-byte.
        assert sf.read_bytes() == sf_before
        assert main.read_bytes() == main_before
