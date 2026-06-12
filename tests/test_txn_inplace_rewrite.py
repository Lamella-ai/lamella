# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the in-place txn rewrite (NEXTGEN Phase E3 per-txn complement).

The rewrite directly edits the .bean file to replace a FIXME
posting's account with a real target. Every happy path: the raw
ledger loads cleanly after the edit, no override block is needed.
Every failure path: the file is restored byte-identical."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.rewrite.txn_inplace import (
    InPlaceRewriteError,
    rewrite_fixme_to_account,
    rewrite_fixme_to_multiple_postings,
)


MAIN_BEAN = """option "title" "t"
option "operating_currency" "USD"
include "txns.bean"
2023-01-01 open Assets:Personal:WF:Checking USD
2023-01-01 open Liabilities:Personal:Card:Chase USD
2023-01-01 open Expenses:Personal:Food:FastFood USD
2023-01-01 open Expenses:FIXME USD
"""

TXNS_BEAN = """
2026-04-10 * "MCDONALDS" "drive-thru"
  Liabilities:Personal:Card:Chase   -5.49 USD
  Expenses:FIXME                     5.49 USD

2026-04-11 * "STAPLES" "printer paper"
  Liabilities:Personal:Card:Chase  -14.99 USD
  Expenses:FIXME                    14.99 USD
"""


@pytest.fixture
def ledger_dir(tmp_path: Path) -> Path:
    d = tmp_path / "ledger"
    d.mkdir()
    (d / "main.bean").write_text(MAIN_BEAN, encoding="utf-8")
    (d / "txns.bean").write_text(TXNS_BEAN, encoding="utf-8")
    return d


def _txn_start_line(text: str, narration: str) -> int:
    """Return the 1-indexed line of the transaction header that
    carries ``narration``."""
    for i, line in enumerate(text.splitlines(), start=1):
        if f'"{narration}"' in line and line.lstrip().startswith(
            tuple("0123456789")
        ):
            return i
    raise AssertionError(f"no txn header for {narration!r}")


class TestHappyPath:
    def test_rewrites_fixme_to_real_account(
        self, ledger_dir, monkeypatch
    ):
        # Skip actually shelling out to bean-check in tests.
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )

        txns_file = ledger_dir / "txns.bean"
        start = _txn_start_line(txns_file.read_text(encoding="utf-8"), "drive-thru")

        pre, post = rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",
            expected_amount=Decimal("5.49"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        # The new file should have the correct account; the old file
        # wouldn't have had it.
        assert "Expenses:Personal:Food:FastFood" in post
        assert "Expenses:FIXME                     5.49 USD" not in post
        # The OTHER FIXME on Apr 11 should still be intact.
        assert "Expenses:FIXME                    14.99 USD" in post

    def test_preserves_whitespace(self, ledger_dir, monkeypatch):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "drive-thru")

        rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",
            expected_amount=Decimal("5.49"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        new_text = txns_file.read_text(encoding="utf-8")
        # On-touch identity normalization mints a `lamella-txn-id`
        # line on the touched txn (NORMALIZE_TXN_IDENTITY self-healing),
        # so we expect exactly one extra line vs. the original — the
        # account replacement itself is single-line in/out.
        delta = (
            len(new_text.splitlines()) - len(original.splitlines())
        )
        assert delta == 1, (
            f"expected exactly 1 added line for lineage stamp; got {delta}"
        )
        assert "lamella-txn-id" in new_text
        # Indentation preserved on the rewritten posting.
        for line in new_text.splitlines():
            if "Expenses:Personal:Food:FastFood" in line:
                assert line.startswith("  "), (
                    f"expected 2-space indent preserved; got {line!r}"
                )

    def test_backup_file_written_before_edit(self, ledger_dir, monkeypatch):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        start = _txn_start_line(txns_file.read_text(encoding="utf-8"), "drive-thru")

        rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",
            expected_amount=Decimal("5.49"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        # The backup dir should exist and contain a copy of the
        # pre-edit file.
        backups = list(ledger_dir.glob(".pre-inplace-*"))
        assert backups, "expected a .pre-inplace-* backup dir"
        backup_file = backups[0] / "txns.bean"
        assert backup_file.exists()
        # And the backup should still carry the original FIXME line.
        assert "Expenses:FIXME                     5.49 USD" in (
            backup_file.read_text(encoding="utf-8")
        )


class TestOnTouchIdentityNormalization:
    """In-place rewrites self-heal legacy identity meta on the touched txn —
    NORMALIZE_TXN_IDENTITY's converge-as-the-user-goes contract."""

    def test_mints_lineage_when_missing(self, ledger_dir, monkeypatch):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        assert "lamella-txn-id" not in original
        start = _txn_start_line(original, "drive-thru")

        rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",
            expected_amount=Decimal("5.49"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        new_text = txns_file.read_text(encoding="utf-8")
        # The drive-thru txn should now have lineage stamped on it.
        # The Apr 11 STAPLES txn (we didn't touch) should NOT.
        drive_thru_block = new_text.split('2026-04-11')[0]
        staples_block = '2026-04-11' + new_text.split('2026-04-11')[1]
        assert "lamella-txn-id" in drive_thru_block
        assert "lamella-txn-id" not in staples_block

    def test_migrates_legacy_simplefin_id_to_paired_source(
        self, ledger_dir, monkeypatch,
    ):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        # Stamp a legacy SimpleFIN id on the drive-thru txn.
        txns_file = ledger_dir / "txns.bean"
        text = txns_file.read_text(encoding="utf-8")
        modified = text.replace(
            '2026-04-10 * "MCDONALDS" "drive-thru"\n',
            '2026-04-10 * "MCDONALDS" "drive-thru"\n'
            '  lamella-simplefin-id: "TRN-MCD-1"\n',
        )
        txns_file.write_text(modified, encoding="utf-8")
        start = _txn_start_line(modified, "drive-thru")

        rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",
            expected_amount=Decimal("5.49"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        new_text = txns_file.read_text(encoding="utf-8")
        # Legacy txn-level key migrated → paired indexed source on
        # source-side posting; lineage minted.
        assert "lamella-simplefin-id:" not in new_text
        assert 'lamella-source-0: "simplefin"' in new_text
        assert 'lamella-source-reference-id-0: "TRN-MCD-1"' in new_text
        assert "lamella-txn-id:" in new_text

    def test_does_not_re_mint_when_lineage_already_present(
        self, ledger_dir, monkeypatch,
    ):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        # Stamp lineage on the drive-thru txn already.
        txns_file = ledger_dir / "txns.bean"
        text = txns_file.read_text(encoding="utf-8")
        existing = "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"
        modified = text.replace(
            '2026-04-10 * "MCDONALDS" "drive-thru"\n',
            '2026-04-10 * "MCDONALDS" "drive-thru"\n'
            f'  lamella-txn-id: "{existing}"\n',
        )
        txns_file.write_text(modified, encoding="utf-8")
        start = _txn_start_line(modified, "drive-thru")

        rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",
            expected_amount=Decimal("5.49"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        new_text = txns_file.read_text(encoding="utf-8")
        assert new_text.count(f'"{existing}"') == 1
        # No second lineage stamped.
        assert new_text.count("lamella-txn-id:") == 1


class TestSafetyGuards:
    def test_refuses_path_outside_ledger_dir(
        self, ledger_dir, tmp_path, monkeypatch
    ):
        outside = tmp_path / "outside.bean"
        outside.write_text("not your file\n", encoding="utf-8")
        with pytest.raises(InPlaceRewriteError, match="outside ledger_dir"):
            rewrite_fixme_to_account(
                source_file=outside,
                line_number=1,
                old_account="Expenses:FIXME",
                new_account="Expenses:Personal:Food:FastFood",
                expected_amount=None,
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
                run_check=False,
            )
        # File untouched.
        assert outside.read_text(encoding="utf-8") == "not your file\n"

    def test_refuses_when_amount_mismatch(self, ledger_dir, monkeypatch):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "drive-thru")

        with pytest.raises(InPlaceRewriteError, match="no posting line"):
            rewrite_fixme_to_account(
                source_file=txns_file,
                line_number=start,
                old_account="Expenses:FIXME",
                new_account="Expenses:Personal:Food:FastFood",
                expected_amount=Decimal("999.00"),  # wrong amount
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        # File untouched.
        assert txns_file.read_text(encoding="utf-8") == original

    def test_only_edits_the_targeted_posting(
        self, ledger_dir, monkeypatch
    ):
        """Both the Apr 10 and Apr 11 txns have FIXME postings. The
        rewrite must only change the one we asked for."""
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        start_apr11 = _txn_start_line(
            txns_file.read_text(encoding="utf-8"), "printer paper",
        )
        rewrite_fixme_to_account(
            source_file=txns_file,
            line_number=start_apr11,
            old_account="Expenses:FIXME",
            new_account="Expenses:Personal:Food:FastFood",  # intentional wrong-label; we only care about the rewrite scope
            expected_amount=Decimal("14.99"),
            ledger_dir=ledger_dir,
            main_bean=ledger_dir / "main.bean",
        )
        new_text = txns_file.read_text(encoding="utf-8")
        # Apr 10's FIXME must still be there.
        assert "Expenses:FIXME                     5.49 USD" in new_text
        # Apr 11's FIXME must be replaced.
        assert "Expenses:FIXME                    14.99 USD" not in new_text


class TestRollback:
    def test_bean_check_failure_rolls_back(
        self, ledger_dir, monkeypatch
    ):
        """If bean-check rejects the post-edit state, the source
        file must be restored byte-identical."""
        from lamella.core.ledger_writer import BeanCheckError

        def _fake_check(main_bean):
            # Pretend bean-check produces no errors at baseline.
            return None
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check", _fake_check,
        )
        def _fake_vs_baseline(main_bean, baseline):
            raise BeanCheckError("synthetic post-edit failure")
        monkeypatch.setattr(
            "lamella.core.rewrite.txn_inplace.run_bean_check_vs_baseline",
            _fake_vs_baseline,
        )

        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "drive-thru")

        with pytest.raises(InPlaceRewriteError, match="bean-check"):
            rewrite_fixme_to_account(
                source_file=txns_file,
                line_number=start,
                old_account="Expenses:FIXME",
                new_account="Expenses:Personal:Food:FastFood",
                expected_amount=Decimal("5.49"),
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        # File restored.
        assert txns_file.read_text(encoding="utf-8") == original


class TestMultiPostingRewrite:
    """rewrite_fixme_to_multiple_postings — replace one FIXME line
    with N posting lines that sum to the original amount. Same
    backup / bean-check / rollback discipline as the single-line
    rewriter."""

    def test_replaces_one_fixme_with_two_postings(
        self, ledger_dir, monkeypatch
    ):
        # Need an extra account in the chart so the split target
        # validates. Append it to main.bean.
        main_bean = ledger_dir / "main.bean"
        main_bean.write_text(
            main_bean.read_text(encoding="utf-8")
            + "2023-01-01 open Expenses:Personal:Office:Supplies USD\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        start = _txn_start_line(
            txns_file.read_text(encoding="utf-8"), "printer paper",
        )

        # Split $14.99 into $9.99 office supplies + $5.00 food.
        pre, post = rewrite_fixme_to_multiple_postings(
            source_file=txns_file,
            line_number=start,
            old_account="Expenses:FIXME",
            splits=[
                ("Expenses:Personal:Office:Supplies", Decimal("9.99")),
                ("Expenses:Personal:Food:FastFood", Decimal("5.00")),
            ],
            expected_amount=Decimal("14.99"),
            currency="USD",
            ledger_dir=ledger_dir,
            main_bean=main_bean,
        )

        # The original FIXME line is gone.
        assert "Expenses:FIXME                    14.99 USD" not in post
        # Both new postings landed.
        assert "Expenses:Personal:Office:Supplies" in post
        assert "Expenses:Personal:Food:FastFood" in post
        assert "9.99 USD" in post
        # Indentation preserved (2 spaces — matches the source).
        for line in post.splitlines():
            if "Expenses:Personal:Office:Supplies" in line:
                assert line.startswith("  "), (
                    f"expected 2-space indent; got {line!r}"
                )
        # Other transaction's FIXME still intact.
        assert "Expenses:FIXME                     5.49 USD" in post

    def test_refuses_when_splits_dont_sum_to_original(
        self, ledger_dir, monkeypatch
    ):
        """The whole reason this function exists is to preserve
        balance. If sum(splits) != expected_amount, the rewriter
        must refuse BEFORE touching the file."""
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "printer paper")

        with pytest.raises(
            InPlaceRewriteError, match="sum to|preserve transaction balance",
        ):
            rewrite_fixme_to_multiple_postings(
                source_file=txns_file,
                line_number=start,
                old_account="Expenses:FIXME",
                splits=[
                    ("Expenses:Personal:Food:FastFood", Decimal("9.00")),
                    ("Expenses:Personal:Food:FastFood", Decimal("5.00")),
                ],  # sums to 14.00, expected 14.99
                expected_amount=Decimal("14.99"),
                currency="USD",
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        # File untouched.
        assert txns_file.read_text(encoding="utf-8") == original

    def test_refuses_empty_splits(self, ledger_dir, monkeypatch):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        with pytest.raises(InPlaceRewriteError, match="at least one"):
            rewrite_fixme_to_multiple_postings(
                source_file=txns_file,
                line_number=1,
                old_account="Expenses:FIXME",
                splits=[],
                expected_amount=Decimal("14.99"),
                currency="USD",
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )

    def test_bean_check_failure_rolls_back_multi(
        self, ledger_dir, monkeypatch
    ):
        """Same rollback discipline as the single-line variant —
        if bean-check rejects the result, the file restores
        byte-identical from the snapshot."""
        from lamella.core.ledger_writer import BeanCheckError

        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )

        def _fake_vs_baseline(main_bean, baseline):
            raise BeanCheckError("synthetic post-edit failure")
        monkeypatch.setattr(
            "lamella.core.rewrite.txn_inplace.run_bean_check_vs_baseline",
            _fake_vs_baseline,
        )

        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "printer paper")

        with pytest.raises(InPlaceRewriteError, match="bean-check"):
            rewrite_fixme_to_multiple_postings(
                source_file=txns_file,
                line_number=start,
                old_account="Expenses:FIXME",
                splits=[
                    ("Expenses:Personal:Food:FastFood", Decimal("9.99")),
                    ("Expenses:Personal:Food:FastFood", Decimal("5.00")),
                ],
                expected_amount=Decimal("14.99"),
                currency="USD",
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        assert txns_file.read_text(encoding="utf-8") == original

    def test_signed_amounts_preserve_source_sign(
        self, ledger_dir, monkeypatch
    ):
        """The FIXME posting in the fixture is +14.99 USD. Splits
        should be positive too. If a caller passes negatives by
        mistake, the sum check catches it (positive expected,
        negative sum)."""
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        start = _txn_start_line(
            txns_file.read_text(encoding="utf-8"), "printer paper",
        )

        with pytest.raises(InPlaceRewriteError, match="sum to|preserve"):
            rewrite_fixme_to_multiple_postings(
                source_file=txns_file,
                line_number=start,
                old_account="Expenses:FIXME",
                splits=[
                    ("Expenses:Personal:Food:FastFood", Decimal("-9.99")),
                    ("Expenses:Personal:Food:FastFood", Decimal("-5.00")),
                ],
                expected_amount=Decimal("14.99"),
                currency="USD",
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )


# ---------------------------------------- M → N rewriter


from lamella.core.rewrite.txn_inplace import rewrite_txn_postings


class TestMtoNRewrite:
    """rewrite_txn_postings — replace the entire posting block of a
    transaction with new postings. Handles 1→1, 1→N, M→N, and lets
    the caller stamp txn-level meta. Same backup / bean-check /
    rollback discipline."""

    def test_replaces_two_postings_with_four(
        self, ledger_dir, monkeypatch
    ):
        """The intercompany / undo-redo case: original has 2
        postings (Card + FIXME), result has 4 (Card + DueFrom +
        DueTo + expense). The Card line gets rewritten too — it's
        legitimately part of the new structure."""
        # Add the new accounts the test needs.
        main_bean = ledger_dir / "main.bean"
        main_bean.write_text(
            main_bean.read_text(encoding="utf-8")
            + "2023-01-01 open Assets:Personal:DueFrom:Acme USD\n"
              "2023-01-01 open Liabilities:Acme:DueTo:Personal USD\n"
              "2023-01-01 open Expenses:Acme:Office:Supplies USD\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        start = _txn_start_line(
            txns_file.read_text(encoding="utf-8"), "drive-thru",
        )

        # 4-leg intercompany rewrite.
        pre, post = rewrite_txn_postings(
            source_file=txns_file,
            txn_start_line=start,
            new_postings=[
                ("Liabilities:Personal:Card:Chase", Decimal("-5.49"), "USD"),
                ("Assets:Personal:DueFrom:Acme", Decimal("5.49"), "USD"),
                ("Liabilities:Acme:DueTo:Personal", Decimal("-5.49"), "USD"),
                ("Expenses:Acme:Office:Supplies", Decimal("5.49"), "USD"),
            ],
            ledger_dir=ledger_dir,
            main_bean=main_bean,
        )

        # All four new postings present.
        assert "Assets:Personal:DueFrom:Acme  5.49 USD" in post
        assert "Liabilities:Acme:DueTo:Personal  -5.49 USD" in post
        assert "Expenses:Acme:Office:Supplies  5.49 USD" in post
        # The original FIXME line is gone.
        assert "Expenses:FIXME                     5.49 USD" not in post
        # The other transaction is untouched.
        assert "Expenses:FIXME                    14.99 USD" in post

    def test_refuses_unbalanced_posting_block(
        self, ledger_dir, monkeypatch
    ):
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "drive-thru")

        with pytest.raises(
            InPlaceRewriteError, match="balance to zero",
        ):
            rewrite_txn_postings(
                source_file=txns_file,
                txn_start_line=start,
                new_postings=[
                    ("Liabilities:Personal:Card:Chase", Decimal("-5.49"), "USD"),
                    ("Expenses:Personal:Food:FastFood", Decimal("4.00"), "USD"),
                ],  # sums to -1.49
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        assert txns_file.read_text(encoding="utf-8") == original

    def test_safety_check_old_accounts(
        self, ledger_dir, monkeypatch
    ):
        """If the caller supplies expected_old_accounts and the
        actual block doesn't match, refuse."""
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "drive-thru")

        with pytest.raises(
            InPlaceRewriteError, match="don't match",
        ):
            rewrite_txn_postings(
                source_file=txns_file,
                txn_start_line=start,
                new_postings=[
                    ("Liabilities:Personal:Card:Chase", Decimal("-5.49"), "USD"),
                    ("Expenses:Personal:Food:FastFood", Decimal("5.49"), "USD"),
                ],
                expected_old_accounts=[
                    "SomeOther:Account",  # wrong on purpose
                    "Expenses:FIXME",
                ],
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        assert txns_file.read_text(encoding="utf-8") == original

    def test_inserts_extra_meta_after_header(
        self, ledger_dir, monkeypatch
    ):
        """Loan auto-classify use case: rewrite the postings AND
        stamp `lamella-loan-slug` / `lamella-loan-autoclass-tier` meta
        on the txn so the sustained-overflow detector can read
        it back."""
        main_bean = ledger_dir / "main.bean"
        main_bean.write_text(
            main_bean.read_text(encoding="utf-8")
            + "2023-01-01 open Liabilities:Personal:Mortgage USD\n"
              "2023-01-01 open Expenses:Personal:Mortgage:Interest USD\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        txns_file = ledger_dir / "txns.bean"
        start = _txn_start_line(
            txns_file.read_text(encoding="utf-8"), "printer paper",
        )

        rewrite_txn_postings(
            source_file=txns_file,
            txn_start_line=start,
            new_postings=[
                ("Liabilities:Personal:Card:Chase", Decimal("-14.99"), "USD"),
                ("Liabilities:Personal:Mortgage", Decimal("3.00"), "USD"),
                ("Expenses:Personal:Mortgage:Interest", Decimal("11.99"), "USD"),
            ],
            extra_meta=[
                ("lamella-loan-slug", "TestMortgage"),
                ("lamella-loan-autoclass-tier", "exact"),
            ],
            ledger_dir=ledger_dir,
            main_bean=main_bean,
        )
        new_text = txns_file.read_text(encoding="utf-8")
        # Meta keys present, values quoted, on indented lines.
        assert 'lamella-loan-slug: "TestMortgage"' in new_text
        assert 'lamella-loan-autoclass-tier: "exact"' in new_text
        # Postings present.
        assert "Liabilities:Personal:Mortgage  3.00 USD" in new_text
        assert "Expenses:Personal:Mortgage:Interest  11.99 USD" in new_text

    def test_rollback_on_bean_check_failure(
        self, ledger_dir, monkeypatch
    ):
        from lamella.core.ledger_writer import BeanCheckError

        monkeypatch.setattr(
            "lamella.features.receipts.linker.run_bean_check",
            lambda main_bean: None,
        )
        def _fake_vs_baseline(main_bean, baseline):
            raise BeanCheckError("synthetic post-edit failure")
        monkeypatch.setattr(
            "lamella.core.rewrite.txn_inplace.run_bean_check_vs_baseline",
            _fake_vs_baseline,
        )

        txns_file = ledger_dir / "txns.bean"
        original = txns_file.read_text(encoding="utf-8")
        start = _txn_start_line(original, "drive-thru")

        with pytest.raises(InPlaceRewriteError, match="bean-check"):
            rewrite_txn_postings(
                source_file=txns_file,
                txn_start_line=start,
                new_postings=[
                    ("Liabilities:Personal:Card:Chase", Decimal("-5.49"), "USD"),
                    ("Expenses:Personal:Food:FastFood", Decimal("5.49"), "USD"),
                ],
                ledger_dir=ledger_dir,
                main_bean=ledger_dir / "main.bean",
            )
        assert txns_file.read_text(encoding="utf-8") == original
