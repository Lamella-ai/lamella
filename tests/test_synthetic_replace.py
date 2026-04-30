# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0046 Phase 2 — synthetic replacement matcher + rewriter.

Pin the find-and-replace round trip:
- Match by (account, signed amount, date within window).
- Honor lamella-synthetic-replaceable: TRUE; refuse to auto-replace
  legs with FALSE.
- In-place rewrite preserves the transaction's lamella-txn-id, strips
  the four synthetic-* meta keys, and adds paired lamella-source-N /
  lamella-source-reference-id-N lines.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.features.bank_sync.synthetic_replace import (
    demote_synthetic_to_replaceable,
    find_loose_synthetic_match,
    find_replaceable_synthetic_match,
    promote_synthetic_to_confirmed,
    replace_synthetic_in_place,
    rewrite_synthetic_account_in_place,
)


SYNTHETIC_BLOCK = '''
2026-04-22 * "PAYPAL TRANSFER 1049791515428"
  lamella-txn-id: "01JN3F7Z9XABCDEF1234567890AB"
  Assets:Personal:BankOne:Checking  -840.82 USD
    lamella-source-0: "simplefin"
    lamella-source-reference-id-0: "TRN-aaa"
  Assets:Personal:PayPal             840.82 USD
    lamella-synthetic: "user-classified-counterpart"
    lamella-synthetic-confidence: "guessed"
    lamella-synthetic-replaceable: TRUE
    lamella-synthetic-decided-at: "2026-04-22T18:32:14+00:00"
'''


def _load_entries(text: str):
    """Parse a small inline ledger into Beancount entries."""
    from beancount.loader import load_string
    entries, errors, _ = load_string(text)
    assert not errors, errors
    return entries


@pytest.fixture
def simple_ledger() -> str:
    """Minimal valid ledger carrying one synthetic-counterpart txn."""
    return (
        '1970-01-01 open Assets:Personal:BankOne:Checking\n'
        '1970-01-01 open Assets:Personal:PayPal\n'
        + SYNTHETIC_BLOCK
    )


class TestFindReplaceableSyntheticMatch:
    def test_match_same_account_same_amount_same_date(self, simple_ledger):
        entries = _load_entries(simple_ledger)
        match = find_replaceable_synthetic_match(
            entries,
            account="Assets:Personal:PayPal",
            amount=Decimal("840.82"),
            posted_date=date(2026, 4, 22),
        )
        assert match is not None
        assert match["lamella_txn_id"] == "01JN3F7Z9XABCDEF1234567890AB"
        assert match["posting_account"] == "Assets:Personal:PayPal"

    def test_match_within_5_day_window(self, simple_ledger):
        entries = _load_entries(simple_ledger)
        # Synthetic leg dated 2026-04-22, incoming row dated 04-25 (3d).
        match = find_replaceable_synthetic_match(
            entries,
            account="Assets:Personal:PayPal",
            amount=Decimal("840.82"),
            posted_date=date(2026, 4, 25),
        )
        assert match is not None

    def test_no_match_outside_window(self, simple_ledger):
        entries = _load_entries(simple_ledger)
        match = find_replaceable_synthetic_match(
            entries,
            account="Assets:Personal:PayPal",
            amount=Decimal("840.82"),
            posted_date=date(2026, 5, 10),  # 18d outside window
        )
        assert match is None

    def test_no_match_different_account(self, simple_ledger):
        entries = _load_entries(simple_ledger)
        match = find_replaceable_synthetic_match(
            entries,
            account="Assets:Personal:Venmo",
            amount=Decimal("840.82"),
            posted_date=date(2026, 4, 22),
        )
        assert match is None

    def test_no_match_different_amount(self, simple_ledger):
        entries = _load_entries(simple_ledger)
        match = find_replaceable_synthetic_match(
            entries,
            account="Assets:Personal:PayPal",
            amount=Decimal("100.00"),
            posted_date=date(2026, 4, 22),
        )
        assert match is None

    def test_loose_match_finds_different_account(self, simple_ledger):
        # Loose matcher: same date+amount but the incoming row's
        # account is NOT the synthetic posting's account. Used for
        # the wrong-account guess case.
        entries = _load_entries(simple_ledger)
        match = find_loose_synthetic_match(
            entries,
            amount=Decimal("840.82"),
            posted_date=date(2026, 4, 22),
            exclude_account="Assets:Personal:Venmo",
        )
        assert match is not None
        # Synthetic was on PayPal; incoming was on Venmo.
        assert match["synthetic_account"] == "Assets:Personal:PayPal"

    def test_loose_match_excludes_same_account(self, simple_ledger):
        # Strict-match case (same account) should NOT surface as a
        # loose match — that's the strict-match path's job.
        entries = _load_entries(simple_ledger)
        match = find_loose_synthetic_match(
            entries,
            amount=Decimal("840.82"),
            posted_date=date(2026, 4, 22),
            exclude_account="Assets:Personal:PayPal",
        )
        assert match is None

    def test_replaceable_false_skips_match(self):
        # Same shape but synthetic-replaceable: FALSE.
        text = (
            '1970-01-01 open Assets:Personal:BankOne:Checking\n'
            '1970-01-01 open Assets:Personal:PayPal\n'
            '\n'
            '2026-04-22 * "PAYPAL TRANSFER"\n'
            '  lamella-txn-id: "01JN3F7Z9XABCDEF1234567890AB"\n'
            '  Assets:Personal:BankOne:Checking  -840.82 USD\n'
            '    lamella-source-0: "simplefin"\n'
            '    lamella-source-reference-id-0: "TRN-aaa"\n'
            '  Assets:Personal:PayPal             840.82 USD\n'
            '    lamella-synthetic: "user-classified-counterpart"\n'
            '    lamella-synthetic-confidence: "confirmed"\n'
            '    lamella-synthetic-replaceable: FALSE\n'
            '    lamella-synthetic-decided-at: "2026-04-22T18:32:14+00:00"\n'
        )
        entries = _load_entries(text)
        match = find_replaceable_synthetic_match(
            entries,
            account="Assets:Personal:PayPal",
            amount=Decimal("840.82"),
            posted_date=date(2026, 4, 22),
        )
        assert match is None


class TestReplaceSyntheticInPlace:
    def test_strips_synthetic_meta_and_adds_source_meta(
        self, tmp_path: Path, simple_ledger: str,
    ):
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        ok = replace_synthetic_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
            source="simplefin",
            source_reference_id="TRN-bbb",
        )
        assert ok is True
        out = f.read_text(encoding="utf-8")
        # All four synthetic-* keys are gone.
        assert "lamella-synthetic:" not in out
        assert "lamella-synthetic-confidence:" not in out
        assert "lamella-synthetic-replaceable:" not in out
        assert "lamella-synthetic-decided-at:" not in out
        # Real source meta was added (index 0 since the synthetic
        # posting had no other source meta on it).
        assert 'lamella-source-0: "simplefin"' in out
        assert 'lamella-source-reference-id-0: "TRN-bbb"' in out
        # The lamella-txn-id is unchanged — identity stays stable.
        assert '"01JN3F7Z9XABCDEF1234567890AB"' in out
        # The source-side posting's existing meta is preserved.
        assert 'lamella-source-reference-id-0: "TRN-aaa"' in out

    def test_returns_false_for_unknown_txn_id(
        self, tmp_path: Path, simple_ledger: str,
    ):
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        ok = replace_synthetic_in_place(
            bean_file=f,
            lamella_txn_id="01JNOMATCH",
            posting_account="Assets:Personal:PayPal",
            source="simplefin",
            source_reference_id="TRN-bbb",
        )
        assert ok is False
        # File unchanged.
        assert f.read_text(encoding="utf-8") == simple_ledger

    def test_idempotent_no_synthetic_meta_left(
        self, tmp_path: Path, simple_ledger: str,
    ):
        # Running replace twice should be safe — the second call finds
        # no synthetic meta on the posting and returns False.
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        replace_synthetic_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
            source="simplefin",
            source_reference_id="TRN-bbb",
        )
        ok2 = replace_synthetic_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
            source="simplefin",
            source_reference_id="TRN-ccc",  # different ref
        )
        assert ok2 is False
        # First replacement still in place.
        out = f.read_text(encoding="utf-8")
        assert 'TRN-bbb' in out
        assert 'TRN-ccc' not in out

    def test_promote_to_confirmed_flips_replaceable_flag(
        self, tmp_path: Path, simple_ledger: str,
    ):
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        ok = promote_synthetic_to_confirmed(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
        )
        assert ok is True
        out = f.read_text(encoding="utf-8")
        assert "lamella-synthetic-replaceable: FALSE" in out
        assert "lamella-synthetic-replaceable: TRUE" not in out
        assert 'lamella-synthetic-confidence: "confirmed"' in out
        # Other meta keys preserved (provenance + decided_at).
        assert "lamella-synthetic:" in out
        assert "lamella-synthetic-decided-at:" in out

    def test_promote_idempotent_on_already_confirmed(
        self, tmp_path: Path, simple_ledger: str,
    ):
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        promote_synthetic_to_confirmed(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
        )
        ok2 = promote_synthetic_to_confirmed(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
        )
        assert ok2 is False  # already FALSE; no change

    def test_demote_round_trips_promote(
        self, tmp_path: Path, simple_ledger: str,
    ):
        """ADR-0046 Phase 4b — demote inverts promote."""
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        promote_synthetic_to_confirmed(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
        )
        ok = demote_synthetic_to_replaceable(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
        )
        assert ok is True
        out = f.read_text(encoding="utf-8")
        assert "lamella-synthetic-replaceable: TRUE" in out
        assert "lamella-synthetic-replaceable: FALSE" not in out
        assert 'lamella-synthetic-confidence: "guessed"' in out

    def test_demote_idempotent_on_already_replaceable(
        self, tmp_path: Path, simple_ledger: str,
    ):
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(simple_ledger, encoding="utf-8")
        # simple_ledger already has replaceable=TRUE; demote is a no-op.
        ok = demote_synthetic_to_replaceable(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
        )
        assert ok is False

    def test_round_trip_parses_under_beancount(
        self, tmp_path: Path,
    ):
        ledger = (
            '1970-01-01 open Assets:Personal:BankOne:Checking\n'
            '1970-01-01 open Assets:Personal:PayPal\n'
            + SYNTHETIC_BLOCK
        )
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(ledger, encoding="utf-8")
        replace_synthetic_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            posting_account="Assets:Personal:PayPal",
            source="simplefin",
            source_reference_id="TRN-bbb",
        )
        # Parse the rewritten file — it must still be valid Beancount.
        from beancount.loader import load_string
        entries, errors, _ = load_string(f.read_text(encoding="utf-8"))
        assert not errors, errors
        # Find the txn and its real-now-PayPal posting.
        from beancount.core.data import Transaction
        txns = [e for e in entries if isinstance(e, Transaction)]
        assert len(txns) == 1
        paypal = next(
            p for p in txns[0].postings
            if p.account == "Assets:Personal:PayPal"
        )
        assert (paypal.meta or {}).get("lamella-source-0") == "simplefin"
        assert (paypal.meta or {}).get("lamella-source-reference-id-0") == "TRN-bbb"
        # Synthetic keys really are gone from the parsed entry too.
        for k in (
            "lamella-synthetic",
            "lamella-synthetic-confidence",
            "lamella-synthetic-replaceable",
            "lamella-synthetic-decided-at",
        ):
            assert k not in (paypal.meta or {})


# ---------------------------------------------------------------------
# ADR-0046 Phase 3b — wrong-account rewrite helper
# ---------------------------------------------------------------------


class TestRewriteSyntheticAccountInPlace:

    def test_renames_account_and_swaps_meta(self, tmp_path: Path):
        ledger = (
            '1970-01-01 open Assets:Personal:BankOne:Checking\n'
            '1970-01-01 open Assets:Personal:WrongAccount\n'
            '1970-01-01 open Assets:Personal:RightAccount\n'
            + SYNTHETIC_BLOCK.replace(
                "Assets:Personal:PayPal", "Assets:Personal:WrongAccount",
            )
        )
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(ledger, encoding="utf-8")
        ok = rewrite_synthetic_account_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            wrong_account="Assets:Personal:WrongAccount",
            right_account="Assets:Personal:RightAccount",
            source="simplefin",
            source_reference_id="TRN-correct",
        )
        assert ok is True
        text = f.read_text(encoding="utf-8")
        # The Open directive at the top still references WrongAccount;
        # we only rewrite postings inside the matched transaction. So
        # we assert the posting was renamed by checking the txn block.
        block_start = text.index("PAYPAL TRANSFER")
        block = text[block_start:]
        assert "Assets:Personal:WrongAccount" not in block
        assert "Assets:Personal:RightAccount" in block
        assert 'lamella-source-0: "simplefin"' in text
        assert 'lamella-source-reference-id-0: "TRN-correct"' in text
        # All four synthetic keys must be gone from the renamed posting.
        for k in (
            "lamella-synthetic",
            "lamella-synthetic-confidence",
            "lamella-synthetic-replaceable",
            "lamella-synthetic-decided-at",
        ):
            assert k not in text or k + ":" not in text

    def test_preserves_lamella_txn_id(self, tmp_path: Path):
        ledger = (
            '1970-01-01 open Assets:Personal:BankOne:Checking\n'
            '1970-01-01 open Assets:Personal:WrongAccount\n'
            '1970-01-01 open Assets:Personal:RightAccount\n'
            + SYNTHETIC_BLOCK.replace(
                "Assets:Personal:PayPal", "Assets:Personal:WrongAccount",
            )
        )
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(ledger, encoding="utf-8")
        rewrite_synthetic_account_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            wrong_account="Assets:Personal:WrongAccount",
            right_account="Assets:Personal:RightAccount",
            source="simplefin",
            source_reference_id="TRN-correct",
        )
        text = f.read_text(encoding="utf-8")
        # Original txn-id must survive the rewrite (URL stability).
        assert '"01JN3F7Z9XABCDEF1234567890AB"' in text

    def test_idempotent_on_already_renamed_block(self, tmp_path: Path):
        ledger = (
            '1970-01-01 open Assets:Personal:BankOne:Checking\n'
            '1970-01-01 open Assets:Personal:WrongAccount\n'
            '1970-01-01 open Assets:Personal:RightAccount\n'
            + SYNTHETIC_BLOCK.replace(
                "Assets:Personal:PayPal", "Assets:Personal:WrongAccount",
            )
        )
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(ledger, encoding="utf-8")
        first = rewrite_synthetic_account_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            wrong_account="Assets:Personal:WrongAccount",
            right_account="Assets:Personal:RightAccount",
            source="simplefin",
            source_reference_id="TRN-x",
        )
        assert first is True
        # Second run can't find the wrong-account posting any more.
        second = rewrite_synthetic_account_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            wrong_account="Assets:Personal:WrongAccount",
            right_account="Assets:Personal:RightAccount",
            source="simplefin",
            source_reference_id="TRN-x",
        )
        assert second is False

    def test_returns_false_when_txn_id_missing(self, tmp_path: Path):
        f = tmp_path / "empty.bean"
        f.write_text(
            '1970-01-01 open Assets:Personal:BankOne:Checking\n',
            encoding="utf-8",
        )
        ok = rewrite_synthetic_account_in_place(
            bean_file=f,
            lamella_txn_id="not-here",
            wrong_account="Assets:Personal:Whatever",
            right_account="Assets:Personal:RightAccount",
            source="simplefin",
            source_reference_id="TRN-x",
        )
        assert ok is False

    def test_round_trip_parses_under_beancount(self, tmp_path: Path):
        ledger = (
            '1970-01-01 open Assets:Personal:BankOne:Checking\n'
            '1970-01-01 open Assets:Personal:WrongAccount\n'
            '1970-01-01 open Assets:Personal:RightAccount\n'
            + SYNTHETIC_BLOCK.replace(
                "Assets:Personal:PayPal", "Assets:Personal:WrongAccount",
            )
        )
        f = tmp_path / "simplefin_transactions.bean"
        f.write_text(ledger, encoding="utf-8")
        rewrite_synthetic_account_in_place(
            bean_file=f,
            lamella_txn_id="01JN3F7Z9XABCDEF1234567890AB",
            wrong_account="Assets:Personal:WrongAccount",
            right_account="Assets:Personal:RightAccount",
            source="simplefin",
            source_reference_id="TRN-correct",
        )
        from beancount.loader import load_string
        entries, errors, _ = load_string(f.read_text(encoding="utf-8"))
        assert not errors, errors
        from beancount.core.data import Transaction
        txns = [e for e in entries if isinstance(e, Transaction)]
        assert len(txns) == 1
        right = next(
            p for p in txns[0].postings
            if p.account == "Assets:Personal:RightAccount"
        )
        # Per-posting source indexes; the cleaned synthetic meta left
        # the posting empty, so index 0 is the next free slot.
        assert (right.meta or {}).get("lamella-source-0") == "simplefin"
        assert (
            (right.meta or {}).get("lamella-source-reference-id-0")
            == "TRN-correct"
        )
