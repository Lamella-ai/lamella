# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date
from decimal import Decimal

from lamella.core.beancount_io import LedgerReader
from lamella.features.receipts.matcher import find_candidates


def test_find_exact_match(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    # Hardware Store on 2026-04-10 for 42.17 on card ending 1234
    candidates = find_candidates(
        loaded.entries,
        receipt_total=Decimal("42.17"),
        receipt_date=date(2026, 4, 10),
        last_four="1234",
        date_window_days=0,
    )
    assert len(candidates) == 1
    assert candidates[0].day_delta == 0
    assert "Hardware Store" in (candidates[0].txn.payee or "") + " " + (candidates[0].txn.narration or "")


def test_last_four_filters_out_other_card(ledger_dir):
    """Two $42.17 transactions exist on 2026-04-10 and 2026-04-12, one on CardA1234
    and one on CardB9876. last_four should discriminate."""
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    candidates = find_candidates(
        loaded.entries,
        receipt_total=Decimal("42.17"),
        receipt_date=date(2026, 4, 11),
        last_four="9876",
        date_window_days=3,
    )
    assert len(candidates) == 1
    assert "9876" in candidates[0].txn.postings[0].account or any(
        "9876" in p.account for p in candidates[0].txn.postings
    )


def test_ambiguous_without_last_four(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    # Same total hits both cards on consecutive days -> multiple candidates
    candidates = find_candidates(
        loaded.entries,
        receipt_total=Decimal("42.17"),
        receipt_date=date(2026, 4, 11),
        date_window_days=3,
    )
    assert len(candidates) == 2


def test_no_match_outside_window(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    candidates = find_candidates(
        loaded.entries,
        receipt_total=Decimal("42.17"),
        receipt_date=date(2026, 5, 1),
        date_window_days=1,
    )
    assert candidates == []


def test_returns_empty_on_missing_inputs(ledger_dir):
    reader = LedgerReader(ledger_dir / "main.bean")
    loaded = reader.load()
    assert find_candidates(loaded.entries, receipt_total=None, receipt_date=date(2026, 4, 10)) == []
    assert find_candidates(loaded.entries, receipt_total="42.17", receipt_date=None) == []
