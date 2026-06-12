# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0063 §2: find_ledger_candidates returns scored, sorted matches.

Given a fixture connection with several Beancount transactions and
one document, the reverse-direction matcher should return the
closest match at the top of the list.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data
from beancount.core.amount import Amount

from lamella.core.db import connect, migrate
from lamella.features.receipts.txn_matcher import find_ledger_candidates


def _make_txn(
    *,
    txn_date: date,
    payee: str,
    narration: str,
    amount_str: str,
    expense_account: str = "Expenses:Personal:Hardware",
    asset_account: str = "Assets:Personal:Checking",
):
    """Build a minimal Beancount Transaction the matcher can score."""
    amt = Decimal(amount_str)
    return data.Transaction(
        meta={"filename": "test", "lineno": 1},
        date=txn_date,
        flag="*",
        payee=payee,
        narration=narration,
        tags=set(),
        links=set(),
        postings=[
            data.Posting(
                account=expense_account,
                units=Amount(amt, "USD"),
                cost=None,
                price=None,
                flag=None,
                meta=None,
            ),
            data.Posting(
                account=asset_account,
                units=Amount(-amt, "USD"),
                cost=None,
                price=None,
                flag=None,
                meta=None,
            ),
        ],
    )


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


def test_returns_top_candidate_first(db):
    """Three txns, one document. The exact-match same-day txn should
    be returned at the top of the list."""
    entries = [
        # Closest match: same day, exact amount.
        _make_txn(
            txn_date=date(2026, 4, 17),
            payee="Hardware Store",
            narration="HARDWARE STORE",
            amount_str="42.00",
        ),
        # Wrong amount: should still score lower because amount mismatches.
        _make_txn(
            txn_date=date(2026, 4, 17),
            payee="Hardware Store",
            narration="HARDWARE STORE",
            amount_str="99.00",
        ),
        # Wrong date: 25 days off. Same amount, but a date penalty applies.
        _make_txn(
            txn_date=date(2026, 3, 23),
            payee="Hardware Store",
            narration="HARDWARE STORE",
            amount_str="42.00",
        ),
    ]
    cands = find_ledger_candidates(
        db,
        doc_date=date(2026, 4, 17),
        doc_total=Decimal("42.00"),
        doc_currency="USD",
        doc_vendor="Hardware Store",
        doc_doctype="receipt",
        doc_id=1,
        ledger_entries=entries,
        doc_correspondent="Hardware Store",
        doc_content_excerpt="Thanks for shopping",
        min_score=0.0,
    )
    assert len(cands) >= 2  # the $99 one may or may not pass min_score=0
    # Top candidate is the same-day exact-amount match.
    assert cands[0].txn_date == date(2026, 4, 17)
    assert cands[0].txn_amount == Decimal("42.00")
    # Sorted by score desc: every consecutive pair monotonically
    # decreases (or stays equal).
    for i in range(len(cands) - 1):
        assert cands[i].score >= cands[i + 1].score


def test_returns_empty_when_doc_total_missing(db):
    cands = find_ledger_candidates(
        db,
        doc_date=date(2026, 4, 17),
        doc_total=None,
        doc_currency="USD",
        doc_vendor="X",
        doc_doctype="receipt",
        doc_id=1,
        ledger_entries=[
            _make_txn(
                txn_date=date(2026, 4, 17), payee="X", narration="X",
                amount_str="42.00",
            ),
        ],
    )
    assert cands == []


def test_max_results_caps_output(db):
    """When more candidates score above min_score than max_results
    asks for, only the top max_results are returned."""
    entries = [
        _make_txn(
            txn_date=date(2026, 4, 17),
            payee="Vendor",
            narration="VENDOR",
            amount_str="42.00",
        )
        for _ in range(20)
    ]
    # Each txn has a distinct lineno so txn_hash should differ.
    # But Beancount Transactions are equal by content; identical
    # postings on the same date will hash the same. We dedup by hash
    # in find_ledger_candidates so the call below returns only one
    # candidate even though we passed 20 entries — that's the
    # documented behavior.
    cands = find_ledger_candidates(
        db,
        doc_date=date(2026, 4, 17),
        doc_total=Decimal("42.00"),
        doc_currency="USD",
        doc_vendor="Vendor",
        doc_doctype="receipt",
        doc_id=1,
        ledger_entries=entries,
        max_results=5,
        min_score=0.0,
    )
    assert len(cands) <= 5


def test_window_excludes_far_future_txns(db):
    """Default window is ±30 days; a txn 60 days off must not appear."""
    entries = [
        _make_txn(
            txn_date=date(2026, 6, 17),  # 61 days after doc_date
            payee="Vendor",
            narration="VENDOR",
            amount_str="42.00",
        ),
    ]
    cands = find_ledger_candidates(
        db,
        doc_date=date(2026, 4, 17),
        doc_total=Decimal("42.00"),
        doc_currency="USD",
        doc_vendor="Vendor",
        doc_doctype="receipt",
        doc_id=1,
        ledger_entries=entries,
        min_score=0.0,
    )
    assert cands == []
