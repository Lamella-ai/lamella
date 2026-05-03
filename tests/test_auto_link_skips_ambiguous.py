# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0063 §3: confidence-gap rule.

When the top candidate scores >= AUTO_LINK_THRESHOLD AND the
second-place candidate trails by less than the configured gap,
NEITHER is auto-linked. The doc is queued for review instead.

Two near-equal candidates are ambiguous; the scorer can't pick
between them, so we let the human decide rather than silently
choose the wrong one.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data
from beancount.core.amount import Amount

from lamella.core.db import connect, migrate
from lamella.features.receipts.auto_match import auto_link_unlinked_documents


def _make_txn(*, txn_date, payee, amount_str, narration=None):
    amt = Decimal(amount_str)
    return data.Transaction(
        meta={"filename": "test", "lineno": 1},
        date=txn_date,
        flag="*",
        payee=payee,
        narration=narration or payee,
        tags=set(),
        links=set(),
        postings=[
            data.Posting(
                account="Expenses:Personal:Misc",
                units=Amount(amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
            data.Posting(
                account="Assets:Personal:Checking",
                units=Amount(-amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


def _seed_doc(conn):
    conn.execute(
        "INSERT INTO paperless_doc_index "
        "(paperless_id, title, total_amount, document_date, document_type, "
        " document_type_name, correspondent_name, content_excerpt) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            500, "Vendor Receipt", "42.00", "2026-04-17",
            "receipt", "Receipt", "Vendor", "",
        ),
    )


class _StubReader:
    def __init__(self, entries):
        self._entries = entries

    def load(self):
        class _R:
            entries = self._entries
        r = _R()
        r.entries = self._entries
        return r


def test_two_near_equal_candidates_are_not_auto_linked(db, tmp_path):
    """The top two candidates score 0.92 and 0.91 (gap 0.01 < 0.10).
    Neither is auto-linked; both are queued for review."""
    _seed_doc(db)
    # Two same-day exact-amount txns from the same vendor: scores are
    # essentially identical. The scorer can't tell them apart.
    entries = [
        _make_txn(
            txn_date=date(2026, 4, 17),
            payee="Vendor",
            amount_str="42.00",
            narration="VENDOR PURCHASE A",
        ),
        _make_txn(
            txn_date=date(2026, 4, 17),
            payee="Vendor",
            amount_str="42.00",
            narration="VENDOR PURCHASE B",
        ),
    ]
    from lamella.core.config import Settings
    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "ledger",
    )
    (tmp_path / "ledger").mkdir()
    (tmp_path / "ledger" / "main.bean").write_text("")

    report = auto_link_unlinked_documents(
        db,
        reader=_StubReader(entries),
        settings=settings,
        confidence_gap=0.10,
        dry_run=True,
    )
    # Top score >= 0.90 but the gap is too small — must skip both.
    assert report.linked == 0
    assert report.skipped_ambiguous == 1
    assert report.queued_for_review == 1


def test_clear_winner_is_auto_linked(db, tmp_path):
    """When the top candidate dominates by more than the gap, it
    DOES auto-link (sanity check that the gap rule isn't always
    blocking)."""
    _seed_doc(db)
    entries = [
        # Strong match: same day exact amount + content overlap.
        _make_txn(
            txn_date=date(2026, 4, 17),
            payee="Vendor",
            amount_str="42.00",
            narration="VENDOR EXACT MATCH",
        ),
        # Weak match: 25 days off + amount mismatch.
        _make_txn(
            txn_date=date(2026, 3, 23),
            payee="Different",
            amount_str="1.00",
            narration="UNRELATED",
        ),
    ]
    from lamella.core.config import Settings
    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "ledger",
    )
    (tmp_path / "ledger").mkdir()
    (tmp_path / "ledger" / "main.bean").write_text("")

    report = auto_link_unlinked_documents(
        db,
        reader=_StubReader(entries),
        settings=settings,
        confidence_gap=0.10,
        dry_run=True,
    )
    assert report.linked == 1
    assert report.skipped_ambiguous == 0
