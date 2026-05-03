# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Workstream B — context-richness priority sort in bulk_classify.

_context_score ranks FIXMEs by how much input the classifier has to
work with: receipt > memo > active project > nearby mileage, with
user-flag priority as a tie-breaker *within* a context bucket.

These tests lock the ranking so a later "just make user-flag win"
drive-by refactor can't silently invert the philosophy.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction

from lamella.features.ai_cascade.bulk_classify import (
    _collect_fixme_txns,
    _context_score,
)


# ---------------------------------------- helpers


def _mk_fixme_txn(
    *,
    txn_date: date = date(2024, 5, 10),
    narration: str = "",
    payee: str | None = None,
    card_account: str = "Assets:Personal:Checking",
    amount: Decimal = Decimal("42.00"),
) -> Transaction:
    return Transaction(
        meta={"filename": "x", "lineno": 1},
        date=txn_date,
        flag="*",
        payee=payee,
        narration=narration,
        tags=set(),
        links=set(),
        postings=[
            Posting(
                account=card_account,
                units=Amount(-amount, "USD"),
                cost=None, price=None, flag=None, meta={},
            ),
            Posting(
                account="Expenses:FIXME",
                units=Amount(amount, "USD"),
                cost=None, price=None, flag=None, meta={},
            ),
        ],
    )


def _fixme_row(txn: Transaction) -> tuple:
    # _context_score expects the (txn, fixme_account, amt, currency)
    # shape that _collect_fixme_txns emits.
    return (txn, "Expenses:FIXME", Decimal("42.00"), "USD")


def _compute_hash(txn: Transaction) -> str:
    from lamella.core.beancount_io.txn_hash import txn_hash
    return txn_hash(txn)


# ---------------------------------------- unit tests


def test_score_bare_txn_is_zero(db):
    """A txn with empty narration and no side tables should score 0."""
    txn = _mk_fixme_txn(narration="")
    assert _context_score(_fixme_row(txn), db) == 0


def test_score_memo_only(db):
    """Narration alone triggers the memo bit."""
    txn = _mk_fixme_txn(narration="Cafe")
    score = _context_score(_fixme_row(txn), db)
    assert score == 8 << 16


def test_score_receipt_beats_memo(db):
    """A receipted bare txn ranks above a memo-rich receipt-less txn."""
    receipted = _mk_fixme_txn(narration="")
    memo_only = _mk_fixme_txn(narration="Memo here")

    db.execute(
        "INSERT INTO document_links "
        "(paperless_id, txn_hash, match_method) VALUES (?, ?, ?)",
        (101, _compute_hash(receipted), "manual"),
    )
    db.commit()

    assert (
        _context_score(_fixme_row(receipted), db)
        > _context_score(_fixme_row(memo_only), db)
    )


def test_score_receipt_alone_beats_memo_project_mileage_combined(db):
    """Priority is strictly hierarchical: even three lower-tier bits
    stacked together do not outrank a single receipt. Protects against
    a future edit that converts priority to a flat sum."""
    receipted = _mk_fixme_txn(narration="")
    stacked = _mk_fixme_txn(
        txn_date=date(2024, 5, 10),
        narration="Memo",
        card_account="Assets:Personal:Checking",
    )

    db.execute(
        "INSERT INTO document_links "
        "(paperless_id, txn_hash, match_method) VALUES (?, ?, ?)",
        (101, _compute_hash(receipted), "manual"),
    )
    # Active project covering 2024-05-10
    db.execute(
        "INSERT INTO projects "
        "(slug, display_name, start_date, end_date, is_active) "
        "VALUES (?, ?, ?, ?, ?)",
        ("reno", "Kitchen Reno", "2024-05-01", "2024-06-30", 1),
    )
    # Mileage ± 3 days (May 10 itself)
    db.execute(
        "INSERT INTO mileage_entries "
        "(entry_date, vehicle, miles, entity) VALUES (?, ?, ?, ?)",
        ("2024-05-10", "Truck", 12.0, "Personal"),
    )
    db.commit()

    assert (
        _context_score(_fixme_row(receipted), db)
        > _context_score(_fixme_row(stacked), db)
    )


def test_score_project_signal(db):
    """Active project on the txn date flips the project bit."""
    txn = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 10))
    db.execute(
        "INSERT INTO projects "
        "(slug, display_name, start_date, end_date, is_active) "
        "VALUES (?, ?, ?, ?, ?)",
        ("reno", "Kitchen Reno", "2024-05-01", "2024-06-30", 1),
    )
    db.commit()

    assert _context_score(_fixme_row(txn), db) == 4 << 16


def test_score_project_outside_window_ignored(db):
    """Projects that don't cover the date are not a signal."""
    txn = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 10))
    db.execute(
        "INSERT INTO projects "
        "(slug, display_name, start_date, end_date, is_active) "
        "VALUES (?, ?, ?, ?, ?)",
        ("old", "Old Project", "2023-01-01", "2023-12-31", 1),
    )
    db.commit()

    assert _context_score(_fixme_row(txn), db) == 0


def test_score_mileage_within_window(db):
    """Mileage within ± 3 days of the txn flips the mileage bit."""
    txn = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 10))
    db.execute(
        "INSERT INTO mileage_entries "
        "(entry_date, vehicle, miles, entity) VALUES (?, ?, ?, ?)",
        ("2024-05-12", "Truck", 40.0, "Personal"),
    )
    db.commit()

    assert _context_score(_fixme_row(txn), db) == 2 << 16


def test_score_mileage_outside_window_ignored(db):
    """A mileage entry 10 days away does not count."""
    txn = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 10))
    db.execute(
        "INSERT INTO mileage_entries "
        "(entry_date, vehicle, miles, entity) VALUES (?, ?, ?, ?)",
        ("2024-05-20", "Truck", 40.0, "Personal"),
    )
    db.commit()

    assert _context_score(_fixme_row(txn), db) == 0


def test_score_user_flag_is_tiebreaker_not_override(db):
    """User-flag boosts within a context bucket but never crosses it:
    flagged-memo-only still ranks below unflagged-receipted."""
    memo_flagged = _mk_fixme_txn(narration="Memo")
    receipt_unflagged = _mk_fixme_txn(narration="")

    db.execute(
        "INSERT INTO document_links "
        "(paperless_id, txn_hash, match_method) VALUES (?, ?, ?)",
        (101, _compute_hash(receipt_unflagged), "manual"),
    )
    db.execute(
        "INSERT INTO review_queue (kind, source_ref, priority) "
        "VALUES (?, ?, ?)",
        ("fixme", _compute_hash(memo_flagged), 500),
    )
    db.commit()

    assert (
        _context_score(_fixme_row(receipt_unflagged), db)
        > _context_score(_fixme_row(memo_flagged), db)
    )


def test_score_user_flag_wins_within_same_bucket(db):
    """Two bare txns, one flagged — the flagged one sorts first.
    Distinct dates so the two txns hash differently; both have
    empty narration so neither gets the memo bit."""
    flagged = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 10))
    unflagged = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 11))

    db.execute(
        "INSERT INTO review_queue (kind, source_ref, priority) "
        "VALUES (?, ?, ?)",
        ("fixme", _compute_hash(flagged), 100),
    )
    db.commit()

    assert (
        _context_score(_fixme_row(flagged), db)
        > _context_score(_fixme_row(unflagged), db)
    )


def test_score_resolved_flag_ignored(db):
    """A review_queue row that's already resolved must not contribute."""
    txn = _mk_fixme_txn(narration="")
    db.execute(
        "INSERT INTO review_queue "
        "(kind, source_ref, priority, resolved_at) "
        "VALUES (?, ?, ?, ?)",
        ("fixme", _compute_hash(txn), 999, "2024-05-01T00:00:00"),
    )
    db.commit()

    assert _context_score(_fixme_row(txn), db) == 0


# ---------------------------------------- integration: ordering


def test_collect_fixme_txns_sorts_receipted_first(db):
    """Two FIXMEs in the ledger, only one has a receipt linked. The
    sort places the receipted row first regardless of ledger order.

    This is the behavior that makes `limit=1` in bulk_classify land
    the AI spend on the context-rich row instead of the bare one.
    """
    bare = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 1))
    receipted = _mk_fixme_txn(narration="", txn_date=date(2024, 5, 2))

    # Non-txn entries and the bare row come before the receipted row
    # in ledger order — the sort must not preserve that.
    entries = [bare, receipted]

    db.execute(
        "INSERT INTO document_links "
        "(paperless_id, txn_hash, match_method) VALUES (?, ?, ?)",
        (101, _compute_hash(receipted), "manual"),
    )
    db.commit()

    sorted_rows = _collect_fixme_txns(entries, conn=db)
    assert sorted_rows[0][0] is receipted
    assert sorted_rows[1][0] is bare


def test_collect_fixme_txns_tolerates_missing_conn():
    """Without a conn, the sort still returns rows (falls back to the
    always-False branches of each signal check). This keeps callers
    that don't pass conn — if any remain — from crashing."""
    bare_a = _mk_fixme_txn(narration="a", txn_date=date(2024, 5, 1))
    bare_b = _mk_fixme_txn(narration="b", txn_date=date(2024, 5, 2))
    out = _collect_fixme_txns([bare_a, bare_b], conn=None)
    assert len(out) == 2
