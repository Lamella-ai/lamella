# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Paperless receipt context in classification.

The motivating case: a Hardware Store charge on its own is ambiguous
(supplies vs COGS vs office expense vs personal). The linked (or
candidate) receipt's OCR line items disambiguate — lumber +
concrete mix points at a different account than printer paper +
ink cartridges, even at the same merchant. The AI sees the
excerpt in the prompt and classifies accordingly.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.features.ai_cascade.receipt_context import (
    DEFAULT_EXCERPT_CHARS,
    ReceiptContext,
    fetch_receipt_context,
)
from lamella.core.db import connect, migrate


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _seed_paperless_doc(
    conn, *,
    paperless_id: int = 1,
    vendor: str = "Hardware Store",
    total: str = "142.35",
    receipt_date: str = "2026-04-17",
    content: str = (
        "Hardware Store #4521\nLumber: 2x4 spruce x6 $34.80\n"
        "Concrete mix 80lb bag x3 $21.00\n"
        "Landscape fabric 3ft x 50ft $86.55\n"
        "SUBTOTAL 142.35\nTOTAL 142.35\n"
    ),
):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, vendor, total_amount, receipt_date,
             content_excerpt, title)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (paperless_id, vendor, total, receipt_date, content, "Receipt"),
    )


def _seed_link(conn, *, paperless_id: int, txn_hash: str):
    conn.execute(
        """
        INSERT INTO receipt_links
            (paperless_id, txn_hash, txn_date, txn_amount, match_method)
        VALUES (?, ?, ?, ?, 'manual')
        """,
        (paperless_id, txn_hash, "2026-04-18", 142.35),
    )


# --- linked-receipt path ------------------------------------------


class TestLinked:
    def test_linked_receipt_returns_context(self, conn):
        _seed_paperless_doc(conn, paperless_id=1)
        _seed_link(conn, paperless_id=1, txn_hash="abc-txn")
        ctx = fetch_receipt_context(conn, txn_hash="abc-txn")
        assert ctx is not None
        assert isinstance(ctx, ReceiptContext)
        assert ctx.vendor == "Hardware Store"
        assert ctx.total == Decimal("142.35")
        assert ctx.source == "linked"
        assert "Lumber" in ctx.content_excerpt
        assert "Concrete" in ctx.content_excerpt

    def test_unlinked_txn_hash_returns_none_linked(self, conn):
        _seed_paperless_doc(conn, paperless_id=1)
        ctx = fetch_receipt_context(conn, txn_hash="unknown-hash")
        assert ctx is None

    def test_content_excerpt_truncated_to_max_chars(self, conn):
        long_content = "X" * 3000
        _seed_paperless_doc(conn, paperless_id=1, content=long_content)
        _seed_link(conn, paperless_id=1, txn_hash="abc")
        ctx = fetch_receipt_context(
            conn, txn_hash="abc", max_chars=500,
        )
        assert ctx is not None
        assert len(ctx.content_excerpt) <= 501  # 500 + trailing ellipsis
        assert ctx.content_excerpt.endswith("…")


# --- candidate-by-amount+date path -------------------------------


class TestCandidate:
    def test_single_match_by_amount_and_date(self, conn):
        """Hardware Store receipt uploaded to Paperless yesterday,
        matching txn posted today with the same amount. No link
        yet, but the candidate match is unambiguous — return it
        as context so the AI can use the line items."""
        _seed_paperless_doc(
            conn, paperless_id=1,
            total="142.35", receipt_date="2026-04-17",
        )
        ctx = fetch_receipt_context(
            conn,
            posting_date=date(2026, 4, 18),
            amount=Decimal("-142.35"),
        )
        assert ctx is not None
        assert ctx.source == "candidate"
        assert ctx.vendor == "Hardware Store"

    def test_date_outside_tolerance_no_match(self, conn):
        _seed_paperless_doc(
            conn, paperless_id=1,
            total="142.35", receipt_date="2026-04-01",
        )
        ctx = fetch_receipt_context(
            conn,
            posting_date=date(2026, 4, 18),
            amount=Decimal("-142.35"),
            tolerance_days=3,
        )
        assert ctx is None

    def test_amount_mismatch_no_match(self, conn):
        _seed_paperless_doc(
            conn, paperless_id=1,
            total="142.35", receipt_date="2026-04-17",
        )
        ctx = fetch_receipt_context(
            conn,
            posting_date=date(2026, 4, 18),
            amount=Decimal("-99.00"),
        )
        assert ctx is None

    def test_multiple_candidates_return_none(self, conn):
        """Ambiguous match shouldn't bias — if two receipts could
        be the one, the AI is better off without receipt context
        than with the wrong one."""
        _seed_paperless_doc(
            conn, paperless_id=1,
            total="142.35", receipt_date="2026-04-16",
            content="Receipt A",
        )
        _seed_paperless_doc(
            conn, paperless_id=2, vendor="Home Center",
            total="142.35", receipt_date="2026-04-17",
            content="Receipt B",
        )
        ctx = fetch_receipt_context(
            conn,
            posting_date=date(2026, 4, 18),
            amount=Decimal("-142.35"),
            tolerance_days=3,
        )
        assert ctx is None

    def test_linked_wins_over_candidate(self, conn):
        """When a txn has both a link AND a matching candidate,
        the linked one is returned (explicit > inferred)."""
        _seed_paperless_doc(
            conn, paperless_id=1, vendor="LinkedDoc",
            total="142.35", receipt_date="2026-04-17",
        )
        _seed_paperless_doc(
            conn, paperless_id=2, vendor="CandidateDoc",
            total="142.35", receipt_date="2026-04-18",
        )
        _seed_link(conn, paperless_id=1, txn_hash="abc")
        # Both the link and the candidate match amount+date, but
        # the linked one should win.
        ctx = fetch_receipt_context(
            conn,
            txn_hash="abc",
            posting_date=date(2026, 4, 18),
            amount=Decimal("-142.35"),
        )
        assert ctx is not None
        assert ctx.source == "linked"
        assert ctx.vendor == "LinkedDoc"


# --- graceful degradation ---------------------------------------


class TestDegradation:
    def test_empty_paperless_index_returns_none(self, conn):
        ctx = fetch_receipt_context(
            conn, txn_hash="anything",
            posting_date=date(2026, 4, 20), amount=Decimal("10"),
        )
        assert ctx is None

    def test_no_inputs_returns_none(self, conn):
        _seed_paperless_doc(conn)
        ctx = fetch_receipt_context(conn)
        assert ctx is None


# --- classify wire ---------------------------------------------


class TestClassifyWire:
    def test_build_classify_context_returns_8_tuple_with_receipt(self, conn):
        """Confirms receipt becomes the 8th element of
        build_classify_context's return tuple and populates when a
        candidate exists."""
        from beancount.core import data as bdata
        from beancount.core.amount import Amount
        from beancount.core.number import D

        _seed_paperless_doc(
            conn, paperless_id=1,
            total="42.50", receipt_date="2026-04-17",
        )
        posting_card = bdata.Posting(
            account="Liabilities:Acme:Card:0123",
            units=Amount(D("-42.50"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )
        posting_fixme = bdata.Posting(
            account="Expenses:Acme:FIXME",
            units=Amount(D("42.50"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )
        txn = bdata.Transaction(
            meta={"filename": "x", "lineno": 1},
            date=date(2026, 4, 18),
            flag="*",
            payee="Hardware Store",
            narration="purchase",
            tags=frozenset(),
            links=frozenset(),
            postings=[posting_card, posting_fixme],
        )

        from lamella.features.ai_cascade.classify import build_classify_context
        result = build_classify_context(
            entries=[], txn=txn, conn=conn,
        )
        # 10-tuple as of the per-vehicle log-density addition.
        assert len(result) == 10
        receipt = result[7]
        assert receipt is not None
        assert receipt.source == "candidate"
        assert receipt.vendor == "Hardware Store"
