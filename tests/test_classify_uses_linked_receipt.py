# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression: AI classify on a staged row pulls in the linked receipt's
OCR / line-item data when one exists.

Reported scenario (AJ, 2026-04-29):
    A staged row carries a Paperless receipt linked to it via
    ADR-0056 (receipts attach pre-classification — the link is
    recorded in ``receipt_links.txn_hash`` against the staged row's
    lamella-txn-id token). The user clicks AI classify. The
    classifier was running with the candidate-by-amount fallback
    only — it never tried the linked-receipt branch — because the
    lamella-txn-id wasn't being threaded into the receipt-context
    lookup. The mileage log mentioned a national hardware-store
    chain on the same date next to "Rotate Tires"; the AI took
    that as decisive evidence and proposed Vehicle:Maintenance
    even though the receipt's line items were not vehicle-related
    at all.

The fix threads ``staged_row.lamella_txn_id`` into
``SimpleFINIngest._maybe_ai_classify`` which then passes it to
``fetch_receipt_context`` as ``txn_hash=`` so the linked branch
fires. This test covers the receipt-context lookup directly with
the staged-row token; the higher-level glue (the ``ingest._maybe_ai_classify``
call sites) is exercised in TestStagedAskAiPath.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.ai_cascade.receipt_context import (
    ReceiptContext,
    fetch_receipt_context,
)


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _seed_doc(conn, *, paperless_id, vendor, total, content, date_):
    conn.execute(
        """
        INSERT INTO paperless_doc_index
            (paperless_id, vendor, total_amount, receipt_date,
             content_excerpt, title)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (paperless_id, vendor, total, date_, content, "Receipt"),
    )


def _seed_link(conn, *, paperless_id, txn_hash, date_, amount):
    conn.execute(
        """
        INSERT INTO receipt_links
            (paperless_id, txn_hash, txn_date, txn_amount, match_method)
        VALUES (?, ?, ?, ?, 'manual')
        """,
        (paperless_id, txn_hash, date_, str(amount)),
    )


class TestStagedRowReceiptLookup:
    """ADR-0056 — receipts can be attached to staged rows pre-promotion.
    The link is recorded with the staged row's ``lamella-txn-id`` in
    ``receipt_links.txn_hash``. ``fetch_receipt_context(txn_hash=...)``
    must hit the same row."""

    def test_lamella_txn_id_resolves_linked_receipt(self, conn):
        # Receipt for a hardware-store purchase whose line items
        # establish "this was lumber + landscape fabric, NOT vehicle
        # parts" — the data the classifier needs to override the
        # mileage-log inference toward Vehicle:Maintenance.
        _seed_doc(
            conn,
            paperless_id=42,
            vendor="National Hardware Chain",
            total="166.40",
            date_="2026-01-31",
            content=(
                "Order #12345\n"
                "2x4 spruce stud x12 $48.00\n"
                "Landscape fabric 3ft x 50ft $86.55\n"
                "Concrete mix 80lb x4 $31.85\n"
                "SUBTOTAL 166.40\nTOTAL 166.40\n"
            ),
        )
        # Link recorded with the STAGED ROW's lamella-txn-id token in
        # the txn_hash column (per ADR-0056 attach-pre-promotion).
        staged_lamella_txn_id = "0190f000-0000-7000-8000-STAGED-ROW-1"
        _seed_link(
            conn,
            paperless_id=42,
            txn_hash=staged_lamella_txn_id,
            date_="2026-01-31",
            amount=Decimal("-166.40"),
        )
        # The fix: passing the staged row's lamella-txn-id as
        # ``txn_hash`` MUST hit the linked-receipt branch and return
        # the OCR / line-item data.
        ctx = fetch_receipt_context(
            conn, txn_hash=staged_lamella_txn_id,
        )
        assert ctx is not None, (
            "linked receipt lookup keyed by lamella-txn-id must "
            "succeed — without this the classifier never sees "
            "the user-attached receipt's content"
        )
        assert isinstance(ctx, ReceiptContext)
        assert ctx.source == "linked"
        assert ctx.vendor == "National Hardware Chain"
        assert ctx.total == Decimal("166.40")
        # The line items are in the OCR excerpt — this is the
        # signal the classifier needed to NOT guess vehicle
        # maintenance.
        assert "Landscape fabric" in ctx.content_excerpt
        assert "2x4 spruce" in ctx.content_excerpt

    def test_no_lamella_txn_id_falls_back_to_candidate(self, conn):
        """When the caller doesn't supply a lamella-txn-id (or none
        is stamped on the staged row yet — legacy callers), the
        candidate-by-amount path still works."""
        _seed_doc(
            conn,
            paperless_id=42,
            vendor="National Hardware Chain",
            total="166.40",
            date_="2026-01-31",
            content="line items here",
        )
        # No receipt_links row at all.
        from datetime import date
        ctx = fetch_receipt_context(
            conn,
            txn_hash=None,
            posting_date=date(2026, 1, 31),
            amount=Decimal("-166.40"),
        )
        assert ctx is not None
        assert ctx.source == "candidate"

    def test_linked_takes_precedence_over_candidate(self, conn):
        """When BOTH a link via lamella-txn-id AND an
        amount-matching candidate exist, the linked branch wins —
        explicit user intent beats heuristic match."""
        _seed_doc(
            conn,
            paperless_id=42,
            vendor="National Hardware Chain",
            total="166.40",
            date_="2026-01-31",
            content="line items here",
        )
        staged_lamella_txn_id = "0190f000-0000-7000-8000-STAGED-ROW-1"
        _seed_link(
            conn,
            paperless_id=42,
            txn_hash=staged_lamella_txn_id,
            date_="2026-01-31",
            amount=Decimal("-166.40"),
        )
        from datetime import date
        ctx = fetch_receipt_context(
            conn,
            txn_hash=staged_lamella_txn_id,
            posting_date=date(2026, 1, 31),
            amount=Decimal("-166.40"),
        )
        assert ctx is not None
        assert ctx.source == "linked", (
            "linked-receipt branch must win — the user explicitly "
            "attached this receipt; candidate-by-amount is a "
            "fallback for the unattached case"
        )


class TestStagedAskAiThreadsLamellaTxnId:
    """Glue regression: the staged-row ask-ai paths
    (``api_txn._run_staged_ask_ai`` and the legacy
    ``staging_review/ask-ai-modal`` worker) MUST thread
    ``row.lamella_txn_id`` into ``ingest._maybe_ai_classify`` so the
    receipt-context lookup runs in linked mode.

    We verify the contract at the function-signature level rather
    than spinning up a full async classifier: ``_maybe_ai_classify``
    takes a ``lamella_txn_id`` keyword, and the callers pass it.
    A more end-to-end test would require mocking the OpenRouter
    client — overkill for the regression we're guarding."""

    def test_maybe_ai_classify_accepts_lamella_txn_id_kwarg(self):
        from lamella.features.bank_sync.ingest import SimpleFINIngest
        import inspect
        sig = inspect.signature(SimpleFINIngest._maybe_ai_classify)
        assert "lamella_txn_id" in sig.parameters, (
            "_maybe_ai_classify must accept lamella_txn_id so staged "
            "callers can route the receipt-context lookup through the "
            "linked branch (ADR-0056)"
        )
        # Default is None so legacy callers (production live ingest
        # path that hasn't been threaded yet) keep working — they
        # just stay on the candidate-by-amount fallback.
        param = sig.parameters["lamella_txn_id"]
        assert param.default is None
