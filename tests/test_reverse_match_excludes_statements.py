# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0063 §6: documents with document_type IN ('statement', 'tax')
must NOT be auto-linked in either direction.

Bank statements contain exact-amount line items for many real
receipts; auto-linking one as the receipt for a single transaction
is a false positive the user has to clean up by hand. Same for
tax forms — they reference dollar amounts but their semantics are
not a single transaction.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.receipts.auto_match import (
    AutoLinkReport,
    auto_link_unlinked_documents,
)


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


def _seed_doc(conn, *, paperless_id: int, document_type: str, total: str = "42.00"):
    """Seed a paperless_doc_index row with the given canonical
    document_type. document_date populated so the row passes the
    auto-link gate's date-presence check."""
    conn.execute(
        "INSERT INTO paperless_doc_index "
        "(paperless_id, title, total_amount, document_date, document_type, "
        " document_type_name, correspondent_name, content_excerpt) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            paperless_id,
            f"doc {paperless_id}",
            total,
            "2026-04-17",
            document_type,
            document_type.title(),
            "Bank",
            "",
        ),
    )


class _StubReader:
    """Minimal reader stand-in that hands back a fixed entries list."""

    def __init__(self, entries):
        self._entries = entries

    def load(self):
        class _R:
            entries = self._entries

        # bind to instance: closure on self
        r = _R()
        r.entries = self._entries
        return r


def test_statement_doc_is_skipped_excluded(db, tmp_path):
    """A statement-typed document never triggers a reverse-direction
    candidate search; it lands in skipped_excluded."""
    _seed_doc(db, paperless_id=100, document_type="statement")
    # Empty ledger — the only thing being tested is the exclusion.
    reader = _StubReader([])
    # Build a settings stub with the bare minimum the function reads.
    from lamella.core.config import Settings
    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "ledger",
    )
    (tmp_path / "ledger").mkdir()
    (tmp_path / "ledger" / "main.bean").write_text("")

    report = auto_link_unlinked_documents(
        db, reader=reader, settings=settings, dry_run=True,
    )
    assert isinstance(report, AutoLinkReport)
    assert report.scanned == 1
    assert report.skipped_excluded == 1
    assert report.linked == 0
    assert report.queued_for_review == 0


def test_tax_doc_is_skipped_excluded(db, tmp_path):
    """Same exclusion applies to tax-form documents."""
    _seed_doc(db, paperless_id=200, document_type="tax")
    reader = _StubReader([])
    from lamella.core.config import Settings
    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "ledger",
    )
    (tmp_path / "ledger").mkdir()
    (tmp_path / "ledger" / "main.bean").write_text("")

    report = auto_link_unlinked_documents(
        db, reader=reader, settings=settings, dry_run=True,
    )
    assert report.scanned == 1
    assert report.skipped_excluded == 1


def test_receipt_doc_is_not_excluded(db, tmp_path):
    """A receipt-typed document does NOT land in skipped_excluded —
    it passes through to the candidate search (which finds nothing
    in our empty ledger, so it ends up in skipped_no_candidate)."""
    _seed_doc(db, paperless_id=300, document_type="receipt")
    reader = _StubReader([])
    from lamella.core.config import Settings
    settings = Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "ledger",
    )
    (tmp_path / "ledger").mkdir()
    (tmp_path / "ledger" / "main.bean").write_text("")

    report = auto_link_unlinked_documents(
        db, reader=reader, settings=settings, dry_run=True,
    )
    assert report.scanned == 1
    assert report.skipped_excluded == 0
    # No txns in the empty ledger, so this falls into no-candidate.
    assert report.skipped_no_candidate == 1


def test_find_ledger_candidates_short_circuits_on_excluded_doctype(db):
    """The matcher-level entry point ALSO refuses to score a
    statement / tax document — defense in depth ensures direct
    callers can't bypass the auto-link gate."""
    from datetime import date
    from decimal import Decimal

    from lamella.features.receipts.txn_matcher import find_ledger_candidates

    cands = find_ledger_candidates(
        db,
        doc_date=date(2026, 4, 17),
        doc_total=Decimal("100.00"),
        doc_currency="USD",
        doc_vendor="Bank",
        doc_doctype="statement",
        doc_id=999,
        ledger_entries=[],
    )
    assert cands == []
