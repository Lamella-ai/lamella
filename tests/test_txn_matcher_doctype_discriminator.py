# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0061 Phase 3: txn_matcher document_type discriminator.

Verifies that ``find_document_candidates`` excludes statement and
tax documents via the canonical ``document_type`` column when
present, and falls back to the legacy regex over
``document_type_name`` when the column is absent or NULL.

The Phase-2 column (Worker A's migration 067) may not have landed
yet; this test detects that and skips the column-aware
assertions, leaving the regex-fallback assertions running so the
matcher's correctness is still verified during the rolling
migration window.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.receipts.txn_matcher import (
    EXCLUDED_DOCUMENT_TYPES,
    _doctype_excluded,
    _has_document_type_column,
    find_document_candidates,
)


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


def _add_document_type_column_if_missing(conn) -> bool:
    """Best-effort: add the ``document_type`` column for tests that
    need it when Phase-2's migration 067 hasn't landed yet. Returns
    True when the column is now present (either it was already
    there or we just added it), False if we couldn't add it.
    """
    if _has_document_type_column(conn):
        return True
    try:
        conn.execute(
            "ALTER TABLE paperless_doc_index ADD COLUMN document_type TEXT"
        )
        return True
    except Exception:  # noqa: BLE001
        return False


def _has_document_date_column(conn) -> bool:
    """Detect whether the Phase-2 column rename
    ``receipt_date → document_date`` has landed."""
    rows = conn.execute(
        "PRAGMA table_info(paperless_doc_index)"
    ).fetchall()
    for row in rows:
        try:
            name = row["name"]
        except (IndexError, KeyError):
            name = row[1]
        if name == "document_date":
            return True
    return False


def _seed_doc(
    conn,
    *,
    paperless_id: int,
    title: str,
    total: str,
    receipt_date_iso: str = "2026-04-17",
    document_type_name: str | None = None,
    document_type: str | None = None,
    correspondent_name: str | None = None,
    content_excerpt: str = "",
):
    """Seed a row into paperless_doc_index. ``document_type`` is the
    Phase-2 canonical column; only inserted when the column exists
    on the connected DB. Likewise ``document_date`` vs the legacy
    ``receipt_date`` column name is detected at runtime so this
    helper works with both pre-Phase-2 and post-Phase-2 schemas."""
    has_dt_col = _has_document_type_column(conn)
    use_document_date = _has_document_date_column(conn)
    date_col = "document_date" if use_document_date else "receipt_date"
    cols = [
        "paperless_id", "title", "total_amount", date_col,
        "document_type_name", "correspondent_name", "content_excerpt",
    ]
    vals = [
        paperless_id, title, total, receipt_date_iso,
        document_type_name, correspondent_name, content_excerpt,
    ]
    if has_dt_col:
        cols.append("document_type")
        vals.append(document_type)
    placeholders = ", ".join(["?"] * len(cols))
    conn.execute(
        f"INSERT INTO paperless_doc_index ({', '.join(cols)}) "
        f"VALUES ({placeholders})",
        vals,
    )


# ------------------------------------------------------------------
# Pure-helper tests (work whether or not Phase 2 column exists)
# ------------------------------------------------------------------


class TestDoctypeExcludedHelper:
    def test_excluded_document_types_set(self):
        """Canonical exclusion set is statement + tax (ADR-0061 §4)."""
        assert EXCLUDED_DOCUMENT_TYPES == frozenset({"statement", "tax"})

    def test_canonical_statement_excluded(self, db):
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name=None,
            document_type="statement",
        ) is True

    def test_canonical_tax_excluded(self, db):
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name=None,
            document_type="tax",
        ) is True

    def test_canonical_receipt_included(self, db):
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name=None,
            document_type="receipt",
        ) is False

    def test_canonical_invoice_included(self, db):
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name=None,
            document_type="invoice",
        ) is False

    def test_canonical_order_included(self, db):
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name=None,
            document_type="order",
        ) is False

    def test_canonical_other_included(self, db):
        """`other` is the catch-all default per ADR-0061 §4 — not
        excluded; the user sees it as a candidate and decides."""
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name=None,
            document_type="other",
        ) is False

    def test_null_canonical_falls_through_to_regex_excluded(self, db):
        """A NULL document_type with a name matching the legacy
        regex is still excluded — pre-Phase-2 deploys keep working."""
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name="Bank Statement",
            document_type=None,
        ) is True

    def test_null_canonical_falls_through_to_regex_included(self, db):
        """A NULL document_type with a name that doesn't match the
        regex is included — typical receipt during the rolling
        migration window."""
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name="Receipt",
            document_type=None,
        ) is False

    def test_canonical_takes_precedence_over_name(self, db):
        """When the canonical document_type is set the legacy
        regex over document_type_name is ignored entirely. A row
        whose name says "Statement" but whose canonical type is
        "receipt" should be INCLUDED — the user has explicitly
        classified it."""
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name="Bank Statement",
            document_type="receipt",
        ) is False

    def test_canonical_excluded_overrides_friendly_name(self, db):
        """A canonical exclusion still wins over a friendly name
        the regex would have allowed."""
        assert _doctype_excluded(
            db,
            document_type_id=None,
            document_type_name="Receipt",
            document_type="statement",
        ) is True


# ------------------------------------------------------------------
# Integration: find_document_candidates with mixed rows
# ------------------------------------------------------------------


class TestFindPaperlessCandidatesDiscriminator:
    def test_legacy_regex_path_excludes_statements(self, db):
        """With NULL document_type (or no column), the legacy regex
        on document_type_name still excludes bank statements. This
        is the pre-Phase-2 behavior the matcher must preserve."""
        # Receipt-shaped doc that matches our query.
        _seed_doc(
            db,
            paperless_id=1,
            title="Hardware Store Receipt",
            total="42.00",
            document_type_name="Receipt",
            correspondent_name="Example Hardware",
        )
        # Bank statement that ALSO contains the same exact total
        # (typical false-positive) — must be excluded by the regex
        # fallback.
        _seed_doc(
            db,
            paperless_id=2,
            title="April Statement",
            total="42.00",
            document_type_name="Bank Statement",
            correspondent_name="Example Bank",
        )
        cands = find_document_candidates(
            db,
            txn_amount=Decimal("42.00"),
            txn_date=date(2026, 4, 17),
            min_score=0.0,
        )
        ids = {c.paperless_id for c in cands}
        assert 1 in ids, "receipt candidate must be returned"
        assert 2 not in ids, "bank statement must be excluded by regex fallback"

    def test_canonical_discriminator_excludes_statement(self, db):
        """When the Phase-2 column is present, a canonical
        ``document_type='statement'`` value excludes the doc — even
        if its document_type_name doesn't match the legacy regex
        (e.g., user named the type "Periodic Summary")."""
        if not _add_document_type_column_if_missing(db):
            pytest.skip(
                "depends on Phase 2 migration: paperless_doc_index."
                "document_type column unavailable"
            )
        # Doc whose friendly name doesn't trip the regex but whose
        # canonical type is statement — only the discriminator can
        # catch this.
        _seed_doc(
            db,
            paperless_id=10,
            title="Periodic Summary",
            total="100.00",
            document_type_name="Periodic Summary",  # not in regex
            document_type="statement",              # canonical excludes
        )
        # Plain receipt for comparison.
        _seed_doc(
            db,
            paperless_id=11,
            title="Coffee Shop",
            total="100.00",
            document_type_name="Receipt",
            document_type="receipt",
        )
        cands = find_document_candidates(
            db,
            txn_amount=Decimal("100.00"),
            txn_date=date(2026, 4, 17),
            min_score=0.0,
        )
        ids = {c.paperless_id for c in cands}
        assert 11 in ids, "receipt candidate must be returned"
        assert 10 not in ids, (
            "statement must be excluded by canonical document_type "
            "discriminator even when document_type_name doesn't "
            "match the legacy regex"
        )

    def test_canonical_tax_excluded_via_discriminator(self, db):
        if not _add_document_type_column_if_missing(db):
            pytest.skip(
                "depends on Phase 2 migration: paperless_doc_index."
                "document_type column unavailable"
            )
        _seed_doc(
            db,
            paperless_id=20,
            title="Annual Form",
            total="500.00",
            document_type_name="Annual Form",  # not in regex
            document_type="tax",
        )
        _seed_doc(
            db,
            paperless_id=21,
            title="Vendor Receipt",
            total="500.00",
            document_type_name="Receipt",
            document_type="receipt",
        )
        cands = find_document_candidates(
            db,
            txn_amount=Decimal("500.00"),
            txn_date=date(2026, 4, 17),
            min_score=0.0,
        )
        ids = {c.paperless_id for c in cands}
        assert 21 in ids
        assert 20 not in ids, "tax document must be excluded via discriminator"

    def test_null_canonical_with_receipt_name_still_included(self, db):
        """A row with NULL document_type and a friendly name that
        doesn't match the regex stays as a candidate. Ensures the
        rolling migration window doesn't accidentally drop real
        receipts whose Paperless type-id hasn't been mapped yet."""
        if not _add_document_type_column_if_missing(db):
            pytest.skip(
                "depends on Phase 2 migration: paperless_doc_index."
                "document_type column unavailable"
            )
        _seed_doc(
            db,
            paperless_id=30,
            title="Unmapped Receipt",
            total="25.00",
            document_type_name="Receipt",
            document_type=None,  # explicit NULL
        )
        cands = find_document_candidates(
            db,
            txn_amount=Decimal("25.00"),
            txn_date=date(2026, 4, 17),
            min_score=0.0,
        )
        ids = {c.paperless_id for c in cands}
        assert 30 in ids, (
            "NULL canonical document_type must fall through to the "
            "legacy regex; a friendly name of 'Receipt' is included"
        )

    def test_null_canonical_with_statement_name_still_excluded(self, db):
        """Mixed rolling state: some rows have canonical, some
        don't. NULL + statement-name keeps the regex behavior so
        bank statements stay excluded across the migration."""
        if not _add_document_type_column_if_missing(db):
            pytest.skip(
                "depends on Phase 2 migration: paperless_doc_index."
                "document_type column unavailable"
            )
        _seed_doc(
            db,
            paperless_id=40,
            title="Brokerage Statement",
            total="9.99",
            document_type_name="Brokerage Statement",
            document_type=None,
        )
        _seed_doc(
            db,
            paperless_id=41,
            title="Coffee Shop",
            total="9.99",
            document_type_name="Receipt",
            document_type="receipt",
        )
        cands = find_document_candidates(
            db,
            txn_amount=Decimal("9.99"),
            txn_date=date(2026, 4, 17),
            min_score=0.0,
        )
        ids = {c.paperless_id for c in cands}
        assert 41 in ids
        assert 40 not in ids, (
            "NULL canonical with statement-matching name must fall "
            "through to the regex and stay excluded"
        )
