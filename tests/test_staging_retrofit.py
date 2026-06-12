# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the metadata retrofit writer — NEXTGEN.md Phase E2.

The retrofit pass is the "exit condition that makes the problem
go away forever": when the reboot scan detects historical
duplicates via ``content_fingerprint``, the retrofit writer
stamps ``lamella-source-ref`` metadata onto each member's ledger
line. Future imports from any source dedup on that exact key
instead of falling through to the fuzzy fingerprint path.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    RebootService,
    RetrofitError,
    content_fingerprint,
    retrofit_fingerprint,
)


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


@pytest.fixture(autouse=True)
def stub_bean_check(monkeypatch):
    """Short-circuit the shell-out bean-check so retrofit tests are
    fast and deterministic. Individual tests that want to exercise
    the rollback path override these back."""
    monkeypatch.setattr(
        "lamella.features.import_.staging.retrofit.capture_bean_check",
        lambda _main_bean: (0, ""),
    )
    monkeypatch.setattr(
        "lamella.features.import_.staging.retrofit.run_bean_check_vs_baseline",
        lambda _main_bean, _baseline: None,
    )


def _write_ledger(dir_: Path, body: str) -> Path:
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Expenses:Food USD\n"
        + body,
        encoding="utf-8",
    )
    return main


class TestRetrofit:
    def test_stamps_source_ref_on_matching_ledger_line(
        self, conn, tmp_path: Path,
    ):
        body = (
            '2026-04-20 * "AMAZON.COM"\n'
            "  Assets:Bank    -12.34 USD\n"
            "  Expenses:Food   12.34 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)

        fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("12.34"),
            description="AMAZON.COM",
        )
        result = retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)
        assert result.lines_stamped == 1
        assert result.lines_already_tagged == 0

        text = main.read_text(encoding="utf-8")
        assert 'lamella-source-ref: "' + fp + '"' in text

    def test_retrofit_is_idempotent(self, conn, tmp_path: Path):
        body = (
            '2026-04-20 * "Coffee"\n'
            "  Assets:Bank    -4.50 USD\n"
            "  Expenses:Food   4.50 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)

        fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("4.50"),
            description="Coffee",
        )
        r1 = retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)
        r2 = retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)
        assert r1.lines_stamped == 1
        assert r2.lines_stamped == 0
        assert r2.lines_already_tagged == 1

        # The metadata line must appear exactly once.
        text = main.read_text(encoding="utf-8")
        assert text.count("lamella-source-ref:") == 1

    def test_stamps_all_members_of_a_duplicate_group(
        self, conn, tmp_path: Path,
    ):
        """Two ledger txns with the same fingerprint — both get
        stamped so a future import dedups against either."""
        body = (
            '2026-04-20 * "Target" "SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
            "\n"
            # Double-import of the same real-world txn, written to the
            # ledger a second time.
            '2026-04-20 * "Target" "SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)

        fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("25.99"),
            description="SKU 1234",
        )
        result = retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)
        assert result.lines_stamped == 2

        # Both txns now carry the metadata line.
        text = main.read_text(encoding="utf-8")
        assert text.count("lamella-source-ref:") == 2

    def test_postings_remain_balanced_after_stamp(
        self, conn, tmp_path: Path,
    ):
        """Inserting a metadata line must not break the transaction's
        balance — the postings still follow, just one line further
        down."""
        body = (
            '2026-04-20 * "Target" "A"\n'
            "  Assets:Bank    -10.00 USD\n"
            "  Expenses:Food   10.00 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)
        fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("10.00"),
            description="A",
        )
        retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)

        # Re-load the ledger; the parser should see 1 transaction with 2 postings.
        reader2 = LedgerReader(main)
        from beancount.core.data import Transaction
        txns = [
            e for e in reader2.load(force=True).entries
            if isinstance(e, Transaction)
        ]
        assert len(txns) == 1
        assert len(txns[0].postings) == 2

    def test_no_reboot_rows_match_returns_empty_result(
        self, conn, tmp_path: Path,
    ):
        """Retrofit called with a fingerprint that nothing matches
        is a no-op, not an error."""
        main = _write_ledger(tmp_path, "")
        result = retrofit_fingerprint(
            conn, fingerprint="doesnotmatchanything", main_bean=main,
        )
        assert result.lines_targeted == 0
        assert result.lines_stamped == 0

    def test_bean_check_failure_rolls_back(
        self, conn, tmp_path: Path, monkeypatch,
    ):
        """If bean-check flags new errors after the stamp, every
        touched file is restored and RetrofitError is raised."""
        body = (
            '2026-04-20 * "Coffee"\n'
            "  Assets:Bank    -4.50 USD\n"
            "  Expenses:Food   4.50 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        original_bytes = main.read_bytes()
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)

        # Re-arm the bean-check to fail.
        from lamella.core.ledger_writer import BeanCheckError
        def explode(_main_bean, _baseline):
            raise BeanCheckError("synthetic")
        monkeypatch.setattr(
            "lamella.features.import_.staging.retrofit.run_bean_check_vs_baseline",
            explode,
        )

        fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("4.50"),
            description="Coffee",
        )
        with pytest.raises(RetrofitError, match="bean-check"):
            retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)

        # File was restored to original bytes exactly.
        assert main.read_bytes() == original_bytes

    def test_retrofit_then_rescan_drops_the_group(
        self, conn, tmp_path: Path,
    ):
        """The end-to-end contract: after retrofit, a rescan must NOT
        re-surface the same duplicate group. The group has been
        resolved — the ledger lines now carry lamella-source-ref and the
        staged rows are dismissed. This is the 'resolve once, forever'
        guarantee the user specified."""
        body = (
            '2026-04-20 * "Target" "SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
            "\n"
            '2026-04-20 * "Target" "SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        reader = LedgerReader(main)
        svc = RebootService(conn)

        # First scan: 1 group with 2 members.
        first = svc.scan_ledger(reader)
        assert len(first.duplicate_groups) == 1

        # Retrofit.
        fp = first.duplicate_groups[0].fingerprint
        retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)

        # Re-scan: same group should NOT show. The staged rows are
        # dismissed, the ledger lines carry lamella-source-ref, and the
        # group-detection query excludes dismissed rows.
        second = svc.scan_ledger(reader)
        assert second.duplicate_groups == []

    def test_stamp_aligns_with_phase_d1_fingerprint(
        self, conn, tmp_path: Path,
    ):
        """End-to-end guarantee: the fingerprint stamped onto a
        ledger line matches the fingerprint a future Phase D1.1
        paste would compute for the same real-world transaction.
        This is the dedup-on-exact-key contract."""
        body = (
            '2026-04-20 * "Amazon" "Prime"\n'
            "  Assets:Bank    -99.99 USD\n"
            "  Expenses:Food   99.99 USD\n"
        )
        main = _write_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)
        fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("99.99"),
            description="Prime",
        )
        retrofit_fingerprint(conn, fingerprint=fp, main_bean=main)
        text = main.read_text(encoding="utf-8")
        # The stamped value must be exactly the fingerprint a paste-
        # intake row computes for the same (date, abs-amount, desc).
        assert f'lamella-source-ref: "{fp}"' in text
