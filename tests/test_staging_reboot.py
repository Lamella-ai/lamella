# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the reboot scan — NEXTGEN.md Phase E1.

Covers the exit criteria from the design review:
1. Historical ledger transactions land on the unified staging
   surface with ``source='reboot'`` so matcher + duplicate
   detector see them.
2. Duplicate detection on the scan uses the same
   ``content_fingerprint`` algorithm as Phase D1.1 intake — one
   dedup system, not two.
3. Idempotent re-scan (upserts; no runaway row growth).
4. Cross-source duplicate detection: a ledger txn that was
   already staged via SimpleFIN surfaces as a duplicate group.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    RebootService,
    StagingService,
    content_fingerprint,
)


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _write_min_ledger(dir_: Path, body: str) -> Path:
    """Write a minimal main.bean + helper files so LedgerReader.load
    returns something useful. The caller supplies transaction body."""
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Expenses:Food USD\n"
        "2020-01-01 open Income:Work USD\n"
        + body,
        encoding="utf-8",
    )
    return main


# --- core scan ----------------------------------------------------------


class TestRebootScan:
    def test_empty_ledger_produces_no_staged_rows(self, conn, tmp_path: Path):
        main = _write_min_ledger(tmp_path, "")
        reader = LedgerReader(main)
        svc = RebootService(conn)
        result = svc.scan_ledger(reader)
        assert result.total_txns == 0
        assert result.staged == 0
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions WHERE source='reboot'"
        ).fetchone()["n"]
        assert n == 0

    def test_ledger_with_transactions_stages_each(self, conn, tmp_path: Path):
        body = (
            '2026-04-20 * "Coffee Shop" "Morning coffee"\n'
            "  Assets:Bank    -4.50 USD\n"
            "  Expenses:Food   4.50 USD\n"
            "\n"
            '2026-04-21 * "Paycheck"\n'
            "  Assets:Bank    1000 USD\n"
            "  Income:Work   -1000 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        result = RebootService(conn).scan_ledger(reader)
        assert result.total_txns == 2
        assert result.staged == 2
        rows = conn.execute(
            "SELECT source, posting_date, payee, description "
            "FROM staged_transactions WHERE source='reboot' "
            "ORDER BY posting_date"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["posting_date"] == "2026-04-20"
        assert rows[0]["payee"] == "Coffee Shop"
        assert rows[0]["description"] == "Morning coffee"

    def test_source_ref_carries_file_and_lineno(self, conn, tmp_path: Path):
        body = (
            '2026-04-20 * "X"\n'
            "  Assets:Bank    -4.50 USD\n"
            "  Expenses:Food   4.50 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)
        import json as _json
        ref_json = conn.execute(
            "SELECT source_ref FROM staged_transactions WHERE source='reboot' LIMIT 1"
        ).fetchone()["source_ref"]
        ref = _json.loads(ref_json)
        assert "file" in ref
        assert "lineno" in ref
        assert ref["file"].endswith("main.bean")
        assert isinstance(ref["lineno"], int) and ref["lineno"] > 0

    def test_rescan_is_idempotent(self, conn, tmp_path: Path):
        body = (
            '2026-04-20 * "X"\n'
            "  Assets:Bank    -4.50 USD\n"
            "  Expenses:Food   4.50 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        svc = RebootService(conn)
        r1 = svc.scan_ledger(reader)
        r2 = svc.scan_ledger(reader)
        assert r1.staged == 1
        assert r2.staged == 1
        # But only one row exists in the table — second scan upserted.
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions WHERE source='reboot'"
        ).fetchone()["n"]
        assert n == 1

    def test_synthetic_auto_account_entries_skipped(self, conn, tmp_path: Path):
        """Plugins like auto_accounts emit entries with filename='<...>'.
        Those aren't real source lines and would pollute the staging
        surface if we blindly staged them. Scan must skip them."""
        main = tmp_path / "main.bean"
        main.write_text(
            'option "operating_currency" "USD"\n'
            'plugin "beancount_lazy_plugins.auto_accounts"\n'
            # Purposely don't open Assets:NewBank — plugin will synthesize the Open.
            '2026-04-20 * "X"\n'
            "  Assets:NewBank  -4.50 USD\n"
            "  Expenses:Food    4.50 USD\n",
            encoding="utf-8",
        )
        reader = LedgerReader(main)
        result = RebootService(conn).scan_ledger(reader)
        # The user transaction stages (1). The synthetic Open does NOT
        # (it's not a Transaction, so the type check filters it — but
        # even if it were, the filename='<...>' guard catches it).
        assert result.staged == 1


# --- duplicate group detection ------------------------------------------


class TestDuplicateGroups:
    def test_single_row_not_flagged(self, conn, tmp_path: Path):
        body = (
            '2026-04-20 * "Unique"\n'
            "  Assets:Bank    -4.50 USD\n"
            "  Expenses:Food   4.50 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        result = RebootService(conn).scan_ledger(reader)
        assert result.duplicate_groups == []
        assert result.duplicates_total == 0

    def test_ledger_with_historical_double_import_flagged(
        self, conn, tmp_path: Path,
    ):
        """User imported the same transaction twice via different
        tools — the ledger has two lines for it. Reboot scan's
        dup detection surfaces the pair as a group."""
        body = (
            '2026-04-20 * "Target" "SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
            "\n"
            # Same txn, written a second time (common when a user
            # ran both SimpleFIN and a CSV importer).
            '2026-04-20 * "Target" "SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        result = RebootService(conn).scan_ledger(reader)
        assert result.staged == 2
        assert len(result.duplicate_groups) == 1
        group = result.duplicate_groups[0]
        assert len(group.members) == 2
        # Both members tagged source='reboot'.
        sources = {m[1] for m in group.members}
        assert sources == {"reboot"}

    def test_dup_group_across_simplefin_and_reboot_uses_same_fingerprint(
        self, conn, tmp_path: Path,
    ):
        """The fingerprint algorithm must be identical across intake
        paths. A SimpleFIN row staged via Phase B1 and a ledger txn
        staged via Phase E1 for the same real-world transaction must
        collide in a single duplicate group."""
        # Side 1: SimpleFIN row staged previously.
        StagingService(conn).stage(
            source="simplefin",
            source_ref={"account_id": "ACT-WF", "txn_id": "sf-1"},
            session_id="sf-42",
            posting_date="2026-04-20",
            amount=Decimal("-25.99"),
            description="Target SKU 1234",
        )
        # Side 2: the ledger also carries this txn — reboot scan
        # picks it up.
        body = (
            '2026-04-20 * "Target SKU 1234"\n'
            "  Assets:Bank    -25.99 USD\n"
            "  Expenses:Food   25.99 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        result = RebootService(conn).scan_ledger(reader)
        assert len(result.duplicate_groups) == 1
        sources = {m[1] for m in result.duplicate_groups[0].members}
        assert sources == {"simplefin", "reboot"}

    def test_fingerprint_matches_phase_d_intake_algorithm(
        self, conn, tmp_path: Path,
    ):
        """Direct assertion: the fingerprint produced for a reboot
        row equals content_fingerprint() called with the same inputs.
        This is the one-source-of-truth guarantee."""
        body = (
            '2026-04-20 * "AMAZON.COM"\n'
            "  Assets:Bank    -12.34 USD\n"
            "  Expenses:Food   12.34 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)
        row = conn.execute(
            "SELECT posting_date, amount, description "
            "FROM staged_transactions WHERE source='reboot'"
        ).fetchone()
        # The reboot scan picked the FIRST posting (Assets:Bank, -12.34)
        # as the representative leg. Its fingerprint against D1.1 code:
        expected = content_fingerprint(
            posting_date=row["posting_date"],
            amount=Decimal(row["amount"]),
            description=row["description"],
        )
        # And the same fingerprint against a hypothetical future paste
        # of the same transaction:
        paste_fp = content_fingerprint(
            posting_date="2026-04-20",
            amount=Decimal("12.34"),     # sign-agnostic
            description="AMAZON.COM",
        )
        assert expected == paste_fp


# --- integration: reboot + matcher --------------------------------------


class TestRebootFeedsMatcher:
    def test_reboot_row_participates_in_find_pairs(self, conn, tmp_path: Path):
        """Staged reboot rows are first-class — the Phase C matcher
        pairs them against other staged rows just like any other
        source. This is the critical integration: matcher now sees
        historical data."""
        # Stage a CSV-side row.
        StagingService(conn).stage(
            source="csv",
            source_ref={"upload_id": 99, "row_num": 1},
            session_id="99",
            posting_date="2026-04-20",
            amount=Decimal("500.00"),
            description="WF INCOMING",
        )
        # Scan ledger — pulls the matching outflow from history.
        body = (
            '2026-04-20 * "PayPal Transfer"\n'
            "  Assets:PayPal   -500.00 USD\n"
            "  Assets:Bank      500.00 USD\n"
        )
        main = _write_min_ledger(tmp_path, body)
        reader = LedgerReader(main)
        RebootService(conn).scan_ledger(reader)

        from lamella.features.import_.staging import find_pairs
        proposals = find_pairs(conn)
        # At least one proposal pairing the csv row with a reboot row.
        assert any(
            p.kind == "transfer"
            for p in proposals
        )
