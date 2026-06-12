# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Reboot rows reference an existing ledger entry — they must stage
under the entry's existing ``lamella-txn-id`` so /txn/{id} resolves
to the same URL pre-promotion (staged detail) and post-promotion
(ledger entry, after the in-place rewrite or override write).

v3 guarantees every entry has lineage on disk; the reboot path must
preserve it through the staging surface."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging import StagingService
from lamella.features.import_.staging.reboot import RebootService


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _make_ledger_with_lineage(tmp_path: Path) -> Path:
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 custom "lamella-ledger-version" "3"\n'
        '\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Groceries USD\n'
        '2020-01-01 open Expenses:FIXME USD\n'
        '\n'
        '2026-04-15 * "Acme Co." "groceries"\n'
        '  lamella-txn-id: "01900000-0000-7000-8000-cafef00d0001"\n'
        '  Assets:Bank             -42.17 USD\n'
        '  Expenses:FIXME           42.17 USD\n',
        encoding="utf-8",
    )
    return main


def test_reboot_stage_carries_existing_lineage(tmp_path: Path, conn):
    main = _make_ledger_with_lineage(tmp_path)
    reader = LedgerReader(main)
    rb = RebootService(conn=conn)

    rb.scan_ledger(reader, session_id="reboot-test")

    # The staged row's lamella_txn_id must equal the on-disk entry's
    # lamella-txn-id, NOT a freshly minted UUID.
    rows = conn.execute(
        "SELECT lamella_txn_id, source FROM staged_transactions "
        "WHERE source = 'reboot'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["lamella_txn_id"] == "01900000-0000-7000-8000-cafef00d0001"


def test_reboot_idempotent_id_preservation(tmp_path: Path, conn):
    """A second reboot scan over the same ledger must not change the
    staged row's lamella_txn_id."""
    main = _make_ledger_with_lineage(tmp_path)
    reader = LedgerReader(main)
    rb = RebootService(conn=conn)

    rb.scan_ledger(reader, session_id="first")
    first = conn.execute(
        "SELECT lamella_txn_id FROM staged_transactions WHERE source='reboot'"
    ).fetchone()["lamella_txn_id"]

    rb.scan_ledger(reader, session_id="second")
    second = conn.execute(
        "SELECT lamella_txn_id FROM staged_transactions WHERE source='reboot'"
    ).fetchone()["lamella_txn_id"]

    assert first == second
    assert first == "01900000-0000-7000-8000-cafef00d0001"
