# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Data Integrity Check — NEXTGEN.md Phase F."""
from __future__ import annotations

from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    latest_integrity_report,
    run_integrity_check,
)


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _ledger(dir_: Path, body: str = "") -> Path:
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Expenses:Food USD\n"
        + body,
        encoding="utf-8",
    )
    return main


def test_clean_ledger_reports_clean(conn, tmp_path: Path):
    main = _ledger(tmp_path)
    reader = LedgerReader(main)
    report = run_integrity_check(conn, reader)
    assert report.is_clean
    assert "No changes needed" in report.summary()
    assert report.total_ledger_txns == 0
    assert report.duplicate_groups == 0


def test_ledger_with_historical_duplicates_flags_them(conn, tmp_path: Path):
    body = (
        '2026-04-20 * "Target"\n'
        "  Assets:Bank    -25.99 USD\n"
        "  Expenses:Food   25.99 USD\n"
        "\n"
        '2026-04-20 * "Target"\n'
        "  Assets:Bank    -25.99 USD\n"
        "  Expenses:Food   25.99 USD\n"
    )
    main = _ledger(tmp_path, body)
    reader = LedgerReader(main)
    report = run_integrity_check(conn, reader)
    assert not report.is_clean
    assert report.duplicate_groups == 1
    assert report.duplicate_rows == 2
    assert "duplicate group" in report.summary()


def test_check_persists_history_row(conn, tmp_path: Path):
    main = _ledger(tmp_path)
    reader = LedgerReader(main)
    run_integrity_check(conn, reader)
    run_integrity_check(conn, reader)
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM integrity_check_history"
    ).fetchone()["n"]
    assert n == 2


def test_latest_report_returns_most_recent(conn, tmp_path: Path):
    main = _ledger(tmp_path)
    reader = LedgerReader(main)
    run_integrity_check(conn, reader)
    latest = latest_integrity_report(conn)
    assert latest is not None
    assert latest.is_clean


def test_latest_report_none_on_empty_history(conn):
    # Table doesn't exist yet on a fresh DB.
    assert latest_integrity_report(conn) is None
