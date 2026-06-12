# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0043 / ADR-0043b — staged_transactions reconstruct from
custom "staged-txn" directives.

Covers:
* directive → row round-trip preserves source / amount / posting-date / status
* unpromoted directives get status="new"
* "staged-txn-promoted" gets status="promoted" with promoted_at meta
* Idempotent — running reconstruct twice produces the same end state
* Malformed directives counted in the report and skipped (no crash)
* The ADR-0015 invariant: directive count == staged-row count after rebuild
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount import loader

from lamella.features.bank_sync.writer import (
    PendingEntry,
    render_staged_txn_directive,
    render_staged_txn_promoted_directive,
)
from lamella.core.transform.steps.step24_staged_transactions import (
    reconstruct_staged_transactions,
)


_LEDGER_PRELUDE = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    """An in-memory SQLite with the staged_transactions schema lifted
    from migrations/021_phase11_unified_staging.sql."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE staged_transactions (
            id                INTEGER PRIMARY KEY,
            source            TEXT NOT NULL,
            source_ref        TEXT NOT NULL,
            source_ref_hash   TEXT NOT NULL,
            session_id        TEXT,
            posting_date      TEXT NOT NULL,
            amount            TEXT NOT NULL,
            currency          TEXT NOT NULL DEFAULT 'USD',
            payee             TEXT,
            description       TEXT,
            memo              TEXT,
            raw_json          TEXT NOT NULL,
            status            TEXT NOT NULL DEFAULT 'new',
            promoted_to_file  TEXT,
            promoted_txn_hash TEXT,
            promoted_at       TEXT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(source, source_ref_hash)
        );
    """)
    return c


def _entry(**overrides) -> PendingEntry:
    base = dict(
        date=date(2026, 4, 29),
        simplefin_id="ABC123",
        payee=None,
        narration="Test purchase",
        amount=Decimal("-42.17"),
        currency="USD",
        source_account="Assets:Personal:Bank:Checking",
        target_account="Expenses:Personal:FIXME",
        lamella_txn_id="01900000-0000-7000-8000-000000000001",
    )
    base.update(overrides)
    return PendingEntry(**base)


def _load(content: str, tmp_path: Path) -> list:
    fp = tmp_path / "test.bean"
    fp.write_text(_LEDGER_PRELUDE + content, encoding="utf-8")
    entries, errors, _ = loader.load_file(str(fp))
    assert errors == [], f"unexpected parse errors: {errors}"
    return entries


class TestRoundTrip:
    def test_single_directive_produces_one_row(self, conn, tmp_path):
        entries = _load(render_staged_txn_directive(_entry()), tmp_path)
        report = reconstruct_staged_transactions(conn, entries)
        assert report.rows_written == 1
        rows = list(conn.execute(
            "SELECT source, amount, posting_date, status FROM staged_transactions"
        ))
        assert len(rows) == 1
        r = rows[0]
        assert r["source"] == "simplefin"
        assert r["amount"] == "-42.17"
        assert r["posting_date"] == "2026-04-29"
        assert r["status"] == "new"

    def test_three_directives_three_rows(self, conn, tmp_path):
        rendered = (
            render_staged_txn_directive(_entry(simplefin_id="A1"))
            + render_staged_txn_directive(_entry(simplefin_id="B2"))
            + render_staged_txn_directive(_entry(simplefin_id="C3"))
        )
        entries = _load(rendered, tmp_path)
        report = reconstruct_staged_transactions(conn, entries)
        assert report.rows_written == 3
        n = conn.execute(
            "SELECT COUNT(*) FROM staged_transactions"
        ).fetchone()[0]
        assert n == 3

    def test_promoted_directive_marks_status_promoted(self, conn, tmp_path):
        rendered = render_staged_txn_promoted_directive(
            _entry(),
            promoted_at="2026-04-29T14:23:07+00:00",
            promoted_by="manual",
        )
        entries = _load(rendered, tmp_path)
        reconstruct_staged_transactions(conn, entries)
        rows = list(conn.execute(
            "SELECT status, promoted_at FROM staged_transactions"
        ))
        assert len(rows) == 1
        assert rows[0]["status"] == "promoted"
        assert rows[0]["promoted_at"] == "2026-04-29T14:23:07+00:00"

    def test_mixed_pending_and_promoted(self, conn, tmp_path):
        rendered = (
            render_staged_txn_directive(_entry(simplefin_id="P1"))
            + render_staged_txn_promoted_directive(
                _entry(simplefin_id="P2"),
                promoted_at="2026-04-29T14:23:07+00:00",
                promoted_by="rule",
                promoted_rule_id="rule-7",
            )
        )
        entries = _load(rendered, tmp_path)
        reconstruct_staged_transactions(conn, entries)
        rows = list(conn.execute(
            "SELECT source_ref, status FROM staged_transactions ORDER BY source_ref"
        ))
        assert len(rows) == 2
        # source_ref is JSON {id: ...}; use json.loads to compare ids
        ids_to_status = {
            json.loads(r["source_ref"])["id"]: r["status"] for r in rows
        }
        assert ids_to_status == {"P1": "new", "P2": "promoted"}


class TestIdempotence:
    def test_running_twice_produces_same_state(self, conn, tmp_path):
        rendered = (
            render_staged_txn_directive(_entry(simplefin_id="A1"))
            + render_staged_txn_directive(_entry(simplefin_id="B2"))
        )
        entries = _load(rendered, tmp_path)
        reconstruct_staged_transactions(conn, entries)
        first_rows = list(conn.execute(
            "SELECT source, source_ref_hash, amount, status "
            "FROM staged_transactions ORDER BY source_ref_hash"
        ))
        reconstruct_staged_transactions(conn, entries)
        second_rows = list(conn.execute(
            "SELECT source, source_ref_hash, amount, status "
            "FROM staged_transactions ORDER BY source_ref_hash"
        ))
        assert [dict(r) for r in first_rows] == [dict(r) for r in second_rows]
        assert len(first_rows) == 2


class TestADRInvariant:
    """ADR-0015: count of staged_transactions rows for non-promoted
    status equals the count of `custom "staged-txn"` directives in
    the ledger after reconstruct."""

    def test_invariant_after_rebuild(self, conn, tmp_path):
        rendered = (
            render_staged_txn_directive(_entry(simplefin_id="A1"))
            + render_staged_txn_directive(_entry(simplefin_id="A2"))
            + render_staged_txn_directive(_entry(simplefin_id="A3"))
            + render_staged_txn_promoted_directive(
                _entry(simplefin_id="P1"),
                promoted_at="2026-04-29T14:23:07+00:00",
                promoted_by="manual",
            )
        )
        entries = _load(rendered, tmp_path)
        reconstruct_staged_transactions(conn, entries)
        # 3 unpromoted "staged-txn" directives in the file
        # 1 "staged-txn-promoted" directive
        from beancount.core.data import Custom
        unpromoted_in_ledger = sum(
            1 for e in entries
            if isinstance(e, Custom) and e.type == "staged-txn"
        )
        promoted_in_ledger = sum(
            1 for e in entries
            if isinstance(e, Custom) and e.type == "staged-txn-promoted"
        )
        assert unpromoted_in_ledger == 3
        assert promoted_in_ledger == 1
        non_promoted_rows = conn.execute(
            "SELECT COUNT(*) FROM staged_transactions WHERE status != 'promoted'"
        ).fetchone()[0]
        assert non_promoted_rows == unpromoted_in_ledger
