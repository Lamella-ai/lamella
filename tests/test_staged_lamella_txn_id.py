# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 1 of the immutable /txn/{token} URL invariant: every staged
row gets a UUIDv7 ``lamella_txn_id`` at insert time, the column is
populated for legacy rows by the post-migration backfill, and re-stage
of the same source_ref preserves the original id."""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import _backfill_python_minted_columns, connect, migrate
from lamella.features.import_.staging import StagingService


_UUIDV7_RE = (
    # 36-char canonical, version nibble = 7, variant high bits = 10.
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


@pytest.fixture()
def conn() -> sqlite3.Connection:
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _stage_one(svc: StagingService, *, source_ref: dict | None = None):
    return svc.stage(
        source="csv",
        source_ref=source_ref or {"id": "csv-1"},
        posting_date="2026-04-20",
        amount=Decimal("-12.34"),
        currency="USD",
        payee="Acme Co.",
        description="staged-test",
    )


class TestMintOnInsert:
    def test_stage_assigns_uuidv7(self, conn):
        row = _stage_one(StagingService(conn))
        import re
        assert row.lamella_txn_id is not None
        assert re.match(_UUIDV7_RE, row.lamella_txn_id)

    def test_re_stage_preserves_id(self, conn):
        svc = StagingService(conn)
        first = _stage_one(svc)
        # Same source_ref → same row, same identity, even though
        # mutable fields refresh.
        second = svc.stage(
            source="csv",
            source_ref={"id": "csv-1"},
            posting_date="2026-04-20",
            amount=Decimal("-12.34"),
            currency="USD",
            payee="Acme Co.",
            description="staged-test (refreshed)",
        )
        assert first.id == second.id
        assert first.lamella_txn_id == second.lamella_txn_id

    def test_two_rows_get_distinct_ids(self, conn):
        svc = StagingService(conn)
        a = _stage_one(svc, source_ref={"id": "csv-1"})
        b = _stage_one(svc, source_ref={"id": "csv-2"})
        assert a.lamella_txn_id != b.lamella_txn_id

    def test_get_by_lamella_txn_id_round_trip(self, conn):
        svc = StagingService(conn)
        row = _stage_one(svc)
        looked_up = svc.get_by_lamella_txn_id(row.lamella_txn_id)
        assert looked_up is not None
        assert looked_up.id == row.id

    def test_get_by_lamella_txn_id_unknown_returns_none(self, conn):
        svc = StagingService(conn)
        assert svc.get_by_lamella_txn_id("00000000-0000-7000-8000-000000000000") is None

    def test_get_by_lamella_txn_id_empty_returns_none(self, conn):
        assert StagingService(conn).get_by_lamella_txn_id("") is None


class TestBackfill:
    def test_backfill_fills_null_rows(self, conn):
        # Insert a row directly with NULL id, simulating a row that
        # predates migration 059. Use a parameterised statement so the
        # test stays compatible with the locked connection wrapper.
        conn.execute(
            "INSERT INTO staged_transactions ("
            "  source, source_ref, source_ref_hash, posting_date, "
            "  amount, raw_json, status, lamella_txn_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
            ("csv", "{}", "deadbeef", "2026-04-20", "-1.00", "{}", "new"),
        )
        before = conn.execute(
            "SELECT lamella_txn_id FROM staged_transactions"
        ).fetchone()
        assert before["lamella_txn_id"] is None

        _backfill_python_minted_columns(conn)

        after = conn.execute(
            "SELECT lamella_txn_id FROM staged_transactions"
        ).fetchone()
        import re
        assert after["lamella_txn_id"] is not None
        assert re.match(_UUIDV7_RE, after["lamella_txn_id"])

    def test_backfill_idempotent(self, conn):
        svc = StagingService(conn)
        row = _stage_one(svc)
        _backfill_python_minted_columns(conn)
        # Same identity after a second pass — we only fill NULLs.
        same = svc.get(row.id)
        assert same.lamella_txn_id == row.lamella_txn_id
