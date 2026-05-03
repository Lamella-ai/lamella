# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the dangling-link purge path (migration 066).

Covers three scenarios per the implementation spec:
  (a) A confirmed-dead row (3+ 404s, 7-day cooldown elapsed) is deleted
      from paperless_doc_index after purge_confirmed_dead runs.
  (b) The corresponding "paperless-doc-deleted" tombstone directive is
      written to connector_links.bean.
  (c) A future txn_matcher lookup does NOT return the tombstoned paperless_id
      as a candidate.
  (d) A future sync upsert skips a tombstoned paperless_id.

We monkeypatch bean-check out of the way (no real Beancount ledger on disk)
for all tests that write directives, following the pattern established in
test_setup_resurrection.py and test_feature_calendar.py.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.receipts.dangling import (
    DEFAULT_CONSECUTIVE_404_THRESHOLD,
    DEFAULT_COOLDOWN_DAYS,
    purge_confirmed_dead,
)
from lamella.features.receipts.txn_matcher import find_document_candidates


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _old_timestamp(days_ago: int) -> str:
    """ISO-8601 UTC timestamp far enough in the past to clear the cooldown."""
    dt = datetime.utcnow() - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _make_db(tmp_path: Path) -> sqlite3.Connection:
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    return conn


def _seed_index_row(conn: sqlite3.Connection, *, paperless_id: int = 100) -> None:
    """Insert a minimal paperless_doc_index row."""
    conn.execute(
        """
        INSERT OR IGNORE INTO paperless_doc_index
            (paperless_id, title, total_amount, document_date, created_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        (paperless_id, "Acme Co Receipt", "42.00", "2026-01-15", "2026-01-15"),
    )
    conn.commit()


def _seed_health_row(
    conn: sqlite3.Connection,
    *,
    paperless_id: int = 100,
    consecutive_404s: int = 3,
    days_ago: int = 8,
) -> None:
    """Insert a paperless_link_health row that has crossed the gate."""
    first_404_at = _old_timestamp(days_ago)
    conn.execute(
        """
        INSERT OR REPLACE INTO paperless_link_health
            (paperless_id, consecutive_404s, first_404_at,
             last_404_at, last_check_at)
        VALUES (?, ?, ?, datetime('now'), datetime('now'))
        """,
        (paperless_id, consecutive_404s, first_404_at),
    )
    conn.commit()


def _make_minimal_ledger(tmp_path: Path) -> tuple[Path, Path]:
    """Create a bare main.bean + empty connector_links.bean for directive writes."""
    ledger_dir = tmp_path / "ledger"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    main_bean = ledger_dir / "main.bean"
    main_bean.write_text(
        '2026-01-01 custom "lamella-ledger-version" "2"\n',
        encoding="utf-8",
    )
    connector_links = ledger_dir / "connector_links.bean"
    return main_bean, connector_links


# ---------------------------------------------------------------------------
# (a) Confirmed-dead row is purged from paperless_doc_index
# ---------------------------------------------------------------------------

class TestPurgeDeletesIndexRow:
    def test_row_deleted_after_gate_crossed(self, tmp_path: Path, monkeypatch):
        """After purge_confirmed_dead, the paperless_doc_index row is gone."""
        conn = _make_db(tmp_path)
        main_bean, connector_links = _make_minimal_ledger(tmp_path)

        _seed_index_row(conn, paperless_id=100)
        _seed_health_row(conn, paperless_id=100, consecutive_404s=3, days_ago=8)

        # Monkeypatch bean-check so we don't need a real beancount ledger.
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.capture_bean_check",
            lambda _: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.run_bean_check_vs_baseline",
            lambda _main, _baseline: None,
        )

        result = purge_confirmed_dead(
            conn,
            connector_links=connector_links,
            main_bean=main_bean,
        )

        assert result.purged == 1
        # Row must be gone from paperless_doc_index.
        row = conn.execute(
            "SELECT 1 FROM paperless_doc_index WHERE paperless_id = 100"
        ).fetchone()
        assert row is None

    def test_row_not_purged_below_threshold(self, tmp_path: Path, monkeypatch):
        """A health row with only 2 consecutive 404s must not be purged."""
        conn = _make_db(tmp_path)
        main_bean, connector_links = _make_minimal_ledger(tmp_path)

        _seed_index_row(conn, paperless_id=200)
        _seed_health_row(conn, paperless_id=200, consecutive_404s=2, days_ago=8)

        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.capture_bean_check",
            lambda _: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.run_bean_check_vs_baseline",
            lambda _main, _baseline: None,
        )

        result = purge_confirmed_dead(
            conn,
            connector_links=connector_links,
            main_bean=main_bean,
        )

        assert result.purged == 0
        row = conn.execute(
            "SELECT 1 FROM paperless_doc_index WHERE paperless_id = 200"
        ).fetchone()
        assert row is not None  # still present

    def test_row_not_purged_before_cooldown(self, tmp_path: Path, monkeypatch):
        """A row with 3 consecutive 404s but only 2 days old must not be purged."""
        conn = _make_db(tmp_path)
        main_bean, connector_links = _make_minimal_ledger(tmp_path)

        _seed_index_row(conn, paperless_id=300)
        _seed_health_row(conn, paperless_id=300, consecutive_404s=3, days_ago=2)

        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.capture_bean_check",
            lambda _: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.run_bean_check_vs_baseline",
            lambda _main, _baseline: None,
        )

        result = purge_confirmed_dead(
            conn,
            connector_links=connector_links,
            main_bean=main_bean,
        )

        assert result.purged == 0
        row = conn.execute(
            "SELECT 1 FROM paperless_doc_index WHERE paperless_id = 300"
        ).fetchone()
        assert row is not None  # still present

    def test_idempotent_second_purge_is_noop(self, tmp_path: Path, monkeypatch):
        """Running purge_confirmed_dead twice on the same row is a no-op
        on the second call (the tombstone table gates re-processing)."""
        conn = _make_db(tmp_path)
        main_bean, connector_links = _make_minimal_ledger(tmp_path)

        _seed_index_row(conn, paperless_id=400)
        _seed_health_row(conn, paperless_id=400, consecutive_404s=3, days_ago=8)

        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.capture_bean_check",
            lambda _: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.run_bean_check_vs_baseline",
            lambda _main, _baseline: None,
        )

        result1 = purge_confirmed_dead(
            conn, connector_links=connector_links, main_bean=main_bean,
        )
        result2 = purge_confirmed_dead(
            conn, connector_links=connector_links, main_bean=main_bean,
        )

        assert result1.purged == 1
        assert result2.purged == 0  # already tombstoned


# ---------------------------------------------------------------------------
# (b) Tombstone is written to connector_links.bean
# ---------------------------------------------------------------------------

class TestTombstoneDirective:
    def test_tombstone_row_inserted_in_db(self, tmp_path: Path, monkeypatch):
        """A paperless_deleted_docs row is inserted during purge."""
        conn = _make_db(tmp_path)
        main_bean, connector_links = _make_minimal_ledger(tmp_path)

        _seed_index_row(conn, paperless_id=500)
        _seed_health_row(conn, paperless_id=500, consecutive_404s=3, days_ago=10)

        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.capture_bean_check",
            lambda _: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.run_bean_check_vs_baseline",
            lambda _main, _baseline: None,
        )

        purge_confirmed_dead(
            conn, connector_links=connector_links, main_bean=main_bean,
        )

        row = conn.execute(
            "SELECT paperless_id FROM paperless_deleted_docs WHERE paperless_id = 500"
        ).fetchone()
        assert row is not None
        assert int(row["paperless_id"]) == 500

    def test_directive_written_to_connector_links(self, tmp_path: Path, monkeypatch):
        """The 'paperless-doc-deleted' directive is appended to connector_links.bean."""
        conn = _make_db(tmp_path)
        main_bean, connector_links = _make_minimal_ledger(tmp_path)

        _seed_index_row(conn, paperless_id=600)
        _seed_health_row(conn, paperless_id=600, consecutive_404s=4, days_ago=14)

        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.capture_bean_check",
            lambda _: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.core.transform.custom_directive.run_bean_check_vs_baseline",
            lambda _main, _baseline: None,
        )

        result = purge_confirmed_dead(
            conn, connector_links=connector_links, main_bean=main_bean,
        )

        assert result.tombstoned == 1
        assert connector_links.exists()
        content = connector_links.read_text(encoding="utf-8")
        assert "paperless-doc-deleted" in content
        assert "lamella-paperless-id: 600" in content


# ---------------------------------------------------------------------------
# (c) Future txn_matcher does NOT propose a tombstoned doc as candidate
# ---------------------------------------------------------------------------

class TestTxnMatcherExcludesTombstoned:
    def test_tombstoned_id_excluded_from_candidates(self, tmp_path: Path):
        """A paperless_id in paperless_deleted_docs never surfaces as a
        candidate in find_document_candidates."""
        conn = _make_db(tmp_path)

        # Seed a doc that would otherwise match perfectly: same amount, today.
        paperless_id = 700
        today = date.today().isoformat()
        conn.execute(
            """
            INSERT INTO paperless_doc_index
                (paperless_id, title, total_amount, document_date, created_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            (paperless_id, "Example LLC Receipt", "99.00", today, today),
        )
        conn.commit()

        # Confirm it IS found before tombstoning.
        before = find_document_candidates(
            conn,
            txn_amount=Decimal("99.00"),
            txn_date=date.today(),
        )
        assert any(c.paperless_id == paperless_id for c in before), (
            "Doc should be a candidate before tombstoning"
        )

        # Tombstone it.
        conn.execute(
            "INSERT INTO paperless_deleted_docs (paperless_id) VALUES (?)",
            (paperless_id,),
        )
        conn.commit()

        # Now the same query must not return it.
        after = find_document_candidates(
            conn,
            txn_amount=Decimal("99.00"),
            txn_date=date.today(),
        )
        assert not any(c.paperless_id == paperless_id for c in after), (
            "Tombstoned doc must not appear as a candidate"
        )


# ---------------------------------------------------------------------------
# (d) Future sync upsert skips tombstoned paperless_ids
# ---------------------------------------------------------------------------

class TestSyncSkipsTombstoned:
    def test_sync_skips_tombstoned_doc(self, tmp_path: Path, monkeypatch):
        """PaperlessSync._upsert_doc skips a tombstoned paperless_id."""
        from lamella.features.paperless_bridge.sync import PaperlessSync
        from unittest.mock import MagicMock, AsyncMock

        conn = _make_db(tmp_path)

        # Tombstone paperless_id=800 before any sync.
        conn.execute(
            "INSERT INTO paperless_deleted_docs (paperless_id) VALUES (?)", (800,)
        )
        conn.commit()

        # Confirm nothing is in the index yet.
        before = conn.execute(
            "SELECT 1 FROM paperless_doc_index WHERE paperless_id = 800"
        ).fetchone()
        assert before is None

        # Build a minimal mock Document object for doc id 800.
        from lamella.adapters.paperless.schemas import Document
        doc = Document(
            id=800,
            title="Acme Co Invoice",
            created="2026-03-01",
            modified="2026-03-01",
            tags=[],
            custom_fields=[],
        )

        # Build a PaperlessSync with a real conn but a no-op client.
        mock_client = MagicMock()
        syncer = PaperlessSync(conn=conn, client=mock_client)

        # Call _upsert_doc directly. A mock mapping + empty dicts suffice.
        mock_mapping = MagicMock()
        mock_mapping.__iter__ = MagicMock(return_value=iter([]))
        syncer._upsert_doc(doc, {}, {}, mock_mapping)

        # Row must NOT be in paperless_doc_index.
        after = conn.execute(
            "SELECT 1 FROM paperless_doc_index WHERE paperless_id = 800"
        ).fetchone()
        assert after is None, (
            "Tombstoned doc must not be re-ingested by sync"
        )
