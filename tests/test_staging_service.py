# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the unified staging service (NEXTGEN.md Phase A)."""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    StagedDecision,
    StagedPair,
    StagedRow,
    StagingError,
    StagingService,
)


@pytest.fixture()
def svc() -> StagingService:
    conn = connect(Path(":memory:"))
    migrate(conn)
    return StagingService(conn)


# --- stage -----------------------------------------------------------


class TestStage:
    def test_insert_returns_row(self, svc: StagingService):
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-1", "txn_id": "T1"},
            posting_date="2026-04-21",
            amount=Decimal("-12.34"),
            payee="Coffee Shop",
            description="CARD PURCHASE",
            raw={"id": "T1", "posted": 1713657600},
        )
        assert isinstance(row, StagedRow)
        assert row.source == "simplefin"
        assert row.source_ref == {"account_id": "ACT-1", "txn_id": "T1"}
        assert row.amount == Decimal("-12.34")
        assert row.status == "new"
        assert row.payee == "Coffee Shop"

    def test_unknown_source_refused(self, svc: StagingService):
        with pytest.raises(StagingError, match="unknown source"):
            svc.stage(
                source="not-a-real-source",
                source_ref={},
                posting_date="2026-04-21",
                amount="1.00",
            )

    def test_dedup_on_source_ref(self, svc: StagingService):
        """Staging the same (source, source_ref) twice updates in place
        rather than inserting a duplicate."""
        r1 = svc.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-1", "txn_id": "T1"},
            posting_date="2026-04-21",
            amount="-12.34",
            payee="Coffee Shop",
        )
        r2 = svc.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-1", "txn_id": "T1"},
            posting_date="2026-04-21",
            amount="-12.34",
            payee="Coffee Shop Updated",  # changed
        )
        assert r1.id == r2.id
        assert r2.payee == "Coffee Shop Updated"
        # And only one row in total.
        count = svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions"
        ).fetchone()["n"]
        assert count == 1

    def test_dedup_status_preserved_on_restage(self, svc: StagingService):
        """Re-staging a row whose status already moved past 'new' must
        not reset it to 'new'. This keeps ingest-replay idempotent."""
        r = svc.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-1", "txn_id": "T1"},
            posting_date="2026-04-21",
            amount="-12.34",
        )
        svc.record_decision(
            staged_id=r.id,
            account="Expenses:Acme:Supplies",
            confidence="high",
            decided_by="rule",
        )
        # status should now be 'classified'
        assert svc.get(r.id).status == "classified"
        # Re-stage the same ref — status must stay 'classified', not revert.
        svc.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-1", "txn_id": "T1"},
            posting_date="2026-04-21",
            amount="-12.34",
        )
        assert svc.get(r.id).status == "classified"

    def test_ref_hash_stable_across_key_order(self, svc: StagingService):
        """The (source, source_ref) dedup key must not depend on the
        order keys appear in the caller's dict."""
        a = svc.stage(
            source="simplefin",
            source_ref={"account_id": "ACT-1", "txn_id": "T1"},
            posting_date="2026-04-21",
            amount="-12.34",
        )
        b = svc.stage(
            source="simplefin",
            source_ref={"txn_id": "T1", "account_id": "ACT-1"},  # reversed
            posting_date="2026-04-21",
            amount="-12.34",
        )
        assert a.id == b.id

    def test_different_sources_do_not_collide(self, svc: StagingService):
        """A CSV row and a SimpleFIN row that happen to share a
        source_ref JSON shape must be treated as distinct."""
        a = svc.stage(
            source="csv",
            source_ref={"upload_id": 1, "row": 42},
            posting_date="2026-04-21",
            amount="-12.34",
        )
        b = svc.stage(
            source="simplefin",
            source_ref={"upload_id": 1, "row": 42},
            posting_date="2026-04-21",
            amount="-12.34",
        )
        assert a.id != b.id


# --- read ------------------------------------------------------------


class TestRead:
    def test_get_by_ref_miss_returns_none(self, svc: StagingService):
        assert svc.get_by_ref("simplefin", {"txn_id": "nope"}) is None

    def test_get_by_ref_hit(self, svc: StagingService):
        r = svc.stage(
            source="csv",
            source_ref={"upload_id": 7, "row": 3},
            posting_date="2026-04-21",
            amount="10.00",
        )
        found = svc.get_by_ref("csv", {"upload_id": 7, "row": 3})
        assert found is not None
        assert found.id == r.id

    def test_list_by_status_filters(self, svc: StagingService):
        a = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-20",
            amount="1.00",
        )
        b = svc.stage(
            source="csv", source_ref={"r": 2}, posting_date="2026-04-21",
            amount="2.00",
        )
        svc.record_decision(
            staged_id=a.id, account="X", confidence="high", decided_by="rule",
        )
        # a: classified, b: new
        new_only = svc.list_by_status("new")
        classified_only = svc.list_by_status("classified")
        assert [r.id for r in new_only] == [b.id]
        assert [r.id for r in classified_only] == [a.id]

    def test_list_by_date_amount_exact(self, svc: StagingService):
        a = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-20",
            amount="12.50",
        )
        svc.stage(
            source="csv", source_ref={"r": 2}, posting_date="2026-04-20",
            amount="99.00",  # different amount
        )
        found = svc.list_by_date_amount(
            posting_date="2026-04-20", amount="12.50",
        )
        assert [r.id for r in found] == [a.id]

    def test_list_by_date_amount_tolerance(self, svc: StagingService):
        a = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-19",
            amount="12.50",
        )
        b = svc.stage(
            source="simplefin", source_ref={"r": 2}, posting_date="2026-04-21",
            amount="12.50",
        )
        # Looking for a row on 2026-04-20 with a 1-day window — both should hit.
        found = svc.list_by_date_amount(
            posting_date="2026-04-20", amount="12.50", tolerance_days=1,
        )
        ids = {r.id for r in found}
        assert ids == {a.id, b.id}


# --- decisions -------------------------------------------------------


class TestDecision:
    def test_record_and_fetch(self, svc: StagingService):
        r = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="10.00",
        )
        dec = svc.record_decision(
            staged_id=r.id,
            account="Expenses:Acme:Supplies",
            confidence="high",
            confidence_score=0.95,
            decided_by="rule",
            rationale="matched rule #7",
        )
        assert isinstance(dec, StagedDecision)
        assert dec.account == "Expenses:Acme:Supplies"
        assert dec.confidence == "high"
        assert dec.needs_review is False
        fetched = svc.get_decision(r.id)
        assert fetched is not None
        assert fetched.account == dec.account

    def test_invalid_confidence_refused(self, svc: StagingService):
        r = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="1.00",
        )
        with pytest.raises(StagingError, match="invalid confidence"):
            svc.record_decision(
                staged_id=r.id,
                account="X",
                confidence="bogus",
                decided_by="rule",
            )

    def test_decision_upserts(self, svc: StagingService):
        r = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="10.00",
        )
        svc.record_decision(
            staged_id=r.id, account="A", confidence="low", decided_by="ai",
            needs_review=True,
        )
        # Human override: same staged_id, different decision.
        svc.record_decision(
            staged_id=r.id, account="B", confidence="high", decided_by="human",
        )
        final = svc.get_decision(r.id)
        assert final is not None
        assert final.account == "B"
        assert final.decided_by == "human"
        # Only one decision row per staged id.
        n = svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_decisions WHERE staged_id = ?",
            (r.id,),
        ).fetchone()["n"]
        assert n == 1


# --- pairs -----------------------------------------------------------


class TestPairs:
    def test_record_pair_between_staged_rows(self, svc: StagingService):
        a = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-20",
            amount="-100.00",
        )
        b = svc.stage(
            source="simplefin", source_ref={"r": 2}, posting_date="2026-04-21",
            amount="100.00",
        )
        pair = svc.record_pair(
            kind="transfer",
            confidence="high",
            a_staged_id=a.id,
            b_staged_id=b.id,
            reason="opposite-sign, 1-day window",
        )
        assert isinstance(pair, StagedPair)
        assert pair.kind == "transfer"
        # Both sides should advance to 'matched'.
        assert svc.get(a.id).status == "matched"
        assert svc.get(b.id).status == "matched"

    def test_pair_to_ledger_only_side(self, svc: StagingService):
        """A staged row can pair against a transaction already committed
        to the ledger — b_ledger_hash replaces b_staged_id."""
        a = svc.stage(
            source="simplefin", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="50.00",
        )
        pair = svc.record_pair(
            kind="duplicate",
            confidence="medium",
            a_staged_id=a.id,
            b_ledger_hash="abc123deadbeef",
            reason="matches committed ledger txn",
        )
        assert pair.b_staged_id is None
        assert pair.b_ledger_hash == "abc123deadbeef"

    def test_pair_requires_one_side(self, svc: StagingService):
        a = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="1.00",
        )
        with pytest.raises(StagingError, match="second staged row or a ledger"):
            svc.record_pair(kind="transfer", confidence="high", a_staged_id=a.id)

    def test_pairs_for_finds_both_sides(self, svc: StagingService):
        a = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-20",
            amount="-100.00",
        )
        b = svc.stage(
            source="simplefin", source_ref={"r": 2}, posting_date="2026-04-21",
            amount="100.00",
        )
        svc.record_pair(
            kind="transfer", confidence="high",
            a_staged_id=a.id, b_staged_id=b.id,
        )
        # The same pair record is returned whether we query from side A or B.
        from_a = svc.pairs_for(a.id)
        from_b = svc.pairs_for(b.id)
        assert len(from_a) == 1
        assert len(from_b) == 1
        assert from_a[0].id == from_b[0].id


# --- lifecycle -------------------------------------------------------


class TestLifecycle:
    def test_mark_promoted(self, svc: StagingService):
        r = svc.stage(
            source="simplefin", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="10.00",
        )
        after = svc.mark_promoted(
            r.id, promoted_to_file="simplefin_transactions.bean",
            promoted_txn_hash="hash-xyz",
        )
        assert after.status == "promoted"
        assert after.promoted_to_file == "simplefin_transactions.bean"
        assert after.promoted_txn_hash == "hash-xyz"
        assert after.promoted_at is not None

    def test_mark_promoted_missing_row(self, svc: StagingService):
        with pytest.raises(StagingError, match="no staged row"):
            svc.mark_promoted(99999, promoted_to_file="x.bean")

    def test_mark_failed_stamps_rationale(self, svc: StagingService):
        r = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="1.00",
        )
        svc.record_decision(
            staged_id=r.id, account="X", confidence="high", decided_by="ai",
            rationale="classified via AI",
        )
        svc.mark_failed(r.id, reason="bean-check failed")
        assert svc.get(r.id).status == "failed"
        dec = svc.get_decision(r.id)
        assert dec is not None and dec.rationale is not None
        assert "FAILED: bean-check failed" in dec.rationale
        assert "classified via AI" in dec.rationale

    def test_dismiss_creates_decision_if_missing(self, svc: StagingService):
        r = svc.stage(
            source="paste", source_ref={"session": "abc"}, posting_date="2026-04-21",
            amount="1.00",
        )
        svc.dismiss(r.id, reason="spurious entry")
        assert svc.get(r.id).status == "dismissed"
        dec = svc.get_decision(r.id)
        assert dec is not None
        assert dec.rationale is not None
        # User-facing word is "Ignored" — the underlying status column
        # name is still ``dismissed`` for migration/compat reasons.
        assert "Ignored: spurious entry" in dec.rationale

    def test_restore_flips_dismissed_back_to_new(self, svc: StagingService):
        r = svc.stage(
            source="paste", source_ref={"session": "restore"},
            posting_date="2026-04-21", amount="1.00",
        )
        svc.dismiss(r.id, reason="changed my mind")
        assert svc.get(r.id).status == "dismissed"
        svc.restore(r.id)
        assert svc.get(r.id).status == "new"
        # restore is idempotent — calling on a non-dismissed row is a
        # no-op (status stays whatever it was).
        svc.restore(r.id)
        assert svc.get(r.id).status == "new"


# --- reconstruct-awareness (staging is cache, not state) -------------


class TestReconstructAwareness:
    def test_staging_tables_not_registered_as_reconstruct_state(self):
        """Post-ADR-0043 the ``staged_transactions`` table IS rebuilt
        from `custom "staged-txn"` ledger directives (step24), so it
        legitimately appears in reconstruct state. The remaining
        cache-only tables — ``staged_decisions`` and ``staged_pairs`` —
        must still not be claimed by any pass."""
        from lamella.core.transform import reconstruct
        reconstruct._import_all_steps()
        cache_only_tables = {"staged_decisions", "staged_pairs"}
        for p in reconstruct.registered_passes():
            assert not (set(p.state_tables) & cache_only_tables), (
                f"reconstruct pass {p.name!r} claims cache-only staging "
                f"tables as state: {p.state_tables}"
            )


# --- maintenance -----------------------------------------------------


class TestCleanup:
    def test_cleanup_skips_active_rows(self, svc: StagingService):
        live = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="1.00",
        )
        # Even if we backdate updated_at, 'new' rows must not be deleted.
        svc.conn.execute(
            "UPDATE staged_transactions SET updated_at = datetime('now', '-90 days') "
            "WHERE id = ?",
            (live.id,),
        )
        deleted = svc.cleanup_terminal(older_than_days=30)
        assert deleted == 0
        assert svc.get(live.id).status == "new"

    def test_cleanup_removes_old_promoted(self, svc: StagingService):
        r = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="1.00",
        )
        svc.mark_promoted(r.id, promoted_to_file="connector_imports/2026.bean")
        # Backdate so cleanup window catches it.
        svc.conn.execute(
            "UPDATE staged_transactions SET updated_at = datetime('now', '-90 days') "
            "WHERE id = ?",
            (r.id,),
        )
        deleted = svc.cleanup_terminal(older_than_days=30)
        assert deleted == 1
        assert svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_transactions"
        ).fetchone()["n"] == 0

    def test_cleanup_keeps_old_dismissed(self, svc: StagingService):
        """Ignored (dismissed) rows must persist — even very old ones —
        so reconciling against a future bank balance can still see
        what the user previously chose to skip."""
        r = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2024-01-01",
            amount="1.00",
        )
        svc.dismiss(r.id, reason="really old skip")
        svc.conn.execute(
            "UPDATE staged_transactions SET updated_at = datetime('now', '-365 days') "
            "WHERE id = ?",
            (r.id,),
        )
        deleted = svc.cleanup_terminal(older_than_days=30)
        assert deleted == 0
        assert svc.get(r.id).status == "dismissed"


# --- view ------------------------------------------------------------


class TestPendingView:
    def test_pending_view_shows_active_rows(self, svc: StagingService):
        new = svc.stage(
            source="csv", source_ref={"r": 1}, posting_date="2026-04-21",
            amount="1.00",
        )
        classified = svc.stage(
            source="csv", source_ref={"r": 2}, posting_date="2026-04-20",
            amount="2.00",
        )
        svc.record_decision(
            staged_id=classified.id, account="X", confidence="medium",
            decided_by="ai",
        )
        promoted = svc.stage(
            source="csv", source_ref={"r": 3}, posting_date="2026-04-19",
            amount="3.00",
        )
        svc.mark_promoted(promoted.id, promoted_to_file="x.bean")
        dismissed = svc.stage(
            source="csv", source_ref={"r": 4}, posting_date="2026-04-18",
            amount="4.00",
        )
        svc.dismiss(dismissed.id)

        rows = svc.conn.execute(
            "SELECT id, status FROM v_staged_pending"
        ).fetchall()
        ids = {int(r["id"]) for r in rows}
        assert ids == {new.id, classified.id}
