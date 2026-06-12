# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the unified cross-source transfer matcher (Phase C)."""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.import_.staging import (
    PairProposal,
    StagingService,
    apply_pairs,
    find_pairs,
)


@pytest.fixture()
def svc() -> StagingService:
    conn = connect(Path(":memory:"))
    migrate(conn)
    return StagingService(conn)


def _stage(
    svc: StagingService, *,
    source: str, ref_key: str,
    date: str, amount: str,
    payee: str | None = None, description: str | None = None,
    session_id: str | None = None,
) -> int:
    return svc.stage(
        source=source,
        source_ref={"k": ref_key},
        session_id=session_id,
        posting_date=date,
        amount=amount,
        payee=payee,
        description=description,
    ).id


# --- core pairing ---------------------------------------------------


class TestCrossSourceTransfer:
    def test_cross_source_transfer_same_day_high_confidence(self, svc):
        """The motivating scenario: PayPal CSV shows money out,
        Bank One SimpleFIN shows matching money in, same day.
        Must land as a high-confidence transfer proposal."""
        a = _stage(svc, source="csv", ref_key="paypal-out",
                   date="2026-04-20", amount="-500.00",
                   payee="PayPal", description="Transfer to bank")
        b = _stage(svc, source="simplefin", ref_key="wf-in",
                   date="2026-04-20", amount="500.00",
                   payee="PayPal", description="Incoming transfer")

        proposals = find_pairs(svc.conn)
        assert len(proposals) == 1
        p = proposals[0]
        assert p.kind == "transfer"
        assert p.confidence == "high"
        assert {p.a_staged_id, p.b_staged_id} == {a, b}

    def test_cross_source_transfer_next_day_still_medium(self, svc):
        _stage(svc, source="csv", ref_key="out",
               date="2026-04-20", amount="-500.00")
        _stage(svc, source="simplefin", ref_key="in",
               date="2026-04-21", amount="500.00")
        proposals = find_pairs(svc.conn)
        assert len(proposals) == 1
        assert proposals[0].confidence in {"high", "medium"}

    def test_outside_window_does_not_pair(self, svc):
        _stage(svc, source="csv", ref_key="out",
               date="2026-04-01", amount="-500.00")
        _stage(svc, source="simplefin", ref_key="in",
               date="2026-04-15", amount="500.00")
        proposals = find_pairs(svc.conn, window_days=7)
        assert proposals == []

    def test_different_amounts_do_not_pair(self, svc):
        _stage(svc, source="csv", ref_key="a",
               date="2026-04-20", amount="-500.00")
        _stage(svc, source="simplefin", ref_key="b",
               date="2026-04-20", amount="499.00")
        assert find_pairs(svc.conn) == []

    def test_below_min_amount_skipped(self, svc):
        """Tiny transactions are skipped to reduce noise — common
        for coffee-shop pocket change that happens to match another
        trivial amount."""
        _stage(svc, source="csv", ref_key="a",
               date="2026-04-20", amount="-0.50")
        _stage(svc, source="simplefin", ref_key="b",
               date="2026-04-20", amount="0.50")
        assert find_pairs(svc.conn, min_amount="1.00") == []


# --- duplicate detection (same-sign, cross-source) -----------------


class TestDuplicateDetection:
    def test_same_sign_cross_source_same_day_is_duplicate(self, svc):
        """Two sources independently reporting the same outflow —
        double-import that shouldn't have happened. Matcher flags
        as a duplicate pair, not a transfer."""
        a = _stage(svc, source="csv", ref_key="a",
                   date="2026-04-20", amount="-100.00",
                   payee="Acme")
        b = _stage(svc, source="simplefin", ref_key="b",
                   date="2026-04-20", amount="-100.00",
                   payee="Acme Inc")
        proposals = find_pairs(svc.conn)
        assert len(proposals) == 1
        assert proposals[0].kind == "duplicate"
        assert {proposals[0].a_staged_id, proposals[0].b_staged_id} == {a, b}


# --- source-scope rules --------------------------------------------


class TestSourceScope:
    def test_same_source_opposite_sign_skipped_by_default(self, svc):
        """Default mode is cross-source only. Two same-source rows
        are left to the source-specific detector (importer's
        transfers.detect for CSV) or ignored."""
        _stage(svc, source="csv", ref_key="a",
               date="2026-04-20", amount="-500.00",
               session_id="1")
        _stage(svc, source="csv", ref_key="b",
               date="2026-04-20", amount="500.00",
               session_id="1")
        assert find_pairs(svc.conn) == []

    def test_same_source_paired_when_cross_source_disabled(self, svc):
        """Phase E (reboot re-ingest) pulls everything from one
        source='reboot' bucket, so the matcher needs a mode that
        ignores the cross-source rule."""
        a = _stage(svc, source="reboot", ref_key="a",
                   date="2026-04-20", amount="-500.00")
        b = _stage(svc, source="reboot", ref_key="b",
                   date="2026-04-20", amount="500.00")
        proposals = find_pairs(svc.conn, require_cross_source=False)
        assert len(proposals) == 1
        assert {proposals[0].a_staged_id, proposals[0].b_staged_id} == {a, b}


def _stage_with_path(
    svc: StagingService, *, source: str, ref_key: str,
    date: str, amount: str, account_path: str,
    payee: str | None = None, description: str | None = None,
) -> int:
    """Stage a row with an explicit ``account_path`` in source_ref,
    bypassing the simplefin_account_id → path lookup. Lets the
    account-root resolver classify the row's backing account
    without standing up the full accounts_meta table."""
    return svc.stage(
        source=source,
        source_ref={"k": ref_key, "account_path": account_path},
        posting_date=date,
        amount=amount,
        payee=payee,
        description=description,
    ).id


class TestLiabilityPaymentCarveOut:
    """A credit-card / loan / line-of-credit / mortgage account can't
    hold cash — its only inflow is a transfer leg from an Asset.
    Both legs commonly arrive on the same SimpleFIN feed, so the
    cross-source default would silently exclude this dominant
    transfer pattern. The carve-out admits same-source
    Asset↔Liability pairs when the direction is right."""

    def test_intra_simplefin_credit_card_payment_pairs(self, svc):
        # Cash leaves checking
        a = _stage_with_path(
            svc, source="simplefin", ref_key="checking-out",
            date="2026-04-20", amount="-150.00",
            account_path="Assets:Checking:Acme",
            payee="ONLINE PAYMENT",
        )
        # Debt reduced on credit card
        b = _stage_with_path(
            svc, source="simplefin", ref_key="cc-pmt",
            date="2026-04-20", amount="150.00",
            account_path="Liabilities:CreditCard:Visa",
            payee="PAYMENT THANK YOU",
        )
        proposals = find_pairs(svc.conn)
        assert len(proposals) == 1
        assert proposals[0].kind == "transfer"
        assert proposals[0].confidence == "high"
        assert {proposals[0].a_staged_id, proposals[0].b_staged_id} == {a, b}

    def test_intra_simplefin_refund_plus_cc_purchase_does_not_pair(self, svc):
        """Direction guard: a same-day refund into checking + a
        same-amount CC purchase share an amount and have opposite
        signs but neither leg is a transfer. The carve-out should
        not admit the pair."""
        _stage_with_path(
            svc, source="simplefin", ref_key="refund",
            date="2026-04-20", amount="150.00",
            account_path="Assets:Checking:Acme",
            payee="REFUND FROM SOMEWHERE",
        )
        _stage_with_path(
            svc, source="simplefin", ref_key="cc-charge",
            date="2026-04-20", amount="-150.00",
            account_path="Liabilities:CreditCard:Visa",
            payee="A STORE",
        )
        # Cross-source rule still applies (same source, wrong direction)
        assert find_pairs(svc.conn) == []

    def test_intra_simplefin_loan_payment_pairs(self, svc):
        a = _stage_with_path(
            svc, source="simplefin", ref_key="ach-out",
            date="2026-04-20", amount="-1234.56",
            account_path="Assets:Checking:Acme",
            payee="MORTGAGE PMT",
        )
        b = _stage_with_path(
            svc, source="simplefin", ref_key="loan-recv",
            date="2026-04-20", amount="1234.56",
            account_path="Liabilities:Mortgage:Primary",
            payee="PAYMENT RECEIVED",
        )
        proposals = find_pairs(svc.conn)
        assert len(proposals) == 1
        assert proposals[0].kind == "transfer"
        assert {proposals[0].a_staged_id, proposals[0].b_staged_id} == {a, b}

    def test_same_source_asset_to_asset_still_excluded(self, svc):
        """The carve-out only covers Asset↔Liability — a checking-
        to-savings same-source pair still hits the cross-source
        skip. (We could broaden later if user demand surfaces.)"""
        _stage_with_path(
            svc, source="simplefin", ref_key="ck-out",
            date="2026-04-20", amount="-200.00",
            account_path="Assets:Checking:Acme",
        )
        _stage_with_path(
            svc, source="simplefin", ref_key="sv-in",
            date="2026-04-20", amount="200.00",
            account_path="Assets:Savings:Acme",
        )
        assert find_pairs(svc.conn) == []


# --- greedy assignment + already-paired ----------------------------


class TestGreedyAssignment:
    def test_each_row_appears_in_at_most_one_proposal(self, svc):
        """Three candidates of the same amount in the same window:
        the best-scoring pair wins; the third row is not paired
        twice."""
        a = _stage(svc, source="csv", ref_key="a",
                   date="2026-04-20", amount="-500.00",
                   payee="Target Match")
        b = _stage(svc, source="simplefin", ref_key="b",
                   date="2026-04-20", amount="500.00",
                   payee="Target Match")
        c = _stage(svc, source="simplefin", ref_key="c",
                   date="2026-04-22", amount="500.00",
                   payee="Unrelated")
        proposals = find_pairs(svc.conn)
        # The a↔b pair wins on same-day + shared payee tokens. c is
        # not re-paired with a even though amount matches.
        used = {sid for p in proposals for sid in (p.a_staged_id, p.b_staged_id)}
        assert a in used and b in used
        # c may appear in a second pair with a different row, but not
        # with a (a is already used). In this fixture it doesn't pair.
        assert len(proposals) == 1

    def test_already_paired_rows_excluded(self, svc):
        a = _stage(svc, source="csv", ref_key="a",
                   date="2026-04-20", amount="-500.00")
        b = _stage(svc, source="simplefin", ref_key="b",
                   date="2026-04-20", amount="500.00")
        svc.record_pair(
            kind="transfer", confidence="high",
            a_staged_id=a, b_staged_id=b, reason="pre-existing",
        )
        assert find_pairs(svc.conn) == []


# --- apply_pairs ---------------------------------------------------


class TestApplyPairs:
    def test_apply_only_high_by_default(self, svc):
        _stage(svc, source="csv", ref_key="a1",
               date="2026-04-20", amount="-500.00")
        _stage(svc, source="simplefin", ref_key="b1",
               date="2026-04-20", amount="500.00")
        _stage(svc, source="csv", ref_key="a2",
               date="2026-04-14", amount="-250.00")
        _stage(svc, source="simplefin", ref_key="b2",
               date="2026-04-20", amount="250.00")  # 6 days apart
        proposals = find_pairs(svc.conn, min_confidence="low")
        bands = {p.confidence for p in proposals}
        # Two pairs but only one high-confidence. Default apply
        # threshold is 'high'.
        written = apply_pairs(svc.conn, proposals)
        assert written == sum(1 for p in proposals if p.confidence == "high")
        assert "high" in bands

    def test_apply_advances_both_sides_to_matched_status(self, svc):
        a = _stage(svc, source="csv", ref_key="a",
                   date="2026-04-20", amount="-500.00")
        b = _stage(svc, source="simplefin", ref_key="b",
                   date="2026-04-20", amount="500.00")
        proposals = find_pairs(svc.conn)
        apply_pairs(svc.conn, proposals)
        assert svc.get(a).status == "matched"
        assert svc.get(b).status == "matched"

    def test_apply_is_idempotent_skips_already_paired(self, svc):
        a = _stage(svc, source="csv", ref_key="a",
                   date="2026-04-20", amount="-500.00")
        b = _stage(svc, source="simplefin", ref_key="b",
                   date="2026-04-20", amount="500.00")
        proposals = find_pairs(svc.conn)
        first = apply_pairs(svc.conn, proposals)
        assert first == 1
        # Re-running with the same proposal list must not double-write.
        second = apply_pairs(svc.conn, proposals)
        assert second == 0
        # Only one pair in the table.
        n = svc.conn.execute(
            "SELECT COUNT(*) AS n FROM staged_pairs"
        ).fetchone()["n"]
        assert n == 1


# --- scoring / narration similarity --------------------------------


class TestScoring:
    def test_narration_overlap_boosts_score(self, svc):
        """Two pairs with identical amount/date/source setup except
        one shares a narration token. The shared-narration pair
        scores higher."""
        # Pair 1: no narration overlap.
        _stage(svc, source="csv", ref_key="p1a",
               date="2026-04-15", amount="-100.00",
               payee="Anonymous Merchant A")
        _stage(svc, source="simplefin", ref_key="p1b",
               date="2026-04-15", amount="100.00",
               payee="Unrelated Deposit X")
        # Pair 2: narration overlap ("PayPal").
        _stage(svc, source="csv", ref_key="p2a",
               date="2026-04-20", amount="-200.00",
               payee="PayPal Transfer")
        _stage(svc, source="simplefin", ref_key="p2b",
               date="2026-04-20", amount="200.00",
               payee="PayPal Inc")
        proposals = find_pairs(svc.conn, min_confidence="low")
        assert len(proposals) == 2
        by_amount = {p.a_staged_id: p for p in proposals}
        # The PayPal pair (200) should have a higher score than the
        # 100-amount pair.
        scores_by_amount: dict[Decimal, float] = {}
        for p in proposals:
            a_row = svc.get(p.a_staged_id)
            scores_by_amount[abs(Decimal(a_row.amount))] = p.score
        assert scores_by_amount[Decimal("200.00")] > scores_by_amount[Decimal("100.00")]


# --- matched rows stay out of future runs --------------------------


class TestMatchedRowsStaySettled:
    def test_already_matched_row_not_repaired(self, svc):
        """After apply_pairs advances rows to status='matched',
        a follow-up find_pairs call does not re-propose them."""
        a = _stage(svc, source="csv", ref_key="a",
                   date="2026-04-20", amount="-500.00")
        b = _stage(svc, source="simplefin", ref_key="b",
                   date="2026-04-20", amount="500.00")
        apply_pairs(svc.conn, find_pairs(svc.conn))
        # Stage a new candidate with matching amount — should not pair
        # with a or b (they're already paired), even though status
        # 'matched' is in the candidate-load whitelist.
        c = _stage(svc, source="simplefin", ref_key="c",
                   date="2026-04-20", amount="500.00")
        second = find_pairs(svc.conn)
        # c should not pair with b (b is paired to a); nor with a.
        assert second == []
        # And a, b are still matched; c is still new.
        assert svc.get(a).status == "matched"
        assert svc.get(b).status == "matched"
        assert svc.get(c).status == "new"
