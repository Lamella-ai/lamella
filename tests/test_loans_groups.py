# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP5 — multi-leg payment group proposer + apply_group writer.

Covers: proposer tier-matching, dense-window cap, group_id stability,
exclusion of already-grouped hashes, in-flight math in apply_group,
and reader roundtrip for reconstruct.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction

from lamella.features.loans.groups import (
    DEFAULT_MAX_MEMBERS,
    DEFAULT_SUBSET_CAP,
    FixmeLeg,
    MemberPosting,
    ProposedGroup,
    _subset_count,
    apply_group,
    compute_group_id,
    from_transactions,
    in_flight_path_for,
    propose_groups,
    read_loan_payment_groups,
)


def _loan(**overrides):
    base = {
        "slug": "Main",
        "entity_slug": "Personal",
        "liability_account_path": "Liabilities:Personal:Main",
        "monthly_payment_estimate": "3500.00",
    }
    base.update(overrides)
    return base


def _leg(h, d, amt, *, liability=False):
    return FixmeLeg(h, d, Decimal(str(amt)), touches_liability=liability)


# --------------------------------------------------------------- proposer


class TestProposerHappyPath:
    def test_three_member_group_sums_to_monthly(self):
        loan = _loan()
        legs = [
            _leg("h1", date(2026, 3, 1), "2500.00", liability=True),
            _leg("h2", date(2026, 3, 2), "800.00"),
            _leg("h3", date(2026, 3, 3), "200.00"),
        ]
        report = propose_groups(loan, legs, Decimal("3500.00"))
        assert len(report.groups) == 1
        g = report.groups[0]
        assert g.aggregate_amount == Decimal("3500.00")
        assert g.member_hashes == ("h1", "h2", "h3")
        assert g.suggested_primary_hash == "h1"
        assert g.suggested_in_flight_path == "Assets:Personal:InFlight:Loans:Main"
        assert g.sum_delta == Decimal("0.00")

    def test_group_id_is_stable_across_reruns(self):
        loan = _loan()
        legs = [
            _leg("zz", date(2026, 3, 1), "2500.00"),
            _leg("aa", date(2026, 3, 2), "800.00"),
            _leg("mm", date(2026, 3, 3), "200.00"),
        ]
        r1 = propose_groups(loan, legs, Decimal("3500.00"))
        r2 = propose_groups(loan, legs, Decimal("3500.00"))
        assert r1.groups[0].group_id == r2.groups[0].group_id

    def test_group_id_order_independent(self):
        g1 = compute_group_id(["a", "b", "c"])
        g2 = compute_group_id(["c", "b", "a"])
        assert g1 == g2

    def test_tolerance_match(self):
        # 3499.50 is within ±2.00 of 3500.00.
        legs = [
            _leg("h1", date(2026, 3, 1), "2500.00"),
            _leg("h2", date(2026, 3, 2), "799.50"),
            _leg("h3", date(2026, 3, 3), "200.00"),
        ]
        report = propose_groups(_loan(), legs, Decimal("3500.00"))
        assert len(report.groups) == 1
        assert report.groups[0].sum_delta == Decimal("-0.50")

    def test_out_of_tolerance_rejected(self):
        legs = [
            _leg("h1", date(2026, 3, 1), "2500.00"),
            _leg("h2", date(2026, 3, 2), "496.00"),  # 2996 total, ≠ 3500
        ]
        report = propose_groups(_loan(), legs, Decimal("3500.00"))
        assert report.groups == []

    def test_primary_selection_prefers_liability_toucher(self):
        loan = _loan()
        # Third member is biggest but fourth touches liability — the
        # liability-toucher wins even though it's smaller.
        legs = [
            _leg("small", date(2026, 3, 1), "100.00"),
            _leg("mid", date(2026, 3, 1), "800.00", liability=True),
            _leg("big", date(2026, 3, 2), "2600.00"),
        ]
        report = propose_groups(loan, legs, Decimal("3500.00"))
        assert len(report.groups) == 1
        assert report.groups[0].suggested_primary_hash == "mid"

    def test_primary_selection_falls_back_to_largest(self):
        loan = _loan()
        legs = [
            _leg("a", date(2026, 3, 1), "100.00"),
            _leg("b", date(2026, 3, 1), "800.00"),
            _leg("c", date(2026, 3, 2), "2600.00"),
        ]
        report = propose_groups(loan, legs, Decimal("3500.00"))
        assert len(report.groups) == 1
        # None touches liability → largest wins.
        assert report.groups[0].suggested_primary_hash == "c"


class TestProposerExclusions:
    def test_already_grouped_hashes_excluded(self):
        loan = _loan()
        legs = [
            _leg("h1", date(2026, 3, 1), "2500.00", liability=True),
            _leg("h2", date(2026, 3, 2), "800.00"),
            _leg("h3", date(2026, 3, 3), "200.00"),
        ]
        # With h1 claimed, remaining h2+h3 = 1000 ≠ 3500 — no group.
        report = propose_groups(
            loan, legs, Decimal("3500.00"),
            already_grouped_hashes=["h1"],
        )
        assert report.groups == []

    def test_consumed_hashes_not_reused_across_windows(self):
        # Two adjacent windows both sum to monthly; the first group's
        # members must not be reused in the second.
        loan = _loan()
        legs = [
            _leg("a1", date(2026, 3, 1), "2500.00", liability=True),
            _leg("a2", date(2026, 3, 2), "1000.00"),
            # second group, one week later
            _leg("b1", date(2026, 3, 10), "2500.00", liability=True),
            _leg("b2", date(2026, 3, 11), "1000.00"),
        ]
        report = propose_groups(loan, legs, Decimal("3500.00"))
        # Two distinct groups, no shared members.
        assert len(report.groups) == 2
        members_a = set(report.groups[0].member_hashes)
        members_b = set(report.groups[1].member_hashes)
        assert members_a.isdisjoint(members_b)


class TestDenseWindowCap:
    def test_subset_count_math(self):
        # C(n,2)+C(n,3)+C(n,4) — cap=500 trips at n≥11.
        assert _subset_count(4, 4) == 6 + 4 + 1  # 11
        assert _subset_count(10, 4) == 45 + 120 + 210  # 375
        assert _subset_count(11, 4) == 55 + 165 + 330  # 550 (>500)
        assert _subset_count(12, 4) == 66 + 220 + 495  # 781

    def test_dense_window_skipped_with_info_signal(self):
        loan = _loan()
        # 15 candidate legs all in a 5-day window → 1925 subsets.
        legs = [
            FixmeLeg(
                f"h{i}",
                date(2026, 3, 1) + timedelta(days=i % 5),
                Decimal("100.00"),
            )
            for i in range(15)
        ]
        report = propose_groups(
            loan, legs, Decimal("500.00"), subset_cap=500,
        )
        assert report.groups == []
        assert len(report.dense_windows) >= 1
        assert any("dense window" in s for s in report.info_signals)
        assert any("1,925" in s for s in report.info_signals)

    def test_just_under_cap_still_enumerates(self):
        loan = _loan()
        # 10 legs in window → 375 subsets < 500; proposer runs normally.
        legs = [
            FixmeLeg(
                f"h{i}",
                date(2026, 3, 1) + timedelta(days=i % 5),
                Decimal("50.00") * (i + 1),
            )
            for i in range(10)
        ]
        # Target the 3-leg sum (50+100+150=300).
        report = propose_groups(
            loan, legs, Decimal("300.00"), subset_cap=500,
        )
        assert report.dense_windows == []
        # At least one match exists (50+100+150 = 300).
        assert len(report.groups) >= 1


class TestInFlightPath:
    def test_default_shape(self):
        assert in_flight_path_for({"slug": "M", "entity_slug": "P"}) == (
            "Assets:P:InFlight:Loans:M"
        )

    def test_missing_entity_falls_back(self):
        assert in_flight_path_for({"slug": "M"}) == (
            "Assets:Personal:InFlight:Loans:M"
        )


# ----------------------------------------------------------- apply_group


class _FakeOverrideWriter:
    def __init__(self):
        self.calls = []

    def append_split(self, **kwargs):
        self.calls.append(kwargs)
        return ""


class TestApplyGroup:
    def _members(self):
        return [
            MemberPosting("h1", date(2026, 3, 1), Decimal("2500.00"), "Expenses:FIXME:Chk"),
            MemberPosting("h2", date(2026, 3, 2), Decimal("800.00"), "Expenses:FIXME:Chk"),
            MemberPosting("h3", date(2026, 3, 3), Decimal("200.00"), "Expenses:FIXME:Sav"),
        ]

    def test_balanced_writes(self):
        loan = _loan()
        members = self._members()
        writer = _FakeOverrideWriter()
        splits = [
            ("Liabilities:Personal:Main", Decimal("2200.00")),
            ("Expenses:Personal:Main:Interest", Decimal("1100.00")),
            ("Assets:Personal:Main:Escrow", Decimal("200.00")),
        ]
        result = apply_group(
            loan,
            group_id="abc",
            members=members,
            primary_hash="h1",
            primary_splits=splits,
            in_flight_path="Assets:Personal:InFlight:Loans:Main",
            writer=writer,
        )
        assert result.blocks_written == 3
        assert len(writer.calls) == 3

        # Non-primary blocks route to in-flight with positive amounts.
        member_blocks = [c for c in writer.calls if c["extra_meta"]["lamella-loan-group-role"] == "member"]
        assert len(member_blocks) == 2
        for c in member_blocks:
            assert len(c["splits"]) == 1
            acct, amt = c["splits"][0]
            assert acct == "Assets:Personal:InFlight:Loans:Main"
            assert amt > 0

        # Primary block has the real splits plus a negative in-flight leg.
        primary_block = next(c for c in writer.calls if c["extra_meta"]["lamella-loan-group-role"] == "primary")
        in_flight_legs = [
            amt for acct, amt in primary_block["splits"]
            if acct == "Assets:Personal:InFlight:Loans:Main"
        ]
        assert in_flight_legs == [Decimal("-1000.00")]  # = non-primary sum

    def test_in_flight_nets_to_zero(self):
        """The whole point: across the chain, in-flight balances to $0."""
        writer = _FakeOverrideWriter()
        apply_group(
            _loan(),
            group_id="abc",
            members=self._members(),
            primary_hash="h1",
            primary_splits=[
                ("Liabilities:Personal:Main", Decimal("2200.00")),
                ("Expenses:Personal:Main:Interest", Decimal("1100.00")),
                ("Assets:Personal:Main:Escrow", Decimal("200.00")),
            ],
            in_flight_path="Assets:Personal:InFlight:Loans:Main",
            writer=writer,
        )
        net = Decimal("0")
        for c in writer.calls:
            for acct, amt in c["splits"]:
                if acct == "Assets:Personal:InFlight:Loans:Main":
                    net += amt
        assert net == Decimal("0")

    def test_rejects_empty_members(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            apply_group(_loan(), group_id="x", members=[], primary_hash="h",
                        primary_splits=[("Acct", Decimal("1"))],
                        in_flight_path="P", writer=_FakeOverrideWriter())

    def test_rejects_single_member(self):
        members = [self._members()[0]]
        with pytest.raises(ValueError, match="at least 2 members"):
            apply_group(_loan(), group_id="x", members=members,
                        primary_hash="h1",
                        primary_splits=[("Acct", Decimal("2500"))],
                        in_flight_path="P", writer=_FakeOverrideWriter())

    def test_rejects_primary_not_in_members(self):
        with pytest.raises(ValueError, match="not in members"):
            apply_group(_loan(), group_id="x", members=self._members(),
                        primary_hash="unknown",
                        primary_splits=[("Acct", Decimal("3500"))],
                        in_flight_path="P", writer=_FakeOverrideWriter())

    def test_rejects_split_mismatch(self):
        # Primary splits total 3000 — aggregate is 3500 — rejected.
        with pytest.raises(ValueError, match="does not match group aggregate"):
            apply_group(
                _loan(), group_id="x", members=self._members(),
                primary_hash="h1",
                primary_splits=[("A", Decimal("3000.00"))],
                in_flight_path="P", writer=_FakeOverrideWriter(),
            )

    def test_stamps_group_meta_on_every_block(self):
        writer = _FakeOverrideWriter()
        apply_group(
            _loan(), group_id="gid123",
            members=self._members(),
            primary_hash="h1",
            primary_splits=[
                ("Liabilities:Personal:Main", Decimal("2200.00")),
                ("Expenses:Personal:Main:Interest", Decimal("1100.00")),
                ("Assets:Personal:Main:Escrow", Decimal("200.00")),
            ],
            in_flight_path="Assets:Personal:InFlight:Loans:Main",
            writer=writer,
        )
        for c in writer.calls:
            assert c["extra_meta"]["lamella-loan-group-id"] == "gid123"
            assert c["extra_meta"]["lamella-loan-group-members"] == "h1,h2,h3"
            assert c["extra_meta"]["lamella-loan-slug"] == "Main"


# ----------------------------------------------------------- reader roundtrip


def _txn(d, meta, postings):
    return Transaction(
        meta=meta, date=d, flag="*", payee=None, narration="t",
        tags=frozenset(), links=frozenset(), postings=postings,
    )


def _post(acct, n):
    return Posting(
        account=acct,
        units=Amount(Decimal(str(n)), "USD"),
        cost=None, price=None, flag=None, meta={},
    )


class TestReader:
    def test_roundtrip_confirmed_group(self):
        members_csv = "h1,h2,h3"
        common = {
            "lamella-loan-slug": "Main",
            "lamella-loan-group-id": "abc1234567890def",
            "lamella-loan-group-members": members_csv,
        }
        inflight = "Assets:Personal:InFlight:Loans:Main"
        entries = [
            _txn(date(2026, 3, 2),
                 {**common, "lamella-override-of": "h2", "lamella-loan-group-role": "member"},
                 [_post("Expenses:FIXME:Chk", -800), _post(inflight, 800)]),
            _txn(date(2026, 3, 3),
                 {**common, "lamella-override-of": "h3", "lamella-loan-group-role": "member"},
                 [_post("Expenses:FIXME:Sav", -200), _post(inflight, 200)]),
            _txn(date(2026, 3, 1),
                 {**common, "lamella-override-of": "h1", "lamella-loan-group-role": "primary"},
                 [_post("Expenses:FIXME:Chk", -2500),
                  _post(inflight, -1000),
                  _post("Liabilities:Personal:Main", 2200),
                  _post("Expenses:Personal:Main:Interest", 1100),
                  _post("Assets:Personal:Main:Escrow", 200)]),
        ]
        rows = read_loan_payment_groups(entries)
        assert len(rows) == 1
        row = rows[0]
        assert row["group_id"] == compute_group_id(["h1", "h2", "h3"])
        assert row["member_hashes"] == "h1,h2,h3"
        assert row["loan_slug"] == "Main"
        assert row["primary_hash"] == "h1"
        # Aggregate = sum of positive legs on the primary block.
        assert Decimal(row["aggregate_amount"]) == Decimal("3500")
        # Span covers the earliest + latest member.
        assert row["date_span_start"] == "2026-03-01"
        assert row["date_span_end"] == "2026-03-03"

    def test_non_group_txns_ignored(self):
        # A transaction without lamella-loan-group-members is not grouped.
        entries = [
            _txn(date(2026, 3, 1),
                 {"lamella-override-of": "h1"},
                 [_post("A", -100), _post("B", 100)]),
        ]
        assert read_loan_payment_groups(entries) == []

    def test_single_member_list_ignored(self):
        # Degenerate 1-member group (malformed meta) is dropped rather
        # than reconstructed as a bogus group.
        entries = [
            _txn(date(2026, 3, 1),
                 {"lamella-loan-slug": "M", "lamella-loan-group-members": "h1"},
                 [_post("A", -100), _post("B", 100)]),
        ]
        assert read_loan_payment_groups(entries) == []


# --------------------------------------------------------------- from_transactions


class TestFromTransactions:
    def test_extracts_fixme_legs_with_liability_flag(self):
        entries = [
            # FIXME on liability-adjacent txn
            _txn(date(2026, 3, 1), {},
                 [_post("Assets:Chk", -2500),
                  _post("Expenses:FIXME:Chk", 2500),
                  _post("Liabilities:Personal:Main", 0)]),
            # FIXME without liability
            _txn(date(2026, 3, 2), {},
                 [_post("Assets:Chk", -100),
                  _post("Expenses:FIXME:Chk", 100)]),
            # No FIXME — should be filtered
            _txn(date(2026, 3, 3), {},
                 [_post("Assets:Chk", -50), _post("Expenses:Food", 50)]),
        ]
        legs = from_transactions(
            entries, "Expenses:FIXME", "Liabilities:Personal:Main",
        )
        assert len(legs) == 2
        amounts = sorted(l.amount for l in legs)
        assert amounts == [Decimal("100"), Decimal("2500")]
        # First one touches liability, second does not.
        by_amt = {l.amount: l for l in legs}
        assert by_amt[Decimal("2500")].touches_liability is True
        assert by_amt[Decimal("100")].touches_liability is False
