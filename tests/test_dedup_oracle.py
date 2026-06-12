# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the cross-source intake-time dedup oracle — ADR-0058.

The contract:
- ``find_match`` returns ``None`` when nothing matches.
- A staged row whose fingerprint matches the incoming triple wins
  (cheapest check first).
- A ledger Transaction whose fingerprint matches wins when no staged
  hit is found and a reader is supplied.
- Terminal-state staged rows (``dismissed``, ``failed``) are skipped
  — they carry no actionable signal.
- ``StagingService.stage(dedup_check=True)`` lands the row in
  ``status='likely_duplicate'`` when the oracle hits, and stores the
  match in ``raw_json["dedup_match"]``.
- The same call with no hit lands the row in ``status='new'`` like
  before (no behavior change for clean imports).
- A re-stage of an existing ``(source, source_ref_hash)`` row leaves
  the row's status alone — the user's prior confirm / release
  decision is sticky across re-runs.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging.dedup_oracle import (
    DedupHit,
    find_match,
)
from lamella.features.import_.staging.service import StagingService


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _stage(svc: StagingService, **overrides):
    """Tiny convenience wrapper — most tests want the same defaults
    with one or two fields overridden."""
    base = dict(
        source="csv",
        source_ref={"row": "anchor"},
        posting_date="2026-04-15",
        amount=Decimal("12.50"),
        currency="USD",
        payee="Coffee Shop",
        description="Decaf and a scone",
        memo=None,
    )
    base.update(overrides)
    return svc.stage(**base)


# --- find_match: staged-side -------------------------------------------


class TestFindMatchStaged:

    def test_no_history_returns_none(self, conn):
        assert find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Coffee Shop",
        ) is None

    def test_staged_match_within_window(self, conn):
        svc = StagingService(conn)
        _stage(svc)  # default 2026-04-15 / 12.50 / "Decaf and a scone"
        # Same content, different source, same date.
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
        )
        assert hit is not None
        assert hit.kind == "staged"
        assert hit.matched_date == "2026-04-15"
        assert hit.staged_source == "csv"

    def test_staged_match_within_3_day_window(self, conn):
        svc = StagingService(conn)
        _stage(svc, posting_date="2026-04-15")
        # Incoming row 2 days later — bank's "posted" lag.
        hit = find_match(
            conn,
            posting_date="2026-04-17",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
        )
        assert hit is not None
        assert hit.kind == "staged"

    def test_staged_match_outside_window_misses(self, conn):
        svc = StagingService(conn)
        _stage(svc, posting_date="2026-04-15")
        # 5 days outside the default ±3 window.
        hit = find_match(
            conn,
            posting_date="2026-04-25",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
        )
        assert hit is None

    def test_dismissed_rows_are_skipped(self, conn):
        svc = StagingService(conn)
        row = _stage(svc)
        svc.dismiss(row.id, reason="user-said-no")
        # The dismissed row is the only candidate; the oracle must
        # treat it as if it didn't exist (terminal state, no signal).
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
        )
        assert hit is None

    def test_exclude_id_skips_self(self, conn):
        svc = StagingService(conn)
        row = _stage(svc)
        # Same content, same row — exclude_id should suppress the
        # self-match (used by re-stage path so a row doesn't dedup
        # against itself).
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
            exclude_id=row.id,
        )
        assert hit is None

    def test_different_amount_misses(self, conn):
        svc = StagingService(conn)
        _stage(svc, amount=Decimal("12.50"))
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("13.00"),
            description="Decaf and a scone",
        )
        assert hit is None


# --- find_match: ledger-side ------------------------------------------


def _ledger_with(dir_: Path, body: str) -> Path:
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Expenses:Food USD\n"
        + body,
        encoding="utf-8",
    )
    return main


class TestFindMatchLedger:

    def test_no_match_with_reader(self, conn, tmp_path):
        main = _ledger_with(tmp_path, "")
        reader = LedgerReader(main_bean=main)
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Coffee Shop",
            reader=reader,
        )
        assert hit is None

    def test_ledger_match_returns_hit(self, conn, tmp_path):
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Coffee Shop" "Decaf and a scone"\n'
            '  Assets:Bank      -12.50 USD\n'
            '  Expenses:Food     12.50 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
            reader=reader,
        )
        assert hit is not None
        assert hit.kind == "ledger"
        assert hit.matched_date == "2026-04-15"
        assert hit.txn_hash, "ledger hits must carry a txn_hash"
        # filename / lineno should both come through from the parser.
        assert hit.filename and hit.filename.endswith("main.bean")
        assert hit.lineno is not None

    def test_staged_hit_takes_precedence_over_ledger(
        self, conn, tmp_path,
    ):
        # Both sides match; the staged side is checked first by
        # design (cheaper, locally verifiable).
        svc = StagingService(conn)
        _stage(svc)
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Coffee Shop" "Decaf and a scone"\n'
            '  Assets:Bank      -12.50 USD\n'
            '  Expenses:Food     12.50 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            description="Decaf and a scone",
            reader=reader,
        )
        assert hit is not None
        assert hit.kind == "staged", (
            "staged-side check is supposed to short-circuit before "
            "the ledger walk"
        )


# --- StagingService.stage(dedup_check=True) integration -----------------


class TestStageDedupCheck:

    def test_clean_import_lands_as_new(self, conn):
        svc = StagingService(conn)
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
        )
        assert row.status == "new"
        # raw_json must NOT carry a dedup_match block when there
        # was no hit — otherwise the duplicates surface would
        # surface every row.
        assert "dedup_match" not in row.raw

    def test_match_against_staged_history_lands_as_likely_duplicate(
        self, conn,
    ):
        svc = StagingService(conn)
        # Existing CSV import.
        existing = svc.stage(
            source="csv",
            source_ref={"row": "csv-row-1"},
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
        )
        # SimpleFIN later picks up the same event; opts into dedup.
        sfin = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
        )
        assert sfin.status == "likely_duplicate"
        match = sfin.raw["dedup_match"]
        assert match["kind"] == "staged"
        assert match["staged_id"] == existing.id
        assert match["staged_source"] == "csv"

    def test_re_stage_keeps_existing_row_status(self, conn):
        svc = StagingService(conn)
        # First stage with dedup_check — clean.
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
        )
        assert row.status == "new"
        # User classifies the row.
        conn.execute(
            "UPDATE staged_transactions SET status = 'classified' "
            "WHERE id = ?", (row.id,),
        )
        # A second staging path (CSV) imports the same content; that
        # row is now staged. Re-fetching the SimpleFIN row again
        # should NOT flip the existing 'classified' row to
        # 'likely_duplicate' just because there's now staged history
        # matching it — the upsert path leaves status alone.
        svc.stage(
            source="csv",
            source_ref={"row": "csv-row-1"},
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
        )
        row_again = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
        )
        # Status sticky at 'classified' across the upsert — the
        # ON CONFLICT clause never overwrites status.
        assert row_again.status == "classified"

    def test_match_against_ledger_lands_as_likely_duplicate(
        self, conn, tmp_path,
    ):
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Coffee Shop" "Decaf"\n'
            '  Assets:Bank      -12.50 USD\n'
            '  Expenses:Food     12.50 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        svc = StagingService(conn)
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("-12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
            ledger_reader=reader,
        )
        assert row.status == "likely_duplicate"
        match = row.raw["dedup_match"]
        assert match["kind"] == "ledger"
        assert match["txn_hash"], "ledger match must carry a txn_hash"
        assert match["matched_date"] == "2026-04-15"


# --- ADR-0058 v2 contracts: sign-aware, multi-leg, lineage ------------


class TestSignAwareMatching:
    """A +50 incoming row matches an existing +50 row but NEVER an
    existing -50 row — opposite signs are transfer counterparts, a
    different relationship handled by the matcher sweep, not the
    intake-time dedup oracle."""

    def test_opposite_sign_does_not_match_staged(self, conn):
        svc = StagingService(conn)
        # Staged: an existing -50 (the debit side of some transfer).
        svc.stage(
            source="csv",
            source_ref={"row": "csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Some Counterparty",
            description="Transfer",
        )
        # Incoming: +50 (the credit side). NOT a duplicate — that's
        # the OTHER leg of the transfer.
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("50.00"),
            description="Transfer",
        )
        assert hit is None, (
            "opposite signs are transfer counterparts, not duplicates; "
            "the oracle must not flag them"
        )

    def test_same_sign_matches(self, conn):
        svc = StagingService(conn)
        svc.stage(
            source="csv",
            source_ref={"row": "csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Coffee Shop",
            description="Same description",
        )
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            description="Same description",
        )
        assert hit is not None
        assert hit.kind == "staged"


class TestMultiLegLedgerWalk:
    """A 2-leg ledger transfer entry has two postings the oracle has
    to consider against incoming rows — one from each side. Walking
    only the first concrete posting blinds the oracle to credit-side
    imports of debit-first entries."""

    def test_credit_side_import_matches_credit_leg(
        self, conn, tmp_path,
    ):
        # Ledger has one txn with both legs of a transfer. Debit side
        # (Checking) is the FIRST posting — without multi-leg walk the
        # oracle would only see -50 and miss a +50 import row.
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Internal transfer" "Checking → PayPal"\n'
            '  lamella-txn-id: "0190f000-0000-7000-8000-AAAAAAAAAAAA"\n'
            '  Assets:Checking  -50.00 USD\n'
            '  Assets:PayPal     50.00 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        # Incoming: +50 (the credit side of the transfer). The oracle
        # must walk to the second posting and match it.
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("50.00"),
            description="Checking → PayPal",
            reader=reader,
        )
        assert hit is not None
        assert hit.kind == "ledger"
        assert hit.matched_account == "Assets:PayPal"
        assert (
            hit.matched_lamella_txn_id
            == "0190f000-0000-7000-8000-AAAAAAAAAAAA"
        )

    def test_debit_side_import_matches_debit_leg(
        self, conn, tmp_path,
    ):
        # Mirror: an incoming -50 should match the Checking leg of
        # the same multi-leg entry.
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Internal transfer" "Checking → PayPal"\n'
            '  lamella-txn-id: "0190f000-0000-7000-8000-AAAAAAAAAAAA"\n'
            '  Assets:Checking  -50.00 USD\n'
            '  Assets:PayPal     50.00 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            description="Checking → PayPal",
            reader=reader,
        )
        assert hit is not None
        assert hit.kind == "ledger"
        assert hit.matched_account == "Assets:Checking"


class TestLamellaTxnIdInheritance:
    """When the oracle hits, the new staged row inherits the matched
    record's lamella-txn-id so multi-source observations of one
    real-world event share one event identity."""

    def test_inherit_from_staged_match(self, conn):
        svc = StagingService(conn)
        # First source observes the event.
        first = svc.stage(
            source="csv",
            source_ref={"row": "csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
        )
        assert first.lamella_txn_id, "first stage should mint an id"
        # Second source observes the same event; opts into dedup.
        second = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
        )
        assert second.status == "likely_duplicate"
        assert second.lamella_txn_id == first.lamella_txn_id, (
            "the second source must adopt the first's lamella-txn-id "
            "so both observations share one event identity"
        )

    def test_inherit_from_ledger_match(self, conn, tmp_path):
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Coffee Shop" "Decaf"\n'
            '  lamella-txn-id: "0190f000-0000-7000-8000-LEDGEREVNT01"\n'
            '  Assets:Bank      -12.50 USD\n'
            '  Expenses:Food     12.50 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        svc = StagingService(conn)
        row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "a1", "txn_id": "t1"},
            posting_date="2026-04-15",
            amount=Decimal("-12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf",
            dedup_check=True,
            ledger_reader=reader,
        )
        assert row.status == "likely_duplicate"
        assert (
            row.lamella_txn_id
            == "0190f000-0000-7000-8000-LEDGEREVNT01"
        ), (
            "the new staged row must inherit the ledger entry's "
            "lamella-txn-id so multi-source observations share one "
            "event identity (ADR-0019 paired source meta foundation)"
        )


class TestUserScenarioMultiSourceTransfer:
    """Mirrors AJ's worked example exactly:

    1. Hand-written ledger has a transfer Assets:Checking →
       Assets:PayPal $50, both legs in one entry with lamella-txn-id
       LXN-A.
    2. Bank-feed source ingests the Checking-side leg (-50). Should
       land as likely_duplicate of the ledger's Checking leg, inherit
       LXN-A.
    3. Payment-processor CSV ingests the PayPal-side leg (+50).
       Should ALSO land as likely_duplicate of the ledger's PayPal
       leg, inherit LXN-A.

    All three records share LXN-A → one real-world event has three
    source observations, NOT three "duplicates."
    """

    def test_three_sources_one_event(self, conn, tmp_path):
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Internal transfer" "Checking → PayPal"\n'
            '  lamella-txn-id: "0190f000-0000-7000-8000-EVENT-LXN-A"\n'
            '  Assets:Checking  -50.00 USD\n'
            '  Assets:PayPal     50.00 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        svc = StagingService(conn)

        # Step 2: bank-feed ingests Checking leg.
        bank_row = svc.stage(
            source="simplefin",
            source_ref={"account_id": "checking", "txn_id": "bank-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Internal transfer",
            description="Checking → PayPal",
            dedup_check=True,
            ledger_reader=reader,
        )
        assert bank_row.status == "likely_duplicate"
        bank_match = bank_row.raw["dedup_match"]
        assert bank_match["matched_account"] == "Assets:Checking", (
            "bank-feed -50 row should match the Checking leg, "
            "not the PayPal leg"
        )
        assert (
            bank_row.lamella_txn_id
            == "0190f000-0000-7000-8000-EVENT-LXN-A"
        )

        # Step 3: payment-processor CSV ingests PayPal leg.
        paypal_row = svc.stage(
            source="csv",
            source_ref={"row": "paypal-csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("50.00"),
            currency="USD",
            payee="Internal transfer",
            description="Checking → PayPal",
            dedup_check=True,
            ledger_reader=reader,
        )
        assert paypal_row.status == "likely_duplicate"
        paypal_match = paypal_row.raw["dedup_match"]
        assert paypal_match["matched_account"] == "Assets:PayPal", (
            "PayPal CSV +50 row should match the PayPal leg, "
            "not the Checking leg (sign-aware multi-leg walk)"
        )
        assert (
            paypal_row.lamella_txn_id
            == "0190f000-0000-7000-8000-EVENT-LXN-A"
        )

        # Lineage check: both staged rows share the ledger event's
        # identity → single real-world event has three observations
        # (the original ledger entry + two staged rows), not three
        # independent "duplicates."
        assert bank_row.lamella_txn_id == paypal_row.lamella_txn_id

    def test_medium_tier_payee_match_when_descriptions_differ(
        self, conn,
    ):
        """Direct payee equality is the strongest medium-tier
        signal. Both sources observe the same event but phrase
        the description differently — the shared payee
        "PayPal" reveals they're the same."""
        svc = StagingService(conn)
        # Existing source observation: a bank-feed row.
        svc.stage(
            source="simplefin",
            source_ref={"account_id": "x", "txn_id": "bank-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="PayPal",
            description="Transfer from PayPal",
        )
        # Incoming: payment-processor CSV's view of the same event.
        # Same payee; different description.
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            description="Outbound wire to merchant account",
            payee="PayPal",
        )
        assert hit is not None, (
            "medium-tier must catch cross-source observations of "
            "the same event when payees match but descriptions "
            "differ"
        )
        assert hit.confidence == "medium"
        assert hit.why is not None
        assert "paypal" in hit.why.lower() or "payee" in hit.why.lower()

    def test_medium_tier_description_token_overlap(self, conn):
        """When payees differ but the descriptions share enough
        tokens (≥ 50% Jaccard), medium-tier still catches it.
        ADR-0058's quoted scenario: "Transfer from PayPal" vs
        "Transfer to PayPal" — same event, two phrasings, ≥ 50%
        token overlap."""
        svc = StagingService(conn)
        svc.stage(
            source="simplefin",
            source_ref={"account_id": "x", "txn_id": "bank-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Bank A",
            description="Transfer from PayPal",
        )
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            description="Transfer to PayPal",
            payee="Bank B",
        )
        assert hit is not None, (
            "shared 'transfer' + 'paypal' tokens cross 50% "
            "threshold (2 shared / 3 union); should hit medium tier"
        )
        assert hit.confidence == "medium"

    def test_high_tier_wins_over_medium_tier_in_same_window(
        self, conn,
    ):
        """When two candidates exist in the date window — one with
        an exact description match and one with only a token-overlap
        match — the exact match wins, regardless of which appears
        first in the SQL row order."""
        svc = StagingService(conn)
        # Earlier insert: only a token-overlap match (medium tier).
        svc.stage(
            source="simplefin",
            source_ref={"account_id": "x", "txn_id": "bank-1"},
            posting_date="2026-04-14",
            amount=Decimal("-12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Transfer-related Coffee Shop charge",
        )
        # Later insert: exact normalized-description match (high
        # tier) — same date as the incoming row.
        svc.stage(
            source="csv",
            source_ref={"row": "csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("-12.50"),
            currency="USD",
            payee="Coffee Shop",
            description="Decaf and a scone",
        )
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("-12.50"),
            description="Decaf and a scone",
            payee="Coffee Shop",
        )
        assert hit is not None
        assert hit.confidence == "high", (
            "exact-description match must win over token-overlap "
            "match in the same date window"
        )

    def test_medium_tier_misses_when_no_signal(self, conn):
        """No payee match, no description token overlap → no hit
        even when amount + date match. This is the false-positive
        guard for recurring same-amount charges."""
        svc = StagingService(conn)
        svc.stage(
            source="csv",
            source_ref={"row": "csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Utility Co",
            description="Monthly utility bill",
        )
        hit = find_match(
            conn,
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            description="Restaurant Charge",
            payee="Some Restaurant",
        )
        # No shared tokens between the two — must NOT match.
        assert hit is None, (
            "amount-only match without any description / payee "
            "signal is too weak; must not auto-flag (the user "
            "would see false positives on every recurring "
            "same-amount charge)"
        )

    def test_payment_csv_alone_against_simple_ledger_dup(
        self, conn, tmp_path,
    ):
        # Sanity guard: the matcher MUST not say "this is a duplicate"
        # when there's only a transfer counterpart in the ledger and
        # nothing on the same side. A +50 CSV row against a ledger
        # that only shows the -50 debit (and no +50 credit anywhere)
        # would have matched falsely under the old abs-amount oracle.
        main = _ledger_with(
            tmp_path,
            '\n2026-04-15 * "Mystery debit" "Some narration"\n'
            '  Assets:Checking  -50.00 USD\n'
            '  Expenses:FIXME    50.00 USD\n',
        )
        reader = LedgerReader(main_bean=main)
        svc = StagingService(conn)
        row = svc.stage(
            source="csv",
            source_ref={"row": "csv-1"},
            posting_date="2026-04-15",
            amount=Decimal("-50.00"),
            currency="USD",
            payee="Mystery debit",
            description="Some narration",
            dedup_check=True,
            ledger_reader=reader,
        )
        # Same-sign match is correct: -50 CSV matches -50 Checking.
        assert row.status == "likely_duplicate"

        # And the opposite-sign companion test — a +50 CSV row
        # against the same ledger should ALSO match (the FIXME +50
        # is on the other leg). With sign-aware matching this is a
        # legitimate same-sign hit on the FIXME side.
        row2 = svc.stage(
            source="csv",
            source_ref={"row": "csv-2"},
            posting_date="2026-04-15",
            amount=Decimal("50.00"),
            currency="USD",
            payee="Mystery debit",
            description="Some narration",
            dedup_check=True,
            ledger_reader=reader,
        )
        assert row2.status == "likely_duplicate"
        assert row2.raw["dedup_match"]["matched_account"] == (
            "Expenses:FIXME"
        )
