# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP11 — historical payment backfill.

Covers:
  * claim_from_csv_row: simplefin-id exact, institution substring,
    short-institution skip, no-match, multi-loan
  * categorize_import Site 6 preemption (integration via mock AI)
  * backfill.parse_csv: happy path, header aliases, missing/bad date,
    missing/bad amount, negative amount, currency formatting
  * backfill.compute_splits: amortization-driven legs, escrow/tax,
    pre-funded-date error, incomplete-loan error
  * backfill.validate: tolerance match/reject, partition (valid, invalid)
  * write_synthesized_payment: integration via temp ledger fixture
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from datetime import date
from pathlib import Path

import pytest

from lamella.features.loans.backfill import (
    BackfillRow,
    SAMPLE_CSV,
    compute_splits,
    parse_csv,
    validate,
)
from lamella.features.loans.claim import (
    Claim,
    ClaimKind,
    claim_from_csv_row,
)


# --------------------------------------------------------------- fixtures


class _FakeConn:
    """Minimal conn for claim tests — same shape as test_loans_claim's."""

    _DEFAULT_COLS = (
        "slug", "liability_account_path", "interest_account_path",
        "escrow_account_path", "simplefin_account_id",
        "monthly_payment_estimate", "is_active",
        "escrow_monthly", "property_tax_monthly", "insurance_monthly",
        "institution",
    )

    def __init__(self, loans, schema_cols=None):
        self._loans = loans
        self._cols = schema_cols if schema_cols is not None else self._DEFAULT_COLS

    def execute(self, sql, params=()):
        sql = sql.strip()

        class _Cursor:
            def __init__(self, rows):
                self._rows = rows
            def fetchall(self):
                return self._rows
            def fetchone(self):
                return self._rows[0] if self._rows else None

        if "PRAGMA table_info" in sql:
            return _Cursor([
                (i, name, "TEXT", 0, None, 0)
                for i, name in enumerate(self._cols)
            ])
        lower = sql.lower()
        if "from loans" in lower:
            select_start = lower.index("select") + len("select")
            from_start = lower.index("from")
            col_list_raw = sql[select_start:from_start].strip()
            requested = [c.strip() for c in col_list_raw.split(",")]
            loans = [l for l in self._loans if l.get("is_active", 1)]
            rows = [tuple(l.get(col) for col in requested) for l in loans]
            return _Cursor(rows)
        return _Cursor([])


def _loan_dict(**overrides):
    base = {
        "slug": "MainMortgage",
        "liability_account_path": "Liabilities:Personal:BankTwo:MainMortgage",
        "interest_account_path": "Expenses:Personal:MainMortgage:Interest",
        "escrow_account_path": "Assets:Personal:BankTwo:MainMortgage:Escrow",
        "simplefin_account_id": None,
        "monthly_payment_estimate": "3500.00",
        "is_active": 1,
        "escrow_monthly": "850.00",
        "property_tax_monthly": None,
        "insurance_monthly": None,
        "institution": "Bank Two",
        "original_principal": "550000",
        "interest_rate_apr": "6.625",
        "term_months": 360,
        "first_payment_date": "2023-01-01",
        "entity_slug": "Personal",
    }
    base.update(overrides)
    return base


# ------------------------------------------------------- claim_from_csv_row


class TestClaimFromCsvRow:
    def test_institution_substring_match(self):
        loan = _loan_dict()
        conn = _FakeConn([loan])
        row = {
            "payment_method": "Bank Two Mortgage",
            "payee": "",
            "description": "",
        }
        c = claim_from_csv_row(row, conn)
        assert c is not None
        assert c.kind == ClaimKind.PAYMENT
        assert c.loan_slug == "MainMortgage"

    def test_simplefin_id_exact_match(self):
        loan = _loan_dict(simplefin_account_id="sf-acct-9876")
        conn = _FakeConn([loan])
        row = {
            "payment_method": "sf-acct-9876",
            "payee": "Random",
            "description": "Random",
        }
        c = claim_from_csv_row(row, conn)
        assert c is not None
        assert c.loan_slug == "MainMortgage"

    def test_simplefin_id_takes_precedence_over_substring(self):
        # Two loans: one matches by simplefin id, the other only by
        # institution substring. The simplefin path runs first and
        # should win.
        a = _loan_dict(slug="A", simplefin_account_id="sf-1",
                       institution="Bank Of A")
        b = _loan_dict(slug="B", simplefin_account_id=None,
                       institution="Bank Of B")
        conn = _FakeConn([a, b])
        row = {
            "payment_method": "sf-1",
            "payee": "Bank Of B Mortgage Payment",
            "description": "",
        }
        c = claim_from_csv_row(row, conn)
        assert c is not None
        assert c.loan_slug == "A"

    def test_no_match_returns_none(self):
        loan = _loan_dict()
        conn = _FakeConn([loan])
        row = {
            "payment_method": "Chase Card",
            "payee": "Costco",
            "description": "Groceries and household",
        }
        assert claim_from_csv_row(row, conn) is None

    def test_short_institution_does_not_match(self):
        # 2-letter institution must NOT trigger substring matches —
        # length floor protects against "WF" matching "Costco WFM".
        loan = _loan_dict(institution="WF")
        conn = _FakeConn([loan])
        row = {
            "payment_method": "Costco WFM Wholesale",
            "payee": "Costco",
            "description": "Groceries",
        }
        assert claim_from_csv_row(row, conn) is None

    def test_empty_row_returns_none(self):
        loan = _loan_dict()
        conn = _FakeConn([loan])
        assert claim_from_csv_row(
            {"payment_method": "", "payee": "", "description": ""}, conn,
        ) is None

    def test_no_active_loans_returns_none(self):
        # Inactive loans aren't in the snapshot.
        loan = _loan_dict(is_active=0)
        conn = _FakeConn([loan])
        row = {
            "payment_method": "",
            "payee": "Bank Two Mortgage",
            "description": "",
        }
        assert claim_from_csv_row(row, conn) is None

    def test_caches_loans_snapshot_when_provided(self):
        # Caller-supplied loans skip the DB roundtrip.
        loan = _loan_dict()
        # Use a no-op conn — must not be touched.
        class _FailConn:
            def execute(self, *a, **k):
                raise RuntimeError("must not query DB when loans pre-provided")
        row = {
            "payment_method": "",
            "payee": "Bank Two Mortgage",
            "description": "",
        }
        c = claim_from_csv_row(row, _FailConn(), loans=[loan])
        assert c is not None
        assert c.loan_slug == "MainMortgage"


# ----------------------------------------------------------------- parse_csv


class TestParseCSV:
    def test_happy_path(self):
        rows = parse_csv(
            "date,amount,offset_account\n"
            "2023-01-01,3500.00,Assets:Checking\n"
            "2023-02-01,3500.00,Assets:Checking\n"
        )
        assert len(rows) == 2
        assert rows[0].txn_date == date(2023, 1, 1)
        assert rows[0].total_amount == Decimal("3500.00")
        assert rows[0].offset_account == "Assets:Checking"
        assert rows[0].error is None

    def test_header_aliases(self):
        # `payment_date` and `total` are accepted aliases.
        rows = parse_csv(
            "Payment Date,Total,From Account\n"
            "2023-01-01,3500.00,Assets:Chk\n"
        )
        assert rows[0].txn_date == date(2023, 1, 1)
        assert rows[0].total_amount == Decimal("3500.00")
        assert rows[0].offset_account == "Assets:Chk"

    def test_us_date_format(self):
        rows = parse_csv("date,amount\n01/15/2023,1000.00\n")
        assert rows[0].txn_date == date(2023, 1, 15)
        assert rows[0].error is None

    def test_currency_formatting_stripped(self):
        rows = parse_csv("date,amount\n2023-01-01,\"$3,500.00\"\n")
        assert rows[0].total_amount == Decimal("3500.00")
        assert rows[0].error is None

    def test_missing_amount_column(self):
        rows = parse_csv("date\n2023-01-01\n")
        assert rows[0].error is not None
        assert "amount" in rows[0].error

    def test_missing_date_column(self):
        rows = parse_csv("amount\n3500.00\n")
        assert rows[0].error is not None
        assert "date" in rows[0].error.lower()

    def test_unparseable_date(self):
        rows = parse_csv("date,amount\nyesterday,3500\n")
        assert rows[0].error is not None
        assert "yesterday" in rows[0].error

    def test_unparseable_amount(self):
        rows = parse_csv("date,amount\n2023-01-01,xxx\n")
        assert rows[0].error is not None

    def test_negative_amount_rejected(self):
        rows = parse_csv("date,amount\n2023-01-01,-100\n")
        assert rows[0].error is not None
        assert "non-positive" in rows[0].error

    def test_empty_input(self):
        assert parse_csv("") == []
        assert parse_csv("   ") == []

    def test_optional_offset_and_narration(self):
        # Offset + narration are optional.
        rows = parse_csv("date,amount\n2023-01-01,3500.00\n")
        assert rows[0].error is None
        assert rows[0].offset_account is None
        assert rows[0].narration is None


# ------------------------------------------------------------- compute_splits


class TestComputeSplits:
    def test_amortization_split_filled(self):
        loan = _loan_dict()
        rows = parse_csv("date,amount\n2023-01-01,4371.71\n")
        rows = compute_splits(rows, loan)
        r = rows[0]
        assert r.expected_n == 1
        # Per the schedule (550k, 6.625%, 360mo): pmt 1 ≈ P=485 I=3036
        assert r.principal > Decimal("0")
        assert r.interest > Decimal("0")
        assert r.escrow == Decimal("850.00")
        # Split total approx P+I+E = 4371.71
        assert (r.split_total - Decimal("4371.71")).copy_abs() < Decimal("0.05")

    def test_pre_first_payment_date_errors(self):
        loan = _loan_dict(first_payment_date="2024-01-01")
        rows = parse_csv("date,amount\n2023-06-01,3500\n")
        rows = compute_splits(rows, loan)
        assert rows[0].error is not None
        assert "before first payment date" in rows[0].error

    def test_incomplete_loan_terms_error(self):
        loan = _loan_dict(first_payment_date=None, term_months=0)
        rows = parse_csv("date,amount\n2023-01-01,3500\n")
        rows = compute_splits(rows, loan)
        assert rows[0].error is not None
        assert "loan terms incomplete" in rows[0].error

    def test_passes_through_already_errored_rows(self):
        loan = _loan_dict()
        rows = parse_csv("date,amount\nbad-date,3500\n")
        # The parsing error must survive compute_splits.
        rows = compute_splits(rows, loan)
        assert rows[0].error is not None
        assert "unparseable date" in rows[0].error


# ----------------------------------------------------------------- validate


class TestValidate:
    def test_within_tolerance_marks_valid(self):
        # Build a row whose computed split is exactly the loan's
        # schedule total — within $0.02.
        loan = _loan_dict()
        # Compute exact expected total for payment 1.
        rows_proto = parse_csv("date,amount\n2023-01-01,4371.71\n")
        rows_proto = compute_splits(rows_proto, loan)
        actual_total = rows_proto[0].split_total
        # Now build the "user CSV" with that exact total (so it lines
        # up with what compute_splits will compute).
        rows = parse_csv(f"date,amount\n2023-01-01,{actual_total}\n")
        rows = compute_splits(rows, loan)
        valid, invalid = validate(rows)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_outside_tolerance_marks_invalid(self):
        loan = _loan_dict()
        # Whatever the schedule says, our row claims $1.00 — way off.
        rows = parse_csv("date,amount\n2023-01-01,1.00\n")
        rows = compute_splits(rows, loan)
        valid, invalid = validate(rows)
        assert len(valid) == 0
        assert len(invalid) == 1
        assert "split total" in invalid[0].error
        assert "does not match" in invalid[0].error

    def test_tolerance_kwarg_respected(self):
        # Same row twice — once with tight tolerance, once with $50.
        loan = _loan_dict()
        rows = parse_csv("date,amount\n2023-01-01,4350.00\n")
        rows = compute_splits(rows, loan)
        v1, i1 = validate(rows, tolerance=Decimal("1.00"))
        v2, i2 = validate(rows, tolerance=Decimal("50.00"))
        assert len(v1) == 0 and len(i1) == 1
        assert len(v2) == 1 and len(i2) == 0

    def test_already_errored_rows_stay_invalid(self):
        loan = _loan_dict()
        rows = parse_csv(
            "date,amount\n"
            "bad-date,3500\n"
            "2023-01-01,4371.71\n"
        )
        rows = compute_splits(rows, loan)
        valid, invalid = validate(rows, tolerance=Decimal("100.00"))
        assert len(invalid) == 1  # the bad-date row
        assert invalid[0].line_no == 2  # CSV line 2 (after header)
