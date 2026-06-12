# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP12 — forbearance / payment-pause windows.

Covers:
  - read_loan_pauses: roundtrip, tombstone semantics, last-seen-wins
  - coverage integration: paused months don't generate expected rows
  - late-fee leg shape in write_synthesized_payment
  - reconstruct: step23 rebuilds rows from ledger meta
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core.data import Custom

from lamella.features.loans.coverage import build_schedule, coverage_for
from lamella.features.loans.reader import read_loan_pauses


# --------------------------------------------------------------- helpers


class _V:
    """Stand-in for beancount Custom value tuples (which expose .value)."""
    def __init__(self, v):
        self.value = v


def _custom(d, type_, args, meta=None):
    return Custom(
        meta=dict(meta or {}),
        date=d, type=type_,
        values=[_V(a) for a in args],
    )


def _loan(**overrides):
    base = {
        "slug": "M",
        "loan_type": "mortgage",
        "entity_slug": "Personal",
        "institution": "Bank",
        "original_principal": "100000",
        "funded_date": "2025-01-01",
        "first_payment_date": "2025-02-01",
        "payment_due_day": 1,
        "term_months": 360,
        "interest_rate_apr": "5.0",
        "escrow_monthly": None,
        "property_tax_monthly": None,
        "insurance_monthly": None,
        "liability_account_path": "Liabilities:Personal:Bank:M",
        "interest_account_path": "Expenses:Personal:M:Interest",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------- read_loan_pauses


class TestReader:
    def test_roundtrip_open_pause(self):
        c = _custom(
            date(2024, 1, 1), "loan-pause", ["M1"],
            meta={"lamella-pause-reason": "covid", "lamella-pause-notes": "verify"},
        )
        rows = read_loan_pauses([c])
        assert len(rows) == 1
        assert rows[0]["loan_slug"] == "M1"
        assert rows[0]["start_date"] == "2024-01-01"
        assert rows[0]["end_date"] is None
        assert rows[0]["reason"] == "covid"

    def test_closed_pause_with_end_date(self):
        c = _custom(
            date(2024, 1, 1), "loan-pause", ["M1"],
            meta={
                "lamella-pause-end": date(2024, 6, 30),
                "lamella-pause-reason": "forbearance",
                "lamella-pause-accrued-interest": "1234.56",
            },
        )
        rows = read_loan_pauses([c])
        assert rows[0]["end_date"] == "2024-06-30"
        assert rows[0]["accrued_interest"] == "1234.56"

    def test_revoked_pause_dropped(self):
        c = _custom(
            date(2024, 1, 1), "loan-pause", ["M1"],
            meta={"lamella-pause-reason": "covid"},
        )
        revoke = _custom(
            date(2024, 8, 1), "loan-pause-revoked", ["M1"],
            meta={"lamella-pause-start": date(2024, 1, 1)},
        )
        rows = read_loan_pauses([c, revoke])
        assert rows == []

    def test_revoke_only_drops_matching_start(self):
        # Two pauses, only the first revoked.
        c1 = _custom(date(2024, 1, 1), "loan-pause", ["M1"],
                     meta={"lamella-pause-reason": "first"})
        c2 = _custom(date(2024, 7, 1), "loan-pause", ["M1"],
                     meta={"lamella-pause-reason": "second"})
        revoke = _custom(date(2024, 8, 1), "loan-pause-revoked", ["M1"],
                         meta={"lamella-pause-start": date(2024, 1, 1)})
        rows = read_loan_pauses([c1, c2, revoke])
        assert len(rows) == 1
        assert rows[0]["start_date"] == "2024-07-01"
        assert rows[0]["reason"] == "second"

    def test_last_seen_wins_per_start_date(self):
        # Two directives with the same (slug, start_date) — second wins.
        c1 = _custom(date(2024, 1, 1), "loan-pause", ["M1"],
                     meta={"lamella-pause-reason": "first version"})
        c2 = _custom(date(2024, 1, 1), "loan-pause", ["M1"],
                     meta={"lamella-pause-reason": "updated", "lamella-pause-end": date(2024, 6, 30)})
        rows = read_loan_pauses([c1, c2])
        assert len(rows) == 1
        assert rows[0]["reason"] == "updated"
        assert rows[0]["end_date"] == "2024-06-30"


# ---------------------------------------------------- coverage integration


class TestCoveragePauseSkip:
    def _pause(self, start, end=None):
        # Match build_schedule's getattr-based duck typing.
        from dataclasses import dataclass
        @dataclass(frozen=True)
        class _P:
            start_date: date
            end_date: date | None
        return _P(start_date=start, end_date=end)

    def test_paused_months_not_in_schedule(self):
        # Loan starts paying 2025-02-01, term 12 months. As-of
        # 2025-08-01 means 7 expected payments. Pausing March-May
        # (3 months) should reduce that to 4.
        loan = _loan(term_months=12, original_principal="100000")
        as_of = date(2025, 8, 1)
        no_pause = build_schedule(loan, as_of=as_of)
        paused = build_schedule(
            loan, as_of=as_of,
            pauses=[self._pause(date(2025, 3, 1), date(2025, 5, 31))],
        )
        assert len(no_pause) == 7
        assert len(paused) == 4
        # The skipped months are exactly March/April/May 2025.
        paused_dates = {r.expected_date for r in paused}
        assert date(2025, 3, 1) not in paused_dates
        assert date(2025, 4, 1) not in paused_dates
        assert date(2025, 5, 1) not in paused_dates

    def test_open_ended_pause_skips_until_today(self):
        # End_date None means "still active" — those rows should be
        # skipped through the as_of date.
        loan = _loan(term_months=12)
        as_of = date(2025, 8, 1)
        # Pause from May onwards, no end set.
        paused = build_schedule(
            loan, as_of=as_of,
            pauses=[self._pause(date(2025, 5, 1))],
        )
        # Feb, Mar, Apr expected (n=1..3); May+ paused.
        dates = {r.expected_date for r in paused}
        assert date(2025, 2, 1) in dates
        assert date(2025, 4, 1) in dates
        assert date(2025, 5, 1) not in dates
        assert date(2025, 6, 1) not in dates

    def test_pause_does_not_affect_unpaused_loans(self):
        # Sanity: empty pauses list = unchanged behavior.
        loan = _loan(term_months=12)
        as_of = date(2025, 8, 1)
        a = build_schedule(loan, as_of=as_of, pauses=())
        b = build_schedule(loan, as_of=as_of)
        assert len(a) == len(b)


# --------------------------------------------- write_synthesized_payment late-fee


class TestLateFeeLeg:
    def test_block_includes_late_fee_leg(self, tmp_path):
        from lamella.features.loans.writer import write_synthesized_payment
        # Minimal fake settings + ledger files.
        main_bean = tmp_path / "main.bean"
        overrides = tmp_path / "connector_overrides.bean"
        accounts = tmp_path / "connector_accounts.bean"
        # Bare ledger that opens the accounts we'll post to.
        main_bean.write_text(
            'option "operating_currency" "USD"\n'
            'plugin "beancount.plugins.auto_accounts"\n'
            'include "connector_overrides.bean"\n'
            'include "connector_accounts.bean"\n',
            encoding="utf-8",
        )
        # auto_accounts opens accounts on first use, so we don't
        # need explicit Opens in the test ledger.
        overrides.write_text("", encoding="utf-8")
        accounts.write_text("", encoding="utf-8")

        from types import SimpleNamespace
        settings = SimpleNamespace(
            ledger_main=main_bean,
            connector_overrides_path=overrides,
            connector_accounts_path=accounts,
        )
        loan = _loan(slug="LF", entity_slug="Personal",
                     liability_account_path="Liabilities:Personal:LF",
                     interest_account_path="Expenses:Personal:LF:Interest",
                     escrow_account_path="Assets:Personal:LF:Escrow")

        write_synthesized_payment(
            loan=loan, settings=settings,
            txn_date=date(2024, 5, 1),
            expected_n=4,
            principal=Decimal("500.00"),
            interest=Decimal("400.00"),
            escrow=Decimal("100.00"),
            late_fee=Decimal("35.00"),
            offset_account="Assets:Personal:Checking",
            run_check=False,  # avoid bean-check needing full ledger
        )
        text = overrides.read_text(encoding="utf-8")
        assert "Expenses:Personal:LF:LateFees" in text
        assert "35.00 USD" in text
        # Total = 500+400+100+35 = 1035; offset gets -1035.
        assert "-1035.00 USD" in text

    def test_late_fee_without_entity_raises(self, tmp_path):
        from lamella.features.loans.writer import write_synthesized_payment
        main_bean = tmp_path / "main.bean"
        overrides = tmp_path / "overrides.bean"
        accounts = tmp_path / "accounts.bean"
        main_bean.write_text("", encoding="utf-8")
        from types import SimpleNamespace
        settings = SimpleNamespace(
            ledger_main=main_bean,
            connector_overrides_path=overrides,
            connector_accounts_path=accounts,
        )
        loan = _loan(slug="LF2", entity_slug=None,
                     liability_account_path="Liabilities:Foo")
        with pytest.raises(ValueError, match="late_fee provided"):
            write_synthesized_payment(
                loan=loan, settings=settings,
                txn_date=date(2024, 5, 1),
                expected_n=1,
                principal=Decimal("100"),
                interest=Decimal("0"),
                late_fee=Decimal("25"),
                offset_account="Assets:Checking",
                run_check=False,
            )


# ---------------------------------------------------- pauses service module


class TestPausesService:
    def _conn(self):
        # Use the real migration pipeline to get loans + loan_pauses.
        from lamella.core.db import migrate, connect
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        conn = connect(path)
        migrate(conn)
        # Insert a loan we can attach pauses to.
        conn.execute(
            "INSERT INTO loans (slug, loan_type, original_principal, "
            "funded_date, is_active) VALUES (?, ?, ?, ?, ?)",
            ("M1", "mortgage", "100000", "2024-01-01", 1),
        )
        return conn

    def _settings(self, tmp_path):
        from types import SimpleNamespace
        main_bean = tmp_path / "main.bean"
        config = tmp_path / "connector_config.bean"
        main_bean.write_text(
            'option "operating_currency" "USD"\n'
            'include "connector_config.bean"\n',
            encoding="utf-8",
        )
        config.write_text("", encoding="utf-8")
        return SimpleNamespace(
            ledger_main=main_bean,
            connector_config_path=config,
        )

    def test_create_and_list_pause(self, tmp_path):
        from lamella.features.loans.pauses import create_pause, list_pauses

        conn = self._conn()
        settings = self._settings(tmp_path)
        p = create_pause(
            conn, settings=settings, loan_slug="M1",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 30),
            reason="forbearance",
            accrued_interest=Decimal("1500.00"),
        )
        assert p.start_date == date(2024, 1, 1)
        assert p.end_date == date(2024, 6, 30)
        assert p.accrued_interest == Decimal("1500.00")
        listed = list_pauses(conn, "M1")
        assert len(listed) == 1
        assert listed[0].id == p.id

    def test_create_collision_raises(self, tmp_path):
        from lamella.features.loans.pauses import PauseError, create_pause

        conn = self._conn()
        settings = self._settings(tmp_path)
        create_pause(conn, settings=settings, loan_slug="M1",
                     start_date=date(2024, 1, 1))
        with pytest.raises(PauseError) as exc_info:
            create_pause(conn, settings=settings, loan_slug="M1",
                         start_date=date(2024, 1, 1))
        assert exc_info.value.status == 409

    def test_end_pause_updates(self, tmp_path):
        from lamella.features.loans.pauses import (
            create_pause, end_pause, get_pause,
        )
        conn = self._conn()
        settings = self._settings(tmp_path)
        p = create_pause(
            conn, settings=settings, loan_slug="M1",
            start_date=date(2024, 1, 1),
        )
        end_pause(conn, settings=settings, pause_id=p.id,
                  end_date=date(2024, 6, 30))
        updated = get_pause(conn, p.id)
        assert updated.end_date == date(2024, 6, 30)

    def test_end_pause_already_ended_raises(self, tmp_path):
        from lamella.features.loans.pauses import (
            PauseError, create_pause, end_pause,
        )
        conn = self._conn()
        settings = self._settings(tmp_path)
        p = create_pause(
            conn, settings=settings, loan_slug="M1",
            start_date=date(2024, 1, 1), end_date=date(2024, 6, 30),
        )
        with pytest.raises(PauseError) as exc:
            end_pause(conn, settings=settings, pause_id=p.id,
                      end_date=date(2024, 12, 31))
        assert exc.value.status == 409

    def test_end_before_start_raises(self, tmp_path):
        from lamella.features.loans.pauses import (
            PauseError, create_pause, end_pause,
        )
        conn = self._conn()
        settings = self._settings(tmp_path)
        p = create_pause(
            conn, settings=settings, loan_slug="M1",
            start_date=date(2024, 6, 1),
        )
        with pytest.raises(PauseError) as exc:
            end_pause(conn, settings=settings, pause_id=p.id,
                      end_date=date(2024, 1, 1))
        assert exc.value.status == 400

    def test_delete_pause_drops_row_and_writes_tombstone(self, tmp_path):
        from lamella.features.loans.pauses import (
            create_pause, delete_pause, get_pause,
        )
        conn = self._conn()
        settings = self._settings(tmp_path)
        p = create_pause(
            conn, settings=settings, loan_slug="M1",
            start_date=date(2024, 1, 1),
        )
        delete_pause(conn, settings=settings, pause_id=p.id)
        assert get_pause(conn, p.id) is None
        # Tombstone present in the ledger.
        text = settings.connector_config_path.read_text(encoding="utf-8")
        assert 'custom "loan-pause-revoked" "M1"' in text
        assert "lamella-pause-start" in text


# ---------------------------------------------------- reconstruct (step23)


class TestReconstructStep23:
    def test_rebuilds_pause_rows_from_ledger(self):
        from lamella.core.db import migrate, connect
        from lamella.core.transform.steps.step23_loan_pauses import (
            reconstruct_loan_pauses,
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        conn = connect(path)
        migrate(conn)
        conn.execute(
            "INSERT INTO loans (slug, loan_type, original_principal, "
            "funded_date, is_active) VALUES (?, ?, ?, ?, ?)",
            ("M1", "mortgage", "100000", "2024-01-01", 1),
        )
        # Synthesize entries: one pause + a separate revoked pause.
        c1 = _custom(date(2024, 1, 1), "loan-pause", ["M1"],
                     meta={"lamella-pause-reason": "covid",
                           "lamella-pause-end": date(2024, 6, 30)})
        c2 = _custom(date(2024, 7, 1), "loan-pause", ["M1"],
                     meta={"lamella-pause-reason": "should-be-revoked"})
        revoke = _custom(date(2024, 8, 1), "loan-pause-revoked", ["M1"],
                         meta={"lamella-pause-start": date(2024, 7, 1)})
        report = reconstruct_loan_pauses(conn, [c1, c2, revoke])
        assert report.rows_written == 1
        rows = list(conn.execute(
            "SELECT loan_slug, start_date, end_date, reason "
            "FROM loan_pauses ORDER BY start_date"
        ))
        assert len(rows) == 1
        r = rows[0]
        assert r["loan_slug"] == "M1"
        assert r["start_date"] == "2024-01-01"
        assert r["end_date"] == "2024-06-30"
        assert r["reason"] == "covid"

    def test_idempotent_on_rerun(self):
        from lamella.core.db import migrate, connect
        from lamella.core.transform.steps.step23_loan_pauses import (
            reconstruct_loan_pauses,
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = Path(f.name)
        conn = connect(path)
        migrate(conn)
        conn.execute(
            "INSERT INTO loans (slug, loan_type, original_principal, "
            "funded_date, is_active) VALUES (?, ?, ?, ?, ?)",
            ("M1", "mortgage", "100000", "2024-01-01", 1),
        )
        c = _custom(date(2024, 1, 1), "loan-pause", ["M1"],
                    meta={"lamella-pause-reason": "hardship"})
        reconstruct_loan_pauses(conn, [c])
        reconstruct_loan_pauses(conn, [c])
        rows = list(conn.execute("SELECT * FROM loan_pauses"))
        assert len(rows) == 1
