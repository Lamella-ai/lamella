# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP13 — revolving / HELOC support.

Covers:
  - reader/writer round-trip for is_revolving + credit_limit meta
  - reader handles legacy directives (no meta keys) → None / False
  - writer omits the meta keys when default values, stamps when set
  - discovery scanner defaults is_revolving=True for HELOC matches
  - reconstruct (step9) flows the new fields into SQLite
"""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import pytest
from beancount.core.data import Custom, Open
from beancount.loader import load_file

from lamella.core.db import migrate
from lamella.features.loans.reader import read_loans
from lamella.features.loans.writer import append_loan
from lamella.core.registry.discovery import discover_loan_candidates
from lamella.core.transform.steps.step9_loans import reconstruct_loans


# --------------------------------------------------------------- helpers


class _V:
    def __init__(self, v):
        self.value = v


def _custom(d, type_, args, meta=None):
    return Custom(
        meta=dict(meta or {}),
        date=d, type=type_,
        values=[_V(a) for a in args],
    )


def _open(account: str, d: date = date(2024, 1, 1)) -> Open:
    return Open(
        meta={"filename": "x.bean", "lineno": 1},
        date=d, account=account, currencies=["USD"], booking=None,
    )


# --------------------------------------------------------------- reader


def test_reader_parses_revolving_and_credit_limit():
    entries = [
        _custom(
            date(2024, 1, 1), "loan", ["HELOC1"],
            meta={
                "lamella-loan-type": "heloc",
                "lamella-loan-original-principal": "0",
                "lamella-loan-funded-date": date(2024, 1, 1),
                "lamella-loan-is-revolving": True,
                "lamella-loan-credit-limit": "100000.00",
            },
        ),
    ]
    rows = read_loans(entries)
    assert len(rows) == 1
    row = rows[0]
    assert row["slug"] == "HELOC1"
    assert row["is_revolving"] is True
    assert row["credit_limit"] == "100000.00"


def test_reader_legacy_loan_without_revolving_meta_returns_none():
    """Existing fixed-term loans don't have the new keys — reader
    yields None / None rather than crashing."""
    entries = [
        _custom(
            date(2024, 1, 1), "loan", ["MortgageOld"],
            meta={
                "lamella-loan-type": "mortgage",
                "lamella-loan-original-principal": "300000",
                "lamella-loan-funded-date": date(2020, 1, 1),
            },
        ),
    ]
    rows = read_loans(entries)
    assert len(rows) == 1
    row = rows[0]
    assert row["is_revolving"] is None
    assert row["credit_limit"] is None


# --------------------------------------------------------------- writer


def test_writer_round_trip_revolving_true(tmp_path: Path):
    main_bean = tmp_path / "main.bean"
    config = tmp_path / "connector_config.bean"
    main_bean.write_text(
        'option "title" "Test"\noption "operating_currency" "USD"\n'
        '2024-01-01 open Liabilities:Personal:Bank:HELOC1 USD\n'
    )
    append_loan(
        connector_config=config, main_bean=main_bean,
        slug="HELOC1", display_name=None, loan_type="heloc",
        entity_slug=None, institution=None,
        original_principal="0", funded_date="2024-01-01",
        liability_account_path="Liabilities:Personal:Bank:HELOC1",
        is_revolving=True, credit_limit="100000.00",
        run_check=False,
    )
    body = config.read_text()
    assert 'lamella-loan-is-revolving: TRUE' in body
    assert 'lamella-loan-credit-limit: "100000.00"' in body


def test_writer_omits_revolving_keys_when_defaults(tmp_path: Path):
    main_bean = tmp_path / "main.bean"
    config = tmp_path / "connector_config.bean"
    main_bean.write_text(
        'option "title" "Test"\noption "operating_currency" "USD"\n'
        '2024-01-01 open Liabilities:Personal:Bank:Mortgage USD\n'
    )
    append_loan(
        connector_config=config, main_bean=main_bean,
        slug="MortgageDefault", display_name=None, loan_type="mortgage",
        entity_slug=None, institution=None,
        original_principal="300000", funded_date="2024-01-01",
        liability_account_path="Liabilities:Personal:Bank:Mortgage",
        # is_revolving + credit_limit defaulted; nothing to write.
        run_check=False,
    )
    body = config.read_text()
    assert "lamella-loan-is-revolving" not in body
    assert "lamella-loan-credit-limit" not in body


# --------------------------------------------------------------- discovery


def test_discovery_defaults_heloc_to_revolving():
    entries = [
        _open("Liabilities:Personal:BankTwo:Mortgage"),
        _open("Liabilities:Personal:Wells:HELOC"),
    ]
    candidates = discover_loan_candidates(entries)
    by_path = {c["account_path"]: c for c in candidates}

    mortgage = by_path["Liabilities:Personal:BankTwo:Mortgage"]
    assert mortgage["loan_type"] == "mortgage"
    assert mortgage["is_revolving"] is False

    heloc = by_path["Liabilities:Personal:Wells:HELOC"]
    assert heloc["loan_type"] == "heloc"
    assert heloc["is_revolving"] is True


# --------------------------------------------------------------- reconstruct


def test_reconstruct_flows_revolving_into_sqlite(tmp_path: Path):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)

    entries = [
        _custom(
            date(2024, 1, 1), "loan", ["HELOC1"],
            meta={
                "lamella-loan-type": "heloc",
                "lamella-loan-original-principal": "0",
                "lamella-loan-funded-date": date(2024, 1, 1),
                "lamella-loan-is-revolving": True,
                "lamella-loan-credit-limit": "75000.00",
            },
        ),
    ]
    reconstruct_loans(db, entries)

    row = db.execute(
        "SELECT slug, is_revolving, credit_limit FROM loans WHERE slug = ?",
        ("HELOC1",),
    ).fetchone()
    assert row is not None
    assert row["is_revolving"] == 1
    assert row["credit_limit"] == "75000.00"
