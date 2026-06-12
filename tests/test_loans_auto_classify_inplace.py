# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Auto-classify in-place migration — verifies that loan auto-classify
prefers the M→N in-place rewrite when it can, and falls back to the
override overlay when it can't.

The original WP6 surface routed every classify-write through
OverrideWriter.append_split, which leaves the FIXME line in the source
file with an override block layered on top. The migration to
rewrite_txn_postings cleans the FIXME away and stamps the
lamella-loan-autoclass-* audit meta on the txn header itself — same
audit signal location the WP8 sustained-overflow detector reads.

Tests cover:
  - in-place happy path: real ledger file gets rewritten, FIXME gone,
    audit meta stamped on header, no override block produced
  - audit meta survives the rewrite (lamella-loan-autoclass-tier readable)
  - fallback path: missing source file falls through to overlay
  - SQLite loan_autoclass_log row is written either way
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction
from beancount.core.number import D as Bn
from beancount.loader import load_file

from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.db import migrate
from lamella.features.loans.auto_classify import (
    ClassifyPlan,
    apply,
)


# --------------------------------------------------------------- fixtures


def _conn() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    return db


def _settings_for(ledger_dir: Path) -> SimpleNamespace:
    return SimpleNamespace(
        ledger_dir=ledger_dir,
        ledger_main=ledger_dir / "main.bean",
        connector_overrides_path=ledger_dir / "connector_overrides.bean",
    )


def _seed_ledger(ledger_dir: Path) -> tuple[Path, str]:
    """Create a real ledger with a FIXME mortgage payment. Returns
    (txns_path, target_hash)."""
    main = ledger_dir / "main.bean"
    txns = ledger_dir / "txns.bean"
    overrides = ledger_dir / "connector_overrides.bean"
    main.write_text(
        'option "title" "t"\n'
        'option "operating_currency" "USD"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        'include "txns.bean"\n'
        'include "connector_overrides.bean"\n'
        '2023-01-01 open Assets:Personal:WF:Checking USD\n'
        '2023-01-01 open Liabilities:Personal:Bank:Mortgage USD\n'
        '2023-01-01 open Expenses:Personal:Mortgage:Interest USD\n'
        '2023-01-01 open Expenses:FIXME USD\n',
        encoding="utf-8",
    )
    txns.write_text(
        '\n2024-03-01 * "Mortgage payment"\n'
        '  Assets:Personal:WF:Checking  -3500.00 USD\n'
        '  Expenses:FIXME                3500.00 USD\n',
        encoding="utf-8",
    )
    overrides.write_text("", encoding="utf-8")

    entries, _, _ = load_file(str(main))
    target = next(
        (e for e in entries
         if isinstance(e, Transaction) and e.narration == "Mortgage payment"),
        None,
    )
    assert target is not None
    return txns, txn_hash(target)


def _build_plan(splits: list[tuple[str, Decimal]]) -> ClassifyPlan:
    total = sum((a for _, a in splits), Decimal("0"))
    return ClassifyPlan(
        tier="exact",
        splits=splits,
        from_account="Expenses:FIXME",
        actual_total=total,
        expected_total=total,
        overflow_amount=Decimal("0"),
        overflow_dest=None,
        overflow_dest_source="default",
        decision_id="test-decision-uuid",
        narration_hint_escrow=False,
        skip_reason=None,
    )


# --------------------------------------------------------------- in-place


def test_apply_uses_in_place_when_source_file_in_ledger_dir(tmp_path, monkeypatch):
    """Happy path: a FIXME txn whose source file lives in ledger_dir
    gets rewritten in-place. Override overlay file stays empty.
    Audit meta lands on the txn header."""
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.features.receipts.linker.capture_bean_check",
        lambda main_bean: (0, ""),
    )
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check_vs_baseline",
        lambda main_bean, baseline: None,
    )

    ledger = tmp_path / "ledger"
    ledger.mkdir()
    txns_path, target_hash = _seed_ledger(ledger)
    settings = _settings_for(ledger)
    db = _conn()
    db.execute(
        "INSERT INTO loans (slug, loan_type, original_principal, "
        "funded_date, is_active, liability_account_path, "
        "interest_account_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("M", "mortgage", "300000", "2023-01-01", 1,
         "Liabilities:Personal:Bank:Mortgage",
         "Expenses:Personal:Mortgage:Interest"),
    )

    # Re-load entries so the txn carries real filename + lineno meta.
    entries, _, _ = load_file(str(settings.ledger_main))
    target_txn = next(
        e for e in entries
        if isinstance(e, Transaction) and e.narration == "Mortgage payment"
    )

    plan = _build_plan([
        ("Liabilities:Personal:Bank:Mortgage", Decimal("3000.00")),
        ("Expenses:Personal:Mortgage:Interest", Decimal("500.00")),
    ])

    apply(
        plan, target_txn,
        loan={"slug": "M", "entity_slug": "Personal"},
        settings=settings, reader=None, conn=db,
    )

    # The FIXME line is gone from txns.bean.
    body = txns_path.read_text(encoding="utf-8")
    assert "Expenses:FIXME" not in body
    # The split legs are present in the source file.
    assert "Liabilities:Personal:Bank:Mortgage  3000.00 USD" in body
    assert "Expenses:Personal:Mortgage:Interest  500.00 USD" in body
    # The audit meta got stamped on the txn header.
    assert 'lamella-loan-autoclass-tier: "exact"' in body
    assert 'lamella-loan-autoclass-decision-id: "test-decision-uuid"' in body
    # No override block was produced — overlay stayed empty.
    overlay = settings.connector_overrides_path.read_text(encoding="utf-8")
    assert "lamella-override" not in overlay
    # SQLite log row written.
    log_row = db.execute(
        "SELECT decision_id, tier FROM loan_autoclass_log WHERE txn_hash = ?",
        (target_hash,),
    ).fetchone()
    assert log_row is not None
    assert log_row["decision_id"] == "test-decision-uuid"
    assert log_row["tier"] == "exact"


def test_apply_falls_back_to_overlay_when_filename_outside_ledger(
    tmp_path, monkeypatch,
):
    """Fall-back path: if the txn's filename meta points outside
    ledger_dir (or doesn't exist), in-place refuses and the
    override overlay path takes over."""
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check",
        lambda main_bean: None,
    )
    monkeypatch.setattr(
        "lamella.features.receipts.linker.capture_bean_check",
        lambda main_bean: (0, ""),
    )
    monkeypatch.setattr(
        "lamella.features.receipts.linker.run_bean_check_vs_baseline",
        lambda main_bean, baseline: None,
    )

    ledger = tmp_path / "ledger"
    ledger.mkdir()
    _, _ = _seed_ledger(ledger)
    settings = _settings_for(ledger)
    db = _conn()
    db.execute(
        "INSERT INTO loans (slug, loan_type, original_principal, "
        "funded_date, is_active, liability_account_path) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("M", "mortgage", "300000", "2023-01-01", 1,
         "Liabilities:Personal:Bank:Mortgage"),
    )

    # Synthetic txn with a filename pointing outside the ledger_dir
    # (the typical synthetic-test shape: meta={"filename": "x", "lineno": 1}).
    txn = Transaction(
        meta={"filename": "x", "lineno": 1},
        date=date(2024, 3, 1), flag="*",
        payee=None, narration="Mortgage payment",
        tags=set(), links=set(),
        postings=[
            Posting(account="Assets:Personal:WF:Checking",
                    units=Amount(Bn("-3500.00"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Expenses:FIXME",
                    units=Amount(Bn("3500.00"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )

    plan = _build_plan([
        ("Liabilities:Personal:Bank:Mortgage", Decimal("3000.00")),
        ("Expenses:Personal:Mortgage:Interest", Decimal("500.00")),
    ])

    apply(
        plan, txn,
        loan={"slug": "M", "entity_slug": "Personal"},
        settings=settings, reader=None, conn=db,
    )

    # Source file untouched (no real file at "x").
    txns_body = (ledger / "txns.bean").read_text(encoding="utf-8")
    assert "Expenses:FIXME" in txns_body
    # Override block appeared in the overlay file.
    overlay_body = settings.connector_overrides_path.read_text(encoding="utf-8")
    assert "lamella-override-of" in overlay_body
    assert "lamella-loan-autoclass-tier" in overlay_body
