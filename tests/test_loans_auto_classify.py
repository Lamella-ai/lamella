# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP6 — tier-based auto-classification."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Posting, Transaction
from beancount.loader import load_file

from lamella.core.db import connect, migrate
from lamella.features.loans import auto_classify
from lamella.features.loans.auto_classify import (
    ClassifyPlan,
    ProcessOutcome,
    TIER_EXACT_TOLERANCE,
    _compute_tier,
    plan,
    process,
)
from lamella.features.loans.claim import Claim, ClaimKind


# ---------------------------------------------------------------- fixtures


def _loan(**o) -> dict:
    base = {
        "slug": "M",
        "display_name": "Test",
        "entity_slug": "Personal",
        "institution": "Bank",
        "original_principal": "100000.00",
        "interest_rate_apr": "6.0",
        "term_months": 360,
        "first_payment_date": "2024-01-01",
        "monthly_payment_estimate": "599.55",
        "escrow_monthly": None,
        "property_tax_monthly": None,
        "insurance_monthly": None,
        "liability_account_path": "Liabilities:Personal:Bank:M",
        "interest_account_path": "Expenses:Personal:M:Interest",
        "escrow_account_path": None,
        "is_active": 1,
        "auto_classify_enabled": 1,
        "overflow_default": "bonus_principal",
    }
    base.update(o)
    return base


def _fixme_txn(d: date, amount: Decimal, narration: str = "Mortgage payment") -> Transaction:
    """Build a FIXME transaction — what lands in the ledger pre-split."""
    return Transaction(
        meta={"filename": "x", "lineno": 1},
        date=d, flag="*", payee=None, narration=narration,
        tags=set(), links=set(),
        postings=[
            Posting(account="Assets:Personal:Checking",
                    units=Amount(-amount, "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Expenses:Personal:FIXME",
                    units=Amount(amount, "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )


# ------------------------------------------------------------- tier compute


def test_tier_exact_within_2_cents():
    assert _compute_tier(Decimal("599.55"), Decimal("599.55")) == "exact"
    assert _compute_tier(Decimal("599.56"), Decimal("599.55")) == "exact"
    assert _compute_tier(Decimal("599.54"), Decimal("599.55")) == "exact"


def test_tier_over_within_50_percent():
    # $799 vs $599 = 33% over → "over".
    assert _compute_tier(Decimal("799"), Decimal("599")) == "over"


def test_tier_far_beyond_50_percent_on_high_side():
    # $1500 vs $599 = 150% over → "far".
    assert _compute_tier(Decimal("1500"), Decimal("599")) == "far"


def test_tier_under_within_50_percent():
    # $400 vs $599 = 33% under → "under".
    assert _compute_tier(Decimal("400"), Decimal("599")) == "under"


def test_tier_far_beyond_50_percent_on_low_side():
    # $100 vs $599 = 83% under → "far".
    assert _compute_tier(Decimal("100"), Decimal("599")) == "far"


def test_tier_far_when_expected_is_zero():
    """Defensive: expected=0 should never happen on a real loan, but
    if it slips through, we shouldn't div-by-zero."""
    assert _compute_tier(Decimal("500"), Decimal("0")) == "far"


# ----------------------------------------------------------------- plan()


def test_plan_exact_match_emits_correct_splits():
    """A $599.55 payment matches the model exactly for payment #3
    of a 100k/6%/360 loan. Principal + interest splits expected."""
    loan = _loan()
    # Payment #3 would be Jan 2024 + 2 months = March 2024.
    txn = _fixme_txn(date(2024, 3, 1), Decimal("599.55"))
    p = plan(txn, loan)
    assert p.tier == "exact"
    assert len(p.splits) == 2  # principal + interest
    assert p.from_account == "Assets:Personal:Checking"
    # Splits sum to the full amount.
    total = sum((amt for _, amt in p.splits), Decimal("0"))
    assert total == Decimal("599.55")


def test_plan_over_with_bonus_principal_folds_into_liability_leg():
    """$100 over-expected with overflow_default=bonus_principal:
    principal leg should grow by $100 (no separate overflow leg)."""
    loan = _loan(overflow_default="bonus_principal")
    txn = _fixme_txn(date(2024, 3, 1), Decimal("699.55"))  # $100 over
    p = plan(txn, loan)
    assert p.tier == "over"
    assert p.overflow_amount == Decimal("100")
    # Principal leg = model principal + $100 overflow; interest leg unchanged.
    liability_leg = next(
        amt for acct, amt in p.splits
        if acct == loan["liability_account_path"]
    )
    interest_leg = next(
        amt for acct, amt in p.splits
        if acct == loan["interest_account_path"]
    )
    # Model P for month 3 is roughly $100; with $100 bonus it's ~$200.
    assert liability_leg > Decimal("150")
    # Splits still sum to actual.
    total = sum((amt for _, amt in p.splits), Decimal("0"))
    assert total == Decimal("699.55")


def test_plan_over_with_bonus_escrow_folds_into_escrow_leg():
    loan = _loan(
        overflow_default="bonus_escrow",
        escrow_monthly="100.00",
        escrow_account_path="Assets:Personal:Bank:M:Escrow",
    )
    # Expected = principal + interest + escrow ≈ $699.55.
    # Pay $799.55 → $100 over.
    txn = _fixme_txn(date(2024, 3, 1), Decimal("799.55"))
    p = plan(txn, loan)
    assert p.tier == "over"
    escrow_leg = next(
        amt for acct, amt in p.splits
        if acct == "Assets:Personal:Bank:M:Escrow"
    )
    # Escrow = configured $100 + $100 overflow.
    assert escrow_leg == Decimal("200")


def test_plan_over_with_ask_downgrades_to_surface():
    """overflow_default='ask' means 'don't auto-route, prompt me.'
    plan() should NOT emit splits; caller surfaces."""
    loan = _loan(overflow_default="ask")
    txn = _fixme_txn(date(2024, 3, 1), Decimal("699.55"))
    p = plan(txn, loan)
    assert p.tier in ("under", "far")  # downgraded
    assert p.splits == []
    assert p.skip_reason and "ask" in p.skip_reason.lower()


def test_plan_under_skips_no_splits():
    loan = _loan()
    txn = _fixme_txn(date(2024, 3, 1), Decimal("500.00"))  # $99 under
    p = plan(txn, loan)
    assert p.tier == "under"
    assert p.splits == []
    assert p.skip_reason


def test_plan_far_skips_no_splits():
    loan = _loan()
    txn = _fixme_txn(date(2024, 3, 1), Decimal("2000.00"))  # wildly off
    p = plan(txn, loan)
    assert p.tier == "far"
    assert p.splits == []


def test_plan_narration_hint_escrow_detected():
    loan = _loan()
    txn = _fixme_txn(
        date(2024, 3, 1), Decimal("599.55"),
        narration="Mortgage payment (includes escrow deposit)",
    )
    p = plan(txn, loan)
    assert p.narration_hint_escrow is True


def test_plan_narration_hint_escrow_avoids_substring_match():
    """'escrowed' or 'microbrews' shouldn't trigger the whole-word match."""
    loan = _loan()
    txn = _fixme_txn(
        date(2024, 3, 1), Decimal("599.55"),
        narration="Fee for escrowed-type service",  # escrowed, not escrow
    )
    p = plan(txn, loan)
    # 'escrowed' contains 'escrow' as substring but not as a whole word.
    assert p.narration_hint_escrow is False


def test_plan_unique_decision_id_per_call():
    loan = _loan()
    txn = _fixme_txn(date(2024, 3, 1), Decimal("599.55"))
    p1 = plan(txn, loan)
    p2 = plan(txn, loan)
    assert p1.decision_id != p2.decision_id
    assert len(p1.decision_id) > 20  # uuid-shaped


def test_plan_from_facts_exact_match_uses_source_account_as_from():
    """Facts-level variant takes source_account instead of walking
    txn postings. Resulting splits must match the txn-level plan()."""
    from lamella.features.loans.auto_classify import plan_from_facts

    loan = _loan()
    p = plan_from_facts(
        actual_total=Decimal("599.55"),
        txn_date=date(2024, 3, 1),
        loan=loan,
        source_account="Assets:Personal:Checking",
        narration="Mortgage payment",
    )
    assert p.tier == "exact"
    assert p.from_account == "Assets:Personal:Checking"
    total = sum((amt for _, amt in p.splits), Decimal("0"))
    assert total == Decimal("599.55")


def test_plan_from_facts_tier_matches_txn_plan():
    """plan_from_facts and plan must produce identical tier decisions
    for the same (amount, date, loan). This is the shared-code
    contract — only the input shape differs."""
    from lamella.features.loans.auto_classify import plan_from_facts

    loan = _loan()
    txn = _fixme_txn(date(2024, 3, 1), Decimal("699.55"))
    p_txn = plan(txn, loan)
    p_facts = plan_from_facts(
        actual_total=Decimal("699.55"),
        txn_date=date(2024, 3, 1),
        loan=loan,
        source_account="Assets:Personal:Checking",
    )
    assert p_txn.tier == p_facts.tier
    assert p_txn.expected_total == p_facts.expected_total
    assert p_txn.overflow_amount == p_facts.overflow_amount


def test_plan_from_facts_narration_hint_from_text():
    from lamella.features.loans.auto_classify import plan_from_facts

    loan = _loan()
    p = plan_from_facts(
        actual_total=Decimal("599.55"),
        txn_date=date(2024, 3, 1),
        loan=loan,
        narration="ACME BANK — mortgage payment (includes escrow deposit)",
    )
    assert p.narration_hint_escrow is True


def test_plan_from_facts_ask_overflow_downgrades_at_ingest():
    """Matches the user's Tier 2 guidance: overflow_default='ask' at
    ingest time has no user to ask — plan must surface via tier=under,
    not silently pick a default."""
    from lamella.features.loans.auto_classify import plan_from_facts

    loan = _loan(overflow_default="ask")
    p = plan_from_facts(
        actual_total=Decimal("699.55"),
        txn_date=date(2024, 3, 1),
        loan=loan,
        source_account="Assets:Personal:Checking",
    )
    assert p.tier in ("under", "far")
    assert p.splits == []
    assert "ask" in p.skip_reason.lower()


def test_plan_no_fixme_returns_far():
    """Defensive: transaction with no FIXME leg — plan() shouldn't
    crash, just return 'far' with skip_reason."""
    txn = Transaction(
        meta={"filename": "x", "lineno": 1},
        date=date(2024, 3, 1), flag="*", payee=None, narration="x",
        tags=set(), links=set(),
        postings=[
            Posting(account="Assets:Personal:Checking",
                    units=Amount(Decimal("-100"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
            Posting(account="Expenses:Groceries",
                    units=Amount(Decimal("100"), "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )
    loan = _loan()
    p = plan(txn, loan)
    assert p.tier == "far"
    assert "no fixme leg" in p.skip_reason.lower()


# --------------------------------------------- process() — real DB + ledger


def _make_real_ledger(tmp_path: Path):
    main = tmp_path / "main.bean"
    accounts = tmp_path / "connector_accounts.bean"
    overrides = tmp_path / "connector_overrides.bean"
    config = tmp_path / "connector_config.bean"
    main.write_text(
        'option "title" "Test"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        f'include "{accounts.name}"\n'
        f'include "{overrides.name}"\n'
        f'include "{config.name}"\n'
        "\n"
        "2020-01-01 open Assets:Personal:Checking USD\n"
        "2020-01-01 open Assets:Personal:FIXME USD\n"
        "2020-01-01 open Expenses:Personal:FIXME USD\n"
        "2020-01-01 open Liabilities:Personal:Bank:M USD\n"
        "2020-01-01 open Expenses:Personal:M:Interest USD\n",
        encoding="utf-8",
    )
    accounts.write_text("; connector_accounts.bean\n", encoding="utf-8")
    overrides.write_text("; connector_overrides.bean\n", encoding="utf-8")
    config.write_text("; connector_config.bean\n", encoding="utf-8")
    return {"main": main, "accounts": accounts,
            "overrides": overrides, "config": config}


class _FakeSettings:
    def __init__(self, paths):
        self.ledger_main = paths["main"]
        self.connector_accounts_path = paths["accounts"]
        self.connector_overrides_path = paths["overrides"]
        self.connector_config_path = paths["config"]


class _RealReader:
    def __init__(self, main: Path):
        self.main = main
        self._loaded = None

    def load(self):
        if self._loaded is None:
            entries, _errors, _opts = load_file(str(self.main))

            class _L:
                def __init__(self, e):
                    self.entries = e
            self._loaded = _L(entries)
        return self._loaded

    def invalidate(self):
        self._loaded = None


def test_process_payment_exact_writes_override_with_autoclass_meta(tmp_path: Path):
    paths = _make_real_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])

    db_path = tmp_path / "test.sqlite"
    conn = connect(db_path)
    migrate(conn)
    # Seed entity + loan (migrate runs all migrations including 047).
    conn.execute(
        "INSERT INTO entities (slug, display_name, is_active) VALUES (?, ?, ?)",
        ("Personal", "Personal", 1),
    )
    loan_dict = _loan()
    conn.execute(
        "INSERT INTO loans (slug, display_name, loan_type, entity_slug, "
        "institution, original_principal, funded_date, first_payment_date, "
        "term_months, interest_rate_apr, monthly_payment_estimate, "
        "liability_account_path, interest_account_path, is_active, "
        "auto_classify_enabled, overflow_default) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            loan_dict["slug"], loan_dict["display_name"], "mortgage",
            loan_dict["entity_slug"], loan_dict["institution"],
            loan_dict["original_principal"], "2024-01-01",
            loan_dict["first_payment_date"], loan_dict["term_months"],
            loan_dict["interest_rate_apr"], loan_dict["monthly_payment_estimate"],
            loan_dict["liability_account_path"], loan_dict["interest_account_path"],
            1, 1, "bonus_principal",
        ),
    )
    conn.commit()

    txn = _fixme_txn(date(2024, 3, 1), Decimal("599.55"))
    claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    outcome = process(claim, txn, loan_dict,
                      settings=settings, reader=reader, conn=conn)

    assert outcome.wrote_override is True
    assert outcome.tier == "exact"
    assert outcome.decision_id

    # Override block landed in the overrides file with autoclass meta.
    content = paths["overrides"].read_text(encoding="utf-8")
    assert "lamella-loan-autoclass-tier" in content
    assert '"exact"' in content
    assert "lamella-loan-autoclass-decision-id" in content
    assert outcome.decision_id in content

    # Log row inserted.
    log_row = conn.execute(
        "SELECT tier, expected_total, actual_total FROM loan_autoclass_log "
        "WHERE decision_id = ?", (outcome.decision_id,),
    ).fetchone()
    assert log_row is not None
    assert log_row[0] == "exact"
    assert log_row[2] == "599.55"

    conn.close()


def test_process_auto_classify_disabled_preempts_without_writing(tmp_path: Path):
    """Per-loan master switch: when auto_classify_enabled=0, still
    preempt (principle 3) but never auto-split."""
    paths = _make_real_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])
    db_path = tmp_path / "test.sqlite"
    conn = connect(db_path)
    migrate(conn)

    loan_dict = _loan(auto_classify_enabled=0)
    txn = _fixme_txn(date(2024, 3, 1), Decimal("599.55"))
    claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    outcome = process(claim, txn, loan_dict,
                      settings=settings, reader=reader, conn=conn)

    assert outcome.wrote_override is False
    assert outcome.tier is None  # never computed tier
    assert "auto_classify_enabled" in outcome.skip_reason
    # No override written.
    content = paths["overrides"].read_text(encoding="utf-8")
    assert "lamella-loan-autoclass-tier" not in content
    conn.close()


def test_process_under_tier_does_not_write(tmp_path: Path):
    paths = _make_real_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])
    db_path = tmp_path / "test.sqlite"
    conn = connect(db_path)
    migrate(conn)

    loan_dict = _loan()
    # Pay $400 vs $599 expected → under.
    txn = _fixme_txn(date(2024, 3, 1), Decimal("400.00"))
    claim = Claim(kind=ClaimKind.PAYMENT, loan_slug="M")
    outcome = process(claim, txn, loan_dict,
                      settings=settings, reader=reader, conn=conn)

    assert outcome.wrote_override is False
    assert outcome.tier == "under"
    # No override written, no log row.
    content = paths["overrides"].read_text(encoding="utf-8")
    assert "lamella-loan-autoclass-tier" not in content
    rows = conn.execute("SELECT * FROM loan_autoclass_log").fetchall()
    assert rows == []
    conn.close()


def test_process_escrow_disbursement_preempts_without_writing(tmp_path: Path):
    paths = _make_real_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])
    db_path = tmp_path / "test.sqlite"
    conn = connect(db_path)
    migrate(conn)

    loan_dict = _loan()
    txn = _fixme_txn(date(2024, 3, 1), Decimal("4000"))
    claim = Claim(kind=ClaimKind.ESCROW_DISBURSEMENT, loan_slug="M")
    outcome = process(claim, txn, loan_dict,
                      settings=settings, reader=reader, conn=conn)

    assert outcome.wrote_override is False
    assert outcome.claim_kind == ClaimKind.ESCROW_DISBURSEMENT
    assert outcome.tier is None
    content = paths["overrides"].read_text(encoding="utf-8")
    assert "lamella-loan-autoclass-tier" not in content
    conn.close()


def test_process_revolving_skip_preempts_without_writing(tmp_path: Path):
    paths = _make_real_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])
    db_path = tmp_path / "test.sqlite"
    conn = connect(db_path)
    migrate(conn)

    loan_dict = _loan()
    txn = _fixme_txn(date(2024, 3, 1), Decimal("500"))
    claim = Claim(kind=ClaimKind.REVOLVING_SKIP, loan_slug="M")
    outcome = process(claim, txn, loan_dict,
                      settings=settings, reader=reader, conn=conn)

    assert outcome.wrote_override is False
    assert outcome.claim_kind == ClaimKind.REVOLVING_SKIP
    conn.close()


# ------------------------------------------------- extra_meta integration


def test_override_writer_stamps_extra_meta(tmp_path: Path):
    """WP6 adds extra_meta kwarg to OverrideWriter.append_split. Confirm
    that passing a dict emits lamella-* meta lines between modified-at and
    the from_account posting."""
    from lamella.features.rules.overrides import OverrideWriter

    paths = _make_real_ledger(tmp_path)
    # We need the FIXME transaction to exist in the ledger for
    # bean-check to pass the override-of reference.
    with paths["main"].open("a", encoding="utf-8") as f:
        f.write(
            '\n2024-03-01 * "Mortgage payment" ^fixme-01\n'
            '  Assets:Personal:Checking  -599.55 USD\n'
            '  Expenses:Personal:FIXME  599.55 USD\n'
        )

    writer = OverrideWriter(
        main_bean=paths["main"],
        overrides=paths["overrides"],
        run_check=False,  # skip bean-check — we're only testing the emit
    )
    block = writer.append_split(
        txn_date=date(2024, 3, 1),
        txn_hash="fake-hash-01",
        from_account="Expenses:Personal:FIXME",
        splits=[
            ("Liabilities:Personal:Bank:M", Decimal("100")),
            ("Expenses:Personal:M:Interest", Decimal("499.55")),
        ],
        extra_meta={
            "lamella-loan-autoclass-tier": "exact",
            "lamella-loan-autoclass-decision-id": "dec-abc-123",
        },
    )
    assert 'lamella-loan-autoclass-tier: "exact"' in block
    assert 'lamella-loan-autoclass-decision-id: "dec-abc-123"' in block
    # Meta lines appear between modified-at and the from_account posting
    # so a reader can pick them up without scanning the whole block.
    tier_idx = block.index("lamella-loan-autoclass-tier")
    from_idx = block.index("Expenses:Personal:FIXME")
    modified_idx = block.index("lamella-modified-at")
    assert modified_idx < tier_idx < from_idx
