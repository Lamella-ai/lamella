# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""WP4 — scaffolding self-check + autofix.

Most tests are pure: synthesized Beancount entries + a FakeConn for
the loan_balance_anchors/properties reads. A small set of autofix
tests use a real tmp-ledger fixture since those exercise
AccountsWriter.write_opens and bean-check.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core.amount import Amount
from beancount.core.data import Open, Posting, Transaction
from beancount.loader import load_file

from lamella.features.loans import scaffolding
from lamella.features.loans.scaffolding import Issue, ScaffoldingError, check


# ------------------------------------------------------------------ fixtures


def _loan(**overrides) -> dict:
    base = {
        "slug": "MainResidenceMortgage",
        "display_name": "Main Residence Mortgage",
        "loan_type": "mortgage",
        "entity_slug": "Personal",
        "institution": "BankTwo",
        "original_principal": "550000.00",
        "funded_date": "2025-10-27",
        "first_payment_date": "2025-11-01",
        "payment_due_day": 1,
        "term_months": 360,
        "interest_rate_apr": "6.625",
        "monthly_payment_estimate": "3521.64",
        "escrow_monthly": None,
        "property_tax_monthly": None,
        "insurance_monthly": None,
        "liability_account_path": "Liabilities:Personal:BankTwo:MainResidenceMortgage",
        "interest_account_path":  "Expenses:Personal:MainResidenceMortgage:Interest",
        "escrow_account_path":    None,
        "simplefin_account_id":   None,
        "property_slug": None,
        "is_active": 1,
    }
    base.update(overrides)
    return base


def _open(account: str, d: date = date(2025, 10, 27)) -> Open:
    return Open(
        meta={"filename": "test_connector_accounts.bean", "lineno": 1},
        date=d, account=account, currencies=["USD"], booking=None,
    )


def _open_in_user_file(account: str, d: date) -> Open:
    """An Open directive that lives in a user-authored file (not ours)."""
    return Open(
        meta={"filename": "user_main.bean", "lineno": 12},
        date=d, account=account, currencies=["USD"], booking=None,
    )


def _txn_on(account: str, d: date, amount: Decimal) -> Transaction:
    return Transaction(
        meta={"filename": "x", "lineno": 1},
        date=d, flag="*", payee=None, narration="test",
        tags=set(), links=set(),
        postings=[
            Posting(account=account, units=Amount(amount, "USD"),
                    cost=None, price=None, flag=None, meta={}),
        ],
    )


class _FakeConn:
    """Minimal conn: supports the two SELECTs scaffolding.check issues."""

    def __init__(self, properties: list[str] | None = None):
        self.properties = properties or []

    def execute(self, sql: str, params=()):
        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

            def fetchall(self):
                return self._rows

        if "FROM properties" in sql:
            slug = params[0] if params else ""
            rows = [(slug,)] if slug in self.properties else []
            return _Cursor(rows)
        if "FROM loan_balance_anchors" in sql:
            return _Cursor([])
        return _Cursor([])


# ----------------------------------------------------------- open-missing


def test_open_missing_detected_when_liability_has_no_open():
    loan = _loan()
    # Only the interest account is open; liability is not.
    entries = [_open("Expenses:Personal:MainResidenceMortgage:Interest")]
    issues = check(loan, entries, _FakeConn(), settings=None)

    kinds = [i.kind for i in issues]
    assert "open-missing" in kinds
    missing = next(i for i in issues if i.kind == "open-missing")
    assert missing.severity == "blocking"
    assert missing.path == "Liabilities:Personal:BankTwo:MainResidenceMortgage"
    assert missing.can_autofix is True
    assert missing.fix_payload["kind"] == "open-missing"


def test_open_present_on_time_has_no_issue():
    loan = _loan()
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    assert not any(i.kind == "open-missing" for i in issues)


# ------------------------------------------------------- open-date-too-late


def test_open_date_too_late_detected_when_txn_precedes_open():
    loan = _loan()
    late_open = _open(
        "Liabilities:Personal:BankTwo:MainResidenceMortgage",
        date(2025, 12, 1),
    )
    early_txn = _txn_on(
        "Liabilities:Personal:BankTwo:MainResidenceMortgage",
        date(2025, 10, 27), Decimal("-550000"),
    )
    entries = [
        late_open,
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
        early_txn,
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)

    kinds = [i.kind for i in issues]
    assert "open-date-too-late" in kinds
    issue = next(i for i in issues if i.kind == "open-date-too-late")
    assert issue.severity == "blocking"
    # Target date should be min(earliest txn, funded_date) = earliest txn here.
    assert issue.fix_payload["target_date"] == "2025-10-27"


# ----------------------------------------------------------- escrow path


def test_escrow_monthly_without_path_detected():
    loan = _loan(escrow_monthly="850.00")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)

    escrow = [i for i in issues if i.kind == "escrow-path-missing"]
    assert len(escrow) == 1
    assert escrow[0].severity == "attention"
    assert escrow[0].can_autofix is True
    assert escrow[0].path == "Assets:Personal:BankTwo:MainResidenceMortgage:Escrow"


def test_escrow_with_path_set_no_issue():
    loan = _loan(
        escrow_monthly="850.00",
        escrow_account_path="Assets:Personal:BankTwo:MainResidenceMortgage:Escrow",
    )
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
        _open("Assets:Personal:BankTwo:MainResidenceMortgage:Escrow"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    assert not any(i.kind == "escrow-path-missing" for i in issues)


# -------------------------------------------------------- tax / insurance


def test_tax_monthly_without_open_detected():
    loan = _loan(property_tax_monthly="200.00")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    tax = [i for i in issues if i.kind == "tax-path-missing"]
    assert len(tax) == 1
    assert tax[0].path == "Expenses:Personal:MainResidenceMortgage:PropertyTax"


def test_insurance_monthly_without_open_detected():
    loan = _loan(insurance_monthly="100.00")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    insurance = [i for i in issues if i.kind == "insurance-path-missing"]
    assert len(insurance) == 1
    assert insurance[0].path == "Expenses:Personal:MainResidenceMortgage:Insurance"


def test_tax_path_already_open_clears_issue():
    loan = _loan(property_tax_monthly="200.00")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
        _open("Expenses:Personal:MainResidenceMortgage:PropertyTax"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    assert not any(i.kind == "tax-path-missing" for i in issues)


# --------------------------------------------------- property-slug-dangling


def test_property_slug_dangling_detected():
    loan = _loan(property_slug="MissingProperty")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    conn = _FakeConn(properties=["OtherProperty"])
    issues = check(loan, entries, conn, settings=None)

    dangling = [i for i in issues if i.kind == "property-slug-dangling"]
    assert len(dangling) == 1
    assert dangling[0].severity == "attention"
    assert dangling[0].can_autofix is False  # requires user input


def test_property_slug_valid_no_issue():
    loan = _loan(property_slug="MainResidence")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    conn = _FakeConn(properties=["MainResidence"])
    issues = check(loan, entries, conn, settings=None)
    assert not any(i.kind == "property-slug-dangling" for i in issues)


# ----------------------------------------------------------- simplefin-stale


def test_simplefin_stale_when_no_txn_with_matching_id():
    loan = _loan(simplefin_account_id="sf-abc-123")
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    stale = [i for i in issues if i.kind == "simplefin-stale"]
    assert len(stale) == 1
    assert stale[0].severity == "info"
    assert stale[0].can_autofix is False


def test_simplefin_recent_txn_no_issue():
    loan = _loan(simplefin_account_id="sf-abc-123")
    recent = date.today()
    txn = Transaction(
        meta={"lamella-simplefin-account-id": "sf-abc-123", "filename": "x", "lineno": 1},
        date=recent, flag="*", payee=None, narration="recent",
        tags=set(), links=set(), postings=[],
    )
    entries = [
        _open("Liabilities:Personal:BankTwo:MainResidenceMortgage"),
        _open("Expenses:Personal:MainResidenceMortgage:Interest"),
        txn,
    ]
    issues = check(loan, entries, _FakeConn(), settings=None)
    assert not any(i.kind == "simplefin-stale" for i in issues)


# -------------------------------------------------- WP1 integration check


def test_health_assess_auto_folds_scaffolding_issues():
    """WP1 + WP4 — when assess() is called without a scaffolding kwarg,
    it runs scaffolding.check() and folds the blockers into next_actions."""
    from lamella.features.loans.health import assess

    loan = _loan()
    # Only the interest Open exists — liability is missing, triggers blocker.
    entries = [_open("Expenses:Personal:MainResidenceMortgage:Interest")]
    conn = _FakeConn()

    h = assess(loan, entries, conn, settings=None, as_of=date(2026, 4, 24))

    assert h.summary_badge == "blocking"
    assert h.scaffolding.has_blockers is True
    # First action should be the scaffolding-open-missing blocker,
    # ahead of fund-initial (also blocking but higher priority number).
    assert h.next_actions[0].kind == "scaffolding-open-missing"
    assert h.next_actions[0].severity == "blocking"


# ------------------------------------------------ autofix (real ledger fixture)


def _make_ledger(tmp_path: Path) -> dict:
    """Minimal real ledger fixture that bean-check will load."""
    main = tmp_path / "main.bean"
    connector_accounts = tmp_path / "connector_accounts.bean"
    connector_config = tmp_path / "connector_config.bean"
    connector_overrides = tmp_path / "connector_overrides.bean"

    main.write_text(
        'option "title" "Test"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        f'include "{connector_accounts.name}"\n'
        f'include "{connector_overrides.name}"\n'
        f'include "{connector_config.name}"\n'
        "\n"
        "2020-01-01 open Assets:Cash USD\n",
        encoding="utf-8",
    )
    connector_accounts.write_text(
        "; connector_accounts.bean\n", encoding="utf-8",
    )
    connector_config.write_text(
        "; connector_config.bean\n", encoding="utf-8",
    )
    connector_overrides.write_text(
        "; connector_overrides.bean\n", encoding="utf-8",
    )
    return {
        "main": main,
        "connector_accounts": connector_accounts,
        "connector_config": connector_config,
        "connector_overrides": connector_overrides,
    }


class _FakeSettings:
    """Minimal settings shape scaffolding.autofix reads from."""

    def __init__(self, paths: dict):
        self.ledger_main = paths["main"]
        self.connector_accounts_path = paths["connector_accounts"]
        self.connector_config_path = paths["connector_config"]
        self.connector_overrides_path = paths["connector_overrides"]


class _RealReader:
    """Minimal LedgerReader stand-in — loads from disk each call."""

    def __init__(self, main: Path):
        self.main = main
        self._loaded = None

    def load(self):
        if self._loaded is None:
            entries, _errors, _opts = load_file(str(self.main))

            class _L:
                def __init__(self, ents):
                    self.entries = ents

            self._loaded = _L(entries)
        return self._loaded

    def invalidate(self):
        self._loaded = None


def test_autofix_open_missing_writes_open_and_clears_issue(tmp_path: Path):
    paths = _make_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])

    loan = _loan()
    # No relevant Opens — the liability account is unopened.
    issues_before = check(loan, reader.load().entries, _FakeConn(), settings)
    assert any(
        i.kind == "open-missing"
        and i.path == "Liabilities:Personal:BankTwo:MainResidenceMortgage"
        for i in issues_before
    )

    # Run autofix for the liability Open.
    scaffolding.autofix(
        "open-missing", loan,
        path="Liabilities:Personal:BankTwo:MainResidenceMortgage",
        settings=settings, reader=reader, conn=_FakeConn(),
    )

    # Fresh reader load — the Open is now present.
    reader.invalidate()
    issues_after = check(loan, reader.load().entries, _FakeConn(), settings)
    assert not any(
        i.kind == "open-missing"
        and i.path == "Liabilities:Personal:BankTwo:MainResidenceMortgage"
        for i in issues_after
    )


def test_autofix_unknown_kind_raises(tmp_path: Path):
    paths = _make_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])

    with pytest.raises(ScaffoldingError):
        scaffolding.autofix(
            "not-a-real-kind", _loan(), path=None,
            settings=settings, reader=reader, conn=_FakeConn(),
        )


def test_autofix_open_missing_without_path_raises(tmp_path: Path):
    paths = _make_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])

    with pytest.raises(ScaffoldingError):
        scaffolding.autofix(
            "open-missing", _loan(), path=None,
            settings=settings, reader=reader, conn=_FakeConn(),
        )


def test_ensure_open_rewrites_our_file_when_open_is_too_late(tmp_path: Path):
    """When an Open directive lives in our connector_accounts.bean at a
    date later than the target, ensure_open_on_or_before must rewrite
    the date in place rather than raising."""
    from lamella.core.registry.accounts_writer import AccountsWriter

    paths = _make_ledger(tmp_path)

    # Seed a late Open in OUR connector_accounts.bean.
    paths["connector_accounts"].write_text(
        "; connector_accounts.bean\n"
        "2026-06-01 open Liabilities:Personal:BankTwo:MainResidenceMortgage USD\n",
        encoding="utf-8",
    )
    reader = _RealReader(paths["main"])
    opener = AccountsWriter(
        main_bean=paths["main"],
        connector_accounts=paths["connector_accounts"],
    )

    # Target: backdate to 2025-10-27 (loan funded_date).
    scaffolding.ensure_open_on_or_before(
        reader, opener,
        "Liabilities:Personal:BankTwo:MainResidenceMortgage",
        date(2025, 10, 27),
        connector_accounts_path=paths["connector_accounts"],
        comment_tag="test rewrite",
    )
    reader.invalidate()

    # The Open's date must now be <= 2025-10-27.
    from beancount.core.data import Open as _Open
    opens = [
        e for e in reader.load().entries
        if isinstance(e, _Open)
        and e.account == "Liabilities:Personal:BankTwo:MainResidenceMortgage"
    ]
    assert len(opens) == 1
    assert opens[0].date <= date(2025, 10, 27)


def test_ensure_open_raises_when_open_is_in_user_file(tmp_path: Path):
    """When an Open lives in a user-authored file (not our
    connector_accounts.bean) at a too-late date, we must NEVER
    rewrite it — raise ScaffoldingError pointing at file:line so
    the user can fix it themselves."""
    from lamella.core.registry.accounts_writer import AccountsWriter

    paths = _make_ledger(tmp_path)

    # Seed a late Open in a USER file, include it in main.
    user_file = tmp_path / "user_accounts.bean"
    user_file.write_text(
        "2026-06-01 open Liabilities:Personal:BankTwo:MainResidenceMortgage USD\n",
        encoding="utf-8",
    )
    paths["main"].write_text(
        paths["main"].read_text(encoding="utf-8")
        + f'\ninclude "{user_file.name}"\n',
        encoding="utf-8",
    )
    reader = _RealReader(paths["main"])
    opener = AccountsWriter(
        main_bean=paths["main"],
        connector_accounts=paths["connector_accounts"],
    )

    with pytest.raises(ScaffoldingError) as excinfo:
        scaffolding.ensure_open_on_or_before(
            reader, opener,
            "Liabilities:Personal:BankTwo:MainResidenceMortgage",
            date(2025, 10, 27),
            connector_accounts_path=paths["connector_accounts"],
            comment_tag="test user-file refusal",
        )

    # Error message should name the file, line, and the dates involved
    # so the user can fix it themselves.
    msg = str(excinfo.value)
    assert "user_accounts.bean" in msg
    assert "2026-06-01" in msg
    assert "2025-10-27" in msg


def test_autofix_escrow_path_missing_end_to_end(tmp_path: Path):
    """The most complex autofix: derives a default escrow path, writes
    it to SQLite, re-emits the loan directive, and scaffolds the Open
    directive at funded_date — all in one call."""
    import sqlite3

    from lamella.core.db import connect as db_connect, migrate as db_migrate

    paths = _make_ledger(tmp_path)
    settings = _FakeSettings(paths)
    reader = _RealReader(paths["main"])

    # Real SQLite (not FakeConn) — the autofix writes with UPDATE.
    db_path = tmp_path / "test.sqlite"
    conn = db_connect(db_path)
    db_migrate(conn)

    # Seed the entity the loan FK references.
    conn.execute(
        "INSERT INTO entities (slug, display_name, is_active) "
        "VALUES (?, ?, ?)",
        ("Personal", "Personal", 1),
    )

    # Seed the loan with escrow_monthly set but no escrow_account_path.
    conn.execute(
        "INSERT INTO loans "
        "(slug, display_name, loan_type, entity_slug, institution, "
        " original_principal, funded_date, escrow_monthly, "
        " liability_account_path, interest_account_path, is_active) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "MainResidenceMortgage", "Main Residence Mortgage", "mortgage",
            "Personal", "BankTwo",
            "550000.00", "2025-10-27", "850.00",
            "Liabilities:Personal:BankTwo:MainResidenceMortgage",
            "Expenses:Personal:MainResidenceMortgage:Interest",
            1,
        ),
    )
    conn.commit()

    loan_row = conn.execute(
        "SELECT * FROM loans WHERE slug = ?", ("MainResidenceMortgage",),
    ).fetchone()
    loan = dict(loan_row)

    # Sanity: the check engine flags this.
    issues_before = check(loan, reader.load().entries, conn, settings)
    assert any(i.kind == "escrow-path-missing" for i in issues_before)

    # Run the autofix. Pass path=None to force the "derive default"
    # branch.
    scaffolding.autofix(
        "escrow-path-missing", loan, path=None,
        settings=settings, reader=reader, conn=conn,
    )
    reader.invalidate()

    # SQLite now carries the derived escrow path.
    updated = conn.execute(
        "SELECT escrow_account_path FROM loans WHERE slug = ?",
        ("MainResidenceMortgage",),
    ).fetchone()
    assert updated[0] == "Assets:Personal:BankTwo:MainResidenceMortgage:Escrow"

    # The ledger now has an Open for that path (scaffolded).
    from beancount.core.data import Open as _Open
    opens = [
        e for e in reader.load().entries
        if isinstance(e, _Open)
        and e.account == "Assets:Personal:BankTwo:MainResidenceMortgage:Escrow"
    ]
    assert len(opens) == 1

    # The loan directive was re-emitted carrying the new escrow path
    # (reconstruct contract — if SQLite is wiped, the directive
    # rebuilds the row).
    from lamella.features.loans.reader import read_loans
    rebuilt = read_loans(reader.load().entries)
    rebuilt_loan = next(
        (r for r in rebuilt if r["slug"] == "MainResidenceMortgage"),
        None,
    )
    assert rebuilt_loan is not None
    assert (
        rebuilt_loan["escrow_account_path"]
        == "Assets:Personal:BankTwo:MainResidenceMortgage:Escrow"
    )

    # Re-check from the updated state — the escrow-path-missing
    # issue is gone.
    loan_after = dict(conn.execute(
        "SELECT * FROM loans WHERE slug = ?", ("MainResidenceMortgage",),
    ).fetchone())
    issues_after = check(loan_after, reader.load().entries, conn, settings)
    assert not any(i.kind == "escrow-path-missing" for i in issues_after)

    conn.close()
