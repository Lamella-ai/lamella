# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Regression: account-meta reconstruct must accept directives that
omit ``lamella-display-name``.

The writer at ``account_meta_writer.append_account_meta`` only emits
``lamella-display-name`` when the user actually set one — every other
field works the same way. Step 21 (the reconstruct reader) used to
forward the raw value into the INSERT, which violated the schema's
``display_name TEXT NOT NULL`` and broke ``/setup/import`` whenever a
ledger carried a vanilla ``custom "account-meta"`` entry that only
labeled the path (no display name yet).

The fix: fall back to the same heuristic ``seed_accounts_meta`` uses
for unlabeled accounts, so an INSERT always has a non-null display
name. The COALESCE on UPDATE preserves any existing user-set value.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from beancount.loader import load_file

from lamella.core.transform.steps.step21_account_meta import (
    reconstruct_account_meta,
)


def _fresh_db(tmp_path: Path) -> sqlite3.Connection:
    from lamella.core.db import connect, migrate
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    return conn


def test_directive_without_display_name_inserts_with_fallback(tmp_path: Path):
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:EntityA:Mercury:Checking USD\n'
        '2020-01-01 custom "account-meta" Assets:EntityA:Mercury:Checking\n'
        '  lamella-modified-at: "2026-04-25T00:00:00-06:00"\n',
        encoding="utf-8",
    )
    entries, _errors, _opts = load_file(str(main))
    conn = _fresh_db(tmp_path / "db")
    report = reconstruct_account_meta(conn, entries)

    # The INSERT must succeed (this is the regression — previously raised
    # NOT NULL constraint failed: accounts_meta.display_name).
    assert report.rows_written == 1

    row = conn.execute(
        "SELECT display_name FROM accounts_meta WHERE account_path = ?",
        ("Assets:EntityA:Mercury:Checking",),
    ).fetchone()
    assert row is not None
    # Fallback should be the heuristic short name (something non-empty).
    assert row["display_name"]
    assert row["display_name"].strip() != ""


def test_directive_with_display_name_uses_it(tmp_path: Path):
    # Sanity: an explicit lamella-display-name still wins over the fallback.
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:EntityA:Mercury:Checking USD\n'
        '2020-01-01 custom "account-meta" Assets:EntityA:Mercury:Checking\n'
        '  lamella-display-name: "Mercury Checking"\n'
        '  lamella-modified-at: "2026-04-25T00:00:00-06:00"\n',
        encoding="utf-8",
    )
    entries, _errors, _opts = load_file(str(main))
    conn = _fresh_db(tmp_path / "db")
    reconstruct_account_meta(conn, entries)
    row = conn.execute(
        "SELECT display_name FROM accounts_meta WHERE account_path = ?",
        ("Assets:EntityA:Mercury:Checking",),
    ).fetchone()
    assert row["display_name"] == "Mercury Checking"


def test_existing_row_display_name_preserved_on_update(tmp_path: Path):
    # If an accounts_meta row already exists with a user-set display
    # name, a directive that omits lamella-display-name must NOT clobber
    # it. The COALESCE on UPDATE preserves the existing value.
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:EntityA:Mercury:Checking USD\n'
        '2020-01-01 custom "account-meta" Assets:EntityA:Mercury:Checking\n'
        '  lamella-institution: "Mercury"\n'
        '  lamella-modified-at: "2026-04-25T00:00:00-06:00"\n',
        encoding="utf-8",
    )
    entries, _errors, _opts = load_file(str(main))
    conn = _fresh_db(tmp_path / "db")
    conn.execute(
        "INSERT INTO accounts_meta (account_path, display_name) VALUES (?, ?)",
        ("Assets:EntityA:Mercury:Checking", "User Picked Name"),
    )
    conn.commit()

    reconstruct_account_meta(conn, entries)

    row = conn.execute(
        "SELECT display_name, institution FROM accounts_meta WHERE account_path = ?",
        ("Assets:EntityA:Mercury:Checking",),
    ).fetchone()
    assert row["display_name"] == "User Picked Name"
    assert row["institution"] == "Mercury"
