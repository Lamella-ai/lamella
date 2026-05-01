# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the v2 → v3 ledger migration: stamp lamella-txn-id on
every Transaction that lacks one.

v3 retires the legacy hex (txn_hash) URL form by guaranteeing every
entry has a UUIDv7 lamella-txn-id at the txn-meta level. The
migration delegates to ``normalize_txn_identity.run`` (which already
handles the surgical insert + snapshot + bean-check) and then bumps
the ledger-version stamp from "2" to "3"."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lamella.core.db import connect, migrate
from lamella.features.recovery.migrations.migrate_ledger_v2_to_v3 import (
    MigrateLedgerV2ToV3,
)


class _Settings:
    """Minimal Settings stand-in. The migration reads ledger_dir +
    ledger_main only."""

    def __init__(self, ledger_dir: Path):
        self.ledger_dir = ledger_dir
        self.ledger_main = ledger_dir / "main.bean"


def _make_v2_ledger(tmp_path: Path) -> _Settings:
    """Stand up a v2 ledger with two transactions: one already
    carrying lamella-txn-id (post-Phase-7 write) and one without
    (legacy / hand-edited)."""
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '\n'
        '; Schema marker — v2\n'
        '2020-01-01 custom "lamella-ledger-version" "2"\n'
        '\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Income:Salary USD\n'
        '2020-01-01 open Expenses:Groceries USD\n'
        '\n'
        '2026-01-15 * "Paycheck" "January"\n'
        '  lamella-txn-id: "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  Assets:Bank      1000.00 USD\n'
        '  Income:Salary   -1000.00 USD\n'
        '\n'
        '2026-01-16 * "Acme Co." "groceries"\n'
        '  Expenses:Groceries   42.17 USD\n'
        '  Assets:Bank         -42.17 USD\n',
        encoding="utf-8",
    )
    return _Settings(tmp_path)


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def test_declared_paths_returns_all_bean_files(tmp_path: Path):
    settings = _make_v2_ledger(tmp_path)
    (tmp_path / "manual_transactions.bean").write_text(
        '; user-authored\n', encoding="utf-8",
    )
    paths = MigrateLedgerV2ToV3().declared_paths(settings)
    names = {p.name for p in paths}
    assert "main.bean" in names
    assert "manual_transactions.bean" in names


def test_dry_run_reports_lineage_to_mint(tmp_path: Path, conn):
    settings = _make_v2_ledger(tmp_path)
    pre = settings.ledger_main.read_text(encoding="utf-8")
    result = MigrateLedgerV2ToV3().dry_run(conn=conn, settings=settings)
    # One entry already has lineage; the second one needs minting.
    assert "1 transaction" in result.summary
    # Disk unchanged.
    assert settings.ledger_main.read_text(encoding="utf-8") == pre


def test_apply_mints_lineage_and_bumps_stamp(tmp_path: Path, conn):
    settings = _make_v2_ledger(tmp_path)
    MigrateLedgerV2ToV3().apply(conn=conn, settings=settings)
    text = settings.ledger_main.read_text(encoding="utf-8")
    # Stamp bumped.
    assert 'custom "lamella-ledger-version" "3"' in text
    assert 'custom "lamella-ledger-version" "2"' not in text
    # The previously-stamped entry kept its UUID verbatim.
    assert "01900000-0000-7000-8000-aaaaaaaaaaaa" in text
    # The second entry now has a lamella-txn-id meta line.
    import re
    matches = re.findall(r'lamella-txn-id:\s*"([^"]+)"', text)
    assert len(matches) == 2
    # Both look like UUIDv7s (version nibble == 7, variant 8/9/a/b).
    uuid_re = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-"
        r"[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    for m in matches:
        assert uuid_re.match(m), f"not UUIDv7: {m}"


def test_apply_idempotent_on_v3_ledger(tmp_path: Path, conn):
    settings = _make_v2_ledger(tmp_path)
    mig = MigrateLedgerV2ToV3()
    mig.apply(conn=conn, settings=settings)
    after_first = settings.ledger_main.read_text(encoding="utf-8")
    # Second apply on already-v3 ledger should be a no-op.
    mig.apply(conn=conn, settings=settings)
    after_second = settings.ledger_main.read_text(encoding="utf-8")
    assert after_first == after_second


def test_apply_handles_missing_main_bean(tmp_path: Path, conn):
    """Defensive: if main.bean disappears mid-flight, the migration
    raises a MigrationError rather than silently completing."""
    from lamella.features.recovery.migrations.base import MigrationError
    settings = _make_v2_ledger(tmp_path)
    # Walk the lineage step (which writes), then nuke main.bean before
    # the stamp step. Easiest: just run apply once cleanly to exercise
    # the success path, then delete main and run again.
    MigrateLedgerV2ToV3().apply(conn=conn, settings=settings)
    settings.ledger_main.unlink()
    with pytest.raises(MigrationError, match="main.bean is missing"):
        MigrateLedgerV2ToV3().apply(conn=conn, settings=settings)


def test_registry_includes_v2_to_v3():
    """The @register_migration decorator should land in the ledger
    registry under the (v2 → 3) key."""
    from lamella.features.recovery.migrations import _LEDGER_REGISTRY
    assert ("2", 3) in _LEDGER_REGISTRY
    assert isinstance(_LEDGER_REGISTRY[("2", 3)], MigrateLedgerV2ToV3)
