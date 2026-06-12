# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``heal_schema_drift`` — Phase 5.4 of /setup/recovery.

Pins down: (a) sqlite-axis Findings dispatch to the catch-up
Migration and run db.migrate without snapshot-envelope wrapping,
(b) ledger-axis Findings dispatch to v0→v1 and run inside
with_bean_snapshot, (c) bean-check failure rolls back ledger
files byte-identically, (d) HealRefused for unregistered
axis/version pairs, (e) the version stamp is written by apply()
on the ledger axis.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest
from beancount.loader import load_file

from lamella.features.recovery.findings.schema_drift import (
    detect_schema_drift,
)
from lamella.features.recovery.heal.legacy_paths import HealRefused
from lamella.features.recovery.heal.schema_drift import heal_schema_drift
from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)
from lamella.core.db import migrate


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_ledger(
    tmp_path: Path, *, stamp: bool = False, with_content: bool = True,
) -> dict:
    """Minimal real ledger. ``stamp=True`` writes the v1 directive
    so the ledger reads as already-migrated. ``with_content=True``
    adds a single balanced transaction so the detector recognizes
    the ledger as 'has content' (otherwise it counts as
    structurally empty and the missing-stamp finding doesn't fire)."""
    main = tmp_path / "main.bean"
    connector_accounts = tmp_path / "connector_accounts.bean"
    connector_config = tmp_path / "connector_config.bean"
    connector_links = tmp_path / "connector_links.bean"
    connector_rules = tmp_path / "connector_rules.bean"
    connector_budgets = tmp_path / "connector_budgets.bean"

    main_body = (
        'option "title" "Recovery schema-drift test"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        f'include "{connector_accounts.name}"\n'
        f'include "{connector_config.name}"\n'
        f'include "{connector_links.name}"\n'
        f'include "{connector_rules.name}"\n'
        f'include "{connector_budgets.name}"\n'
    )
    if stamp:
        main_body += '2020-01-01 custom "lamella-ledger-version" "1"\n'
    if with_content:
        main_body += (
            '\n2024-06-01 * "ledger-content sentinel"\n'
            "  Assets:Personal:Cash    1.00 USD\n"
            "  Equity:OpeningBalances\n"
        )
    main.write_text(main_body, encoding="utf-8")
    connector_accounts.write_text(
        "; connector_accounts.bean\n", encoding="utf-8",
    )
    connector_config.write_text(
        "; connector_config.bean\n", encoding="utf-8",
    )
    connector_links.write_text(
        "; connector_links.bean\n", encoding="utf-8",
    )
    connector_rules.write_text(
        "; connector_rules.bean\n", encoding="utf-8",
    )
    connector_budgets.write_text(
        "; connector_budgets.bean\n", encoding="utf-8",
    )
    return {
        "ledger_dir": tmp_path,
        "main": main,
        "connector_accounts": connector_accounts,
        "connector_config": connector_config,
        "connector_links": connector_links,
        "connector_rules": connector_rules,
        "connector_budgets": connector_budgets,
    }


class _Settings:
    def __init__(self, paths: dict):
        self.ledger_dir = paths["ledger_dir"]
        self.ledger_main = paths["main"]
        self.connector_accounts_path = paths["connector_accounts"]
        self.connector_config_path = paths["connector_config"]
        self.connector_links_path = paths["connector_links"]
        self.connector_rules_path = paths["connector_rules"]
        self.connector_budgets_path = paths["connector_budgets"]
        # Sibling paths the migrate_to_ledger writers may consult.
        self.connector_overrides_path = paths["ledger_dir"] / "connector_overrides.bean"


class _Reader:
    def __init__(self, main: Path):
        self.main = main
        self._loaded = None
        self.invalidate_count = 0

    def load(self):
        if self._loaded is None:
            entries, _errs, _opts = load_file(str(self.main))

            class _L:
                def __init__(self, ents):
                    self.entries = ents

            self._loaded = _L(entries)
        return self._loaded

    def invalidate(self):
        self._loaded = None
        self.invalidate_count += 1


@pytest.fixture
def empty_db():
    """A DB with the schema_migrations table missing entries above
    a chosen head — simulates a deploy lagging the code's expected
    head by N migrations."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Refusal path
# ---------------------------------------------------------------------------


def test_refuses_non_schema_drift_finding(tmp_path, empty_db):
    """A legacy_path finding fed to heal_schema_drift refuses up
    front. Defense against route mis-dispatch."""
    paths = _make_ledger(tmp_path)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    bogus = Finding(
        id=make_finding_id("legacy_path", "Assets:Vehicles:Toy"),
        category="legacy_path",
        severity="warning",
        target_kind="account",
        target="Assets:Vehicles:Toy",
        summary="x", detail=None,
        proposed_fix=fix_payload(action="close"),
        alternatives=(),
        confidence="low", source="x",
    )
    with pytest.raises(HealRefused):
        heal_schema_drift(
            bogus, conn=empty_db, settings=settings, reader=reader,
        )


def test_refuses_unregistered_axis_version_pair(tmp_path, empty_db):
    """A schema_drift finding with an axis+version pair we don't
    have a Migration for refuses. Lets future-Phase-5 register
    additional pairs without making the heal action dispatch
    silently broken."""
    paths = _make_ledger(tmp_path)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    target = "ledger:5:6"
    f = Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary="x", detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="ledger",
            from_version="5", to_version=6,
        ),
        alternatives=(),
        confidence="high", source="detect_schema_drift",
    )
    with pytest.raises(HealRefused) as exc:
        heal_schema_drift(
            f, conn=empty_db, settings=settings, reader=reader,
        )
    assert "no migration registered" in str(exc.value)


# ---------------------------------------------------------------------------
# SQLite axis (catch-up)
# ---------------------------------------------------------------------------


def test_sqlite_axis_apply_runs_db_migrate(tmp_path):
    """Apply a sqlite catch-up Finding against a fresh DB and verify
    every migration in ``migrations/`` ends up recorded.

    Why we don't simulate a 'partially migrated DB' here: deleting
    rows from ``schema_migrations`` doesn't roll back the schema
    changes those migrations made (columns added, tables created),
    so a re-apply hits 'duplicate column name' on the second pass.
    The honest test is 'fresh DB → catch up to head'; the failure-
    classification path is covered separately via the boom-stub test
    below."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    # No migrate() — heal action runs it.

    paths = _make_ledger(tmp_path, stamp=True)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    from lamella.core.db import _migration_files

    expected_head = max(v for v, _n, _s in _migration_files())

    target = f"sqlite:0:{expected_head}"
    f = Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift", severity="blocker", target_kind="schema",
        target=target, summary="x", detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="sqlite",
            from_version=0, to_version=expected_head,
        ),
        alternatives=(),
        confidence="high", source="detect_schema_drift",
    )

    result = heal_schema_drift(
        f, conn=db, settings=settings, reader=reader,
    )

    assert result.success, result.message
    new_head = db.execute(
        "SELECT MAX(version) AS v FROM schema_migrations"
    ).fetchone()["v"]
    assert new_head == expected_head
    db.close()


def test_sqlite_axis_failure_uses_classifier(tmp_path):
    """Stub the migration's apply() to raise; verify HealResult
    routes the exception through ``failure_message_for`` rather
    than letting the trace bubble out."""
    from lamella.features.recovery.migrations import (
        find_for_finding,
    )

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)

    paths = _make_ledger(tmp_path, stamp=True)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    target = "sqlite:0:99"
    f = Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift", severity="blocker", target_kind="schema",
        target=target, summary="x", detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="sqlite",
            from_version=0, to_version=99,
        ),
        alternatives=(),
        confidence="high", source="detect_schema_drift",
    )

    migration = find_for_finding(f)
    real = migration.apply

    def boom(conn, settings):
        raise sqlite3.IntegrityError("UNIQUE constraint failed: x.y")

    migration.apply = boom
    try:
        result = heal_schema_drift(
            f, conn=db, settings=settings, reader=reader,
        )
    finally:
        migration.apply = real

    assert not result.success
    assert "constraint" in result.message.lower()
    db.close()


def test_sqlite_axis_no_op_when_in_sync(tmp_path, empty_db):
    """A sqlite Finding fed when the DB is actually in sync (e.g.
    raced with another tab) should still succeed as a no-op —
    db.migrate finds nothing pending and returns []."""
    paths = _make_ledger(tmp_path, stamp=True)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    head = empty_db.execute(
        "SELECT MAX(version) AS v FROM schema_migrations"
    ).fetchone()["v"]

    target = f"sqlite:{head}:{head + 1}"
    f = Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift", severity="blocker", target_kind="schema",
        target=target, summary="x", detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis="sqlite",
            from_version=head, to_version=head + 1,
        ),
        alternatives=(),
        confidence="high", source="detect_schema_drift",
    )
    result = heal_schema_drift(
        f, conn=empty_db, settings=settings, reader=reader,
    )
    assert result.success


# ---------------------------------------------------------------------------
# Ledger axis (v0 → v1)
# ---------------------------------------------------------------------------


def test_ledger_axis_apply_writes_version_stamp(tmp_path, empty_db):
    """Apply v0→v1 on a stampless ledger. Verify (a) the stamp lands
    in main.bean, (b) HealResult.success=True, (c) reader.invalidate
    was called so the next detect_schema_drift on this ledger sees
    no drift."""
    paths = _make_ledger(tmp_path)  # no stamp
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    # Pre-condition: detector reports ledger drift.
    pre_findings = detect_schema_drift(empty_db, reader.load().entries)
    pre_ledger = [f for f in pre_findings if f.target.startswith("ledger:")]
    assert len(pre_ledger) >= 1
    finding = pre_ledger[0]

    result = heal_schema_drift(
        finding, conn=empty_db, settings=settings, reader=reader,
    )
    assert result.success, result.message
    assert reader.invalidate_count >= 1

    # The stamp directive landed in main.bean.
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    new_main = paths["main"].read_text(encoding="utf-8")
    assert (
        f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
        in new_main
    )


def test_ledger_axis_apply_idempotent(tmp_path, empty_db):
    """Two consecutive applies don't double-stamp. The second write
    finds the stamp already there and short-circuits."""
    paths = _make_ledger(tmp_path)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    findings = detect_schema_drift(empty_db, reader.load().entries)
    ledger_finding = next(
        f for f in findings if f.target.startswith("ledger:")
    )

    r1 = heal_schema_drift(
        ledger_finding, conn=empty_db, settings=settings, reader=reader,
    )
    assert r1.success

    # Second apply on a now-stamped ledger.
    r2 = heal_schema_drift(
        ledger_finding, conn=empty_db, settings=settings, reader=reader,
    )
    assert r2.success
    # Stamp count stays at 1.
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    stamp = f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
    text = paths["main"].read_text(encoding="utf-8")
    assert text.count(stamp) == 1


def test_ledger_axis_bean_check_failure_rolls_back(tmp_path, empty_db):
    """A bean-check that returns errors after the migration writes
    must trigger snapshot rollback of every declared path. main.bean
    should contain no version stamp after rollback."""
    paths = _make_ledger(tmp_path)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    findings = detect_schema_drift(empty_db, reader.load().entries)
    ledger_finding = next(
        f for f in findings if f.target.startswith("ledger:")
    )

    pre_main = paths["main"].read_text(encoding="utf-8")

    def fake_bean_check(_path):
        return ["fabricated bean-check error: rollback should kick in"]

    result = heal_schema_drift(
        ledger_finding,
        conn=empty_db, settings=settings, reader=reader,
        bean_check=fake_bean_check,
    )
    assert not result.success
    assert "bean-check failed" in result.message

    # File rolled back byte-identically.
    post_main = paths["main"].read_text(encoding="utf-8")
    assert post_main == pre_main
    assert 'custom "lamella-ledger-version"' not in post_main


def test_ledger_axis_failure_message_uses_classifier(tmp_path, empty_db):
    """An OSError raised mid-apply gets translated by the
    Migration's failure_message_for, not bubbled up as a stack
    trace string. Test by monkey-patching the migration's apply
    method to raise."""
    from lamella.features.recovery.migrations import find_for_finding

    paths = _make_ledger(tmp_path)
    reader = _Reader(paths["main"])
    settings = _Settings(paths)

    findings = detect_schema_drift(empty_db, reader.load().entries)
    ledger_finding = next(
        f for f in findings if f.target.startswith("ledger:")
    )

    migration = find_for_finding(ledger_finding)
    real_apply = migration.apply

    def boom(conn, settings):
        raise PermissionError(13, "permission denied", "/ledger/main.bean")

    migration.apply = boom
    try:
        result = heal_schema_drift(
            ledger_finding, conn=empty_db, settings=settings, reader=reader,
        )
    finally:
        migration.apply = real_apply

    assert not result.success
    # The classifier turns PermissionError into a "permission denied" string.
    assert "permission denied" in result.message.lower()
