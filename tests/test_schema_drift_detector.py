# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``detect_schema_drift`` — Phase 5.1 of /setup/recovery.

Pins down: (a) two-axis comparison (SQLite + ledger) yields
independent findings, (b) distinct ``target`` strings produce
distinct ids, (c) detail text spells out which axis failed, (d)
version-ahead is silently a no-op (downgrade is out of scope),
(e) freshly empty ledger doesn't get flagged, (f) idempotency —
same input yields same Finding ids on repeated calls.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest
from beancount.core import data, flags
from beancount.core.amount import Amount

from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
from lamella.features.recovery.findings.schema_drift import (
    AXIS_LEDGER,
    AXIS_SQLITE,
    detect_schema_drift,
)
from lamella.core.db import migrate


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    migrate(db)
    yield db
    db.close()


@pytest.fixture
def empty_conn():
    """DB without any migrations applied — ``schema_migrations``
    doesn't exist. Mimics first-boot uninitialized state."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    yield db
    db.close()


def _meta(filename: str = "x.bean", lineno: int = 1) -> dict:
    return {"filename": filename, "lineno": lineno}


class _V:
    """Wrapper used by beancount 3.x to box Custom values in a
    ``.value`` attribute (mirrors the loader's behavior)."""

    def __init__(self, v):
        self.value = v


def _stamp(version: int) -> data.Custom:
    return data.Custom(
        meta=_meta("connector_config.bean"),
        date=date(2026, 1, 1),
        type="lamella-ledger-version",
        values=[_V(version)],
    )


def _txn(account: str = "Expenses:Personal:Misc") -> data.Transaction:
    return data.Transaction(
        meta=_meta(),
        date=date(2026, 1, 1),
        flag=flags.FLAG_OKAY,
        payee=None,
        narration="x",
        tags=set(),
        links=set(),
        postings=[
            data.Posting(
                account=account,
                units=Amount(Decimal("1"), "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


# --- helpers --------------------------------------------------------------


def _set_sqlite_head(conn: sqlite3.Connection, head: int) -> None:
    """Force the migrated DB to look as though only migrations
    ≤ ``head`` had been applied. The ``migrate()`` fixture leaves
    every available migration recorded; rewinding the table lets us
    simulate "code shipped a new migration but boot-replay hasn't
    caught up yet"."""
    conn.execute("DELETE FROM schema_migrations WHERE version > ?", (head,))
    conn.commit()


# --- Happy path: no drift -------------------------------------------------


def test_no_drift_returns_empty_tuple(conn):
    """All migrations applied + ledger stamped at expected version =
    no findings. The detector returns an empty tuple, not a Finding
    list with zero rows."""
    findings = detect_schema_drift(conn, [_stamp(LATEST_LEDGER_VERSION)])
    assert findings == ()


def test_empty_ledger_with_current_sqlite_no_drift(conn):
    """A freshly scaffolded ledger has no transactions and no
    version stamp. That's STRUCTURALLY_EMPTY — wizard's job, not
    recovery's. Detector must not flag it."""
    findings = detect_schema_drift(conn, [])
    assert findings == ()


# --- SQLite axis ----------------------------------------------------------


def test_sqlite_axis_drift_one_behind(conn):
    """One pending migration → one Finding with high confidence,
    blocker severity, schema target_kind, axis=sqlite in proposed_fix."""
    expected = _max_migration_version()
    _set_sqlite_head(conn, expected - 1)

    findings = detect_schema_drift(conn, [_stamp(LATEST_LEDGER_VERSION)])
    assert len(findings) == 1
    f = findings[0]
    assert f.category == "schema_drift"
    assert f.severity == "blocker"
    assert f.target_kind == "schema"
    assert f.confidence == "high"
    assert f.source == "detect_schema_drift"
    assert f.target == f"{AXIS_SQLITE}:{expected - 1}:{expected}"
    fix = f.proposed_fix_dict
    assert fix["action"] == "migrate"
    assert fix["axis"] == AXIS_SQLITE
    assert fix["from_version"] == expected - 1
    assert fix["to_version"] == expected
    assert f.alternatives == ()


def test_sqlite_axis_detail_names_the_axis(conn):
    """Per the locked spec: detail text must spell out which axis
    failed. A user reading 'schema drift' alone can't tell which
    knob to turn — the message has to say SQLite."""
    expected = _max_migration_version()
    _set_sqlite_head(conn, expected - 2)

    findings = detect_schema_drift(conn, [_stamp(LATEST_LEDGER_VERSION)])
    f = findings[0]
    assert "SQLite" in f.summary or "migration" in f.summary
    assert "schema_migrations" in f.detail
    assert f"migrations/{expected:03d}" in f.detail


def test_sqlite_axis_two_behind(conn):
    """Two pending migrations still surfaces as one finding (the
    finding represents 'this axis is N behind'). Plurality gets
    the s in the detail string."""
    expected = _max_migration_version()
    _set_sqlite_head(conn, expected - 2)

    findings = detect_schema_drift(conn, [_stamp(LATEST_LEDGER_VERSION)])
    assert len(findings) == 1
    assert "2 pending migrations" in findings[0].detail


def test_sqlite_axis_ahead_silently_ok(conn):
    """If applied > expected (e.g. dev DB carrying a yet-unshipped
    migration), the detector is silent. Phase 5 doesn't auto-heal
    downgrades — that requires upgrading the image, not running a
    migration."""
    expected = _max_migration_version()
    conn.execute(
        "INSERT INTO schema_migrations (version) VALUES (?)",
        (expected + 5,),
    )
    conn.commit()

    findings = detect_schema_drift(conn, [_stamp(LATEST_LEDGER_VERSION)])
    sqlite_findings = [f for f in findings if f.target.startswith(AXIS_SQLITE)]
    assert sqlite_findings == []


def test_uninitialized_db_does_not_flag_sqlite(empty_conn):
    """No ``schema_migrations`` table → DB hasn't been migrated even
    once → uninitialized, not drifting. Wizard's job."""
    findings = detect_schema_drift(empty_conn, [_stamp(LATEST_LEDGER_VERSION)])
    sqlite_findings = [f for f in findings if f.target.startswith(AXIS_SQLITE)]
    assert sqlite_findings == []


# --- Ledger axis ----------------------------------------------------------


def test_ledger_axis_drift_numeric(conn):
    """Ledger stamped at v(n-1), code expects vn. Same shape as the
    SQLite drift finding but with axis=ledger and the ``from`` value
    spelled out."""
    if LATEST_LEDGER_VERSION < 1:
        pytest.skip("LATEST_LEDGER_VERSION must be ≥1 for this test")

    older = LATEST_LEDGER_VERSION - 1
    findings = detect_schema_drift(conn, [_stamp(older), _txn()])
    ledger_findings = [f for f in findings if f.target.startswith(AXIS_LEDGER)]
    assert len(ledger_findings) == 1
    f = ledger_findings[0]
    assert f.severity == "blocker"
    assert f.target_kind == "schema"
    assert f.target == f"{AXIS_LEDGER}:{older}:{LATEST_LEDGER_VERSION}"
    fix = f.proposed_fix_dict
    assert fix["axis"] == AXIS_LEDGER
    assert fix["from_version"] == str(older)
    assert fix["to_version"] == LATEST_LEDGER_VERSION


def test_ledger_axis_drift_missing_stamp_with_content(conn):
    """Ledger has transactions but no stamp directive → drift in the
    'absent stamp' direction. ``from`` is the literal string 'none'
    so the target string is non-ambiguous."""
    findings = detect_schema_drift(conn, [_txn()])
    ledger_findings = [f for f in findings if f.target.startswith(AXIS_LEDGER)]
    assert len(ledger_findings) == 1
    f = ledger_findings[0]
    assert f.target == f"{AXIS_LEDGER}:none:{LATEST_LEDGER_VERSION}"
    assert f.proposed_fix_dict["from_version"] == "none"
    # Summary or detail must call out the missing stamp explicitly.
    assert "no `lamella-ledger-version`" in f.summary
    assert "lamella-ledger-version" in f.detail


def test_ledger_axis_detail_names_the_axis(conn):
    """Same locked-spec requirement as the SQLite axis — detail must
    say which axis. The string 'ledger' or the directive name should
    appear so a user reading the detail can tell."""
    if LATEST_LEDGER_VERSION < 1:
        pytest.skip("LATEST_LEDGER_VERSION must be ≥1")
    older = LATEST_LEDGER_VERSION - 1
    findings = detect_schema_drift(conn, [_stamp(older), _txn()])
    f = next(f for f in findings if f.target.startswith(AXIS_LEDGER))
    assert "lamella-ledger-version" in f.detail
    assert (
        "ledger" in f.summary.lower() or "ledger" in f.detail.lower()
    )


def test_ledger_axis_ahead_silently_ok(conn):
    """Ledger stamped at vN+5 vs. code expecting vN — silent (same
    rationale as the SQLite axis: downgrades aren't Phase 5 scope)."""
    findings = detect_schema_drift(
        conn, [_stamp(LATEST_LEDGER_VERSION + 5), _txn()],
    )
    ledger_findings = [f for f in findings if f.target.startswith(AXIS_LEDGER)]
    assert ledger_findings == []


def test_ledger_axis_no_stamp_no_content_silent(conn):
    """A ledger with neither a version stamp nor any content is
    structurally empty. The wizard's job — recovery doesn't fire."""
    findings = detect_schema_drift(conn, [])
    ledger_findings = [f for f in findings if f.target.startswith(AXIS_LEDGER)]
    assert ledger_findings == []


# --- Both axes simultaneously ---------------------------------------------


def test_both_axes_drift_yields_distinct_findings(conn):
    """Worst case: deploy is one SQLite migration behind AND one
    ledger version behind. Two findings, both blockers, distinct
    ids, distinct targets. They share a category but the id helper
    folds (category, target) into the hash so collision is
    impossible by construction."""
    if LATEST_LEDGER_VERSION < 1:
        pytest.skip("LATEST_LEDGER_VERSION must be ≥1")

    expected_sqlite = _max_migration_version()
    _set_sqlite_head(conn, expected_sqlite - 1)
    older = LATEST_LEDGER_VERSION - 1

    findings = detect_schema_drift(conn, [_stamp(older), _txn()])
    assert len(findings) == 2
    ids = {f.id for f in findings}
    assert len(ids) == 2  # distinct
    targets = {f.target for f in findings}
    assert any(t.startswith(AXIS_SQLITE) for t in targets)
    assert any(t.startswith(AXIS_LEDGER) for t in targets)


def test_findings_emitted_in_axis_order(conn):
    """SQLite axis first, then ledger. Locked so the route can
    render them in a predictable order without re-sorting."""
    if LATEST_LEDGER_VERSION < 1:
        pytest.skip("LATEST_LEDGER_VERSION must be ≥1")

    expected_sqlite = _max_migration_version()
    _set_sqlite_head(conn, expected_sqlite - 1)
    older = LATEST_LEDGER_VERSION - 1

    findings = detect_schema_drift(conn, [_stamp(older), _txn()])
    assert findings[0].target.startswith(AXIS_SQLITE)
    assert findings[1].target.startswith(AXIS_LEDGER)


# --- Idempotency ----------------------------------------------------------


def test_idempotent_same_input_same_ids(conn):
    """A detector that returns different ids on identical input
    breaks the Phase 6 repair-state overlay (the overlay keys by
    id; non-stable ids = stale overlays). This is a property test
    — same conn, same entries, two calls."""
    if LATEST_LEDGER_VERSION < 1:
        pytest.skip("LATEST_LEDGER_VERSION must be ≥1")

    expected_sqlite = _max_migration_version()
    _set_sqlite_head(conn, expected_sqlite - 1)
    older = LATEST_LEDGER_VERSION - 1
    entries = [_stamp(older), _txn()]

    a = detect_schema_drift(conn, entries)
    b = detect_schema_drift(conn, entries)
    assert tuple(f.id for f in a) == tuple(f.id for f in b)


# --- helper ---------------------------------------------------------------


def _max_migration_version() -> int:
    from lamella.core.db import _migration_files

    files = _migration_files()
    assert files, "expected at least one migration file"
    return max(v for v, _n, _s in files)
