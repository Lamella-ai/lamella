# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``detect_all`` — Phase 6.1.2.5 of /setup/recovery.

Pins down: (a) calls every registered detector, (b) order is
schema_drift first, legacy_paths second, (c) returning empty
when nothing is drifting, (d) materializes entries so multiple
detectors see the same view (consumes-once iterables don't
break later detectors), (e) a raising detector is logged + skipped,
not propagated, (f) idempotency — same input yields same Finding
ids on repeated calls.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest
from beancount.core import data, flags
from beancount.core.amount import Amount

from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
from lamella.features.recovery.findings import (
    DETECTORS,
    detect_all,
    detect_legacy_paths,
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


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _meta(filename: str = "x.bean", lineno: int = 1) -> dict:
    return {"filename": filename, "lineno": lineno}


def _open(account: str) -> data.Open:
    return data.Open(_meta(), date(2020, 1, 1), account, None, None)


def _txn(account: str = "Expenses:Personal:Misc") -> data.Transaction:
    return data.Transaction(
        meta=_meta(),
        date=date(2026, 1, 1),
        flag=flags.FLAG_OKAY,
        payee=None, narration="x",
        tags=set(), links=set(),
        postings=[data.Posting(
            account=account,
            units=Amount(Decimal("1"), "USD"),
            cost=None, price=None, flag=None, meta=None,
        )],
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_detectors_tuple_includes_known_detectors():
    """The DETECTORS registry exists and lists schema_drift +
    legacy_paths in the locked order."""
    assert detect_schema_drift in DETECTORS
    assert detect_legacy_paths in DETECTORS
    assert DETECTORS.index(detect_schema_drift) < DETECTORS.index(
        detect_legacy_paths,
    )


def test_detectors_tuple_is_immutable():
    """Tuple, not list — callers can't accidentally mutate the
    registry. (Adding a detector is an explicit code change.)"""
    assert isinstance(DETECTORS, tuple)


# ---------------------------------------------------------------------------
# Aggregation behavior
# ---------------------------------------------------------------------------


def test_detect_all_no_findings_returns_empty_tuple(conn):
    """Stamped ledger + fully-migrated DB + no legacy paths = no
    findings. The aggregator returns an empty tuple, not None."""
    # Stamp the ledger so schema_drift reports clean.
    class _V:
        def __init__(self, v): self.value = v
    stamp = data.Custom(
        meta=_meta("connector_config.bean"),
        date=date(2020, 1, 1),
        type="lamella-ledger-version",
        values=[_V(LATEST_LEDGER_VERSION)],
    )
    findings = detect_all(conn, [stamp])
    assert findings == ()


def test_detect_all_concatenates_per_detector_output(conn):
    """Mix one schema_drift finding + one legacy_path finding,
    verify both surface and order is detector-registration order."""
    # Schema-drift trigger: a transaction without a stamp.
    # Legacy-path trigger: an Assets:Vehicles:V* open directive.
    entries = [
        _open("Assets:Vehicles:V2008Fabrikam"),  # legacy path
        _txn(),  # ensures ledger isn't structurally empty for stamp check
    ]
    findings = detect_all(conn, entries)
    assert len(findings) >= 2
    # Schema drift first.
    categories = [f.category for f in findings]
    schema_idx = categories.index("schema_drift")
    legacy_idx = categories.index("legacy_path")
    assert schema_idx < legacy_idx


def test_detect_all_materializes_entries(conn):
    """A generator passed in is iterated multiple times — each
    detector should see the same view. The aggregator must
    materialize, not pass the iterator straight through."""
    def _entries_gen():
        yield _open("Assets:Vehicles:V2008Fabrikam")
        yield _txn()

    findings = detect_all(conn, _entries_gen())
    # Both detectors saw entries: legacy_path detector saw the
    # Open + posting, schema_drift detector saw the txn (so it
    # detects "missing stamp + has content" — ledger axis fires).
    categories = {f.category for f in findings}
    assert "schema_drift" in categories
    assert "legacy_path" in categories


def test_detect_all_idempotent(conn):
    """Phase 3 detector contract: same input → same Finding ids
    on repeated calls. The aggregator inherits that property; if
    it doesn't, Phase 6's setup_repair_state overlay breaks."""
    entries = [_open("Assets:Vehicles:V2008Fabrikam"), _txn()]
    a = detect_all(conn, entries)
    b = detect_all(conn, entries)
    assert tuple(f.id for f in a) == tuple(f.id for f in b)


# ---------------------------------------------------------------------------
# Resilience
# ---------------------------------------------------------------------------


def test_raising_detector_is_skipped_not_propagated(conn, monkeypatch, caplog):
    """A buggy detector returning an exception should not poison
    detect_all's output for the rest of the registry. Log loudly,
    skip, continue."""
    def _broken_detector(conn, entries):
        raise RuntimeError("simulated detector bug")

    # Patch DETECTORS to insert a broken one in front of the real ones.
    import lamella.features.recovery.findings as findings_mod
    monkeypatch.setattr(
        findings_mod,
        "DETECTORS",
        (_broken_detector,) + findings_mod.DETECTORS,
    )

    entries = [_open("Assets:Vehicles:V2008Fabrikam"), _txn()]

    import logging
    with caplog.at_level(logging.ERROR):
        findings = detect_all(conn, entries)

    # The broken detector returned no findings; the real ones still
    # fired.
    categories = {f.category for f in findings}
    assert "legacy_path" in categories
    assert "schema_drift" in categories
    # Loud log captured.
    assert any(
        "_broken_detector" in r.message or "raised during detect_all" in r.message
        for r in caplog.records
    )
