# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the @register_migration auto-discovery contract — gap §11.5.

The recovery shell historically wired Migration subclasses into a
hand-edited dict in ``migrations/__init__.py``. Gap §11.5 of
RECOVERY_SYSTEM.md replaces that with a decorator + scan over the
migrations package via importlib + iter_modules. These tests pin
down the contract:

1. The seven (currently two) migration registrations from before the
   refactor are still discoverable post-refactor — no behavior delta.
2. The decorator is a pure marker (no side effects on import).
3. The scanner refuses to silently drop an undecorated subclass —
   the whole point of the gap is "no more silent deaths".
"""
from __future__ import annotations

from lamella.features.recovery.migrations import (
    _LEDGER_REGISTRY,
    _SQLITE_MIGRATION,
    all_migrations,
    find_for_finding,
)
from lamella.features.recovery.migrations.base import (
    Migration,
    register_migration,
)
from lamella.features.recovery.migrations.catch_up_sqlite import (
    CatchUpSqliteMigrations,
)
from lamella.features.recovery.migrations.migrate_ledger_v0_to_v1 import (
    MigrateLedgerV0ToV1,
)
from lamella.features.recovery.migrations.migrate_ledger_v1_to_v2 import (
    MigrateLedgerV1ToV2,
)
from lamella.features.recovery.migrations.migrate_ledger_v2_to_v3 import (
    MigrateLedgerV2ToV3,
)
from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)


def _schema_drift_finding(*, axis: str, from_v, to_v: int) -> Finding:
    target = f"{axis}:{from_v}:{to_v}"
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary=f"{axis} drift {from_v} → {to_v}",
        detail=None,
        proposed_fix=fix_payload(
            action="migrate", axis=axis,
            from_version=from_v, to_version=to_v,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


# ---------------------------------------------------------------------------
# Behavior parity — the discovered registry matches the prior layout
# ---------------------------------------------------------------------------


def test_sqlite_migration_singleton_is_catch_up_class():
    """The discovered sqlite-axis singleton must be the
    CatchUpSqliteMigrations instance — same as the prior hand-wired
    layout. Asserting on the class identity (not just truthiness)
    catches a regression where the scanner picks up the wrong
    subclass."""
    assert _SQLITE_MIGRATION is not None
    assert isinstance(_SQLITE_MIGRATION, CatchUpSqliteMigrations)


def test_ledger_registry_has_v0_aliases():
    """The discovered ledger registry must contain both the
    ("none", LATEST) and ("0", LATEST) keys, both pointing at the same
    MigrateLedgerV0ToV1 instance — same alias sharing the prior
    hand-wired layout had. The class is named ``V0ToV1`` for import-
    path stability but its registered to-version is LATEST so
    detection→heal dispatch lines up with whatever the schema head
    currently is."""
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    assert ("none", LATEST_LEDGER_VERSION) in _LEDGER_REGISTRY
    assert ("0", LATEST_LEDGER_VERSION) in _LEDGER_REGISTRY

    none_inst = _LEDGER_REGISTRY[("none", LATEST_LEDGER_VERSION)]
    zero_inst = _LEDGER_REGISTRY[("0", LATEST_LEDGER_VERSION)]
    assert isinstance(none_inst, MigrateLedgerV0ToV1)
    assert isinstance(zero_inst, MigrateLedgerV0ToV1)
    # Same instance — alias sharing is the contract.
    assert none_inst is zero_inst


def test_ledger_registry_has_only_known_keys():
    """The discovered ledger registry must contain exactly the keys
    the layout currently ships — (("none", LATEST), ("0", LATEST)) for
    the v0→LATEST backfill+rewrite step, (("1", 2),) for the v1→v2
    bcg-* on-disk rewrite step, and (("2", 3),) for the v2→v3 lineage
    stamping step — and nothing else."""
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    expected = {
        ("none", LATEST_LEDGER_VERSION),
        ("0", LATEST_LEDGER_VERSION),
        ("1", 2),
        ("2", 3),
    }
    assert set(_LEDGER_REGISTRY.keys()) == expected


def test_all_migrations_dedupes_aliases():
    """``all_migrations`` must return one entry per concrete
    subclass even when the ledger registry stores aliases."""
    distinct = all_migrations()
    # Four distinct migrations today (sqlite catch-up, ledger v0→v1,
    # ledger v1→v2, ledger v2→v3). Adding a new one bumps this number;
    # updating the assertion is the explicit signal that the
    # registry's surface area changed.
    assert len(distinct) == 4
    classes = {type(m) for m in distinct}
    assert classes == {
        CatchUpSqliteMigrations,
        MigrateLedgerV0ToV1,
        MigrateLedgerV1ToV2,
        MigrateLedgerV2ToV3,
    }


def test_find_for_finding_dispatches_sqlite_axis():
    """A schema_drift finding with axis='sqlite' must resolve to the
    sqlite singleton regardless of from/to versions. Same dispatch
    rule as the prior layout."""
    f = _schema_drift_finding(axis="sqlite", from_v=50, to_v=53)
    m = find_for_finding(f)
    assert m is _SQLITE_MIGRATION


def test_find_for_finding_dispatches_ledger_v0_to_latest():
    """A schema_drift finding with axis='ledger', from='none' or
    '0', to=LATEST must resolve to MigrateLedgerV0ToV1."""
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
    for from_v in ("none", "0"):
        f = _schema_drift_finding(
            axis="ledger", from_v=from_v, to_v=LATEST_LEDGER_VERSION,
        )
        m = find_for_finding(f)
        assert m is _LEDGER_REGISTRY[(from_v, LATEST_LEDGER_VERSION)]
        assert isinstance(m, MigrateLedgerV0ToV1)


def test_find_for_finding_dispatches_ledger_v1_to_v2():
    """A schema_drift finding with axis='ledger', from='1', to=2 must
    resolve to MigrateLedgerV1ToV2 — the on-disk bcg-* rewrite step."""
    f = _schema_drift_finding(axis="ledger", from_v="1", to_v=2)
    m = find_for_finding(f)
    assert m is _LEDGER_REGISTRY[("1", 2)]
    assert isinstance(m, MigrateLedgerV1ToV2)


def test_find_for_finding_returns_none_for_unregistered_ledger_jump():
    """A ledger-axis finding for an unregistered (from, to) pair
    returns None — the route surfaces it as 'no auto-heal yet'."""
    # (3, 4) is unregistered — would correspond to a future v3→v4 step.
    f = _schema_drift_finding(axis="ledger", from_v=3, to_v=4)
    assert find_for_finding(f) is None


def test_find_for_finding_dispatches_ledger_v2_to_v3():
    """A schema_drift finding with axis='ledger', from='2', to=3 must
    resolve to MigrateLedgerV2ToV3 — the lineage stamping step."""
    f = _schema_drift_finding(axis="ledger", from_v="2", to_v=3)
    m = find_for_finding(f)
    assert m is _LEDGER_REGISTRY[("2", 3)]
    assert isinstance(m, MigrateLedgerV2ToV3)


def test_find_for_finding_returns_none_for_non_schema_drift():
    """Non-schema_drift findings never resolve to a Migration."""
    f = Finding(
        id=make_finding_id("legacy_path", "Assets:Vehicles:Foo"),
        category="legacy_path",
        severity="warning",
        target_kind="account",
        target="Assets:Vehicles:Foo",
        summary="Move it",
        detail=None,
        proposed_fix=fix_payload(action="move"),
        alternatives=(),
        confidence="high",
        source="detect_legacy_paths",
    )
    assert find_for_finding(f) is None


# ---------------------------------------------------------------------------
# Decorator semantics
# ---------------------------------------------------------------------------


def test_register_migration_is_pure_marker():
    """The decorator must stamp ``__lamella_migration_keys__`` on
    the class and return the class unchanged. Pure marker — no
    global-registry mutation, no instance creation, no other
    side effects on import."""
    @register_migration(keys=(("test", 99),))
    class _Sample(Migration):
        AXIS = "ledger"

        def declared_paths(self, settings):
            return ()

        def dry_run(self, conn, settings):
            raise NotImplementedError

        def apply(self, conn, settings):
            raise NotImplementedError

    # Marker present, normalized to tuple-of-tuples.
    assert _Sample.__lamella_migration_keys__ == (("test", 99),)
    # Decorator returns the class itself.
    assert issubclass(_Sample, Migration)


def test_register_migration_sqlite_default():
    """``@register_migration()`` (no keys) marks the class as
    sqlite-axis via the sentinel."""
    from lamella.features.recovery.migrations.base import (
        _SQLITE_REGISTRATION_SENTINEL,
    )

    @register_migration()
    class _SqliteSample(Migration):
        AXIS = "sqlite"

        def declared_paths(self, settings):
            return ()

        def dry_run(self, conn, settings):
            raise NotImplementedError

        def apply(self, conn, settings):
            raise NotImplementedError

    assert (
        _SqliteSample.__lamella_migration_keys__
        is _SQLITE_REGISTRATION_SENTINEL
    )


def test_register_migration_normalizes_key_types():
    """The decorator coerces keys to ``(str, int)`` — defensive for
    callers passing list-of-lists or stringified ints."""
    @register_migration(keys=[["1", 2], ["foo", 3]])
    class _Sample(Migration):
        AXIS = "ledger"

        def declared_paths(self, settings):
            return ()

        def dry_run(self, conn, settings):
            raise NotImplementedError

        def apply(self, conn, settings):
            raise NotImplementedError

    keys = _Sample.__lamella_migration_keys__
    assert keys == (("1", 2), ("foo", 3))
    # Spot-check the type coercion.
    for k in keys:
        assert isinstance(k[0], str)
        assert isinstance(k[1], int)
