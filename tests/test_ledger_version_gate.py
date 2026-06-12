# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Ledger-version bump gate.

Bumping ``LATEST_LEDGER_VERSION`` is never just a constant change —
the v3→v4 bump landed without updating the test surface and left the
suite red for weeks (migration-registry pins, the shared fixture
ledger's version stamp, the schema-drift route's in-sync state).

This module makes that mistake impossible to merge quietly. If you
are here because the pinned assertion below failed, work through the
checklist — every line is something the last bump actually missed:

  1. Write + register the ``MigrateLedgerV(N-1)ToV(N)`` migration in
     ``src/lamella/features/recovery/migrations/`` (the structural
     tests below also enforce this).
  2. Update ``tests/test_recovery_migration_registry.py`` — the
     known-keys set, the distinct-count, the unregistered-jump
     example, and add a dispatch test for the new step.
  3. Bump the ``lamella-ledger-version`` stamp in
     ``tests/fixtures/ledger/main.bean`` (also enforced below) —
     otherwise every app_client test boots into schema-drift state
     and ``test_setup_schema_route`` fails.
  4. Add a ``tests/test_recovery_migration_v(N-1)_to_v(N).py``
     covering the new migration's dry-run + apply.
  5. Update the pin here, last.
"""
from __future__ import annotations

import re
from pathlib import Path

from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
from lamella.features.recovery.migrations import _LEDGER_REGISTRY

_FIXTURE_MAIN = Path(__file__).parent / "fixtures" / "ledger" / "main.bean"


def test_latest_ledger_version_is_pinned():
    """Explicit pin. A failure here means LATEST_LEDGER_VERSION
    changed — read the module docstring checklist before touching
    this assertion."""
    assert LATEST_LEDGER_VERSION == 4, (
        "LATEST_LEDGER_VERSION changed. Do NOT just update this "
        "number — work through the checklist in this module's "
        "docstring first (migration, registry tests, fixture stamp)."
    )


def test_fixture_ledger_stamp_matches_latest():
    """The shared fixture ledger must always be stamped at the
    current schema head, or every app_client test silently boots
    into schema-drift / needs-migration state."""
    text = _FIXTURE_MAIN.read_text(encoding="utf-8")
    m = re.search(r'custom "lamella-ledger-version" "(\d+)"', text)
    assert m, "fixture ledger is missing its lamella-ledger-version stamp"
    assert int(m.group(1)) == LATEST_LEDGER_VERSION, (
        f"tests/fixtures/ledger/main.bean is stamped v{m.group(1)} but "
        f"LATEST_LEDGER_VERSION is {LATEST_LEDGER_VERSION} — bump the "
        "fixture stamp as part of the version bump (see the checklist "
        "in this module's docstring)."
    )


def test_migration_exists_from_previous_version():
    """Every version bump ships a registered migration from the
    previous head, or upgrading users have no auto-heal path."""
    prev = str(LATEST_LEDGER_VERSION - 1)
    assert (prev, LATEST_LEDGER_VERSION) in _LEDGER_REGISTRY, (
        f"no migration registered for ledger v{prev} → "
        f"v{LATEST_LEDGER_VERSION}. Write and @register_migration it "
        "before (or with) the LATEST_LEDGER_VERSION bump."
    )


def test_v0_backfill_targets_latest():
    """The v0/none backfill registers against LATEST dynamically —
    if a bump forgot to re-point it, fresh-recovery dispatch breaks."""
    assert ("none", LATEST_LEDGER_VERSION) in _LEDGER_REGISTRY
    assert ("0", LATEST_LEDGER_VERSION) in _LEDGER_REGISTRY
