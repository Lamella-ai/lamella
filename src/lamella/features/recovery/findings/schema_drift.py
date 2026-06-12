# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Schema-drift detector — first non-warning category in /setup/recovery.

Phase 5.1 of SETUP_IMPLEMENTATION.md. Compares two independent
schema axes against the running code's expected state:

- **SQLite axis.** Code expects every ``migrations/*.sql`` file to be
  applied. The DB's ``schema_migrations`` table records what's been
  run. Drift = max(applied) < max(available_on_disk). A deploy that
  installed a code release containing migration 060 but hasn't booted
  yet to replay it lands here.

- **Ledger axis.** Code constant
  ``lamella.core.bootstrap.detection.LATEST_LEDGER_VERSION`` is what the
  current source assumes. The ledger's
  ``custom "lamella-ledger-version"`` directive (under
  ``connector_config.bean``) is the recorded state. Drift =
  recorded < expected, OR recorded is missing on a populated ledger.

The two axes fail independently — a deploy can be one SQLite
migration ahead and on the same ledger version, or vice versa, or
both. The detector emits one Finding per drifting axis. Distinct
``target`` strings (``"sqlite:<from>:<to>"`` vs.
``"ledger:<from>:<to>"``) yield distinct ids via ``make_finding_id``,
so simultaneous drifts don't collide in the Phase 6 repair-state
overlay.

Fresh-empty ledgers (no version stamp AND no content) are
``STRUCTURALLY_EMPTY`` — the wizard handles that, not recovery.
We skip flagging them here so a just-scaffolded install doesn't
get a "ledger version drift" finding before the user has even
written a transaction.

Detector contract is pure: ``(conn, entries) -> tuple[Finding, ...]``.
No writes, no DB mutations. Multiple calls on the same input return
the same Findings (same ids) so the Phase 6 overlay is stable
across reboots.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Iterable

from beancount.core import data as bdata

from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
from lamella.features.recovery.models import (
    Finding,
    fix_payload,
    make_finding_id,
)


__all__ = ["detect_schema_drift"]


# Sentinel target prefixes. These also appear inside the ``target``
# string and inside ``proposed_fix["axis"]`` so heal actions can
# dispatch without re-parsing the target.
AXIS_SQLITE = "sqlite"
AXIS_LEDGER = "ledger"


def detect_schema_drift(
    conn: sqlite3.Connection,
    entries: Iterable[Any],
) -> tuple[Finding, ...]:
    """Compare both schema axes against the code's expected state.

    Returns:
        Tuple of Findings, in axis order (SQLite first, ledger
        second). Empty tuple when both axes match expectation.
    """
    findings: list[Finding] = []

    sqlite_finding = _sqlite_axis(conn)
    if sqlite_finding is not None:
        findings.append(sqlite_finding)

    ledger_finding = _ledger_axis(entries)
    if ledger_finding is not None:
        findings.append(ledger_finding)

    return tuple(findings)


# --- SQLite axis ----------------------------------------------------------


def _sqlite_axis(conn: sqlite3.Connection) -> Finding | None:
    """Compare the ``schema_migrations`` head to the latest .sql file
    on disk. The expected head is the max numeric version across
    ``migrations/*.sql``; the applied head is the max recorded in
    ``schema_migrations``.

    Returns ``None`` when (a) the table doesn't exist (uninitialized
    DB — wizard territory, not recovery), (b) no .sql files are
    discoverable (deployment misconfiguration; surfaced elsewhere),
    or (c) applied and expected agree. Returns ``None`` even when
    applied > expected — that's a downgrade, which Phase 5 doesn't
    auto-heal (different shape than forward migration; the user
    would need to upgrade their image, not run a migration).
    """
    expected = _expected_sqlite_head()
    if expected is None:
        return None

    applied = _applied_sqlite_head(conn)
    if applied is None:
        return None  # uninitialized DB — wizard concern

    if applied >= expected:
        return None  # in sync OR downgrade (out of Phase 5 scope)

    target = f"{AXIS_SQLITE}:{applied}:{expected}"
    summary = (
        f"SQLite schema is at migration {applied}; code expects "
        f"migration {expected}."
    )
    detail = (
        f"The `schema_migrations` table records migration {applied} as "
        f"the highest applied, but the running code ships "
        f"`migrations/{expected:03d}_*.sql` (and "
        f"{expected - applied} pending migration"
        f"{'s' if expected - applied != 1 else ''}). "
        "This typically means the app was just upgraded and a boot "
        "cycle is needed to replay the new SQL. Apply runs the "
        "pending migrations in order, each in its own SQLite "
        "transaction; any failure rolls back to before that "
        "migration started."
    )
    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary=summary,
        detail=detail,
        proposed_fix=fix_payload(
            action="migrate",
            axis=AXIS_SQLITE,
            from_version=applied,
            to_version=expected,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


def _expected_sqlite_head() -> int | None:
    """Highest migration version present in the migrations dir.
    ``None`` when the dir is missing or empty (deployment problem;
    not our category to flag)."""
    try:
        # Reuse the file walker that ``db.migrate`` consumes —
        # single source of truth for what counts as a migration.
        from lamella.core.db import _migration_files

        files = _migration_files()
    except Exception:  # noqa: BLE001 — defensive, never user-visible
        return None
    if not files:
        return None
    return max(version for version, _name, _sql in files)


def _applied_sqlite_head(conn: sqlite3.Connection) -> int | None:
    """Highest version in ``schema_migrations``. ``None`` when the
    table doesn't exist (which means the DB hasn't been migrated
    even once — uninitialized, not drifting)."""
    try:
        row = conn.execute(
            "SELECT MAX(version) AS v FROM schema_migrations"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    head = row["v"] if isinstance(row, sqlite3.Row) else row[0]
    if head is None:
        return None
    return int(head)


# --- ledger axis ----------------------------------------------------------


def _ledger_axis(entries: Iterable[Any]) -> Finding | None:
    """Compare the ledger's version stamp to ``LATEST_LEDGER_VERSION``.

    Three outcomes:

    - Stamp present, value matches: no drift.
    - Stamp present, value < expected: drift (numeric).
    - Stamp absent on a ledger with content (Transaction / Balance /
      Pad): drift (missing stamp). The "no stamp + no content"
      case is structural emptiness — wizard concern, not ours.

    Returns ``None`` when the stamp is ahead of the code constant
    (downgrade — same scope-out as the SQLite axis).
    """
    entries_list = list(entries)
    current = _extract_ledger_version(entries_list)

    if current is not None and current >= LATEST_LEDGER_VERSION:
        return None

    has_content = any(
        isinstance(e, (bdata.Transaction, bdata.Balance, bdata.Pad))
        for e in entries_list
    )

    if current is None and not has_content:
        return None  # structurally empty — wizard handles

    from_repr = "none" if current is None else str(current)
    target = f"{AXIS_LEDGER}:{from_repr}:{LATEST_LEDGER_VERSION}"

    if current is None:
        summary = (
            f"Ledger has no `lamella-ledger-version` stamp; code expects "
            f"v{LATEST_LEDGER_VERSION}."
        )
        detail = (
            "The ledger has transactions but no "
            "`custom \"lamella-ledger-version\"` directive in "
            "`connector_config.bean`. The stamp isn't tax-relevant — "
            "it's the schema-version marker that lets future migrations "
            "run safely. Apply writes the directive at the current code "
            f"version (v{LATEST_LEDGER_VERSION}). The ledger's content "
            "is not modified."
        )
    else:
        summary = (
            f"Ledger is at v{current}; code expects v{LATEST_LEDGER_VERSION}."
        )
        detail = (
            f"The ledger's `lamella-ledger-version` directive reads "
            f"v{current}, but the running code ships v{LATEST_LEDGER_VERSION}. "
            f"{LATEST_LEDGER_VERSION - current} ledger migration"
            f"{'s' if LATEST_LEDGER_VERSION - current != 1 else ''} "
            "must run to bring the layout up to date. Each migration "
            "writes inside a bean-snapshot envelope — bean-check failure "
            "rolls the ledger back byte-identically before the version "
            "stamp advances."
        )

    return Finding(
        id=make_finding_id("schema_drift", target),
        category="schema_drift",
        severity="blocker",
        target_kind="schema",
        target=target,
        summary=summary,
        detail=detail,
        proposed_fix=fix_payload(
            action="migrate",
            axis=AXIS_LEDGER,
            from_version=from_repr,
            to_version=LATEST_LEDGER_VERSION,
        ),
        alternatives=(),
        confidence="high",
        source="detect_schema_drift",
    )


def _extract_ledger_version(entries: list[Any]) -> int | None:
    """Mirror ``bootstrap.detection._extract_ledger_version`` — kept
    inline rather than imported so the detector survives a refactor
    that moves the helper. Walks Custom directives looking for
    ``lamella-ledger-version`` (or the legacy ``bcg-ledger-version``)
    and returns the integer value, or ``None`` when no parseable stamp
    is present."""
    for e in entries:
        if not isinstance(e, bdata.Custom):
            continue
        if e.type not in ("lamella-ledger-version", "bcg-ledger-version"):
            continue
        if not e.values:
            continue
        raw = e.values[0]
        if hasattr(raw, "value"):
            raw = raw.value
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None
