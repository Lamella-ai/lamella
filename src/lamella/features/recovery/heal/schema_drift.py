# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Heal action for schema-drift findings.

Phase 5.4 of SETUP_IMPLEMENTATION.md. Looks up the Migration class
that handles the Finding's axis + version pair, runs ``apply()``
inside the appropriate envelope (bean-snapshot for ledger-touching
migrations; bare for SQLite-only ones), and returns a HealResult.

Per-finding atomicity. Each click runs one Migration. Failure
restores files byte-identically and the version stamp is unchanged.
The detector re-runs on the next page render and the same finding
reappears with the prior error captured as a banner above it.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from lamella.features.recovery.heal.legacy_paths import HealRefused
from lamella.features.recovery.migrations import (
    Migration,
    find_for_finding,
)
from lamella.features.recovery.models import Finding, HealResult
from lamella.features.recovery.snapshot import (
    BeanSnapshotCheckError,
    with_bean_snapshot,
)


__all__ = ["heal_schema_drift"]


_LOG = logging.getLogger(__name__)


def heal_schema_drift(
    finding: Finding,
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    bean_check: Any | None = None,
    bulk_context: Any | None = None,
) -> HealResult:
    """Run the Migration registered for ``finding`` and return its
    HealResult.

    Args:
        finding: a schema_drift Finding from ``detect_schema_drift``.
            Its proposed_fix carries ``axis``, ``from_version``,
            ``to_version`` — used to look up the Migration.
        conn: live SQLite connection. The Migration writes through
            this; the heal action does not start its own transaction
            (the underlying ``db.migrate`` and ``transform/migrate_to_ledger``
            already manage their commits).
        settings: ``lamella.core.config.Settings`` — Migration uses this
            to find ``ledger_main`` and the connector_*_path siblings.
        reader: a LedgerReader. Invalidated after a successful apply
            so subsequent reads see the new directives. Some
            heal-action callers also pass it to the Migration's
            dry_run for entry-loading; this action doesn't.
        bean_check: optional callable matching
            :data:`lamella.features.recovery.snapshot.BeanCheck`. When
            non-None, the snapshot envelope runs it after the
            migration's writes complete; failure rolls everything back.
            Pass ``None`` in tests where bean-check would be expensive
            to set up.
        bulk_context: present for API symmetry with ``heal_legacy_path``
            (Phase 6.1.3.5). Schema drift stays best-effort even when
            an orchestrator is calling — SQLite DDL committed by
            ``db.migrate``'s inner BEGIN/COMMIT can't be rolled back
            by an outer envelope, and the ledger-touching path's own
            snapshot envelope still provides per-finding atomicity.
            Currently ignored; the orchestrator routes the schema
            group through the best-effort path regardless.

    Returns:
        HealResult with success=True on apply success, success=False
        with a classified error message on failure. Snapshot rollback
        has already happened by the time False is returned.

    Raises:
        HealRefused: no Migration registered for this finding's axis
            + version pair, or the finding isn't a schema_drift
            finding. Caller turns this into a 4xx with the message.
    """
    if finding.category != "schema_drift":
        raise HealRefused(
            f"heal_schema_drift: not a schema_drift finding "
            f"(got category={finding.category!r})"
        )

    migration = find_for_finding(finding)
    if migration is None:
        fix = finding.proposed_fix_dict
        raise HealRefused(
            f"no migration registered for "
            f"axis={fix.get('axis')!r} "
            f"from={fix.get('from_version')!r} "
            f"to={fix.get('to_version')!r}. "
            "Phase 5 ships SQLite catch-up and ledger v0→v1 only."
        )

    declared = migration.declared_paths(settings)

    # Two execution paths:
    #
    # - Empty declared set (SQLite-only migration): run apply() raw.
    #   ``db.migrate`` provides per-file SQLite atomicity internally;
    #   wrapping it in our own envelope would either no-op (no .bean
    #   files declared) or accidentally serialize against a
    #   nested-transaction error.
    # - Non-empty declared set (ledger-touching migration): wrap in
    #   ``with_bean_snapshot``. The snapshot restores every declared
    #   path on exception or post-apply bean-check failure.

    if not declared:
        return _apply_bare(migration, finding, conn=conn, settings=settings, reader=reader)

    return _apply_with_snapshot(
        migration, finding,
        conn=conn, settings=settings, reader=reader,
        declared=declared, bean_check=bean_check,
    )


# --- bare apply (SQLite-only migration) -----------------------------------


def _apply_bare(
    migration: Migration,
    finding: Finding,
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
) -> HealResult:
    """Run a SQLite-only migration outside the snapshot envelope.

    Failure semantics: ``db.migrate`` commits per-file. A mid-batch
    raise leaves earlier migrations applied — the detector picks
    that up on next render and emits a finding for the new
    ``from_version``. Resume click runs only the still-pending ones.
    """
    try:
        migration.apply(conn, settings)
    except Exception as exc:  # noqa: BLE001 — translated to user message
        _LOG.exception("schema_drift heal failed (sqlite axis)")
        return HealResult(
            success=False,
            message=migration.failure_message_for(exc),
            files_touched=(),
            finding_id=finding.id,
        )

    # Reader caches by file mtime; SQLite-only migrations don't
    # touch any .bean file so an invalidate isn't strictly needed,
    # but it's cheap insurance against a subtle case where
    # ``db.migrate`` runs a SQL fixture that ends up triggering a
    # write through a reconstruct-style path.
    try:
        reader.invalidate()
    except AttributeError:
        # Test reader fakes may not implement invalidate.
        pass

    return HealResult(
        success=True,
        message=_summarize_apply(migration),
        files_touched=(),
        finding_id=finding.id,
    )


# --- snapshot apply (ledger-touching migration) ---------------------------


def _apply_with_snapshot(
    migration: Migration,
    finding: Finding,
    *,
    conn: sqlite3.Connection,
    settings: Any,
    reader: Any,
    declared: tuple[Path, ...],
    bean_check: Any | None,
) -> HealResult:
    """Run a ledger-touching migration inside ``with_bean_snapshot``.

    On exception or bean-check failure, every declared file is
    restored byte-identically and we return a HealResult with
    ``success=False`` carrying the classified error message.
    """
    main_bean = Path(settings.ledger_main)

    try:
        with with_bean_snapshot(
            declared,
            bean_check=bean_check,
            bean_check_path=main_bean,
        ) as snap:
            migration.apply(conn, settings)
            # The Migration writes through the writers (or transform
            # modules) that already track their own touched files —
            # we don't try to re-derive what changed. The snapshot's
            # restore set is fixed at entry and covers every declared
            # path regardless.
            for p in declared:
                snap.add_touched(p)
    except BeanSnapshotCheckError as exc:
        _LOG.warning("schema_drift bean-check failure: %s", exc)
        return HealResult(
            success=False,
            message=migration.failure_message_for(exc),
            files_touched=(),
            finding_id=finding.id,
        )
    except Exception as exc:  # noqa: BLE001 — translated to user message
        _LOG.exception("schema_drift heal failed (ledger axis)")
        return HealResult(
            success=False,
            message=migration.failure_message_for(exc),
            files_touched=(),
            finding_id=finding.id,
        )

    try:
        reader.invalidate()
    except AttributeError:
        pass

    return HealResult(
        success=True,
        message=_summarize_apply(migration),
        files_touched=declared,
        finding_id=finding.id,
    )


# --- summary --------------------------------------------------------------


def _summarize_apply(migration: Migration) -> str:
    """One-line user-facing message for a successful apply. Kept
    short — the full per-step breakdown is what the dry_run preview
    showed; success is the confirmation, not the report."""
    return f"Migration applied: {type(migration).__name__}."
