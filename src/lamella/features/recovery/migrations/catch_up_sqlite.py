# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""SQLite axis: catch up to the latest ``migrations/*.sql`` head.

Phase 5.3 of SETUP_IMPLEMENTATION.md. Wraps :func:`lamella.core.db.migrate`
which already does sequential atomic application — each migration
file runs inside its own ``BEGIN/COMMIT`` block, so a failure mid-batch
leaves earlier migrations committed and never starts later ones. This
matches Shape 1 (sequential atomic units) per the locked spec without
extra plumbing.

Singleton: there's exactly one of these. The registry returns it
for any (axis='sqlite', from_version, to_version) Finding — the
underlying ``db.migrate`` figures out what to apply by querying
``schema_migrations`` and comparing against the ``migrations/``
directory.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from lamella.features.recovery.migrations.base import (
    DryRunResult,
    Migration,
    register_migration,
)
from lamella.core.config import Settings


__all__ = ["CatchUpSqliteMigrations"]


@register_migration()
class CatchUpSqliteMigrations(Migration):
    """Run every pending ``migrations/*.sql`` file in version order.

    Per-file atomicity is provided by ``db.migrate``'s
    ``BEGIN/COMMIT`` wrapping. The recovery shell's job is just to
    invoke it — the heal-action doesn't add a SQLite tx envelope on
    top because that would cause a nested transaction error.
    """

    AXIS = "sqlite"
    SUPPORTS_DRY_RUN = True

    def declared_paths(self, settings: Settings) -> tuple[Path, ...]:
        """SQLite-only migration: doesn't touch any ``.bean`` file.
        Empty declared set → the heal action skips the snapshot
        envelope entirely. If a future SQL migration needs to touch
        the ledger as a side effect, that's a different Migration
        subclass — not this one."""
        return ()

    # --- dry-run ----------------------------------------------------------

    def dry_run(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> DryRunResult:
        """List every pending migration filename. Most SQLite
        migrations are additive (CREATE TABLE / ALTER ADD COLUMN);
        we tag the kind as ``additive`` so the UI can render
        "this adds N items, no existing data is modified" rather
        than the recompute-style "cannot be previewed" hedge."""
        pending = self._pending_versions(conn)
        if not pending:
            return DryRunResult(
                kind="additive",
                summary="No pending SQLite migrations.",
                detail=None,
            )

        lines = [f"Will apply {len(pending)} pending migration"
                 f"{'s' if len(pending) != 1 else ''}:"]
        for version, name in pending:
            lines.append(f"- `{name}`")
        detail = "\n".join(lines)
        return DryRunResult(
            kind="additive",
            summary=(
                f"{len(pending)} pending migration"
                f"{'s' if len(pending) != 1 else ''} to apply."
            ),
            detail=detail,
            counts={"pending": len(pending)},
        )

    # --- apply ------------------------------------------------------------

    def apply(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> None:
        """Replay pending SQL files. ``db.migrate`` returns the list
        of applied versions; we don't need it (the heal action's
        result message comes from the dry_run + a follow-up read of
        ``schema_migrations``)."""
        from lamella.core.db import migrate

        migrate(conn)

    # --- helpers ----------------------------------------------------------

    def _pending_versions(
        self, conn: sqlite3.Connection,
    ) -> list[tuple[int, str]]:
        """Versions present in ``migrations/*.sql`` but absent from
        ``schema_migrations``. Empty list when in sync."""
        from lamella.core.db import _migration_files

        try:
            applied = {
                row["version"] if isinstance(row, sqlite3.Row) else row[0]
                for row in conn.execute(
                    "SELECT version FROM schema_migrations"
                ).fetchall()
            }
        except sqlite3.OperationalError:
            applied = set()
        out: list[tuple[int, str]] = []
        for version, name, _sql in _migration_files():
            if version not in applied:
                out.append((version, name))
        out.sort(key=lambda r: r[0])
        return out
