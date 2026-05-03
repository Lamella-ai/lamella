# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Ledger v2 → v3: every Transaction carries ``lamella-txn-id`` on disk.

v2 ledgers are clean of legacy ``bcg-*`` keys but still hold a mix of
entries: post-Phase-7 writes carry ``lamella-txn-id`` lineage, while
legacy / hand-edited entries don't. The /txn/{token} resolver compensated
by accepting either a UUIDv7 (lamella-txn-id) or the legacy hex
content-hash (txn_hash). That dual-format URL surface is the same shape
mistake as the bcg/lamella key duplication — two ways to refer to the
same thing.

v3 retires the hex form. Every Transaction MUST have ``lamella-txn-id``
at the transaction-meta level after this migration. Routes can now drop
hex acceptance entirely; new bookmarks emit UUID only.

Implementation reuses :func:`lamella.core.transform.normalize_txn_identity.run`
with ``apply=True``. That orchestrator already mints lineage on disk for
entries that lack one, normalizes legacy txn-level source keys to the
paired-indexed posting-level shape, snapshots before write, and rolls
back on bean-check regression. Wrapping it in a Migration subclass plugs
it into the same recovery-shell envelope every other migration runs in
(snapshot → apply → bean-check → commit/rollback owned by the heal
action, not the Migration itself).

Idempotent: a re-run on a v3 ledger produces zero changes — every entry
already has lineage. The dry-run reports 0 substitutions and the apply
path returns immediately.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from lamella.core.config import Settings
from lamella.core.transform import normalize_txn_identity
from lamella.features.recovery.migrations.base import (
    DryRunResult,
    Migration,
    MigrationError,
    register_migration,
)


__all__ = ["MigrateLedgerV2ToV3"]


log = logging.getLogger(__name__)


_STAMP_DIRECTIVE_V3 = (
    "\n; Ledger schema version — marks this ledger as lamella-managed (v3).\n"
    '2020-01-01 custom "lamella-ledger-version" "3"\n'
)


@register_migration(keys=(("2", 3),))
class MigrateLedgerV2ToV3(Migration):
    """Ledger axis: from v2 (mixed lineage) to v3 (every txn stamped).

    Two responsibilities:

    1. **Mint ``lamella-txn-id`` on every entry that lacks one.** Done
       by ``normalize_txn_identity.run(apply=True)``, which also
       cleans up legacy txn-level source keys (``lamella-simplefin-id``,
       ``lamella-import-txn-id``) by migrating them to the paired
       indexed posting-level shape. This is the shape the writers have
       emitted since Phase 7 — the migration brings legacy entries
       forward.
    2. **Bump the version stamp from "2" to "3"** in main.bean.
       Idempotent: if the file already has a v3 stamp we skip.

    Both writes happen inside the heal-action's bean-snapshot envelope.
    The transform module additionally takes its own snapshot before
    rewriting; nested snapshots are intentional belt-and-suspenders for
    a one-shot on-disk rewrite the user will run exactly once.
    """

    AXIS = "ledger"
    SUPPORTS_DRY_RUN = True

    # --- declared paths ---------------------------------------------------

    def declared_paths(self, settings: Settings) -> tuple[Path, ...]:
        """Every ``.bean`` file under the ledger root.

        Same scope as v1→v2 — entries needing lineage can land
        anywhere a user pasted them, not just connector-owned files.
        """
        return tuple(sorted(settings.ledger_dir.rglob("*.bean")))

    # --- dry-run ----------------------------------------------------------

    def dry_run(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> DryRunResult:
        """Plan-only pass through ``normalize_txn_identity.run``."""
        result = normalize_txn_identity.run(
            settings, apply=False, db_conn=None, run_check=False,
        )
        per_file: dict[str, int] = {}
        # The transform's TransformResult exposes per-pass counts but
        # not per-file numbers in the public dataclass — the per-file
        # detail lives in the planner output. For a dry-run summary
        # the totals are enough to set user expectation; the apply
        # logs the per-file counts.
        total = result.lineage_minted

        if total == 0:
            summary = (
                "Every transaction already carries `lamella-txn-id`. "
                "Apply will only bump the version stamp from v2 to v3."
            )
            detail = (
                "The migration walks every `.bean` file under the "
                "ledger root and mints `lamella-txn-id` for any "
                "transaction that lacks one. Nothing to mint here — "
                "apply will rewrite the `lamella-ledger-version` "
                'stamp in main.bean from `"2"` to `"3"` and finish.'
            )
        else:
            lines = [
                f"The migration will mint `lamella-txn-id` (UUIDv7) for "
                f"{total} transaction{'s' if total != 1 else ''} that "
                "currently lack one.",
            ]
            if result.simplefin_migrated:
                lines.append(
                    f"- Migrate {result.simplefin_migrated} legacy "
                    "txn-level SimpleFIN id"
                    f"{'s' if result.simplefin_migrated != 1 else ''} "
                    "to the paired posting-level source meta."
                )
            if result.csv_migrated:
                lines.append(
                    f"- Migrate {result.csv_migrated} legacy "
                    "txn-level CSV import id"
                    f"{'s' if result.csv_migrated != 1 else ''} "
                    "to the paired posting-level source meta."
                )
            lines.append(
                f"\nFiles touched: {result.files_changed} of "
                f"{result.files_planned} `.bean` files have at least "
                "one transaction needing the lineage stamp."
            )
            lines.append(
                "\nThe `lamella-ledger-version` stamp is bumped from "
                '`"2"` to `"3"` after the rewrite.'
            )
            detail = "\n".join(lines)
            summary = (
                f"Mint `lamella-txn-id` on {total} transaction"
                f"{'s' if total != 1 else ''} across "
                f"{result.files_changed} file"
                f"{'s' if result.files_changed != 1 else ''}, plus the "
                "v3 version stamp."
            )

        return DryRunResult(
            kind="rename",
            summary=summary,
            detail=detail,
            counts=per_file,
        )

    # --- apply ------------------------------------------------------------

    def apply(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> None:
        """Stamp lineage on every legacy entry, then bump the stamp.

        Order matters: stamp first (the transform module does its own
        snapshot + bean-check), then bump the version. Bean-check
        failure inside ``normalize_txn_identity.run`` rolls every
        ``.bean`` file back from its inner snapshot and surfaces the
        error here as a MigrationError; the heal-action's outer
        snapshot envelope is the second line of defense.
        """
        # Step 1: stamp lineage on every legacy entry.
        result = normalize_txn_identity.run(
            settings, apply=True, db_conn=conn, run_check=True,
        )
        if not result.applied:
            raise MigrationError(
                "normalize_txn_identity refused to apply: "
                f"{result.bean_check_error or 'unknown error'}"
            )

        # Step 2: bump the version stamp in main.bean. Three cases:
        #   (a) v3 stamp already present (interrupted apply re-run) → skip
        #   (b) v2 stamp present → in-place value bump preserves date /
        #       comments
        #   (c) no stamp at all → append a fresh v3 directive (defensive)
        main = settings.ledger_main
        if not main.is_file():
            raise MigrationError(
                f"main.bean is missing at {main} — cannot stamp version"
            )
        existing = main.read_text(encoding="utf-8")

        if 'custom "lamella-ledger-version" "3"' in existing:
            return

        if 'custom "lamella-ledger-version" "2"' in existing:
            updated = existing.replace(
                'custom "lamella-ledger-version" "2"',
                'custom "lamella-ledger-version" "3"',
                1,
            )
            main.write_text(updated, encoding="utf-8")
            return

        main.write_text(existing + _STAMP_DIRECTIVE_V3, encoding="utf-8")
