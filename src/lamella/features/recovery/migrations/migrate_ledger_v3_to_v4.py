# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Ledger v3 → v4: receipt-* directive vocabulary generalizes to document-*.

Per ADR-0061 the v3 ``receipt-link`` / ``receipt-dismissed`` /
``receipt-link-blocked`` (and revoke / hash-backfill cousins)
directive types are renamed to ``document-*``. The v4 reader still
accepts every legacy form indefinitely; the v4 writer emits only
the new vocabulary. Existing receipt-* directives stay valid in a
migrated ledger and are rewritten opportunistically (the next time
a writer touches the file for any other reason — link, unlink,
dismiss — the new line is ``document-*``).

This migration is therefore stamp-only: it bumps the
``lamella-ledger-version`` directive in main.bean from ``"3"`` to
``"4"`` and does not touch any other file. The version bump exists
to prevent accidental downgrade — v3 software refuses to read a
v4 ledger rather than silently dropping unknown ``document-*``
directives.

Idempotent: a re-run on a v4 ledger produces zero changes.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from lamella.core.config import Settings
from lamella.features.recovery.migrations.base import (
    DryRunResult,
    Migration,
    MigrationError,
    register_migration,
)


__all__ = ["MigrateLedgerV3ToV4"]


log = logging.getLogger(__name__)


_STAMP_DIRECTIVE_V4 = (
    "\n; Ledger schema version — marks this ledger as lamella-managed (v4).\n"
    '2020-01-01 custom "lamella-ledger-version" "4"\n'
)


@register_migration(keys=(("3", 4),))
class MigrateLedgerV3ToV4(Migration):
    """Ledger axis: from v3 (receipt-* vocabulary) to v4 (document-*
    vocabulary; both readable, only document-* written going forward).

    One responsibility: bump the version stamp in main.bean. The
    ``custom "receipt-link"`` / ``custom "receipt-dismissed"`` /
    ``custom "receipt-link-blocked"`` directives in
    ``connector_links.bean`` (and their revoke/hash-backfill cousins)
    are NOT rewritten by this migration — readers accept them as
    aliases and the next write to the same file (a new link, an
    unlink, a dismissal) will naturally produce the ``document-*``
    form because the writers all emit the new vocabulary as of v4.

    See ADR-0061 §1 (opportunistic rewrite) for the rationale: an
    eager rewrite would produce a giant one-shot diff against years
    of receipt-link directives in users' connector_links.bean files.
    Lazy rewrite on touch keeps each commit small and reviewable.
    """

    AXIS = "ledger"
    SUPPORTS_DRY_RUN = True

    # --- declared paths ---------------------------------------------------

    def declared_paths(self, settings: Settings) -> tuple[Path, ...]:
        """Just main.bean — the only file we modify is the stamp."""
        return (settings.ledger_main,)

    # --- dry-run ----------------------------------------------------------

    def dry_run(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> DryRunResult:
        """Plan-only: report whether the stamp needs bumping."""
        main = settings.ledger_main
        existing = main.read_text(encoding="utf-8") if main.is_file() else ""

        if 'custom "lamella-ledger-version" "4"' in existing:
            return DryRunResult(
                kind="rename",
                summary="Ledger is already at v4 — nothing to do.",
                detail=(
                    "The `lamella-ledger-version` stamp in main.bean "
                    'already reads `"4"`. This migration is idempotent '
                    "and will exit immediately if applied."
                ),
                counts={},
            )

        return DryRunResult(
            kind="rename",
            summary="Bump `lamella-ledger-version` from `\"3\"` to `\"4\"`.",
            detail=(
                "The v3 → v4 cutover renames the receipt-* directive "
                "vocabulary to document-* (per ADR-0061). Readers accept "
                "both vocabularies indefinitely; writers emit only "
                "document-* going forward. This migration only updates "
                "the version stamp in main.bean — existing receipt-* "
                "directives in your connector_links.bean are not "
                "rewritten and remain valid. They will be replaced "
                "naturally over time as you link, unlink, or dismiss "
                "documents (each such action writes the new "
                "vocabulary)."
            ),
            counts={},
        )

    # --- apply ------------------------------------------------------------

    def apply(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> None:
        """Bump the version stamp. Three cases match v2→v3:

        1. v4 stamp already present (interrupted apply re-run) → skip.
        2. v3 stamp present → in-place value bump preserves date /
           comments.
        3. No stamp at all → append a fresh v4 directive (defensive;
           ``detect_ledger_state`` should have routed un-stamped
           ledgers to NEEDS_VERSION_STAMP before getting here).
        """
        main = settings.ledger_main
        if not main.is_file():
            raise MigrationError(
                f"main.bean is missing at {main} — cannot stamp version"
            )
        existing = main.read_text(encoding="utf-8")

        if 'custom "lamella-ledger-version" "4"' in existing:
            return

        if 'custom "lamella-ledger-version" "3"' in existing:
            updated = existing.replace(
                'custom "lamella-ledger-version" "3"',
                'custom "lamella-ledger-version" "4"',
                1,
            )
            main.write_text(updated, encoding="utf-8")
            return

        main.write_text(existing + _STAMP_DIRECTIVE_V4, encoding="utf-8")
