# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Ledger v1 → v2: rewrite legacy ``bcg-*`` keys/tags/types on disk.

v1 ledgers carry the legacy ``bcg-*`` metadata prefix that predates the
Lamella rebrand. The at-load shim in ``lamella.utils._legacy_meta``
rewrites them transparently in memory, so the app reads either prefix
correctly. This migration produces an on-disk v2 ledger where every
``.bean`` file has been rewritten to use the new prefix — the read-side
shim becomes belt-and-suspenders rather than load-bearing.

Scope — every ``.bean`` file under ``settings.ledger_dir``. Unlike the
``transform.bcg_to_lamella`` CLI, which only touches Connector-owned
files by design, this migration is the *coordinated* cutover: the user
explicitly authorized it via the ``/setup/recovery`` UI, the snapshot
envelope is in place, and bean-check rolls everything back if the
post-write parse regresses.

Idempotent: a file with no remaining ``bcg-*`` references rewrites to
itself byte-identically and the substitution counter stays at zero. A
re-run after a partial failure picks up wherever the rollback left off.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from lamella.features.recovery.migrations.base import (
    DryRunResult,
    Migration,
    MigrationError,
    register_migration,
)
from lamella.core.config import Settings
from lamella.core.transform.bcg_to_lamella import rewrite_text


__all__ = ["MigrateLedgerV1ToV2"]


# Stamp matches what `_stamp_directive_text` in routes/setup.py emits
# for the current LATEST_LEDGER_VERSION. Kept in sync intentionally so
# a ledger stamped via either path looks identical.
_STAMP_DIRECTIVE_V2 = (
    "\n; Ledger schema version — marks this ledger as lamella-managed (v2).\n"
    '2020-01-01 custom "lamella-ledger-version" "2"\n'
)


@register_migration(keys=(("1", 2),))
class MigrateLedgerV1ToV2(Migration):
    """Ledger axis: from v1 (bcg-* keys on disk) to v2 (clean lamella-*).

    Two responsibilities:

    1. **Rewrite ``bcg-*`` → ``lamella-*``** in every ``.bean`` file
       under ``settings.ledger_dir``. The three forms — metadata keys,
       transaction tags, custom directive types — are all handled by
       ``transform.bcg_to_lamella.rewrite_text``. The version directive
       itself (``custom "bcg-ledger-version" "1"``) is rewritten in the
       same pass to ``custom "lamella-ledger-version" "1"``.
    2. **Bump the version stamp from "1" to "2"** in main.bean. The
       previous step left the value at ``"1"``; this step rewrites the
       value. Idempotent: if a v2 stamp is already present we skip.

    Both writes happen inside the heal-action's bean-snapshot envelope;
    if any step or the post-apply bean-check fails, every declared file
    rolls back byte-identically.
    """

    AXIS = "ledger"
    SUPPORTS_DRY_RUN = True

    # --- declared paths ---------------------------------------------------

    def declared_paths(self, settings: Settings) -> tuple[Path, ...]:
        """Every ``.bean`` file under the ledger root.

        Unlike v0→v1 (which only touches connector-owned files), v1→v2
        must rewrite hand-authored files too — bcg-* keys can land
        anywhere a user pasted them in. The snapshot envelope protects
        the full set; writes outside this set are programmer error.
        """
        return tuple(sorted(settings.ledger_dir.rglob("*.bean")))

    # --- dry-run ----------------------------------------------------------

    def dry_run(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> DryRunResult:
        """Count substitutions per file. Pure — no writes happen here."""
        per_file: dict[str, int] = {}
        total = 0
        for path in self.declared_paths(settings):
            try:
                original = path.read_text(encoding="utf-8")
            except OSError:
                # Unreadable file — surface in apply, not here. The
                # dry-run shouldn't fail loudly; the user already
                # clicked through detection.
                continue
            _, count = rewrite_text(original)
            if count == 0:
                continue
            per_file[path.name] = count
            total += count

        if total == 0:
            summary = (
                "No `bcg-*` references on disk. Apply will only bump "
                "the version stamp from v1 to v2."
            )
            detail = (
                "Every `.bean` file under the ledger root is already "
                "free of legacy `bcg-*` keys, tags, and custom directive "
                "types. The migration will rewrite the "
                "`lamella-ledger-version` stamp in main.bean from "
                '`"1"` to `"2"` and finish.'
            )
        else:
            file_count = len(per_file)
            lines = [
                f"The migration will rewrite {total} `bcg-*` reference"
                f"{'s' if total != 1 else ''} across "
                f"{file_count} file{'s' if file_count != 1 else ''}:",
            ]
            for name in sorted(per_file):
                lines.append(f"- `{name}` — {per_file[name]} substitution"
                             f"{'s' if per_file[name] != 1 else ''}")
            lines.append(
                "\nThe `lamella-ledger-version` stamp is bumped from "
                '`"1"` to `"2"` after the rewrite.'
            )
            detail = "\n".join(lines)
            summary = (
                f"{total} substitution{'s' if total != 1 else ''} across "
                f"{file_count} file{'s' if file_count != 1 else ''}, "
                "plus the v2 version stamp."
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
        """Rewrite every .bean file, then bump the version stamp.

        Order matters: rewrite first (which incidentally turns
        ``custom "bcg-ledger-version" "1"`` into
        ``custom "lamella-ledger-version" "1"``), then bump the value
        from ``"1"`` to ``"2"``. If the rewrite raises mid-pass, the
        snapshot envelope rolls every file back; the version stays
        stale at v1 and a Resume click runs the full pass again.
        """
        # Step 1: rewrite every .bean file under the ledger root.
        for path in self.declared_paths(settings):
            try:
                original = path.read_text(encoding="utf-8")
            except OSError as exc:
                raise MigrationError(
                    f"could not read {path}: {exc}"
                ) from exc
            rewritten, count = rewrite_text(original)
            if count == 0:
                continue
            try:
                path.write_text(rewritten, encoding="utf-8")
            except OSError as exc:
                raise MigrationError(
                    f"could not write {path}: {exc}"
                ) from exc

        # Step 2: bump the version stamp in main.bean. Two cases handled:
        # (a) the rewrite turned a bcg- stamp into a lamella- stamp at v1
        #     → rewrite the value "1" → "2" in place.
        # (b) the file already has a lamella-version stamp at v2
        #     (interrupted apply re-run) → skip.
        # (c) no stamp present at all → append the v2 stamp directive.
        main = settings.ledger_main
        if not main.is_file():
            raise MigrationError(
                f"main.bean is missing at {main} — cannot stamp version"
            )
        existing = main.read_text(encoding="utf-8")

        if 'custom "lamella-ledger-version" "2"' in existing:
            return  # already stamped at v2, nothing to do

        if 'custom "lamella-ledger-version" "1"' in existing:
            # In-place version bump. Replace just the value to preserve
            # the user's chosen directive date and any surrounding
            # comments.
            updated = existing.replace(
                'custom "lamella-ledger-version" "1"',
                'custom "lamella-ledger-version" "2"',
                1,
            )
            main.write_text(updated, encoding="utf-8")
            return

        # No existing stamp at v1 or v2 — append a fresh v2 directive.
        # This shouldn't happen on a v1 ledger reaching this migration,
        # but handle defensively rather than leaving the file unstamped.
        main.write_text(existing + _STAMP_DIRECTIVE_V2, encoding="utf-8")
