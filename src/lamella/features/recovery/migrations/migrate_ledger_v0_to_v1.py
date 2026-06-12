# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Ledger v0 → LATEST: backfill SQLite state, rewrite bcg-* on disk, stamp.

Phase 5.3 of SETUP_IMPLEMENTATION.md, extended for the v1→v2 cutover.
Wraps ``lamella.core.transform.migrate_to_ledger`` (which copies
dismissals, rules, budgets, paperless field mappings, recurring
confirmations, day reviews, and non-secret settings from SQLite into
the matching ``custom "..."`` directives), follows with the on-disk
``bcg-*`` → ``lamella-*`` rewrite, and finishes by stamping
``lamella-ledger-version`` at ``LATEST_LEDGER_VERSION``.

"v0" is "no version stamp present, possibly with backfillable SQLite
state lying around (and possibly bcg-* keys on disk if the ledger
predates the rebrand)". After apply(), the ledger reads as
``LATEST_LEDGER_VERSION`` and ``detect_schema_drift`` returns no
ledger-axis finding on the next call.

Recompute-shape: the dry-run reports per-step counts (how many
directives WOULD be written + how many bcg-* references exist) —
same shape ``transform/migrate_to_ledger`` already supports. Honest
preview is impossible without a scratch copy of the ledger, so the
route layer surfaces a confirm step before apply (per the locked
spec).

Class name kept as ``MigrateLedgerV0ToV1`` for stability of import
paths and external test references; the registered keys point at
``LATEST_LEDGER_VERSION`` so detection→heal dispatch lines up with
whatever the current schema head is.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from lamella.features.recovery.migrations.base import (
    DryRunResult,
    Migration,
    register_migration,
)
from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION
from lamella.core.config import Settings


__all__ = ["MigrateLedgerV0ToV1"]


def _stamp_directive() -> str:
    """Build the version-stamp directive at the current LATEST. Kept
    as a function (vs module-level constant) so the directive picks up
    a future LATEST bump without a separate edit here."""
    return (
        f"\n; Ledger schema version — marks this ledger as "
        f"lamella-managed (v{LATEST_LEDGER_VERSION}).\n"
        f'2020-01-01 custom "lamella-ledger-version" '
        f'"{LATEST_LEDGER_VERSION}"\n'
    )


@register_migration(keys=(("none", LATEST_LEDGER_VERSION),
                          ("0", LATEST_LEDGER_VERSION)))
class MigrateLedgerV0ToV1(Migration):
    """Ledger axis: from absent stamp (v0) to LATEST.

    Three responsibilities:

    1. **Backfill SQLite state to ledger directives.** Delegates to
       ``transform.migrate_to_ledger.run(conn, settings, apply=True)``.
       Idempotent — already-stamped directives are skipped, so a
       second apply() after a partial failure is safe.
    2. **Rewrite ``bcg-*`` → ``lamella-*``** on disk in every ``.bean``
       file. A v0 ledger predates the lamella-* rebrand, so there
       may be legacy keys / tags / custom types lying around that
       need to be cleaned up before we stamp the modern version.
    3. **Stamp ``lamella-ledger-version "{LATEST}"``** in main.bean.
       Skipped if a stamp at LATEST already exists; updated in place
       if a stamp at an older version is present (e.g. the rewrite
       turned a bcg-ledger-version "1" into a lamella one).

    All writes happen inside the heal-action's bean-snapshot
    envelope; if any step or the post-apply bean-check fails,
    the snapshot restores every declared file byte-identically.

    Class name kept as ``MigrateLedgerV0ToV1`` for stability of
    external imports.
    """

    AXIS = "ledger"
    SUPPORTS_DRY_RUN = True

    # --- declared paths ---------------------------------------------------

    def declared_paths(self, settings: Settings) -> tuple[Path, ...]:
        """Every ``.bean`` file the migration may touch.

        v0→LATEST writes to:
        - main.bean (stamp).
        - Connector-owned files (the migrate_to_ledger backfill).
        - Every ``.bean`` under the ledger root (bcg-* rewrite — bcg
          keys can land in user-authored files, not just connector
          ones).

        We declare the union so the snapshot envelope protects
        anything we may write. Listing is sorted/deduped so the heal
        action's audit pass sees a stable set across runs."""
        explicit = {
            settings.ledger_main,
            settings.connector_links_path,
            settings.connector_rules_path,
            settings.connector_budgets_path,
            settings.connector_config_path,
        }
        all_bean = set(settings.ledger_dir.rglob("*.bean"))
        return tuple(sorted(explicit | all_bean))

    # --- dry-run ----------------------------------------------------------

    def dry_run(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> DryRunResult:
        """Count what a ``--apply`` run would write per step. Pure
        — no writes happen here."""
        from lamella.core.transform.migrate_to_ledger import run
        from lamella.core.transform.bcg_to_lamella import rewrite_text

        counts = run(conn, settings, apply=False)
        backfill_total = sum(counts.values())

        # Bcg-* substitution count, scanned across every .bean.
        bcg_total = 0
        for path in settings.ledger_dir.rglob("*.bean"):
            try:
                original = path.read_text(encoding="utf-8")
            except OSError:
                continue
            _, count = rewrite_text(original)
            bcg_total += count

        if backfill_total == 0 and bcg_total == 0:
            summary = (
                f"Apply will only write the v{LATEST_LEDGER_VERSION} "
                "version stamp."
            )
            detail = (
                "All applicable SQLite state already has matching "
                "ledger directives, no `bcg-*` references remain on "
                "disk, and no migratable rows exist. The migration "
                f"will append the `lamella-ledger-version "
                f'"{LATEST_LEDGER_VERSION}"` stamp to main.bean and '
                "finish."
            )
        else:
            lines = ["The migration will write:"]
            for step, n in counts.items():
                if n:
                    lines.append(f"- {n} {step}")
            if bcg_total:
                lines.append(
                    f"- {bcg_total} `bcg-*` reference"
                    f"{'s' if bcg_total != 1 else ''} rewritten "
                    "across .bean files"
                )
            lines.append(
                f"\nPlus the `lamella-ledger-version "
                f'"{LATEST_LEDGER_VERSION}"` stamp in main.bean.'
            )
            detail = "\n".join(lines)
            parts = []
            if backfill_total:
                parts.append(
                    f"{backfill_total} directive"
                    f"{'s' if backfill_total != 1 else ''}"
                )
            if bcg_total:
                parts.append(
                    f"{bcg_total} bcg-* rewrite"
                    f"{'s' if bcg_total != 1 else ''}"
                )
            summary = (
                f"{', '.join(parts)}, plus the v{LATEST_LEDGER_VERSION} "
                "version stamp."
            )

        return DryRunResult(
            kind="recompute",
            summary=summary,
            detail=detail,
            counts={**counts, "bcg_rewrites": bcg_total},
        )

    # --- apply ------------------------------------------------------------

    def apply(
        self, conn: sqlite3.Connection, settings: Settings,
    ) -> None:
        """Run the backfill, rewrite legacy keys, mint lineage, stamp.

        Order matters: backfill → bcg rewrite → lineage stamp →
        version stamp. If any step raises, the snapshot envelope
        rolls every declared file back; the version stays stale so
        Resume re-runs the full migration end-to-end instead of
        leaving a half-stamped state.
        """
        from lamella.core.transform.migrate_to_ledger import run
        from lamella.core.transform.bcg_to_lamella import rewrite_text
        from lamella.core.transform import normalize_txn_identity
        from lamella.features.recovery.migrations.base import (
            MigrationError,
        )

        # Step 1: backfill SQLite state into ledger directives.
        run(conn, settings, apply=True)

        # Step 2: rewrite bcg-* → lamella-* in every .bean file under
        # the ledger root. Idempotent — files with no remaining bcg-*
        # rewrite to themselves byte-identically.
        for path in sorted(settings.ledger_dir.rglob("*.bean")):
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

        # Step 3: stamp ``lamella-txn-id`` (UUIDv7) on every entry that
        # lacks one. v3+ requires lineage on every transaction so the
        # /txn/{token} resolver can be UUID-only. Idempotent — entries
        # already carrying lineage are left alone.
        if LATEST_LEDGER_VERSION >= 3:
            result = normalize_txn_identity.run(
                settings, apply=True, db_conn=conn, run_check=True,
            )
            if not result.applied:
                raise MigrationError(
                    "normalize_txn_identity refused to apply: "
                    f"{result.bean_check_error or 'unknown error'}"
                )

        # Step 3: stamp the version directive in main.bean. Three cases:
        # (a) main has no stamp at all → append a fresh LATEST stamp.
        # (b) main has an older lamella-ledger-version stamp (e.g. "1"
        #     after the rewrite turned bcg-ledger-version into the
        #     modern type) → bump the value in place.
        # (c) main already has the LATEST stamp (re-run after partial
        #     failure) → skip.
        main = settings.ledger_main
        if not main.is_file():
            raise MigrationError(
                f"main.bean is missing at {main} — cannot stamp version"
            )
        existing = main.read_text(encoding="utf-8")
        latest_stamp = (
            f'custom "lamella-ledger-version" "{LATEST_LEDGER_VERSION}"'
        )
        if latest_stamp in existing:
            return  # case (c)

        # Case (b): replace any older numeric stamp value with LATEST.
        # Match any single-character version digit; if a future bump
        # crosses 9 this needs a regex, but until then exact-string
        # replace is sufficient and obvious.
        bumped = False
        for old in range(LATEST_LEDGER_VERSION):
            old_stamp = f'custom "lamella-ledger-version" "{old}"'
            if old_stamp in existing:
                existing = existing.replace(old_stamp, latest_stamp, 1)
                bumped = True
                break
        if bumped:
            main.write_text(existing, encoding="utf-8")
            return

        # Case (a): no stamp present — append a fresh LATEST directive.
        main.write_text(existing + _stamp_directive(), encoding="utf-8")
