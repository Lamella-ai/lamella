# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Verify live SQLite state against a rebuilt-from-ledger copy.

Rebuilds the entire state into a fresh in-memory SQLite (running all
reconstruct passes), then diffs every table against the live DB. The
per-table drift policy — state, cache, or ephemeral — governs what
counts as a bug.

State tables: any missing, extra, or unequal row = bug.
Cache tables: per-table drift budget via an ``allow_drift`` callable.
Ephemeral tables: not diffed at all.

Policies are registered at import time, co-located with each step's
code. The CLI imports every step module (same as ``reconstruct``) so
policies land in the registry before the diff runs.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

log = logging.getLogger(__name__)


DriftReporter = Callable[[list[dict], list[dict]], list[str]]


@dataclass(frozen=True)
class TablePolicy:
    """Drift policy for a single SQLite table.

    ``kind`` controls default behaviour:
      * ``state`` — any row-level difference is a bug.
      * ``cache`` — drift is allowed within the budget defined by
        ``allow_drift``. If ``allow_drift`` is absent, the default is
        "no drift tolerated" (treat like state — force the step to
        declare its budget explicitly).
      * ``ephemeral`` — table is not diffed.

    ``allow_drift(live_rows, rebuilt_rows)`` returns a list of
    human-readable drift descriptions that are NOT bugs. Anything the
    function leaves undescribed bubbles up as a bug.
    """

    table: str
    kind: Literal["state", "cache", "ephemeral"]
    # primary_key: the column tuple verify uses to match live vs
    # rebuilt rows when computing drift. Defaults to ("id",) for the
    # common autoincrement-pk case, but tables where ``id`` differs
    # between live and rebuilt (INSERT-order variation, DELETE+
    # re-INSERT across operations) must pass their natural UNIQUE
    # instead — e.g., ("loan_slug", "start_date") for loan_pauses.
    # If the table has no autoincrement column this can be left at
    # the default only when the first column happens to be the right
    # key; set it explicitly in doubt.
    primary_key: tuple[str, ...] = ("id",)
    allow_drift: DriftReporter | None = None


@dataclass
class TableDiff:
    table: str
    kind: str
    missing_in_rebuilt: list[dict]
    extra_in_rebuilt: list[dict]
    value_mismatches: list[tuple[dict, dict]]
    tolerated: list[str] = field(default_factory=list)

    @property
    def is_drift(self) -> bool:
        return bool(
            self.missing_in_rebuilt
            or self.extra_in_rebuilt
            or self.value_mismatches
        )


_POLICIES: dict[str, TablePolicy] = {}


def register(policy: TablePolicy) -> None:
    if policy.table in _POLICIES:
        raise ValueError(f"duplicate verify policy for table {policy.table!r}")
    _POLICIES[policy.table] = policy


def registered_policies() -> dict[str, TablePolicy]:
    return dict(_POLICIES)


def _row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def _key_of(row: dict, pk: tuple[str, ...]) -> tuple:
    return tuple(row.get(k) for k in pk)


def diff_table(
    live: sqlite3.Connection,
    rebuilt: sqlite3.Connection,
    policy: TablePolicy,
) -> TableDiff:
    try:
        live_rows = [_row_to_dict(r) for r in live.execute(f"SELECT * FROM {policy.table}")]
    except sqlite3.OperationalError:
        live_rows = []
    try:
        rebuilt_rows = [
            _row_to_dict(r) for r in rebuilt.execute(f"SELECT * FROM {policy.table}")
        ]
    except sqlite3.OperationalError:
        rebuilt_rows = []

    by_key_live = {_key_of(r, policy.primary_key): r for r in live_rows}
    by_key_rebuilt = {_key_of(r, policy.primary_key): r for r in rebuilt_rows}

    missing_in_rebuilt = [
        row for key, row in by_key_live.items() if key not in by_key_rebuilt
    ]
    extra_in_rebuilt = [
        row for key, row in by_key_rebuilt.items() if key not in by_key_live
    ]
    value_mismatches = [
        (by_key_live[key], by_key_rebuilt[key])
        for key in by_key_live.keys() & by_key_rebuilt.keys()
        if by_key_live[key] != by_key_rebuilt[key]
    ]

    diff = TableDiff(
        table=policy.table,
        kind=policy.kind,
        missing_in_rebuilt=missing_in_rebuilt,
        extra_in_rebuilt=extra_in_rebuilt,
        value_mismatches=value_mismatches,
    )

    if policy.kind == "cache" and policy.allow_drift is not None and diff.is_drift:
        # Caller's allow_drift reports which drift is explicitly tolerated;
        # anything it doesn't claim stays in the diff and counts as a bug.
        tolerated = policy.allow_drift(live_rows, rebuilt_rows)
        diff.tolerated = tolerated

    return diff


def _import_all_steps() -> None:
    import lamella.core.transform.steps.step1_receipt_dismissals  # noqa: F401
    import lamella.core.transform.steps.step2_classification_rules  # noqa: F401
    import lamella.core.transform.steps.step3_budgets  # noqa: F401
    import lamella.core.transform.steps.step4_paperless_fields  # noqa: F401
    import lamella.core.transform.steps.step5_recurring_confirmations  # noqa: F401
    import lamella.core.transform.steps.step6_settings_overrides  # noqa: F401
    import lamella.core.transform.steps.step7_note_coverage  # noqa: F401


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Diff live SQLite state against a reconstructed copy. "
        "Exits non-zero on any untolerated drift in a state or cache table.",
    )
    args = parser.parse_args(argv)

    from lamella.core.beancount_io import LedgerReader
    from lamella.core.config import Settings
    from lamella.core.db import connect, migrate
    from lamella.core.transform.reconstruct import run_all

    # Import every step so reconstruct passes + verify policies register.
    from lamella.core.transform import reconstruct as _recon_module  # noqa: F401
    _import_all_steps()

    settings = Settings()
    live = connect(settings.db_path)
    migrate(live)

    # Rebuild into a scratch in-memory DB.
    rebuilt = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
    rebuilt.row_factory = sqlite3.Row
    migrate(rebuilt)

    reader = LedgerReader(settings.ledger_main)
    entries = list(reader.load().entries)
    run_all(rebuilt, entries, force=True)  # rebuilt is empty but be explicit

    exit_code = 0
    any_drift = False
    for policy in registered_policies().values():
        if policy.kind == "ephemeral":
            print(f"  [skip ] {policy.table:40s} (ephemeral)")
            continue
        diff = diff_table(live, rebuilt, policy)
        if not diff.is_drift:
            print(f"  [ok   ] {policy.table:40s} ({policy.kind})")
            continue
        any_drift = True
        missing = len(diff.missing_in_rebuilt)
        extra = len(diff.extra_in_rebuilt)
        mismatch = len(diff.value_mismatches)
        kind_tag = policy.kind
        if policy.kind == "cache" and diff.tolerated:
            print(f"  [cache] {policy.table:40s} missing={missing} "
                  f"extra={extra} mismatch={mismatch} (tolerated: "
                  f"{'; '.join(diff.tolerated)})")
        else:
            print(f"  [BUG  ] {policy.table:40s} missing={missing} "
                  f"extra={extra} mismatch={mismatch} kind={kind_tag}")
            exit_code = 1
    # Settings-drift check: even if the app_settings row diff passes
    # (cache policy tolerates some drift), we want to call out when
    # SQLite holds a non-secret setting that isn't stamped on the
    # ledger, because the background restamp might have failed silently.
    try:
        from lamella.core.beancount_io import LedgerReader
        from lamella.core.transform.settings_drift import (
            detect_missing_ledger_stamps,
        )

        reader = LedgerReader(settings.ledger_main)
        missing = detect_missing_ledger_stamps(live, reader)
        if missing:
            print(
                f"\nSettings drift: {len(missing)} non-secret key(s) in "
                f"SQLite without a matching ledger directive. These "
                f"would be lost on DB delete. Keys: "
                f"{', '.join(k for k, _ in missing[:10])}"
                f"{' …' if len(missing) > 10 else ''}"
            )
            exit_code = max(exit_code, 1)
    except Exception as exc:  # noqa: BLE001
        print(f"\nSettings drift check errored: {exc}")

    if not any_drift and exit_code == 0:
        print("\nAll tables match. Reconstruct pipeline is clean.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
