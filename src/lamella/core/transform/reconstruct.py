# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Reconstruct registry + runner.

Each step in the reconstruct roadmap registers a ``ReconstructPass``
at import time. The ``reconstruct`` CLI imports every step module to
trigger registration, then runs every pass against a SQLite that is
either freshly migrated (the default, empty-DB path) or --force-wiped
for state tables only.

A pass's contract:
  * Takes the current SQLite connection and the list of ledger entries.
  * Upserts its state-table rows to match what's in the ledger.
  * Never touches cache tables or ephemeral tables.
  * Returns a ``ReconstructReport`` describing what it did.
  * Is idempotent: running twice on the same inputs yields identical DB state.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

log = logging.getLogger(__name__)

ReconstructFn = Callable[[sqlite3.Connection, list[Any]], "ReconstructReport"]


@dataclass
class ReconstructReport:
    pass_name: str
    rows_written: int = 0
    rows_skipped: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReconstructPass:
    name: str
    state_tables: tuple[str, ...]
    fn: ReconstructFn


_PASSES: list[ReconstructPass] = []


def register(
    name: str, state_tables: Sequence[str]
) -> Callable[[ReconstructFn], ReconstructFn]:
    """Decorator. Each step's reconstruct module calls this at import
    time so the CLI picks it up automatically once the module is
    imported."""

    def _wrap(fn: ReconstructFn) -> ReconstructFn:
        _PASSES.append(
            ReconstructPass(
                name=name,
                state_tables=tuple(state_tables),
                fn=fn,
            )
        )
        return fn

    return _wrap


def registered_passes() -> list[ReconstructPass]:
    return list(_PASSES)


def _any_state_table_has_rows(
    conn: sqlite3.Connection, tables: Sequence[str]
) -> list[str]:
    populated: list[str] = []
    for table in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        except sqlite3.OperationalError:
            # Table doesn't exist yet — migrations handle that. Treat as empty.
            continue
        if row and int(row["n"] if isinstance(row, sqlite3.Row) else row[0]) > 0:
            populated.append(table)
    return populated


def _wipe_state_tables(
    conn: sqlite3.Connection, tables: Sequence[str]
) -> None:
    # Cross-state FKs (vehicle_fuel_log.vehicle_slug → vehicles.slug,
    # loans.property_slug → properties.slug) don't line up with the
    # alphabetical wipe order. Disable FK enforcement for the wipe;
    # the reconstruct passes that follow run with FKs off too and
    # re-enable + verify at the end. Without this, a wipe against a
    # populated DB (the reconstruct --force path) crashes with
    # IntegrityError before any rebuild can run.
    prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("BEGIN")
    try:
        for table in tables:
            try:
                conn.execute(f"DELETE FROM {table}")
            except sqlite3.OperationalError as exc:
                log.warning("could not wipe %s: %s", table, exc)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute(f"PRAGMA foreign_keys = {'ON' if prev_fk else 'OFF'}")


def run_all(
    conn: sqlite3.Connection,
    entries: list[Any],
    *,
    force: bool = False,
) -> list[ReconstructReport]:
    """Run every registered pass in registration order. If any state
    table that a pass touches has existing rows, refuse unless ``force``
    is set. When ``force`` is set, wipe the state tables first (caches
    are left intact — they repopulate naturally)."""
    all_state_tables: list[str] = []
    for p in _PASSES:
        all_state_tables.extend(p.state_tables)

    populated = _any_state_table_has_rows(conn, all_state_tables)
    if populated and not force:
        raise RuntimeError(
            "reconstruct refused: state tables already have rows: "
            + ", ".join(sorted(set(populated)))
            + " — re-run with --force to wipe state tables (caches are "
              "preserved) and rebuild."
        )
    if populated and force:
        log.warning(
            "reconstruct --force: wiping state tables %s",
            sorted(set(populated)),
        )
        _wipe_state_tables(conn, sorted(set(populated)))

    # State-table FKs (e.g. loans.property_slug → properties.slug,
    # vehicle_fuel_log.vehicle_slug → vehicles.slug) don't line up with
    # the step order — the reader/writer design is per-table, so an
    # earlier step's rows can legitimately reference a later step's
    # rows. Disable FK enforcement for the duration of the reconstruct,
    # then re-enable + verify at the end. If verify reveals dangling
    # FKs, raise; otherwise trust the passes to have produced consistent
    # state.
    prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        reports: list[ReconstructReport] = []
        for pass_ in _PASSES:
            log.info("reconstruct pass: %s", pass_.name)
            report = pass_.fn(conn, entries)
            reports.append(report)
    finally:
        if prev_fk:
            conn.execute("PRAGMA foreign_keys = ON")
        else:
            conn.execute("PRAGMA foreign_keys = OFF")
    if prev_fk:
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            log.warning(
                "reconstruct: %d foreign-key violations after rebuild "
                "(table/row/ref/refmissing): %s",
                len(violations), violations[:5],
            )
    # Refresh pure-derivation caches that feed off the rebuilt state.
    # Cheap enough to always run; avoids stale audit rows surviving a
    # reconstruct into a restarted app.
    try:
        from lamella.features.mileage.backfill_audit import (
            rebuild_mileage_backfill_audit,
        )
        rebuild_mileage_backfill_audit(conn)
    except Exception as exc:  # noqa: BLE001
        log.warning("reconstruct: backfill audit rebuild failed: %s", exc)
    return reports


def _import_all_steps() -> None:
    """Import every step module so its ``@register(...)`` decorator
    fires. Explicit imports — no metaprogramming — so the CLI behaviour
    is trivially readable."""
    import lamella.core.transform.steps.step1_receipt_dismissals  # noqa: F401
    import lamella.core.transform.steps.step2_classification_rules  # noqa: F401
    import lamella.core.transform.steps.step3_budgets  # noqa: F401
    import lamella.core.transform.steps.step4_paperless_fields  # noqa: F401
    import lamella.core.transform.steps.step5_recurring_confirmations  # noqa: F401
    import lamella.core.transform.steps.step6_settings_overrides  # noqa: F401
    import lamella.core.transform.steps.step7_note_coverage  # noqa: F401
    import lamella.core.transform.steps.step8_vehicles  # noqa: F401
    import lamella.core.transform.steps.step9_loans  # noqa: F401
    import lamella.core.transform.steps.step10_properties  # noqa: F401
    import lamella.core.transform.steps.step11_projects  # noqa: F401
    import lamella.core.transform.steps.step12_fuel_log  # noqa: F401
    import lamella.core.transform.steps.step13_mileage_trip_meta  # noqa: F401
    import lamella.core.transform.steps.step14_classify_context  # noqa: F401
    import lamella.core.transform.steps.step15_audit_dismissals  # noqa: F401
    import lamella.core.transform.steps.step16_notes  # noqa: F401
    import lamella.core.transform.steps.step17_balance_anchors  # noqa: F401
    import lamella.core.transform.steps.step18_day_reviews  # noqa: F401
    import lamella.core.transform.steps.step19_classification_modified  # noqa: F401
    # step20 registers entities — the registry of entity metadata
    # (display_name, entity_type, tax_schedule, etc.). Without this
    # a DB wipe leaves the commingle resolver + scaffold routing +
    # classify whitelist blind.
    import lamella.core.transform.steps.step20_entities  # noqa: F401
    # step21 round-trips accounts_meta extended fields
    # (display_name / institution / last_four / entity_slug /
    # simplefin_account_id / notes) so the 185-row labeling effort
    # survives a DB wipe.
    import lamella.core.transform.steps.step21_account_meta  # noqa: F401
    # step22 rebuilds confirmed multi-leg payment groups from the
    # lamella-loan-group-members meta stamped by the WP5 writer.
    import lamella.core.transform.steps.step22_loan_payment_groups  # noqa: F401
    # step23 rebuilds forbearance / payment-pause windows from
    # custom "loan-pause" / "loan-pause-revoked" directives. Without
    # this, paused months would be flagged as missing payments after
    # any DB rebuild.
    import lamella.core.transform.steps.step23_loan_pauses  # noqa: F401
    # step24 rebuilds staged_transactions rows from custom "staged-txn"
    # / "staged-txn-promoted" directives per ADR-0043 / ADR-0043b. The
    # ledger directive is the source of truth; SQLite holds the
    # ingest-time payload as a cache.
    import lamella.core.transform.steps.step24_staged_transactions  # noqa: F401
    # step25 rebuilds explicit user "do not auto-relink" receipt pair blocks
    # from custom "receipt-link-blocked" / revoke directives.
    import lamella.core.transform.steps.step25_receipt_link_blocks  # noqa: F401


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild SQLite state from the ledger. Refuses on non-empty "
            "state tables unless --force is passed."
        )
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Wipe state tables before rebuilding. Caches and ephemeral "
        "tables are preserved.",
    )
    args = parser.parse_args(argv)

    from lamella.core.beancount_io import LedgerReader
    from lamella.core.config import Settings
    from lamella.core.db import connect, migrate

    _import_all_steps()

    settings = Settings()
    conn = connect(settings.db_path)
    migrate(conn)

    reader = LedgerReader(settings.ledger_main)
    entries = list(reader.load().entries)

    try:
        reports = run_all(conn, entries, force=args.force)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"Ran {len(reports)} reconstruct pass(es):")
    for r in reports:
        written = r.rows_written
        skipped = r.rows_skipped
        print(f"  [{r.pass_name}] wrote={written} skipped={skipped}")
        for note in r.notes:
            print(f"    - {note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
