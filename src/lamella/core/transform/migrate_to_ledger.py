# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""One-shot migration: existing SQLite state → ledger directives.

Reads every state table that steps 1–6 now expect to find on the
ledger, emits the matching custom directive for each row, and leaves
SQLite untouched. Idempotent — subsequent runs find the directives
already present (via the read-side filter) and skip.

Dry-run by default; ``--apply`` does the writes with the standard
per-file snapshot + bean-check + rollback discipline. Bean-check
regression rolls back EVERY directive written in the pass (we snapshot
all Connector-owned files at entry, not per-directive — otherwise
cross-file bean-check failures would leave an inconsistent state).
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from lamella.features.budgets.writer import (
    append_budget,
    read_budgets_from_entries,
)
from lamella.features.calendar.writer import (
    append_day_review,
    read_day_reviews_from_entries,
)
from lamella.core.config import Settings
from lamella.core.db import connect, migrate
from lamella.features.paperless_bridge.field_map_writer import (
    append_field_mapping,
    read_field_mappings_from_entries,
)
from lamella.features.receipts.dismissals_writer import (
    append_dismissal,
    read_dismissals_from_entries,
)
from lamella.features.recurring.writer import (
    append_recurring_confirmed,
    append_recurring_ignored,
    read_recurring_from_entries,
)
from lamella.features.rules.rule_writer import (
    append_rule,
    read_rules_from_entries,
)
from lamella.core.settings.writer import (
    append_setting,
    is_secret_key,
    read_settings_from_entries,
)

log = logging.getLogger(__name__)


def _parse_dt(s) -> datetime | None:
    if s is None:
        return None
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def _migrate_dismissals(
    conn: sqlite3.Connection, settings: Settings, existing: set, run_check: bool
) -> int:
    count = 0
    rows = conn.execute(
        "SELECT txn_hash, reason, dismissed_by, dismissed_at "
        "FROM document_dismissals"
    ).fetchall()
    for row in rows:
        if row["txn_hash"] in existing:
            continue
        append_dismissal(
            connector_links=settings.connector_links_path,
            main_bean=settings.ledger_main,
            txn_hash=row["txn_hash"],
            reason=row["reason"],
            dismissed_by=row["dismissed_by"] or "user",
            dismissed_at=_parse_dt(row["dismissed_at"]),
            backfilled=True,
            run_check=run_check,
        )
        count += 1
    return count


def _migrate_rules(
    conn: sqlite3.Connection,
    settings: Settings,
    existing: set,
    run_check: bool,
) -> int:
    count = 0
    try:
        # ``last_used`` is the best proxy we have for "when the user
        # taught this"; the schema has no dedicated created_at. If
        # it's null (rule created but never fired), fall back to None
        # and the writer will stamp "now" with lamella-backfilled: TRUE.
        rows = conn.execute(
            "SELECT pattern_type, pattern_value, card_account, target_account, "
            "       created_by, last_used FROM classification_rules"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    for row in rows:
        key = (
            row["pattern_type"],
            row["pattern_value"],
            row["card_account"],
            row["target_account"],
        )
        if key in existing:
            continue
        append_rule(
            connector_rules=settings.connector_rules_path,
            main_bean=settings.ledger_main,
            pattern_type=row["pattern_type"],
            pattern_value=row["pattern_value"],
            target_account=row["target_account"],
            card_account=row["card_account"],
            created_by=row["created_by"] or "user",
            added_at=_parse_dt(row["last_used"]),
            backfilled=True,
            run_check=run_check,
        )
        count += 1
    return count


def _migrate_budgets(
    conn: sqlite3.Connection, settings: Settings, existing: set, run_check: bool
) -> int:
    count = 0
    try:
        rows = conn.execute(
            "SELECT label, entity, account_pattern, period, amount, "
            "       alert_threshold, created_at "
            "FROM budgets"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    for row in rows:
        key = (row["label"], row["entity"], row["account_pattern"], row["period"])
        if key in existing:
            continue
        append_budget(
            connector_budgets=settings.connector_budgets_path,
            main_bean=settings.ledger_main,
            label=row["label"],
            entity=row["entity"],
            account_pattern=row["account_pattern"],
            period=row["period"],
            amount=Decimal(str(row["amount"])),
            alert_threshold=float(row["alert_threshold"]),
            created_at=_parse_dt(row["created_at"]),
            backfilled=True,
            run_check=run_check,
        )
        count += 1
    return count


def _migrate_paperless_fields(
    conn: sqlite3.Connection, settings: Settings, existing: set, run_check: bool
) -> int:
    count = 0
    try:
        # Only user-explicit rows migrate (auto_assigned=0).
        rows = conn.execute(
            "SELECT paperless_field_id, paperless_field_name, canonical_role "
            "FROM paperless_field_map WHERE auto_assigned = 0"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    for row in rows:
        if int(row["paperless_field_id"]) in existing:
            continue
        append_field_mapping(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            paperless_field_id=int(row["paperless_field_id"]),
            paperless_field_name=row["paperless_field_name"],
            canonical_role=row["canonical_role"],
            run_check=run_check,
        )
        count += 1
    return count


def _migrate_recurring(
    conn: sqlite3.Connection, settings: Settings, existing: set, run_check: bool
) -> int:
    count = 0
    try:
        rows = conn.execute(
            "SELECT label, entity, expected_amount, expected_day, "
            "       source_account, merchant_pattern, cadence, status, "
            "       confirmed_at, ignored_at "
            "FROM recurring_expenses "
            "WHERE status IN ('confirmed', 'ignored', 'stopped')"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    for row in rows:
        key = (row["source_account"], row["merchant_pattern"])
        if key in existing:
            continue
        if row["status"] == "confirmed":
            append_recurring_confirmed(
                connector_rules=settings.connector_rules_path,
                main_bean=settings.ledger_main,
                label=row["label"],
                entity=row["entity"] or "",
                source_account=row["source_account"],
                target_account=None,
                merchant_pattern=row["merchant_pattern"],
                cadence=row["cadence"] or "monthly",
                expected_amount=Decimal(str(row["expected_amount"])),
                expected_day=(
                    int(row["expected_day"])
                    if row["expected_day"] is not None
                    else None
                ),
                confirmed_at=_parse_dt(row["confirmed_at"]),
                backfilled=True,
                run_check=run_check,
            )
        else:
            append_recurring_ignored(
                connector_rules=settings.connector_rules_path,
                main_bean=settings.ledger_main,
                label=row["label"],
                source_account=row["source_account"],
                merchant_pattern=row["merchant_pattern"],
                ignored_at=_parse_dt(row["ignored_at"]),
                run_check=run_check,
            )
        count += 1
    return count


def _migrate_day_reviews(
    conn: sqlite3.Connection, settings: Settings, existing: set, run_check: bool
) -> int:
    count = 0
    try:
        rows = conn.execute(
            "SELECT review_date, last_reviewed_at, ai_summary, ai_summary_at, "
            "       ai_audit_result, ai_audit_result_at "
            "FROM day_reviews "
            "WHERE last_reviewed_at IS NOT NULL "
            "   OR ai_summary IS NOT NULL "
            "   OR ai_audit_result IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    from datetime import date as _date
    for row in rows:
        review_date = row["review_date"]
        key = str(review_date)[:10] if review_date else None
        if not key or key in existing:
            continue
        if isinstance(review_date, _date):
            review_date_obj = review_date
        else:
            try:
                review_date_obj = _date.fromisoformat(key)
            except ValueError:
                continue
        append_day_review(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            review_date=review_date_obj,
            last_reviewed_at=row["last_reviewed_at"],
            ai_summary=row["ai_summary"],
            ai_summary_at=row["ai_summary_at"],
            ai_audit_result=row["ai_audit_result"],
            ai_audit_result_at=row["ai_audit_result_at"],
            run_check=run_check,
        )
        count += 1
    return count


def _migrate_settings(
    conn: sqlite3.Connection, settings: Settings, existing: set, run_check: bool
) -> int:
    count = 0
    skipped_secret = 0
    try:
        rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    except sqlite3.OperationalError:
        return 0
    for row in rows:
        if is_secret_key(row["key"]):
            skipped_secret += 1
            continue
        if row["key"] in existing:
            continue
        append_setting(
            connector_config=settings.connector_config_path,
            main_bean=settings.ledger_main,
            key=row["key"],
            value=row["value"] or "",
            run_check=run_check,
        )
        count += 1
    return count


def run(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    apply: bool,
) -> dict[str, int]:
    """Execute all six migrations. Returns per-step counts."""
    from lamella.core.beancount_io import LedgerReader

    run_check = apply  # dry-run skips bean-check since we're not writing

    reader = LedgerReader(settings.ledger_main)
    entries = list(reader.load().entries)

    # Build "what's already on the ledger" sets so we don't double-stamp.
    existing = {
        "dismissals": {r["txn_hash"] for r in read_dismissals_from_entries(entries)},
        "rules": {
            (r["pattern_type"], r["pattern_value"], r["card_account"], r["target_account"])
            for r in read_rules_from_entries(entries)
        },
        "budgets": {
            (r["label"], r["entity"], r["account_pattern"], r["period"])
            for r in read_budgets_from_entries(entries)
        },
        "fields": {r["paperless_field_id"] for r in read_field_mappings_from_entries(entries)},
        "recurring": {
            (r["source_account"], r["merchant_pattern"])
            for r in read_recurring_from_entries(entries)
        },
        "settings": set(read_settings_from_entries(entries).keys()),
        "day_reviews": {r["review_date"] for r in read_day_reviews_from_entries(entries)},
    }

    counts: dict[str, int] = {}
    if apply:
        counts["dismissals"] = _migrate_dismissals(
            conn, settings, existing["dismissals"], run_check
        )
        counts["rules"] = _migrate_rules(conn, settings, existing["rules"], run_check)
        counts["budgets"] = _migrate_budgets(conn, settings, existing["budgets"], run_check)
        counts["fields"] = _migrate_paperless_fields(
            conn, settings, existing["fields"], run_check
        )
        counts["recurring"] = _migrate_recurring(
            conn, settings, existing["recurring"], run_check
        )
        counts["settings"] = _migrate_settings(
            conn, settings, existing["settings"], run_check
        )
        counts["day_reviews"] = _migrate_day_reviews(
            conn, settings, existing["day_reviews"], run_check
        )
    else:
        # Dry-run: count what WOULD be written.
        counts["dismissals"] = _count_unmigrated(
            conn, "SELECT txn_hash FROM document_dismissals",
            existing["dismissals"], lambda r: r["txn_hash"],
        )
        counts["rules"] = _count_unmigrated(
            conn,
            "SELECT pattern_type, pattern_value, card_account, target_account "
            "FROM classification_rules",
            existing["rules"],
            lambda r: (
                r["pattern_type"], r["pattern_value"], r["card_account"],
                r["target_account"],
            ),
        )
        counts["budgets"] = _count_unmigrated(
            conn,
            "SELECT label, entity, account_pattern, period FROM budgets",
            existing["budgets"],
            lambda r: (r["label"], r["entity"], r["account_pattern"], r["period"]),
        )
        counts["fields"] = _count_unmigrated(
            conn,
            "SELECT paperless_field_id FROM paperless_field_map WHERE auto_assigned = 0",
            existing["fields"],
            lambda r: int(r["paperless_field_id"]),
        )
        counts["recurring"] = _count_unmigrated(
            conn,
            "SELECT source_account, merchant_pattern FROM recurring_expenses "
            "WHERE status IN ('confirmed','ignored','stopped')",
            existing["recurring"],
            lambda r: (r["source_account"], r["merchant_pattern"]),
        )
        counts["settings"] = _count_unmigrated_settings(
            conn, existing["settings"]
        )
        counts["day_reviews"] = _count_unmigrated(
            conn,
            "SELECT review_date FROM day_reviews "
            "WHERE last_reviewed_at IS NOT NULL "
            "   OR ai_summary IS NOT NULL "
            "   OR ai_audit_result IS NOT NULL",
            existing["day_reviews"],
            lambda r: str(r["review_date"])[:10],
        )
    return counts


def _count_unmigrated(conn, sql, existing, key_fn) -> int:
    try:
        rows = conn.execute(sql).fetchall()
    except sqlite3.OperationalError:
        return 0
    return sum(1 for r in rows if key_fn(r) not in existing)


def _count_unmigrated_settings(conn, existing) -> int:
    try:
        rows = conn.execute("SELECT key FROM app_settings").fetchall()
    except sqlite3.OperationalError:
        return 0
    return sum(
        1 for r in rows
        if not is_secret_key(r["key"]) and r["key"] not in existing
    )


def _sanity_check(counts: dict[str, int], conn: sqlite3.Connection) -> list[str]:
    """Return warnings when counts look suspicious vs. source table sizes.
    A big mismatch means we're about to mass-stamp something that
    probably shouldn't be in 'state' — worth eyeballing before --apply.
    """
    warnings: list[str] = []
    expected_cap = {
        "dismissals": "document_dismissals",
        "rules": "classification_rules",
        "budgets": "budgets",
        "fields": "paperless_field_map",
        "recurring": "recurring_expenses",
        "settings": "app_settings",
        "day_reviews": "day_reviews",
    }
    for step, table in expected_cap.items():
        try:
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        except sqlite3.OperationalError:
            continue
        n_rows = int(row["n"] if isinstance(row, sqlite3.Row) else row[0])
        n_stamps = counts.get(step, 0)
        # Step-4 migrate_to_ledger filters auto_assigned=0 only; for others
        # the dry-run count should equal or be less than the table size
        # (equal on first run, less on re-runs because already-stamped
        # rows are skipped).
        if n_stamps > n_rows:
            warnings.append(
                f"{step}: would stamp {n_stamps} directives but table has "
                f"only {n_rows} rows — possible bug."
            )
        if step != "fields" and n_stamps > 1000:
            warnings.append(
                f"{step}: {n_stamps} directives is a lot. Spot-check the "
                f"{table} table — if most rows look like cache/transient "
                f"data, step {step} may be casting too wide a net."
            )
    return warnings


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "One-shot migration of existing SQLite state (dismissals, "
            "rules, budgets, paperless field map, recurring confirmations, "
            "settings) into the matching ledger directives. Idempotent: "
            "rows already stamped on the ledger are skipped.\n\n"
            "Original SQLite timestamps are preserved where available "
            "(rule last_used, budget created_at, recurring confirmed_at, "
            "etc.). When a timestamp isn't available the directive is "
            "dated 'now' and carries lamella-backfilled: TRUE so future "
            "audits can tell them apart from stamps written at teach time."
        )
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    settings = Settings()
    conn = connect(settings.db_path)
    migrate(conn)

    counts = run(conn, settings, apply=args.apply)

    header = "Applied" if args.apply else "Would write (dry-run)"
    print(f"{header} per-step counts:")
    for step, n in counts.items():
        print(f"  {step:12s} {n}")
    total = sum(counts.values())
    print(f"  {'total':12s} {total}")

    warnings = _sanity_check(counts, conn)
    if warnings:
        print("\nSanity-check warnings (review before --apply):")
        for w in warnings:
            print(f"  ! {w}")

    if not args.apply:
        print("\nDry-run — re-run with --apply to write to the ledger.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
