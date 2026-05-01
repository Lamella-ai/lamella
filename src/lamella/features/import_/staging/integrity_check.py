# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Data Integrity Check — NEXTGEN.md Phase F.

Productized form of the reboot scan (E1) + retrofit (E2): a
settings-area "health check" the user can run whenever they want
reassurance. On a clean ledger it reports "no changes needed."
On a ledger with drift, it surfaces the specific problems.

Phase F ships a ``run_integrity_check`` function callable from
routes or the scheduler, a persisted check-history table so the
dashboard can show "last run: Xh ago," and a status record
summarizing what was found.

The check is a read-only operation by default. It runs the
reboot scan and surfaces duplicate groups / unpaired transfers
but does NOT retrofit or apply anything — the user reviews and
acts via the existing data-integrity UI. A future option could
enable auto-retrofit for high-confidence groups.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from lamella.core.beancount_io.reader import LedgerReader
from lamella.features.import_.staging import RebootService, find_pairs

log = logging.getLogger(__name__)

__all__ = [
    "IntegrityReport",
    "run_integrity_check",
    "latest_integrity_report",
    "ensure_integrity_table",
]


@dataclass
class IntegrityReport:
    """What the check found."""
    started_at: str
    finished_at: str
    total_ledger_txns: int = 0
    files_covered: int = 0
    duplicate_groups: int = 0
    duplicate_rows: int = 0
    pending_pair_proposals: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return (
            self.duplicate_groups == 0
            and self.pending_pair_proposals == 0
            and not self.errors
        )

    def summary(self) -> str:
        if self.is_clean:
            return "No changes needed — books look consistent."
        bits: list[str] = []
        if self.duplicate_groups:
            bits.append(
                f"{self.duplicate_groups} duplicate group(s) "
                f"({self.duplicate_rows} row(s))"
            )
        if self.pending_pair_proposals:
            bits.append(f"{self.pending_pair_proposals} potential transfer pair(s)")
        if self.errors:
            bits.append(f"{len(self.errors)} error(s)")
        return "Found: " + "; ".join(bits) + "."


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS integrity_check_history (
    id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    total_ledger_txns INTEGER NOT NULL DEFAULT 0,
    files_covered INTEGER NOT NULL DEFAULT 0,
    duplicate_groups INTEGER NOT NULL DEFAULT 0,
    duplicate_rows INTEGER NOT NULL DEFAULT 0,
    pending_pair_proposals INTEGER NOT NULL DEFAULT 0,
    errors_json TEXT
)
"""


def ensure_integrity_table(conn: sqlite3.Connection) -> None:
    """Phase F table. Created lazily on first check run to avoid a
    dedicated migration for a cache-class table (the history is
    purely informational; losing it has no correctness impact)."""
    conn.execute(_TABLE_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_integrity_check(
    conn: sqlite3.Connection,
    reader: LedgerReader,
) -> IntegrityReport:
    """Runs the reboot scan + matcher sweep in read-only mode,
    persists a summary row, returns the report.

    Does NOT retrofit or apply anything. The user reviews the
    result via the existing data-integrity UI and clicks through
    to the specific resolution actions.
    """
    ensure_integrity_table(conn)
    started = _now_iso()
    report = IntegrityReport(started_at=started, finished_at=started)

    try:
        scan = RebootService(conn).scan_ledger(reader, force_reload=True)
        report.total_ledger_txns = scan.total_txns
        report.files_covered = len(scan.files_covered)
        report.duplicate_groups = len(scan.duplicate_groups)
        report.duplicate_rows = scan.duplicates_total
        report.errors.extend(scan.errors)
    except Exception as exc:  # noqa: BLE001
        log.exception("integrity check scan failed")
        report.errors.append(f"scan: {type(exc).__name__}: {exc}")

    try:
        # How many pair proposals would the matcher surface right
        # now? A clean ledger returns zero (everything's matched or
        # unrelated).
        proposals = find_pairs(
            conn, min_confidence="medium", require_cross_source=False,
        )
        report.pending_pair_proposals = len(proposals)
    except Exception as exc:  # noqa: BLE001
        log.exception("integrity check pair pass failed")
        report.errors.append(f"pairs: {type(exc).__name__}: {exc}")

    report.finished_at = _now_iso()

    conn.execute(
        """
        INSERT INTO integrity_check_history
            (started_at, finished_at, total_ledger_txns, files_covered,
             duplicate_groups, duplicate_rows, pending_pair_proposals,
             errors_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            report.started_at, report.finished_at,
            report.total_ledger_txns, report.files_covered,
            report.duplicate_groups, report.duplicate_rows,
            report.pending_pair_proposals,
            json.dumps(report.errors) if report.errors else None,
        ),
    )
    conn.commit()
    log.info("integrity check: %s", report.summary())
    return report


def latest_integrity_report(
    conn: sqlite3.Connection,
) -> IntegrityReport | None:
    """Fetch the most recent ``run_integrity_check`` result from
    history, or ``None`` if the table is empty or missing."""
    try:
        row = conn.execute(
            "SELECT * FROM integrity_check_history "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    errors = json.loads(row["errors_json"]) if row["errors_json"] else []
    return IntegrityReport(
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        total_ledger_txns=int(row["total_ledger_txns"] or 0),
        files_covered=int(row["files_covered"] or 0),
        duplicate_groups=int(row["duplicate_groups"] or 0),
        duplicate_rows=int(row["duplicate_rows"] or 0),
        pending_pair_proposals=int(row["pending_pair_proposals"] or 0),
        errors=errors,
    )
