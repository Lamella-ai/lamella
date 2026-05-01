# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from lamella.core.backups.sqlite_dump import run_backup
from lamella.core.db import connect, migrate


def _make_db(path: Path):
    conn = connect(path)
    migrate(conn)
    conn.execute(
        "INSERT INTO notes (body) VALUES (?)",
        ("hello backup",),
    )
    conn.close()


def test_backup_creates_file_and_is_readable(tmp_path: Path):
    db_path = tmp_path / "data.sqlite"
    _make_db(db_path)
    backup_dir = tmp_path / "backups"

    result = run_backup(
        db_path=db_path,
        backup_dir=backup_dir,
        today=date(2026, 4, 20),
    )
    assert result.created is True
    assert result.path is not None
    assert result.path.name == "connector-20260420.sqlite"

    # Readable with COUNT(*) from notes.
    conn = connect(result.path)
    row = conn.execute("SELECT COUNT(*) AS n FROM notes").fetchone()
    assert row["n"] == 1
    conn.close()


def test_backup_is_idempotent_for_same_day(tmp_path: Path):
    db_path = tmp_path / "data.sqlite"
    _make_db(db_path)
    backup_dir = tmp_path / "backups"
    day = date(2026, 4, 20)

    r1 = run_backup(db_path=db_path, backup_dir=backup_dir, today=day)
    r2 = run_backup(db_path=db_path, backup_dir=backup_dir, today=day)
    assert r1.created is True
    assert r2.created is False
    assert r2.skipped_reason == "exists"


def test_backup_prunes_to_keep(tmp_path: Path):
    db_path = tmp_path / "data.sqlite"
    _make_db(db_path)
    backup_dir = tmp_path / "backups"

    # Seed 35 dated backups, run a new one with keep=30, and check retention.
    start = date(2026, 1, 1)
    for i in range(35):
        day = start + timedelta(days=i)
        run_backup(db_path=db_path, backup_dir=backup_dir, today=day, keep=60)

    # Now run one more with keep=30 — should prune down to 30 most recent.
    last = start + timedelta(days=35)
    result = run_backup(db_path=db_path, backup_dir=backup_dir, today=last, keep=30)
    assert result.created is True

    remaining = sorted(p.name for p in backup_dir.iterdir())
    assert len(remaining) == 30
    # The oldest kept should be day=6 (2026-01-07).
    assert remaining[0] >= "connector-20260107.sqlite"
