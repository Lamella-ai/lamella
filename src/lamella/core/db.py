# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)

_MIGRATION_NAME_RE = re.compile(r"^(\d+)_.+\.sql$")


class _LockedConnection(sqlite3.Connection):
    """sqlite3.Connection subclass that serializes execute / commit
    across threads.

    The app opens ONE connection in lifespan and hands it to every
    route via ``app.state.db``. FastAPI runs sync endpoints in a
    threadpool, so concurrent requests touch the same connection
    from different threads. Python's sqlite3 with
    ``check_same_thread=False`` permits that but does NOT serialize
    internally — two threads binding parameters simultaneously
    corrupts the shared statement state and raises
    ``sqlite3.InterfaceError: bad parameter or other API misuse``
    sporadically. This surfaced as a 500 on /receipts/needed when
    a template filter (``alias``) hit the DB while another handler
    was mid-query.

    Wrap ``execute`` / ``executemany`` / ``executescript`` / ``commit``
    with an RLock so statement preparation is atomic per call. RLock
    (not Lock) so a single thread can re-enter if a row_factory or
    application hook wants to re-query.

    Cursor-level ``.execute`` is not wrapped; none of our call sites
    use ``conn.cursor()`` explicitly. If that changes, wrap
    ``cursor()`` as well.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # sqlite3.Connection has no __dict__ normally; the subclass
        # adds one, so attribute assignment works here.
        self._bcg_lock = threading.RLock()

    def execute(self, *args, **kwargs):  # type: ignore[override]
        with self._bcg_lock:
            return super().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):  # type: ignore[override]
        with self._bcg_lock:
            return super().executemany(*args, **kwargs)

    def executescript(self, *args, **kwargs):  # type: ignore[override]
        with self._bcg_lock:
            return super().executescript(*args, **kwargs)

    def commit(self, *args, **kwargs):  # type: ignore[override]
        with self._bcg_lock:
            return super().commit(*args, **kwargs)


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # No PARSE_DECLTYPES: the stdlib's default timestamp converter is
    # deprecated in Python 3.12+. We read ISO strings and parse explicitly
    # in service code.
    conn = sqlite3.connect(
        db_path,
        isolation_level=None,
        check_same_thread=False,
        factory=_LockedConnection,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _migrations_dir() -> Path:
    # In the Docker image the package is pip-installed into the venv, so the repo-relative
    # parents[2] walk no longer lands on /app/migrations. LAMELLA_MIGRATIONS_DIR (legacy
    # CONNECTOR_MIGRATIONS_DIR also accepted) lets the image point at the copied-in
    # directory explicitly; falls back to the repo layout for dev.
    from lamella.utils._legacy_env import read_env
    override = read_env("LAMELLA_MIGRATIONS_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "migrations"


def _migration_files() -> list[tuple[int, str, str]]:
    root = _migrations_dir()
    files: list[tuple[int, str, str]] = []
    for path in sorted(root.glob("*.sql")):
        m = _MIGRATION_NAME_RE.match(path.name)
        if not m:
            continue
        files.append((int(m.group(1)), path.name, path.read_text(encoding="utf-8")))
    files.sort(key=lambda row: row[0])
    return files


def migrate(conn: sqlite3.Connection) -> list[int]:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    already = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations")}
    applied: list[int] = []
    for version, name, sql in _migration_files():
        if version in already:
            continue
        log.info("Applying migration %s", name)
        conn.executescript("BEGIN;\n" + sql + "\nCOMMIT;")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))
        applied.append(version)
    # Post-migration backfills — pure SQL migrations can't mint UUIDv7,
    # so any column whose semantics require Python-side minting gets
    # filled in here. Idempotent (WHERE … IS NULL); cheap when no rows
    # need it.
    _backfill_python_minted_columns(conn)
    return applied


def _backfill_python_minted_columns(conn: sqlite3.Connection) -> None:
    """Mint values for columns whose backfill needs Python (UUIDv7).

    Migration 059 added ``staged_transactions.lamella_txn_id`` — the
    immutable identity that lets /txn/{token} resolve to a staged row
    or its eventual ledger entry with the same URL. A pure-SQL
    migration can't mint UUIDv7s; we do it here, scoped to rows that
    still have NULL.
    """
    try:
        cur = conn.execute(
            "SELECT 1 FROM pragma_table_info('staged_transactions') "
            "WHERE name = 'lamella_txn_id' LIMIT 1"
        )
        if cur.fetchone() is None:
            return
    except sqlite3.DatabaseError:
        return
    rows = conn.execute(
        "SELECT id FROM staged_transactions WHERE lamella_txn_id IS NULL"
    ).fetchall()
    if not rows:
        return
    # Local import — identity.py has no db.py dependency, so this is
    # safe; importing at module level would create an import-order
    # surprise for the rare caller that imports core.db before
    # core.identity during early bootstrap.
    from lamella.core.identity import mint_txn_id
    log.info(
        "backfilling staged_transactions.lamella_txn_id for %d row(s)",
        len(rows),
    )
    for row in rows:
        conn.execute(
            "UPDATE staged_transactions SET lamella_txn_id = ? WHERE id = ?",
            (mint_txn_id(), int(row["id"])),
        )


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
