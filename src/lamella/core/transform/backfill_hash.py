# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Backfill ``lamella-paperless-hash`` and ``lamella-paperless-url`` onto every
existing receipt-link entry in ``connector_links.bean``, and populate the
``paperless_hash`` column on ``receipt_links`` in SQLite.

Source of truth for the hash is Paperless's ``original_checksum`` (MD5),
cached in ``paperless_doc_index`` by the sync job. If the cache is empty
or stale, we fetch the document directly — one HTTP round-trip per
missing row, throttled by a tiny asyncio.Semaphore so we don't hammer
Paperless. Rows whose document Paperless no longer has are logged and
skipped (not fatal).

Implementation:
  1. Enumerate receipt_links rows where paperless_hash IS NULL.
  2. For each row, resolve hash via paperless_doc_index first, falling
     back to a live GET /api/documents/{id}/.
  3. Dry-run: print a per-row report. --apply: append one amendment
     block per resolved row to connector_links.bean:

        2026-04-21 custom "receipt-link-hash-backfill" "<txn_hash>"
          lamella-paperless-id: <paperless_id>
          lamella-paperless-hash: "md5:<hex>"
          lamella-paperless-url: "<url>"

     Appending (not rewriting the original line) preserves the original
     entry verbatim and makes the backfill auditable. Readers that look
     up by (paperless_id, txn_hash) join both directives correctly.
  4. Update the receipt_links.paperless_hash column.
  5. Run bean-check; rollback both the file and the DB changes on
     regression.

Idempotent: rows that already have paperless_hash set are skipped.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from lamella.core.config import Settings
from lamella.core.db import connect, migrate
from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.features.paperless_bridge.lookups import cached_original_checksum
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.core.ledger_writer import (
    BeanCheckError,
    ensure_connector_links_exists,
    ensure_include_in_main,
)
from lamella.core.transform._files import (
    FileSnapshot,
    baseline,
    run_check_with_rollback,
    snapshot,
)

log = logging.getLogger(__name__)


@dataclass
class BackfillRow:
    link_id: int
    paperless_id: int
    txn_hash: str
    resolved_hash: str | None = None
    resolved_url: str | None = None
    error: str | None = None


@dataclass
class BackfillResult:
    to_fill: list[BackfillRow] = field(default_factory=list)
    skipped_existing: int = 0
    unresolved: list[BackfillRow] = field(default_factory=list)

    @property
    def resolvable(self) -> list[BackfillRow]:
        return [r for r in self.to_fill if r.resolved_hash]


async def _resolve_hash(
    conn: sqlite3.Connection,
    client: PaperlessClient | None,
    paperless_id: int,
    *,
    http_sem: asyncio.Semaphore,
) -> tuple[str | None, str | None]:
    cached = cached_original_checksum(conn, paperless_id)
    if cached:
        return cached, None
    if client is None:
        return None, "no-cache-and-paperless-not-configured"
    # Two-step fallback: try the main document endpoint first (cheap,
    # one call we'd already make anyway); if the Paperless version
    # doesn't include checksums there (many don't), fall through to
    # /metadata/ which is Paperless-ngx's dedicated file-metadata
    # subroute.
    checksum: str | None = None
    async with http_sem:
        try:
            doc = await client.get_document(paperless_id)
            checksum = doc.original_checksum or None
        except PaperlessError as exc:
            return None, f"paperless-error: {exc}"
        if not checksum:
            try:
                meta = await client.get_document_metadata(paperless_id)
            except PaperlessError as exc:
                return None, f"paperless-metadata-error: {exc}"
            raw = meta.get("original_checksum") or meta.get("archive_checksum")
            if isinstance(raw, str) and raw.strip():
                checksum = raw.strip()
    if not checksum:
        return None, "paperless-did-not-return-checksum"
    # Opportunistically backfill the cache so later runs are offline.
    conn.execute(
        "UPDATE paperless_doc_index SET original_checksum = ? "
        "WHERE paperless_id = ? AND (original_checksum IS NULL OR original_checksum = '')",
        (checksum, paperless_id),
    )
    return checksum, None


async def plan(
    conn: sqlite3.Connection, client: PaperlessClient | None
) -> BackfillResult:
    result = BackfillResult()
    rows: list[sqlite3.Row] = list(
        conn.execute(
            "SELECT id AS link_id, paperless_id, txn_hash, paperless_hash "
            "FROM receipt_links ORDER BY id ASC"
        )
    )
    http_sem = asyncio.Semaphore(4)
    for row in rows:
        if row["paperless_hash"]:
            result.skipped_existing += 1
            continue
        br = BackfillRow(
            link_id=int(row["link_id"]),
            paperless_id=int(row["paperless_id"]),
            txn_hash=str(row["txn_hash"]),
        )
        checksum, err = await _resolve_hash(
            conn, client, br.paperless_id, http_sem=http_sem
        )
        if checksum:
            br.resolved_hash = f"md5:{checksum}"
        else:
            br.error = err
        result.to_fill.append(br)
        if not checksum:
            result.unresolved.append(br)
    return result


def _amendment_block(
    row: BackfillRow, *, today: str
) -> str:
    lines = [
        f'\n{today} custom "receipt-link-hash-backfill" "{row.txn_hash}"',
        f"  lamella-paperless-id: {row.paperless_id}",
    ]
    if row.resolved_hash:
        lines.append(f'  lamella-paperless-hash: "{row.resolved_hash}"')
    if row.resolved_url:
        lines.append(f'  lamella-paperless-url: "{row.resolved_url}"')
    return "\n".join(lines) + "\n"


def apply(
    conn: sqlite3.Connection,
    result: BackfillResult,
    *,
    settings: Settings,
    run_check: bool = True,
) -> None:
    resolvable = result.resolvable
    if not resolvable:
        return
    connector_links = settings.connector_links_path
    main_bean = settings.ledger_main
    ensure_connector_links_exists(connector_links)
    ensure_include_in_main(main_bean, connector_links)
    snaps: list[FileSnapshot] = [snapshot(connector_links), snapshot(main_bean)]
    base_output = baseline(main_bean, run_check=run_check)
    today = date.today().isoformat()
    try:
        with connector_links.open("a", encoding="utf-8") as fh:
            for row in resolvable:
                row.resolved_url = paperless_url_for(
                    settings.paperless_url, row.paperless_id
                )
                fh.write(_amendment_block(row, today=today))
        # DB update
        conn.execute("BEGIN")
        try:
            for row in resolvable:
                conn.execute(
                    "UPDATE receipt_links SET paperless_hash = ? WHERE id = ?",
                    (row.resolved_hash, row.link_id),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        run_check_with_rollback(
            main_bean, base_output, snaps, run_check=run_check
        )
    except (BeanCheckError, Exception):
        # File rollback already handled inside run_check_with_rollback
        # on bean-check failure; for other exceptions restore ourselves.
        # The DB rollback above is best-effort; re-set any rows we
        # updated back to NULL.
        conn.execute("BEGIN")
        try:
            for row in resolvable:
                conn.execute(
                    "UPDATE receipt_links SET paperless_hash = NULL "
                    "WHERE id = ? AND paperless_hash = ?",
                    (row.link_id, row.resolved_hash),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
        for s in snaps:
            s.restore()
        raise


async def _amain(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Backfill lamella-paperless-hash on existing receipt_links."
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--no-check", action="store_true")
    args = parser.parse_args(argv)

    settings = Settings()
    conn = connect(settings.db_path)
    migrate(conn)

    client: PaperlessClient | None = None
    if settings.paperless_configured:
        client = PaperlessClient(
            base_url=settings.paperless_url or "",
            api_token=(settings.paperless_api_token.get_secret_value()
                       if settings.paperless_api_token else ""),
            extra_headers=settings.paperless_extra_headers(),
        )

    try:
        result = await plan(conn, client)
    finally:
        if client is not None:
            await client.aclose()

    print(f"Receipt links with hash already set: {result.skipped_existing}")
    print(f"Receipt links needing backfill: {len(result.to_fill)}")
    print(f"  Resolvable: {len(result.resolvable)}")
    print(f"  Unresolved: {len(result.unresolved)}")
    for r in result.unresolved:
        print(f"    link #{r.link_id} (doc {r.paperless_id}): {r.error}")

    if not result.resolvable:
        print("\nNothing to apply.")
        return 0

    if not args.apply:
        print("\nDry-run — re-run with --apply to write.")
        for r in result.resolvable[:10]:
            print(
                f"  would stamp link #{r.link_id} doc={r.paperless_id} "
                f"hash={r.resolved_hash}"
            )
        if len(result.resolvable) > 10:
            print(f"  … and {len(result.resolvable) - 10} more")
        return 0

    apply(conn, result, settings=settings, run_check=not args.no_check)
    print(f"Applied {len(result.resolvable)} backfill row(s).")
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    sys.exit(main())
