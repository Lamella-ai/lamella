# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Receipt linker — paperless_id <-> txn_hash bridge.

The generic ledger-writer helpers (``BeanCheckError``, ``WriteError``,
``ensure_include_in_main``, ``run_bean_check``, ``run_bean_check_vs_baseline``,
``capture_bean_check``, ``ensure_connector_links_exists``,
``CONNECTOR_LINKS_HEADER``) moved to :mod:`lamella.core.ledger_writer`
in Phase 8b subgroup 2h. They are re-exported here for back-compat;
new code should import from ``lamella.core.ledger_writer`` directly.

This file moves to ``features/receipts/linker.py`` in 5e.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

from lamella.core.fs import validate_safe_path
from lamella.core.ledger_writer import (
    CONNECTOR_LINKS_HEADER,
    BeanCheckError,
    WriteError,
    capture_bean_check,
    ensure_connector_links_exists,
    ensure_include_in_main,
    run_bean_check,
    run_bean_check_vs_baseline,
)
from lamella.features.receipts.directive_types import (
    DIRECTIVE_LINK_LEGACY,
    DIRECTIVE_LINK_NEW,
)
from lamella.features.receipts.link_block_writer import append_link_block_revoke

log = logging.getLogger(__name__)


__all__ = [
    "BeanCheckError",
    "CONNECTOR_LINKS_HEADER",
    "DocumentLinker",
    "WriteError",
    "capture_bean_check",
    "ensure_connector_links_exists",
    "ensure_include_in_main",
    "run_bean_check",
    "run_bean_check_vs_baseline",
    "remove_document_link",
]


def remove_document_link(
    *,
    main_bean: Path,
    connector_links: Path,
    txn_id: str,
    paperless_id: int,
    conn: sqlite3.Connection | None = None,
    run_check: bool = True,
) -> bool:
    """Strip a single ``custom "document-link" "<txn_id>"`` block from
    ``connector_links.bean`` whose ``lamella-paperless-id`` matches
    ``paperless_id``. The legacy ``custom "receipt-link"`` directive
    is matched by the same pass per ADR-0061 — a v3 ledger that was
    never fully migrated still has its blocks removed correctly.

    Returns ``True`` when a block was removed, ``False`` when nothing
    matched (idempotent on a no-op).

    Snapshots both ``main_bean`` and ``connector_links`` and runs
    bean-check vs baseline on success; on a NEW bean-check error the
    snapshots are restored and ``BeanCheckError`` is re-raised so the
    caller surfaces the failure.
    Mirrors :class:`DocumentLinker`'s contract: Beancount stays
    authoritative; the SQLite ``document_links`` row is also removed
    when ``conn`` is provided so callers don't end up with a dangling
    cache row pointing at a directive that no longer exists.
    The block shape produced by :class:`DocumentLinker.link` is::
        YYYY-MM-DD custom "document-link" "<txn_id>"
          lamella-paperless-id: <id>
          ... (optional hash/url/method/confidence/date/amount lines)
    Each block ends at the first blank line OR the next top-level
    directive line (one not starting with whitespace).
    """
    # ADR-0030: validate paths land inside the ledger directory.
    ledger_dir = main_bean.parent
    main_bean = validate_safe_path(main_bean, allowed_roots=[ledger_dir])
    connector_links = validate_safe_path(
        connector_links, allowed_roots=[ledger_dir]
    )

    if not connector_links.exists():
        return False

    backup_main = main_bean.read_bytes()
    backup_links = connector_links.read_bytes()
    text = backup_links.decode("utf-8")

    # Baseline so we don't fail on pre-existing chatter.
    _, baseline_output = (
        capture_bean_check(main_bean) if run_check else (0, "")
    )

    # Walk the file directive-by-directive. A "directive" starts on a
    # non-blank, non-indented line and continues until the next such
    # line or EOF. We re-emit every directive whose block does NOT
    # match (txn_id, paperless_id), drop the matching one(s).
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    # Match either the v4 (document-link) or legacy v3 (receipt-link)
    # block head per ADR-0061. Removal is direction-free: an unlink
    # removes whichever directive shape backed the original link.
    head_re = (
        r'^\d{4}-\d{2}-\d{2}\s+custom\s+'
        rf'"(?:{DIRECTIVE_LINK_NEW}|{DIRECTIVE_LINK_LEGACY})"\s+'
        r'"([^"]+)"\s*$'
    )
    import re as _re
    head_pat = _re.compile(head_re)
    pid_pat = _re.compile(
        r'^\s+lamella-paperless-id:\s*(\d+)\s*$'
    )
    removed = False
    while i < len(lines):
        line = lines[i]
        m = head_pat.match(line.rstrip("\n"))
        if m is None:
            out.append(line)
            i += 1
            continue
        # Capture the full block (head + indented continuation lines).
        block_start = i
        block_end = i + 1
        while block_end < len(lines):
            nxt = lines[block_end]
            stripped = nxt.lstrip()
            if not stripped:
                # blank line ends the block but stays as a separator
                break
            if not (nxt.startswith(" ") or nxt.startswith("\t")):
                # next directive
                break
            block_end += 1
        block = lines[block_start:block_end]
        # Check identity match: the head's "<txn_id>" AND any
        # ``lamella-paperless-id: <paperless_id>`` line in the block.
        head_txn_id = m.group(1)
        pid_match = False
        for ln in block[1:]:
            mm = pid_pat.match(ln.rstrip("\n"))
            if mm and int(mm.group(1)) == paperless_id:
                pid_match = True
                break
        if head_txn_id == txn_id and pid_match:
            removed = True
            i = block_end
            # Consume one trailing blank line if present so we don't
            # leave double blanks where a block was removed.
            if i < len(lines) and not lines[i].strip():
                i += 1
            continue
        out.extend(block)
        i = block_end

    if not removed:
        return False

    new_text = "".join(out)
    connector_links.write_text(new_text, encoding="utf-8")

    if conn is not None:
        conn.execute(
            "DELETE FROM document_links "
            "WHERE paperless_id = ? AND txn_hash = ?",
            (paperless_id, txn_id),
        )

    if run_check:
        try:
            run_bean_check_vs_baseline(main_bean, baseline_output)
        except BeanCheckError:
            connector_links.write_bytes(backup_links)
            main_bean.write_bytes(backup_main)
            if conn is not None:
                # Re-insert the row we just deleted is impossible without
                # the original metadata; the file restore is what matters
                # for ledger truth. Surface the failure so the caller can
                # re-link via the normal write path if desired.
                pass
            raise

    return True


class DocumentLinker:
    """Write document_links rows AND stamp a meta line in connector_links.bean.

    Contract: Beancount stays authoritative. After every write we run
    `bean-check`; on failure we revert both the ledger file and the DB row.
    """

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        main_bean: Path,
        connector_links: Path,
        run_check: bool = True,
    ):
        self.conn = conn
        # ADR-0030: validate both paths land inside the ledger directory
        # before the linker captures them for later writes.
        ledger_dir = main_bean.parent
        self.main_bean = validate_safe_path(
            main_bean, allowed_roots=[ledger_dir]
        )
        self.connector_links = validate_safe_path(
            connector_links, allowed_roots=[ledger_dir]
        )
        self.run_check = run_check

    def link(
        self,
        *,
        paperless_id: int,
        txn_hash: str,
        txn_date: date,
        txn_amount: Decimal,
        match_method: str,
        match_confidence: float,
        paperless_hash: str | None = None,
        paperless_url: str | None = None,
    ) -> int:
        # Snapshot originals BEFORE any mutation so revert is exact.
        backup_main = self.main_bean.read_bytes()
        links_existed = self.connector_links.exists()
        backup_links = self.connector_links.read_bytes() if links_existed else None

        # Baseline bean-check so we can tolerate pre-existing errors
        # (plugin chatter, missing totals/*.bean, etc.) and only fail
        # if the link introduces NEW errors.
        _, baseline_output = capture_bean_check(self.main_bean) if self.run_check else (0, "")

        ensure_connector_links_exists(self.connector_links)
        ensure_include_in_main(self.main_bean, self.connector_links)

        hash_line = (
            f'  lamella-paperless-hash: "{paperless_hash}"\n'
            if paperless_hash
            else ""
        )
        url_line = (
            f'  lamella-paperless-url: "{paperless_url}"\n'
            if paperless_url
            else ""
        )
        stamp = (
            f'\n{txn_date.isoformat()} custom "{DIRECTIVE_LINK_NEW}" "{txn_hash}"\n'
            f'  lamella-paperless-id: {paperless_id}\n'
            f'{hash_line}'
            f'{url_line}'
            f'  lamella-match-method: "{match_method}"\n'
            f'  lamella-match-confidence: {match_confidence:.2f}\n'
            f'  lamella-txn-date: {txn_date.isoformat()}\n'
            f'  lamella-txn-amount: {Decimal(txn_amount):.2f} USD\n'
        )
        with self.connector_links.open("a", encoding="utf-8") as fh:
            fh.write(stamp)

        # If the user previously unlinked this exact pair on purpose,
        # a new explicit link action means "allow again".
        blocked_row = self.conn.execute(
            "SELECT 1 FROM document_link_blocks "
            "WHERE paperless_id = ? AND txn_hash = ? LIMIT 1",
            (paperless_id, txn_hash),
        ).fetchone()
        self.conn.execute(
            "DELETE FROM document_link_blocks "
            "WHERE paperless_id = ? AND txn_hash = ?",
            (paperless_id, txn_hash),
        )
        if blocked_row is not None:
            append_link_block_revoke(
                connector_links=self.connector_links,
                main_bean=self.main_bean,
                txn_hash=txn_hash,
                paperless_id=int(paperless_id),
                run_check=False,
            )

        cursor = self.conn.execute(
            """
            INSERT INTO document_links
                (paperless_id, paperless_hash, txn_hash, txn_date, txn_amount,
                 match_method, match_confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (paperless_id, txn_hash) DO UPDATE SET
                paperless_hash = COALESCE(excluded.paperless_hash, document_links.paperless_hash)
            """,
            (
                paperless_id,
                paperless_hash,
                txn_hash,
                txn_date.isoformat(),
                # ADR-0022: document_links.txn_amount is TEXT (migration 057);
                # store the canonical Decimal string, not a float.
                str(Decimal(txn_amount)),
                match_method,
                match_confidence,
            ),
        )
        link_id = cursor.lastrowid or 0

        if self.run_check:
            try:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
            except BeanCheckError:
                self.main_bean.write_bytes(backup_main)
                if backup_links is None:
                    self.connector_links.unlink(missing_ok=True)
                else:
                    self.connector_links.write_bytes(backup_links)
                self.conn.execute(
                    "DELETE FROM document_links WHERE paperless_id = ? AND txn_hash = ?",
                    (paperless_id, txn_hash),
                )
                raise

        return link_id
