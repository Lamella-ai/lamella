# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""In-batch transfer/duplicate detection.

Scoped to a single ``import_id`` — cross-file transfer matching is
handled by the unified matcher (``staging/matcher.py``). This
module is retained for the intra-upload case because the unified
matcher runs with ``require_cross_source=True`` by default and
therefore won't pair two ``source='csv'`` rows from the same
spreadsheet batch.

**Retirement status (NEXTGEN Phase C2c).** This module is slated
for removal once the unified matcher grows an option to run with
``require_cross_source=False`` for the duration of a single
import's sweep, covering the intra-batch case. Until then:

  * ``detect()`` is still called from ``importer.service.commit``.
  * ``_mirror_pairs_to_staging`` bridges ``row_pairs`` →
    ``staged_pairs`` so the rest of the pipeline (writer,
    transfer writer) sees pairs in one place.
  * The Phase C2b transfer writer is the canonical consumer.

Do NOT add new callers. Any new pair-detection work should flow
through ``staging/matcher.py`` directly.

Populates ``row_pairs`` with:
  (row_a_id, row_b_id, kind, confidence, reason)

Two detection passes:
  1. cross-sheet duplicate — same date/amount/desc prefix within
     a single upload but different sources (sheets).
     kind='duplicate'.
  2. paired opposite-sign amounts — e.g. PayPal outflow matched
     to a WF deposit, both within this upload, ±2 days.
     kind='transfer'.
"""
from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal

log = logging.getLogger(__name__)


def clear_for_import(conn: sqlite3.Connection, import_id: int) -> None:
    conn.execute(
        """
        DELETE FROM row_pairs
         WHERE row_a_id IN (
                SELECT r.id FROM raw_rows r
                  JOIN sources s ON s.id = r.source_id
                 WHERE s.upload_id = ?)
            OR row_b_id IN (
                SELECT r.id FROM raw_rows r
                  JOIN sources s ON s.id = r.source_id
                 WHERE s.upload_id = ?)
        """,
        (import_id, import_id),
    )


def detect_cross_sheet_duplicates(
    conn: sqlite3.Connection, import_id: int
) -> int:
    rows = conn.execute(
        """
        SELECT MIN(rr.id) AS a_id, MAX(rr.id) AS b_id,
               COUNT(*) AS n, rr.date, rr.amount,
               substr(COALESCE(rr.description, rr.payee, ''), 1, 30) d,
               s.upload_id
          FROM raw_rows rr
          JOIN sources s ON rr.source_id = s.id
         WHERE s.upload_id = ?
           AND rr.amount IS NOT NULL AND rr.date IS NOT NULL
         GROUP BY s.upload_id, rr.date, rr.amount,
                  substr(COALESCE(rr.description, rr.payee, ''), 1, 30)
        HAVING n = 2
           AND (SELECT COUNT(DISTINCT rr2.source_id)
                  FROM raw_rows rr2
                  JOIN sources s2 ON rr2.source_id = s2.id
                 WHERE s2.upload_id = s.upload_id
                   AND rr2.date = rr.date
                   AND rr2.amount = rr.amount
                   AND substr(COALESCE(rr2.description, rr2.payee, ''), 1, 30) = d
               ) = 2
        """,
        (import_id,),
    ).fetchall()
    batch = [
        {
            "a": r["a_id"],
            "b": r["b_id"],
            "reason": "cross-sheet duplicate within upload",
        }
        for r in rows
    ]
    if batch:
        conn.executemany(
            "INSERT INTO row_pairs (row_a_id, row_b_id, kind, confidence, reason) "
            "VALUES (:a, :b, 'duplicate', 'high', :reason)",
            batch,
        )
    return len(batch)


def detect_transfers_paired(
    conn: sqlite3.Connection, import_id: int, min_abs_amount: Decimal = Decimal("50.0")
) -> int:
    # raw_rows.amount is TEXT (post-migration 057). Bind the threshold as
    # a string so SQLite's CAST in ABS() compares numerically without
    # round-tripping through float.
    rows = conn.execute(
        """
        SELECT rr.id, rr.date, rr.amount, s.source_class, s.id AS src_id
          FROM raw_rows rr
          JOIN sources s ON rr.source_id = s.id
         WHERE s.upload_id = ?
           AND rr.amount IS NOT NULL AND ABS(rr.amount) >= ?
           AND rr.date IS NOT NULL
           AND rr.id NOT IN (SELECT row_a_id FROM row_pairs
                             UNION ALL SELECT row_b_id FROM row_pairs)
        """,
        (import_id, str(min_abs_amount)),
    ).fetchall()

    by_amt_day: dict[tuple[Decimal, str], list] = {}
    for r in rows:
        amt = Decimal(str(r["amount"]))
        key = (abs(amt).quantize(Decimal("0.01")), r["date"])
        by_amt_day.setdefault(key, []).append((amt, r))

    used: set[int] = set()
    batch: list[dict] = []
    for (absamt, date), group in by_amt_day.items():
        if len(group) < 2:
            continue
        pos = [(amt, r) for amt, r in group if amt > 0 and r["id"] not in used]
        neg = [(amt, r) for amt, r in group if amt < 0 and r["id"] not in used]
        for _p_amt, p in pos:
            for _n_amt, n in neg:
                if p["id"] in used or n["id"] in used:
                    continue
                if (
                    p["source_class"] == n["source_class"]
                    and p["src_id"] == n["src_id"]
                ):
                    continue
                used.add(p["id"])
                used.add(n["id"])
                batch.append(
                    {
                        "a": p["id"],
                        "b": n["id"],
                        "reason": (
                            f"paired amount-match {absamt:.2f} date={date} "
                            f"({p['source_class']}<->{n['source_class']})"
                        ),
                    }
                )
                break
    if batch:
        conn.executemany(
            "INSERT INTO row_pairs (row_a_id, row_b_id, kind, confidence, reason) "
            "VALUES (:a, :b, 'transfer', 'medium', :reason)",
            batch,
        )
    return len(batch)


def detect(conn: sqlite3.Connection, import_id: int) -> dict[str, int]:
    """Run both detection passes for an upload. Idempotent: prior
    row_pairs for this upload are cleared first."""
    clear_for_import(conn, import_id)
    dupes = detect_cross_sheet_duplicates(conn, import_id)
    transfers = detect_transfers_paired(conn, import_id)
    log.info(
        "transfers.detect import_id=%d duplicates=%d transfers=%d",
        import_id, dupes, transfers,
    )
    _mirror_pairs_to_staging(conn, import_id)
    return {"duplicates": dupes, "transfers": transfers}


def _mirror_pairs_to_staging(
    conn: sqlite3.Connection, import_id: int
) -> None:
    """Mirror ``row_pairs`` rows for this upload into ``staged_pairs``.

    NEXTGEN.md Phase A: the unified transfer matcher (Phase C) queries
    ``staged_pairs`` only. Keep this mirror until the importer's own
    pair detection is folded into the unified matcher.
    """
    try:
        # Clear any staged pairs we previously mirrored for this upload
        # so re-running detect() doesn't leave stale rows behind.
        conn.execute(
            """
            DELETE FROM staged_pairs
             WHERE a_staged_id IN (
                     SELECT id FROM staged_transactions
                      WHERE source = 'csv' AND session_id = ?)
                OR b_staged_id IN (
                     SELECT id FROM staged_transactions
                      WHERE source = 'csv' AND session_id = ?)
            """,
            (str(import_id), str(import_id)),
        )
        conn.execute(
            """
            INSERT INTO staged_pairs
                (kind, confidence, a_staged_id, b_staged_id, reason)
            SELECT rp.kind,
                   rp.confidence,
                   sa.id,
                   sb.id,
                   rp.reason
              FROM row_pairs rp
              JOIN raw_rows ra ON ra.id = rp.row_a_id
              JOIN raw_rows rb ON rb.id = rp.row_b_id
              JOIN sources  s  ON s.id = ra.source_id
              JOIN staged_transactions sa
                     ON sa.source = 'csv'
                    AND json_extract(sa.source_ref, '$.raw_row_id') = ra.id
              JOIN staged_transactions sb
                     ON sb.source = 'csv'
                    AND json_extract(sb.source_ref, '$.raw_row_id') = rb.id
             WHERE s.upload_id = ?
            """,
            (import_id,),
        )
        # Advance staged rows that now participate in a pair to 'matched'.
        conn.execute(
            """
            UPDATE staged_transactions
               SET status = 'matched',
                   updated_at = datetime('now')
             WHERE status IN ('new', 'classified')
               AND id IN (SELECT a_staged_id FROM staged_pairs
                          UNION
                          SELECT b_staged_id FROM staged_pairs
                           WHERE b_staged_id IS NOT NULL)
            """
        )
    except sqlite3.OperationalError:
        # Staging tables missing on a legacy DB — skip mirror, don't
        # break the importer.
        return
