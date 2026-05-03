from __future__ import annotations

import sqlite3
from typing import Any

from lamella.core.transform.reconstruct import ReconstructReport, register
from lamella.core.transform.verify import TablePolicy
from lamella.core.transform.verify import register as register_policy
from lamella.features.receipts.link_block_writer import read_link_blocks_from_entries


@register("step25:receipt-link-blocks", state_tables=["document_link_blocks"])
def reconstruct_receipt_link_blocks(
    conn: sqlite3.Connection, entries: list[Any]
) -> ReconstructReport:
    rows = read_link_blocks_from_entries(entries)
    written = 0
    for row in rows:
        cur = conn.execute(
            "INSERT INTO document_link_blocks (paperless_id, txn_hash, reason) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT (paperless_id, txn_hash) DO UPDATE SET "
            "reason = excluded.reason, blocked_at = CURRENT_TIMESTAMP",
            (int(row["paperless_id"]), str(row["txn_hash"]), row["reason"]),
        )
        if cur.rowcount:
            written += 1
    return ReconstructReport(
        pass_name="step25:receipt-link-blocks",
        rows_written=written,
        notes=[f"{len(rows)} active receipt link block(s)"],
    )


register_policy(
    TablePolicy(
        table="document_link_blocks",
        kind="state",
        primary_key=("paperless_id", "txn_hash"),
    )
)
