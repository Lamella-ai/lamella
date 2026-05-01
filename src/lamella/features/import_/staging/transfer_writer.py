# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Transfer writer — emit balanced Beancount txns for staged pairs.

NEXTGEN.md Phase C2b. When the matcher identifies a cross-source
transfer pair (e.g., PayPal CSV row ↔ Bank One SimpleFIN row),
this writer collapses both sides into a single balanced Beancount
transaction written to ``connector_transfers.bean`` — instead of
emitting two one-sided FIXME entries from each source's own writer.

Contract:

* Runs AFTER the matcher sweep and BEFORE the source-specific
  writer (SimpleFIN writer, importer emit) promotes rows.
* Only consumes pairs where ``kind='transfer'``,
  ``confidence='high'``, and both sides are still un-promoted.
* Resolves each side's source account via source-specific lookups:
    - SimpleFIN: ``source_ref['account_id']`` → ``accounts_meta``
    - CSV importer: ``source_ref['raw_row_id']`` →
      ``classifications.source_account``
    - Other sources: skipped in C2b (paste / reboot carry account
      in source_ref when Phase D / E wires them).
* Skips a pair if either side's account cannot be resolved — the
  pair record stays, the individual-source writers handle the
  legs, and the user can consolidate manually through the review
  UI.
* bean-check is run against the post-write ledger with the usual
  baseline-subtracted tolerance. On new errors, the writer
  reverts its append and raises ``BeanCheckError``; the pair
  stays in ``staged_pairs`` and both rows stay un-promoted so the
  source writers take over.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date as _date
from decimal import Decimal
from json import loads as _json_loads
from pathlib import Path
from typing import Any

from lamella.core.identity import (
    REF_KEY,
    SOURCE_KEY,
    TXN_ID_KEY,
    mint_txn_id,
)
from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)
from lamella.features.import_.staging.service import StagingService

log = logging.getLogger(__name__)

__all__ = [
    "TransferWriter",
    "emit_pending_pairs",
    "ensure_transfers_file_exists",
    "TRANSFERS_HEADER",
]


TRANSFERS_HEADER = (
    "; connector_transfers.bean — Managed by Lamella "
    "(NEXTGEN Phase C2b).\n"
    "; Balanced transfer transactions produced by the unified "
    "matcher when two staged rows\n"
    "; from different sources pair up. Do not hand-edit; the app "
    "is the sole writer.\n"
)


def ensure_transfers_file_exists(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(TRANSFERS_HEADER, encoding="utf-8")


# --- account resolution -------------------------------------------------


def _resolve_account_for_side(
    conn: sqlite3.Connection,
    *,
    source: str,
    source_ref: dict[str, Any],
) -> str | None:
    """Look up the source-POV account for one side of a pair.

    Returns ``None`` when the account can't be resolved. Caller skips
    the pair and lets the per-source writers emit normally.
    """
    if source == "simplefin":
        account_id = source_ref.get("account_id")
        if not account_id:
            return None
        row = conn.execute(
            "SELECT account_path FROM accounts_meta "
            "WHERE simplefin_account_id = ?",
            (str(account_id),),
        ).fetchone()
        return row["account_path"] if row else None
    if source in {"csv", "ods", "xlsx"}:
        raw_row_id = source_ref.get("raw_row_id")
        if not raw_row_id:
            return None
        row = conn.execute(
            "SELECT source_account FROM classifications WHERE raw_row_id = ?",
            (int(raw_row_id),),
        ).fetchone()
        if row and row["source_account"]:
            return row["source_account"]
        return None
    # paste / reboot / unknown — not yet supported in C2b.
    return None


# --- rendering ----------------------------------------------------------


def _q(s: str | None) -> str:
    if not s:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _normalize_amount(amount_text: str) -> Decimal:
    """Parse staged amount text. Staging stores exact Decimal strings."""
    return Decimal(amount_text)


@dataclass(frozen=True)
class _PairContext:
    """One row ready to render. Both sides of a pair produce one
    ``_PairContext`` each; the writer renders them together."""
    pair_id: int
    a_staged_id: int
    a_source: str
    a_account: str
    a_amount: Decimal
    a_date: str
    a_payee: str | None
    a_description: str | None
    a_currency: str
    a_source_ref: dict[str, Any]
    a_lamella_txn_id: str | None
    b_staged_id: int
    b_source: str
    b_account: str
    b_amount: Decimal
    b_date: str
    b_payee: str | None
    b_description: str | None
    b_currency: str
    b_source_ref: dict[str, Any]
    b_lamella_txn_id: str | None


def _render_transfer(ctx: _PairContext) -> str:
    """One balanced transaction with two legs + source-back-reference
    metadata per leg. Deterministic formatting for stable diffs."""
    # Pick the earlier date of the two sides for the txn header. Both
    # legs are semantically the same event.
    txn_date = min(ctx.a_date, ctx.b_date)
    payee = ctx.a_payee or ctx.b_payee or ""
    narration_bits: list[str] = []
    if ctx.a_description and ctx.a_description != payee:
        narration_bits.append(ctx.a_description)
    elif ctx.b_description and ctx.b_description != payee:
        narration_bits.append(ctx.b_description)
    narration = " | ".join(narration_bits) or "transfer"

    # Leg amount formatting: preserve decimal precision, align sign.
    def _fmt(amt: Decimal) -> str:
        return f"{amt:.2f}"

    lines: list[str] = []
    lines.append(
        f'{txn_date} * "{_q(payee)}" "{_q(narration)}"'
        if payee
        else f'{txn_date} * "{_q(narration)}"'
    )
    # Leg A
    lines.append(f"  {ctx.a_account:<40s}  {_fmt(ctx.a_amount)} {ctx.a_currency}")
    for key, val in _leg_metadata(ctx.a_source, ctx.a_source_ref).items():
        lines.append(f"    {key}: \"{_q(str(val))}\"")
    # Leg B
    lines.append(f"  {ctx.b_account:<40s}  {_fmt(ctx.b_amount)} {ctx.b_currency}")
    for key, val in _leg_metadata(ctx.b_source, ctx.b_source_ref).items():
        lines.append(f"    {key}: \"{_q(str(val))}\"")
    # Txn-level meta inserted right after the header line. Lineage
    # comes first so audit-log scans + AI history (post Phase 3) find
    # it without walking the file. Use leg A's staged-side identity
    # so any /txn/{a_token} bookmark continues to resolve to the same
    # transaction post-promotion. Leg B's identity is preserved as an
    # alias so /txn/{b_token} continues to resolve too — the
    # immutable-URL invariant covers both legs of a paired transfer.
    primary_id = ctx.a_lamella_txn_id or mint_txn_id()
    meta_lines = [f'  {TXN_ID_KEY}: "{primary_id}"']
    if ctx.b_lamella_txn_id and ctx.b_lamella_txn_id != primary_id:
        meta_lines.append(
            f'  {TXN_ID_KEY}-alias-0: "{ctx.b_lamella_txn_id}"'
        )
    meta_lines.append(f'  lamella-transfer-pair-id: "{ctx.pair_id}"')
    for offset, line in enumerate(meta_lines, start=1):
        lines.insert(offset, line)
    return "\n" + "\n".join(lines) + "\n"


def _leg_metadata(source: str, source_ref: dict[str, Any]) -> dict[str, str]:
    """Per-leg traceability: which source + source-ref produced this
    leg? Consumers that want to trace a transfer back to the bridge
    or spreadsheet can follow these metadata keys.

    Schema (post Phase 2 of NORMALIZE_TXN_IDENTITY.md): paired indexed
    source keys (``lamella-source-0`` + ``lamella-source-reference-id-0``).
    The legacy debug keys (``lamella-import-raw-row``,
    ``lamella-import-session``) are kept on importer legs for now but
    the indexed pair becomes the canonical identity for cross-source
    dedup. SimpleFIN legs no longer write the legacy txn-level
    ``lamella-simplefin-id`` because a transfer leg is on a posting,
    where the legacy format wasn't appropriate anyway.
    """
    if source == "simplefin":
        txn_id = source_ref.get("txn_id")
        if txn_id:
            return {
                f"{SOURCE_KEY}-0": "simplefin",
                f"{REF_KEY}-0": str(txn_id),
            }
    if source in {"csv", "ods", "xlsx"}:
        raw_row_id = source_ref.get("raw_row_id")
        upload_id = source_ref.get("upload_id")
        # Reference id: source-provided ids are durable across DB wipes
        # only when stamped on the source itself. ``raw_row_id`` is a
        # SQLite PK (reconstruct-unsafe) — we still emit it as a
        # legacy debug key so dedup against existing entries finds a
        # match, but the canonical paired source uses a content hash
        # if no durable id is available.
        out: dict[str, str] = {}
        durable_ref = source_ref.get("transaction_id") or source_ref.get("source_ref")
        if durable_ref:
            out[f"{SOURCE_KEY}-0"] = source if source == "csv" else "csv"
            out[f"{REF_KEY}-0"] = str(durable_ref)
        if raw_row_id is not None:
            out["lamella-import-raw-row"] = str(raw_row_id)
        if upload_id is not None:
            out["lamella-import-session"] = str(upload_id)
        return out
    return {}


# --- writer -------------------------------------------------------------


class TransferWriter:
    """Writes balanced transfer transactions from staged pairs to a
    Connector-owned ``.bean`` file. One instance per ingest pass."""

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        main_bean: Path,
        transfers_path: Path,
    ):
        self.conn = conn
        self.main_bean = main_bean
        self.transfers_path = transfers_path

    def emit_pending_pairs(
        self, *, min_confidence: str = "high",
    ) -> int:
        """Write balanced transactions for every eligible pair.

        Returns count of pair transactions written. Idempotent: a
        pair whose rows are already promoted is skipped.
        """
        contexts = self._load_eligible_pairs(min_confidence=min_confidence)
        if not contexts:
            return 0

        _rc, baseline = capture_bean_check(self.main_bean)
        ensure_transfers_file_exists(self.transfers_path)
        pre_bytes = self.transfers_path.read_bytes()

        svc = StagingService(self.conn)
        written = 0
        try:
            # Append all pair txns in one write to minimize bean-check runs.
            rendered: list[str] = [_render_transfer(c) for c in contexts]
            with self.transfers_path.open("a", encoding="utf-8") as fh:
                for text in rendered:
                    fh.write(text)
                fh.flush()

            # One bean-check after the batch write.
            run_bean_check_vs_baseline(self.main_bean, baseline)

            # Mark both sides of every pair promoted.
            for ctx in contexts:
                svc.mark_promoted(
                    ctx.a_staged_id,
                    promoted_to_file=str(self.transfers_path),
                    promoted_txn_hash=f"pair-{ctx.pair_id}",
                )
                svc.mark_promoted(
                    ctx.b_staged_id,
                    promoted_to_file=str(self.transfers_path),
                    promoted_txn_hash=f"pair-{ctx.pair_id}",
                )
                written += 1
        except BeanCheckError:
            log.warning(
                "transfer writer bean-check failed — reverting append"
            )
            self.transfers_path.write_bytes(pre_bytes)
            raise

        log.info("transfer writer: emitted %d paired transaction(s)", written)
        return written

    def _load_eligible_pairs(
        self, *, min_confidence: str,
    ) -> list[_PairContext]:
        """Pull pairs whose kind is transfer, confidence is at or
        above floor, and both sides are un-promoted. Resolves source
        accounts; skips any pair that can't be resolved."""
        confidence_sql = {"low", "medium", "high"}
        floor_order = {"low": 0, "medium": 1, "high": 2}
        floor = floor_order.get(min_confidence, 2)
        wanted = {c for c in confidence_sql if floor_order[c] >= floor}
        placeholders = ",".join("?" for _ in wanted)

        rows = self.conn.execute(
            f"""
            SELECT p.id AS pair_id,
                   p.a_staged_id, p.b_staged_id,
                   a.source AS a_source, a.source_ref AS a_ref,
                   a.amount AS a_amount, a.posting_date AS a_date,
                   a.payee AS a_payee, a.description AS a_desc,
                   a.currency AS a_currency,
                   a.lamella_txn_id AS a_lamella_txn_id,
                   b.source AS b_source, b.source_ref AS b_ref,
                   b.amount AS b_amount, b.posting_date AS b_date,
                   b.payee AS b_payee, b.description AS b_desc,
                   b.currency AS b_currency,
                   b.lamella_txn_id AS b_lamella_txn_id
              FROM staged_pairs p
              JOIN staged_transactions a ON a.id = p.a_staged_id
              JOIN staged_transactions b ON b.id = p.b_staged_id
             WHERE p.kind = 'transfer'
               AND p.confidence IN ({placeholders})
               AND a.status IN ('new', 'classified', 'matched')
               AND b.status IN ('new', 'classified', 'matched')
            """,
            list(wanted),
        ).fetchall()

        out: list[_PairContext] = []
        for r in rows:
            a_ref = _json_loads(r["a_ref"]) if r["a_ref"] else {}
            b_ref = _json_loads(r["b_ref"]) if r["b_ref"] else {}
            a_account = _resolve_account_for_side(
                self.conn, source=r["a_source"], source_ref=a_ref,
            )
            b_account = _resolve_account_for_side(
                self.conn, source=r["b_source"], source_ref=b_ref,
            )
            if not a_account or not b_account:
                log.info(
                    "transfer pair %s: account unresolvable (a=%r b=%r) — "
                    "leaving for per-source writers",
                    r["pair_id"], a_account, b_account,
                )
                continue
            row_keys = r.keys() if hasattr(r, "keys") else []
            a_lid = r["a_lamella_txn_id"] if "a_lamella_txn_id" in row_keys else None
            b_lid = r["b_lamella_txn_id"] if "b_lamella_txn_id" in row_keys else None
            out.append(
                _PairContext(
                    pair_id=int(r["pair_id"]),
                    a_staged_id=int(r["a_staged_id"]),
                    a_source=r["a_source"],
                    a_account=a_account,
                    a_amount=_normalize_amount(r["a_amount"]),
                    a_date=r["a_date"],
                    a_payee=r["a_payee"],
                    a_description=r["a_desc"],
                    a_currency=r["a_currency"] or "USD",
                    a_source_ref=a_ref,
                    a_lamella_txn_id=a_lid,
                    b_staged_id=int(r["b_staged_id"]),
                    b_source=r["b_source"],
                    b_account=b_account,
                    b_amount=_normalize_amount(r["b_amount"]),
                    b_date=r["b_date"],
                    b_payee=r["b_payee"],
                    b_description=r["b_desc"],
                    b_currency=r["b_currency"] or "USD",
                    b_source_ref=b_ref,
                    b_lamella_txn_id=b_lid,
                )
            )
        return out


# Functional wrapper for callers that don't want to instantiate the class.
def emit_pending_pairs(
    conn: sqlite3.Connection,
    *,
    main_bean: Path,
    transfers_path: Path,
    min_confidence: str = "high",
) -> int:
    return TransferWriter(
        conn=conn, main_bean=main_bean, transfers_path=transfers_path,
    ).emit_pending_pairs(min_confidence=min_confidence)
