# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Post-ingest receipt sweep — auto-link high-confidence Paperless
candidates to unlinked transactions.

The user explicitly complained that receipt matching "has NEVER run
automatically." Today ``receipts.hunt`` only executes when the user
clicks a button in /search — so a 13k-FIXME backlog and every future
SimpleFIN fetch produces lots of unlinked txns that SHOULD have had
their receipts attached silently.

This module runs post-ingest (and is also callable from a standalone
bulk-sweep job): it walks every unlinked Transaction in a time window,
asks ``find_paperless_candidates`` for candidates, and auto-links the
top hit when its score crosses ``AUTO_LINK_THRESHOLD``. Lower-scored
candidates stay dormant so the user can review them via the existing
/search/receipt-hunt UI.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.adapters.paperless.client import PaperlessClient
from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.features.paperless_bridge.lookups import cached_paperless_hash
from lamella.adapters.paperless.schemas import paperless_url_for
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.receipts.linker import ReceiptLinker
from lamella.features.receipts.txn_matcher import find_paperless_candidates

log = logging.getLogger(__name__)

# Min score for automatic (unattended) linking. The interactive /card
# UI uses 0.85 for "click-to-attach-default-checked"; here we go a
# touch higher because nobody is watching.
AUTO_LINK_THRESHOLD = 0.90

# Window we scan on each sweep. 60 days covers most post-ingest
# backfills and a typical Paperless-upload-after-purchase lag.
DEFAULT_WINDOW_DAYS = 60


@dataclass
class AutoMatchResult:
    scanned: int = 0
    already_linked: int = 0
    matched: int = 0
    no_candidate: int = 0
    low_confidence: int = 0
    linked_pairs: list[tuple[str, int, float]] = field(default_factory=list)  # (txn_hash, paperless_id, score)
    errors: list[str] = field(default_factory=list)


def _pair_blocked(
    conn: sqlite3.Connection,
    *,
    txn_hash_value: str,
    paperless_id: int,
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM receipt_link_blocks "
        "WHERE txn_hash = ? AND paperless_id = ? LIMIT 1",
        (txn_hash_value, int(paperless_id)),
    ).fetchone()
    return row is not None


def _has_existing_link(conn: sqlite3.Connection, txn_hash_value: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM receipt_links WHERE txn_hash = ? LIMIT 1",
        (txn_hash_value,),
    ).fetchone()
    return row is not None


def _best_expense_amount(txn: Transaction) -> tuple[Decimal, str] | None:
    """Pick the amount we should use to search Paperless for a receipt.

    Mirrors the logic used in ``search._best_expense_amount`` so the
    sweep sees the same candidates the UI does."""
    target_roots = ("Expenses", "Income", "Liabilities", "Equity")
    best: tuple[Decimal, str] | None = None
    for p in txn.postings or ():
        acct = p.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root not in target_roots:
            continue
        if p.units and p.units.number is not None:
            amt = abs(Decimal(p.units.number))
            ccy = p.units.currency or "USD"
            if best is None or amt > best[0]:
                best = (amt, ccy)
    return best


def sweep_recent(
    *,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    settings: Settings,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_score: float = AUTO_LINK_THRESHOLD,
    emit=None,
) -> AutoMatchResult:
    """Sweep Transactions in the last ``window_days`` for unlinked
    receipts. Auto-link any with a top candidate score >= ``min_score``.

    ``emit`` — optional ``JobContext.emit``-compatible callable for
    per-txn progress when called from a background job. Safe to leave
    None for post-ingest invocations that don't have a job context.
    """
    result = AutoMatchResult()
    cutoff = date.today() - timedelta(days=max(1, int(window_days)))

    entries = list(reader.load().entries)
    linker = ReceiptLinker(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_links=settings.connector_links_path,
    )

    # ADR-0044: best-effort Lamella_* field writeback after each
    # successful link. We open one Paperless client + one event
    # loop for the whole sweep so HTTP/2 connection pools stay
    # warm; both close in finally below. Writeback failures never
    # block matching — they're logged inside write_match_fields.
    live_paperless: PaperlessClient | None = None
    loop: asyncio.AbstractEventLoop | None = None
    if settings.paperless_configured:
        try:
            live_paperless = PaperlessClient(
                base_url=settings.paperless_url,  # type: ignore[arg-type]
                api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
                extra_headers=settings.paperless_extra_headers(),
            )
            loop = asyncio.new_event_loop()
        except Exception as exc:  # noqa: BLE001
            log.info(
                "ADR-0044: auto-match Paperless client setup failed; "
                "skipping Lamella_* writeback for this sweep: %s", exc,
            )
            live_paperless = None
            loop = None

    try:
        for entry in entries:
            if not isinstance(entry, Transaction):
                continue
            if not isinstance(entry.date, date) or entry.date < cutoff:
                continue
            result.scanned += 1
            target_hash = txn_hash(entry)
            if _has_existing_link(conn, target_hash):
                result.already_linked += 1
                continue
            amt_ccy = _best_expense_amount(entry)
            if amt_ccy is None:
                continue
            amount, currency = amt_ccy
            try:
                candidates = find_paperless_candidates(
                    conn,
                    txn_amount=amount,
                    txn_date=entry.date,
                    narration=entry.narration or None,
                    payee=getattr(entry, "payee", None),
                    limit=1,
                    min_score=min_score,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("auto-match lookup failed for %s: %s", target_hash[:8], exc)
                result.errors.append(f"{target_hash[:8]}: lookup failed ({exc})")
                continue
            if not candidates:
                result.no_candidate += 1
                continue
            top = candidates[0]
            if top.score < min_score:
                result.low_confidence += 1
                continue
            if _pair_blocked(
                conn,
                txn_hash_value=target_hash,
                paperless_id=int(top.paperless_id),
            ):
                log.info(
                    "auto-match skipped blocked pair txn=%s doc=%d",
                    target_hash[:8], int(top.paperless_id),
                )
                result.no_candidate += 1
                continue

            # Guard against stale local index rows / uncertain transport.
            # If a live existence check cannot confirm the candidate,
            # do NOT link it here. Deletion confirmation/removal policy
            # belongs to the dedicated dangling-link sweeper, which
            # requires repeated 404 evidence over time before cleanup.
            if live_paperless is not None and loop is not None:
                try:
                    loop.run_until_complete(
                        live_paperless.get_document(int(top.paperless_id))
                    )
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc)
                    if "returned 404" in msg:
                        log.info(
                            "auto-match skipped missing Paperless #%d; "
                            "awaiting dangling-link confirmation",
                            int(top.paperless_id),
                        )
                    else:
                        log.info(
                            "auto-match skipped candidate #%d due to "
                            "live Paperless check failure: %s",
                            int(top.paperless_id),
                            exc,
                        )
                    result.no_candidate += 1
                    continue

            # Attach.
            try:
                linker.link(
                    paperless_id=int(top.paperless_id),
                    txn_hash=target_hash,
                    txn_date=entry.date,
                    txn_amount=amount,
                    match_method="auto_sweep",
                    match_confidence=float(top.score),
                    paperless_hash=cached_paperless_hash(conn, int(top.paperless_id)),
                    paperless_url=paperless_url_for(
                        settings.paperless_url, int(top.paperless_id),
                    ),
                )
            except BeanCheckError as exc:
                log.warning(
                    "auto-match bean-check rejected %s -> #%s: %s",
                    target_hash[:8], top.paperless_id, exc,
                )
                result.errors.append(
                    f"{target_hash[:8]}: bean-check blocked ({exc})"
                )
                continue
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "auto-match link failed %s -> #%s: %s",
                    target_hash[:8], top.paperless_id, exc,
                )
                result.errors.append(f"{target_hash[:8]}: link failed ({exc})")
                continue
            result.matched += 1
            result.linked_pairs.append(
                (target_hash, int(top.paperless_id), float(top.score))
            )
            if emit is not None:
                emit(
                    f"Linked {target_hash[:8]}… → Paperless #{top.paperless_id} "
                    f"(score {top.score:.2f})",
                    outcome="success",
                )

            # ADR-0044: write the four canonical Lamella_* fields
            # back to Paperless. Failures are logged inside
            # write_match_fields and never undo the link.
            if live_paperless is not None and loop is not None:
                try:
                    from lamella.features.paperless_bridge.writeback import (
                        write_match_fields,
                    )
                    loop.run_until_complete(
                        write_match_fields(
                            client=live_paperless,
                            paperless_id=int(top.paperless_id),
                            txn=entry,
                            conn=conn,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "ADR-0044 writeback unexpected failure for doc %d: %s",
                        int(top.paperless_id), exc,
                    )
    finally:
        if live_paperless is not None and loop is not None:
            try:
                loop.run_until_complete(live_paperless.aclose())
            except Exception:  # noqa: BLE001
                pass
            try:
                loop.close()
            except Exception:  # noqa: BLE001
                pass
    return result
