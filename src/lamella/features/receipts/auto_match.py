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
asks ``find_document_candidates`` for candidates, and auto-links the
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
from lamella.features.receipts.linker import DocumentLinker
from lamella.features.receipts.scorer import (
    AUTO_LINK_THRESHOLD as _SCORER_AUTO_LINK_THRESHOLD,
    REVIEW_THRESHOLD as _SCORER_REVIEW_THRESHOLD,
)
from lamella.features.receipts.txn_matcher import (
    find_document_candidates,
    find_ledger_candidates,
)

log = logging.getLogger(__name__)

# Min score for automatic (unattended) linking. The interactive /card
# UI uses 0.85 for "click-to-attach-default-checked"; here we go a
# touch higher because nobody is watching.
#
# ADR-0063 §2: this constant is **defined once** in scorer.py and
# re-exported here. The forward sweep (sweep_recent) and the reverse
# direction (auto_link_unlinked_documents) both reference the same
# value so a tuning change updates both directions atomically.
AUTO_LINK_THRESHOLD = _SCORER_AUTO_LINK_THRESHOLD
REVIEW_THRESHOLD = _SCORER_REVIEW_THRESHOLD

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
        "SELECT 1 FROM document_link_blocks "
        "WHERE txn_hash = ? AND paperless_id = ? LIMIT 1",
        (txn_hash_value, int(paperless_id)),
    ).fetchone()
    return row is not None


def _has_existing_link(conn: sqlite3.Connection, txn_hash_value: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM document_links WHERE txn_hash = ? LIMIT 1",
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
    linker = DocumentLinker(
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
                candidates = find_document_candidates(
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


# ─── Reverse direction (ADR-0063 §3): document -> ledger auto-link ────


@dataclass
class AutoLinkReport:
    """Outcome of an :func:`auto_link_unlinked_documents` invocation.

    All counters are stable: a no-op invocation returns
    ``AutoLinkReport()`` (every count zero, ``linked_pairs`` empty).
    """

    scanned: int = 0
    linked: int = 0
    queued_for_review: int = 0
    skipped_excluded: int = 0
    skipped_ambiguous: int = 0
    skipped_no_candidate: int = 0
    linked_pairs: list[tuple[int, str, float]] = field(default_factory=list)
    """List of (paperless_id, txn_hash, score) tuples for successful links."""

    errors: list[str] = field(default_factory=list)


def _select_unlinked_extracted_docs(conn: sqlite3.Connection) -> list[dict]:
    """Pull docs that have AI-extracted fields and are not yet linked.

    The "extracted" predicate is currently a (best-effort) proxy:
    rows with a non-null ``total_amount`` are treated as extracted
    because that's the field downstream auto-link cares about. When
    Worker F's tag-workflow lands, this function should switch to
    the explicit ``Lamella_Extracted`` tag check via the doc's
    ``tags_json`` field — both definitions converge in practice.
    """
    deleted_filter = (
        " AND paperless_id NOT IN (SELECT paperless_id FROM paperless_deleted_docs)"
    )
    link_filter = (
        " AND paperless_id NOT IN (SELECT paperless_id FROM document_links)"
    )
    rows = conn.execute(
        "SELECT paperless_id, title, correspondent_name, document_date, "
        "       created_date, total_amount, subtotal_amount, vendor, "
        "       payment_last_four, document_type, document_type_id, "
        "       document_type_name, content_excerpt, tags_json "
        "FROM paperless_doc_index "
        "WHERE total_amount IS NOT NULL "
        "AND (document_date IS NOT NULL OR created_date IS NOT NULL)"
        + deleted_filter
        + link_filter
        + " ORDER BY paperless_id ASC LIMIT 500"
    ).fetchall()
    return [dict(r) for r in rows]


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _date_or_none(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:  # noqa: BLE001
        return None


def auto_link_unlinked_documents(
    conn: sqlite3.Connection,
    *,
    reader: LedgerReader | None = None,
    settings: Settings | None = None,
    paperless_client: PaperlessClient | None = None,
    confidence_gap: float = 0.10,
    dry_run: bool = False,
    emit=None,
) -> AutoLinkReport:
    """ADR-0063 reverse-direction auto-link sweep.

    For every extracted-but-unlinked document, walk the ledger and
    score candidate transactions via the shared :class:`Scorer`. If
    the top candidate's score >= ``AUTO_LINK_THRESHOLD`` AND the
    second-place candidate trails by at least ``confidence_gap``,
    write the link directive (Phase-1 ``document-link`` vocabulary)
    and the ``document_links`` row via :class:`DocumentLinker`.

    Otherwise:
      * If the top candidate is in ``[REVIEW_THRESHOLD, AUTO_LINK_THRESHOLD)``
        the doc is left for sub-threshold review on /inbox (the
        existing review surface) — it shows up via the matching
        page section that ADR-0063 §5 introduces.
      * If the top candidate is below review_threshold, no action
        is taken; the doc waits for the next sweep or for a future
        txn to land that scores higher.

    Excluded outright (``skipped_excluded``):
      * Documents whose canonical ``document_type`` is ``statement``
        or ``tax``.

    ``paperless_client`` is optional. When provided, on a successful
    link the function tags the doc with ``Lamella_Linked`` (Worker F
    owns the tag-write path; see TODO below). When None, link goes
    through but the Paperless tag is not applied — the SQLite row +
    ledger directive still record the link; the next tag-workflow
    sweep will catch up.
    """
    report = AutoLinkReport()
    if settings is None:
        from lamella.core.config import get_settings
        settings = get_settings()
    if reader is None:
        reader = LedgerReader(settings.ledger_main)

    try:
        ledger_entries = list(reader.load().entries)
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"ledger load failed: {exc}")
        return report

    docs = _select_unlinked_extracted_docs(conn)

    linker: DocumentLinker | None = None
    if not dry_run:
        linker = DocumentLinker(
            conn=conn,
            main_bean=settings.ledger_main,
            connector_links=settings.connector_links_path,
        )

    for doc_row in docs:
        report.scanned += 1
        pid = int(doc_row["paperless_id"])
        doctype = (doc_row.get("document_type") or "").strip().lower() or None
        # ADR-0063 §6: statement / tax docs never participate.
        if doctype in {"statement", "tax"}:
            report.skipped_excluded += 1
            continue
        doc_total = _decimal_or_none(doc_row.get("total_amount"))
        doc_subtotal = _decimal_or_none(doc_row.get("subtotal_amount"))
        doc_date = _date_or_none(doc_row.get("document_date")) or _date_or_none(
            doc_row.get("created_date")
        )
        if doc_total is None or doc_date is None:
            report.skipped_no_candidate += 1
            continue
        candidates = find_ledger_candidates(
            conn,
            doc_date=doc_date,
            doc_total=doc_total,
            doc_currency="USD",  # paperless_doc_index has no currency col today
            doc_vendor=doc_row.get("vendor"),
            doc_doctype=doctype,
            doc_id=pid,
            ledger_entries=ledger_entries,
            doc_subtotal=doc_subtotal,
            doc_correspondent=doc_row.get("correspondent_name"),
            doc_content_excerpt=doc_row.get("content_excerpt"),
            doc_last_four=doc_row.get("payment_last_four"),
        )
        if not candidates:
            report.skipped_no_candidate += 1
            continue
        top = candidates[0]
        if top.score < AUTO_LINK_THRESHOLD:
            # Sub-threshold: leave for /inbox sub-threshold review.
            if top.score >= REVIEW_THRESHOLD:
                report.queued_for_review += 1
            else:
                report.skipped_no_candidate += 1
            continue
        if len(candidates) > 1:
            second = candidates[1]
            if (top.score - second.score) < confidence_gap:
                # Ambiguous: two near-equal candidates. Per ADR-0063 §3,
                # do NOT auto-link; leave for human review.
                report.skipped_ambiguous += 1
                report.queued_for_review += 1
                log.info(
                    "auto-link skipped ambiguous doc=%d top=%.2f second=%.2f gap<%.2f",
                    pid, top.score, second.score, confidence_gap,
                )
                continue
        if dry_run or linker is None:
            report.linked_pairs.append((pid, top.txn_hash, float(top.score)))
            report.linked += 1
            continue
        # Apply the link.
        try:
            linker.link(
                paperless_id=pid,
                txn_hash=top.txn_hash,
                txn_date=top.txn_date,
                txn_amount=top.txn_amount,
                match_method="auto_reverse_sweep",
                match_confidence=float(top.score),
                paperless_hash=cached_paperless_hash(conn, pid),
                paperless_url=paperless_url_for(settings.paperless_url, pid),
            )
        except BeanCheckError as exc:
            log.warning(
                "auto-link bean-check rejected doc=%d -> %s: %s",
                pid, top.txn_hash[:8], exc,
            )
            report.errors.append(
                f"doc {pid} -> {top.txn_hash[:8]}: bean-check blocked ({exc})"
            )
            continue
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "auto-link failed doc=%d -> %s: %s",
                pid, top.txn_hash[:8], exc,
            )
            report.errors.append(
                f"doc {pid} -> {top.txn_hash[:8]}: link failed ({exc})"
            )
            continue
        report.linked += 1
        report.linked_pairs.append((pid, top.txn_hash, float(top.score)))
        if emit is not None:
            emit(
                f"Reverse-linked Paperless #{pid} → {top.txn_hash[:8]}… "
                f"(score {top.score:.2f})",
                outcome="success",
            )

        # Tag with Lamella:Linked when we have a live client.
        # TODO(adr-0062): when Worker F lands the tag-workflow engine,
        # delegate this to ``apply_tag(client, pid, TAG_LINKED)`` so
        # all tag mutations route through one place. For now the raw
        # client.patch_document call is the minimum viable wiring.
        # ADR-0064: name uses the canonical colon separator; ensure_tag
        # is backwards-compat with any not-yet-migrated legacy tag.
        if paperless_client is not None:
            from lamella.features.paperless_bridge.lamella_namespace import (
                TAG_LINKED as _TAG_LINKED,
            )
            loop: asyncio.AbstractEventLoop | None = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                tag_id = loop.run_until_complete(
                    paperless_client.ensure_tag(_TAG_LINKED)
                )
                # Read current tags so we union rather than replace.
                try:
                    current = loop.run_until_complete(
                        paperless_client.get_document(pid)
                    )
                    existing_tags = list(current.get("tags") or [])
                except Exception:  # noqa: BLE001
                    existing_tags = []
                if tag_id not in existing_tags:
                    existing_tags.append(tag_id)
                    loop.run_until_complete(
                        paperless_client.patch_document(
                            pid, tags=existing_tags
                        )
                    )
            except Exception as exc:  # noqa: BLE001 — tag write is best effort
                log.info(
                    "auto-link Lamella:Linked tag write failed for doc %d: %s",
                    pid, exc,
                )
            finally:
                if loop is not None:
                    try:
                        loop.close()
                    except Exception:  # noqa: BLE001
                        pass

    return report
