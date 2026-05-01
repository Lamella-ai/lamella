# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Receipt-hunt worker — background-job port of the inline logic that
used to live in routes/search.py::search_receipt_hunt.

The function :func:`run_hunt` is designed to be submitted to the
generic ``JobRunner``. It yields progress events via the
``JobContext`` so the browser's progress modal can show per-txn
updates ("Searching local index for …", "Auto-linked #12345",
"Candidates found — awaiting user pick", etc.) with Success /
Failure / Not Found / Error counters.

Why this exists: the route handler previously ran the entire hunt
synchronously. For a 92-transaction selection on a real user's
Paperless instance it blocked the browser for 20+ minutes with no
UI feedback. Moving the body here lets the route return a ``job_id``
immediately and the user sees live progress.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction

from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.core.config import Settings
from lamella.core.jobs import JobContext
from lamella.core.ledger_writer import BeanCheckError
from lamella.features.receipts.linker import ReceiptLinker

log = logging.getLogger(__name__)


def _best_expense_amount(txn: Transaction) -> Decimal | None:
    target_roots = ("Expenses", "Income", "Liabilities", "Equity")
    best: Decimal | None = None
    for p in txn.postings or []:
        acct = p.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root not in target_roots:
            continue
        if p.units and p.units.number is not None:
            amt = abs(Decimal(p.units.number))
            if best is None or amt > best:
                best = amt
    return best


def _merchant_needle(txn: Transaction | "_StagedSynth") -> str:
    import re as _re
    text = " ".join(filter(None, [
        getattr(txn, "payee", None),
        getattr(txn, "narration", "") or "",
    ])).strip()
    tokens = _re.findall(r"[A-Za-z]{4,}", text)
    return " ".join(tokens[:3]) if tokens else text


@dataclass
class _StagedSynth:
    """Minimal txn-shaped object for staged_transactions rows (ADR-0056).

    The hunt's amount-score / candidate-find / link path needs four
    pieces: target_amount, date, payee/narration label, currency. Real
    Transaction objects expose these via ``postings`` + ``date`` +
    ``payee`` + ``narration``; staged rows are flat. This shim mirrors
    the small subset of attribute access ``_process_one`` performs.

    Branches that require a full ``Transaction`` (Lamella_* writeback
    via ``write_match_fields``, post-link verify-and-correct) are gated
    on ``isinstance(txn, Transaction)`` so staged-side links succeed
    even though the auxiliary side-effects only run on the ledger
    side. Once a staged row is promoted, the standard ledger-side
    pipeline (which does have a Transaction) handles those.
    """
    date: date
    payee: str | None
    narration: str
    target_amount: Decimal
    currency: str
    # Empty postings list keeps any defensive `txn.postings or []`
    # iteration safe; _best_expense_amount is bypassed for synth.
    postings: tuple = ()


def run_hunt(
    ctx: JobContext,
    *,
    selected: list[str],
    q: str,
    lookback_days: str,
    tolerance_days: int,
    conn: sqlite3.Connection,
    reader: LedgerReader,
    settings: Settings,
) -> dict:
    """Run the receipt hunt for `selected` txn hashes.

    Emits one event per transaction (plus any side-channel notes)
    and returns a summary dict that the result template renders.
    """
    from lamella.features.paperless_bridge.sync import is_paperless_syncing

    if is_paperless_syncing(conn):
        ctx.emit(
            "Paperless sync is running — hunt refused (try again after sync).",
            outcome="error",
        )
        return {
            "report": {
                "q": q,
                "lookback_days": lookback_days,
                "tolerance_days": tolerance_days,
                "already_linked": [],
                "auto_linked": [],
                "ambiguous": [],
                "no_match": [],
                "errors": [{
                    "txn_hash": h,
                    "reason": "Paperless sync is currently running.",
                } for h in selected],
                "total_paperless_docs_indexed": 0,
                "paperless_sync_lookback_days": settings.paperless_sync_lookback_days,
                "_blocked_by_sync": True,
            },
        }

    ctx.set_total(len(selected))
    ctx.emit(f"Starting hunt over {len(selected)} transaction(s)", outcome="info")

    # Resolve selected txns from the ledger.
    by_hash: dict[str, Transaction] = {}
    for entry in reader.load().entries:
        if isinstance(entry, Transaction):
            by_hash[txn_hash(entry)] = entry

    # ADR-0056: same-shape receipt-find on staged rows. Selected values
    # may be Beancount content hashes (ledger rows) OR UUIDv7
    # lamella_txn_id tokens (staged rows). Resolve any token that
    # didn't match by_hash against staged_transactions and synthesise a
    # txn-shaped object the hunt's downstream amount-score / candidate
    # logic can consume. Skip already-promoted/dismissed rows — the
    # ledger pass picks those up via by_hash, and dismissed shouldn't
    # accept new links.
    by_token: dict[str, _StagedSynth] = {}
    unresolved = [s for s in selected if s not in by_hash]
    if unresolved:
        qmarks = ",".join("?" * len(unresolved))
        try:
            for r in conn.execute(
                f"SELECT lamella_txn_id, posting_date, amount, currency, "
                f"payee, description "
                f"FROM staged_transactions "
                f"WHERE lamella_txn_id IN ({qmarks}) "
                f"AND status NOT IN ('promoted','dismissed')",
                tuple(unresolved),
            ):
                try:
                    d = date.fromisoformat(str(r["posting_date"])[:10])
                except Exception:  # noqa: BLE001
                    continue
                try:
                    raw_amt = Decimal(str(r["amount"]))
                except Exception:  # noqa: BLE001
                    continue
                # Hunt needs |amount| (target spend); sign on the
                # staged row matches the SimpleFIN convention (negative
                # = charge). abs() is correct for receipt matching.
                target_amount = abs(raw_amt)
                if target_amount == 0:
                    continue
                by_token[r["lamella_txn_id"]] = _StagedSynth(
                    date=d,
                    payee=r["payee"],
                    narration=r["description"] or "",
                    target_amount=target_amount,
                    currency=r["currency"] or "USD",
                )
        except sqlite3.Error as exc:
            log.warning("receipt-hunt: staged_transactions lookup failed: %s", exc)

    existing_links = {
        row["txn_hash"]
        for row in conn.execute(
            f"SELECT txn_hash FROM receipt_links WHERE txn_hash IN "
            f"({','.join('?' * len(selected))})",
            tuple(selected),
        ).fetchall()
    }

    try:
        total_indexed = int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM paperless_doc_index"
            ).fetchone()["n"] or 0
        )
    except sqlite3.Error:
        total_indexed = 0
    report: dict[str, Any] = {
        "q": q,
        "lookback_days": lookback_days,
        "tolerance_days": tolerance_days,
        "already_linked": [],
        "auto_linked": [],
        "ambiguous": [],
        "no_match": [],
        "errors": [],
        "total_paperless_docs_indexed": total_indexed,
        "paperless_sync_lookback_days": settings.paperless_sync_lookback_days,
    }

    from lamella.features.paperless_bridge.lookups import cached_paperless_hash
    from lamella.adapters.paperless.schemas import paperless_url_for

    linker = ReceiptLinker(
        conn=conn,
        main_bean=settings.ledger_main,
        connector_links=settings.connector_links_path,
    )

    # Writeback-enabled check.
    from lamella.features.ai_cascade.service import AIService as _AIService
    _ai = _AIService(settings=settings, conn=conn)
    writeback_on = False
    try:
        raw = _ai.settings_store.get("paperless_writeback_enabled")
        if raw is None:
            writeback_on = bool(settings.paperless_writeback_enabled)
        else:
            writeback_on = str(raw).strip().lower() not in ("0", "false", "no", "off")
    except Exception:  # noqa: BLE001
        writeback_on = bool(settings.paperless_writeback_enabled)

    # Paperless client — opened once, closed in finally below.
    live_paperless = None
    live_field_mapping = None
    verify_service = None
    if settings.paperless_configured:
        try:
            from lamella.adapters.paperless.client import PaperlessClient
            from lamella.features.paperless_bridge.field_map import get_map
            live_paperless = PaperlessClient(
                base_url=settings.paperless_url,  # type: ignore[arg-type]
                api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
                extra_headers=settings.paperless_extra_headers(),
            )
            live_field_mapping = get_map(conn)
            if writeback_on:
                from lamella.features.paperless_bridge.verify import VerifyService
                verify_service = VerifyService(
                    ai=_ai, paperless=live_paperless, conn=conn,
                )
        except Exception as exc:  # noqa: BLE001
            ctx.emit(
                f"Live-Paperless prep failed: {exc}", outcome="error",
            )
            live_paperless = None
            verify_service = None

    # A single event loop for all async Paperless + verify calls
    # this thread will make. Opening/closing one per call inflates
    # latency on a 92-item hunt; owning it for the whole run keeps
    # HTTP/2 connection pools warm across transactions.
    loop = asyncio.new_event_loop()
    try:
        for h in selected:
            ctx.raise_if_cancelled()
            try:
                _process_one(
                    ctx=ctx,
                    h=h,
                    report=report,
                    by_hash=by_hash,
                    by_token=by_token,
                    existing_links=existing_links,
                    tolerance_days=tolerance_days,
                    conn=conn,
                    settings=settings,
                    linker=linker,
                    cached_paperless_hash_fn=cached_paperless_hash,
                    paperless_url_for_fn=paperless_url_for,
                    live_paperless=live_paperless,
                    live_field_mapping=live_field_mapping,
                    verify_service=verify_service,
                    loop=loop,
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("receipt-hunt: unexpected error on %s", h[:8])
                report["errors"].append({
                    "txn_hash": h,
                    "reason": f"internal error: {exc}",
                })
                ctx.emit(
                    f"{h[:8]}… — internal error: {exc}",
                    outcome="error",
                )
            finally:
                ctx.advance()
    finally:
        if live_paperless is not None:
            try:
                loop.run_until_complete(live_paperless.aclose())
            except Exception:  # noqa: BLE001
                pass
        try:
            loop.close()
        except Exception:  # noqa: BLE001
            pass
        reader.invalidate()

    ctx.emit(
        f"Hunt complete — auto-linked {len(report['auto_linked'])}, "
        f"ambiguous {len(report['ambiguous'])}, "
        f"no-match {len(report['no_match'])}, "
        f"errors {len(report['errors'])}",
        outcome="info",
    )
    return {"report": report}


def _process_one(
    *,
    ctx: JobContext,
    h: str,
    report: dict[str, Any],
    by_hash: dict[str, Transaction],
    by_token: dict[str, _StagedSynth] | None = None,
    existing_links: set[str],
    tolerance_days: int,
    conn: sqlite3.Connection,
    settings: Settings,
    linker: ReceiptLinker,
    cached_paperless_hash_fn,
    paperless_url_for_fn,
    live_paperless,
    live_field_mapping,
    verify_service,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Single-transaction branch of the hunt. Updates ``report`` in
    place and emits an event with the outcome so the progress modal
    ticks.

    ``h`` may be a Beancount content hash (ledger row) or a
    lamella-txn-id UUIDv7 (staged row, ADR-0056). Ledger lookup wins
    when both happen to be present; falls back to ``by_token`` for
    staged-only selections.
    """
    if h in existing_links:
        report["already_linked"].append({"txn_hash": h})
        ctx.emit(f"{h[:8]}… — already linked, skipping", outcome="info")
        return
    txn: Transaction | _StagedSynth | None = by_hash.get(h)
    is_staged = False
    if txn is None and by_token:
        synth = by_token.get(h)
        if synth is not None:
            txn = synth
            is_staged = True
    if txn is None:
        report["errors"].append({"txn_hash": h, "reason": "not in ledger or staging"})
        ctx.emit(f"{h[:8]}… — not in ledger or staging", outcome="error")
        return
    if is_staged:
        # Synth carries the precomputed |amount|; postings-walk doesn't apply.
        target_amount = txn.target_amount  # type: ignore[union-attr]
    else:
        target_amount = _best_expense_amount(txn)  # type: ignore[arg-type]
    if target_amount is None:
        report["errors"].append({"txn_hash": h, "reason": "no expense amount"})
        ctx.emit(f"{h[:8]}… — no expense amount", outcome="error")
        return

    label = (
        (getattr(txn, "payee", None) or txn.narration or "(unknown)").strip()
        or "(unknown)"
    )
    ctx.emit(
        f"{h[:8]}… {txn.date} ${target_amount:.2f} at {label[:40]}",
        outcome="info",
    )

    # ADR-0022: format directly from Decimal — no float trip.
    amount_str = f"{target_amount:.2f}"
    amount_like = f"%{amount_str}%"
    txn_iso = txn.date.isoformat()
    cents_threshold = Decimal("0.01")

    def _amount_score(row_amount: Any, content_excerpt: str | None) -> int:
        """Replicates the SQL CASE for amount_score: 3 for amount match,
        1 for amount-string in content, else 0. Uses Decimal compare so
        precision survives the TEXT round-trip on total_amount."""
        if row_amount is not None:
            try:
                d = Decimal(str(row_amount))
            except Exception:  # noqa: BLE001
                d = None
            if d is not None and abs(d - target_amount) < cents_threshold:
                return 3
        if content_excerpt and amount_str in content_excerpt:
            return 1
        return 0

    def _amount_match(row_amount: Any) -> bool:
        if row_amount is None:
            return False
        try:
            d = Decimal(str(row_amount))
        except Exception:  # noqa: BLE001
            return False
        return abs(d - target_amount) < cents_threshold

    # Local-index query (Pass 1).
    # ADR-0022: paperless_doc_index.total_amount is TEXT post-migration 057.
    # Pull rows by date window + (amount-not-null OR content-LIKE), then
    # compute amount_score / filter exact-amount in Python via Decimal.
    raw_pass1 = conn.execute(
        """
        SELECT pdi.paperless_id, pdi.vendor, pdi.receipt_date,
               pdi.total_amount, pdi.title, pdi.content_excerpt,
               pdi.created_date, pdi.document_type_id, pdi.document_type_name,
               CASE
                 WHEN pdi.receipt_date IS NOT NULL
                      AND ABS(julianday(pdi.receipt_date) - julianday(?)) <= ?
                 THEN 3
                 WHEN pdi.created_date IS NOT NULL
                      AND ABS(julianday(pdi.created_date) - julianday(?)) <= ?
                 THEN 2
                 ELSE 0
               END AS date_score
          FROM paperless_doc_index pdi
          LEFT JOIN receipt_links rl
                 ON rl.paperless_id = pdi.paperless_id
         WHERE rl.paperless_id IS NULL
           AND (pdi.total_amount IS NOT NULL OR pdi.content_excerpt LIKE ?)
           AND (
                (pdi.receipt_date IS NOT NULL
                 AND ABS(julianday(pdi.receipt_date) - julianday(?)) <= ?)
                OR (pdi.created_date IS NOT NULL
                    AND ABS(julianday(pdi.created_date) - julianday(?)) <= ?)
           )
         LIMIT 200
        """,
        (
            txn_iso, tolerance_days,
            txn_iso, tolerance_days,
            amount_like,
            txn_iso, tolerance_days,
            txn_iso, tolerance_days,
        ),
    ).fetchall()

    from lamella.features.receipts.txn_matcher import _doctype_excluded
    pass1_filtered = []
    for r in raw_pass1:
        a_score = _amount_score(r["total_amount"], r["content_excerpt"])
        if a_score == 0:
            continue
        # Skip non-receipt summaries (bank statements, 1099s, etc.).
        # See RECEIPT_EXCLUDED_DOCTYPE_PATTERNS in txn_matcher.
        try:
            dt_name = r["document_type_name"]
        except (IndexError, KeyError):
            dt_name = None
        if _doctype_excluded(
            conn, document_type_id=r["document_type_id"], document_type_name=dt_name
        ):
            continue
        pass1_filtered.append((a_score, r["date_score"], r))
    # ORDER BY amount_score DESC, date_score DESC, receipt_date
    pass1_filtered.sort(
        key=lambda t: (-t[0], -t[1], t[2]["receipt_date"] or ""),
    )
    candidates = [r for _, _, r in pass1_filtered[:20]]

    # Pass 2 — live-API amount+date fallback.
    if not candidates and live_paperless is not None:
        try:
            from lamella.features.paperless_bridge.sync import PaperlessSync
            lo = (txn.date - timedelta(days=tolerance_days)).isoformat()
            hi = (txn.date + timedelta(days=tolerance_days)).isoformat()
            params = {"created__date__gte": lo, "created__date__lte": hi}
            empty_corr: dict = {}
            empty_doctypes: dict = {}

            async def _fetch():
                out: list = []
                count = 0
                async for doc in live_paperless.iter_documents(params):
                    out.append(doc)
                    count += 1
                    if count >= 50:
                        break
                return out

            fetched_live = loop.run_until_complete(_fetch())
            if fetched_live:
                sync = PaperlessSync(
                    conn=conn, client=live_paperless,
                    lookback_days=settings.paperless_sync_lookback_days,
                )
                for d in fetched_live:
                    sync._upsert_doc(
                        d, empty_corr, empty_doctypes, live_field_mapping,
                    )
                conn.commit()
                # ADR-0022: pull by date+nullability, filter by Decimal in Py.
                raw_pass2 = conn.execute(
                    """
                    SELECT pdi.paperless_id, pdi.vendor,
                           pdi.receipt_date, pdi.total_amount,
                           pdi.title, pdi.content_excerpt,
                           pdi.created_date, pdi.document_type_id, pdi.document_type_name
                      FROM paperless_doc_index pdi
                      LEFT JOIN receipt_links rl
                             ON rl.paperless_id = pdi.paperless_id
                     WHERE rl.paperless_id IS NULL
                       AND (pdi.total_amount IS NOT NULL
                            OR pdi.content_excerpt LIKE ?)
                       AND (
                            (pdi.receipt_date IS NOT NULL
                             AND ABS(julianday(pdi.receipt_date) - julianday(?)) <= ?)
                            OR (pdi.created_date IS NOT NULL
                                AND ABS(julianday(pdi.created_date) - julianday(?)) <= ?)
                       )
                     LIMIT 200
                    """,
                    (
                        amount_like,
                        txn_iso, tolerance_days,
                        txn_iso, tolerance_days,
                    ),
                ).fetchall()
                pass2_filtered = [
                    r for r in raw_pass2
                    if (
                        _amount_match(r["total_amount"])
                        or (r["content_excerpt"] and amount_str in r["content_excerpt"])
                    )
                    and not _doctype_excluded(
                        conn,
                        document_type_id=r["document_type_id"],
                        document_type_name=r["document_type_name"],
                    )
                ]
                pass2_filtered.sort(
                    key=lambda r: (
                        0 if r["total_amount"] is not None else 1,
                        r["receipt_date"] or "",
                    ),
                )
                candidates = pass2_filtered[:20]
        except Exception as exc:  # noqa: BLE001
            log.info("receipt-hunt live-API fallback for %s failed: %s", h[:8], exc)

    # Pass 3 — multi-signal merchant-needle hunt.
    if not candidates and live_paperless is not None:
        needle = _merchant_needle(txn)
        if needle:
            try:
                from datetime import timedelta as _td
                date_lo = (txn.date - _td(days=tolerance_days)).isoformat()
                date_hi = (txn.date + _td(days=tolerance_days)).isoformat()
                scored: dict[int, dict] = {}

                async def _scan_params(params, signal):
                    seen = 0
                    async for doc in live_paperless.iter_documents(params):
                        if doc.id not in scored:
                            scored[doc.id] = {"doc": doc, "signals": set()}
                        scored[doc.id]["signals"].add(signal)
                        seen += 1
                        if seen >= 30:
                            break

                async def _run_all():
                    await _scan_params(
                        {
                            "title__icontains": needle,
                            "created__date__gte": date_lo,
                            "created__date__lte": date_hi,
                        }, "title",
                    )
                    await _scan_params(
                        {
                            "content__icontains": needle,
                            "created__date__gte": date_lo,
                            "created__date__lte": date_hi,
                        }, "content",
                    )
                    await _scan_params(
                        {
                            "content__icontains": amount_str,
                            "created__date__gte": date_lo,
                            "created__date__lte": date_hi,
                        }, "amount_in_content",
                    )
                loop.run_until_complete(_run_all())
                ranked = sorted(
                    scored.values(),
                    key=lambda x: len(x["signals"]),
                    reverse=True,
                )
                if ranked:
                    from lamella.features.paperless_bridge.sync import PaperlessSync
                    empty_corr: dict = {}
                    empty_doctypes: dict = {}
                    sync = PaperlessSync(
                        conn=conn, client=live_paperless,
                        lookback_days=settings.paperless_sync_lookback_days,
                    )
                    for row in ranked[:10]:
                        sync._upsert_doc(
                            row["doc"], empty_corr, empty_doctypes,
                            live_field_mapping,
                        )
                    conn.commit()
                    # ADR-0022: filter exact amount via Decimal in Python.
                    raw_pass3 = conn.execute(
                        """
                        SELECT pdi.paperless_id, pdi.vendor,
                               pdi.receipt_date, pdi.total_amount, pdi.title,
                               pdi.document_type_id, pdi.document_type_name
                          FROM paperless_doc_index pdi
                          LEFT JOIN receipt_links rl
                                 ON rl.paperless_id = pdi.paperless_id
                         WHERE rl.paperless_id IS NULL
                           AND pdi.total_amount IS NOT NULL
                           AND pdi.receipt_date IS NOT NULL
                           AND ABS(julianday(pdi.receipt_date) - julianday(?)) <= ?
                         ORDER BY pdi.receipt_date
                        """,
                        (txn.date.isoformat(), tolerance_days),
                    ).fetchall()
                    candidates = [
                        r for r in raw_pass3
                        if _amount_match(r["total_amount"])
                        and not _doctype_excluded(
                            conn,
                            document_type_id=r["document_type_id"],
                            document_type_name=r["document_type_name"],
                        )
                    ]
                    # Also filter the live-search ranked results by
                    # document_type. The local index was just updated
                    # via sync._upsert_doc above, so the document_type_
                    # name is queryable. Exclude bank statements / tax
                    # forms / etc. before they reach the candidate set.
                    if ranked:
                        ranked_pids = [r["doc"].id for r in ranked]
                        if ranked_pids:
                            placeholders = ",".join("?" * len(ranked_pids))
                            excluded_set = {
                                int(row["paperless_id"])
                                for row in conn.execute(
                                    f"SELECT paperless_id, document_type_id, document_type_name "
                                    f"FROM paperless_doc_index "
                                    f"WHERE paperless_id IN ({placeholders})",
                                    ranked_pids,
                                ).fetchall()
                                if _doctype_excluded(
                                    conn,
                                    document_type_id=row["document_type_id"],
                                    document_type_name=row["document_type_name"],
                                )
                            }
                            if excluded_set:
                                ranked = [
                                    r for r in ranked
                                    if r["doc"].id not in excluded_set
                                ]

                    if not candidates and ranked:
                        # Multi-signal fallback. Earlier behaviour put
                        # ANY doc with a single signal hit (e.g. "the
                        # amount string appears in the content") into
                        # `candidates`, which then triggered the
                        # `len(candidates) == 1 → auto-link with
                        # confidence=1.0` path further down. That auto-
                        # linked huge multi-page tax statements (a
                        # Fidelity 1099 contains the digits "3.55"
                        # somewhere in 30 pages of dividend tables)
                        # against unrelated bank-side charges with
                        # spuriously perfect confidence.
                        #
                        # Two guards:
                        # 1. Require ≥2 signals from the ranked search
                        #    (title contains needle, content contains
                        #    needle, content contains amount). A single
                        #    coincidence isn't strong enough to auto-
                        #    link.
                        # 2. Surface ranked single-signal hits as
                        #    AMBIGUOUS (>1 candidate) so the user
                        #    confirms in the UI; never as a sole
                        #    candidate that gets auto-linked.
                        strong = [
                            r for r in ranked[:5]
                            if len(r.get("signals") or set()) >= 2
                        ]
                        weak = [
                            r for r in ranked[:5]
                            if r not in strong
                        ]
                        candidates = [
                            {
                                "paperless_id": r["doc"].id,
                                "vendor": None,
                                "receipt_date": None,
                                "total_amount": None,
                                "title": r["doc"].title,
                            }
                            for r in strong
                        ]
                        # If only weak hits exist, force ambiguity by
                        # returning ≥2 so the user must pick. Pad with
                        # a placeholder ID-of-self to push len > 1 — no,
                        # cleaner: just emit them all as candidates and
                        # let the >1-is-ambiguous branch downstream
                        # handle them. With only 1 weak hit, treat as
                        # no_match (low signal isn't a confident match).
                        if not strong:
                            if len(weak) >= 2:
                                candidates = [
                                    {
                                        "paperless_id": r["doc"].id,
                                        "vendor": None,
                                        "receipt_date": None,
                                        "total_amount": None,
                                        "title": r["doc"].title,
                                    }
                                    for r in weak
                                ]
                            # len(weak) == 1: leave candidates empty →
                            # no_match. A single coincidence is too
                            # weak for auto-link.
            except Exception as exc:  # noqa: BLE001
                log.info(
                    "receipt-hunt multi-signal fallback for %s failed: %s",
                    h[:8], exc,
                )

    txn_summary = {
        "txn_hash": h,
        "date": txn.date.isoformat(),
        "amount": f"{target_amount:.2f}",
        "payee": getattr(txn, "payee", None) or "",
        "narration": txn.narration or "",
    }

    if len(candidates) == 0:
        report["no_match"].append(txn_summary)
        ctx.emit(f"{h[:8]}… — no matching receipt", outcome="not_found")
        return
    if len(candidates) > 1:
        txn_summary["candidates"] = [
            {
                "paperless_id": int(c["paperless_id"]),
                "vendor": c["vendor"],
                "receipt_date": c["receipt_date"],
                "total_amount": c["total_amount"],
                "title": c["title"],
            }
            for c in candidates
        ]
        report["ambiguous"].append(txn_summary)
        ctx.emit(
            f"{h[:8]}… — {len(candidates)} candidates, awaiting user pick",
            outcome="failure",
        )
        return

    # Exactly one match → auto-link.
    c = candidates[0]
    paperless_id = int(c["paperless_id"])
    try:
        linker.link(
            paperless_id=paperless_id,
            txn_date=txn.date,
            txn_amount=target_amount,
            txn_hash=h,
            match_method="search-hunt-exact",
            match_confidence=1.0,
            paperless_hash=cached_paperless_hash_fn(conn, paperless_id),
            paperless_url=paperless_url_for_fn(
                settings.paperless_url, paperless_id,
            ),
        )
        txn_summary["paperless_id"] = paperless_id
        txn_summary["vendor"] = c["vendor"]
        report["auto_linked"].append(txn_summary)
        ctx.emit(
            f"{h[:8]}… — linked to Paperless #{paperless_id}",
            outcome="success",
        )

        # ADR-0044: write the four canonical Lamella_* fields back
        # to Paperless so the document is searchable by entity /
        # category / txn-id / payment account. Best-effort:
        # writeback failures are logged inside write_match_fields
        # and do NOT undo the link.
        # ADR-0056: skip on staged-side links — staged rows don't yet
        # carry entity/account postings; the standard ledger-side
        # pipeline runs writeback once the row is promoted.
        if live_paperless is not None and not is_staged:
            try:
                from lamella.features.paperless_bridge.writeback import (
                    write_match_fields,
                )
                wrote = loop.run_until_complete(
                    write_match_fields(
                        client=live_paperless,
                        paperless_id=paperless_id,
                        txn=txn,
                        conn=conn,
                    )
                )
                if wrote:
                    ctx.emit(
                        f"{h[:8]}… — wrote {len(wrote)} Lamella_* "
                        f"field(s) to #{paperless_id} "
                        f"({', '.join(sorted(wrote.keys()))})",
                        outcome="info",
                    )
            except Exception as exc:  # noqa: BLE001
                # write_match_fields already swallows expected
                # PaperlessError + InvalidWritebackFieldError; this
                # catches anything truly unexpected so the match
                # success is preserved no matter what.
                log.warning(
                    "ADR-0044 writeback unexpected failure for doc %d: %s",
                    paperless_id, exc,
                )

        needs_population = (
            c["total_amount"] is None
            or c["receipt_date"] is None
            or (
                c["vendor"]
                and getattr(txn, "payee", None)
                and c["vendor"].lower() not in (txn.payee or "").lower()
                and (txn.payee or "").lower() not in c["vendor"].lower()
            )
        )
        if needs_population and verify_service is not None and not is_staged:
            try:
                from lamella.features.paperless_bridge.verify import VerifyHypothesis
                loop.run_until_complete(
                    verify_service.verify_and_correct(
                        paperless_id,
                        hypothesis=VerifyHypothesis(
                            suspected_date=txn.date,
                            suspected_total=target_amount,
                            suspected_vendor=(getattr(txn, "payee", None) or None),
                            reason=(
                                f"Post-link populate: txn on {txn.date} for "
                                f"${target_amount:.2f}. Verify against the "
                                f"image and populate NULL custom fields."
                            ),
                        ),
                    )
                )
                ctx.emit(
                    f"{h[:8]}… — vision verify populated missing fields on "
                    f"#{paperless_id}",
                    outcome="info",
                )
            except Exception as exc:  # noqa: BLE001
                log.info(
                    "post-link verify failed for doc %d: %s",
                    paperless_id, exc,
                )
    except BeanCheckError as exc:
        report["errors"].append({
            "txn_hash": h,
            "reason": f"bean-check blocked: {exc}",
        })
        ctx.emit(
            f"{h[:8]}… — bean-check blocked the link: {exc}",
            outcome="error",
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("receipt-hunt link failed for %s: %s", h[:8], exc)
        report["errors"].append({
            "txn_hash": h,
            "reason": f"link failed: {exc}",
        })
        ctx.emit(
            f"{h[:8]}… — link failed: {exc}", outcome="error",
        )
