# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import yaml

from lamella.features.ai_cascade.classify import ClassifyResponse
from lamella.adapters.openrouter.client import AIError
from lamella.features.ai_cascade.gating import AIProposal, ConfidenceGate, GateAction, RuleProposal
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.config import Settings
from lamella.core.ledger_writer import BeanCheckError
from lamella.core.registry.account_open_guard import (
    check_account_open_on,
    ensure_target_account_open,
)
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.engine import evaluate
from lamella.features.rules.models import TxnFacts
from lamella.features.rules.service import RuleService
from lamella.adapters.simplefin.client import SimpleFINClient, SimpleFINError
from lamella.features.bank_sync.dedup import (
    _meta_simplefin_id,
    build_index,
)
from lamella.features.bank_sync.synthetic_replace import (
    find_loose_synthetic_match,
    find_replaceable_synthetic_match,
    replace_synthetic_in_place,
)
from lamella.adapters.simplefin.schemas import (
    SimpleFINAccount,
    SimpleFINBridgeResponse,
    SimpleFINTransaction,
)
from lamella.features.bank_sync.writer import PendingEntry, SimpleFINWriter
from lamella.features.import_.staging import (
    StagingService,
    TransferWriter,
    sweep as staging_sweep,
)

log = logging.getLogger(__name__)

FIXME_ACCOUNT_DEFAULT = "Expenses:FIXME"

# Sign threshold for routing: anything strictly > 0 is a credit-to-account
# (deposit on bank, payment on credit card) — places it in Income's box;
# anything <= 0 stays in Expenses's box. Exact-zero is ambiguous (e.g. a
# fee waiver) and biases to Expenses since fees are the more common
# zero-or-near-zero case.
_SIGN_INCOME_THRESHOLD = Decimal("0")


@dataclass
class _AIClassifyInline:
    """Carrier for the inline classify result — wraps the proposal
    with the context signals (receipt candidate, mileage, entity)
    so the caller can fire Paperless enrichment without re-running
    the context lookups."""
    proposal: AIProposal
    entity: str | None
    receipt_paperless_id: int | None
    mileage_entries: list
    active_notes: list
    txn_date: date


def _fixme_for_entity(
    entity: str | None,
    amount: Decimal | None = None,
    *,
    source_account: str | None = None,
) -> str:
    """Sign-aware placeholder routing for unclassified bank rows.

    Universal sign convention (verified against actual ledger writes —
    see writer.py:166-172 + render_entry, plus real entries like
    ``Liabilities:CC -9.49`` for charges and ``Liabilities:CC +20.95``
    for refunds): positive amount = money IN to the user (deposit on
    checking, refund/paydown on a credit card) → Income side. Negative
    amount = money OUT (withdrawal/expense on checking, charge on a
    credit card) → Expenses side. Same convention regardless of asset
    vs. liability source — earlier "liability-aware inversion" code
    misread SimpleFIN's sign convention and silently routed every
    credit-card charge to Income:*:FIXME (wrong whitelist) and every
    refund to Expenses:*:FIXME (wrong whitelist).

    Routing the placeholder to the right root is load-bearing for AI
    classification quality: the AI's whitelist is scoped per-root
    (``classify.py``: "FIXME root … Income:FIXME gets Income:*
    suggestions, Expenses:FIXME gets Expenses:*"). If a deposit lands
    on Expenses:Personal:FIXME, the AI literally cannot propose
    Income:* accounts — it picks the closest-shaped Expenses leaf
    (often a fee account), which is what the "$15 Mobile Deposit
    classified as bank fees" bug surfaced.

    ``amount`` defaults to None for back-compat with callers that
    don't carry the signed value yet — those keep the legacy
    Expenses-only routing. ``source_account`` is accepted for API
    symmetry with other helpers; the routing rule no longer branches
    on it.
    """
    root = "Expenses"
    if amount is not None and amount > _SIGN_INCOME_THRESHOLD:
        root = "Income"
    if entity:
        return f"{root}:{entity}:FIXME"
    return f"{root}:FIXME"


@dataclass
class AccountOutcome:
    account_id: str
    account_name: str
    new_txns: int = 0
    duplicate_txns: int = 0
    classified_by_rule: int = 0
    classified_by_ai: int = 0
    fixme_txns: int = 0
    unmapped: bool = False
    error: str | None = None


@dataclass
class LargeFixmeEvent:
    """Recorded by the ingest for notifier hooks — one per FIXME written at
    or above the threshold. Dispatch happens outside the writer lock."""
    txn_id: str
    amount: Decimal
    merchant: str | None
    source_account: str
    posted_date: date


@dataclass
class IngestResult:
    ingest_id: int | None = None
    trigger: str = "manual"
    mode: str = "disabled"
    target_path: Path | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    new_txns: int = 0
    duplicate_txns: int = 0
    classified_by_rule: int = 0
    classified_by_ai: int = 0
    fixme_txns: int = 0
    bean_check_ok: bool = False
    error: str | None = None
    per_account: list[AccountOutcome] = field(default_factory=list)
    bridge_hash: str | None = None
    large_fixmes: list[LargeFixmeEvent] = field(default_factory=list)

    def to_summary_json(self) -> str:
        return json.dumps(
            {
                "mode": self.mode,
                "per_account": [a.__dict__ for a in self.per_account],
            },
            default=str,
        )


def load_account_map(path: Path) -> dict[str, str]:
    """Map SimpleFIN ``account.id`` → Beancount source account. Returns
    empty dict if the file is missing — callers surface that as unmapped
    review items rather than failing hard."""
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        log.warning("simplefin account map at %s is malformed: %s", path, exc)
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    # Tolerate both a flat {id: account} dict and a wrapped {"accounts": {...}} shape.
    if "accounts" in data and isinstance(data["accounts"], dict):
        data = data["accounts"]
    for k, v in data.items():
        if v and isinstance(v, str):
            out[str(k)] = v
    return out


def _entity_from_source(source_account: str, conn=None) -> str | None:
    """Resolve a SimpleFIN source account to its entity slug.

    Prefers the Phase G2 registry lookup (``accounts_meta.entity_slug``)
    when a conn is available; falls back to the second-path-segment
    heuristic otherwise. Keeps the old positional-only signature so
    existing callers that don't have a conn still work.
    """
    if conn is not None:
        from lamella.features.ai_cascade.context import resolve_entity_for_account
        return resolve_entity_for_account(conn, source_account)
    parts = (source_account or "").split(":")
    if len(parts) >= 2:
        return parts[1]
    return None


def _txn_facts(
    txn: SimpleFINTransaction,
    *,
    source_account: str,
) -> TxnFacts:
    merchant = txn.merchant or None
    narration = txn.memo or txn.description or None
    return TxnFacts(
        payee=merchant,
        narration=narration,
        amount=abs(Decimal(txn.amount)),
        card_account=source_account,
    )


def _ingest_insert(conn: sqlite3.Connection, *, trigger: str, bridge_hash: str | None) -> int:
    cursor = conn.execute(
        """
        INSERT INTO simplefin_ingests (trigger, bridge_response_hash)
        VALUES (?, ?)
        """,
        (trigger, bridge_hash),
    )
    return int(cursor.lastrowid)


def _ingest_finalize(conn: sqlite3.Connection, result: IngestResult) -> None:
    if result.ingest_id is None:
        return
    conn.execute(
        """
        UPDATE simplefin_ingests
           SET finished_at     = ?,
               new_txns        = ?,
               duplicate_txns  = ?,
               classified_by_rule = ?,
               classified_by_ai   = ?,
               fixme_txns      = ?,
               bean_check_ok   = ?,
               error           = ?,
               result_summary  = ?
         WHERE id = ?
        """,
        (
            (result.finished_at or datetime.now(timezone.utc)).isoformat(timespec="seconds"),
            result.new_txns,
            result.duplicate_txns,
            result.classified_by_rule,
            result.classified_by_ai,
            result.fixme_txns,
            1 if result.bean_check_ok else 0,
            result.error,
            result.to_summary_json(),
            result.ingest_id,
        ),
    )


class SimpleFINIngest:
    """Orchestrates a full fetch → dedup → classify → write run.

    One instance per run; holds no state between runs. Callers supply the
    client, services, and writer so tests can substitute fakes."""

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        settings: Settings,
        reader: LedgerReader,
        rules: RuleService,
        reviews: ReviewService,
        writer: SimpleFINWriter,
        ai: AIService | None = None,
        gate: ConfidenceGate | None = None,
        account_map: dict[str, str] | None = None,
        paperless_client_factory=None,
    ):
        self.conn = conn
        self.settings = settings
        self.reader = reader
        self.rules = rules
        self.reviews = reviews
        self.writer = writer
        self.ai = ai
        self.gate = gate or ConfidenceGate()
        self._account_map_override = account_map
        self.paperless_client_factory = paperless_client_factory
        # NEXTGEN Phase C2a/C2b: per-run deferred queues.
        # _pending_writes: each account's classified entries waiting to
        #   be appended to simplefin_transactions.bean. The writer runs
        #   AFTER the matcher sweep + transfer writer so paired rows
        #   don't double-write (paired → connector_transfers.bean,
        #   un-paired → simplefin_transactions.bean).
        # _pending_promotions: staged_ids → (target_file, txn_hash)
        #   produced by the per-account writer, drained after all
        #   promotions settle.
        self._pending_writes: list[
            tuple["AccountOutcome", "SimpleFINAccount", str, list[PendingEntry], str]
        ] = []
        self._pending_promotions: list[tuple[int, str | None, str]] = []
        # ADR-0043 P2 — when settings.enable_staged_txn_directives is
        # True, deferred (un-classified) rows accumulate here as
        # PendingEntry shapes; drained at the end of the run via
        # writer.append_staged_txn_directives so each staged row gets
        # a `custom "staged-txn"` directive in the connector-owned
        # .bean file. Cache-vs-truth: the directive is source-of-truth,
        # the staged_transactions row is reconstructable cache.
        self._pending_staged_directives: list[PendingEntry] = []
        # WP6 Site 2 — transactions the loan module claimed (principle 3)
        # before the AI classifier could run. Filled during _classify
        # when `claim_from_simplefin_facts` returns a Claim; drained at
        # the end of the _pending_writes flush by
        # `_auto_classify_claimed_ingest_entries` (WP6 Tier 2, next commit).
        # Each entry: (staged_id, sf_txn, source_account, claim).
        self._claimed_ingest_entries: list[tuple[int | None, Any, str, Any]] = []

    def _load_account_map(self) -> dict[str, str]:
        """Source the SimpleFIN ID → account-path mapping from the DB
        (accounts_meta), which is now the canonical home. Falls back to
        the YAML file when the DB has no mapping (first-boot scenarios
        before the discovery seed runs). Admin edits on /settings/accounts
        propagate here automatically."""
        if self._account_map_override is not None:
            return self._account_map_override
        try:
            rows = self.conn.execute(
                "SELECT simplefin_account_id, account_path FROM accounts_meta "
                "WHERE simplefin_account_id IS NOT NULL AND simplefin_account_id <> ''"
            ).fetchall()
            db_map = {
                str(r["simplefin_account_id"]): r["account_path"]
                for r in rows
                if r["account_path"]
            }
        except Exception as exc:  # noqa: BLE001
            log.warning("simplefin account map from DB failed: %s", exc)
            db_map = {}
        if db_map:
            return db_map
        # DB is empty on this axis — fall back to the YAML one-time seed.
        return load_account_map(self.settings.simplefin_account_map_resolved)

    def _run_fixme_scan_post_ingest(self) -> None:
        """Workstream D2 — after an ingest write-commit, sweep the
        ledger once for FIXMEs that match a user-rule. Mostly a
        no-op in steady state (ingest rarely writes FIXMEs post-C1),
        but picks up any pre-existing unresolved FIXMEs that a new
        rule created this run could now auto-apply. Failures never
        propagate — the ingest write already landed."""
        from lamella.features.review_queue.service import ReviewService
        from lamella.features.rules.overrides import OverrideWriter
        from lamella.features.rules.scanner import FixmeScanner
        try:
            self.reader.invalidate()
            scanner = FixmeScanner(
                reader=self.reader,
                reviews=ReviewService(self.conn),
                rules=self.rules,
                override_writer=OverrideWriter(
                    main_bean=self.settings.ledger_main,
                    overrides=self.settings.connector_overrides_path,
                    conn=self.conn,
                ),
            )
            scanner.scan()
        except Exception as exc:  # noqa: BLE001
            log.warning("simplefin: post-ingest FIXME scan failed: %s", exc)

    def _auto_classify_claimed_ingest_entries(
        self, *, target_path: Path | None = None,
    ) -> None:
        """WP6 Site 2 Tier 2 — drain ``self._claimed_ingest_entries``
        and write the auto-classified splits.

        Invoked from ``run()`` after the regular pending_writes flush.
        For each (staged_id, sf_txn, source_account, claim) tuple:

        - Load the matching loan.
        - Call ``auto_classify.plan_from_facts`` with the SimpleFIN
          facts. Tier exact/over → ``apply_ingest_split`` writes a
          multi-leg transaction to simplefin_transactions.bean and
          the staged row is marked promoted. Tier under/far → leave
          staging unresolved for the user to handle via
          ``/review/staged``.

        Best-effort: write failures on one claim don't abort the
        others. Each log line carries the claim slug so a failure
        triage is straightforward.
        """
        if not self._claimed_ingest_entries:
            return

        from lamella.features.loans import auto_classify
        from lamella.features.loans.auto_classify import plan_from_facts
        from lamella.features.loans.claim import ClaimKind
        from lamella.features.import_.staging import StagingService

        staging = StagingService(self.conn)

        for staged_id, sf_txn, source_account, claim in self._claimed_ingest_entries:
            if claim.kind != ClaimKind.PAYMENT:
                # ESCROW_DISBURSEMENT / DRAW / REVOLVING_SKIP: we
                # claimed the AI preemption but don't auto-classify.
                # Row stays in staging; user handles via /review/staged.
                continue

            try:
                loan_row = self.conn.execute(
                    "SELECT * FROM loans WHERE slug = ?", (claim.loan_slug,),
                ).fetchone()
                if loan_row is None:
                    continue
                loan = dict(loan_row)
                # Respect the per-loan master switch: claim
                # preemption fired (AI was skipped) but if the user
                # opted out of auto-classify they get a staging row
                # they classify manually.
                if not loan.get("auto_classify_enabled", 1):
                    continue

                plan = plan_from_facts(
                    actual_total=abs(Decimal(str(sf_txn.amount))),
                    txn_date=sf_txn.posted_date,
                    loan=loan,
                    source_account=source_account,
                    narration=sf_txn.description or sf_txn.memo,
                    payee=sf_txn.payee,
                )

                if plan.tier not in ("exact", "over"):
                    # Under / far / skipped — leave in staging for
                    # user handling. No write, no promotion.
                    log.info(
                        "loans.tier-skip slug=%s simplefin_id=%s tier=%s reason=%s",
                        claim.loan_slug, sf_txn.id, plan.tier,
                        plan.skip_reason or "",
                    )
                    continue

                # Carry the staged row's immutable identity onto the
                # written entry so /txn/{token} keeps resolving to the
                # same URL post-promotion.
                staged_lid: str | None = None
                if staged_id is not None:
                    try:
                        staged_lid = staging.get(staged_id).lamella_txn_id
                    except Exception:  # noqa: BLE001
                        staged_lid = None
                auto_classify.apply_ingest_split(
                    plan, sf_txn, source_account, loan,
                    writer=self.writer, conn=self.conn,
                    target_path=target_path,
                    lamella_txn_id=staged_lid,
                )

                # Mark staging promoted so the next ingest doesn't
                # re-process it and so /review/staged doesn't show
                # it anymore.
                if staged_id is not None:
                    try:
                        staging.mark_promoted(
                            staged_id,
                            promoted_to_file=(
                                str(target_path) if target_path else None
                            ),
                            promoted_txn_hash=f"simplefin:{sf_txn.id}",
                        )
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "simplefin: mark_promoted(%s) after loan "
                            "auto-classify failed: %s",
                            staged_id, exc,
                        )

                log.info(
                    "loans.auto-classified slug=%s simplefin_id=%s tier=%s "
                    "decision_id=%s",
                    claim.loan_slug, sf_txn.id, plan.tier, plan.decision_id,
                )
            except Exception as exc:  # noqa: BLE001
                # Never let one failing auto-classify abort the
                # remaining entries. bean-check failures are logged
                # and the row falls back to staging-unresolved.
                log.warning(
                    "loans auto-classify for simplefin_id=%s slug=%s "
                    "failed: %s",
                    getattr(sf_txn, "id", "?"), claim.loan_slug, exc,
                )

        self._claimed_ingest_entries = []

    async def run(
        self,
        *,
        client: SimpleFINClient | None,
        trigger: str = "manual",
        include_pending: bool = False,
        lookback_days_override: int | None = None,
    ) -> IngestResult:
        mode = (self.settings.simplefin_mode or "disabled").strip().lower()
        if mode not in {"shadow", "active"}:
            raise SimpleFINError(f"simplefin mode is {mode!r} — fetch disabled")

        result = IngestResult(trigger=trigger, mode=mode)
        result.ingest_id = _ingest_insert(self.conn, trigger=trigger, bridge_hash=None)
        if mode == "shadow":
            result.target_path = self.settings.simplefin_preview_path
        else:
            result.target_path = self.settings.simplefin_transactions_path

        # Reset the per-run queues. _process_account fills _pending_writes
        # with each account's classified entries; _process drains after
        # sweep + transfer writer complete.
        self._pending_writes = []
        self._pending_promotions = []
        self._pending_staged_directives = []
        self._claimed_ingest_entries = []

        try:
            response = await self._fetch(
                client,
                include_pending=include_pending,
                lookback_days_override=lookback_days_override,
            )
            raw_hash = hashlib.sha256(
                response.model_dump_json(exclude_none=True).encode("utf-8")
            ).hexdigest()
            result.bridge_hash = raw_hash
            self.conn.execute(
                "UPDATE simplefin_ingests SET bridge_response_hash = ? WHERE id = ?",
                (raw_hash, result.ingest_id),
            )

            await self._process(response, result)

            # NEXTGEN Phase C2a: run the matcher now — every written
            # row is still in status='classified' because _process_account
            # deferred promotion. The sweep sees this fetch's rows as
            # valid candidates for cross-source pairing against any
            # pending CSV / paste / reboot rows. Pair records advance
            # both sides to 'matched'; the drain below then promotes.
            try:
                sweep_stats = staging_sweep(self.conn)
                if sweep_stats["applied"]:
                    log.info(
                        "simplefin: matcher paired %d staged rows "
                        "(%d candidates found)",
                        sweep_stats["applied"], sweep_stats["found"],
                    )
            except Exception:  # noqa: BLE001
                log.exception("simplefin: matcher sweep failed")

            # NEXTGEN Phase C2b: emit paired transfers as balanced
            # single transactions to connector_transfers.bean BEFORE
            # the per-account writer runs. The transfer writer marks
            # both sides as promoted on success; the per-account writer
            # below then filters them out of its batches.
            try:
                n_transfers = TransferWriter(
                    conn=self.conn,
                    main_bean=self.settings.ledger_main,
                    transfers_path=self.settings.connector_transfers_path,
                ).emit_pending_pairs()
                if n_transfers:
                    log.info(
                        "simplefin: wrote %d balanced transfer txn(s) to %s",
                        n_transfers,
                        self.settings.connector_transfers_path.name,
                    )
            except BeanCheckError as exc:
                log.warning(
                    "simplefin: transfer writer bean-check failed: %s — "
                    "per-source writer will emit the individual legs instead",
                    exc,
                )
            except Exception:  # noqa: BLE001
                log.exception("simplefin: transfer writer failed")

            # Drain the per-account write queue. Rows already promoted
            # (i.e., handled by the transfer writer) are filtered out
            # so they don't produce a duplicate one-sided entry in
            # simplefin_transactions.bean.
            staging = StagingService(self.conn)
            for outcome, account, source_account, entries, commit_message in self._pending_writes:
                remaining: list[PendingEntry] = []
                for e in entries:
                    if e.staged_id is None:
                        remaining.append(e)
                        continue
                    try:
                        current = staging.get(e.staged_id)
                    except Exception:  # noqa: BLE001
                        remaining.append(e)
                        continue
                    if current.status == "promoted":
                        # Transfer writer already emitted this row as
                        # part of a balanced pair — skip the one-sided
                        # write.
                        continue
                    remaining.append(e)
                if not remaining:
                    continue
                try:
                    written = self.writer.append_entries(
                        remaining,
                        target_path=result.target_path,
                        commit_message=commit_message,
                    )
                except BeanCheckError:
                    for e in remaining:
                        if e.staged_id is None:
                            continue
                        try:
                            staging.mark_failed(
                                e.staged_id,
                                reason="bean-check failed on write",
                            )
                        except Exception as exc:  # noqa: BLE001
                            log.warning(
                                "simplefin: mark_failed(%d) failed: %s",
                                e.staged_id, exc,
                            )
                    raise
                outcome.new_txns += written
                result.new_txns += written
                target = str(result.target_path) if result.target_path else None
                for e in remaining:
                    if e.staged_id is None:
                        continue
                    try:
                        staging.mark_promoted(
                            e.staged_id,
                            promoted_to_file=target,
                            promoted_txn_hash=e.simplefin_id,
                        )
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "simplefin: mark_promoted(%d) failed: %s",
                            e.staged_id, exc,
                        )
            self._pending_writes = []
            self._pending_promotions = []

            # ADR-0043 P2 — drain the queued PendingEntry shapes for
            # un-classified rows into `custom "staged-txn"` directives.
            # Flag-gated; only fires when the user opts into staged-txn
            # directives. Failures here log and continue: the
            # staged_transactions DB row is already written and is the
            # cache layer; the directive being missing means the user
            # can still classify but the directive will land on the
            # next ingest cycle once the underlying issue is resolved.
            if self._pending_staged_directives:
                try:
                    self.writer.append_staged_txn_directives(
                        self._pending_staged_directives,
                        source="simplefin",
                        target_path=result.target_path,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "simplefin: staged-txn directive write failed (%d "
                        "directives queued): %s — DB row still recorded; "
                        "directive will retry on next ingest",
                        len(self._pending_staged_directives), exc,
                    )
                self._pending_staged_directives = []

            # WP6 Site 2 Tier 2 — auto-classify loan-claimed entries.
            # Runs inline at the ingest commit boundary because loan
            # payments are information-complete at ingest: the
            # configured amortization + escrow are the full context;
            # no receipt / note / mileage that lands later changes
            # the answer. This is the exception to the general
            # classifier's deliberate delay-for-context design.
            self._auto_classify_claimed_ingest_entries(
                target_path=result.target_path,
            )

            # Workstream D2 — event-driven FixmeScanner trigger. The
            # wall-clock 30-min schedule is retired; ingest fires the
            # scan on successful write because that's when new ledger
            # state actually lands. Any FIXMEs now in the file that
            # match an existing user-rule get auto-resolved in one pass.
            self._run_fixme_scan_post_ingest()

            result.bean_check_ok = True
        except BeanCheckError as exc:
            result.error = f"bean-check: {exc}"
            log.error("simplefin ingest failed bean-check: %s", exc)
        except SimpleFINError as exc:
            result.error = str(exc)
            log.error("simplefin ingest failed: %s", exc)
        except Exception as exc:  # noqa: BLE001
            result.error = f"{type(exc).__name__}: {exc}"
            log.exception("simplefin ingest crashed")
        finally:
            result.finished_at = datetime.now(timezone.utc)
            _ingest_finalize(self.conn, result)

        if result.error is None:
            self.reader.invalidate()
        return result

    async def _fetch(
        self,
        client: SimpleFINClient | None,
        *,
        include_pending: bool,
        lookback_days_override: int | None = None,
    ) -> SimpleFINBridgeResponse:
        if client is None:
            raise SimpleFINError("no SimpleFIN client configured")
        lookback = (
            lookback_days_override
            if lookback_days_override is not None
            else self.settings.simplefin_lookback_days
        )
        return await client.fetch_accounts(
            lookback_days=lookback,
            include_pending=include_pending,
        )

    async def _process(
        self,
        response: SimpleFINBridgeResponse,
        result: IngestResult,
    ) -> None:
        account_map = self._load_account_map()
        self.reader.load()
        seen_ids = build_index(self.reader.load().entries)
        # Content-fingerprint index: maps (date, account, |amount|,
        # normalized narration) → the primary SimpleFIN id that
        # already exists on that event in the ledger. When SimpleFIN
        # re-delivers the same bank event with a fresh id (which it
        # does routinely until the event slides out of the
        # 30/60/90-day window), we match on content here and append
        # the new id as an alias instead of writing a fresh duplicate.
        self._content_index = _build_content_index(
            self.reader.load().entries
        )

        for account in response.accounts:
            outcome = AccountOutcome(
                account_id=account.id,
                account_name=account.name or account.id,
            )
            result.per_account.append(outcome)
            source_account = account_map.get(account.id)
            if not source_account:
                outcome.unmapped = True
                self._enqueue_unmapped_account(account)
                continue

            # Stamp a balance-anchor using today's SimpleFIN-reported
            # balance so the /reports/balance-audit page can compute
            # drift over time. Idempotent on (account_path, today).
            self._stamp_simplefin_anchor(source_account, account)

            try:
                await self._process_account(
                    account=account,
                    source_account=source_account,
                    seen_ids=seen_ids,
                    outcome=outcome,
                    result=result,
                )
            except BeanCheckError:
                # Writer already rolled back its write — bubble up so the
                # whole ingest records bean_check_ok=False and stops
                # touching the ledger.
                raise
            except Exception as exc:  # noqa: BLE001
                outcome.error = f"{type(exc).__name__}: {exc}"
                log.exception("simplefin: account %s failed", account.id)

    def _stamp_simplefin_anchor(
        self, account_path: str, account: "SimpleFINAccount",
    ) -> None:
        """Record SimpleFIN's current balance as a balance-anchor for
        this account. Runs once per account per ingest; the UNIQUE
        constraint on (account_path, as_of_date) keeps re-ingests on
        the same day from piling up anchors.
        """
        if account.balance is None:
            return
        from datetime import date as _date_t
        from lamella.features.dashboard.balances import service as _balance_service
        from lamella.features.dashboard.balances.writer import append_balance_anchor
        today = _date_t.today().isoformat()
        try:
            _balance_service.upsert_anchor(
                self.conn,
                account_path=account_path,
                as_of_date=today,
                balance=str(account.balance),
                currency=account.currency or "USD",
                source="simplefin",
                notes=f"auto: simplefin pull for {account.name or account.id}",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "simplefin: failed to stamp balance-anchor for %s: %s",
                account_path, exc,
            )
            return
        try:
            append_balance_anchor(
                connector_config=self.settings.connector_config_path,
                main_bean=self.settings.ledger_main,
                account_path=account_path,
                as_of_date=today,
                balance=str(account.balance),
                currency=account.currency or "USD",
                source="simplefin",
                notes=f"auto: simplefin pull for {account.name or account.id}",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "simplefin: balance-anchor directive write failed for %s: %s",
                account_path, exc,
            )

    def _enqueue_unmapped_account(self, account: SimpleFINAccount) -> None:
        payload = json.dumps(
            {
                "account_id": account.id,
                "account_name": account.name,
                "org": (account.org or {}).get("name") if account.org else None,
            }
        )
        source_ref = f"simplefin_account:{account.id}"
        # Dedup: don't stack the same unmapped-account item repeatedly.
        existing = self.reviews.conn.execute(
            "SELECT 1 FROM review_queue WHERE source_ref = ? AND resolved_at IS NULL LIMIT 1",
            (source_ref,),
        ).fetchone()
        if existing:
            return
        self.reviews.enqueue(
            kind="simplefin_unmapped_account",
            source_ref=source_ref,
            priority=1000,  # bubbles above normal FIXMEs
            ai_suggestion=payload,
        )

    async def _process_account(
        self,
        *,
        account: SimpleFINAccount,
        source_account: str,
        seen_ids: set[str],
        outcome: AccountOutcome,
        result: IngestResult,
    ) -> None:
        entries: list[PendingEntry] = []
        # Resolve currency: prefer the account's declared currency, default USD.
        currency = (account.currency or "USD").upper() or "USD"
        staging = StagingService(self.conn)

        for txn in account.transactions:
            if not txn.id:
                continue
            if txn.id in seen_ids:
                outcome.duplicate_txns += 1
                result.duplicate_txns += 1
                continue

            # ADR-0046 Phase 2: synthetic-replacement check. Before
            # staging a fresh row, look for an existing synthetic
            # counterpart leg in the loaded ledger that matches this
            # row's (account, signed amount, posted_date within 5d).
            # On hit, swap the synthetic-* posting meta for real
            # source meta in place and skip staging — the real other
            # half is now wired into the existing transaction without
            # creating a duplicate.
            #
            # ADR-0046 Phase 3b: ``loose`` is reset per-iteration so a
            # loose-match captured on a prior loop iteration cannot
            # bleed into this row's staging marker write.
            loose: dict | None = None
            try:
                txn_amount = Decimal(txn.amount)
            except (TypeError, ValueError, InvalidOperation):
                txn_amount = None  # type: ignore[assignment]
            if txn_amount is not None:
                synth_match = find_replaceable_synthetic_match(
                    self.reader.load().entries,
                    account=source_account,
                    amount=txn_amount,
                    posted_date=txn.posted_date,
                )
                if synth_match is not None:
                    rewrote = False
                    try:
                        rewrote = replace_synthetic_in_place(
                            bean_file=(
                                self.settings.ledger_main.parent
                                / "simplefin_transactions.bean"
                            ),
                            lamella_txn_id=synth_match["lamella_txn_id"],
                            posting_account=synth_match["posting_account"],
                            source="simplefin",
                            source_reference_id=txn.id,
                        )
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "simplefin: synthetic-replace rewrite "
                            "failed for txn %s on %s: %s — falling "
                            "through to normal stage path",
                            txn.id, source_account, exc,
                        )
                    if rewrote:
                        seen_ids.add(txn.id)
                        outcome.duplicate_txns += 1
                        result.duplicate_txns += 1
                        log.info(
                            "simplefin: synthetic-replace promoted "
                            "txn-id=%s posting=%s; new ref=%s",
                            synth_match["lamella_txn_id"],
                            synth_match["posting_account"],
                            txn.id,
                        )
                        # Invalidate the reader so a subsequent
                        # synthetic-match scan reflects the rewrite.
                        try:
                            self.reader.invalidate()
                        except Exception:  # noqa: BLE001
                            pass
                        continue
                # ADR-0046 Phase 3 — loose match: same date+amount
                # within window but DIFFERENT account from this row's
                # source. Most common cause: the user picked the wrong
                # destination account on the original transfer-suspect
                # classify, and the real other-half is now arriving on
                # the actual account. We don't auto-rewrite (the
                # symmetric case "two genuinely separate transactions
                # happen to share a date+amount" is real); instead, we
                # capture the conflict in synthetic_match_meta on the
                # staged row so /review/staged can render a one-click
                # confirm prompt (Phase 3b).
                loose = find_loose_synthetic_match(
                    self.reader.load().entries,
                    amount=txn_amount,
                    posted_date=txn.posted_date,
                    exclude_account=source_account,
                )
                if loose is not None:
                    log.info(
                        "simplefin: synthetic-conflict candidate — "
                        "incoming row on %s amount=%s date=%s loosely "
                        "matches synthetic leg on %s for txn-id=%s "
                        "(staging with marker for /review confirm)",
                        source_account, txn_amount, txn.posted_date,
                        loose["synthetic_account"],
                        loose["lamella_txn_id"],
                    )

            # Content-fingerprint dedup: same bank event, fresh
            # SimpleFIN id. Stamp the new id as an alias on the
            # existing ledger entry and move on — no duplicate lands.
            content_key = _content_key_from_simplefin(
                txn=txn,
                source_account=source_account,
                currency=currency,
            )
            existing_sfid = self._content_index.get(content_key) if content_key else None
            if existing_sfid and existing_sfid != txn.id:
                try:
                    _stamp_alias_on_ledger(
                        main_bean=self.settings.ledger_main,
                        simplefin_transactions=(
                            self.settings.ledger_main.parent
                            / "simplefin_transactions.bean"
                        ),
                        primary_sfid=existing_sfid,
                        alias=txn.id,
                    )
                    seen_ids.add(txn.id)
                    outcome.duplicate_txns += 1
                    result.duplicate_txns += 1
                    log.info(
                        "simplefin: content-matched %s to existing %s; aliased",
                        txn.id, existing_sfid,
                    )
                    continue
                except Exception as exc:  # noqa: BLE001
                    # Alias stamp failed — fall through to the normal
                    # write path. The user can clean up via the
                    # /settings/data-integrity/duplicates page if this
                    # produces a visible duplicate.
                    log.warning(
                        "simplefin: alias-stamp failed for %s->%s: %s",
                        txn.id, existing_sfid, exc,
                    )

            # NEXTGEN Phase B: stage the raw row before classification
            # so every fetched txn lands on the unified surface the
            # matcher and review pipeline consume. Staging is
            # dedup-by-upsert, so a re-fetch of the same window is
            # idempotent.
            #
            # ADR-0058 — opt into the cross-source dedup oracle. A
            # row whose (date, signed amount, description) matches an
            # existing staged row OR a ledger posting from any other
            # source lands as ``status='likely_duplicate'`` and
            # inherits the matched event's lamella-txn-id, so all
            # source observations of one real-world event share one
            # identity. Re-fetches preserve the user's earlier
            # confirm/release decision (the upsert never overwrites
            # status). This same opt-in applies to any future
            # bank-feed adapter that reaches this code path through
            # the shared ``BankDataPort`` interface.
            staged_row = staging.stage(
                source="simplefin",
                source_ref={
                    "account_id": account.id,
                    "txn_id": txn.id,
                },
                session_id=(
                    str(result.ingest_id) if result.ingest_id is not None else None
                ),
                posting_date=txn.posted_date.isoformat(),
                amount=Decimal(txn.amount),
                currency=currency,
                payee=(txn.merchant or None),
                description=(txn.description or None),
                memo=(txn.memo or None),
                raw=txn.model_dump(mode="json"),
                dedup_check=True,
                ledger_reader=self.reader,
            )
            # ADR-0046 Phase 3b: stamp the loose-match marker on the
            # freshly staged row so /review/staged can render the
            # confirm prompt. Set unconditionally on every fetch (the
            # marker may have changed since the last attempt — e.g.
            # the user classified a different transfer-suspect in the
            # interim that creates a new wrong-account candidate).
            if loose is not None:
                try:
                    self.conn.execute(
                        "UPDATE staged_transactions "
                        "SET synthetic_match_meta = ? WHERE id = ?",
                        (
                            json.dumps({
                                "lamella_txn_id": loose["lamella_txn_id"],
                                "wrong_account": loose["synthetic_account"],
                                "right_account": source_account,
                            }),
                            staged_row.id,
                        ),
                    )
                    self.conn.commit()
                except sqlite3.Error as exc:
                    log.warning(
                        "simplefin: synthetic_match_meta write failed "
                        "for staged_id=%s: %s", staged_row.id, exc,
                    )
            # Fresh-insert detection. ``StagingService.stage`` is
            # insert-or-update keyed on (source, source_ref_hash); on
            # insert it sets created_at == updated_at == now (single
            # _now() call), on update it bumps only updated_at. So
            # equality of the two timestamps is a reliable signal that
            # this row was created by the current ingest run. Used
            # below to gate the fixme_txns counter so re-emits of
            # already-staged-uncategorized rows don't keep inflating
            # "FIXME created" on every fetch.
            staged_inserted = staged_row.created_at == staged_row.updated_at
            # If the same staged row is already promoted (ledger-wipe
            # then re-fetch scenario where seen_ids missed it), do not
            # double-write it.
            if staged_row.status == "promoted":
                outcome.duplicate_txns += 1
                result.duplicate_txns += 1
                continue

            # Failed rows from a prior run get a fresh retry. Reset
            # status so classify treats this as a normal attempt:
            # success flips it to 'classified'/'promoted', a fresh
            # failure flips it back to 'failed'. Without this, the
            # row stays stuck at 'failed' (invisible in /review/staged
            # which filters to {new, classified, matched}) AND the
            # pipeline keeps running classify on every ingest with
            # no terminal outcome.
            if staged_row.status == "failed":
                self.conn.execute(
                    "UPDATE staged_transactions SET status = 'new', "
                    "updated_at = ? WHERE id = ?",
                    (
                        datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        staged_row.id,
                    ),
                )

            pending = await self._classify(
                txn=txn,
                source_account=source_account,
                currency=currency,
                outcome=outcome,
                result=result,
                staged_id=staged_row.id,
                staged_lamella_txn_id=staged_row.lamella_txn_id,
                staged_inserted=staged_inserted,
            )
            if pending is not None:
                entries.append(pending)
                seen_ids.add(txn.id)

        if not entries:
            return

        dates = sorted(e.date for e in entries)
        commit_message = (
            f"simplefin: {len(entries)} txn(s) {dates[0].isoformat()}..{dates[-1].isoformat()} "
            f"[{account.id}]"
        )
        # NEXTGEN Phase C2b: queue the write instead of executing it.
        # _process drains this queue after the matcher sweep + transfer
        # writer have run so paired rows route to connector_transfers.bean
        # and only un-paired rows land in simplefin_transactions.bean.
        self._pending_writes.append(
            (outcome, account, source_account, list(entries), commit_message)
        )

    async def _classify(
        self,
        *,
        txn: SimpleFINTransaction,
        source_account: str,
        currency: str,
        outcome: AccountOutcome,
        result: IngestResult,
        staged_id: int | None = None,
        staged_lamella_txn_id: str | None = None,
        staged_inserted: bool = True,
    ) -> PendingEntry | None:
        facts = _txn_facts(txn, source_account=source_account)
        match = evaluate(facts, list(self.rules.iter_active()))
        entity = _entity_from_source(source_account, conn=self.conn)
        # Sign-aware + liability-aware: deposits route to
        # Income:{entity}:FIXME so the AI sees the Income whitelist;
        # withdrawals route to Expenses:{entity}:FIXME. For a credit-
        # card source, the convention inverts (positive amount on a
        # Liabilities:* source = card was charged = Expense). Without
        # the source_account hint, every credit-card charge gets
        # routed to Income:*:FIXME; that placeholder then feeds
        # build_classify_context's whitelist-by-root selector which
        # also reads the FIXME root, so the wrong placeholder cascades
        # through the entire AI pipeline and lands as "No confident
        # proposal" downstream.
        fixme_account = _fixme_for_entity(
            entity, Decimal(txn.amount), source_account=source_account,
        )

        rule_proposal: RuleProposal | None = None
        if match is not None:
            rule_proposal = RuleProposal(
                rule_id=match.rule.id,
                target_account=match.target_account,
                confidence=float(match.rule.confidence),
                created_by=match.rule.created_by,
            )
        rule_guard_demoted_reason: str | None = None

        # WP6 Site 2 Tier 1 — principle-3 preemption for loan-claimed
        # transactions. The general AI classifier is intentionally
        # delayed-for-context (SimpleFIN runs daily; receipts, notes,
        # mileage accumulate afterward, and classification gets better
        # with them). Loans are the exception: a mortgage payment's
        # correct split is fully determined by the loan's configured
        # amortization + escrow at ingest time — no receipt or note
        # that lands later changes the answer. Preempting AI for these
        # isn't faster-is-better, it's matching cadence to the fact
        # that this specific class is information-complete at ingest.
        from lamella.features.loans.claim import claim_from_simplefin_facts
        claim = claim_from_simplefin_facts(txn, source_account, self.conn)
        if claim is not None:
            log.info(
                "loans.preempted simplefin_id=%s slug=%s kind=%s",
                txn.id, claim.loan_slug, claim.kind.value,
            )
            # Track for the post-commit auto-classify pass. The staged
            # row lands in DEFER-FIXME (neither rule nor AI produced a
            # confident proposal) and stays unresolved until Tier 2
            # promotes it.
            self._claimed_ingest_entries.append(
                (staged_id, txn, source_account, claim)
            )
            # Skip AI and fall through to defer-fixme. rule_proposal
            # stays as-is — if the user has an explicit rule for this
            # txn, that's still respected; loan-claim preemption only
            # targets the AI classify path per the call-site table.

        # Post-workstream-C1: no ingest-time AI classify. SimpleFIN
        # rows are not information-complete at fetch (receipts, notes,
        # mileage arrive later; see docs/specs/AI-CLASSIFICATION.md exception
        # criterion). Rows that don't match a user-rule or a loan
        # claim stage for user touch in /review/staged — the user's
        # 'Ask AI' button is the on-demand tier-3 path when they want
        # an AI suggestion on a specific row.
        ai_proposal = None
        decision = self.gate.decide(rule=rule_proposal, ai=ai_proposal)

        # ADR-0059 — the source's verbatim description for this leg.
        # We pick the richest available text the source provided:
        # description > memo > merchant. The transaction's top-level
        # narration is a separate field (currently the same text
        # today; eventually a synthesized line). Keeping
        # source_description as its own field preserves provenance
        # when the canonical narration gets re-synthesized from
        # multiple sources observing the same event.
        source_description = (
            txn.description or txn.memo or txn.merchant or None
        )
        base = PendingEntry(
            date=txn.posted_date,
            simplefin_id=txn.id,
            payee=(txn.merchant or None),
            narration=(txn.memo or txn.description or None),
            amount=Decimal(txn.amount),
            currency=currency,
            source_account=source_account,
            target_account=fixme_account,
            staged_id=staged_id,
            lamella_txn_id=staged_lamella_txn_id,
            source_description=source_description,
        )

        if decision.action == GateAction.AUTO_APPLY_RULE and rule_proposal is not None:
            # Account-active guard. The rule's target_account may be
            # missing an Open directive entirely, or have one dated
            # after this txn (common when widening the lookback
            # window so older rows surface). Either case crashes
            # bean-check and rolls back the entire batch — costly
            # because every other classified row in the batch is
            # discarded too. Try to auto-scaffold or backdate; on
            # failure, demote to DEFER-FIXME so the rest of the
            # batch still lands and the user resolves this row in
            # /review/staged.
            guard_err = ensure_target_account_open(
                self.reader, self.settings,
                rule_proposal.target_account, txn.posted_date,
            )
            if guard_err is None:
                guard_err = check_account_open_on(
                    self.reader, rule_proposal.target_account,
                    txn.posted_date, settings=self.settings,
                )
            if guard_err is not None:
                log.warning(
                    "simplefin: rule #%s target %r not safely "
                    "writable on %s — demoting to FIXME (%s)",
                    rule_proposal.rule_id,
                    rule_proposal.target_account,
                    txn.posted_date, guard_err,
                )
                # Fall through to the DEFER-FIXME path below. The
                # rule_proposal stays attached so the staged row
                # records the rule as a suggestion instead of
                # auto-applying. The reason rides through to the
                # rationale so the user sees why on /review/staged.
                rule_guard_demoted_reason = guard_err
            else:
                self.rules.bump(rule_proposal.rule_id)
                outcome.classified_by_rule += 1
                result.classified_by_rule += 1
                self.reviews.enqueue_resolved(
                    kind="fixme",
                    source_ref=f"fixme:{txn.id}",
                    priority=0,
                    user_decision=f"auto_accepted→{rule_proposal.target_account}",
                    ai_suggestion=json.dumps(
                        {
                            "rule": {
                                "rule_id": rule_proposal.rule_id,
                                "target_account": rule_proposal.target_account,
                                "confidence": rule_proposal.confidence,
                                "created_by": rule_proposal.created_by,
                            }
                        }
                    ),
                )
                self._record_staged_decision(
                    staged_id=staged_id,
                    account=rule_proposal.target_account,
                    confidence="high",
                    confidence_score=rule_proposal.confidence,
                    decided_by="rule",
                    rule_id=rule_proposal.rule_id,
                    rationale=(
                        f"auto-applied rule #{rule_proposal.rule_id} "
                        f"(created_by={rule_proposal.created_by})"
                    ),
                    needs_review=False,
                )
                return PendingEntry(
                    **{**base.__dict__, "target_account": rule_proposal.target_account,
                       "rule_id": rule_proposal.rule_id, "staged_id": staged_id},
                )

        # Post-workstream-A: the gate no longer produces AUTO_APPLY_AI.
        # High-confidence AI proposals fall through to the DEFER-FIXME
        # path below — the AI suggestion is attached to the staged row
        # and the user's click-accept in /review/staged is what writes
        # to the ledger.

        # DEFER-FIXME path — NEXTGEN Phase B2 full swing.
        #
        # Rows that neither a rule nor the AI confidently classifies
        # stay in staging. We do NOT write a FIXME leg to
        # simplefin_transactions.bean. The user resolves them via
        # /review/staged (Accept / Classify as / Dismiss) and the
        # classified entry is written straight to the bean file at
        # that point — no override layer needed.
        #
        # Trade-off: an un-classified row is invisible to bean-check,
        # Fava, balance reports etc. until the user acts. The
        # mitigation is that SimpleFIN can always be re-pulled — if
        # the DB is wiped the staged row just repopulates on the
        # next fetch. The ledger never accumulates FIXMEs whose
        # only purpose was to remember "we owe a decision on this."
        #
        # The fixme_txns counter is gated on staged_inserted: only a
        # fresh-this-run staged row counts as a newly-uncategorized
        # txn. A re-emit of a previously-staged uncategorized row is
        # the same logical "uncategorized" — counting it again on
        # every ingest inflated the per-run and 7-day "FIXME created"
        # KPIs (e.g. New=20 Dup=209 FIXME=528 when only 20 rows were
        # genuinely new). Now FIXME ≤ New, as the user expects.
        if staged_inserted:
            outcome.fixme_txns += 1
            result.fixme_txns += 1
        # ADR-0022: notify_min_fixme_usd is now Decimal end-to-end;
        # compare in Decimal so the threshold stays in money precision.
        # Decimal(str(...)) handles both Decimal-typed settings (post-
        # config migration) and any pre-migration float that slipped in.
        threshold = Decimal(str(getattr(self.settings, "notify_min_fixme_usd", 0) or 0))
        if threshold > 0 and abs(Decimal(txn.amount)) >= threshold:
            result.large_fixmes.append(
                LargeFixmeEvent(
                    txn_id=txn.id,
                    amount=abs(Decimal(txn.amount)),
                    merchant=(txn.merchant or None),
                    source_account=source_account,
                    posted_date=txn.posted_date,
                )
            )
        # Staging decision row — needs_review=True is the authoritative
        # "human must decide" bucket. Post-workstream-C1 ingest never
        # calls the AI, so the decided_by for this staged row is 'rule'
        # (if a low-confidence rule matched) or 'auto' (nothing matched).
        # AI suggestions land via the on-demand /review/staged/ask-ai
        # endpoint when the user wants one.
        fixme_conf = "unresolved"
        fixme_score: float | None = None
        fixme_decided_by = "auto"
        rationale_bits: list[str] = []
        if rule_guard_demoted_reason is not None and rule_proposal is not None:
            # Auto-apply was blocked by the account-active guard.
            # Surface the rule as a suggestion with the guard
            # reason so the user understands why it didn't apply
            # automatically.
            fixme_score = rule_proposal.confidence
            fixme_conf = "medium"
            fixme_decided_by = "rule"
            rationale_bits.append(
                f"rule #{rule_proposal.rule_id} suggests "
                f"{rule_proposal.target_account} "
                f"(blocked from auto-apply: {rule_guard_demoted_reason})"
            )
        elif rule_proposal is not None and rule_proposal.confidence < self.gate.auto_apply_threshold:
            fixme_score = rule_proposal.confidence
            fixme_conf = "medium" if rule_proposal.confidence >= 0.5 else "low"
            fixme_decided_by = "rule"
            rationale_bits.append(
                f"rule #{rule_proposal.rule_id} suggests "
                f"{rule_proposal.target_account} "
                f"(conf {rule_proposal.confidence:.2f}, below auto-apply)"
            )
        # Staged decision stores the best suggestion in hand. The AI
        # branch of this block is gone post-C1 — if the user wants an
        # AI suggestion on this row they click 'Ask AI' in the review
        # UI (tier-3, on demand).
        proposed_account = fixme_account
        if rule_proposal is not None and rule_proposal.target_account:
            proposed_account = rule_proposal.target_account
        self._record_staged_decision(
            staged_id=staged_id,
            account=proposed_account,
            confidence=fixme_conf,
            confidence_score=fixme_score,
            decided_by=fixme_decided_by,
            rule_id=rule_proposal.rule_id if rule_proposal else None,
            rationale=" | ".join(rationale_bits) if rationale_bits else None,
            needs_review=True,
        )
        # ADR-0043 P2 — when staged-txn directives are enabled, queue
        # this PendingEntry for a metadata-only `custom "staged-txn"`
        # directive write at the end of the run. The directive carries
        # the same lamella-txn-id the staged row holds so /txn/{token}
        # is stable across the staging → promotion bridge. Default
        # OFF in v0.3.1 — flag-gated for the soak window before a
        # follow-up release flips the default.
        if getattr(self.settings, "enable_staged_txn_directives", False):
            self._pending_staged_directives.append(base)
        # Return None so the writer batch doesn't include a FIXME
        # PendingEntry for this row. The staged row stays in
        # status='classified' with needs_review=True; the user
        # finds it on /review/staged.
        return None

    def _record_staged_decision(
        self,
        *,
        staged_id: int | None,
        account: str | None,
        confidence: str,
        decided_by: str,
        confidence_score: float | None = None,
        rule_id: int | None = None,
        ai_decision_id: int | None = None,
        rationale: str | None = None,
        needs_review: bool = False,
    ) -> None:
        """Best-effort staging-decision write. Never breaks the ingest —
        staging is supplementary in Phase B1, the ledger is still
        authoritative.

        Don't downgrade an existing meaningful decision. If a previous
        ingest (or an Ask-AI click) recorded a rule- or AI-sourced
        proposal on this row, a re-ingest that finds nothing new must
        not overwrite it with a 'auto / unresolved / FIXME-placeholder'
        row — that erases visible work the user could otherwise act on.
        Re-ingest carries no signal that the previous decision was
        wrong, so the previous decision stands.
        """
        if staged_id is None:
            return
        if decided_by == "auto":
            try:
                existing = StagingService(self.conn).get_decision(staged_id)
            except Exception:  # noqa: BLE001
                existing = None
            if (
                existing is not None
                and existing.decided_by in ("ai", "rule")
            ):
                return
        try:
            StagingService(self.conn).record_decision(
                staged_id=staged_id,
                account=account,
                confidence=confidence,
                confidence_score=confidence_score,
                decided_by=decided_by,
                rule_id=rule_id,
                ai_decision_id=ai_decision_id,
                rationale=rationale,
                needs_review=needs_review,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "simplefin: staged_decision(staged_id=%d) failed: %s",
                staged_id, exc,
            )

    async def _maybe_ai_classify(
        self,
        *,
        txn: SimpleFINTransaction,
        source_account: str | None,
        lamella_txn_id: str | None = None,
    ) -> "_AIClassifyInline | None":
        # ``lamella_txn_id`` lets the receipt-context lookup hit the
        # linked-receipt branch for pre-promotion staged rows. ADR-0056
        # links receipts to staged rows via the row's lamella_txn_id
        # (stored in ``document_links.txn_hash``), but that lookup was
        # silently bypassed here when no ledger txn_hash exists yet —
        # the classifier ran with the candidate-by-amount fallback
        # only, missing OCR / line-item data on receipts the user had
        # explicitly attached. Passing the id from the caller fixes
        # that.
        #
        # ``source_account`` may be ``None`` when the upstream resolver
        # couldn't map the row's source to a backing account (e.g., a
        # reboot row whose raw payload doesn't carry a usable
        # Assets:/Liabilities: leg). Without a card hint the
        # card-binding entity is unknown, so the classifier widens to
        # cross-entity scope below — same shape as the existing
        # cross-entity widening triggered by user redirects, so the
        # gate's intercompany-flag mitigation continues to apply.
        if self.ai is None or not self.ai.enabled:
            return None
        client = self.ai.new_client()
        if client is None:
            return None
        try:
            entity = (
                _entity_from_source(source_account, conn=self.conn)
                if source_account is not None
                else None
            )
            entries = self.reader.load().entries
            # Sign-aware whitelist: a deposit (positive amount) needs
            # Income:* candidates, not Expenses:*. The fixme_account
            # we render into the prompt also reflects this so the
            # whitelist + prompt + AI proposal all agree on the root.
            #
            # Universal sign convention (matches actual ledger writes
            # in writer.py:166-172): positive amount = money IN to the
            # user (deposit on checking, refund/paydown on a card) →
            # Income whitelist. Negative amount = money OUT (withdrawal,
            # card charge) → Expenses whitelist. Earlier liability-
            # inversion code assumed SimpleFIN delivered card charges
            # as positive amounts — actual ledger entries show the
            # opposite (charge = Liabilities:CC -X, refund = +X), so
            # the inversion silently routed every charge to the Income
            # whitelist and every refund to the Expenses whitelist.
            from lamella.features.ai_cascade.context import (
                valid_accounts_by_root,
            )
            _amt = Decimal(txn.amount)
            _root = "Income" if _amt > 0 else "Expenses"

            # Cross-entity whitelist widening: when the user's
            # rejection / hint mentions a DIFFERENT entity (either by
            # account-path "Expenses:OtherCo:..." or by plain-text
            # entity name), widen the whitelist to include every
            # entity's accounts under this root. Without this, the AI
            # is locked to the card's entity and silently maps the
            # user's redirect to the closest same-entity leaf — the
            # user-reported "the AI can't assign this to a different
            # business" failure.
            cross_entity = False
            memo_blob = " ".join(
                s or "" for s in (txn.memo, txn.description, txn.merchant)
            )
            import re as _re
            for _slug in _re.findall(
                r"(?:Expenses|Income|Assets|Liabilities|Equity):"
                r"([A-Za-z][A-Za-z0-9_]+):",
                memo_blob,
            ):
                if _slug and _slug != entity:
                    cross_entity = True
                    break
            if not cross_entity:
                # Plain-text mention of any other registered entity
                # slug. Whole-word match to avoid partial collisions
                # ("Acme" inside "AcmeCorp").
                try:
                    _all_slugs = [
                        r["slug"] for r in self.conn.execute(
                            "SELECT slug FROM entities"
                        ).fetchall()
                    ]
                except Exception:  # noqa: BLE001
                    _all_slugs = []
                _blob_lower = memo_blob.lower()
                for _s in _all_slugs:
                    if not _s or _s == entity:
                        continue
                    if _re.search(
                        rf"\b{_re.escape(_s.lower())}\b", _blob_lower,
                    ):
                        cross_entity = True
                        break

            valid_accounts = valid_accounts_by_root(
                entries,
                root=_root,
                entity=None if cross_entity else entity,
            )
            if not valid_accounts:
                return None
            # Build a tiny prompt; at ingest time we don't have a txn_hash
            # yet, so input_ref is the simplefin-id.
            from lamella.features.ai_cascade.context import render, similar_transactions

            similar = similar_transactions(
                entries,
                needle=(txn.merchant or txn.description or "").strip(),
                reference_date=txn.posted_date,
            )
            # Active-note context — notes the user wrote within a few
            # days of this txn's date (or with an explicit range
            # covering it). They are directional priors, not scoping
            # constraints: the AI weighs them against similar-history
            # and card binding but nothing forces the classification.
            try:
                from lamella.features.notes.service import NoteService
                active_notes = NoteService(self.conn).notes_active_on(
                    txn.posted_date, entity=entity, card=source_account,
                )
            except Exception:  # noqa: BLE001
                log.warning(
                    "simplefin: notes_active_on failed",
                    exc_info=True,
                )
                active_notes = []
            # Phase G3 — merchant-entity suspicion check. If the
            # merchant's history skews to a different entity, surface
            # that signal in the prompt AND force intercompany_flag on
            # the resulting proposal so the gate sends it to review.
            from lamella.features.ai_cascade.context import suspicious_card_binding
            from lamella.features.ai_cascade.receipt_context import fetch_document_context
            merchant_text = (txn.merchant or txn.description or "").strip()
            card_suspicion = (
                suspicious_card_binding(
                    entries, merchant=merchant_text, card_entity=entity,
                ) if merchant_text else None
            )
            # Paperless receipt context. Two paths:
            #   1. Linked — ``document_links.txn_hash`` carries either
            #      the ledger txn_hash (post-promotion) or the staged
            #      row's lamella-txn-id token (ADR-0056 pre-promotion
            #      attachment). When we have the lamella-txn-id from
            #      the caller, try that first so receipts the user
            #      explicitly attached to this row land in the prompt.
            #   2. Candidate by amount + date — fallback when no
            #      explicit link exists; matches a unique Paperless
            #      doc by total + ±3 day window.
            try:
                receipt_ctx = fetch_document_context(
                    self.conn,
                    txn_hash=lamella_txn_id,
                    posting_date=txn.posted_date,
                    amount=abs(Decimal(txn.amount)),
                )
            except Exception:  # noqa: BLE001
                log.warning(
                    "simplefin: receipt_context lookup failed",
                    exc_info=True,
                )
                receipt_ctx = None
            # Mileage log — same proximity model. Pulls all recent
            # entries ± 3 days, entity-ranked. Disambiguates
            # Warehouse Club-fuel-for-multiple-vehicles and amplifies
            # travel-window signals.
            try:
                from lamella.features.ai_cascade.mileage_context import (
                    mileage_context_for_txn,
                )
                mileage_entries = mileage_context_for_txn(
                    self.conn, txn_date=txn.posted_date, entity=entity,
                )
            except Exception:  # noqa: BLE001
                log.warning(
                    "simplefin: mileage_context lookup failed",
                    exc_info=True,
                )
                mileage_entries = []
            # Pass source_account so the FIXME placeholder rendered
            # into the prompt matches the whitelist root chosen above
            # (asset convention for Assets:* sources, inverted for
            # Liabilities:* sources). Without source_account, the
            # FIXME placeholder lands on Income:*:FIXME for any
            # positive-amount credit-card charge while the whitelist
            # root is Expenses — the AI sees a contradictory prompt
            # and either declines or proposes an out-of-whitelist
            # account.
            _fixme_account_for_prompt = _fixme_for_entity(
                entity, Decimal(txn.amount),
                source_account=source_account,
            )
            # Derive fixme_root from the computed FIXME account so the
            # Jinja template's per-root preamble (Income / Expenses /
            # Liabilities / etc.) fires correctly. Without this kwarg
            # the template falls through to the {% else %} block which
            # always renders the Expenses FIXME framing — wrong for
            # reversed entries (deposits) and liability payments.
            # Mirrors bulk_classify._classify_one lines 399-405.
            _fixme_root_for_prompt = (
                (_fixme_account_for_prompt or "").split(":", 1)[0] or "Expenses"
            )
            if _fixme_root_for_prompt not in (
                "Expenses", "Income", "Liabilities", "Equity", "Assets",
            ):
                _fixme_root_for_prompt = "Expenses"
            prompt = render(
                "classify_txn.j2",
                txn={
                    "date": txn.posted_date,
                    "amount": abs(Decimal(txn.amount)),
                    "currency": "USD",
                    "payee": txn.merchant or None,
                    "narration": txn.memo or txn.description or None,
                    "card_account": source_account,
                    "fixme_account": _fixme_account_for_prompt,
                },
                similar=similar,
                entity=entity,
                accounts=valid_accounts,
                active_notes=active_notes,
                card_suspicion=card_suspicion,
                receipt=receipt_ctx,
                mileage_entries=mileage_entries,
                fixme_root=_fixme_root_for_prompt,
            )
            primary_model = self.ai.model_for("classify_txn")
            system_prompt = (
                "You are a meticulous bookkeeper. You classify "
                "transactions into a predefined chart of accounts. "
                "Never invent accounts."
            )
            try:
                result = await client.chat(
                    decision_type="classify_txn",
                    input_ref=txn.id,
                    system=system_prompt,
                    user=prompt,
                    schema=ClassifyResponse,
                    model=primary_model,
                )
            except AIError as exc:
                log.warning("simplefin AI classify failed for %s: %s", txn.id, exc)
                return None
            allowed_accounts = set(valid_accounts)
            primary = _proposal_from_classify_result(
                result, allowed=allowed_accounts,
                card_suspicion=card_suspicion, escalated_from=None,
            )
            # Two-agent cascade — retry with a stronger model when
            # the primary came back uncertain (or its pick got
            # rejected off-whitelist).
            fallback_model = self.ai.fallback_model_for("classify_txn")
            fallback_threshold = self.ai.fallback_threshold()
            if fallback_model and (
                primary is None or primary.confidence < fallback_threshold
            ):
                try:
                    fb_result = await client.chat(
                        decision_type="classify_txn",
                        input_ref=txn.id,
                        system=system_prompt,
                        user=prompt,
                        schema=ClassifyResponse,
                        model=fallback_model,
                    )
                except AIError as exc:
                    log.warning(
                        "simplefin AI classify fallback (%s) failed "
                        "for %s: %s",
                        fallback_model, txn.id, exc,
                    )
                else:
                    escalated = _proposal_from_classify_result(
                        fb_result, allowed=allowed_accounts,
                        card_suspicion=card_suspicion,
                        escalated_from=primary_model,
                    )
                    if escalated is not None:
                        return _AIClassifyInline(
                            proposal=escalated,
                            entity=entity,
                            receipt_paperless_id=(
                                getattr(receipt_ctx, "paperless_id", None)
                                if receipt_ctx else None
                            ),
                            mileage_entries=mileage_entries,
                            active_notes=active_notes,
                            txn_date=txn.posted_date,
                        )
            if primary is None:
                return None
            return _AIClassifyInline(
                proposal=primary,
                entity=entity,
                receipt_paperless_id=(
                    getattr(receipt_ctx, "paperless_id", None)
                    if receipt_ctx else None
                ),
                mileage_entries=mileage_entries,
                active_notes=active_notes,
                txn_date=txn.posted_date,
            )
        finally:
            await client.aclose()



def _proposal_from_classify_result(
    result,
    *,
    allowed: set[str],
    card_suspicion,
    escalated_from: str | None,
) -> AIProposal | None:
    """Shape a classify ChatResponse into an AIProposal, applying
    the off-whitelist guard and the Phase G3 card-suspicion override.
    Mirrors `classify._proposal_from_result`; kept here because the
    SimpleFIN ingest path calls `client.chat` directly."""
    if result.data.target_account not in allowed:
        return None
    ai_flag = bool(getattr(result.data, "intercompany_flag", False))
    ai_owning = getattr(result.data, "owning_entity", None)
    if card_suspicion is not None:
        ai_flag = True
        ai_owning = ai_owning or card_suspicion.dominant_entity
    return AIProposal(
        target_account=result.data.target_account,
        confidence=float(result.data.confidence),
        reasoning=result.data.reasoning,
        decision_id=result.decision_id,
        intercompany_flag=ai_flag,
        owning_entity=ai_owning,
        escalated_from=escalated_from,
    )


def _priority(amount: Decimal | None) -> int:
    if amount is None:
        return 0
    try:
        return max(0, int(abs(Decimal(amount)) // Decimal(100)))
    except Exception:  # noqa: BLE001
        return 0


def _valid_expense_accounts(entries: Iterable, *, entity: str | None) -> list[str]:
    """Re-export a thin alias so ingest doesn't need to import from
    ai.context if the AI block is disabled. Kept as a local wrapper to
    avoid a circular import when AIService is stubbed."""
    from lamella.features.ai_cascade.context import valid_expense_accounts

    return valid_expense_accounts(entries, entity=entity)


# --------------------------------------------------------------------------
# Content-based dedup — catches "SimpleFIN re-delivers the same event with
# a fresh id" so the ledger never grows a duplicate the user has to clean
# up after. Keyed on (date, account, |amount|, normalized narration); when
# a new SimpleFIN txn matches an existing ledger entry on content, the new
# id is stamped as a ``lamella-simplefin-aliases`` entry and the write is
# skipped entirely.
# --------------------------------------------------------------------------

_WS_RE_DEDUP = re.compile(r"\s+")


def _norm_narration(text: str | None) -> str:
    if not text:
        return ""
    return _WS_RE_DEDUP.sub(" ", text).strip().lower()


def _primary_account_for_content(entry) -> str | None:
    """First non-FIXME Assets/Liabilities account on the entry — the
    bank side of the transaction. Mirrors
    duplicates.scanner._primary_account so the ingest-time fingerprint
    matches the one the scanner + UI use."""
    for p in entry.postings or ():
        acct = p.account or ""
        if not acct.startswith(("Assets:", "Liabilities:")):
            continue
        if acct.split(":")[-1].upper() == "FIXME":
            continue
        return acct
    return None


def _content_key_from_entry(entry) -> tuple | None:
    from beancount.core.data import Transaction as _Txn
    if not isinstance(entry, _Txn):
        return None
    account = _primary_account_for_content(entry)
    if not account:
        return None
    amt = None
    for p in entry.postings or ():
        if p.units and p.units.number is not None:
            amt = abs(Decimal(p.units.number))
            break
    if amt is None:
        return None
    narration = _norm_narration(entry.narration)
    payee = _norm_narration(getattr(entry, "payee", None))
    return (str(entry.date), account, f"{amt:.2f}", narration, payee)


def _content_key_from_simplefin(
    *,
    txn,
    source_account: str,
    currency: str,
) -> tuple | None:
    try:
        amt = abs(Decimal(txn.amount))
    except Exception:  # noqa: BLE001
        return None
    narration = _norm_narration(txn.description or txn.memo or "")
    payee = _norm_narration(txn.merchant or "")
    return (txn.posted_date.isoformat(), source_account, f"{amt:.2f}", narration, payee)


def _build_content_index(entries: Iterable) -> dict[tuple, str]:
    """Walk the ledger, index every Transaction by its content
    fingerprint → primary lamella-simplefin-id. When a content key
    appears on multiple entries (i.e. the ledger already has
    duplicates, pre-fix), the first one wins — the aliases get
    stamped on it and the user can still clean up via
    /settings/data-integrity/duplicates.
    """
    index: dict[tuple, str] = {}
    for e in entries:
        sfid = _meta_simplefin_id(e)
        if not sfid:
            continue
        key = _content_key_from_entry(e)
        if key is None:
            continue
        index.setdefault(key, sfid)
    return index


def _stamp_alias_on_ledger(
    *,
    main_bean,
    simplefin_transactions,
    primary_sfid: str,
    alias: str,
) -> bool:
    """Append ``alias`` to the ``lamella-simplefin-aliases`` list on the
    block whose ``lamella-simplefin-id`` equals ``primary_sfid``. Returns
    True when a block was found and modified.

    Uses the same aliases-injection helper the cleanup tool does so
    both code paths produce identical output (idempotent: if
    ``alias`` is already listed, no change is written).
    """
    from lamella.features.data_integrity.cleaner import (
        _extract_sfid_from_block, _inject_aliases_into_block, _TXN_HEADER_RE,
    )
    if not simplefin_transactions.exists():
        return False
    text = simplefin_transactions.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        # Extract SimpleFIN id from either format (legacy txn-level
        # `lamella-simplefin-id` or new posting-level paired source).
        sfid_here = _extract_sfid_from_block(block_lines)
        if sfid_here == primary_sfid:
            new_block = _inject_aliases_into_block(block_lines, {alias})
            if new_block != block_lines:
                modified = True
            out_lines.extend(new_block)
        else:
            out_lines.extend(block_lines)
    if modified:
        simplefin_transactions.write_text("".join(out_lines), encoding="utf-8")
    return modified
