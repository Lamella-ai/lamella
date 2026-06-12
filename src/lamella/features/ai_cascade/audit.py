# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Classification audit — non-destructive disagreement surfacing.

Samples resolved transactions, runs AI classify with current
context, and records any case where the AI disagrees with the
ledger's current target account. The user reviews disagreements
one at a time: Accept → the AI's answer wins (override written,
user_corrected logged), Dismiss → the original stands (silenced
from future audits via audit_dismissals).

Designed to let a user with a large pre-existing ledger probe
whether the current classifications still hold up given today's
context (entity descriptions, active notes, receipt content,
mileage, vector history). Each accept/dismiss is a data point
that strengthens or confirms the classifier.
"""
from __future__ import annotations

import logging
import random
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Iterable

from beancount.core.data import Transaction

from lamella.features.ai_cascade.classify import (
    build_classify_context,
    load_account_descriptions,
    load_entity_context,
    propose_account,
)
from lamella.features.ai_cascade.service import AIService
from lamella.core.beancount_io import LedgerReader
from lamella.core.beancount_io.txn_hash import txn_hash as compute_txn_hash
from lamella.core.registry.ai_context import (
    registry_preamble as build_registry_preamble,
)

log = logging.getLogger(__name__)


UNRESOLVED_LEAVES = frozenset({
    "FIXME", "UNKNOWN", "UNCATEGORIZED", "UNCLASSIFIED",
})


@dataclass
class AuditRun:
    id: int
    sampled: int = 0
    classified: int = 0
    disagreements: int = 0
    errors: int = 0


@dataclass
class AuditItem:
    id: int
    txn_hash: str
    txn_date: date
    txn_amount: Decimal
    merchant_text: str
    current_account: str
    ai_proposed_account: str
    ai_confidence: float
    ai_reasoning: str
    status: str = "open"


def _resolved_txns_eligible(
    entries: Iterable, *, min_amount: Decimal = Decimal("1"),
) -> list[Transaction]:
    """Pick the txns the audit is allowed to test — those with a
    non-FIXME expense target AND an amount worth reviewing."""
    out: list[Transaction] = []
    for e in entries:
        if not isinstance(e, Transaction):
            continue
        target = _primary_expense_account(e)
        if target is None:
            continue
        leaf = target.rsplit(":", 1)[-1].upper()
        if leaf in UNRESOLVED_LEAVES:
            continue
        amount = _primary_amount(e)
        if amount is None or amount < min_amount:
            continue
        out.append(e)
    return out


def _primary_expense_account(txn: Transaction) -> str | None:
    best: str | None = None
    best_amount: Decimal | None = None
    for p in txn.postings or []:
        acct = p.account or ""
        if not acct.startswith("Expenses:"):
            continue
        if p.units and p.units.number is not None:
            amt = abs(Decimal(p.units.number))
            if best_amount is None or amt > best_amount:
                best_amount = amt
                best = acct
    return best


def _primary_amount(txn: Transaction) -> Decimal | None:
    """Cashflow-direction amount for the audit row. Picks the largest-
    magnitude posting and returns its signed cashflow value:
    Assets/Liabilities postings carry cashflow directly; Expenses/
    Income are negated (an Expenses posting of +54.72 represents money
    flowing OUT of the user's wallet, so cashflow is -54.72). Without
    sign preservation /audit's "Open disagreements" list can't
    visually distinguish refunds from charges."""
    primary_signed: Decimal | None = None
    primary_mag: Decimal | None = None
    for p in txn.postings or []:
        if not (p.units and p.units.number is not None):
            continue
        n = Decimal(p.units.number)
        signed = (-n) if (p.account or "").startswith(("Expenses:", "Income:")) else n
        mag = abs(signed)
        if primary_mag is None or mag > primary_mag:
            primary_signed = signed
            primary_mag = mag
    return primary_signed


def _merchant_text(txn: Transaction) -> str:
    return " ".join(filter(None, [
        (txn.payee or "").strip() or None,
        (txn.narration or "").strip() or None,
    ])).strip()


class AuditRunner:
    """One audit pass: sample → classify → record disagreements.

    Instance per run. Bean-check is NOT invoked — we write no
    ledger data. Side effects: audit_runs row + audit_items rows.
    """

    def __init__(
        self,
        *,
        ai: AIService,
        reader: LedgerReader,
        conn: sqlite3.Connection,
        sample_mode: str = "random",
        sample_size: int = 20,
        min_amount: Decimal = Decimal("5"),
        target_account: str | None = None,
        progress_callback=None,
    ):
        self.ai = ai
        self.reader = reader
        self.conn = conn
        self.sample_mode = sample_mode
        self.sample_size = sample_size
        self.min_amount = min_amount
        # When set, restrict the sample pool to txns currently
        # posted to this account. Use when you've added a new
        # narrow child (or updated a parent's description) and
        # want the classifier to re-consider historical txns
        # under that specific account.
        self.target_account = target_account
        # Optional hook called after each classification attempt.
        # Used by the job-runner port to surface live progress.
        # Signature: progress_callback(index, total, txn, outcome)
        # where outcome ∈ {"agree", "disagree", "skipped",
        # "error", "no_context"}.
        self.progress_callback = progress_callback

    async def run(self) -> AuditRun:
        cursor = self.conn.execute(
            "INSERT INTO audit_runs (sample_mode, sample_size) VALUES (?, ?)",
            (self.sample_mode, self.sample_size),
        )
        run_id = int(cursor.lastrowid)
        run = AuditRun(id=run_id)

        # Snapshot pre-run cost so we can log the spend delta when
        # the run completes — gives operators a "this audit cost
        # $0.0X" trace in server logs alongside the per-sample
        # outcomes the job-runner UI already streams.
        cost_at_start: float | None = None
        try:
            cost_at_start = float(self.ai.cost_summary().get("cost_usd", 0.0))
        except Exception:  # noqa: BLE001
            cost_at_start = None

        if not self.ai.enabled or self.ai.spend_cap_reached():
            log.info(
                "audit run #%d skipped: ai_enabled=%s spend_cap_reached=%s",
                run_id, self.ai.enabled, self.ai.spend_cap_reached(),
            )
            self.conn.execute(
                "UPDATE audit_runs SET finished_at = CURRENT_TIMESTAMP, "
                "notes = ? WHERE id = ?",
                ("ai disabled or over cap", run_id),
            )
            return run

        entries = self.reader.load().entries
        pool = _resolved_txns_eligible(entries, min_amount=self.min_amount)
        if self.target_account:
            pool = [
                t for t in pool
                if _primary_expense_account(t) == self.target_account
            ]
        if not pool:
            log.info(
                "audit run #%d: no eligible txns "
                "(sample_mode=%s target=%s)",
                run_id, self.sample_mode, self.target_account or "—",
            )
            self.conn.execute(
                "UPDATE audit_runs SET finished_at = CURRENT_TIMESTAMP, "
                "notes = ? WHERE id = ?",
                ("no eligible txns", run_id),
            )
            return run

        sample = self._sample_pool(pool)
        run.sampled = len(sample)

        dismissed_pairs = self._dismissed_pairs()
        preamble = ""
        try:
            preamble = build_registry_preamble(self.conn)
        except Exception:  # noqa: BLE001
            pass

        client = self.ai.new_client()
        if client is None:
            log.info(
                "audit run #%d: ai client unavailable "
                "(enabled=%s spend_cap=%s)",
                run_id, self.ai.enabled, self.ai.spend_cap_reached(),
            )
            self.conn.execute(
                "UPDATE audit_runs SET finished_at = CURRENT_TIMESTAMP, "
                "sampled = ?, notes = ? WHERE id = ?",
                (run.sampled, "ai client unavailable", run_id),
            )
            return run
        total = len(sample)
        primary_model = self.ai.model_for("classify_txn")
        fallback_model = self.ai.fallback_model_for("classify_txn")
        log.info(
            "audit run #%d started: sample_mode=%s sampled=%d/pool=%d "
            "target=%s primary_model=%s fallback_model=%s",
            run_id, self.sample_mode, run.sampled, len(pool),
            self.target_account or "—",
            primary_model, fallback_model or "—",
        )

        # WP6 Site 4 — principle-3 preemption for the AI audit path.
        #
        # The audit is designed to surface "AI now disagrees with the
        # current classification" flags so users catch mis-categorized
        # transactions. That signal is valuable when the transaction's
        # correct classification depends on context that accumulates
        # over time (notes, receipts, mileage, project tags). It is
        # ACTIVELY HARMFUL for loan payments: the correct split is
        # fixed by the configured amortization at ingest time, and
        # the AI — running against partial accumulated context —
        # produces low-quality disagreement flags that train users to
        # ignore the audit panel.
        #
        # Skipping the AI call here isn't noise reduction; it's
        # preventing the audit from running in the wrong context
        # window entirely. Loan transactions are the exception
        # because they're information-complete at ingest; every other
        # class benefits from the audit running with current context.
        from lamella.features.loans.claim import (
            is_claimed_by_loan as _is_claimed_by_loan,
            load_loans_snapshot as _load_loans_snapshot,
        )
        _loans_cache = _load_loans_snapshot(self.conn)

        try:
            for idx, txn in enumerate(sample):
                current = _primary_expense_account(txn)
                merchant = _merchant_text(txn)
                if current is None:
                    self._notify(idx, total, txn, merchant, "no_context")
                    continue
                if (merchant, current) in dismissed_pairs:
                    # Already told "original was right" — don't re-ask.
                    self._notify(idx, total, txn, merchant, "skipped")
                    continue
                if _loans_cache:
                    claim = _is_claimed_by_loan(txn, self.conn, loans=_loans_cache)
                    if claim is not None:
                        log.info(
                            "loans.preempted slug=%s kind=%s (audit skip)",
                            claim.loan_slug, claim.kind.value,
                        )
                        self._notify(idx, total, txn, merchant, "skipped")
                        continue
                hash12 = compute_txn_hash(txn)[:12]
                log.info(
                    "audit run #%d [%d/%d]: classifying %s "
                    "current=%s model=%s",
                    run_id, idx + 1, total, hash12,
                    current, primary_model,
                )
                try:
                    disagreement = await self._classify_one(
                        client=client, txn=txn, entries=entries,
                        current_account=current, merchant=merchant,
                        preamble=preamble,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "audit classify failed for %s: %s",
                        hash12, exc,
                    )
                    run.errors += 1
                    self._notify(idx, total, txn, merchant, "error")
                    continue
                run.classified += 1
                if disagreement is None:
                    log.info(
                        "audit run #%d [%d/%d] %s: agree (current=%s)",
                        run_id, idx + 1, total, hash12, current,
                    )
                    self._notify(idx, total, txn, merchant, "agree")
                    continue
                run.disagreements += 1
                log.info(
                    "audit run #%d [%d/%d] %s: disagree current=%s "
                    "ai=%s conf=%.2f",
                    run_id, idx + 1, total, hash12, current,
                    disagreement.get("ai_account"),
                    disagreement.get("ai_confidence", 0.0),
                )
                self._record_item(
                    run_id=run_id, txn=txn, current=current,
                    merchant=merchant, **disagreement,
                )
                self._notify(idx, total, txn, merchant, "disagree")
        finally:
            await client.aclose()

        # End-of-run cost trace: re-pull the month-to-date cost and
        # log the delta this run incurred. Operators wondering "did
        # the audit actually call the model?" get a definitive
        # answer in the logs alongside the disagreement counts.
        cost_delta_str = "—"
        try:
            cost_at_end = float(self.ai.cost_summary().get("cost_usd", 0.0))
            if cost_at_start is not None:
                cost_delta_str = f"${cost_at_end - cost_at_start:.4f}"
        except Exception:  # noqa: BLE001
            pass
        log.info(
            "audit run #%d complete: sampled=%d classified=%d "
            "disagreements=%d errors=%d cost_delta=%s model=%s",
            run_id, run.sampled, run.classified,
            run.disagreements, run.errors, cost_delta_str,
            primary_model,
        )

        self.conn.execute(
            """
            UPDATE audit_runs
               SET finished_at = CURRENT_TIMESTAMP,
                   sampled = ?, classified = ?,
                   disagreements = ?, errors = ?
             WHERE id = ?
            """,
            (run.sampled, run.classified, run.disagreements,
             run.errors, run_id),
        )
        return run

    def _notify(
        self,
        idx: int,
        total: int,
        txn: Transaction,
        merchant: str,
        outcome: str,
    ) -> None:
        """Fire the optional progress callback; swallow all errors so a
        broken callback can never corrupt an audit run."""
        cb = self.progress_callback
        if cb is None:
            return
        try:
            cb(idx, total, txn, merchant, outcome)
        except Exception as exc:  # noqa: BLE001
            log.debug("audit progress_callback raised: %s", exc)

    def _sample_pool(self, pool: list[Transaction]) -> list[Transaction]:
        """Today: random sample. Future: prefer high-dollar,
        conflicting-history merchants, card_suspicion windows."""
        if self.sample_mode == "recent":
            pool_sorted = sorted(pool, key=lambda t: t.date, reverse=True)
            return pool_sorted[: self.sample_size]
        if self.sample_mode == "high_dollar":
            pool_sorted = sorted(
                pool,
                key=lambda t: _primary_amount(t) or Decimal(0),
                reverse=True,
            )
            return pool_sorted[: self.sample_size]
        # random
        size = min(self.sample_size, len(pool))
        return random.sample(pool, size)

    def _dismissed_pairs(self) -> set[tuple[str, str]]:
        try:
            rows = self.conn.execute(
                "SELECT merchant_text, current_account FROM audit_dismissals"
            ).fetchall()
        except sqlite3.Error:
            return set()
        return {(r["merchant_text"], r["current_account"]) for r in rows}

    async def _classify_one(
        self,
        *,
        client,
        txn: Transaction,
        entries,
        current_account: str,
        merchant: str,
        preamble: str,
    ) -> dict | None:
        """Run the classifier against an already-resolved txn. If
        the AI's top pick differs from `current_account`, return
        the disagreement dict; else None."""
        (
            view, similar, accounts, entity, active_notes,
            card_suspicion, accounts_by_entity, receipt,
            mileage_entries, vehicle_density,
        ) = build_classify_context(
            entries=entries, txn=txn, conn=self.conn,
        )
        if view is None or not accounts:
            return None
        fixme_root = (
            (view.fixme_account or "").split(":", 1)[0] or "Expenses"
        )
        if fixme_root not in (
            "Expenses", "Income", "Liabilities", "Equity", "Assets",
        ):
            fixme_root = "Expenses"
        proposal = await propose_account(
            client,
            txn=view,
            similar=similar,
            valid_accounts=accounts,
            entity=entity,
            model=self.ai.model_for("classify_txn"),
            registry_preamble=preamble,
            active_notes=active_notes,
            card_suspicion=card_suspicion,
            accounts_by_entity=accounts_by_entity,
            receipt=receipt,
            mileage_entries=mileage_entries,
            vehicle_density=vehicle_density,
            fallback_model=self.ai.fallback_model_for("classify_txn"),
            fallback_threshold=self.ai.fallback_threshold(),
            account_descriptions=load_account_descriptions(self.conn),
            entity_context=load_entity_context(self.conn, entity),
            fixme_root=fixme_root,
        )
        if proposal is None:
            return None
        if proposal.target_account == current_account:
            return None
        # Also skip if the AI's pick is in a different entity AND
        # intercompany wasn't explicitly flagged — audit isn't the
        # right venue to raise cross-entity moves.
        if (
            proposal.target_account.split(":")[1:2]
            != current_account.split(":")[1:2]
            and not proposal.intercompany_flag
        ):
            return None
        return {
            "ai_account": proposal.target_account,
            "ai_confidence": float(proposal.confidence),
            "ai_reasoning": proposal.reasoning or "",
            "ai_decision_id": proposal.decision_id,
        }

    def _record_item(
        self,
        *,
        run_id: int,
        txn: Transaction,
        current: str,
        merchant: str,
        ai_account: str,
        ai_confidence: float,
        ai_reasoning: str,
        ai_decision_id: int | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO audit_items (
                audit_run_id, txn_hash, txn_date, txn_amount,
                merchant_text, current_account, ai_proposed_account,
                ai_confidence, ai_reasoning, ai_decision_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, compute_txn_hash(txn), txn.date.isoformat(),
                str(_primary_amount(txn) or Decimal(0)),
                merchant, current, ai_account,
                ai_confidence, ai_reasoning, ai_decision_id,
            ),
        )
