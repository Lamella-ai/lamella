# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal
from typing import Iterable, Iterator

from beancount.core.data import Transaction

from lamella.core.beancount_io import LedgerReader, txn_hash
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.engine import evaluate
from lamella.features.rules.models import RuleMatch, TxnFacts
from lamella.features.rules.overrides import OverrideWriter
from lamella.features.rules.service import RuleService

log = logging.getLogger(__name__)

_CARD_ROOTS = ("Liabilities", "Assets")

AUTO_APPLY_THRESHOLD = 0.95


def _is_fixme(account: str) -> bool:
    # Matches "Expenses:FIXME" and any account whose leaf is "FIXME".
    if not account:
        return False
    return account.split(":")[-1].upper() == "FIXME"


def iter_fixme_transactions(entries: Iterable) -> Iterator[Transaction]:
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        if any(_is_fixme(p.account) for p in entry.postings):
            yield entry


def _fixme_amount(txn: Transaction) -> Decimal | None:
    for posting in txn.postings:
        if not _is_fixme(posting.account):
            continue
        units = posting.units
        if units is None or units.number is None:
            continue
        return Decimal(units.number)
    return None


def _fixme_currency(txn: Transaction) -> str:
    for posting in txn.postings:
        if _is_fixme(posting.account) and posting.units and posting.units.currency:
            return posting.units.currency
    return "USD"


def _fixme_account(txn: Transaction) -> str | None:
    for posting in txn.postings:
        if _is_fixme(posting.account):
            return posting.account
    return None


def _card_account(txn: Transaction) -> str | None:
    for posting in txn.postings:
        root = (posting.account or "").split(":", 1)[0]
        if root in _CARD_ROOTS:
            return posting.account
    return None


def _priority(amount: Decimal | None) -> int:
    if amount is None:
        return 0
    try:
        return max(0, int(abs(Decimal(amount)) // Decimal(100)))
    except Exception:
        return 0


def _txn_facts(txn: Transaction) -> TxnFacts:
    return TxnFacts(
        payee=txn.payee,
        narration=txn.narration,
        amount=_fixme_amount(txn),
        card_account=_card_account(txn),
    )


def _rule_suggestion_payload(match: RuleMatch) -> dict:
    return {
        "rule_id": match.rule.id,
        "target_account": match.target_account,
        "pattern_type": match.rule.pattern_type,
        "pattern_value": match.rule.pattern_value,
        "tier": match.tier,
        "confidence": match.rule.confidence,
        "created_by": match.rule.created_by,
    }


def combined_suggestion(
    *,
    rule: dict | None = None,
    ai: dict | None = None,
) -> dict | None:
    out: dict = {}
    if rule:
        out["rule"] = rule
    if ai:
        out["ai"] = ai
    return out or None


def _to_date(value) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


class FixmeScanner:
    def __init__(
        self,
        *,
        reader: LedgerReader,
        reviews: ReviewService,
        rules: RuleService,
        override_writer: OverrideWriter | None = None,
    ):
        self.reader = reader
        self.reviews = reviews
        self.rules = rules
        self.override_writer = override_writer

    def scan(self) -> int:
        """Walk the ledger and enqueue any new FIXME transactions.

        Phase 3: a user-created rule at `confidence >= AUTO_APPLY_THRESHOLD`
        triggers an automatic override write; the review row is inserted as
        already-resolved with `user_decision='auto_accepted'` so the paper
        trail is preserved.

        Dedup is by `source_ref = "fixme:<txn_hash>"` — if a review row
        already exists for a given txn hash (open OR resolved) we skip.
        Returns the number of rows newly enqueued.
        """
        ledger = self.reader.load()
        all_rules = list(self.rules.iter_active())

        enqueued = 0
        ledger_invalidated = False
        for txn in iter_fixme_transactions(ledger.entries):
            h = txn_hash(txn)
            source_ref = f"fixme:{h}"
            if self._already_queued(source_ref):
                continue

            match = evaluate(_txn_facts(txn), all_rules)
            rule_payload = _rule_suggestion_payload(match) if match else None
            suggestion = combined_suggestion(rule=rule_payload)
            amount = _fixme_amount(txn)

            should_auto_apply = (
                match is not None
                and match.rule.created_by == "user"
                and match.rule.confidence >= AUTO_APPLY_THRESHOLD
                and self.override_writer is not None
            )

            if should_auto_apply:
                try:
                    self._auto_apply(txn, match.rule.target_account)
                    self.rules.bump(match.rule.id)
                    self.reviews.enqueue_resolved(
                        kind="fixme",
                        source_ref=source_ref,
                        priority=_priority(amount),
                        ai_suggestion=json.dumps(suggestion) if suggestion else None,
                        ai_model=None,
                        user_decision=f"auto_accepted→{match.rule.target_account}",
                    )
                    ledger_invalidated = True
                    enqueued += 1
                    continue
                except Exception as exc:
                    log.warning(
                        "auto-apply failed for %s (rule #%d): %s — falling back to review queue",
                        h[:12],
                        match.rule.id,
                        exc,
                    )

            self.reviews.enqueue(
                kind="fixme",
                source_ref=source_ref,
                priority=_priority(amount),
                ai_suggestion=json.dumps(suggestion) if suggestion else None,
                ai_model=None,
            )
            enqueued += 1
        if ledger_invalidated:
            self.reader.invalidate()
        if enqueued:
            log.info("FIXME scanner enqueued %d item(s)", enqueued)
        return enqueued

    def _already_queued(self, source_ref: str) -> bool:
        row = self.reviews.conn.execute(
            "SELECT 1 FROM review_queue WHERE source_ref = ? LIMIT 1",
            (source_ref,),
        ).fetchone()
        return row is not None

    def _auto_apply(self, txn: Transaction, target_account: str) -> None:
        if self.override_writer is None:
            raise RuntimeError("override_writer not configured")
        amount = _fixme_amount(txn)
        from_account = _fixme_account(txn)
        if amount is None or from_account is None:
            raise ValueError("txn has no FIXME posting with amount")
        self.override_writer.append(
            txn_date=_to_date(txn.date),
            txn_hash=txn_hash(txn),
            amount=amount,
            from_account=from_account,
            to_account=target_account,
            currency=_fixme_currency(txn),
            narration=(txn.narration or "FIXME auto-apply"),
        )
