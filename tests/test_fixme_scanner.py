# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from pathlib import Path

from lamella.core.beancount_io import LedgerReader
from lamella.features.review_queue.service import ReviewService
from lamella.features.rules.scanner import FixmeScanner
from lamella.features.rules.service import RuleService


_FIXME_APPEND = """
2024-01-01 open Expenses:FIXME USD

2026-04-10 * "Hardware Store" "Supplies for workshop (uncat)"
  simplefin-id: "sf-2001"
  Liabilities:Acme:Card:CardA1234  -275.00 USD
  Expenses:FIXME                      275.00 USD

2026-04-12 * "Small merchant" "Tiny uncategorized"
  simplefin-id: "sf-2002"
  Liabilities:Acme:Card:CardA1234  -15.00 USD
  Expenses:FIXME                      15.00 USD
"""


def _add_fixme_txns(ledger_dir: Path) -> None:
    (ledger_dir / "simplefin_transactions.bean").write_text(
        (ledger_dir / "simplefin_transactions.bean").read_text(encoding="utf-8") + _FIXME_APPEND,
        encoding="utf-8",
    )


def test_scanner_enqueues_fixme_items(db, ledger_dir):
    _add_fixme_txns(ledger_dir)
    reader = LedgerReader(ledger_dir / "main.bean")
    scanner = FixmeScanner(
        reader=reader,
        reviews=ReviewService(db),
        rules=RuleService(db),
    )
    count = scanner.scan()
    assert count == 2
    open_items = ReviewService(db).list_open()
    kinds = {i.kind for i in open_items}
    assert kinds == {"fixme"}
    # Dedup: re-scan does not re-enqueue.
    assert scanner.scan() == 0


def test_scanner_priority_scales_with_amount(db, ledger_dir):
    _add_fixme_txns(ledger_dir)
    reader = LedgerReader(ledger_dir / "main.bean")
    scanner = FixmeScanner(
        reader=reader,
        reviews=ReviewService(db),
        rules=RuleService(db),
    )
    scanner.scan()
    items = ReviewService(db).list_open()
    by_priority = sorted(items, key=lambda i: -i.priority)
    # $275 → floor(275/100) = 2, $15 → 0
    assert by_priority[0].priority == 2
    assert by_priority[-1].priority == 0


def test_scanner_attaches_rule_suggestion(db, ledger_dir):
    _add_fixme_txns(ledger_dir)
    rule_svc = RuleService(db)
    rule_svc.create(
        pattern_type="merchant_contains",
        pattern_value="hardware store",
        target_account="Expenses:Acme:Supplies",
        confidence=0.80,  # below auto-apply so the item stays in the queue
    )
    reader = LedgerReader(ledger_dir / "main.bean")
    # Phase 3: scanner takes no override_writer here, so it won't auto-apply
    # even if the rule were at 1.0. This test is specifically about the
    # suggestion payload attached to the open review row.
    FixmeScanner(
        reader=reader,
        reviews=ReviewService(db),
        rules=rule_svc,
    ).scan()

    import json

    items = ReviewService(db).list_open()
    hd_item = None
    for i in items:
        if not i.ai_suggestion:
            continue
        payload = json.loads(i.ai_suggestion)
        rule = payload.get("rule")
        if rule and rule.get("target_account") == "Expenses:Acme:Supplies":
            hd_item = i
            break
    assert hd_item is not None
    payload = json.loads(hd_item.ai_suggestion)
    assert payload["rule"]["pattern_type"] == "merchant_contains"
    assert payload["rule"]["target_account"] == "Expenses:Acme:Supplies"
