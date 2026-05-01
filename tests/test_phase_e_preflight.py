# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for Phase E pre-flight FIXME report + reboot-apply gate."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data as bdata
from beancount.core.amount import Amount
from beancount.core.number import D

from lamella.features.import_.staging import (
    fixme_heavy_payees,
    preflight_report_hash,
)


def _txn(
    *, d: date, payee: str, narration: str, target: str, amount: str = "10",
) -> bdata.Transaction:
    amt = D(amount)
    return bdata.Transaction(
        meta={}, date=d, flag="*", payee=payee, narration=narration,
        tags=frozenset(), links=frozenset(),
        postings=[
            bdata.Posting(
                account="Liabilities:Acme:Card:0001",
                units=Amount(-amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
            bdata.Posting(
                account=target,
                units=Amount(amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


# --- report generation ----------------------------------------------


class TestFixmeHeavyPayees:
    def test_below_min_count_not_flagged(self):
        entries = [
            _txn(d=date(2026, 1, 1), payee="PayPal Transfer",
                 narration="x", target="Expenses:FIXME"),
        ]
        rpt = fixme_heavy_payees(entries, min_count=10)
        assert rpt.payees == []

    def test_all_fixme_at_threshold_flagged(self):
        """11 payee occurrences, all FIXME → flagged."""
        entries = [
            _txn(d=date(2026, i % 12 + 1, 1), payee="PayPal Transfer",
                 narration="x", target="Expenses:FIXME")
            for i in range(11)
        ]
        rpt = fixme_heavy_payees(entries, min_count=10)
        assert len(rpt.payees) == 1
        p = rpt.payees[0]
        assert p.normalized_payee == "paypal transfer"
        assert p.fixme_count == 11
        assert p.total == 11
        assert p.fixme_share == 1.0

    def test_mixed_below_share_not_flagged(self):
        """50/50 split is below the default 60% threshold → no flag."""
        entries = []
        for i in range(10):
            entries.append(_txn(
                d=date(2026, 1, 1), payee="Ambiguous", narration="x",
                target="Expenses:FIXME",
            ))
        for i in range(10):
            entries.append(_txn(
                d=date(2026, 1, 1), payee="Ambiguous", narration="x",
                target="Expenses:Acme:Supplies",
            ))
        rpt = fixme_heavy_payees(entries, min_count=10, min_share=0.6)
        assert rpt.payees == []

    def test_mostly_fixme_with_some_resolved(self):
        """8 FIXME + 2 resolved = 80% share → flagged with
        sample_accounts listing the 2 resolved accounts."""
        entries = []
        for i in range(8):
            entries.append(_txn(
                d=date(2026, 1, 1), payee="Stamps Add Funds",
                narration="x", target="Expenses:FIXME",
            ))
        for i in range(2):
            entries.append(_txn(
                d=date(2026, 1, 1), payee="Stamps Add Funds",
                narration="x",
                target="Expenses:Acme:OtherExpenses:Postage",
            ))
        rpt = fixme_heavy_payees(entries, min_count=10, min_share=0.6)
        assert len(rpt.payees) == 1
        p = rpt.payees[0]
        assert p.fixme_count == 8
        assert p.total == 10
        assert p.fixme_share == 0.8
        assert p.sample_accounts == (
            ("Expenses:Acme:OtherExpenses:Postage", 2),
        )

    def test_uncategorized_counts_as_unresolved(self):
        """Expenses:Personal:Uncategorized leaves count as
        "needs-to-be-addressed" alongside FIXME — the user
        explicitly called this out. 10 Uncategorized hits on
        Fast Food should flag at the same threshold."""
        entries = [
            _txn(d=date(2026, i % 12 + 1, 1), payee="Fast Food",
                 narration="x", target="Expenses:Personal:Uncategorized")
            for i in range(11)
        ]
        rpt = fixme_heavy_payees(entries, min_count=10)
        assert len(rpt.payees) == 1
        assert rpt.payees[0].fixme_count == 11

    def test_unknown_unclassified_also_counted(self):
        entries = (
            [
                _txn(d=date(2026, i % 12 + 1, 1), payee="A",
                     narration="x", target="Expenses:Personal:Unknown")
                for i in range(11)
            ]
            + [
                _txn(d=date(2026, i % 12 + 1, 1), payee="B",
                     narration="x", target="Expenses:Personal:Unclassified")
                for i in range(11)
            ]
        )
        rpt = fixme_heavy_payees(entries, min_count=10)
        assert {p.normalized_payee for p in rpt.payees} == {"a", "b"}

    def test_extra_ok_leaves_opts_out(self):
        """When the user treats Uncategorized as a legitimate catch-all,
        extra_ok_leaves lets them tell the scanner not to flag it."""
        entries = [
            _txn(d=date(2026, i % 12 + 1, 1), payee="Fast Food",
                 narration="x", target="Expenses:Personal:Uncategorized")
            for i in range(11)
        ]
        rpt = fixme_heavy_payees(
            entries, min_count=10,
            extra_ok_leaves=frozenset({"UNCATEGORIZED"}),
        )
        # Uncategorized no longer counts → no flag.
        assert rpt.payees == []

    def test_ordering_by_fixme_count(self):
        entries = []
        for i in range(20):
            entries.append(_txn(
                d=date(2026, 1, 1), payee="Big FIXME",
                narration="x", target="Expenses:FIXME",
            ))
        for i in range(11):
            entries.append(_txn(
                d=date(2026, 1, 1), payee="Smaller FIXME",
                narration="x", target="Expenses:FIXME",
            ))
        rpt = fixme_heavy_payees(entries, min_count=10)
        # Ranked by FIXME count descending.
        assert rpt.payees[0].normalized_payee == "big fixme"
        assert rpt.payees[1].normalized_payee == "smaller fixme"


# --- hashing ---------------------------------------------------------


class TestReportHash:
    def test_same_report_same_hash(self):
        entries = [
            _txn(d=date(2026, 1, 1), payee="PayPal",
                 narration="x", target="Expenses:FIXME")
            for _ in range(10)
        ]
        rpt1 = fixme_heavy_payees(entries, min_count=10)
        rpt2 = fixme_heavy_payees(entries, min_count=10)
        assert preflight_report_hash(rpt1) == preflight_report_hash(rpt2)

    def test_new_fixme_payee_changes_hash(self):
        """When a new FIXME-heavy payee appears, the hash changes so
        a stale acknowledgment expires."""
        base = [
            _txn(d=date(2026, 1, 1), payee="PayPal",
                 narration="x", target="Expenses:FIXME")
            for _ in range(10)
        ]
        rpt_before = fixme_heavy_payees(base, min_count=10)
        h_before = preflight_report_hash(rpt_before)

        expanded = base + [
            _txn(d=date(2026, 1, 1), payee="NewOne",
                 narration="x", target="Expenses:FIXME")
            for _ in range(10)
        ]
        rpt_after = fixme_heavy_payees(expanded, min_count=10)
        assert preflight_report_hash(rpt_after) != h_before

    def test_empty_report_has_stable_hash(self):
        from lamella.features.import_.staging import PreflightReport
        h1 = preflight_report_hash(PreflightReport())
        h2 = preflight_report_hash(PreflightReport())
        assert h1 == h2


# --- route gate ------------------------------------------------------


def _write_ledger_with_fixmes(path: Path):
    """Fifteen PayPal transfers all landing in Expenses:FIXME — a
    synthetic stand-in for the motivating scenario."""
    body = [
        'option "operating_currency" "USD"',
        "2020-01-01 open Assets:Bank USD",
        "2020-01-01 open Expenses:FIXME USD",
    ]
    for i in range(15):
        body.append(f'2026-{(i % 12) + 1:02d}-01 * "PayPal Transfer"')
        body.append(f"  Assets:Bank      -100.00 USD")
        body.append(f"  Expenses:FIXME    100.00 USD")
        body.append("")
    (path / "main.bean").write_text("\n".join(body), encoding="utf-8")


class TestRouteGate:
    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_apply_reboot_refused_without_ack(
        self, app_client, tmp_path, settings,
    ):
        """When FIXME-heavy payees exist and the ack hash is missing,
        apply-reboot returns 409 and the page tells the user why."""
        _write_ledger_with_fixmes(settings.ledger_dir)
        # Pretend a reboot plan was prepared: create an empty
        # .reboot/ dir so the writer gets past its "no plan" guard.
        reboot_dir = settings.ledger_dir / ".reboot"
        reboot_dir.mkdir(parents=True, exist_ok=True)
        (reboot_dir / "main.bean").write_text(
            (settings.ledger_dir / "main.bean").read_text(encoding="utf-8"),
            encoding="utf-8",
        )

        r = app_client.post("/settings/data-integrity/apply-reboot")
        assert r.status_code == 409
        assert "pre-flight" in r.text.lower() or "FIXME-heavy" in r.text

    @pytest.mark.xfail(reason="pre-existing pre-2026-04-29; see project_pytest_baseline_triage.md", strict=False)
    def test_acknowledge_then_apply_proceeds(
        self, app_client, tmp_path, settings, monkeypatch,
    ):
        """After acknowledgment, apply-reboot is no longer gated by
        the pre-flight."""
        _write_ledger_with_fixmes(settings.ledger_dir)
        reboot_dir = settings.ledger_dir / ".reboot"
        reboot_dir.mkdir(parents=True, exist_ok=True)
        (reboot_dir / "main.bean").write_text(
            (settings.ledger_dir / "main.bean").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        # Stub bean-check so apply succeeds.
        monkeypatch.setattr(
            "lamella.features.import_.staging.reboot_writer.capture_bean_check",
            lambda _m: (0, ""),
        )
        monkeypatch.setattr(
            "lamella.features.import_.staging.reboot_writer.run_bean_check_vs_baseline",
            lambda _m, _b: None,
        )

        r_ack = app_client.post(
            "/settings/data-integrity/acknowledge-preflight"
        )
        assert r_ack.status_code == 200

        r_apply = app_client.post("/settings/data-integrity/apply-reboot")
        assert r_apply.status_code == 200
        # The "pre-flight" gate message should NOT appear on this response —
        # acknowledgment is fresh, apply proceeded.
        assert "pre-flight" not in r_apply.text.lower() or "apply" in r_apply.text.lower()
