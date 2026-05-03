# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 2 of the immutable /txn/{token} URL invariant: when a staged
row is promoted to the ledger, the on-disk lamella-txn-id MUST equal
the staged row's lamella_txn_id. Single-leg, multi-leg-split, and
transfer-pair writes all carry the identity through."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
import re

from lamella.features.bank_sync.writer import PendingEntry, render_entry


_UUIDV7_RE = (
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def _entry(**overrides):
    base = dict(
        date=date(2026, 4, 20),
        simplefin_id="sfid-1",
        payee="Acme Co.",
        narration="staged",
        amount=Decimal("-12.34"),
        currency="USD",
        source_account="Assets:Bank:Checking",
        target_account="Expenses:Groceries",
    )
    base.update(overrides)
    return PendingEntry(**base)


class TestRenderEntry:
    def test_uses_supplied_lamella_txn_id(self):
        entry = _entry(lamella_txn_id="01900000-0000-7000-8000-000000000abc")
        text = render_entry(entry)
        assert 'lamella-txn-id: "01900000-0000-7000-8000-000000000abc"' in text

    def test_mints_when_not_supplied(self):
        entry = _entry()
        text = render_entry(entry)
        m = re.search(r'lamella-txn-id: "([^"]+)"', text)
        assert m is not None
        assert re.match(_UUIDV7_RE, m.group(1))

    def test_id_appears_at_txn_meta_not_posting(self):
        # The stamped id is on the transaction header line, before any
        # posting indentation.
        entry = _entry(lamella_txn_id="01900000-0000-7000-8000-000000000abc")
        text = render_entry(entry)
        # The lamella-txn-id line is indented at txn-meta level (2
        # spaces), not posting-meta (4 spaces).
        meta_line = next(
            ln for ln in text.splitlines()
            if "lamella-txn-id" in ln and "alias" not in ln
        )
        assert meta_line.startswith("  lamella-txn-id"), meta_line
        assert not meta_line.startswith("    "), meta_line


class TestTransferPairAlias:
    """Both legs of a transfer pair map to ONE ledger entry. Leg A's
    lamella_txn_id becomes the entry's primary lamella-txn-id; leg B's
    becomes an alias so /txn/{b_token} keeps resolving post-promotion.
    """

    def _ctx(self, *, a_lid: str | None, b_lid: str | None):
        from lamella.features.import_.staging.transfer_writer import (
            _PairContext, _render_transfer,
        )
        ctx = _PairContext(
            pair_id=42,
            a_staged_id=1,
            a_source="simplefin",
            a_account="Assets:Bank:Checking",
            a_amount=Decimal("-100.00"),
            a_date="2026-04-20",
            a_payee="Transfer",
            a_description=None,
            a_currency="USD",
            a_source_ref={"account_id": "x", "txn_id": "T1"},
            a_lamella_txn_id=a_lid,
            b_staged_id=2,
            b_source="simplefin",
            b_account="Assets:Savings",
            b_amount=Decimal("100.00"),
            b_date="2026-04-20",
            b_payee="Transfer",
            b_description=None,
            b_currency="USD",
            b_source_ref={"account_id": "y", "txn_id": "T2"},
            b_lamella_txn_id=b_lid,
        )
        return _render_transfer(ctx)

    def test_a_id_becomes_primary_b_id_becomes_alias(self):
        text = self._ctx(
            a_lid="01900000-0000-7000-8000-aaaaaaaaaaaa",
            b_lid="01900000-0000-7000-8000-bbbbbbbbbbbb",
        )
        assert 'lamella-txn-id: "01900000-0000-7000-8000-aaaaaaaaaaaa"' in text
        assert 'lamella-txn-id-alias-0: "01900000-0000-7000-8000-bbbbbbbbbbbb"' in text

    def test_no_alias_when_b_missing(self):
        text = self._ctx(
            a_lid="01900000-0000-7000-8000-aaaaaaaaaaaa", b_lid=None,
        )
        assert "lamella-txn-id-alias-0" not in text

    def test_falls_back_to_mint_when_a_missing(self):
        text = self._ctx(a_lid=None, b_lid=None)
        m = re.search(r'lamella-txn-id: "([^"]+)"', text)
        assert m is not None
        assert re.match(_UUIDV7_RE, m.group(1))


class TestSplitEntry:
    def test_split_entry_uses_supplied_id(self, tmp_path):
        from lamella.features.bank_sync.writer import (
            SimpleFINWriter, ensure_simplefin_file_exists,
        )
        main_bean = tmp_path / "main.bean"
        sf_bean = tmp_path / "simplefin_transactions.bean"
        # Minimal main.bean header so include-resolution + open
        # directives don't 500 the writer.
        main_bean.write_text(
            '2020-01-01 open Assets:Bank:Checking USD\n'
            '2020-01-01 open Liabilities:Loan:Principal USD\n'
            '2020-01-01 open Expenses:Loan:Interest USD\n'
            'include "simplefin_transactions.bean"\n',
            encoding="utf-8",
        )
        ensure_simplefin_file_exists(sf_bean)
        writer = SimpleFINWriter(
            main_bean=main_bean, simplefin_path=sf_bean, run_check=False,
        )
        writer.append_split_entry(
            txn_date=date(2026, 4, 20),
            simplefin_id="sfid-9",
            source_account="Assets:Bank:Checking",
            source_amount=Decimal("-100.00"),
            splits=[
                ("Liabilities:Loan:Principal", Decimal("80.00")),
                ("Expenses:Loan:Interest", Decimal("20.00")),
            ],
            payee="Acme Mortgage",
            narration="payment",
            lamella_txn_id="01900000-0000-7000-8000-cccccccccccc",
        )
        text = sf_bean.read_text(encoding="utf-8")
        assert 'lamella-txn-id: "01900000-0000-7000-8000-cccccccccccc"' in text
