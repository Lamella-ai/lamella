# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0043 / ADR-0043b — staged-txn directive round-trip.

Covers:
* Renderer output shape matches the frozen ADR-0043b spec
* beancount.loader parses the directive without errors
* All required meta fields land on the parsed Custom entry
* InvalidSourceError fires on closed-enum violations
* Promoted-form carries the supplemental promotion meta
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount import loader
from beancount.core.data import Custom

from lamella.features.bank_sync.writer import (
    InvalidSourceError,
    PendingEntry,
    render_staged_txn_directive,
    render_staged_txn_promoted_directive,
)


_LEDGER_PRELUDE = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
)


def _make_entry(**overrides) -> PendingEntry:
    base = dict(
        date=date(2026, 4, 29),
        simplefin_id="ABC123",
        payee=None,
        narration="Test purchase",
        amount=Decimal("-42.17"),
        currency="USD",
        source_account="Assets:Personal:Bank:Checking",
        target_account="Expenses:Personal:FIXME",
        lamella_txn_id="01900000-0000-7000-8000-000000000001",
    )
    base.update(overrides)
    return PendingEntry(**base)


def _parse(content: str, tmp_path: Path) -> tuple[list, list]:
    fp = tmp_path / "test.bean"
    fp.write_text(_LEDGER_PRELUDE + content, encoding="utf-8")
    entries, errors, _ = loader.load_file(str(fp))
    return entries, errors


class TestRenderShape:
    def test_includes_required_meta_keys(self):
        out = render_staged_txn_directive(_make_entry())
        for key in (
            "lamella-txn-id",
            "lamella-source",
            "lamella-source-reference-id",
            "lamella-txn-date",
            "lamella-txn-amount",
            "lamella-source-account",
            "lamella-txn-narration",
        ):
            assert key in out, f"missing required meta key: {key}"

    def test_header_carries_source_as_positional_arg(self):
        # ADR-0043b § beancount-pad-plugin compat: the source string
        # appears once as a positional arg + once in meta.
        out = render_staged_txn_directive(_make_entry(), source="simplefin")
        assert 'custom "staged-txn" "simplefin"' in out

    def test_signed_amount_preserved_negative(self):
        out = render_staged_txn_directive(_make_entry(amount=Decimal("-42.17")))
        assert "lamella-txn-amount: -42.17 USD" in out

    def test_signed_amount_preserved_positive(self):
        out = render_staged_txn_directive(_make_entry(amount=Decimal("100.00")))
        assert "lamella-txn-amount: 100.00 USD" in out

    def test_narration_falls_back_to_payee_when_narration_missing(self):
        out = render_staged_txn_directive(
            _make_entry(narration=None, payee="Coffee Shop")
        )
        assert "Coffee Shop" in out

    def test_narration_quoting_escapes_double_quote(self):
        out = render_staged_txn_directive(
            _make_entry(narration='ACME "INC" PMT')
        )
        assert 'ACME \\"INC\\" PMT' in out


class TestSourceEnum:
    @pytest.mark.parametrize(
        "valid_source", ["simplefin", "csv", "paste", "reboot"]
    )
    def test_known_sources_pass(self, valid_source):
        # Must not raise.
        out = render_staged_txn_directive(
            _make_entry(), source=valid_source,
        )
        assert f'lamella-source: "{valid_source}"' in out

    def test_unknown_source_rejected(self):
        with pytest.raises(InvalidSourceError) as exc_info:
            render_staged_txn_directive(_make_entry(), source="unknown")
        assert exc_info.value.source == "unknown"

    def test_manual_explicitly_rejected(self):
        # ADR-0043b §2: manual entries skip staging entirely.
        with pytest.raises(InvalidSourceError):
            render_staged_txn_directive(_make_entry(), source="manual")


class TestBeancountRoundTrip:
    def test_parses_without_errors(self, tmp_path):
        entries, errors = _parse(
            render_staged_txn_directive(_make_entry()), tmp_path,
        )
        assert errors == []
        assert any(isinstance(e, Custom) for e in entries)

    def test_meta_survives_parse(self, tmp_path):
        entries, _ = _parse(
            render_staged_txn_directive(_make_entry()), tmp_path,
        )
        custom_entries = [e for e in entries if isinstance(e, Custom)]
        assert len(custom_entries) == 1
        c = custom_entries[0]
        assert c.type == "staged-txn"
        assert c.meta["lamella-txn-id"] == "01900000-0000-7000-8000-000000000001"
        assert c.meta["lamella-source"] == "simplefin"

    def test_multiple_directives_each_parse(self, tmp_path):
        rendered = (
            render_staged_txn_directive(_make_entry(simplefin_id="A1"))
            + render_staged_txn_directive(_make_entry(simplefin_id="B2"))
            + render_staged_txn_directive(_make_entry(simplefin_id="C3"))
        )
        entries, errors = _parse(rendered, tmp_path)
        assert errors == []
        custom_entries = [e for e in entries if isinstance(e, Custom)]
        assert len(custom_entries) == 3

    def test_no_balance_sheet_impact(self, tmp_path):
        # Sanity: a custom directive must not register as a Transaction.
        from beancount.core.data import Transaction
        entries, _ = _parse(
            render_staged_txn_directive(_make_entry()), tmp_path,
        )
        assert not any(isinstance(e, Transaction) for e in entries)


class TestPromotedDirective:
    def test_includes_required_promotion_meta(self):
        out = render_staged_txn_promoted_directive(
            _make_entry(),
            promoted_at="2026-04-29T14:23:07+00:00",
            promoted_by="manual",
        )
        assert 'custom "staged-txn-promoted"' in out
        assert "lamella-promoted-at" in out
        assert "lamella-promoted-by" in out

    def test_rule_promotion_carries_rule_id(self):
        out = render_staged_txn_promoted_directive(
            _make_entry(),
            promoted_at="2026-04-29T14:23:07+00:00",
            promoted_by="rule",
            promoted_rule_id="rule-42",
        )
        assert 'lamella-promoted-rule-id: "rule-42"' in out

    def test_ai_promotion_carries_model(self):
        out = render_staged_txn_promoted_directive(
            _make_entry(),
            promoted_at="2026-04-29T14:23:07+00:00",
            promoted_by="ai",
            promoted_ai_model="claude-haiku-4-5",
        )
        assert 'lamella-promoted-ai-model: "claude-haiku-4-5"' in out

    def test_manual_promotion_omits_rule_and_model(self):
        out = render_staged_txn_promoted_directive(
            _make_entry(),
            promoted_at="2026-04-29T14:23:07+00:00",
            promoted_by="manual",
        )
        assert "lamella-promoted-rule-id" not in out
        assert "lamella-promoted-ai-model" not in out

    def test_invalid_promoted_by_rejected(self):
        from lamella.features.bank_sync.writer import WriteError
        with pytest.raises(WriteError):
            render_staged_txn_promoted_directive(
                _make_entry(),
                promoted_at="2026-04-29T14:23:07+00:00",
                promoted_by="garbage",
            )


class TestWriterAppendStagedTxnDirectives:
    def _writer(self, tmp_path: Path):
        from lamella.features.bank_sync.writer import SimpleFINWriter
        main = tmp_path / "main.bean"
        sf = tmp_path / "simplefin_transactions.bean"
        main.write_text(_LEDGER_PRELUDE, encoding="utf-8")
        return SimpleFINWriter(
            main_bean=main, simplefin_path=sf,
            run_check=False,  # avoid bean-check for unit tests
        )

    def test_appends_one_directive_per_entry(self, tmp_path):
        writer = self._writer(tmp_path)
        entries = [
            _make_entry(simplefin_id="A1"),
            _make_entry(simplefin_id="B2"),
            _make_entry(simplefin_id="C3"),
        ]
        n = writer.append_staged_txn_directives(entries)
        assert n == 3
        body = writer.simplefin_path.read_text(encoding="utf-8")
        assert body.count('custom "staged-txn"') == 3

    def test_empty_input_is_noop(self, tmp_path):
        writer = self._writer(tmp_path)
        assert writer.append_staged_txn_directives([]) == 0

    def test_invalid_source_aborts_before_write(self, tmp_path):
        writer = self._writer(tmp_path)
        # Pre-create the file so the size baseline is meaningful
        writer.simplefin_path.parent.mkdir(parents=True, exist_ok=True)
        writer.simplefin_path.write_text("", encoding="utf-8")
        before = writer.simplefin_path.read_bytes()
        with pytest.raises(InvalidSourceError):
            writer.append_staged_txn_directives(
                [_make_entry()], source="manual",
            )
        # File is byte-identical to its pre-call state.
        assert writer.simplefin_path.read_bytes() == before
