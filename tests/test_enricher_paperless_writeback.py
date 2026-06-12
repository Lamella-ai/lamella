# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Slice A (auto-verify) + Slice C (auto-enrich)
wiring in AIFixmeEnricher. Exercises the private helpers that
translate classify context into Paperless writeback calls."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from lamella.features.ai_cascade.enricher import (
    _enrichment_note_from_context,
    _maybe_writeback_for_receipt,
)
from lamella.features.paperless_bridge.verify import (
    EnrichmentContext,
    VerifyHypothesis,
)


@dataclass
class _MileageStub:
    entry_date: date
    vehicle: str
    entity: str
    purpose: str | None = None


@dataclass
class _ReceiptStub:
    paperless_id: int
    date_mismatch_note: str | None = None


class TestEnrichmentNoteFromContext:
    def test_costco_fuel_pins_vehicle(self):
        """The motivating case: Warehouse Club fuel txn, mileage log has
        'drove to Warehouse Club for gas — 2009 Work SUV' the same
        day. Auto-enrichment composes a note that attributes the
        charge to the vehicle."""
        entries = [_MileageStub(
            entry_date=date(2026, 4, 17),
            vehicle="2009 Work SUV",
            entity="Personal",
            purpose="drove to Warehouse Club for gas",
        )]
        note, vehicle = _enrichment_note_from_context(
            target_account="Expenses:Personal:Vehicles:Work SUV:Fuel",
            mileage_entries=entries, active_notes=[],
            entity="Personal", txn_date=date(2026, 4, 17),
        )
        assert vehicle == "2009 Work SUV"
        assert "2009 Work SUV" in note
        assert "Personal" in note
        assert "Expenses:Personal:Vehicles:Work SUV:Fuel" in note

    def test_picks_closest_mileage_date_when_multiple(self):
        """Three mileage entries within the window; pick the one
        whose date matches the txn, not just whichever came first."""
        entries = [
            _MileageStub(
                entry_date=date(2026, 4, 16),
                vehicle="Work SUV", entity="Personal",
                purpose="gas",
            ),
            _MileageStub(
                entry_date=date(2026, 4, 17),
                vehicle="Cargo Van", entity="Acme",
                purpose="gas",
            ),
            _MileageStub(
                entry_date=date(2026, 4, 18),
                vehicle="Tractor", entity="FarmCo",
                purpose="gas",
            ),
        ]
        _, vehicle = _enrichment_note_from_context(
            target_account="Expenses:Acme:Fuel",
            mileage_entries=entries, active_notes=[],
            entity="Acme", txn_date=date(2026, 4, 17),
        )
        assert vehicle == "Cargo Van"

    def test_empty_context_returns_empty_note(self):
        note, vehicle = _enrichment_note_from_context(
            target_account="Expenses:FIXME",
            mileage_entries=[], active_notes=[],
            entity=None, txn_date=date(2026, 4, 17),
        )
        assert note == ""
        assert vehicle is None

    def test_entity_only_still_produces_note(self):
        """When we have entity but no mileage, still push a note
        so Paperless can surface 'this was classified for Acme'."""
        note, vehicle = _enrichment_note_from_context(
            target_account="Expenses:Acme:Office",
            mileage_entries=[], active_notes=[],
            entity="Acme", txn_date=date(2026, 4, 17),
        )
        assert "Acme" in note
        assert vehicle is None


@pytest.mark.asyncio
async def test_maybe_writeback_triggers_both_paths():
    """Slice A fires when date_mismatch_note is set; Slice C fires
    in parallel when mileage/entity context is available. They're
    independent — one can run without the other."""
    verify_service = AsyncMock()
    verify_service.verify_and_correct = AsyncMock()
    verify_service.verify_and_correct.return_value.changed_anything = True
    verify_service.enrich_with_context = AsyncMock()
    verify_service.enrich_with_context.return_value.note_added = True
    verify_service.enrich_with_context.return_value.tag_applied = True

    receipt = _ReceiptStub(
        paperless_id=42,
        date_mismatch_note=(
            "receipt date 2064-01-08 is likely an OCR error — "
            "transaction posted 2026-04-18"
        ),
    )
    mileage = [_MileageStub(
        entry_date=date(2026, 4, 17), vehicle="Work SUV",
        entity="Personal", purpose="gas",
    )]
    stats: dict[str, int] = {}

    await _maybe_writeback_for_receipt(
        verify_service=verify_service,
        receipt=receipt,
        txn_date=date(2026, 4, 18),
        ai_decision_id=42,
        target_account="Expenses:Personal:Vehicles:Work SUV:Fuel",
        mileage_entries=mileage, active_notes=[],
        entity="Personal",
        stats=stats,
    )
    verify_service.verify_and_correct.assert_awaited_once()
    call_kwargs = verify_service.verify_and_correct.await_args.kwargs
    assert isinstance(call_kwargs["hypothesis"], VerifyHypothesis)
    assert call_kwargs["hypothesis"].suspected_date == date(2026, 4, 18)

    verify_service.enrich_with_context.assert_awaited_once()
    enrich_kwargs = verify_service.enrich_with_context.await_args.kwargs
    ctx = enrich_kwargs["context"]
    assert isinstance(ctx, EnrichmentContext)
    assert ctx.vehicle == "Work SUV"
    assert ctx.entity == "Personal"

    assert stats.get("paperless_corrected") == 1
    assert stats.get("paperless_enriched") == 1


@pytest.mark.asyncio
async def test_maybe_writeback_no_mismatch_skips_verify():
    """If the linked receipt's date looks plausible (no mismatch
    note), verify does NOT fire — saves a vision call on the
    common case."""
    verify_service = AsyncMock()
    verify_service.verify_and_correct = AsyncMock()
    verify_service.enrich_with_context = AsyncMock()
    verify_service.enrich_with_context.return_value.note_added = True
    verify_service.enrich_with_context.return_value.tag_applied = True

    receipt = _ReceiptStub(paperless_id=42, date_mismatch_note=None)
    mileage = [_MileageStub(
        entry_date=date(2026, 4, 17), vehicle="Work SUV",
        entity="Personal",
    )]
    await _maybe_writeback_for_receipt(
        verify_service=verify_service,
        receipt=receipt, txn_date=date(2026, 4, 17),
        ai_decision_id=1,
        target_account="Expenses:Personal:Vehicles:Work SUV:Fuel",
        mileage_entries=mileage, active_notes=[],
        entity="Personal", stats={},
    )
    verify_service.verify_and_correct.assert_not_awaited()
    verify_service.enrich_with_context.assert_awaited_once()


@pytest.mark.asyncio
async def test_maybe_writeback_no_receipt_is_noop():
    verify_service = AsyncMock()
    await _maybe_writeback_for_receipt(
        verify_service=verify_service,
        receipt=_ReceiptStub(paperless_id=0),
        txn_date=date(2026, 4, 17),
        ai_decision_id=None,
        target_account="Expenses:FIXME",
        mileage_entries=[], active_notes=[], entity=None, stats={},
    )
    verify_service.verify_and_correct.assert_not_called()
    verify_service.enrich_with_context.assert_not_called()
