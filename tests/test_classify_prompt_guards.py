# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Locks in the structural anti-hallucination framing in
classify_txn.j2.

Two live failures motivated the rewrite:

  * AI saw a day-note "I bought X from an online store for AcmeCo"
    and classified PayPal transfer fees on the same day as
    AcmeCo-online-store-related at 0.85.
  * AI saw a mileage entry "drove 13mi to a courier" and
    classified a same-day grocery charge as VEHICLE FUEL at 0.92.

Both bugs share a shape: the model bridged a gap between
unrelated context and the txn with a plausible-sounding story.
The fix is structural — direct evidence (memo on this txn,
receipt linked to this txn) gets to ESTABLISH classifications;
circumstantial context (day-notes, mileage, projects, similar
history) can only CORROBORATE a hypothesis that direct evidence
already supports.

These tests pin the structure so a future "clean up the prompt"
pass can't quietly weaken it.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from lamella.features.ai_cascade.classify import TxnForClassify
from lamella.features.ai_cascade.context import render


def _render(**overrides):
    txn = TxnForClassify(
        date=date(2026, 4, 18),
        amount=Decimal("42.00"),
        currency="USD",
        payee="Some Vendor",
        narration="something",
        card_account="Liabilities:Acme:Card:CardA1234",
        fixme_account="Expenses:FIXME",
        txn_hash="hash-test",
    )
    base = dict(
        txn=txn,
        similar=[],
        entity="Acme",
        accounts=["Expenses:Acme:Supplies"],
        accounts_by_entity={},
        registry_preamble="",
        active_notes=[],
        card_suspicion=None,
        receipt=None,
        mileage_entries=[],
        vehicle_density=[],
        account_descriptions={},
        entity_context=None,
        active_projects=[],
        fixme_root="Expenses",
    )
    base.update(overrides)
    return render("classify_txn.j2", **base)


def _flat(out: str) -> str:
    return " ".join(out.split()).lower()


# ---------- top-level evidence-tier framing ----------


def test_evidence_tiers_block_is_present_at_top():
    flat = _flat(_render())
    assert "evidence tiers" in flat
    assert "direct evidence" in flat
    assert "circumstantial context" in flat


def test_circumstantial_can_only_corroborate_not_create():
    """The load-bearing rule: circumstantial context cannot
    establish a classification on its own."""
    flat = _flat(_render())
    # The exact load-bearing language; if you change the wording,
    # update the test deliberately.
    assert "can only corroborate" in flat
    assert "cannot create a hypothesis" in flat


def test_no_specific_merchant_names_baked_into_prompt():
    """Specific brand names risk teaching brand-specific
    behavior. The principle-based prompt avoids them."""
    out = _render()
    for brand in ("Amazon", "Safeway", "Walmart", "Costco", "PayPal", "UPS"):
        assert brand not in out, (
            f"prompt should not name {brand!r} — keep guidance "
            "principle-based so the rule generalizes"
        )


# ---------- per-section provenance tags ----------


def test_notes_section_tags_each_note_by_provenance():
    """Each note line must surface DIRECT (memo on this txn) vs
    CIRCUMSTANTIAL (day-scoped) so the AI doesn't have to
    remember the rule — it's printed alongside the data."""
    from datetime import datetime
    from lamella.features.notes.service import NoteRow

    # One memo for this txn, one day-scoped note.
    memo = NoteRow(
        id=1,
        captured_at=datetime(2026, 4, 18, 12, 0),
        body="this is a memo about THIS txn",
        entity_hint=None, merchant_hint=None,
        resolved_txn=None, resolved_receipt=None,
        status="open",
        active_from=date(2026, 4, 18), active_to=date(2026, 4, 18),
        txn_hash="hash-test",  # ⭐ matches the rendered txn
    )
    day_note = NoteRow(
        id=2,
        captured_at=datetime(2026, 4, 18, 13, 0),
        body="something else from earlier today",
        entity_hint=None, merchant_hint=None,
        resolved_txn=None, resolved_receipt=None,
        status="open",
        active_from=date(2026, 4, 18), active_to=date(2026, 4, 18),
    )
    out = _render(active_notes=[memo, day_note])
    flat = _flat(out)
    # Both tags present.
    assert "[direct" in flat
    assert "[circumstantial" in flat
    # Memo tag is matched by hash, not just date proximity.
    assert "memo for this txn" in flat
    # The "belongs to whatever it was originally written about"
    # rule is the one that fixes the cross-merchant bridging.
    assert "belong to whatever" in flat or "background" in flat


def test_mileage_section_is_tagged_circumstantial():
    mileage = [{
        "entry_date": date(2026, 4, 18),
        "entity": "Personal",
        "vehicle": "Truck A",
        "miles": 13.0,
        "purpose": "errand",
        "from_loc": None,
        "to_loc": None,
        "notes": None,
    }]
    flat = _flat(_render(mileage_entries=mileage))
    assert "[circumstantial" in flat
    # Specifically: the log records miles, not purchases.
    assert "miles driven" in flat or "say nothing about what was purchased" in flat


def test_projects_section_is_tagged_circumstantial():
    proj = [{
        "display_name": "Kitchen Reno",
        "entity_slug": "Personal",
        "start_date": date(2026, 4, 1),
        "end_date": date(2026, 6, 30),
        "description": "renovating the kitchen",
        "budget_amount": "5000",
    }]
    flat = _flat(_render(active_projects=proj))
    assert "[circumstantial" in flat
    # Co-occurrence is not enough.
    assert "merchant + date match is not proof" in flat or "fits the project" in flat


def test_receipt_section_is_tagged_direct():
    """A receipt linked to this txn is direct evidence, even
    though it's circumstantial-feeling because it was OCR-pulled
    from a third-party system. The tag is what tells the model
    to weight it heavily."""

    class _R:
        confidence_note = "linked exact"
        vendor = "X"
        total = "10.00"
        receipt_date = date(2026, 4, 18)
        date_mismatch_note = None
        content_excerpt = "line item: thing"

    flat = _flat(_render(receipt=_R()))
    assert "[direct evidence" in flat


# ---------- absence-of-evidence inversion ----------


def test_absence_of_evidence_is_not_neutral_for_mileage():
    """Per user feedback: missing mileage on the txn date for a
    densely-logged vehicle should weaken vehicle-attribution, not
    be ignored. The prompt has to say so explicitly."""
    flat = _flat(_render())
    assert (
        "absence" in flat
        and ("evidence against" in flat
             or "weaken" in flat
             or "downgrade" in flat)
    )


# ---------- confidence gating ----------


def test_confidence_band_caps_circumstantial_only_reasoning():
    """Direct → can be high. Circumstantial-only → must be
    capped. This is the structural fix that turns confabulation
    into 'send to review' instead of confident wrong answers."""
    out = _render()
    flat = _flat(out)
    assert "confidence gating" in flat
    # The cap exists and uses a number — phrasing is allowed to
    # vary but the gate itself must remain.
    assert "0.50" in out or "0.5" in out
    # The "regardless of how plausible" line is doing real work.
    assert "regardless of how plausible" in flat
