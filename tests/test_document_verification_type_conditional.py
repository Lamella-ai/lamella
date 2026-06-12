# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0061 Phase 3: type-conditional AI extraction.

Verifies that:
  * The renamed ``DocumentVerification`` schema parses fake AI
    responses cleanly and accepts the new optional fields
    (``po_number``, ``invoice_number``, ``due_date``, ``order_number``,
    ``ship_date``).
  * ``_build_user_prompt`` and ``_build_ocr_text_prompt`` emit
    type-specific extraction sections gated on the new
    ``document_type`` keyword-only parameter.
  * ``document_type=None`` produces the same shape as
    ``document_type='receipt'`` for backwards compat with
    pre-Phase-3 callers.
  * Statement and tax documents skip the receipt-specific spec and
    are flagged for exclusion from the auto-link path.
"""
from __future__ import annotations

from lamella.features.paperless_bridge.verify import (
    DocumentVerification,
    FieldConfidences,
    _build_ocr_text_prompt,
    _build_user_prompt,
)


# ------------------------------------------------------------------
# Schema parses with the new optional fields
# ------------------------------------------------------------------


class TestDocumentVerificationSchema:
    def test_receipt_shape_parses(self):
        """Receipt-shaped payload (the legacy default) parses without
        the new invoice/order fields."""
        dv = DocumentVerification(
            receipt_date="2026-04-17",
            vendor="Warehouse Club",
            total="58.12",
            subtotal="54.00",
            tax="4.12",
            tip="",
            confidence=FieldConfidences(
                receipt_date=0.95, vendor=0.92, total=0.99,
                subtotal=0.90, tax=0.88,
            ),
        )
        # New fields default to empty string.
        assert dv.po_number == ""
        assert dv.invoice_number == ""
        assert dv.due_date == ""
        assert dv.order_number == ""
        assert dv.ship_date == ""

    def test_invoice_shape_parses(self):
        dv = DocumentVerification(
            receipt_date="2026-04-17",
            vendor="Acme Industrial",
            total="1248.00",
            po_number="PO-99812",
            invoice_number="INV-2026-0419",
            due_date="2026-05-17",
        )
        assert dv.po_number == "PO-99812"
        assert dv.invoice_number == "INV-2026-0419"
        assert dv.due_date == "2026-05-17"
        # Receipt-only fields can stay empty for invoices.
        assert dv.tip == ""
        assert dv.subtotal == ""

    def test_order_shape_parses(self):
        dv = DocumentVerification(
            receipt_date="2026-04-17",
            vendor="Example LLC",
            total="412.00",
            order_number="SO-44901",
            ship_date="2026-04-22",
        )
        assert dv.order_number == "SO-44901"
        assert dv.ship_date == "2026-04-22"

    def test_unknown_keys_ignored(self):
        """Cached decisions on disk that carry retired keys still
        deserialize cleanly. Pydantic ignores unknown keys by
        default, so payment_last_four (removed per ADR-0044) doesn't
        break."""
        dv = DocumentVerification.model_validate({
            "receipt_date": "2026-04-17",
            "vendor": "Warehouse Club",
            "total": "58.12",
            "payment_last_four": "1234",  # ignored
        })
        assert dv.vendor == "Warehouse Club"


# ------------------------------------------------------------------
# Vision-tier prompt: type-conditional sections
# ------------------------------------------------------------------


def _vision_prompt(document_type: str | None) -> str:
    return _build_user_prompt(
        current={"vendor": "Acme", "total": "10.00", "receipt_date": "2026-04-17"},
        content_excerpt="Some OCR text here",
        hypothesis=None,
        source_type="image",
        page_count=1,
        document_type=document_type,
    )


def _ocr_prompt(document_type: str | None) -> str:
    return _build_ocr_text_prompt(
        current={"vendor": "Acme", "total": "10.00", "receipt_date": "2026-04-17"},
        content_excerpt="Some OCR text here",
        hypothesis=None,
        document_type=document_type,
    )


class TestVisionPromptTypeConditional:
    def test_receipt_prompt_contains_subtotal_and_tip(self):
        body = _vision_prompt("receipt")
        assert "subtotal" in body
        assert "tip" in body
        # Receipt prompts should NOT carry invoice / order fields.
        assert "po_number" not in body
        assert "due_date" not in body
        assert "order_number" not in body
        assert "ship_date" not in body

    def test_invoice_prompt_contains_po_number_and_due_date(self):
        body = _vision_prompt("invoice")
        assert "po_number" in body
        assert "invoice_number" in body
        assert "due_date" in body
        # Receipt-only fields aren't included in the invoice spec.
        assert "tip" not in body

    def test_order_prompt_contains_order_number_and_ship_date(self):
        body = _vision_prompt("order")
        assert "order_number" in body
        assert "ship_date" in body
        assert "tip" not in body
        assert "po_number" not in body

    def test_statement_prompt_omits_receipt_specific_fields(self):
        body = _vision_prompt("statement")
        # Statement prompt explicitly tells the model to LEAVE the
        # receipt/invoice monetary subfields empty — the words appear
        # only inside the "leave ... empty" instruction, not in any
        # extract-this directive. Verify both: the leave-empty clause
        # is present, and the subtotal/tip never appear as fields the
        # model is told to extract.
        assert "leave subtotal" in body
        assert "tip" in body  # only in the leave-empty list
        assert "po_number" in body  # also only in the leave-empty list
        # No "extract subtotal" / "subtotal:" directive — the
        # receipt-spec line is omitted entirely for statements.
        assert "plus the receipt-specific fields" not in body
        assert "plus the invoice-specific fields" not in body
        assert "plus the order-specific fields" not in body
        # Statement extraction must explicitly note auto-link
        # exclusion so the upstream caller knows this isn't a
        # candidate.
        assert "not auto-linked" in body

    def test_tax_prompt_omits_receipt_specific_fields(self):
        body = _vision_prompt("tax")
        assert "leave subtotal" in body
        assert "plus the receipt-specific fields" not in body
        assert "plus the invoice-specific fields" not in body
        assert "not auto-linked" in body

    def test_none_falls_back_to_receipt_shape(self):
        none_body = _vision_prompt(None)
        receipt_body = _vision_prompt("receipt")
        assert none_body == receipt_body

    def test_receipt_label_in_prompt(self):
        """The doc-type noun appears in the prompt body so the AI
        knows what kind of document it's reading."""
        receipt_body = _vision_prompt("receipt")
        invoice_body = _vision_prompt("invoice")
        assert "receipt" in receipt_body.lower()
        assert "invoice" in invoice_body.lower()


class TestOcrTextPromptTypeConditional:
    def test_receipt_ocr_prompt_contains_subtotal_and_tip(self):
        body = _ocr_prompt("receipt")
        assert "subtotal" in body
        assert "tip" in body
        assert "po_number" not in body

    def test_invoice_ocr_prompt_contains_po_number(self):
        body = _ocr_prompt("invoice")
        assert "po_number" in body
        assert "invoice_number" in body
        assert "due_date" in body
        assert "tip" not in body

    def test_order_ocr_prompt_contains_order_number(self):
        body = _ocr_prompt("order")
        assert "order_number" in body
        assert "ship_date" in body

    def test_statement_ocr_prompt_omits_receipt_fields(self):
        body = _ocr_prompt("statement")
        # Receipt-spec / invoice-spec / order-spec lines must be
        # omitted entirely for statements; the only mention of
        # subtotal/tip/po_number is inside the leave-empty clause.
        assert "plus the receipt-specific fields" not in body
        assert "plus the invoice-specific fields" not in body
        assert "plus the order-specific fields" not in body
        assert "leave subtotal" in body
        # Text-only tier never re-extracts content — instruction
        # must persist regardless of document_type.
        assert "Leave corrected_content empty" in body

    def test_none_ocr_prompt_matches_receipt(self):
        none_body = _ocr_prompt(None)
        receipt_body = _ocr_prompt("receipt")
        assert none_body == receipt_body

    def test_corrected_content_stays_empty_in_text_tier(self):
        """Text-only pass never re-extracts content regardless of
        document_type — the model only sees already-OCR'd text."""
        for dt in (None, "receipt", "invoice", "order", "statement", "tax"):
            body = _ocr_prompt(dt)
            assert "Leave corrected_content empty" in body
