# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for the Paperless verify-and-writeback service (Slice A/C)."""
from __future__ import annotations

import json
from decimal import Decimal
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx

from lamella.features.ai_cascade.service import AIService
from lamella.core.config import Settings
from lamella.core.db import connect, migrate
from lamella.adapters.paperless.client import PaperlessClient
from lamella.features.paperless_bridge.verify import (
    EnrichmentContext,
    FieldConfidences,
    DocumentVerification,
    VerifyDiff,
    VerifyHypothesis,
    VerifyService,
    _compute_diffs,
    _current_fields_dict,
    _enrichment_dedup_key,
    _fields_to_patch,
    _ocr_extraction_quality,
    _render_verify_note,
    classify_pdf_bytes,
    receipt_source_type,
)


# ------------------------------------------------------------------
# Pure-helper tests (fast, no network)
# ------------------------------------------------------------------


class TestSourceType:
    def test_image_mimes(self):
        assert receipt_source_type("image/jpeg") == "image"
        assert receipt_source_type("image/png") == "image"
        assert receipt_source_type("image/heic") == "image"

    def test_pdf_is_ocr(self):
        assert receipt_source_type("application/pdf") == "ocr_pdf"

    def test_native_text_mimes(self):
        assert receipt_source_type("text/plain") == "native_text"
        assert receipt_source_type(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ) == "native_text"
        assert receipt_source_type("message/rfc822") == "native_text"

    def test_charset_suffix_stripped(self):
        assert receipt_source_type("image/png; charset=utf-8") == "image"

    def test_unknown_falls_through(self):
        assert receipt_source_type("application/octet-stream") == "unknown"
        assert receipt_source_type(None) == "unknown"


class TestComputeDiffs:
    def test_no_diff_when_values_match(self):
        current = {
            "title": "Warehouse Club", "vendor": "Warehouse Club",
            "receipt_date": "2026-04-17",
            "total": "58.12", "subtotal": None,
            "tax": None, "payment_last_four": None,
        }
        extracted = DocumentVerification(
            vendor="Warehouse Club",
            receipt_date="2026-04-17",
            total="58.12",
            confidence=FieldConfidences(vendor=0.99, receipt_date=0.99, total=0.99),
        )
        assert _compute_diffs(current, extracted) == []

    def test_date_correction_yields_diff(self):
        """The Warehouse Club-2064 case: receipt_date in Paperless is junk,
        AI re-extracts the real date."""
        current = {
            "title": "Warehouse Club", "vendor": "Warehouse Club",
            "receipt_date": "2064-01-08",
            "total": "58.12", "subtotal": None, "tax": None,
            "payment_last_four": None,
        }
        extracted = DocumentVerification(
            vendor="Warehouse Club",
            receipt_date="2026-04-17",
            total="58.12",
            confidence=FieldConfidences(receipt_date=0.95, total=0.98, vendor=0.99),
        )
        diffs = _compute_diffs(current, extracted)
        field_names = [d.field for d in diffs]
        assert "receipt_date" in field_names
        date_diff = next(d for d in diffs if d.field == "receipt_date")
        assert date_diff.before == "2064-01-08"
        assert date_diff.after == "2026-04-17"
        assert date_diff.confidence == 0.95

    def test_none_extracted_fields_are_skipped(self):
        """Null extracted values don't generate diffs — we don't
        know, so we don't claim the current value is wrong."""
        current = {
            "title": "Warehouse Club", "vendor": "Warehouse Club",
            "receipt_date": None, "total": "58.12",
            "subtotal": None, "tax": None, "payment_last_four": None,
        }
        extracted = DocumentVerification()
        assert _compute_diffs(current, extracted) == []


class TestFieldsToPatch:
    def test_confident_diff_is_applied(self):
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            confidence=FieldConfidences(receipt_date=0.95),
        )
        diffs = [VerifyDiff(
            field="receipt_date", before="2064-01-08",
            after="2026-04-17", confidence=0.95,
        )]
        patch, applied = _fields_to_patch(extracted, diffs, threshold=0.80)
        assert patch == {"receipt_date": "2026-04-17"}
        assert applied == ["receipt_date"]

    def test_low_confidence_diff_is_held(self):
        """Field confidence below threshold = diff is shown in the
        audit log but NOT patched to Paperless. Protects against
        the model hallucinating corrections."""
        extracted = DocumentVerification(
            vendor="MaybeWarehouse Club",
            confidence=FieldConfidences(vendor=0.50),
        )
        diffs = [VerifyDiff(
            field="vendor", before="Warehouse Club",
            after="MaybeWarehouse Club", confidence=0.50,
        )]
        patch, applied = _fields_to_patch(extracted, diffs, threshold=0.80)
        assert patch == {}
        assert applied == []

    def test_fields_of_interest_restricts_patch(self):
        """When ``fields_of_interest`` is set, fields NOT in the set
        are dropped from the patch even if Tier 1 extracted them at
        high confidence. Use case: date-only verify must not
        accidentally PATCH vendor or total."""
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            vendor="Warehouse Club Corrected",
            total="58.12",
            confidence=FieldConfidences(
                receipt_date=0.95, vendor=0.99, total=0.97,
            ),
        )
        diffs = [
            VerifyDiff(
                field="receipt_date", before="2064-01-08",
                after="2026-04-17", confidence=0.95,
            ),
            VerifyDiff(
                field="vendor", before="Warehouse Club",
                after="Warehouse Club Corrected", confidence=0.99,
            ),
            VerifyDiff(
                field="total", before="58.13",
                after="58.12", confidence=0.97,
            ),
        ]
        patch, applied = _fields_to_patch(
            extracted, diffs, threshold=0.80,
            fields_of_interest=("receipt_date",),
        )
        assert patch == {"receipt_date": "2026-04-17"}
        assert applied == ["receipt_date"]

    def test_fields_of_interest_skips_correspondent_when_vendor_out_of_scope(
        self,
    ):
        """The vendor-driven correspondent + title side-effects must
        be suppressed when vendor isn't in the field-of-interest set
        — otherwise a date-only verify would still rewrite the
        correspondent."""
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            vendor="Warehouse Club",
            confidence=FieldConfidences(
                receipt_date=0.95, vendor=0.99,
            ),
        )
        diffs = [VerifyDiff(
            field="receipt_date", before="2064-01-08",
            after="2026-04-17", confidence=0.95,
        )]
        patch, applied = _fields_to_patch(
            extracted, diffs, threshold=0.80,
            current_correspondent="Some Other Vendor",
            current_title="scan_001.pdf",
            fields_of_interest=("receipt_date",),
        )
        # Date is patched, but correspondent (vendor-driven) and title
        # (also vendor-driven) are NOT.
        assert patch == {"receipt_date": "2026-04-17"}
        assert "correspondent_name" not in patch
        assert "title" not in patch


class TestOcrExtractionQuality:
    """The Tier-1→Tier-2 escalation gate. Vision is expensive, so the
    rules for when to escalate are critical to cost control."""

    def test_default_averages_all_canonical_fields(self):
        """No fields_of_interest: average is taken across every
        canonical field with a positive confidence."""
        extracted = DocumentVerification(
            receipt_date="2026-04-17", vendor="X", total="10.00",
            confidence=FieldConfidences(
                receipt_date=0.90, vendor=0.30, total=0.30,
            ),
        )
        avg, needs_vision, _ = _ocr_extraction_quality(extracted)
        # avg = (0.90 + 0.30 + 0.30) / 3 = 0.50
        assert abs(avg - 0.50) < 0.001
        # Caller compares against OCR_ONLY_MIN_AVG_CONFIDENCE; the
        # gate function itself only flips needs_vision on model self-
        # flag or vendor mismatch.
        assert needs_vision is False

    def test_fields_of_interest_narrows_average(self):
        """fields_of_interest=(receipt_date,): only the date's
        confidence counts. A blurry vendor or total no longer drags
        the average down to force escalation."""
        extracted = DocumentVerification(
            receipt_date="2026-04-17", vendor="X", total="10.00",
            confidence=FieldConfidences(
                receipt_date=0.95, vendor=0.20, total=0.20,
            ),
        )
        avg, needs_vision, reason = _ocr_extraction_quality(
            extracted, fields_of_interest=("receipt_date",),
        )
        # Only receipt_date counts → avg = 0.95
        assert abs(avg - 0.95) < 0.001
        assert needs_vision is False
        assert reason is None

    def test_vendor_mismatch_suppressed_when_vendor_out_of_scope(self):
        """The vendor-disagreement-with-hypothesis escalation reason
        only fires when vendor is in scope. Otherwise a date-only
        verify would pay for vision because the user happens to have
        a vendor hypothesis the OCR can't match."""
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            vendor="WAREHOUSE",
            confidence=FieldConfidences(receipt_date=0.95, vendor=0.40),
        )
        hypo = VerifyHypothesis(suspected_vendor="Acme Hardware")
        # Without scope narrowing → vendor mismatch escalates.
        _, needs_vision_full, reason_full = _ocr_extraction_quality(
            extracted, hypothesis=hypo,
        )
        assert needs_vision_full is True
        assert "WAREHOUSE" in (reason_full or "")
        # With date-only scope → mismatch is irrelevant, no escalation.
        _, needs_vision_narrow, reason_narrow = _ocr_extraction_quality(
            extracted,
            hypothesis=hypo,
            fields_of_interest=("receipt_date",),
        )
        assert needs_vision_narrow is False
        assert reason_narrow is None

    def test_model_self_flag_still_escalates_in_narrow_scope(self):
        """Even in date-only scope, NEEDS_VISION on the date itself
        (or any reason) still escalates — the cheap path is only
        cheap when Tier 1 is confident about the field we asked for."""
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            confidence=FieldConfidences(receipt_date=0.40),
            ocr_errors_noted=["NEEDS_VISION: date is partially obscured"],
        )
        _, needs_vision, reason = _ocr_extraction_quality(
            extracted, fields_of_interest=("receipt_date",),
        )
        assert needs_vision is True
        assert "NEEDS_VISION" in (reason or "")


class TestPDFClassification:
    """PyMuPDF-based detection that tells us (a) whether the
    source is a native PDF (skip verify) or an OCR'd scan (verify
    against page image), and (b) the page count."""

    def _build_text_pdf(self, text: str, *, pages: int = 1) -> bytes:
        """Produce a synthetic PDF with `text` on page 1 via PyMuPDF —
        guarantees a text layer."""
        import pymupdf
        doc = pymupdf.open()
        for i in range(pages):
            page = doc.new_page()
            page.insert_text((72, 72), text if i == 0 else f"page {i + 1}")
        buf = doc.tobytes()
        doc.close()
        return buf

    def _build_image_pdf(self, *, pages: int = 1) -> bytes:
        """A PDF with no text layer — pymupdf page.get_text() returns
        '' on these. Simulates a scanned receipt."""
        import pymupdf
        doc = pymupdf.open()
        for _ in range(pages):
            doc.new_page()
        buf = doc.tobytes()
        doc.close()
        return buf

    def test_native_pdf_detected(self):
        """A vendor-generated PDF with an embedded text layer
        classifies as native_pdf — no OCR was ever needed and
        verify against an image would be wasted tokens."""
        pdf = self._build_text_pdf(
            "ACME Corp Invoice\nTotal: $142.00\nDate: 2026-04-17\n"
            "Thank you for your business."
        )
        source_type, page_count = classify_pdf_bytes(pdf)
        assert source_type == "native_pdf"
        assert page_count == 1

    def test_image_pdf_classified_as_ocr(self):
        """A scanned PDF has no text layer. classify_pdf_bytes
        returns ocr_pdf so the caller sends it to vision."""
        pdf = self._build_image_pdf(pages=1)
        source_type, page_count = classify_pdf_bytes(pdf)
        assert source_type == "ocr_pdf"
        assert page_count == 1

    def test_multi_page_count_reported(self):
        """Multi-page PDFs surface page_count so the audit note
        can say "only page 1 verified"."""
        pdf = self._build_text_pdf("Full contract text...", pages=5)
        source_type, page_count = classify_pdf_bytes(pdf)
        assert page_count == 5

    def test_corrupt_bytes_return_unknown(self):
        st, n = classify_pdf_bytes(b"not a pdf at all")
        assert st == "unknown"
        assert n == 0

    def test_tiny_text_layer_still_ocr(self):
        """A PDF with only a few chars on page 1 — well under the
        MIN_NATIVE_TEXT_CHARS threshold — should not be treated as
        native. This protects against Paperless-stamped metadata
        pretending to be content."""
        pdf = self._build_text_pdf("x")
        source_type, _ = classify_pdf_bytes(pdf)
        assert source_type == "ocr_pdf"


class TestMultiPageAuditNote:
    """The safety contract: when we only see page 1 of a multi-
    page PDF, the audit note explicitly says so, and the patch
    surface never touches OCR text fields."""

    def test_single_page_no_warning(self):
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            confidence=FieldConfidences(receipt_date=0.95),
        )
        diffs = [VerifyDiff(
            field="receipt_date", before="2064-01-08",
            after="2026-04-17", confidence=0.95,
        )]
        note = _render_verify_note(extracted, diffs, 1, page_count=1)
        assert "page 1" not in note
        assert "Only page 1" not in note

    def test_multi_page_warns_about_scope(self):
        extracted = DocumentVerification(
            receipt_date="2026-04-17",
            confidence=FieldConfidences(receipt_date=0.95),
        )
        diffs = [VerifyDiff(
            field="receipt_date", before="2064-01-08",
            after="2026-04-17", confidence=0.95,
        )]
        note = _render_verify_note(extracted, diffs, 1, page_count=3)
        assert "Only page 1" in note
        assert "3-page" in note
        assert "NOT modified or overwritten" in note


class TestDedupKey:
    def test_same_context_same_key(self):
        c1 = EnrichmentContext(
            vehicle="2009 Work SUV", entity="Personal",
            note_body="Gas for 2009 Work SUV",
        )
        c2 = EnrichmentContext(
            vehicle="2009 Work SUV", entity="Personal",
            note_body="Gas for 2009 Work SUV",
        )
        assert _enrichment_dedup_key(c1) == _enrichment_dedup_key(c2)

    def test_different_vehicle_different_key(self):
        c1 = EnrichmentContext(
            vehicle="2009 Work SUV", note_body="Gas",
        )
        c2 = EnrichmentContext(
            vehicle="Acme Cargo Van", note_body="Gas",
        )
        assert _enrichment_dedup_key(c1) != _enrichment_dedup_key(c2)


# ------------------------------------------------------------------
# Service-level tests (with mocked httpx routes)
# ------------------------------------------------------------------


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


def _seed_doc(db, *, paperless_id: int = 42, mime_type: str = "image/jpeg",
              receipt_date: str = "2064-01-08", total: str = "58.12",
              vendor: str = "Warehouse Club", content: str = "Warehouse Club Wholesale\nFuel\n"):
    # ADR-0061 Phase 2: column ``receipt_date`` was renamed to ``document_date``.
    # The local kwarg name stays for test-call ergonomics.
    db.execute(
        """
        INSERT INTO paperless_doc_index (
            paperless_id, title, vendor, total_amount, document_date,
            content_excerpt, mime_type, tags_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (paperless_id, "Warehouse Club Receipt", vendor, total, receipt_date,
         content, mime_type, "[1,2]"),
    )


def _enable_writeback(db):
    db.execute(
        "INSERT OR REPLACE INTO app_settings (key, value) "
        "VALUES ('paperless_writeback_enabled', '1')"
    )


def _make_ai(db, settings):
    return AIService(settings=settings, conn=db)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=tmp_path / "ledger",
        paperless_url="https://paperless.test",
        paperless_api_token="tok",
        openrouter_api_key="sk-test",
        paperless_writeback_enabled=False,  # tests flip via app_settings
    )


async def test_verify_skips_native_pdf_without_vision_call(db, settings):
    """A PDF with a real text layer skips the vision call entirely —
    the content was never OCR'd so there's nothing to verify. Saves
    the expensive vision token cost on vendor-generated invoices."""
    import pymupdf
    doc_pdf = pymupdf.open()
    page = doc_pdf.new_page()
    page.insert_text((72, 72),
                     "ACME Corp Invoice\nTotal: $142.00\n"
                     "Date: 2026-04-17\nThank you for your business.")
    pdf_bytes = doc_pdf.tobytes()
    doc_pdf.close()

    _seed_doc(db, mime_type="application/pdf")
    _enable_writeback(db)
    ai = _make_ai(db, settings)

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock, respx.mock(
        base_url="https://openrouter.ai/api/v1", assert_all_called=False,
    ) as or_mock:
        mock.get("/api/documents/42/download/").respond(
            200, content=pdf_bytes,
            headers={"content-type": "application/pdf"},
        )
        or_route = or_mock.post("/chat/completions")
        async with PaperlessClient(
            "https://paperless.test", "tok",
        ) as paperless:
            svc = VerifyService(ai=ai, paperless=paperless, conn=db)
            outcome = await svc.verify_and_correct(42)
    assert outcome.source_type == "native_pdf"
    assert outcome.verified is False
    assert "native PDF" in (outcome.skipped_reason or "")
    # Vision call was never made.
    assert or_route.call_count == 0


async def test_verify_skips_native_text(db, settings):
    _seed_doc(db, mime_type="text/plain")
    _enable_writeback(db)
    ai = _make_ai(db, settings)
    async with PaperlessClient("https://paperless.test", "tok") as paperless:
        svc = VerifyService(ai=ai, paperless=paperless, conn=db)
        outcome = await svc.verify_and_correct(42)
    assert outcome.source_type == "native_text"
    assert outcome.verified is False
    assert "native-text" in (outcome.skipped_reason or "")


async def test_verify_skips_when_writeback_disabled(db, settings):
    _seed_doc(db, mime_type="image/jpeg")
    # writeback NOT enabled in app_settings, and config default is False.
    ai = _make_ai(db, settings)

    with respx.mock(
        base_url="https://paperless.test", assert_all_called=False,
    ) as mock:
        mock.get("/api/documents/42/download/").respond(
            200, content=b"fakeimage", content_type="image/jpeg",
        )
        classify_payload = {
            "id": "x", "model": "anthropic/claude-opus-4.7",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": json.dumps({
                        "receipt_date": "2026-04-17",
                        "vendor": "Warehouse Club",
                        "total": "58.12",
                        "confidence": {"receipt_date": 0.95, "total": 0.98,
                                       "vendor": 0.99},
                        "ocr_errors_noted": ["date OCR'd as 2064"],
                        "reasoning": "Year digit misread.",
                    }),
                },
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 500, "completion_tokens": 80},
        }
        with respx.mock(
            base_url="https://openrouter.ai/api/v1",
            assert_all_called=False,
        ) as or_mock:
            or_mock.post("/chat/completions").respond(200, json=classify_payload)
            async with PaperlessClient(
                "https://paperless.test", "tok",
            ) as paperless:
                svc = VerifyService(ai=ai, paperless=paperless, conn=db)
                outcome = await svc.verify_and_correct(42)
    # The vision call should have run (we're verified), but writeback
    # is off so nothing was patched.
    assert outcome.verified is True
    assert outcome.skipped_reason == "writeback disabled in settings"
    assert outcome.fields_patched == 0


async def test_verify_happy_path_patches_corrections(db, settings):
    """End-to-end: OCR says 2064, vision says 2026-04-17, we PATCH
    the corrected date onto the Paperless doc and stamp the tag."""
    _seed_doc(db, mime_type="image/jpeg", receipt_date="2064-01-08")
    _enable_writeback(db)
    ai = _make_ai(db, settings)

    patched_body = {}
    classify_payload = {
        "id": "x", "model": "anthropic/claude-opus-4.7",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": json.dumps({
                    "receipt_date": "2026-04-17",
                    "vendor": "Warehouse Club",
                    "total": "58.12",
                    "confidence": {
                        "receipt_date": 0.95, "total": 0.98, "vendor": 0.99,
                    },
                    "ocr_errors_noted": ["date OCR'd as 2064"],
                    "reasoning": "Year digit misread.",
                }),
            },
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 500, "completion_tokens": 80},
    }

    def _capture_patch(request):
        patched_body.update(json.loads(request.read()))
        return httpx.Response(200, json={"id": 42})

    with respx.mock(base_url="https://paperless.test") as paperless_mock, \
         respx.mock(base_url="https://openrouter.ai/api/v1") as or_mock:
        paperless_mock.get("/api/documents/42/download/").respond(
            200, content=b"fakeimage",
            headers={"content-type": "image/jpeg"},
        )
        paperless_mock.get("/api/tags/").respond(
            200, json={"next": None, "results": [
                {"id": 1, "name": "Receipts"},
            ]},
        )
        paperless_mock.post("/api/tags/").respond(
            201, json={"id": 99, "name": "Lamella Fixed"},
        )
        paperless_mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [1, 2], "custom_fields": []},
        )
        paperless_mock.patch("/api/documents/42/").mock(side_effect=_capture_patch)
        paperless_mock.post("/api/documents/42/notes/").respond(
            200, json={"id": 7},
        )
        or_mock.post("/chat/completions").respond(200, json=classify_payload)

        async with PaperlessClient(
            "https://paperless.test", "tok",
        ) as paperless:
            svc = VerifyService(ai=ai, paperless=paperless, conn=db)
            outcome = await svc.verify_and_correct(
                42,
                hypothesis=VerifyHypothesis(
                    suspected_date=date(2026, 4, 18),
                    reason="Posted 2026-04-18; OCR'd as 2064-01-08",
                ),
            )

    assert outcome.verified is True
    assert outcome.fields_patched >= 1
    assert outcome.tag_applied is True
    assert outcome.note_added is True
    # The patch body must include the corrected date (receipt_date
    # → created) AND the merged tag list including the new tag id.
    assert patched_body.get("created") == "2026-04-17"
    assert 99 in patched_body.get("tags", [])
    # Writeback log captured the correction.
    row = db.execute(
        "SELECT kind, paperless_id FROM paperless_writeback_log"
    ).fetchone()
    assert row is not None
    assert row["kind"] == "verify_correction"
    assert row["paperless_id"] == 42
    # Local index was updated so next classify sees 2026-04-17.
    # ADR-0061 Phase 2: column ``receipt_date`` was renamed to ``document_date``.
    updated = db.execute(
        "SELECT document_date FROM paperless_doc_index WHERE paperless_id = 42"
    ).fetchone()
    assert updated["document_date"] == "2026-04-17"


async def test_verify_patches_content_on_single_page(db, settings):
    """When the doc is single-page + diffs fired + the model
    returned corrected_content, that content is PATCHed onto
    Paperless's content field AND mirrored to our local
    content_excerpt."""
    _seed_doc(db, mime_type="image/jpeg", receipt_date="2064-01-08")
    _enable_writeback(db)
    ai = _make_ai(db, settings)

    patched_body: dict = {}
    classify_payload = {
        "id": "x", "model": "anthropic/claude-opus-4.7",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": json.dumps({
                    "receipt_date": "2026-04-17",
                    "vendor": "Warehouse Club",
                    "total": "58.12",
                    "corrected_content": (
                        "Warehouse Club Wholesale\n"
                        "04/17/2026\n"
                        "Regular Fuel 15.2gal $58.12\n"
                        "TOTAL $58.12"
                    ),
                    "confidence": {
                        "receipt_date": 0.95, "total": 0.98, "vendor": 0.99,
                    },
                    "ocr_errors_noted": ["date misread"],
                    "reasoning": "Fixed.",
                }),
            },
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 500, "completion_tokens": 80},
    }

    def _capture_patch(request):
        patched_body.update(json.loads(request.read()))
        return httpx.Response(200, json={"id": 42})

    with respx.mock(base_url="https://paperless.test") as paperless_mock, \
         respx.mock(base_url="https://openrouter.ai/api/v1") as or_mock:
        paperless_mock.get("/api/documents/42/download/").respond(
            200, content=b"fakeimage",
            headers={"content-type": "image/jpeg"},
        )
        paperless_mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        paperless_mock.post("/api/tags/").respond(
            201, json={"id": 99, "name": "Lamella Fixed"},
        )
        paperless_mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [1], "custom_fields": []},
        )
        paperless_mock.patch("/api/documents/42/").mock(side_effect=_capture_patch)
        paperless_mock.post("/api/documents/42/notes/").respond(
            200, json={"id": 7},
        )
        or_mock.post("/chat/completions").respond(200, json=classify_payload)
        async with PaperlessClient("https://paperless.test", "tok") as paperless:
            svc = VerifyService(ai=ai, paperless=paperless, conn=db)
            await svc.verify_and_correct(42)

    # Paperless was told to replace the content field.
    assert "content" in patched_body
    assert "TOTAL $58.12" in patched_body["content"]
    # Local content_excerpt mirrored.
    row = db.execute(
        "SELECT content_excerpt FROM paperless_doc_index WHERE paperless_id = 42"
    ).fetchone()
    assert "TOTAL $58.12" in row["content_excerpt"]


async def test_verify_does_not_patch_content_on_multi_page(db, settings):
    """Multi-page PDF safety: even if the model returns
    corrected_content, we NEVER overwrite Paperless's content
    field — doing so would destroy pages 2+. Structured fields
    and tag still get stamped."""
    import pymupdf
    doc_pdf = pymupdf.open()
    for _ in range(3):
        doc_pdf.new_page()  # image-only pages (no text layer)
    pdf_bytes = doc_pdf.tobytes()
    doc_pdf.close()

    _seed_doc(db, mime_type="application/pdf", receipt_date="2064-01-08")
    _enable_writeback(db)
    ai = _make_ai(db, settings)

    patched_body: dict = {}
    classify_payload = {
        "id": "x", "model": "anthropic/claude-opus-4.7",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": json.dumps({
                    "receipt_date": "2026-04-17",
                    "vendor": "Warehouse Club",
                    "total": "58.12",
                    # Model ignored instructions and returned content
                    # anyway — caller must still refuse to patch it.
                    "corrected_content": "PAGE 1 ONLY\nthis would truncate",
                    "confidence": {
                        "receipt_date": 0.95, "total": 0.98, "vendor": 0.99,
                    },
                    "ocr_errors_noted": [],
                    "reasoning": "Fixed.",
                }),
            },
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 500, "completion_tokens": 80},
    }

    def _capture_patch(request):
        patched_body.update(json.loads(request.read()))
        return httpx.Response(200, json={"id": 42})

    with respx.mock(base_url="https://paperless.test") as paperless_mock, \
         respx.mock(base_url="https://openrouter.ai/api/v1") as or_mock:
        paperless_mock.get("/api/documents/42/download/").respond(
            200, content=pdf_bytes,
            headers={"content-type": "application/pdf"},
        )
        # Thumbnail is no longer fetched: the verify pipeline renders
        # page 1 locally via PyMuPDF from the already-downloaded PDF
        # bytes (Paperless's /thumb/ endpoint sometimes returns a
        # generic placeholder icon).
        paperless_mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        paperless_mock.post("/api/tags/").respond(
            201, json={"id": 99, "name": "Lamella Fixed"},
        )
        paperless_mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [1], "custom_fields": []},
        )
        paperless_mock.patch("/api/documents/42/").mock(side_effect=_capture_patch)
        paperless_mock.post("/api/documents/42/notes/").respond(
            200, json={"id": 7},
        )
        or_mock.post("/chat/completions").respond(200, json=classify_payload)
        async with PaperlessClient("https://paperless.test", "tok") as paperless:
            svc = VerifyService(ai=ai, paperless=paperless, conn=db)
            outcome = await svc.verify_and_correct(42)

    assert outcome.is_multi_page
    # THE critical assertion: content field NOT in the patch body.
    assert "content" not in patched_body
    # Structured fields still patched (date went to `created`).
    assert patched_body.get("created") == "2026-04-17"
    # Local content_excerpt was NOT touched.
    row = db.execute(
        "SELECT content_excerpt FROM paperless_doc_index WHERE paperless_id = 42"
    ).fetchone()
    assert "PAGE 1 ONLY" not in (row["content_excerpt"] or "")


async def test_enrich_adds_note_and_tag(db, settings):
    """Slice C: when classify context supplies vehicle/entity info,
    push a note + Lamella Enriched tag to the Paperless doc."""
    _seed_doc(db, mime_type="image/jpeg")
    _enable_writeback(db)
    ai = _make_ai(db, settings)

    posted_note = {}
    patched_tags = {}

    def _capture_note(request):
        posted_note.update(json.loads(request.read()))
        return httpx.Response(200, json={"id": 1, "note": "x"})

    def _capture_tag_patch(request):
        patched_tags.update(json.loads(request.read()))
        return httpx.Response(200, json={"id": 42})

    with respx.mock(base_url="https://paperless.test") as paperless_mock:
        paperless_mock.post("/api/documents/42/notes/").mock(side_effect=_capture_note)
        paperless_mock.get("/api/tags/").respond(
            200, json={"next": None, "results": []},
        )
        paperless_mock.post("/api/tags/").respond(
            201, json={"id": 88, "name": "Lamella Enriched"},
        )
        paperless_mock.get("/api/documents/42/").respond(
            200, json={"id": 42, "tags": [1, 2], "custom_fields": []},
        )
        paperless_mock.patch("/api/documents/42/").mock(side_effect=_capture_tag_patch)

        async with PaperlessClient(
            "https://paperless.test", "tok",
        ) as paperless:
            svc = VerifyService(ai=ai, paperless=paperless, conn=db)
            outcome = await svc.enrich_with_context(
                42,
                context=EnrichmentContext(
                    vehicle="2009 Work SUV",
                    entity="Personal",
                    note_body="Gas for 2009 Work SUV",
                ),
            )

    assert outcome.note_added is True
    assert outcome.tag_applied is True
    assert "2009 Work SUV" in posted_note["note"]
    assert 88 in patched_tags["tags"]
    # Second call with identical context should dedup — no extra note posted.
    async with PaperlessClient("https://paperless.test", "tok") as paperless:
        svc = VerifyService(ai=ai, paperless=paperless, conn=db)
        outcome2 = await svc.enrich_with_context(
            42,
            context=EnrichmentContext(
                vehicle="2009 Work SUV",
                entity="Personal",
                note_body="Gas for 2009 Work SUV",
            ),
        )
    assert "already enriched" in (outcome2.skipped_reason or "")
    assert outcome2.note_added is False


# ------------------------------------------------------------------
# Cancel-responsiveness: when the user clicks Cancel mid-Tier-1, we
# must surface JobCancelled within ~1 second instead of waiting for
# the OpenRouter HTTP call to time out.
# ------------------------------------------------------------------


async def test_verify_cancel_during_tier1_raises_jobcancelled(
    db, settings, monkeypatch,
):
    """Reproduces the user-reported case: Tier 1 OCR re-extraction is
    in flight against OpenRouter, the user clicks Cancel, and we want
    the verify task to abort within ~1s. We simulate a long-running
    Tier 1 by stubbing ``_extract_from_ocr_text`` to ``asyncio.sleep``
    for several seconds, fire the cancel event 200ms in, and assert
    JobCancelled is raised within 1.5s.
    """
    import asyncio
    import threading
    import time

    from lamella.core.jobs.context import JobCancelled
    from lamella.web.routes.paperless_verify import _await_with_cancel

    # Tier 1 only runs when content_excerpt is >= OCR_MIN_LEN_FOR_TEXT_PASS
    # (60 chars). Use a long enough excerpt so the cascade enters Tier 1.
    _seed_doc(
        db,
        mime_type="image/jpeg",
        receipt_date="2064-01-08",
        content=(
            "Warehouse Club Wholesale Receipt\n"
            "Fuel - pump 4\nDate: 01/08/2064\n"
            "Total: $58.12\nThank you for shopping.\n"
        ),
    )
    _enable_writeback(db)
    ai = _make_ai(db, settings)

    # Stub the Tier 1 helper to a long sleep so the only way out is
    # asyncio.CancelledError from the watcher.
    async def _slow_tier1(self, *args, **kwargs):  # noqa: ARG001
        await asyncio.sleep(10)
        raise AssertionError("Tier 1 sleep should have been cancelled")

    monkeypatch.setattr(
        VerifyService, "_extract_from_ocr_text", _slow_tier1,
    )

    cancel_event = threading.Event()

    def _check_cancel() -> None:
        if cancel_event.is_set():
            raise JobCancelled()

    async with PaperlessClient(
        "https://paperless.test", "tok",
    ) as paperless:
        svc = VerifyService(ai=ai, paperless=paperless, conn=db)

        # Schedule a cancel-event set 200ms after we start.
        async def _trip_cancel() -> None:
            await asyncio.sleep(0.2)
            cancel_event.set()

        trip = asyncio.create_task(_trip_cancel())
        start = time.monotonic()
        with pytest.raises(JobCancelled):
            await _await_with_cancel(
                svc.verify_and_correct(
                    42,
                    progress=lambda *a, **kw: None,
                    cancel_check=_check_cancel,
                ),
                cancel_event,
                poll_seconds=0.05,
            )
        elapsed = time.monotonic() - start
        await trip

    # Cancel must take effect well under 1 second from event set
    # (200ms wait + ~50ms poll + task.cancel() unwind).
    assert elapsed < 1.5, (
        f"cancel took {elapsed:.2f}s — should be sub-second"
    )
