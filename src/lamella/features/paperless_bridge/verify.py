# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Paperless verify-and-writeback service.

When we suspect a receipt's OCR is wrong — most commonly a
``date_mismatch_note`` set on a linked receipt (Warehouse Club-2064 case)
— we send the original image to a vision-capable AI and ask it to
re-extract the structured fields against the image itself. The
prompt is framed adversarially ("do not assume the hypothesis is
correct — verify against the image") so we don't bias the model
toward our own guess.

Whatever the vision model returns is diffed against the current
Paperless fields. Anything we're confident about is PATCHed back
onto the Paperless document, plus a ``Lamella Fixed`` tag and
a note explaining what changed. The whole transaction is logged
to ``ai_decisions`` (decision_type=``receipt_verify``) and a row
in ``paperless_writeback_log`` (for dedup and audit). The local
``paperless_doc_index`` row is updated to match.

The ``enrich_with_context`` path is for the opposite direction —
we learned something from classify context (mileage log tied this
gas receipt to the 2009 Work SUV) and push that back to Paperless
as a note + custom field + ``Lamella Enriched`` tag. No
vision call; pure writeback from data we already have.

Both paths are gated on ``paperless_writeback_enabled`` (config
default False — user opts in once they trust the first few
corrections).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Callable, Literal

# Signature for verify/enrich progress callbacks. Matches the subset
# of JobContext.emit we actually use — keeps the service decoupled
# from the jobs module so unit tests can pass a trivial lambda.
#
# Arguments: (message, outcome, detail). detail is an optional dict
# of structured data the progress UI can render as a table (field
# diffs, current-values snapshot, writeback summary).
ProgressFn = Callable[[str, str, dict | None], None]

# Signature for the cancel-check callback. The callable raises (any
# exception — typically JobCancelled or asyncio.CancelledError) when
# the job has been cancelled, otherwise returns None. We use a
# zero-arg callable rather than threading JobContext through so the
# verify service stays decoupled from the jobs module.
CancelCheck = Callable[[], None]

from pydantic import BaseModel, Field

from lamella.features.ai_cascade.service import AIService
from lamella.adapters.paperless.client import PaperlessClient, PaperlessError
from lamella.adapters.paperless.schemas import Document

log = logging.getLogger(__name__)

TAG_FIXED = "Lamella Fixed"
TAG_ENRICHED = "Lamella Enriched"

# Per-field confidence threshold for auto-applying a correction.
# Vision models self-report confidence per-field; fields below
# this are flagged in the audit log but NOT patched into Paperless.
DEFAULT_FIELD_CONFIDENCE_THRESHOLD = 0.80

SourceType = Literal["image", "ocr_pdf", "native_pdf", "native_text", "unknown"]

# For multi-page PDFs, the vision call only sees page 1 (we render
# the first page to a thumbnail). We NEVER overwrite Paperless's
# `content` field or our local `content_excerpt` — those hold the
# full multi-page OCR and the single-page extract would destroy
# information on pages 2+. The audit note explicitly calls out
# "only page 1 verified" so a human auditor knows.
MIN_NATIVE_TEXT_CHARS = 40
"""Page 1 needs this many extractable chars before we treat a PDF
as native (text layer was in the original, no OCR was needed).
Anything less is almost certainly a scan."""


NATIVE_TEXT_MIMES: frozenset[str] = frozenset({
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.oasis.opendocument.text",
    "application/vnd.oasis.opendocument.spreadsheet",
    "application/msword",
    "application/vnd.ms-excel",
    "application/rtf",
    "message/rfc822",                  # emails forwarded to Paperless
})


def receipt_source_type(mime_type: str | None) -> SourceType:
    """First-pass classification from mime alone — cheap and used
    before we decide whether to fetch bytes. For PDFs this returns
    ``ocr_pdf`` optimistically; a second pass in
    ``classify_pdf_bytes`` can downgrade it to ``native_pdf``
    when the PDF has a real text layer.

    Images definitely went through OCR; text/docx/xlsx/email
    sources skip verification — their content_excerpt is
    authoritative."""
    if not mime_type:
        return "unknown"
    m = mime_type.lower().strip().split(";", 1)[0].strip()
    if m.startswith("image/"):
        return "image"
    if m == "application/pdf":
        return "ocr_pdf"
    if m.startswith("text/") or m in NATIVE_TEXT_MIMES:
        return "native_text"
    return "unknown"


def _sniff_image_mime(blob: bytes, fallback: str | None = None) -> str:
    """Detect an image's true mime type from its byte signature.

    The vision API rejects the call with HTTP 400 when the declared
    media type disagrees with the actual bytes (Anthropic's grammar
    compiler verifies the signature server-side). Paperless's
    thumbnail endpoint returns WebP on some installations even when
    historic code claimed JPEG, so the declared header isn't
    authoritative. Magic numbers are.

    Returns one of ``image/jpeg``, ``image/png``, ``image/gif``,
    ``image/webp``. Falls back to the caller-supplied mime (or
    ``image/jpeg`` as a last resort) when the bytes don't match a
    known signature, since some payloads are tiny / corrupt and the
    declared mime is the best guess we have.
    """
    if not blob:
        return (fallback or "image/jpeg").split(";")[0].strip()
    head = blob[:16]
    if head[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    return (fallback or "image/jpeg").split(";")[0].strip()


def classify_pdf_bytes(pdf_bytes: bytes) -> tuple[SourceType, int]:
    """Second-pass classification using the file itself.

    Returns ``(source_type, page_count)``. The decision is
    DELIBERATELY CONSERVATIVE: anything that smells like a scan +
    OCR pipeline output stays ``ocr_pdf`` so the caller runs the
    vision pass. Only PDFs with positive evidence of being natively
    typed (PDF metadata producer string or font set that does NOT
    match Tesseract / OCRmyPDF / Paperless OCR-stamping markers)
    downgrade to ``native_pdf``.

    Background: an earlier version of this function classified any
    PDF with extractable text on page 1 as ``native_pdf``, which
    skipped vision. That was wrong. Paperless / OCRmyPDF / Tesseract
    EMBED an invisible OCR text layer back into the PDF after
    scanning, so PyMuPDF reads "lots of text on page 1" for both
    native invoices AND scanned receipts. Distinguishing them
    requires looking at HOW the text got there, not whether it's
    there at all. The presence of Tesseract's ``GlyphLessFont`` or
    a producer / creator metadata string from OCRmyPDF / Tesseract /
    Paperless is the canonical "this PDF was OCR'd" signal.

    * ``("native_pdf", n)`` — text layer present AND no OCR-tool
      signature in producer / creator / fonts. The PDF was born
      digital (vendor invoice, bank statement, emailed receipt)
      and the content IS the actual text — vision adds no value.
    * ``("ocr_pdf", n)`` — anything else: text-layer absent, or
      text-layer present alongside an OCR-tool signature, or empty.
      Caller runs vision.
    * ``("unknown", 0)`` when the file isn't a valid PDF or
      PyMuPDF isn't available.
    """
    try:
        import pymupdf  # PyMuPDF — imports as `pymupdf` in 1.24+
    except ImportError:  # pragma: no cover — dep is core
        log.warning(
            "pymupdf not available; falling back to ocr_pdf "
            "classification for every PDF"
        )
        return "ocr_pdf", 0
    try:
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:  # noqa: BLE001 — corrupt PDFs etc.
        log.info("PDF classify failed (corrupt?): %s", exc)
        return "unknown", 0
    try:
        page_count = doc.page_count
        if page_count <= 0:
            return "unknown", 0
        page = doc[0]
        first_page_text = page.get_text().strip()
        if len(first_page_text) < MIN_NATIVE_TEXT_CHARS:
            return "ocr_pdf", page_count
        # Text layer is present. Now check whether it's stamped-by-OCR
        # text or natively-typed text. Producer / creator metadata is
        # the most reliable signal — OCRmyPDF, Tesseract, ocrmypdf-via-
        # Paperless all stamp themselves there. PyMuPDF font scanning
        # is the fallback: Tesseract + ocrmypdf use ``GlyphLessFont``
        # for the invisible text-overlay layer they add, and that font
        # does not appear in any natively-typed PDF.
        meta = doc.metadata or {}
        producer = (meta.get("producer") or "").lower()
        creator = (meta.get("creator") or "").lower()
        ocr_markers = (
            "ocrmypdf", "tesseract", "paperless",
            "ghostscript",  # OCRmyPDF often re-emits via gs
        )
        if any(m in producer for m in ocr_markers) or any(
            m in creator for m in ocr_markers
        ):
            return "ocr_pdf", page_count
        try:
            fonts = page.get_fonts() or []
        except Exception:  # noqa: BLE001
            fonts = []
        for font in fonts:
            # PyMuPDF returns each font as a tuple where index 3 is
            # the basefont name. Defensive against shape drift across
            # versions: scan all string elements for the marker.
            for piece in font:
                if isinstance(piece, str) and "glyphless" in piece.lower():
                    return "ocr_pdf", page_count
        return "native_pdf", page_count
    finally:
        doc.close()


# ------------------------------------------------------------------
# Input / output shapes
# ------------------------------------------------------------------


@dataclass(frozen=True)
class VerifyHypothesis:
    """Caller's guess about what's wrong with the current OCR.
    Rendered in the prompt EXPLICITLY as a hypothesis — not
    presented as truth. The model is instructed to ignore it if
    the image contradicts it."""
    suspected_date: date | None = None
    suspected_total: Decimal | None = None
    # The merchant name the CALLER thinks this receipt is for
    # (typically the txn's payee when the Paperless correspondent
    # looks wrong). Vision verifies against the image.
    suspected_vendor: str | None = None
    reason: str = ""


class VerifyLineItem(BaseModel):
    """One line on a receipt. Money and quantity are strings so the
    emitted JSON schema never gets an ``anyOf`` union or a
    ``format`` hint — both tripped Anthropic's grammar compiler
    via OpenRouter (``Invalid regex in pattern field`` errors
    synthesized from nullable-union shapes). Empty string means
    "not observed"; numeric values are decimal-formatted ASCII."""
    description: str = ""
    qty: str = ""
    unit_price: str = ""
    line_total: str = ""


class FieldConfidences(BaseModel):
    """Per-field confidence in [0, 1]. Explicit fields (not an open
    dict[str, float]) so the generated JSON schema doesn't emit
    ``additionalProperties`` — OpenRouter's translator to
    Anthropic chokes on that. Missing = 0.0."""
    receipt_date: float = 0.0
    vendor: float = 0.0
    total: float = 0.0
    subtotal: float = 0.0
    tax: float = 0.0
    tip: float = 0.0
    # ``payment_last_four`` removed from the schema per ADR-0044
    # (superseded by ``Lamella_Account``). Pydantic ignores unknown
    # keys on parse by default, so cached decisions on disk that
    # still carry the key deserialize cleanly without it.


class ReceiptVerification(BaseModel):
    """Structured output from the receipt-verify model.

    Every field is a plain ``str`` / ``float`` / ``list`` — no
    ``Decimal | None`` unions, no ``date | None`` with format
    hints, no ``dict[str, float]`` open dicts. Earlier versions of
    this model emitted those constructs and Anthropic (via
    OpenRouter) rejected every call with
    ``output_config.format.schema: Invalid regex in pattern
    field: Quantifier '?' without preceding element`` — the
    provider's schema-to-grammar compiler synthesised a bad
    regex from the nullable-anyOf shapes. Strings parse on our
    side via ``_parse_date`` / ``_parse_decimal``; empty string
    means "not extracted"."""
    receipt_date: str = ""           # ISO YYYY-MM-DD, or ""
    vendor: str = ""
    total: str = ""                  # decimal-formatted ASCII, or ""
    subtotal: str = ""
    tax: str = ""
    tip: str = ""
    # ``payment_last_four`` removed per ADR-0044; see FieldConfidences
    # comment above.
    line_items: list[VerifyLineItem] = Field(default_factory=list)
    confidence: FieldConfidences = Field(default_factory=FieldConfidences)
    ocr_errors_noted: list[str] = Field(default_factory=list)
    reasoning: str = ""
    # Fully re-extracted text content for page 1. When the caller
    # has confirmed this is a single-page document and diffs
    # exceeded threshold, the content gets PATCHed back to
    # Paperless's `content` field so the stored OCR matches the
    # corrected values. For multi-page docs, this is IGNORED —
    # overwriting the stored content with only-page-1 text would
    # lose pages 2+. Empty string = model declined to provide.
    corrected_content: str = ""


@dataclass(frozen=True)
class VerifyDiff:
    field: str
    before: str | None
    after: str | None
    confidence: float


@dataclass
class VerifyOutcome:
    paperless_id: int
    source_type: SourceType
    skipped_reason: str | None = None      # non-None → no vision/ocr call made
    verified: bool = False                  # True iff extraction ran
    diffs: list[VerifyDiff] = field(default_factory=list)
    tag_applied: bool = False
    note_added: bool = False
    fields_patched: int = 0
    extracted: ReceiptVerification | None = None
    decision_id: int | None = None
    # For PDFs, the total page count. >1 means we only sent page 1
    # to the vision model — the audit note surfaces this so a
    # human knows any corrections apply to page-1 fields only.
    page_count: int = 1
    # Which tier actually produced the extracted fields.
    # 'ocr_text' = cheap Haiku pass over the OCR'd content only.
    # 'vision'   = expensive Opus pass against the image.
    # 'skipped'  = extraction didn't run.
    extraction_source: str = "skipped"
    # ADR-0058 follow-up: when the verifier was called with a
    # hypothesis (i.e. a linked txn was claiming this is its receipt)
    # and the AI's response contradicts the hypothesis with high
    # confidence — wrong vendor extracted, monetary fields all 0
    # confidence, ocr_errors_noted explicitly calls out the mismatch
    # — the link is wrong. ``mismatch_detected`` flips True and
    # ``mismatch_reason`` carries the AI's explanation. The caller
    # surfaces these in the audit-note + verify modal so the user
    # can unlink with one click. We never auto-unlink: the caller
    # always controls the destructive action.
    mismatch_detected: bool = False
    mismatch_reason: str = ""

    @property
    def changed_anything(self) -> bool:
        return self.fields_patched > 0 or self.tag_applied or self.note_added

    @property
    def is_multi_page(self) -> bool:
        return self.page_count > 1


@dataclass(frozen=True)
class EnrichmentContext:
    """Facts we derived elsewhere (mileage, notes, classify) that
    should be reflected back onto the Paperless document.

    `vehicle`/`entity`/`project` are optional hints. `note_body`
    is the rendered human-readable summary ("Gas for 2009 Work SUV
    Work SUV (Personal)") that goes on the Paperless note. At least
    one signal must be present or the enrichment is skipped."""
    vehicle: str | None = None
    entity: str | None = None
    project: str | None = None
    note_body: str = ""

    @property
    def has_signal(self) -> bool:
        return bool(
            self.note_body.strip()
            or self.vehicle or self.entity or self.project
        )


@dataclass
class EnrichOutcome:
    paperless_id: int
    skipped_reason: str | None = None
    tag_applied: bool = False
    note_added: bool = False
    custom_fields_set: list[str] = field(default_factory=list)
    decision_id: int | None = None


# ------------------------------------------------------------------
# Vision prompt (sent alongside the image)
# ------------------------------------------------------------------


SYSTEM = (
    "You are a meticulous OCR verifier. You look at the attached "
    "receipt image and re-extract its structured fields. You do "
    "NOT assume the caller's current OCR is correct. You do NOT "
    "assume any provided hypothesis is correct. You verify by "
    "reading the image. Your confidence scores are honest: if a "
    "field is blurry or partially obscured, confidence is low; "
    "if you can read it plainly, confidence is high."
)

SYSTEM_OCR_ONLY = (
    "You extract structured receipt fields from already-OCR'd "
    "text. The OCR was produced by Paperless and is usually "
    "accurate for clear receipts. Extract: receipt_date, vendor "
    "(the actual merchant — often the store name in the header "
    "or logo area, NOT a city/location line), total, subtotal, "
    "tax, tip, and line_items.\n\n"
    "GOVERNANCE: the caller's hypothesis is GROUND TRUTH from the "
    "linked transaction. Your job is to corroborate, not to overrule. "
    "If your extracted fields disagree with the hypothesis, the OCR "
    "is more likely wrong than the hypothesis (stylized logos and "
    "branding don't always come through OCR; receipts can also be "
    "stitched together from multiple sources). When that happens, "
    "or when the OCR text is garbled / truncated / missing a total, "
    "add the literal token NEEDS_VISION to ocr_errors_noted. Only "
    "the exact token is honored; any other string in that list is "
    "ignored by the caller. Otherwise, confidence scores in [0,1] "
    "per field, honest about what you could and couldn't read."
)

# Minimum average field-confidence needed to accept an OCR-only
# extraction without escalating to vision. Lower-confidence
# extractions get a vision pass to double-check.
OCR_ONLY_MIN_AVG_CONFIDENCE = 0.75

# Marker the model uses in ocr_errors_noted to ask the caller to
# escalate to a vision pass.
OCR_ESCALATE_MARKER = "NEEDS_VISION"

# OCR text shorter than this isn't worth a text-only call — likely
# empty / stub index row / near-blank document. Go straight to vision.
OCR_MIN_LEN_FOR_TEXT_PASS = 60


def _build_user_prompt(
    *,
    current: dict[str, Any],
    content_excerpt: str,
    hypothesis: VerifyHypothesis | None,
    source_type: SourceType,
    page_count: int = 1,
) -> str:
    lines: list[str] = []
    lines.append(
        "Re-extract the structured fields from the attached receipt image."
    )
    lines.append("")
    lines.append("Current Paperless fields (may be wrong — verify against the image):")
    for k, v in current.items():
        lines.append(f"  {k}: {v if v not in (None, '') else '(not set)'}")
    lines.append("")
    if content_excerpt:
        excerpt = content_excerpt.strip()
        if len(excerpt) > 1500:
            excerpt = excerpt[:1500].rstrip() + "…"
        lines.append("Current OCR excerpt (also may be wrong — DO NOT trust over the image):")
        lines.append("-----")
        lines.append(excerpt)
        lines.append("-----")
        lines.append("")
    if hypothesis and (
        hypothesis.suspected_date or hypothesis.suspected_total
        or hypothesis.suspected_vendor or hypothesis.reason
    ):
        lines.append(
            "The caller has a hypothesis. DO NOT take it at face value — "
            "verify against the image. If the image clearly contradicts "
            "the hypothesis, report what's actually on the image."
        )
        if hypothesis.suspected_date:
            lines.append(f"  Hypothesis — date may be: {hypothesis.suspected_date}")
        if hypothesis.suspected_total:
            lines.append(f"  Hypothesis — total may be: {hypothesis.suspected_total}")
        if hypothesis.suspected_vendor:
            lines.append(
                f"  Hypothesis — actual merchant may be: "
                f"{hypothesis.suspected_vendor} "
                f"(Paperless has it labeled differently; check whether "
                f"the logo, footer branding, or card-reader text "
                f"identifies the merchant, not the city/location line)"
            )
        if hypothesis.reason:
            lines.append(f"  Hypothesis — reason: {hypothesis.reason}")
        lines.append("")
    if source_type == "ocr_pdf":
        if page_count > 1:
            lines.append(
                f"Note: the attached image is ONLY the first page "
                f"of a {page_count}-page PDF. Only extract fields "
                f"that appear on page 1 (header totals, date, "
                f"vendor are usually here; per-page line items "
                f"on later pages are NOT visible). Leave any "
                f"field null + confidence 0 if it isn't on this "
                f"page."
            )
        else:
            lines.append(
                "Note: the attached image is the first-page thumbnail "
                "of a PDF. Resolution may be modest; do your best. If "
                "a field is genuinely unreadable, leave it null and "
                "set its confidence to 0."
            )
        lines.append("")
    lines.append(
        "Return a JSON object with: receipt_date (YYYY-MM-DD), vendor, "
        "total, subtotal, tax, tip, line_items[], "
        "confidence{field: 0..1}, ocr_errors_noted[], reasoning, "
        "corrected_content."
    )
    if page_count <= 1:
        lines.append(
            "corrected_content: a clean plaintext re-transcription of "
            "EVERYTHING visible on the receipt (header, line items, "
            "totals, tax, footer, payment info) — one line per logical "
            "row. This REPLACES the existing OCR in Paperless when we "
            "push corrections back. If the image is unreadable, leave "
            "it as an empty string and we won't touch the stored OCR."
        )
    else:
        lines.append(
            f"corrected_content: LEAVE EMPTY. This is a {page_count}-page "
            f"PDF and you only see page 1. Overwriting the stored OCR "
            f"content would destroy pages 2+, so we won't push a content "
            f"rewrite for multi-page docs. (The structured fields above "
            f"are still patched if confident.)"
        )
    lines.append(
        "For each field you set, include a confidence in `confidence`. "
        "Use 0.90+ only when the value is plainly legible. Use 0.50-0.80 "
        "when the value is partially obscured but you can still make it "
        "out. Use 0.00-0.49 when you're guessing. Leave a field null "
        "and confidence 0 if you genuinely can't tell."
    )
    lines.append(
        "Flag every OCR error you notice (wrong date, misread digit, "
        "swapped currency) in ocr_errors_noted — short free-text strings."
    )
    return "\n".join(lines)


def _build_ocr_text_prompt(
    *,
    current: dict[str, Any],
    content_excerpt: str,
    hypothesis: VerifyHypothesis | None,
) -> str:
    """Text-only counterpart to ``_build_user_prompt``. No image
    language — the model only sees the OCR'd content and must
    escalate via ``NEEDS_VISION`` if it can't trust what it reads."""
    lines: list[str] = []
    lines.append(
        "Extract structured receipt fields from the OCR text below. "
        "This was OCR'd by Paperless from a scanned or photographed "
        "receipt. It is USUALLY accurate for clear receipts but can "
        "be garbled on blurry / thermal-faded / crumpled ones."
    )
    lines.append("")
    lines.append("Current Paperless fields (for reference — may be wrong):")
    for k, v in current.items():
        lines.append(f"  {k}: {v if v not in (None, '') else '(not set)'}")
    lines.append("")
    excerpt = (content_excerpt or "").strip()
    if len(excerpt) > 4000:
        excerpt = excerpt[:4000].rstrip() + "…"
    lines.append("OCR text:")
    lines.append("-----")
    lines.append(excerpt or "(empty)")
    lines.append("-----")
    lines.append("")
    if hypothesis and (
        hypothesis.suspected_date or hypothesis.suspected_total
        or hypothesis.suspected_vendor or hypothesis.reason
    ):
        lines.append(
            "GROUND TRUTH from the linked transaction — this is what "
            "the user already KNOWS about this receipt (bank statement / "
            "SimpleFIN row). Your job is to corroborate, not to overrule. "
            "If the OCR vendor disagrees with the merchant below, the "
            "OCR is more likely wrong than this hypothesis (stylized "
            "logos and branding don't always come through OCR). When "
            "that happens, add 'NEEDS_VISION' to ocr_errors_noted so "
            "the caller escalates to a vision pass against the image."
        )
        if hypothesis.suspected_date:
            lines.append(f"  Transaction date: {hypothesis.suspected_date}")
        if hypothesis.suspected_total:
            lines.append(f"  Transaction amount: {hypothesis.suspected_total}")
        if hypothesis.suspected_vendor:
            lines.append(
                f"  Transaction merchant: {hypothesis.suspected_vendor}"
            )
        if hypothesis.reason:
            lines.append(f"  User note: {hypothesis.reason}")
        lines.append("")
    lines.append(
        "Return a JSON object with: receipt_date (YYYY-MM-DD), vendor, "
        "total, subtotal, tax, tip, line_items[], "
        "confidence{field: 0..1}, ocr_errors_noted[], reasoning. "
        "Leave corrected_content empty — this is a text-only pass, "
        "not a re-OCR."
    )
    lines.append(
        "Confidence rules: 0.90+ only when the field is plainly present "
        "and unambiguous in the text. 0.50-0.80 when you're inferring "
        "(e.g., total is the largest dollar figure but not labeled). "
        "Below 0.50 when you're guessing. Null + 0 when absent."
    )
    lines.append(
        "If the OCR text is garbled, truncated, missing a total, or "
        "the merchant is ambiguous, add 'NEEDS_VISION' to "
        "ocr_errors_noted — the caller will retry against the image."
    )
    return "\n".join(lines)


_VENDOR_STOPWORDS: frozenset[str] = frozenset({
    # Articles + connectives.
    "the", "a", "an", "and", "of", "for", "at", "by",
    # Generic business / legal suffixes that carry no brand info.
    "co", "inc", "corp", "ltd", "llc", "lp", "llp", "plc", "gmbh",
    "store", "stores", "shop", "shops",
    # Bank-statement noise: "via {processor}" qualifiers and
    # transactional words bank statements love to inject.
    "via", "by", "through", "purchase", "payment",
    # Merchant-category descriptors that often appear on the
    # receipt header but not on the bank statement (or vice versa).
    "self", "service", "online", "online.com", "com", "www",
})


def _vendor_tokens(s: str) -> set[str]:
    """Tokenize a vendor string into a comparison-ready set: lowered,
    punctuation-stripped, stop-words and ≤1-char tokens dropped.

    Used by ``_vendor_matches_hypothesis`` to decide whether two
    written forms describe the same merchant. Bank-statement form
    and receipt-header form of any merchant diverge predictably:
    the bank wraps the brand with city/state and a processor suffix,
    the receipt wraps the brand with category descriptors. Neither
    is a substring of the other but their distinctive brand +
    location tokens are shared.
    """
    norm: list[str] = []
    for ch in (s or "").lower():
        if ch.isalnum() or ch == " ":
            norm.append(ch)
        else:
            norm.append(" ")
    tokens = "".join(norm).split()
    return {
        t for t in tokens
        if len(t) >= 2 and t not in _VENDOR_STOPWORDS
    }


def _vendor_matches_hypothesis(extracted_vendor: str, suspected_vendor: str) -> bool:
    """Decide whether the model's extracted vendor agrees with the
    caller's hypothesis. Two-step:

    1. Substring match either way (fast path for the "the {brand}"
       vs "{brand}" case and any caller-included-as-prefix case).
    2. Token-set overlap: drop stop-words and category descriptors,
       then require that the SMALLER token set is at least 50%
       covered by the larger one. Catches the bank-statement vs
       receipt-header mismatch where the bank's processor suffix
       and the receipt's category descriptor diverge but the brand
       tokens overlap.

    Empty hypothesis or empty extraction returns True (no claim to
    disagree with). The avg-confidence gate handles the
    extraction-empty case independently.
    """
    sus = (suspected_vendor or "").strip().lower()
    ext = (extracted_vendor or "").strip().lower()
    if not sus or not ext:
        return True
    # Quick wins: substring either direction.
    if sus in ext or ext in sus:
        return True
    sus_tokens = _vendor_tokens(sus)
    ext_tokens = _vendor_tokens(ext)
    if not sus_tokens or not ext_tokens:
        # Both reduced to empty after stop-word strip — pre-stop
        # equality is the only signal left, and we already failed
        # the substring check. Treat as match (can't reliably say
        # "different"); avg-confidence gate still handles low
        # extractions.
        return True
    overlap = sus_tokens & ext_tokens
    smaller = min(len(sus_tokens), len(ext_tokens))
    # 50% of the smaller side covered by the overlap. With both
    # sides reduced to brand + location tokens, this catches
    # bank vs receipt vendor variations without false-matching
    # genuinely different merchants (a hardware store and a
    # sewing store share zero brand tokens).
    return (len(overlap) * 2) >= smaller


def _ocr_extraction_quality(
    extracted: ReceiptVerification,
    *,
    hypothesis: VerifyHypothesis | None = None,
) -> tuple[float, bool, str | None]:
    """Return ``(avg_confidence, needs_vision_flag, escalation_reason)``
    for an OCR-text extraction. The caller uses this to decide
    whether to accept the result or escalate to vision.

    Three signals can force vision escalation:
    1. Model self-flagged ``NEEDS_VISION`` in ``ocr_errors_noted``.
    2. Average per-field confidence below the threshold.
    3. **Hypothesis-vendor mismatch**: the caller's hypothesis carried
       a ``suspected_vendor`` (the linked txn's payee) and the
       OCR-tier extraction returned a different vendor. The hypothesis
       is GROUND TRUTH from the bank statement; if OCR disagrees the
       OCR is more likely wrong than the user's data — vision should
       look at the actual receipt image (logos / branding don't OCR).

    Average confidence is taken across the canonical monetary /
    identity fields — line-item confidences are ignored so a
    receipt with many items doesn't drown out a missing total.
    ``payment_last_four`` is excluded per ADR-0044.
    """
    errors = [e.strip().upper() for e in (extracted.ocr_errors_noted or [])]
    model_flag = any(OCR_ESCALATE_MARKER in e for e in errors)
    conf = extracted.confidence
    vals = [
        float(conf.receipt_date),
        float(conf.vendor),
        float(conf.total),
        float(conf.subtotal),
        float(conf.tax),
    ]
    present = [v for v in vals if v > 0]
    avg = sum(present) / len(present) if present else 0.0
    reason: str | None = None
    if model_flag:
        reason = "model self-flagged NEEDS_VISION"
    if hypothesis and hypothesis.suspected_vendor:
        if not _vendor_matches_hypothesis(extracted.vendor, hypothesis.suspected_vendor):
            reason = (
                f"OCR vendor '{extracted.vendor}' disagrees with "
                f"hypothesis '{hypothesis.suspected_vendor}' — escalate "
                "to vision (logos may not OCR)"
            )
    needs_vision = bool(reason)
    return avg, needs_vision, reason


# ------------------------------------------------------------------
# Service
# ------------------------------------------------------------------


class VerifyService:
    """Owns the verify-and-writeback flow. Wired by callers
    (enricher, manual routes) with an AIService (for the vision
    call) and a PaperlessClient (for reads + writes). The DB
    connection is used for the doc index lookup + writeback log
    + ai_decisions insert."""

    def __init__(
        self,
        *,
        ai: AIService,
        paperless: PaperlessClient,
        conn: sqlite3.Connection,
    ):
        self.ai = ai
        self.paperless = paperless
        self.conn = conn

    # --- public API ------------------------------------------------

    async def verify_and_correct(
        self,
        paperless_id: int,
        *,
        hypothesis: VerifyHypothesis | None = None,
        dry_run: bool = False,
        ocr_first: bool = True,
        progress: ProgressFn | None = None,
        cancel_check: CancelCheck | None = None,
    ) -> VerifyOutcome:
        """Cascade: try a cheap text-only extraction against the
        OCR'd content first, fall back to the expensive vision pass
        only when the OCR extraction is low-confidence or the model
        explicitly asks for escalation via ``NEEDS_VISION``. Set
        ``ocr_first=False`` to force the vision path (useful when a
        caller already knows the OCR is bad).

        ``progress`` is an optional ``(message, outcome)`` callback
        used by the job-runner wrapper to surface step-by-step
        status to the user's progress modal. Outcomes map to the
        JobContext vocabulary (``info``, ``success``, ``failure``,
        ``error``). When None, the service stays silent.

        ``cancel_check`` is an optional zero-arg callable that
        raises when the job is cancelled. We invoke it at every
        phase boundary (pre Tier 1, post Tier 1, pre Tier 2, post
        Tier 2, pre writeback). Cancel during an in-flight AI/HTTP
        call requires the caller to also cancel the asyncio task —
        see ``paperless_verify._work``. Best-effort: an httpx call
        in the middle of streaming a response may not honor cancel
        instantly.
        """

        def emit(
            msg: str,
            outcome: str = "info",
            detail: dict | None = None,
        ) -> None:
            if progress is not None:
                try:
                    progress(msg, outcome, detail)
                except Exception:  # noqa: BLE001
                    log.exception("progress callback failed")

        def check_cancel() -> None:
            """Raise (JobCancelled / CancelledError / etc.) if the
            outer job has been cancelled. No-op when no callback was
            wired."""
            if cancel_check is not None:
                cancel_check()

        check_cancel()
        emit(f"Loading document #{paperless_id} from local index …")
        doc_row = self._load_index_row(paperless_id)
        if doc_row is None:
            emit(
                f"Document #{paperless_id} is not in the local index — "
                f"run a Paperless sync first.",
                "failure",
            )
            return VerifyOutcome(
                paperless_id=paperless_id, source_type="unknown",
                skipped_reason="not in local index",
            )

        # Ensure we know the mime_type. Sync leaves it NULL, so we
        # fetch the metadata subroute on demand the first time a
        # doc is verified.
        mime = doc_row["mime_type"]
        if not mime:
            emit("No cached mime type — fetching from Paperless …")
            mime = await self._fetch_and_cache_mime(paperless_id)
        source_type = receipt_source_type(mime)
        emit(f"Source type: {source_type} (mime={mime or 'unknown'})")
        if source_type == "native_text":
            emit(
                "Skipping verify — native-text source (docx / email / etc.) "
                "wasn't OCR'd so there's nothing to cross-check.",
                "info",
            )
            return VerifyOutcome(
                paperless_id=paperless_id, source_type=source_type,
                skipped_reason="native-text source — OCR not used, no verify needed",
            )

        current = _current_fields_dict(doc_row)
        content_excerpt = doc_row["content_excerpt"] or ""
        emit(
            "Current Paperless fields (baseline):",
            detail={
                "kind": "current_fields",
                "fields": {
                    k: (str(v) if v not in (None, "") else None)
                    for k, v in current.items()
                },
            },
        )

        # --- Tier 1: cheap OCR-text extraction --------------------
        extracted: ReceiptVerification | None = None
        decision_id: int | None = None
        extraction_source = "skipped"
        page_count = 1
        ocr_candidate: ReceiptVerification | None = None
        ocr_decision_id: int | None = None
        ocr_error: str | None = None
        if (
            ocr_first
            and content_excerpt
            and len(content_excerpt.strip()) >= OCR_MIN_LEN_FOR_TEXT_PASS
        ):
            check_cancel()
            emit(
                f"Tier 1: re-extracting from stored OCR text "
                f"({len(content_excerpt)} chars) with "
                f"{self.ai.ocr_text_receipt_verify_model()} …",
            )
            ocr_result, ocr_error = await self._extract_from_ocr_text(
                paperless_id,
                current=current,
                content_excerpt=content_excerpt,
                hypothesis=hypothesis,
            )
            check_cancel()
            if ocr_error is not None:
                emit(
                    f"Tier 1 failed: {ocr_error}. Falling back to vision.",
                    "failure",
                )
            if ocr_result is not None:
                candidate, cand_decision_id = ocr_result
                ocr_candidate = candidate
                ocr_decision_id = cand_decision_id
                avg_conf, needs_vision, esc_reason = _ocr_extraction_quality(
                    candidate, hypothesis=hypothesis,
                )
                emit(
                    f"Tier 1 returned avg confidence {avg_conf:.2f} "
                    f"(needs_vision={needs_vision})"
                    + (f" — {esc_reason}" if esc_reason else ""),
                )
                if not needs_vision and avg_conf >= OCR_ONLY_MIN_AVG_CONFIDENCE:
                    extracted = candidate
                    decision_id = cand_decision_id
                    extraction_source = "ocr_text"
                    emit(
                        "Accepted Tier 1 extraction — no vision call needed.",
                        "success",
                    )
                    log.info(
                        "paperless verify: accepted OCR-text extraction "
                        "for %d (avg_conf=%.2f)",
                        paperless_id, avg_conf,
                    )
                else:
                    if esc_reason:
                        emit(
                            f"Escalating to vision: {esc_reason}.",
                            "info",
                        )
                    else:
                        emit(
                            f"Tier 1 confidence below "
                            f"{OCR_ONLY_MIN_AVG_CONFIDENCE:.2f} threshold — "
                            f"escalating to vision.",
                        )
        else:
            emit(
                "OCR excerpt too short / missing — skipping Tier 1, "
                "going straight to vision.",
            )

        # --- Tier 2: vision fallback ------------------------------
        if extracted is None:
            check_cancel()
            emit("Fetching image for vision pass …")
            try:
                fetch_result = await self._fetch_image_for_vision(
                    paperless_id, source_type,
                )
            except PaperlessError as exc:
                emit(f"Couldn't fetch image: {exc}", "error")
                return VerifyOutcome(
                    paperless_id=paperless_id, source_type=source_type,
                    skipped_reason=f"couldn't fetch image: {exc}",
                )
            if fetch_result is None:
                # Native PDF — vision would just rasterize and re-OCR
                # text that's already extractable, gaining nothing.
                # But Tier 1 already read the same authoritative
                # text; if it produced a candidate, accept it even
                # when avg_conf fell below the escalation threshold.
                # The per-field confidence gate in _fields_to_patch
                # still controls what actually gets written. This is
                # the correspondent-correction path for digital
                # invoices (vendor name is in the text layer, current
                # Paperless correspondent is wrong).
                if ocr_candidate is not None:
                    extracted = ocr_candidate
                    decision_id = ocr_decision_id
                    extraction_source = "ocr_text"
                    source_type = "native_pdf"
                    emit(
                        "Native PDF detected — using Tier 1 extraction "
                        "(vision would re-OCR text that's already "
                        "extractable).",
                    )
                    log.info(
                        "paperless verify: native PDF — falling back "
                        "to Tier 1 OCR-text candidate for %d",
                        paperless_id,
                    )
                else:
                    detail = (
                        f" (Tier 1 error: {ocr_error})" if ocr_error else ""
                    )
                    emit(
                        f"Native PDF with no usable Tier 1 result{detail}. "
                        f"Nothing to verify.",
                        "failure",
                    )
                    return VerifyOutcome(
                        paperless_id=paperless_id, source_type="native_pdf",
                        skipped_reason=(
                            "native PDF (text layer present in original); "
                            "Tier 1 extraction produced no candidate — "
                            "nothing to verify"
                            + (f" ({ocr_error})" if ocr_error else "")
                        ),
                    )
            else:
                image_bytes, image_mime, page_count = fetch_result
                emit(
                    f"Tier 2: running vision pass with "
                    f"{self.ai.vision_model()} "
                    f"({len(image_bytes)} bytes, {image_mime}, "
                    f"{page_count} page{'s' if page_count != 1 else ''}) …",
                )

                user_prompt = _build_user_prompt(
                    current=current,
                    content_excerpt=content_excerpt,
                    hypothesis=hypothesis,
                    source_type=source_type,
                    page_count=page_count,
                )
                client = self.ai.new_client()
                if client is None:
                    emit("AI disabled or over monthly cap — aborting.", "error")
                    return VerifyOutcome(
                        paperless_id=paperless_id, source_type=source_type,
                        skipped_reason="AI disabled or over cap",
                    )
                check_cancel()
                try:
                    result = await client.chat(
                        decision_type="receipt_verify",
                        input_ref=f"paperless:{paperless_id}",
                        system=SYSTEM,
                        user=user_prompt,
                        schema=ReceiptVerification,
                        model=self.ai.vision_model(),
                        images=[(image_bytes, image_mime)],
                    )
                except Exception as exc:  # noqa: BLE001 — includes AIError
                    log.warning(
                        "paperless verify vision call failed for %d: %s",
                        paperless_id, exc,
                    )
                    await client.aclose()
                    emit(
                        f"Vision call failed: {type(exc).__name__}: {exc}",
                        "error",
                    )
                    return VerifyOutcome(
                        paperless_id=paperless_id, source_type=source_type,
                        skipped_reason=f"vision call failed: {exc}",
                    )
                await client.aclose()
                check_cancel()
                extracted = result.data
                decision_id = result.decision_id
                extraction_source = "vision"
                emit("Vision extraction complete.", "success")

        diffs = _compute_diffs(current, extracted)
        if diffs:
            emit(
                f"Found {len(diffs)} field difference(s) vs. Paperless.",
                detail={
                    "kind": "diff",
                    "count": len(diffs),
                    "diffs": [
                        {
                            "field": d.field,
                            "before": d.before,
                            "after": d.after,
                            "confidence": float(d.confidence or 0.0),
                            "will_apply": (
                                float(d.confidence or 0.0)
                                >= DEFAULT_FIELD_CONFIDENCE_THRESHOLD
                            ),
                        }
                        for d in diffs
                    ],
                },
            )
        else:
            emit("No differences — Paperless fields already match.", "success")

        outcome = VerifyOutcome(
            paperless_id=paperless_id,
            source_type=source_type,
            verified=True,
            diffs=diffs,
            extracted=extracted,
            decision_id=decision_id,
            page_count=page_count,
            extraction_source=extraction_source,
        )
        # Mismatch detection: when the verify call was hypothesis-
        # driven (a linked txn claimed "this is my receipt") and the
        # AI's response strongly contradicts the hypothesis, flag the
        # outcome so the UI can offer a one-click unlink. Never auto-
        # unlink — that's the user's call. Heuristic uses signals we
        # already have (no schema change to ReceiptVerification):
        #   1. Hypothesis was provided (the verify was for a linked txn).
        #   2. AI returned 0 confidence on EVERY monetary/date field.
        #   3. ocr_errors_noted contains an explicit mismatch keyword.
        # All three together = high-confidence "this isn't a receipt
        # for the claimed txn." Single-signal hits don't trigger.
        if hypothesis is not None:
            try:
                _conf = extracted.confidence
                _money_zero = (
                    float(_conf.receipt_date) == 0.0
                    and float(_conf.total) == 0.0
                    and float(_conf.subtotal) == 0.0
                    and float(_conf.tax) == 0.0
                )
                _err_text = " ".join(
                    (extracted.ocr_errors_noted or [])
                ).lower()
                _MISMATCH_KEYWORDS = (
                    "not a receipt",
                    "not supported",
                    "is incorrect",
                    "is not visible",
                    "doesn't match",
                    "does not match",
                    "doesn't support",
                    "tax statement",
                    "1099",
                    "hypothesis",
                )
                _err_flags_mismatch = any(
                    kw in _err_text for kw in _MISMATCH_KEYWORDS
                )
                if _money_zero and _err_flags_mismatch:
                    outcome.mismatch_detected = True
                    outcome.mismatch_reason = (
                        extracted.reasoning
                        or "; ".join(extracted.ocr_errors_noted or [])
                        or "AI flagged hypothesis mismatch"
                    )[:600]
                    emit(
                        "Mismatch detected — the AI says this document "
                        "doesn't match the linked transaction. Use the "
                        "Unlink button on the verify result to remove "
                        "the link and clear the auto-stamped Lamella_* "
                        "fields. Lamella will NOT auto-unlink.",
                        "failure",
                        detail={
                            "kind": "mismatch_detected",
                            "reason": outcome.mismatch_reason,
                        },
                    )
            except Exception:  # noqa: BLE001
                # Mismatch detection is best-effort — never let a
                # heuristic crash break the rest of verify.
                pass
        if dry_run or not self._writeback_enabled():
            reason = (
                "dry_run" if dry_run else "writeback disabled in settings"
            )
            outcome.skipped_reason = reason
            if dry_run:
                emit(f"Not writing back: {reason}.", "info")
            else:
                # Loud version when the master gate is off — this is
                # the most common reason "verify ran but my Paperless
                # fields are still empty." Surface it as a warn so it
                # stands out in the modal log instead of getting lost
                # in the info-stream.
                emit(
                    "Writeback is OFF in settings. Verify extracted "
                    f"{len(diffs)} field correction(s) but will NOT push "
                    "them to Paperless. Toggle "
                    "`paperless_writeback_enabled` on /settings/paperless to enable.",
                    "failure",
                )
            return outcome

        # Apply only the diffs whose field-level confidence is high
        # enough, and whose actual value changed.
        patch_fields, set_fields = _fields_to_patch(
            extracted, diffs,
            threshold=DEFAULT_FIELD_CONFIDENCE_THRESHOLD,
            current_correspondent=str(current.get("correspondent_name") or ""),
            current_title=str(current.get("title") or ""),
        )
        # Single-page docs with real corrections → rewrite the
        # stored OCR content too. Multi-page never (would truncate
        # to page 1). No diffs → content was already correct, no
        # point rewriting. Empty corrected_content → the model
        # declined, leave the OCR alone. OCR-text tier never
        # rewrites content (the model only saw the already-OCR'd
        # text, so its output can't improve on the source).
        corrected_content: str | None = None
        # Trace each gate independently so the user can see WHICH
        # condition disqualified the content rewrite. Previously the
        # decision was silent — when Tier 2 succeeded but the OCR
        # text didn't get patched back, there was no log signal
        # explaining whether multi-page / no-diffs / empty-corrected
        # was the cause.
        _cc_raw = (extracted.corrected_content or "").strip()
        _cc_gate = {
            "from_vision": extraction_source == "vision",
            "single_page": page_count == 1,
            "have_diffs": bool(diffs),
            "model_returned_content": bool(_cc_raw),
            "content_chars": len(_cc_raw),
        }
        if all([
            _cc_gate["from_vision"],
            _cc_gate["single_page"],
            _cc_gate["have_diffs"],
            _cc_gate["model_returned_content"],
        ]):
            corrected_content = _cc_raw
            emit(
                f"OCR-content rewrite armed ({len(_cc_raw)} chars) — "
                f"will overwrite Paperless's stored content on PATCH.",
                "info",
                detail={"kind": "content_gate", "decision": "armed", **_cc_gate},
            )
        else:
            failed = [k for k in ("from_vision", "single_page",
                                  "have_diffs", "model_returned_content")
                      if not _cc_gate[k]]
            emit(
                f"OCR-content rewrite skipped ({', '.join(failed)} = false). "
                "Field corrections will still be patched if confidence allows.",
                "info",
                detail={"kind": "content_gate", "decision": "skipped",
                        "failed_gates": failed, **_cc_gate},
            )

        check_cancel()
        if patch_fields:
            emit(
                f"Writing {len(set_fields)} correction(s) to Paperless "
                f"({', '.join(set_fields)}) …",
            )
        else:
            emit("No field met the confidence threshold — writing tag + note only.")

        try:
            await self._apply_corrections(
                paperless_id=paperless_id,
                doc_row=doc_row,
                patch=patch_fields,
                note=_render_verify_note(
                    extracted, diffs, decision_id or 0,
                    page_count=page_count,
                    content_patched=corrected_content is not None,
                    extraction_source=extraction_source,
                ),
                decision_id=decision_id or 0,
                extracted_payload=extracted.model_dump(mode="json"),
                diffs=diffs,
                corrected_content=corrected_content,
            )
            outcome.tag_applied = True
            outcome.note_added = True
            outcome.fields_patched = len(set_fields)
            emit(
                f"Paperless updated ({outcome.fields_patched} field(s) "
                f"patched, tag + audit note added).",
                "success",
                detail={
                    "kind": "patch",
                    "paperless_id": paperless_id,
                    "applied": {
                        k: (str(v) if v is not None else None)
                        for k, v in patch_fields.items()
                    },
                    "tag": TAG_FIXED,
                    "note_added": True,
                    "content_rewritten": corrected_content is not None,
                },
            )
        except PaperlessError as exc:
            log.warning(
                "paperless verify writeback failed for %d: %s",
                paperless_id, exc,
            )
            emit(f"Writeback failed: {exc}", "error")
            outcome.skipped_reason = f"writeback failed: {exc}"
        return outcome

    async def enrich_with_context(
        self,
        paperless_id: int,
        *,
        context: EnrichmentContext,
        ai_decision_id: int | None = None,
    ) -> EnrichOutcome:
        if not context.has_signal:
            return EnrichOutcome(
                paperless_id=paperless_id,
                skipped_reason="no enrichment signal",
            )
        if not self._writeback_enabled():
            return EnrichOutcome(
                paperless_id=paperless_id,
                skipped_reason="writeback disabled in settings",
            )

        # Dedup — never post the same note twice.
        dedup_key = _enrichment_dedup_key(context)
        if self._already_enriched(paperless_id, dedup_key):
            return EnrichOutcome(
                paperless_id=paperless_id,
                skipped_reason="already enriched with this context",
            )

        outcome = EnrichOutcome(paperless_id=paperless_id)
        body = context.note_body or _render_default_enrichment(context)
        try:
            if body:
                await self.paperless.add_note(paperless_id, body)
                outcome.note_added = True
            tag_id = await self.paperless.ensure_tag(TAG_ENRICHED)
            doc = await self.paperless.get_document(paperless_id)
            merged = sorted(set(doc.tags) | {tag_id})
            await self.paperless.patch_document(paperless_id, tags=merged)
            outcome.tag_applied = True
            self._record_writeback(
                paperless_id=paperless_id,
                kind="enrichment_note",
                dedup_key=dedup_key,
                payload={"note": body, **context.__dict__},
                ai_decision_id=ai_decision_id,
            )
        except PaperlessError as exc:
            log.warning(
                "paperless enrichment writeback failed for %d: %s",
                paperless_id, exc,
            )
            outcome.skipped_reason = f"writeback failed: {exc}"
        return outcome

    # --- helpers ---------------------------------------------------

    def _load_index_row(self, paperless_id: int) -> sqlite3.Row | None:
        # Pull both the custom-field columns AND Paperless's built-in
        # correspondent_name + created_date. Per ADR-0044 those built-ins
        # ARE the vendor + receipt_date, not "fallbacks": Paperless's
        # canonical vendor IS its correspondent, its canonical document
        # date IS its created date. The prior SELECT omitted both,
        # which is why every verify run rendered "vendor: (not set),
        # receipt_date: (not set)" even when the user's Paperless doc
        # had them set — the columns were never read off disk.
        return self.conn.execute(
            """
            SELECT paperless_id, title, vendor, total_amount,
                   subtotal_amount, tax_amount, receipt_date,
                   payment_last_four, content_excerpt, mime_type,
                   correspondent_id, correspondent_name, created_date,
                   tags_json
              FROM paperless_doc_index
             WHERE paperless_id = ?
            """,
            (paperless_id,),
        ).fetchone()

    async def _fetch_and_cache_mime(self, paperless_id: int) -> str | None:
        try:
            meta = await self.paperless.get_document_metadata(paperless_id)
        except PaperlessError as exc:
            log.info("paperless metadata fetch failed for %d: %s", paperless_id, exc)
            return None
        mime = meta.get("original_mime_type") or meta.get("mime_type")
        if mime:
            self.conn.execute(
                "UPDATE paperless_doc_index SET mime_type = ? WHERE paperless_id = ?",
                (mime, paperless_id),
            )
        return mime

    async def _extract_from_ocr_text(
        self,
        paperless_id: int,
        *,
        current: dict[str, Any],
        content_excerpt: str,
        hypothesis: VerifyHypothesis | None,
    ) -> tuple[tuple[ReceiptVerification, int] | None, str | None]:
        """Cheap first-tier extraction: the primary (Haiku-class)
        model reads the OCR'd content only, no image. Returns
        ``((extracted, decision_id), None)`` on success, or
        ``(None, error_message)`` when the AI is disabled or the
        call fails. The error string is surfaced by the caller to
        the progress modal so a silent failure never repeats.
        """
        client = self.ai.new_client()
        if client is None:
            return None, "AI disabled or over monthly spend cap"
        user_prompt = _build_ocr_text_prompt(
            current=current,
            content_excerpt=content_excerpt,
            hypothesis=hypothesis,
        )
        try:
            result = await client.chat(
                decision_type="receipt_verify",
                input_ref=f"paperless:{paperless_id}:ocr_text",
                system=SYSTEM_OCR_ONLY,
                user=user_prompt,
                schema=ReceiptVerification,
                model=self.ai.ocr_text_receipt_verify_model(),
            )
        except Exception as exc:  # noqa: BLE001
            log.info(
                "paperless verify OCR-text call failed for %d "
                "(will try vision): %s",
                paperless_id, exc,
            )
            await client.aclose()
            return None, f"{type(exc).__name__}: {exc}"
        await client.aclose()
        return (result.data, result.decision_id), None

    async def _fetch_image_for_vision(
        self, paperless_id: int, source_type: SourceType,
    ) -> tuple[bytes, str, int] | None:
        """Return ``(image_bytes, mime, page_count)`` ready for the
        vision model, or ``None`` when the doc is a native PDF and
        verify should be skipped.

        For images: download the original (highest quality).
        For PDFs: download the original PDF bytes so we can introspect
          page count and text-layer presence via PyMuPDF. If the PDF
          has a text layer on page 1 it's native → return None.
          Otherwise fetch the thumbnail for the vision call.
          Multi-page is noted via the page_count return.

        The returned mime is ALWAYS sniffed from the byte content.
        Paperless's HTTP response and download_thumbnail signature
        both used to be trusted as authoritative, but Paperless can
        return ``image/webp`` while the response header (or our hard-
        coded ``image/jpeg`` fallback) claimed JPEG. Anthropic-via-
        Bedrock rejects the call with HTTP 400 when the declared mime
        and the actual bytes disagree:
          ``messages.0.content.0.image.source.base64: The image was
          specified using the image/jpeg media type, but the image
          appears to be a image/webp image``
        Sniffing makes the wire payload self-consistent.
        """
        if source_type == "image":
            bytes_, mime = await self.paperless.download_original(paperless_id)
            return bytes_, _sniff_image_mime(bytes_, mime), 1
        # PDF path — introspect first, then render.
        try:
            pdf_bytes, _ = await self.paperless.download_original(paperless_id)
        except PaperlessError as exc:
            log.info(
                "PDF download failed for %d; falling back to thumbnail: %s",
                paperless_id, exc,
            )
            pdf_bytes = b""
        page_count = 1
        if pdf_bytes:
            classified, n = classify_pdf_bytes(pdf_bytes)
            if n > 0:
                page_count = n
            if classified == "native_pdf":
                return None
        thumb_bytes, ctype = await self.paperless.download_thumbnail(paperless_id)
        return thumb_bytes, _sniff_image_mime(thumb_bytes, ctype), page_count

    def _writeback_enabled(self) -> bool:
        store = self.ai.settings_store
        raw = store.get("paperless_writeback_enabled")
        if raw is None:
            return bool(self.ai.settings.paperless_writeback_enabled)
        return str(raw).strip().lower() not in ("0", "false", "no", "off")

    def _already_enriched(self, paperless_id: int, dedup_key: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1 FROM paperless_writeback_log
             WHERE paperless_id = ?
               AND kind = 'enrichment_note'
               AND dedup_key = ?
             LIMIT 1
            """,
            (paperless_id, dedup_key),
        ).fetchone()
        return row is not None

    def _mirror_patch_to_index(
        self,
        *,
        paperless_id: int,
        patch: dict[str, Any],
        content: str | None,
    ) -> None:
        """Mirror the just-applied PATCH into ``paperless_doc_index``.

        Without this, /receipts and every other surface that reads
        from the local cache keeps showing the pre-correction values
        until the next scheduled sync. The patch dict carries the
        canonical values — this method UPDATEs whatever the patch
        actually changed; nothing else is touched.

        Best-effort: index drift is not load-bearing for correctness
        (the next sync will re-converge), so failures here log and
        return rather than rolling back the Paperless write.
        """
        cols: list[str] = []
        vals: list[Any] = []
        if "title" in patch and patch["title"]:
            cols.append("title")
            vals.append(str(patch["title"]))
        if "receipt_date" in patch and patch["receipt_date"]:
            # Paperless's `created` field maps to our `created_date`;
            # we also expose `receipt_date` separately for the custom
            # field, so update both when the canonical source agrees.
            cols.append("receipt_date")
            vals.append(str(patch["receipt_date"]))
            cols.append("created_date")
            vals.append(str(patch["receipt_date"]))
        if "vendor" in patch and patch["vendor"]:
            cols.append("vendor")
            vals.append(str(patch["vendor"]))
        if "total" in patch and patch["total"]:
            cols.append("total_amount")
            vals.append(str(patch["total"]))
        if "subtotal" in patch and patch["subtotal"]:
            cols.append("subtotal_amount")
            vals.append(str(patch["subtotal"]))
        if "tax" in patch and patch["tax"]:
            cols.append("tax_amount")
            vals.append(str(patch["tax"]))
        if "correspondent_name" in patch and patch["correspondent_name"]:
            cols.append("correspondent_name")
            vals.append(str(patch["correspondent_name"]))
        if content:
            cols.append("content_excerpt")
            # Content excerpts in the index are bounded to keep the
            # cache cheap; mirror only the head.
            vals.append(content[:4000])
        if not cols:
            return
        set_clause = ", ".join(f"{c} = ?" for c in cols)
        vals.append(paperless_id)
        self.conn.execute(
            f"UPDATE paperless_doc_index SET {set_clause}, "
            "last_synced_at = CURRENT_TIMESTAMP WHERE paperless_id = ?",
            vals,
        )
        self.conn.commit()
        log.info(
            "verify: mirrored patch into local index paperless=%d cols=%s",
            paperless_id, cols,
        )

    def _record_writeback(
        self,
        *,
        paperless_id: int,
        kind: str,
        dedup_key: str,
        payload: dict,
        ai_decision_id: int | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO paperless_writeback_log
                (paperless_id, kind, dedup_key, payload_json, ai_decision_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (paperless_id, kind, dedup_key,
             json.dumps(payload, default=str), ai_decision_id),
        )

    async def _apply_corrections(
        self,
        *,
        paperless_id: int,
        doc_row: sqlite3.Row,
        patch: dict[str, Any],
        note: str,
        decision_id: int,
        extracted_payload: dict,
        diffs: list[VerifyDiff],
        corrected_content: str | None = None,
    ) -> None:
        tag_id = await self.paperless.ensure_tag(TAG_FIXED)
        doc = await self.paperless.get_document(paperless_id)
        merged_tags = sorted(set(doc.tags) | {tag_id})

        patch_body: dict[str, Any] = {"tags": merged_tags}
        if "receipt_date" in patch:
            patch_body["created"] = patch["receipt_date"]
        if "title" in patch:
            patch_body["title"] = patch["title"]
        # OCR content rewrite — gated by caller on single-page +
        # actual diffs + non-empty corrected_content. Never reaches
        # this branch for multi-page, which is the safety contract.
        if corrected_content:
            patch_body["content"] = corrected_content

        # Correspondent correction — the MORRISON-CO-instead-of-Home-
        # Depot case. If vision identified a vendor AND the Paperless
        # correspondent disagrees, find-or-create a correspondent
        # matching the vendor and point the document at it. Gated
        # on caller having added `correspondent_name` to the patch
        # dict. Skipped silently if ensure_correspondent fails.
        if patch.get("correspondent_name"):
            try:
                corr_id = await self.paperless.ensure_correspondent(
                    str(patch["correspondent_name"]),
                )
                patch_body["correspondent"] = corr_id
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "verify: correspondent patch failed for %d: %s",
                    paperless_id, exc,
                )

        # Custom fields (total, subtotal, tax, vendor, payment_last_four)
        # need the doc's existing custom_fields list as baseline so we
        # overwrite only what changed.
        if any(
            k in patch for k in
            ("total", "subtotal", "tax", "vendor", "payment_last_four")
        ):
            patch_body["custom_fields"] = await self._merge_custom_fields(
                doc, patch,
            )

        # Last-mile audit log: exact keys + sizes the PATCH carries.
        # Critical when the user reports "content didn't get rewritten"
        # so we can tell whether ``content`` made it into the body or
        # was dropped earlier. Strings get char-counted so we don't
        # spam the log with a full receipt transcript.
        log.info(
            "verify PATCH paperless=%d keys=%s content_chars=%d title=%r created=%r",
            paperless_id,
            sorted(patch_body.keys()),
            len(patch_body.get("content") or "") if isinstance(patch_body.get("content"), str) else 0,
            patch_body.get("title"),
            patch_body.get("created"),
        )
        await self.paperless.patch_document(paperless_id, **patch_body)
        # Mirror the patched fields into ``paperless_doc_index`` so
        # /receipts and any other surface reading the local cache
        # reflects the correction immediately, instead of waiting for
        # the next scheduled sync. Without this, the user PATCHes a
        # title, then /receipts keeps showing the old title — the
        # exact "verify says success but my list looks unchanged"
        # bug. Best-effort: index update failures don't undo the
        # Paperless write that already landed.
        try:
            self._mirror_patch_to_index(
                paperless_id=paperless_id, patch=patch,
                content=corrected_content,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "verify: local index mirror failed for %d: %s",
                paperless_id, exc,
            )
        if note:
            await self.paperless.add_note(paperless_id, note)
        self._record_writeback(
            paperless_id=paperless_id,
            kind="verify_correction",
            dedup_key=_correction_dedup_key(decision_id),
            payload={
                "patch": {k: str(v) for k, v in patch.items()},
                "content_rewritten": bool(corrected_content),
                "diffs": [
                    {
                        "field": d.field, "before": d.before,
                        "after": d.after, "confidence": d.confidence,
                    }
                    for d in diffs
                ],
                "extracted": extracted_payload,
            },
            ai_decision_id=decision_id,
        )
        # Update local index so we don't re-verify against the old
        # values next time.
        self._update_local_index(
            paperless_id, patch, corrected_content=corrected_content,
        )

    async def _merge_custom_fields(
        self, doc: Document, patch: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Merge patched canonical fields into the doc's existing
        custom_fields list. Uses `paperless_field_map` to translate
        canonical roles (total, subtotal, tax, vendor,
        payment_last_four) into the Paperless field_id. Multiple
        fields can map to the same role — we write to each
        mapped field so the UI and queries stay consistent."""
        from lamella.features.paperless_bridge.field_map import get_map
        mapping = get_map(self.conn)
        existing: dict[int, Any] = {
            cf.field: cf.value for cf in doc.custom_fields
        }
        for canonical, new_val in patch.items():
            if canonical not in (
                "total", "subtotal", "tax", "vendor", "payment_last_four",
            ):
                continue
            field_ids = mapping.id_for_role(canonical)
            if not field_ids:
                continue
            value_str = str(new_val) if not isinstance(new_val, str) else new_val
            for field_id in field_ids:
                existing[field_id] = value_str
        return [{"field": fid, "value": val} for fid, val in existing.items()]

    def _update_local_index(
        self, paperless_id: int, patch: dict[str, Any],
        *, corrected_content: str | None = None,
    ) -> None:
        """Reflect corrections into `paperless_doc_index` so the
        next classify round sees the fixed values. Also updates
        content_excerpt when the OCR was rewritten so subsequent
        classify rounds don't consume the stale-and-wrong text."""
        sets: list[str] = []
        args: list[Any] = []
        mapping = {
            "receipt_date": "receipt_date",
            "vendor": "vendor",
            "total": "total_amount",
            "subtotal": "subtotal_amount",
            "tax": "tax_amount",
            "payment_last_four": "payment_last_four",
        }
        for canonical, col in mapping.items():
            if canonical in patch:
                sets.append(f"{col} = ?")
                val = patch[canonical]
                args.append(str(val) if val is not None else None)
        if corrected_content:
            # Mirror the excerpt bound — the index column holds up
            # to ~4KB; truncate defensively.
            sets.append("content_excerpt = ?")
            args.append(corrected_content[:4000])
        if not sets:
            return
        args.append(paperless_id)
        self.conn.execute(
            f"UPDATE paperless_doc_index SET {', '.join(sets)} "
            f"WHERE paperless_id = ?",
            args,
        )


# ------------------------------------------------------------------
# Pure helpers (tested directly)
# ------------------------------------------------------------------


def _current_fields_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Build the "current Paperless fields" snapshot the prompt + diff
    machinery uses as the BEFORE state.

    Paperless's built-in ``correspondent`` IS the canonical vendor.
    Its built-in ``created`` date IS the canonical receipt date.
    There is no separate "vendor" or "receipt_date" concept — those
    are custom-field role names some users CAN map, but the canonical
    Lamella read precedence per ADR-0044 / ADR-0053 is: a populated
    custom field wins, otherwise the built-in is the value (NOT a
    fallback to null). ``payment_last_four`` is no longer emitted at
    all per ADR-0044 — superseded by ``Lamella_Account`` writeback.
    """
    cols = row.keys() if hasattr(row, "keys") else []
    def _get(name: str) -> Any:
        return row[name] if name in cols else None
    # Vendor: custom field role=vendor wins, else correspondent_name.
    vendor = _get("vendor") or _get("correspondent_name")
    # Receipt date: custom field role=receipt_date wins, else created.
    receipt_date = _get("receipt_date") or _get("created_date")
    return {
        "title": _get("title"),
        "vendor": vendor,
        "receipt_date": receipt_date,
        "total": _get("total_amount"),
        "subtotal": _get("subtotal_amount"),
        "tax": _get("tax_amount"),
    }


def _compute_diffs(
    current: dict[str, Any], extracted: ReceiptVerification,
) -> list[VerifyDiff]:
    conf = extracted.confidence
    diffs: list[VerifyDiff] = []

    def _add(field_name: str, extracted_val: str, field_conf: float):
        # Empty string is the "not extracted" sentinel now that
        # every scalar field on ReceiptVerification is a plain str.
        if not extracted_val:
            return
        before = current.get(field_name)
        before_s = _stringify(before)
        after_s = _stringify(extracted_val)
        if before_s == after_s:
            return
        diffs.append(VerifyDiff(
            field=field_name, before=before_s, after=after_s,
            confidence=float(field_conf or 0.0),
        ))

    _add("receipt_date", extracted.receipt_date, conf.receipt_date)
    _add("vendor", extracted.vendor, conf.vendor)
    _add("total", extracted.total, conf.total)
    _add("subtotal", extracted.subtotal, conf.subtotal)
    _add("tax", extracted.tax, conf.tax)
    # ``payment_last_four`` intentionally NOT diffed/patched — per
    # ADR-0044 it's superseded by ``Lamella_Account``. The schema
    # still has the field for back-compat with cached decisions but
    # the system no longer surfaces it as a writeback target.
    return diffs


def _stringify(v: Any) -> str | None:
    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        # Normalize trailing zeros so "58.12" equals Decimal("58.12")
        # when stringified from either source.
        return f"{v:.2f}"
    if isinstance(v, date):
        return v.isoformat()
    return str(v).strip()


_GENERIC_TITLE_RE = re.compile(
    r"^(scan|img|image|document|doc|receipt|untitled|paperless)[_\-\s]?\d*$",
    re.IGNORECASE,
)
# Strong signals that a title was assembled by a scanner / pipeline
# rather than typed by a human: multi-digit dates wedged between
# underscored alphanumeric chunks (``..._20260309_...``), receipt /
# invoice / scan / img tokens used as connectives inside an underscored
# string (``_Receipt_``, ``_Invoice_``, ``_Scan_``), and trailing
# underscored numeric ids (``..._008762``).
_MACHINE_TITLE_FRAGMENTS = re.compile(
    r"_(?:scan|img|image|receipt|invoice|document|doc|paperless)_|"
    r"_\d{6,}_|"
    r"_\d{4,}$",
    re.IGNORECASE,
)


def _looks_generic_title(title: str | None) -> bool:
    """Detect scanner / pipeline auto-titles so we only suggest a
    descriptive replacement when the user hasn't already named the
    document themselves. User-typed titles like "Tax invoice from
    XYZ" stay untouched.

    Three classes of machine titles flagged:

    1. Pure scanner stems: ``Scan_03092026``, ``IMG_1234``,
       ``Untitled``. Matched by ``_GENERIC_TITLE_RE``.
    2. Mostly-digit titles: ``20260309_008762``. Matched by the
       digit-dominance heuristic.
    3. Underscored composite titles where the brand name was glued
       to a date / id / "_Receipt_" tag: ``BrandName_Receipt_075_
       20260309_156``. Matched by ``_MACHINE_TITLE_FRAGMENTS``
       AND the low-space-density heuristic. A real human title
       has spaces; titles with little to no whitespace and
       underscored numeric chunks are pipeline output.
    """
    if not title:
        return True
    s = title.strip()
    if not s:
        return True
    if _GENERIC_TITLE_RE.match(s):
        return True
    letters = sum(1 for c in s if c.isalpha())
    digits = sum(1 for c in s if c.isdigit())
    if letters < 3:
        return True
    # Mostly-digit titles are scanner output regardless of structure.
    if digits > letters * 2:
        return True
    # Underscored composite titles: low space density + machine
    # signatures. Threshold: ≤ 1 space per 12 chars AND a machine
    # fragment present. Real titles like "Tax invoice from Acme Inc"
    # have one space per ~5 chars and don't carry _Receipt_ tags.
    spaces = s.count(" ")
    has_machine_fragment = bool(_MACHINE_TITLE_FRAGMENTS.search(s))
    if has_machine_fragment and spaces * 12 <= len(s):
        return True
    return False


_TITLE_VENDOR_STOPWORDS = frozenset({
    "the", "and", "a", "an", "of", "co", "company", "corp", "corporation",
    "inc", "llc", "ltd", "store", "stores",
})


def _title_mentions_vendor(title: str | None, vendor: str | None) -> bool:
    """True when the title plausibly references the vendor — any
    non-stopword token from the vendor name (≥3 chars) appears in
    the title (case-insensitive). Used to decide whether a coherent-
    looking title is actually stale relative to the corrected vendor.

    The check is deliberately permissive: a title that mentions any
    substantial token of the vendor (e.g., "Acme Hardware" matching
    vendor "Acme Co Inc" via "acme") counts as a mention so we don't
    rewrite already-good titles. A title that mentions NONE of the
    vendor's substantive tokens is considered stale.
    """
    if not title or not vendor:
        return False
    title_l = title.lower()
    tokens = [
        t for t in re.findall(r"[A-Za-z0-9]+", vendor.lower())
        if len(t) >= 3 and t not in _TITLE_VENDOR_STOPWORDS
    ]
    if not tokens:
        # Only stopwords (e.g., vendor "The"): can't decide; treat as
        # not-mentioned so the rewrite path can take over.
        return False
    return any(t in title_l for t in tokens)


def _title_needs_rewrite(
    current_title: str | None,
    extracted: ReceiptVerification,
    diffs: list[VerifyDiff],
    threshold: float,
) -> bool:
    """Decide whether to replace the Paperless title.

    Two cases trigger a rewrite:

    1. The existing title looks like scanner / pipeline auto-output
       (``Scan_20260309``, ``IMG_1234``, mostly-digit gibberish) —
       handled by ``_looks_generic_title``.

    2. The verify pass corrected the vendor at high confidence AND
       the existing title doesn't mention the new vendor. The old
       title was built when Paperless thought the vendor was
       something else, so it carries misleading subject / category
       text even though it reads as a coherent sentence (the
       failure mode where a receipt whose extracted vendor was a
       city-line ends up with a category-derived title that has no
       relation to the actual merchant). Without this rule, a
       coherent-but-stale title survives every correction.

    A user-set title that already mentions the correct vendor is
    preserved — vendor not in diffs (no correction happened) or
    vendor token already present in the title returns False.
    """
    if _looks_generic_title(current_title):
        return True
    by_field = {d.field: d for d in diffs}
    vendor_diff = by_field.get("vendor")
    if vendor_diff is None or vendor_diff.confidence < threshold:
        return False
    new_vendor = (extracted.vendor or "").strip()
    if not new_vendor:
        return False
    return not _title_mentions_vendor(current_title, new_vendor)


def _suggest_title(extracted: ReceiptVerification) -> str:
    """Build a descriptive title from the extracted vendor + date +
    total. Format: ``{Vendor} - {Date} - ${Total}`` with whatever
    pieces are available. Returns empty string when there's nothing
    to work with so the caller knows to skip the title patch."""
    parts: list[str] = []
    vendor = (extracted.vendor or "").strip()
    if vendor:
        parts.append(vendor)
    date = (extracted.receipt_date or "").strip()
    if date:
        parts.append(date)
    total = (extracted.total or "").strip()
    if total:
        # Strip a trailing ".00" for cleaner display: "$18" not "$18.00".
        # Decimals like 18.84 stay as is.
        clean_total = total
        if clean_total.endswith(".00"):
            clean_total = clean_total[:-3]
        parts.append(f"${clean_total}")
    return " - ".join(parts) if vendor and (date or total) else ""


def _fields_to_patch(
    extracted: ReceiptVerification,
    diffs: list[VerifyDiff],
    *,
    threshold: float,
    current_correspondent: str | None = None,
    current_title: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Return (patch_dict, list of fields actually set). `patch`
    keys are canonical names matching VerifyDiff.field; values are
    the extracted values in their natural types.

    When the extracted vendor differs from the current Paperless
    correspondent AND the vendor-confidence is above threshold,
    also populate ``patch["correspondent_name"]`` so
    _apply_corrections can ensure_correspondent + PATCH the doc's
    correspondent id. This is the Home-Depot-mislabeled-as-
    MORRISON-CO recovery path.
    """
    by_field: dict[str, VerifyDiff] = {d.field: d for d in diffs}
    patch: dict[str, Any] = {}
    applied: list[str] = []
    # All canonical values are now plain strings — empty string is
    # the "not extracted" sentinel, and the Paperless patch payload
    # accepts ISO date / decimal-ASCII strings directly. No more
    # isinstance(date) or isinstance(Decimal) branching.
    for field_name, canonical_val in (
        ("receipt_date", extracted.receipt_date),
        ("vendor", extracted.vendor),
        ("total", extracted.total),
        ("subtotal", extracted.subtotal),
        ("tax", extracted.tax),
        # payment_last_four intentionally absent — ADR-0044.
    ):
        diff = by_field.get(field_name)
        if diff is None:
            continue
        if diff.confidence < threshold:
            continue
        if not canonical_val:
            continue
        patch[field_name] = canonical_val
        applied.append(field_name)
    # Correspondent correction signal — independent of the field
    # diffs above. If vision gave us a vendor with high confidence
    # and the doc's current correspondent disagrees, patch the
    # correspondent too.
    vendor_conf = float(extracted.confidence.vendor)
    if (
        extracted.vendor
        and vendor_conf >= threshold
        and (current_correspondent or "").strip().lower()
            != (extracted.vendor or "").strip().lower()
    ):
        patch["correspondent_name"] = extracted.vendor.strip()
        applied.append("correspondent")
    # Title suggestion. Two trigger paths (see _title_needs_rewrite):
    # the existing title looks like scanner / Paperless auto-output,
    # OR the vendor was corrected by this pass and the title doesn't
    # mention the new vendor (catches coherent-but-stale titles like
    # "Receipt for Water Purchase on 2026-03-31" on a receipt whose
    # vendor flipped from a city-line extraction to a national
    # chain). User-set titles that already match the corrected
    # vendor stay untouched.
    if _title_needs_rewrite(current_title, extracted, diffs, threshold):
        suggested = _suggest_title(extracted)
        if (
            suggested
            and float(extracted.confidence.vendor) >= threshold
            and (
                float(extracted.confidence.receipt_date) >= threshold
                or float(extracted.confidence.total) >= threshold
            )
        ):
            patch["title"] = suggested
            applied.append("title")
    return patch, applied


def _render_verify_note(
    extracted: ReceiptVerification,
    diffs: list[VerifyDiff],
    decision_id: int,
    *,
    page_count: int = 1,
    content_patched: bool = False,
    extraction_source: str = "vision",
) -> str:
    lines: list[str] = []
    if extraction_source == "ocr_text":
        lines.append(
            "🤖 Lamella verified this receipt against the OCR text "
            "(cheap pass — no vision call needed)."
        )
    else:
        lines.append("🤖 Lamella verified this receipt against the image.")
    if page_count > 1:
        lines.append("")
        lines.append(
            f"⚠ Only page 1 of this {page_count}-page PDF was sent "
            f"to the vision model. Corrections below apply to "
            f"page-1 fields (date, vendor, total, etc.) only. The "
            f"original OCR'd content for pages 2+ was NOT "
            f"modified or overwritten."
        )
    lines.append("")
    if diffs:
        lines.append("Fields changed:")
        for d in diffs:
            lines.append(
                f"  • {d.field}: {d.before or '(empty)'} → "
                f"{d.after or '(empty)'} (conf {d.confidence:.2f})"
            )
        if content_patched:
            lines.append(
                "  • content: OCR text re-extracted and replaced "
                "with corrected version."
            )
    else:
        lines.append("No changes needed — existing fields matched the image.")
    if extracted.ocr_errors_noted:
        lines.append("")
        lines.append("OCR issues noted:")
        for err in extracted.ocr_errors_noted:
            lines.append(f"  • {err}")
    if extracted.reasoning:
        lines.append("")
        lines.append(f"Reasoning: {extracted.reasoning}")
    lines.append("")
    lines.append(f"(ai_decisions id: {decision_id})")
    return "\n".join(lines)


def _correction_dedup_key(decision_id: int) -> str:
    return f"decision:{decision_id}"


def _enrichment_dedup_key(context: EnrichmentContext) -> str:
    h = hashlib.sha1()
    h.update((context.note_body or "").encode("utf-8"))
    h.update(b"\x00")
    h.update((context.vehicle or "").encode("utf-8"))
    h.update(b"\x00")
    h.update((context.entity or "").encode("utf-8"))
    h.update(b"\x00")
    h.update((context.project or "").encode("utf-8"))
    return h.hexdigest()[:16]


def _render_default_enrichment(context: EnrichmentContext) -> str:
    parts: list[str] = []
    if context.vehicle:
        parts.append(f"Vehicle: {context.vehicle}")
    if context.entity:
        parts.append(f"Entity: {context.entity}")
    if context.project:
        parts.append(f"Project: {context.project}")
    if not parts:
        return ""
    return "🤖 Lamella context: " + " · ".join(parts)
