# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Content-based detection for the structured financial formats — OFX
(plus the Intuit variants QFX/QBO), QIF, and IIF.

The CSV/XLSX/ODS importers all flow through ``preview_workbook`` which
opens the file as a tabular DataFrame. These five formats are *not*
tabular — they're SGML/XML/line-prefixed/tab-record streams — so they
need their own detection and a single-sheet preview shape.

Detection runs on the first ~4KB of the file. Order matters: OFX's
header is the most distinctive (literal ``OFXHEADER:100`` or an XML
``<?OFX`` PI), so it goes first. QFX and QBO are OFX with an
``INTU.BID`` field — same parser, different traceability tag. IIF and
QIF are last because their leading ``!``-marker is generic enough that
content sniffing should rule out the OFX family first.

The format names returned here line up with the ``source_class`` keys
registered in ``importer.sources.INGESTERS`` and in
``importer.classify.KNOWN_SOURCE_CLASSES``.
"""
from __future__ import annotations

import re
from pathlib import Path

# Format names — match the source_class keys in INGESTERS.
OFX = "ofx"
QFX = "qfx"
QBO = "qbo"
QIF = "qif"
IIF = "iif"

STRUCTURED_FORMATS: tuple[str, ...] = (OFX, QFX, QBO, QIF, IIF)

_OFX_HEADER_RE = re.compile(r"^\s*OFXHEADER\s*[:=]\s*100", re.I | re.M)
_OFX_XML_PI_RE = re.compile(r"<\?OFX\b", re.I)
_OFX_ROOT_RE = re.compile(r"<OFX\b", re.I)
_INTU_BID_RE = re.compile(r"\bINTU\.BID\b", re.I)
_QIF_HEADER_RE = re.compile(
    r"^\s*!Type:(Bank|Cash|CCard|Invst|Oth\s*A|Oth\s*L|Bills|Memorized|Class|Cat|Account)\b",
    re.I | re.M,
)
_IIF_HEADER_RE = re.compile(r"^!(HDR|ACCNT|TRNS|CUST|VEND|EMP|CLASS|INVITEM)\b", re.I | re.M)


def _sniff_text(path: Path, *, max_bytes: int = 4096) -> str:
    """Return the first chunk of the file as a best-effort text decode.

    Banks export OFX with a wide range of charsets — Latin-1 is the
    safest "never raises" fallback, and the marker bytes we look for
    are all 7-bit ASCII so encoding mismatches don't matter for
    detection. We don't seek past ``max_bytes``.
    """
    try:
        with path.open("rb") as fh:
            blob = fh.read(max_bytes)
    except OSError:
        return ""
    # All format markers are ASCII; latin-1 always decodes.
    return blob.decode("latin-1", errors="replace")


def sniff_format(path: Path) -> str | None:
    """Return one of OFX/QFX/QBO/QIF/IIF if `path` matches; None otherwise.

    Pure content-based: extension is *not* consulted. Callers that want
    the extension as a tiebreaker (e.g., distinguishing QFX vs QBO when
    both sniff as OFX+INTU.BID) layer that on top via
    :func:`refine_with_extension`.
    """
    head = _sniff_text(path)
    if not head:
        return None
    return sniff_format_text(head)


def sniff_format_text(head: str) -> str | None:
    """Same as :func:`sniff_format` but takes the already-loaded prefix.

    Exposed separately so tests can drive detection from inline strings
    without writing a fixture file.
    """
    if not head:
        return None
    looks_ofx = bool(
        _OFX_HEADER_RE.search(head)
        or (_OFX_XML_PI_RE.search(head) or _OFX_ROOT_RE.search(head))
    )
    if looks_ofx:
        # Intuit's variants embed an INTU.BID field in either the SGML
        # header block or an inner <SONRQ> element. We can't tell QFX
        # from QBO by content alone; both report as 'qfx' here and the
        # caller refines with the extension if it cares.
        if _INTU_BID_RE.search(head):
            return QFX
        return OFX
    if _IIF_HEADER_RE.search(head):
        return IIF
    if _QIF_HEADER_RE.search(head):
        return QIF
    return None


def refine_with_extension(fmt: str | None, path: Path) -> str | None:
    """Disambiguate QFX vs QBO based on the file extension.

    The two formats are byte-identical at the file level — they only
    differ by which downstream Intuit product expects them. We keep
    the source_class distinct so a re-import of the same QBO export
    doesn't get re-tagged as QFX, but the parser is shared.
    """
    if fmt != QFX:
        return fmt
    ext = path.suffix.lower().lstrip(".")
    if ext == "qbo":
        return QBO
    return QFX


def detect(path: Path) -> str | None:
    """Convenience: sniff + refine. Returns final source_class or None."""
    return refine_with_extension(sniff_format(path), path)
