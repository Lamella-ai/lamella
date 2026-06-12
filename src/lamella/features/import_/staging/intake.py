# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generic / pasted-text intake — NEXTGEN.md Phase D.

Accepts tabular data from any shape (CSV, TSV, whitespace-aligned
pasted text, parsed spreadsheet rows) and lands it on the unified
staging surface with ``source='paste'``. Sits alongside the
importer (bulk upload of known-shape CSVs with canonical source
classes) and SimpleFIN (live bridge fetch) as the third intake
path the Phase C matcher + Phase B2 review UI consume.

The core idea: *no new source class required*. The user pastes
a bank statement snippet, we heuristically identify which columns
carry date / amount / description, optionally refine via the AI
service, and stage the rows. Downstream pipeline (matcher,
classification, review) works identically to rows from any other
source.
"""
from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from lamella.features.import_.staging.service import StagedRow, StagingService

log = logging.getLogger(__name__)

__all__ = [
    "CANONICAL_FIELDS",
    "DuplicateReport",
    "IntakeError",
    "IntakeResult",
    "IntakeService",
    "ParsedPaste",
    "RowMatch",
    "SessionOverlap",
    "content_fingerprint",
    "detect_columns_by_content",
    "detect_paste_duplicates",
    "heuristic_column_map",
    "merge_maps",
    "parse_pasted_text",
    "propose_column_map_via_ai",
]


# Canonical fields we care about when staging a row from pasted text.
# Narrower than the importer's column list because pasted text rarely
# carries annotation columns — those are a per-user spreadsheet thing.
CANONICAL_FIELDS: tuple[str, ...] = (
    "date",
    "amount",
    "currency",
    "payee",
    "description",
    "memo",
)


class IntakeError(Exception):
    """Intake refused or couldn't parse."""


# --- parsed preview -----------------------------------------------------


@dataclass
class ParsedPaste:
    """The pasted text parsed into columns + rows.

    ``columns`` is the header row (derived or supplied). ``rows``
    is the list of data rows, each a list of cells indexed by
    ``columns``. ``delimiter_guess`` records what the parser
    decided ("," "\\t" "whitespace") so callers can surface it to
    the user.
    """
    columns: list[str]
    rows: list[list[Any]]
    delimiter_guess: str
    header_row_index: int = 0
    notes: str | None = None


# --- pasted-text parser -------------------------------------------------


# Heuristic: if the text has tabs in most lines, it's TSV. Else if
# commas dominate, it's CSV. Else fall back to whitespace splitting.
def _guess_delimiter(text: str) -> str:
    # Count delimiters on non-empty lines; whichever dominates wins.
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise IntakeError("pasted text is empty")
    tab_hits = sum(1 for ln in lines if "\t" in ln)
    comma_hits = sum(1 for ln in lines if "," in ln)
    if tab_hits >= len(lines) * 0.6:
        return "\t"
    if comma_hits >= len(lines) * 0.6:
        return ","
    return "whitespace"


def _parse_csv_like(text: str, delimiter: str) -> list[list[str]]:
    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(text), delimiter=delimiter, skipinitialspace=True)
    for raw in reader:
        stripped = [c.strip() for c in raw]
        if any(stripped):
            rows.append(stripped)
    return rows


_WS_RUN = re.compile(r"\s{2,}|\t+")


def _parse_whitespace(text: str) -> list[list[str]]:
    """Split each line on runs of 2+ whitespace (so single spaces in
    a description don't break the column). Works for statement-style
    aligned output like:

        2026-04-20  ONLINE.MARKETPLACE   -12.34
        2026-04-21  COFFEE SHOP          -4.50
    """
    rows: list[list[str]] = []
    for raw in text.splitlines():
        if not raw.strip():
            continue
        parts = [p.strip() for p in _WS_RUN.split(raw) if p.strip()]
        if parts:
            rows.append(parts)
    return rows


def parse_pasted_text(
    text: str, *, has_header: bool = True,
) -> ParsedPaste:
    """Parse pasted text into columns + rows.

    Auto-detects delimiter (tab / comma / whitespace). When
    ``has_header`` is true and the first row doesn't contain numeric
    cells, it's treated as the header. Otherwise synthetic column
    names (``col_1``, ``col_2``, …) are generated so the caller can
    still index by name.
    """
    if not text or not text.strip():
        raise IntakeError("pasted text is empty")

    delim = _guess_delimiter(text)
    if delim == "whitespace":
        rows = _parse_whitespace(text)
    else:
        rows = _parse_csv_like(text, delim)

    if not rows:
        raise IntakeError("no rows found in pasted text")

    columns: list[str]
    header_row_index = 0
    if has_header and _looks_like_header(rows[0]):
        columns = [c or f"col_{i + 1}" for i, c in enumerate(rows[0])]
        data_rows = rows[1:]
    else:
        width = max(len(r) for r in rows)
        columns = [f"col_{i + 1}" for i in range(width)]
        data_rows = rows
        header_row_index = -1  # no header row

    # Normalize row widths to the header width.
    normalized: list[list[Any]] = []
    for r in data_rows:
        if len(r) < len(columns):
            r = r + [""] * (len(columns) - len(r))
        elif len(r) > len(columns):
            r = r[: len(columns)]
        normalized.append(r)

    return ParsedPaste(
        columns=columns,
        rows=normalized,
        delimiter_guess=delim,
        header_row_index=header_row_index,
    )


def _looks_like_header(row: list[str]) -> bool:
    """Header rows are mostly non-empty text with no parseable dates
    or numbers. Data rows typically have at least one cell that
    parses as a date or a number."""
    non_empty = [c for c in row if c and c.strip()]
    if len(non_empty) < 2:
        return False
    numeric = 0
    for cell in non_empty:
        if _parse_amount(cell) is not None or _parse_date(cell) is not None:
            numeric += 1
    return numeric == 0


# --- cell parsers -------------------------------------------------------


_DATE_PATTERNS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%b %d %Y",
    "%b %d, %Y",
    "%d %b %Y",
    "%B %d %Y",
    "%B %d, %Y",
)


def _parse_date(cell: Any) -> str | None:
    if cell is None:
        return None
    s = str(cell).strip()
    if not s:
        return None
    for fmt in _DATE_PATTERNS:
        try:
            d = datetime.strptime(s, fmt).date()
            return d.isoformat()
        except ValueError:
            continue
    return None


_AMOUNT_CLEANUP = re.compile(r"[,\s]|USD|\$")


def _parse_amount(cell: Any) -> Decimal | None:
    if cell is None:
        return None
    s = str(cell).strip()
    if not s:
        return None
    # Strip currency markers and thousands separators. Parentheses
    # mean negative (accounting notation).
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    cleaned = _AMOUNT_CLEANUP.sub("", s)
    try:
        val = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None
    return -val if negative else val


# --- column-map heuristic (reused by AI-less intake paths) --------------


async def propose_column_map_via_ai(
    parsed: ParsedPaste,
    *,
    ai_service,
    input_ref: str,
) -> dict[str, str | None] | None:
    """Phase D2 — AI refinement of the column map.

    Adapts a ``ParsedPaste`` into the importer's ``SheetPreview``
    shape and delegates to the existing
    ``importer/mapping.py::propose_mapping`` flow. The AI call is
    logged + cached via the AI decisions table like every other
    ``classify_*`` decision. Returns the AI-proposed map, or
    ``None`` when the AI service is disabled / the call fails —
    callers fall back to the heuristic.
    """
    if ai_service is None or not getattr(ai_service, "enabled", False):
        return None
    try:
        from lamella.features.import_.mapping import propose_mapping
        from lamella.features.import_.preview import SheetPreview
    except Exception:  # noqa: BLE001
        return None
    preview = SheetPreview(
        sheet_name=f"paste-{input_ref}",
        columns=list(parsed.columns),
        rows=[list(r) for r in parsed.rows[:20]],
        row_count=len(parsed.rows),
        header_row_index=max(parsed.header_row_index, 0),
    )
    try:
        result = await propose_mapping(
            ai_service, preview=preview, input_ref=input_ref,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("intake AI column-map refinement failed: %s", exc)
        return None
    return dict(result.column_map)


def merge_maps(
    heuristic: dict[str, str | None],
    ai: dict[str, str | None] | None,
) -> dict[str, str | None]:
    """Combine heuristic + AI column maps.

    Rule: the AI suggestion wins only where the heuristic produced
    ``None``. The heuristic's positive matches are trusted because
    they're exact-pattern (no latency, no spend, deterministic).
    The AI fills the gaps.
    """
    if not ai:
        return dict(heuristic)
    out = dict(heuristic)
    for col, canonical in out.items():
        if canonical is None and col in ai and ai[col]:
            out[col] = ai[col]
    return out


_HEURISTIC_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^date$|posted|transaction.?date|^txn.?date", re.I), "date"),
    (re.compile(r"^amount$|^amt$|^debit$|^credit$", re.I), "amount"),
    (re.compile(r"^currency$|^ccy$", re.I), "currency"),
    (re.compile(r"payee|merchant|vendor|counterparty", re.I), "payee"),
    (re.compile(r"description|narration|details|particulars|name", re.I), "description"),
    (re.compile(r"memo|notes?|ref|reference", re.I), "memo"),
]


def heuristic_column_map(columns: list[str]) -> dict[str, str | None]:
    """Best-guess field mapping from column headers alone.

    Returns ``{source_col: canonical_field_or_None}``. When the AI
    service is unavailable or the caller doesn't want to spend a
    token, this is the fallback."""
    out: dict[str, str | None] = {}
    for col in columns:
        mapped: str | None = None
        for pat, canonical in _HEURISTIC_RULES:
            if pat.search(col):
                mapped = canonical
                break
        out[col] = mapped
    return out


def detect_columns_by_content(parsed: ParsedPaste) -> dict[str, str | None]:
    """Sniff the first N rows for date-shaped and number-shaped cells
    and propose a column map from content alone — useful when the
    pasted text has no header row (``header_row_index == -1``) or
    the headers are nondescript.

    Returns the same ``{source_col: canonical}`` shape as
    ``heuristic_column_map``.
    """
    if not parsed.rows:
        return {c: None for c in parsed.columns}

    # For each column, count how many sampled cells parse as date, as
    # amount, or look like text. Column gets assigned to the role its
    # values most resemble.
    sample = parsed.rows[: min(len(parsed.rows), 10)]
    mapping: dict[str, str | None] = {}
    used_roles: set[str] = set()
    col_scores: list[tuple[str, int, int, int]] = []  # (col, dates, amounts, texts)
    for idx, col in enumerate(parsed.columns):
        dates = amounts = texts = 0
        for row in sample:
            val = row[idx] if idx < len(row) else ""
            if _parse_date(val) is not None:
                dates += 1
            elif _parse_amount(val) is not None:
                amounts += 1
            elif str(val).strip():
                texts += 1
        col_scores.append((col, dates, amounts, texts))

    # First pass: header-based (exact match on canonical names).
    header_map = heuristic_column_map(parsed.columns)
    for col, role in header_map.items():
        if role is not None:
            mapping[col] = role
            used_roles.add(role)

    # Second pass: content-based assignment for columns not yet mapped.
    # Pick the single best column for each unassigned role.
    # Dates: column with most date-parseable cells.
    if "date" not in used_roles:
        best = max(
            (c for c in col_scores if mapping.get(c[0]) is None),
            key=lambda x: (x[1], -x[2], -x[3]),
            default=None,
        )
        if best and best[1] >= max(1, len(sample) // 2):
            mapping[best[0]] = "date"
            used_roles.add("date")
    # Amounts: column with most number-parseable cells.
    if "amount" not in used_roles:
        best = max(
            (c for c in col_scores if mapping.get(c[0]) is None),
            key=lambda x: (x[2], -x[1]),
            default=None,
        )
        if best and best[2] >= max(1, len(sample) // 2):
            mapping[best[0]] = "amount"
            used_roles.add("amount")
    # Description: longest textual column.
    if "description" not in used_roles:
        best = max(
            (c for c in col_scores if mapping.get(c[0]) is None),
            key=lambda x: (x[3],),
            default=None,
        )
        if best and best[3] >= 1:
            mapping[best[0]] = "description"
            used_roles.add("description")

    # Fill remaining columns with None so callers can see "dropped".
    for col in parsed.columns:
        mapping.setdefault(col, None)
    return mapping


# --- duplicate detection ------------------------------------------------


_WS_COLLAPSE = re.compile(r"\s+")


def _normalize_desc(text: str | None) -> str:
    """Lowercase + strip + collapse whitespace. The noise we want to
    ignore when deciding whether two merchant texts are "the same":
    capitalization, surrounding whitespace, and runs of spaces."""
    if not text:
        return ""
    return _WS_COLLAPSE.sub(" ", str(text).lower()).strip()


def content_fingerprint(
    *,
    posting_date: str,
    amount: Decimal | str | int | float,
    description: str | None,
) -> str:
    """Stable hash over (posting_date, abs(amount), normalized_description).

    Source-agnostic on purpose: a SimpleFIN txn and a pasted row that
    represent the same real-world transaction should produce the same
    fingerprint. Sign is stripped because the sign tells us direction,
    but two sides of the same transfer share an abs-value.
    """
    try:
        amt = (
            amount if isinstance(amount, Decimal)
            else Decimal(str(amount))
        )
    except (InvalidOperation, ValueError):
        amt = Decimal(0)
    abs_amt = abs(amt).quantize(Decimal("0.01"))
    payload = f"{posting_date}|{abs_amt}|{_normalize_desc(description)}"
    return hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False).hexdigest()


@dataclass(frozen=True)
class RowMatch:
    """One incoming row's match against history."""
    row_index: int
    fingerprint: str
    matched_staged_ids: tuple[int, ...]
    matched_sources: tuple[str, ...]


@dataclass(frozen=True)
class SessionOverlap:
    """Aggregate: how many incoming rows match a given prior session.

    ``source='simplefin', session_id=None`` is a common case — it
    means "SimpleFIN rows not scoped to any single ingest" (the
    simplefin ingest uses the ingest_id as session_id so it's
    always set, but older rows may predate that).
    """
    source: str
    session_id: str | None
    matches: int
    earliest_date: str | None
    latest_date: str | None


@dataclass(frozen=True)
class DuplicateReport:
    """Summary of fuzzy duplicate detection for one paste.

    * ``overlap_ratio`` is matched_rows / total_rows.
    * ``severity`` is the UX-facing band: ``none`` (< 30%),
      ``partial`` (30–80%), ``high`` (>= 80%).
    * ``likely_duplicate_sessions`` ranks the prior sessions whose
      rows match the incoming paste most; the top entry is usually
      the previous paste of the same statement, or the SimpleFIN
      ingest that already covered the same period.
    """
    total_rows: int
    fingerprinted_rows: int
    matched_rows: int
    overlap_ratio: float
    severity: str
    matches: tuple[RowMatch, ...]
    likely_duplicate_sessions: tuple[SessionOverlap, ...]


def _canonical_row_fields(
    row: list[Any],
    *,
    column_map: dict[str, str | None],
    columns: list[str],
) -> tuple[str | None, Decimal | None, str | None]:
    """Pull the canonical (date, amount, description) out of a paste row
    using the user's column map. Shared with IntakeService.stage_paste."""
    idx: dict[str, int] = {}
    for src, canonical in column_map.items():
        if not canonical:
            continue
        try:
            idx.setdefault(canonical, columns.index(src))
        except ValueError:
            continue

    def _at(field_name: str) -> Any:
        if field_name not in idx:
            return None
        i = idx[field_name]
        if i >= len(row):
            return None
        return row[i]

    date_str = _parse_date(_at("date"))
    amt = _parse_amount(_at("amount"))
    desc = _at("description")
    desc_str = str(desc) if desc is not None else None
    return date_str, amt, desc_str


def detect_paste_duplicates(
    conn: sqlite3.Connection,
    parsed: ParsedPaste,
    column_map: dict[str, str | None],
    *,
    date_window_days: int = 60,
    partial_threshold: float = 0.30,
    high_threshold: float = 0.80,
) -> DuplicateReport:
    """Compare the incoming paste against recent staged history.

    For each row we can fingerprint (needs a parseable date + amount),
    we query ``staged_transactions`` within ±``date_window_days`` of
    that row's date and check for content matches. Matches are
    grouped by ``(source, session_id)`` so the report can surface
    "this paste overlaps 9/10 with your paste from last Tuesday"
    (same source, different session) *and* "this paste overlaps 8/10
    with SimpleFIN's fetch from the same window" (different source).
    Both are real duplicate cases the user needs to resolve.
    """
    # 1. Fingerprint the incoming rows.
    incoming: list[tuple[int, str, str]] = []  # (row_idx, fingerprint, date)
    for row_idx, row in enumerate(parsed.rows):
        date_str, amt, desc = _canonical_row_fields(
            row, column_map=column_map, columns=parsed.columns,
        )
        if date_str is None or amt is None:
            continue
        fp = content_fingerprint(
            posting_date=date_str, amount=amt, description=desc,
        )
        incoming.append((row_idx, fp, date_str))

    total = len(parsed.rows)
    fingerprinted = len(incoming)
    if not incoming:
        return DuplicateReport(
            total_rows=total,
            fingerprinted_rows=0,
            matched_rows=0,
            overlap_ratio=0.0,
            severity="none",
            matches=(),
            likely_duplicate_sessions=(),
        )

    # 2. Pull a bounded slice of history — only rows in the right date
    #    window. Fingerprint each one in Python; at realistic scale
    #    (thousands of historical rows) this is cheap.
    dates = sorted(d for _, _, d in incoming)
    min_d, max_d = dates[0], dates[-1]
    history = conn.execute(
        """
        SELECT id, source, session_id, posting_date, amount, description
          FROM staged_transactions
         WHERE posting_date BETWEEN date(?, ?) AND date(?, ?)
        """,
        (min_d, f"-{date_window_days} days", max_d, f"+{date_window_days} days"),
    ).fetchall()

    history_by_fp: dict[str, list[sqlite3.Row]] = {}
    for r in history:
        try:
            amt = Decimal(r["amount"])
        except (InvalidOperation, ValueError):
            continue
        fp = content_fingerprint(
            posting_date=r["posting_date"],
            amount=amt,
            description=r["description"],
        )
        history_by_fp.setdefault(fp, []).append(r)

    # 3. Walk incoming rows, collect matches.
    matches: list[RowMatch] = []
    per_session_matches: dict[
        tuple[str, str | None],
        list[tuple[int, str]],  # (incoming_row_idx, date)
    ] = {}
    for row_idx, fp, date_str in incoming:
        if fp not in history_by_fp:
            continue
        hits = history_by_fp[fp]
        matches.append(
            RowMatch(
                row_index=row_idx,
                fingerprint=fp,
                matched_staged_ids=tuple(int(h["id"]) for h in hits),
                matched_sources=tuple(sorted({h["source"] for h in hits})),
            )
        )
        for h in hits:
            key = (h["source"], h["session_id"])
            per_session_matches.setdefault(key, []).append((row_idx, date_str))

    matched_rows = len(matches)
    overlap = matched_rows / total if total else 0.0
    if overlap >= high_threshold:
        severity = "high"
    elif overlap >= partial_threshold:
        severity = "partial"
    else:
        severity = "none"

    session_overlaps: list[SessionOverlap] = []
    for (source, session_id), hits in per_session_matches.items():
        dates_hit = sorted({d for _, d in hits})
        session_overlaps.append(
            SessionOverlap(
                source=source,
                session_id=session_id,
                matches=len({i for i, _ in hits}),
                earliest_date=dates_hit[0] if dates_hit else None,
                latest_date=dates_hit[-1] if dates_hit else None,
            )
        )
    session_overlaps.sort(key=lambda s: s.matches, reverse=True)

    return DuplicateReport(
        total_rows=total,
        fingerprinted_rows=fingerprinted,
        matched_rows=matched_rows,
        overlap_ratio=round(overlap, 3),
        severity=severity,
        matches=tuple(matches),
        likely_duplicate_sessions=tuple(session_overlaps[:5]),
    )


# --- intake service -----------------------------------------------------


@dataclass
class IntakeResult:
    """Summary of a paste-intake run."""
    session_id: str
    total_rows: int = 0
    staged: int = 0
    skipped: int = 0
    duplicates_flagged: int = 0
    errors: list[str] = field(default_factory=list)
    staged_ids: list[int] = field(default_factory=list)


class IntakeService:
    """Stage rows from a ParsedPaste + column map into the unified
    staging surface."""

    def __init__(self, conn):
        self.conn = conn
        self.staging = StagingService(conn)

    def stage_paste(
        self,
        *,
        session_id: str,
        parsed: ParsedPaste,
        column_map: dict[str, str | None],
        currency_default: str = "USD",
        source: str = "paste",
        archived_file_id: int | None = None,
    ) -> IntakeResult:
        """Stage each row of ``parsed`` with the given column map.

        ``column_map`` maps verbatim source columns → canonical
        fields (``date``, ``amount``, ``currency``, ``payee``,
        ``description``, ``memo``) or ``None`` to drop the column.

        Rows missing a parseable ``date`` or ``amount`` are skipped
        with a note in the result — staging requires both.

        ADR-0060 — when ``archived_file_id`` is supplied the
        ``source_ref`` for each staged row uses the file-scoped
        ``{"file_id": <int>, "row": <int>}`` shape, so re-importing
        the same archived paste lands the same source_ref_hash and
        the upsert path keeps state in place. When the caller
        doesn't archive (legacy / unit-test paths), the row uses
        the older session-scoped ``{"session_id", "row_index"}``
        shape — useful for tests but production routes should
        always archive first.
        """
        if source not in {"paste", "reboot"}:
            raise IntakeError(
                f"source {source!r} not accepted by paste intake — use 'paste' or 'reboot'"
            )

        # Build index: canonical_field → source column index
        field_to_idx: dict[str, int] = {}
        for source_col, canonical in column_map.items():
            if not canonical:
                continue
            try:
                idx = parsed.columns.index(source_col)
            except ValueError:
                continue
            # First mapping wins; the user can narrow ambiguity
            # explicitly via the UI later.
            field_to_idx.setdefault(canonical, idx)

        result = IntakeResult(session_id=session_id, total_rows=len(parsed.rows))
        missing_date = missing_amount = 0
        for row_idx, row in enumerate(parsed.rows):
            date = _extract(row, field_to_idx.get("date"))
            amount = _extract(row, field_to_idx.get("amount"))
            parsed_date = _parse_date(date) if date is not None else None
            parsed_amount = _parse_amount(amount) if amount is not None else None
            if parsed_date is None:
                missing_date += 1
                result.skipped += 1
                continue
            if parsed_amount is None:
                missing_amount += 1
                result.skipped += 1
                continue

            currency = _extract(row, field_to_idx.get("currency")) or currency_default
            payee = _extract(row, field_to_idx.get("payee"))
            description = _extract(row, field_to_idx.get("description"))
            memo = _extract(row, field_to_idx.get("memo"))

            # source_ref uses the archived file_id + row when an
            # archive has been minted (ADR-0060), so re-importing
            # the same archived paste lands the same hash and the
            # upsert path keeps state in place. Falls back to
            # session-scoped keys when the caller didn't archive.
            if archived_file_id is not None:
                source_ref = {
                    "file_id": archived_file_id,
                    "row": row_idx,
                }
            else:
                source_ref = {
                    "session_id": session_id,
                    "row_index": row_idx,
                }
            raw = {
                parsed.columns[i]: row[i]
                for i in range(min(len(parsed.columns), len(row)))
            }
            staged = self.staging.stage(
                source=source,
                source_ref=source_ref,
                session_id=session_id,
                posting_date=parsed_date,
                amount=parsed_amount,
                currency=str(currency).upper() if currency else "USD",
                payee=payee,
                description=description,
                memo=memo,
                raw=raw,
            )
            result.staged += 1
            result.staged_ids.append(staged.id)

        if missing_date:
            result.errors.append(
                f"{missing_date} row(s) skipped — no parseable date column"
            )
        if missing_amount:
            result.errors.append(
                f"{missing_amount} row(s) skipped — no parseable amount column"
            )
        return result


def _extract(row: list[Any], idx: int | None) -> Any:
    if idx is None:
        return None
    if idx >= len(row):
        return None
    val = row[idx]
    if val is None or (isinstance(val, str) and not val.strip()):
        return None
    return val
