# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Parsers for the mileage import page.

Two input shapes the user provides:

**Format A — single-anchor odometer readings.**
Each line is a mileage marker with an optional time. Trips are
derived between consecutive anchors on the same vehicle::

    2025-01-01 08:00 69001 Home, set out for Warehouse Club
    01/01/2025 12:00 PM 69011 At Warehouse Club, shopping complete
    2025-01-02 16:00 69011 Leaving home for post office
    01/02/2025 4:10pm 69015 went to post office

**Format B — explicit start + end per row.**
Each line is a complete trip with both odometer readings::

    2025-01-01,69001,69011,Home → Warehouse Club
    2025-01-02,69011,69015,Home → post office

**Format C — day-log line with multiple inline anchors.**
A single dated line carrying every odometer reading from that
day, mixed with descriptive text. The line collapses to **one**
preview row: start = first odometer on the line, end = last,
miles = delta, description = the full line text with odometer
numbers stripped::

    November 30, 2024 211,999 – START Logan, Utah Gas, Chevron $77.07 –
        212,039 Gas, Maverick $49.06 – 212,284 → Warehouse 212,523

…produces one row (211,999 → 212,523, 524 miles) with the merged
description text. Intermediary odometer readings are informational
only — the user's expectation is "one row per dated entry."

Dates are flexible: ``YYYY-MM-DD``, ``MM/DD/YYYY``, ``MM/DD/YY``,
or long-form ``December 3, 2024`` / ``Dec 3 2024``. Times optional,
12- or 24-hour. Commas in odometer numbers (``69,001``) are tolerated.

CSV uploads: a file whose header matches the canonical vehicles.csv
(``date,vehicle,odometer_start,odometer_end,miles,purpose,entity,from,to,notes``)
is parsed as Format B with a known column map. Anything else falls
through to the free-text parser.
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, replace
from datetime import date as date_t
from typing import Iterable

from lamella.features.mileage.service import ImportPreviewRow


_MONTH_NAMES = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sept": 9, "sep": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# Written-out month + day + year: "December 3, 2024", "Dec. 3 2024",
# "3 December 2024" (day-first variant some users paste).
_MONTH_NAME_RE = "|".join(_MONTH_NAMES.keys())
_LONG_DATE_MDY_RE = re.compile(
    rf"^(?P<mon>{_MONTH_NAME_RE})\.?\s+(?P<day>\d{{1,2}})(?:\s*,\s*|\s+)(?P<year>\d{{4}})$",
    re.IGNORECASE,
)
_LONG_DATE_DMY_RE = re.compile(
    rf"^(?P<day>\d{{1,2}})\s+(?P<mon>{_MONTH_NAME_RE})\.?\s*,?\s*(?P<year>\d{{4}})$",
    re.IGNORECASE,
)

_DATE_PATTERNS = [
    # ISO
    (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$"), "iso"),
    # US slashes
    (re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$"), "us"),
    # US dashes
    (re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{2,4})$"), "us"),
]

# Matches "8AM", "8:30PM", "08:30", "8 am", "16:00", etc.
_TIME_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*([AaPp][Mm])?",
)

# Leading date — with optional time immediately after. The time
# arm is deliberately restrictive: a bare digit run with no colon
# and no am/pm suffix is NOT a time (otherwise the odometer
# number "285948" gets eaten as a fake hour "28"). A real time
# must have either a ":" or an am/pm marker.
_LEADING_DATE_TIME_RE = re.compile(
    rf"""
    ^\s*
    (?P<date>
      \d{{4}}-\d{{1,2}}-\d{{1,2}}                         # 2025-01-01
      | \d{{1,2}}[/-]\d{{1,2}}[/-]\d{{2,4}}               # 01/01/2025, 1-1-25
      | (?:{_MONTH_NAME_RE})\.?\s+\d{{1,2}}(?:\s*,\s*|\s+)\d{{4}}  # December 3, 2024
      | \d{{1,2}}\s+(?:{_MONTH_NAME_RE})\.?\s*,?\s*\d{{4}}         # 3 December 2024
    )
    (?:
      \s+
      (?P<time>
        \d{{1,2}}:\d{{2}}\s*(?:[AaPp][Mm])?    # 08:00, 8:00 pm, 16:30
        |
        \d{{1,2}}\s*[AaPp][Mm]                 # 8am, 12 PM (no colon)
      )
    )?
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Matches a mileage or trip-distance number at the start of a
# remainder string. Accepts "69,001" with comma-thousands or a bare
# 1-7 digit integer. The anchored ^\s* means we pull tokens off the
# front of the remainder, not scattered matches inside the
# description — "$43.49" embedded in a description never counts.
_LEADING_NUMBER_RE = re.compile(r"^\s*(\d{1,3}(?:,\d{3})+|\d{1,7})\b")

# Matches odometer-shaped numbers anywhere in free-text. Used to
# pull start (min) and end (max) odometers from day-log lines that
# have multiple readings scattered among descriptive text —
# e.g. "211,999 – START ... $77.07 – 212,039 ... 212,523".
#
# Filters via lookarounds:
#   * NOT preceded by ``$`` / digit / ``.`` / ``,`` — keeps dollar
#     amounts like ``$77.07`` out (match on "77" fails the ``$``
#     lookbehind; match on "07" fails the ``.`` lookbehind).
#   * NOT followed by digit / ``.`` — keeps the integer part of a
#     float (``77`` in ``77.07``) from being extracted as an
#     odometer.
#
# Body: either comma-thousand form (``212,039``) with at least one
# comma group (so ≥ 4 digits total) OR a bare 4–7 digit run. The
# 4-digit minimum weeds out trip-distance mentions ("9 MILES") and
# stray small integers in descriptions.
_ODOMETER_IN_TEXT_RE = re.compile(
    r"""
    (?<![$\d.,])
    (
        \d{1,3}(?:,\d{3})+
        |
        \d{4,7}
    )
    (?![\d.])
    """,
    re.VERBOSE,
)


def _extract_odometer_matches(
    text: str,
) -> list[tuple[int, int, int]]:
    """All odometer-shaped numbers in ``text``, in source order, as
    ``(value, span_start, span_end)`` tuples. Span info lets the
    caller slice the surrounding description text per segment.
    Dollar amounts and decimal fragments are filtered out by the
    regex's lookarounds."""
    out: list[tuple[int, int, int]] = []
    for m in _ODOMETER_IN_TEXT_RE.finditer(text):
        try:
            out.append((int(m.group(1).replace(",", "")), m.start(), m.end()))
        except ValueError:
            continue
    return out


def _scrub_description(text: str) -> str | None:
    """Trim a description fragment: drop ``miles``/``mi`` words,
    collapse whitespace, strip leading/trailing punctuation noise."""
    out = re.sub(r"\b(miles?|mi\.?)\b", "", text, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip(" -,:")
    return out or None


def _parse_date(raw: str) -> date_t | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    # Collapse "Dec. 3, 2024" / double spaces before regex matching.
    collapsed = re.sub(r"\s+", " ", raw)
    for pattern, shape in _DATE_PATTERNS:
        m = pattern.match(collapsed)
        if not m:
            continue
        a, b, c = m.groups()
        try:
            if shape == "iso":
                y, mo, d = int(a), int(b), int(c)
            else:
                mo, d, y = int(a), int(b), int(c)
                if y < 100:
                    y += 2000 if y < 70 else 1900
            return date_t(y, mo, d)
        except ValueError:
            return None
    for pattern in (_LONG_DATE_MDY_RE, _LONG_DATE_DMY_RE):
        m = pattern.match(collapsed)
        if not m:
            continue
        mo = _MONTH_NAMES.get(m.group("mon").lower())
        if mo is None:
            continue
        try:
            return date_t(int(m.group("year")), mo, int(m.group("day")))
        except ValueError:
            return None
    return None


def _parse_time(raw: str | None) -> str | None:
    """Return HH:MM in 24-hour, or None."""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    m = _TIME_RE.fullmatch(raw)
    if not m:
        # Try removing trailing period/comma and retrying.
        raw2 = raw.rstrip(".,")
        m = _TIME_RE.fullmatch(raw2)
        if not m:
            return None
    h_raw, mm_raw, ampm = m.groups()
    try:
        h = int(h_raw)
        mm = int(mm_raw) if mm_raw else 0
    except ValueError:
        return None
    if ampm:
        ampm = ampm.lower()
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
    if not (0 <= h <= 23 and 0 <= mm <= 59):
        return None
    return f"{h:02d}:{mm:02d}"


def _parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def detect_csv(text: str) -> bool:
    """Return True if the first non-blank line looks like a mileage
    CSV header. Accepts several common variants so users can paste
    spreadsheets from the real world:
      * canonical vehicles.csv: odometer_start / odometer_end
      * spreadsheet shape: "Starting Mileage" / "Ending Mileage"
      * underscored: start_mileage / end_mileage
    """
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        low = line.lower()
        if "date" not in low:
            return False
        start_hit = any(k in low for k in (
            "odometer_start", "start_mileage", "starting mileage", "start mileage",
        ))
        end_hit = any(k in low for k in (
            "odometer_end", "end_mileage", "ending mileage", "end mileage",
        ))
        return start_hit and end_hit
    return False


def parse_csv_text(text: str) -> list[ImportPreviewRow]:
    """Parse a CSV that has at minimum a date column + two odometer
    columns (canonical vehicles.csv, or the user's preferred shape).
    Missing columns => error on the affected rows, preview still renders."""
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return []
    norm = [h.strip().lower() for h in header]

    def _find(*aliases: str) -> int | None:
        for alias in aliases:
            if alias in norm:
                return norm.index(alias)
        return None

    date_idx = _find("date", "entry_date")
    start_idx = _find(
        "odometer_start", "start_mileage", "starting mileage",
        "start mileage", "start",
    )
    end_idx = _find(
        "odometer_end", "end_mileage", "ending mileage",
        "end mileage", "end",
    )
    miles_idx = _find("miles", "total miles")
    desc_idx = _find("description", "notes", "purpose", "memo")
    vehicle_idx = _find("vehicle")
    time_idx = _find("time", "entry_time")
    personal_idx = _find("personal miles", "personal_miles")
    business_idx = _find("business miles", "business_miles")
    commuting_idx = _find(
        "commuting miles", "commuting_miles", "commute miles", "commute_miles",
    )
    category_idx = _find("category", "purpose_category", "trip category")

    if date_idx is None:
        return [ImportPreviewRow(
            line_no=1, entry_date=None, entry_time=None, vehicle=None,
            odometer_start=None, odometer_end=None, miles=None,
            description=None, error="no 'date' column in CSV header",
        )]

    out: list[ImportPreviewRow] = []
    for lineno, row in enumerate(reader, start=2):
        if not row or all(not c.strip() for c in row):
            continue
        try:
            raw_date = row[date_idx] if date_idx < len(row) else ""
        except IndexError:
            raw_date = ""
        entry_date = _parse_date(raw_date)
        odo_start = _parse_int(row[start_idx]) if start_idx is not None and start_idx < len(row) else None
        odo_end = _parse_int(row[end_idx]) if end_idx is not None and end_idx < len(row) else None
        miles = _parse_float(row[miles_idx]) if miles_idx is not None and miles_idx < len(row) else None
        description = (row[desc_idx].strip() if desc_idx is not None and desc_idx < len(row) else "") or None
        entry_time = _parse_time(row[time_idx]) if time_idx is not None and time_idx < len(row) else None
        vehicle = row[vehicle_idx].strip() if vehicle_idx is not None and vehicle_idx < len(row) else None
        vehicle = vehicle or None

        if miles is None and odo_start is not None and odo_end is not None:
            miles = float(odo_end - odo_start)

        personal = _parse_float(row[personal_idx]) if personal_idx is not None and personal_idx < len(row) else None
        business = _parse_float(row[business_idx]) if business_idx is not None and business_idx < len(row) else None
        commuting = _parse_float(row[commuting_idx]) if commuting_idx is not None and commuting_idx < len(row) else None
        category_raw = (
            row[category_idx].strip().lower()
            if category_idx is not None and category_idx < len(row)
            else ""
        )
        category = category_raw if category_raw in {
            "business", "commuting", "personal", "mixed",
        } else None

        error: str | None = None
        if entry_date is None:
            error = f"invalid date {raw_date!r}"
        elif miles is None:
            error = "row is missing miles and odometer pair"
        elif miles < 0:
            error = f"miles must not be negative (got {miles})"
        # miles == 0 is allowed — "no trips today" markers are a
        # legitimate and useful audit signal for vehicle logs.

        out.append(ImportPreviewRow(
            line_no=lineno,
            entry_date=entry_date,
            entry_time=entry_time,
            vehicle=vehicle,
            odometer_start=odo_start,
            odometer_end=odo_end,
            miles=miles,
            description=description,
            error=error,
            personal_miles=personal,
            business_miles=business,
            commuting_miles=commuting,
            category=category,
        ))
    return out


def parse_free_text(text: str) -> list[ImportPreviewRow]:
    """Parse free-text mileage log lines. Each non-blank non-comment
    line is one entry. We try, in order:
      * comma-shaped "date, start, end, description" (Format B)
      * leading date/time with one or more odometer numbers in the
        remainder (Format A anchor, Format B without commas, or
        Format C day-log with multiple inline anchors)."""
    out: list[ImportPreviewRow] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        out.extend(_parse_free_line(lineno, line))
    return out


def _parse_free_line(lineno: int, line: str) -> list[ImportPreviewRow]:
    # Format B: date, start, end, description
    if "," in line:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 3:
            entry_date = _parse_date(parts[0])
            start = _parse_int(parts[1])
            end = _parse_int(parts[2])
            if entry_date and (start is not None or end is not None):
                desc = ",".join(parts[3:]).strip() or None
                miles: float | None = None
                if start is not None and end is not None:
                    if end < start:
                        return [ImportPreviewRow(
                            line_no=lineno, entry_date=entry_date, entry_time=None,
                            vehicle=None, odometer_start=start, odometer_end=end,
                            miles=None, description=desc,
                            error=f"end odometer {end} is lower than start {start}",
                        )]
                    miles = float(end - start)
                return [ImportPreviewRow(
                    line_no=lineno,
                    entry_date=entry_date,
                    entry_time=None,
                    vehicle=None,
                    odometer_start=start,
                    odometer_end=end,
                    miles=miles,
                    description=desc,
                )]
            # Try date + single odometer (anchor) + desc with commas
            if entry_date and start is not None and end is None:
                desc = ",".join(parts[2:]).strip() or None
                return [ImportPreviewRow(
                    line_no=lineno, entry_date=entry_date, entry_time=None,
                    vehicle=None, odometer_start=None, odometer_end=start,
                    miles=None, description=desc,
                )]

    # Format A / C: "DATE [TIME] ODO [more...]"
    m = _LEADING_DATE_TIME_RE.match(line)
    if not m:
        return [ImportPreviewRow(
            line_no=lineno, entry_date=None, entry_time=None, vehicle=None,
            odometer_start=None, odometer_end=None, miles=None,
            description=line, error="can't parse — need a leading date",
        )]
    entry_date = _parse_date(m.group("date"))
    if entry_date is None:
        return [ImportPreviewRow(
            line_no=lineno, entry_date=None, entry_time=None, vehicle=None,
            odometer_start=None, odometer_end=None, miles=None,
            description=line, error=f"invalid date {m.group('date')!r}",
        )]
    entry_time = _parse_time(m.group("time")) if m.group("time") else None

    remainder = line[m.end():].lstrip(" \t-,:")
    # Extract every odometer-shaped number from the remainder along
    # with the source spans (dollar amounts + "(N MILES)" fragments
    # are filtered out by _ODOMETER_IN_TEXT_RE's lookarounds). Spans
    # let us slice description text BETWEEN consecutive anchors so
    # each segment keeps the context it was logged with.
    matches = _extract_odometer_matches(remainder)

    # No mileage number — that's just a dated note, skip with an error.
    if not matches:
        description = _scrub_description(remainder)
        return [ImportPreviewRow(
            line_no=lineno,
            entry_date=entry_date,
            entry_time=entry_time,
            vehicle=None,
            odometer_start=None,
            odometer_end=None,
            miles=None,
            description=description,
            error="no mileage number found on this line",
        )]

    # One number → ambiguous between an odometer anchor and a trip-
    # distance total. Store as odometer_end; resolve_anchors_to_trips
    # disambiguates against prev_odo.
    if len(matches) == 1:
        value, _, _ = matches[0]
        description = _scrub_description(_ODOMETER_IN_TEXT_RE.sub("", remainder))
        return [ImportPreviewRow(
            line_no=lineno,
            entry_date=entry_date,
            entry_time=entry_time,
            vehicle=None,
            odometer_start=None,
            odometer_end=value,
            miles=None,
            description=description,
        )]

    # Two or more numbers → collapse to a single row using the first
    # and last anchors in source order. The user's convention is one
    # row per dated entry; intermediary odometer readings are
    # informational and belong in the description text, not as their
    # own rows. If the readings aren't monotonically non-decreasing
    # we still use first→last span but flag the conflict so the user
    # can fix the source.
    values = [v for v, _, _ in matches]
    start = values[0]
    end = values[-1]
    description = _scrub_description(_ODOMETER_IN_TEXT_RE.sub("", remainder))
    monotonic = all(values[i] <= values[i + 1] for i in range(len(values) - 1))
    conflict: str | None = None
    if not monotonic:
        ordered_start = min(values)
        ordered_end = max(values)
        conflict = (
            f"odometer readings on this line aren't in ascending "
            f"order ({values}) — collapsed to "
            f"{ordered_start:,}→{ordered_end:,}; verify and edit manually"
        )
        start, end = ordered_start, ordered_end
    miles = float(end - start)
    if miles > 2000:
        conflict = conflict or (
            f"day delta {int(miles)} miles is suspicious — verify "
            f"{start:,} and {end:,} aren't unrelated figures"
        )
    return [ImportPreviewRow(
        line_no=lineno,
        entry_date=entry_date,
        entry_time=entry_time,
        vehicle=None,
        odometer_start=start,
        odometer_end=end,
        miles=miles,
        description=description,
        conflict=conflict,
    )]


def resolve_anchors_to_trips(
    rows: list[ImportPreviewRow],
    *,
    starting_odometer: int | None = None,
) -> list[ImportPreviewRow]:
    """For Format A input (rows that carry only a single
    ``odometer_end`` but no ``odometer_start``), sort by datetime and
    derive each row's ``odometer_start`` + ``miles`` from the
    preceding row's odometer. Errors (non-monotonic readings,
    impossible gaps) get flagged as ``conflict`` or ``error``.

    ``starting_odometer`` supplies the prior anchor when the first
    line in the batch is a "set out for..." reading with no earlier
    known mileage — typically from ``last_odometer_for(vehicle)`` on
    the service. When it's None and the first line is Format A,
    that first line is kept as an un-derivable anchor (conflict)."""
    resolved: list[ImportPreviewRow] = []
    # Preserve original line_no order while sorting by date+time for
    # derivation; but we also need to return rows in input order.
    order_keyed = sorted(
        enumerate(rows),
        key=lambda ix: _sort_key(ix[1]),
    )

    # Backfill detection: ``starting_odometer`` is fetched as the
    # vehicle's LATEST reading in the DB, regardless of dates. If this
    # batch's earliest reading is lower than that seed, the batch is
    # backfilling older trips and the seed is from a later entry —
    # it's the wrong prior anchor. Drop it so the chain derives from
    # the batch's own first reading instead of producing a spurious
    # "odometer went backward" error on every row.
    if starting_odometer is not None:
        earliest: int | None = None
        for _idx, r in order_keyed:
            if r.error is not None:
                continue
            first_reading = r.odometer_start or r.odometer_end
            if first_reading is not None:
                earliest = int(first_reading)
                break
        if earliest is not None and earliest < starting_odometer:
            starting_odometer = None

    # Build a map input_idx -> derived row.
    prev_odo: int | None = starting_odometer
    derived_map: dict[int, ImportPreviewRow] = {}
    for input_idx, row in order_keyed:
        if row.error is not None:
            derived_map[input_idx] = row
            continue
        # Format B already has both anchors → nothing to derive.
        if row.odometer_start is not None and row.odometer_end is not None:
            prev_odo = row.odometer_end
            derived_map[input_idx] = row
            continue
        # Format A (odometer_end only, no start).
        if row.odometer_end is not None and row.odometer_start is None:
            if prev_odo is None:
                derived_map[input_idx] = replace(
                    row,
                    conflict=(
                        "first anchor in batch — no prior odometer to "
                        "derive miles from; stored as 0-mile marker"
                    ),
                    miles=0.0,
                    odometer_start=row.odometer_end,
                )
                prev_odo = row.odometer_end
                continue
            # Disambiguate: is this number a trip distance or an
            # odometer anchor? If it's much smaller than the prior
            # odometer AND small enough to be a plausible single-
            # day drive, reinterpret as miles (not odometer_end).
            # A real vehicle at 285k doesn't suddenly read 76.
            single_number = int(row.odometer_end)
            if (
                prev_odo > 1000
                and single_number < prev_odo // 10
                and single_number < 1000
            ):
                derived_end = int(prev_odo) + single_number
                derived_map[input_idx] = replace(
                    row,
                    odometer_start=int(prev_odo),
                    odometer_end=derived_end,
                    miles=float(single_number),
                    conflict=(
                        f"interpreted {single_number} as trip miles "
                        f"(too small to be an odometer at {prev_odo:,})"
                    ),
                )
                prev_odo = derived_end
                continue
            delta = single_number - int(prev_odo)
            if delta == 0:
                # Two anchors at same odometer on a later date/time
                # usually means "parked, nothing since". That's fine:
                # store as a 0-mile "still at X" marker.
                derived_map[input_idx] = replace(
                    row,
                    odometer_start=prev_odo,
                    miles=0.0,
                )
                continue
            if delta < 0:
                derived_map[input_idx] = replace(
                    row,
                    odometer_start=prev_odo,
                    miles=None,
                    error=(
                        f"odometer went backward ({prev_odo} → "
                        f"{row.odometer_end})"
                    ),
                )
                continue
            if delta > 10_000:
                # Sanity: a 10k-mile jump between anchors is almost
                # always a data error. Accept it but flag.
                derived_map[input_idx] = replace(
                    row,
                    odometer_start=prev_odo,
                    miles=float(delta),
                    conflict=f"suspiciously large jump: {delta} miles",
                )
                prev_odo = row.odometer_end
                continue
            derived_map[input_idx] = replace(
                row,
                odometer_start=prev_odo,
                miles=float(delta),
            )
            prev_odo = row.odometer_end
            continue
        # Neither anchor known — no derivation possible.
        derived_map[input_idx] = row

    for i, row in enumerate(rows):
        resolved.append(derived_map.get(i, row))
    return resolved


def _sort_key(row: ImportPreviewRow) -> tuple:
    d = row.entry_date or date_t(1900, 1, 1)
    t = row.entry_time or "00:00"
    return (d.isoformat(), t, row.line_no)


def detect_conflicts(
    rows: list[ImportPreviewRow],
) -> list[ImportPreviewRow]:
    """Flag intra-batch conflicts: two rows on the same date+time
    with different odometer readings, or non-monotonic odometer
    sequences that slipped past resolve_anchors_to_trips."""
    by_date: dict[str, list[ImportPreviewRow]] = {}
    for row in rows:
        if row.entry_date is None:
            continue
        key = f"{row.entry_date.isoformat()}|{row.entry_time or ''}"
        by_date.setdefault(key, []).append(row)

    marked: dict[int, ImportPreviewRow] = {}
    for key, group in by_date.items():
        if len(group) < 2:
            continue
        odos = [r.odometer_end for r in group if r.odometer_end is not None]
        if len(set(odos)) > 1:
            for r in group:
                if r.conflict is not None or r.error is not None:
                    continue
                marked[id(r)] = replace(
                    r,
                    conflict=(
                        f"multiple odometer readings at {key}: "
                        f"{sorted(set(odos))}"
                    ),
                )

    return [marked.get(id(r), r) for r in rows]


def detect_cross_day_gaps(
    rows: list[ImportPreviewRow],
) -> list[ImportPreviewRow]:
    """Flag cross-day mismatches: day N's odometer_end should equal
    day N+1's odometer_start. When they don't, the user's log is
    missing a reading (Day N didn't record EOD, or Day N+1 didn't
    record morning start) — surface it as a conflict so the user
    can fix before commit. Only runs on rows that already have both
    anchors; anchor-only rows are handled by resolve_anchors_to_trips."""
    order_keyed = sorted(
        enumerate(rows),
        key=lambda ix: _sort_key(ix[1]),
    )
    marked: dict[int, ImportPreviewRow] = {}
    prev: ImportPreviewRow | None = None
    for _idx, row in order_keyed:
        if row.error is not None:
            continue
        if row.odometer_start is None or row.odometer_end is None:
            continue
        if prev is not None and prev.odometer_end is not None:
            gap = row.odometer_start - prev.odometer_end
            if gap != 0 and row.conflict is None:
                marked[id(row)] = replace(
                    row,
                    conflict=(
                        f"gap vs {prev.entry_date.isoformat()} end "
                        f"({prev.odometer_end:,}): {gap:+,} miles "
                        f"unaccounted for"
                    ),
                )
        prev = row
    return [marked.get(id(r), r) for r in rows]


def parse_input(
    *, text: str | None, csv_bytes: bytes | None,
    starting_odometer: int | None = None,
) -> tuple[list[ImportPreviewRow], str]:
    """High-level entry point. Decides CSV vs. free-text, runs the
    right parser, resolves anchors if needed, detects conflicts.
    Returns the preview rows + the detected source_format label."""
    if csv_bytes:
        try:
            text = csv_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = csv_bytes.decode("latin-1")
        rows = parse_csv_text(text)
        fmt = "csv"
    elif text and detect_csv(text):
        rows = parse_csv_text(text)
        fmt = "csv"
    elif text:
        rows = parse_free_text(text)
        has_single = any(
            r.odometer_end is not None and r.odometer_start is None
            for r in rows
        )
        has_pair = any(
            r.odometer_start is not None and r.odometer_end is not None
            for r in rows
        )
        fmt = "text_anchor" if has_single and not has_pair else "text_range"
    else:
        rows = []
        fmt = "csv"

    # Derive miles for anchor-style rows.
    resolved = resolve_anchors_to_trips(
        rows, starting_odometer=starting_odometer,
    )
    resolved = detect_conflicts(resolved)
    resolved = detect_cross_day_gaps(resolved)
    return resolved, fmt
