# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, date as date_t, datetime
from pathlib import Path
from typing import Iterable, Iterator

log = logging.getLogger(__name__)


CSV_HEADER = (
    "date",
    "vehicle",
    "odometer_start",
    "odometer_end",
    "miles",
    "purpose",
    "entity",
    "from",
    "to",
    "notes",
)


class MileageCsvError(RuntimeError):
    """Raised when vehicles.csv is shaped wrong or a row fails validation."""


@dataclass(frozen=True)
class MileageRow:
    entry_date: date_t
    vehicle: str
    odometer_start: int | None
    odometer_end: int | None
    miles: float
    purpose: str | None
    entity: str
    from_loc: str | None
    to_loc: str | None
    notes: str | None
    csv_row_index: int  # 0-based row in vehicles.csv (excluding header)

    def to_csv_row(self) -> list[str]:
        def _opt(value) -> str:
            if value is None:
                return ""
            return str(value)

        return [
            self.entry_date.isoformat(),
            self.vehicle,
            _opt(self.odometer_start),
            _opt(self.odometer_end),
            f"{float(self.miles):.2f}",
            self.purpose or "",
            self.entity,
            self.from_loc or "",
            self.to_loc or "",
            self.notes or "",
        ]


def _parse_int(raw: str | None) -> int | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    return int(float(raw))  # tolerate "12345.0"


def _parse_float(raw: str | None) -> float | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    return float(raw)


def _parse_date(raw: str) -> date_t:
    raw = (raw or "").strip()
    if not raw:
        raise MileageCsvError("date is empty")
    try:
        return date_t.fromisoformat(raw)
    except ValueError as exc:
        raise MileageCsvError(f"invalid date {raw!r}: expected YYYY-MM-DD") from exc


def _row_from_csv(index: int, raw: dict[str, str]) -> MileageRow:
    """Coerce a csv.DictReader row into a MileageRow.

    Raises MileageCsvError on missing/invalid required fields. Required:
    date, vehicle, miles, entity. Odometer fields are optional but if both
    are present miles must be consistent with their delta.
    """
    try:
        entry_date = _parse_date(raw.get("date", ""))
    except MileageCsvError:
        raise

    vehicle = (raw.get("vehicle") or "").strip()
    if not vehicle:
        raise MileageCsvError(f"row {index}: vehicle is required")
    entity = (raw.get("entity") or "").strip()
    if not entity:
        raise MileageCsvError(f"row {index}: entity is required")

    try:
        odo_start = _parse_int(raw.get("odometer_start"))
        odo_end = _parse_int(raw.get("odometer_end"))
        miles = _parse_float(raw.get("miles"))
    except ValueError as exc:
        raise MileageCsvError(f"row {index}: numeric field invalid: {exc}") from exc

    if miles is None and odo_start is not None and odo_end is not None:
        miles = float(odo_end - odo_start)

    if miles is None:
        raise MileageCsvError(f"row {index}: miles missing and cannot derive from odometer")

    if miles < 0:
        raise MileageCsvError(f"row {index}: miles cannot be negative (got {miles})")
    # miles == 0 is allowed: users write "start of year" / checkpoint rows
    # with matching odometer_start and odometer_end to anchor the annual
    # baseline. Rejecting them drops valuable odometer history.

    return MileageRow(
        entry_date=entry_date,
        vehicle=vehicle,
        odometer_start=odo_start,
        odometer_end=odo_end,
        miles=miles,
        purpose=(raw.get("purpose") or "").strip() or None,
        entity=entity,
        from_loc=(raw.get("from") or "").strip() or None,
        to_loc=(raw.get("to") or "").strip() or None,
        notes=(raw.get("notes") or "").strip() or None,
        csv_row_index=index,
    )


class MileageCsvStore:
    """Read/append-only access to vehicles.csv. The file is the source of
    truth for mileage; SQLite holds only a derived cache (see service.py)."""

    def __init__(self, path: Path):
        self.path = path

    def exists(self) -> bool:
        return self.path.exists()

    def mtime(self) -> datetime | None:
        try:
            ts = self.path.stat().st_mtime
        except FileNotFoundError:
            return None
        return datetime.fromtimestamp(ts, tz=UTC)

    def ensure(self) -> None:
        """Create the CSV with a header if it doesn't yet exist. We only
        create files under the mileage directory; the parent must already be
        a sensible mileage location (ledger_dir/mileage/ by default)."""
        if self.path.exists():
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADER)

    def read_all(self, *, strict: bool = False) -> tuple[list[MileageRow], list[str]]:
        """Return parsed rows + a list of warning strings for any row that
        failed validation. With ``strict=True``, raises on the first failure
        instead of collecting it."""
        if not self.path.exists():
            return [], []
        rows: list[MileageRow] = []
        warnings: list[str] = []
        with self.path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            missing = [c for c in CSV_HEADER if c not in (reader.fieldnames or [])]
            if missing:
                raise MileageCsvError(
                    f"vehicles.csv missing columns: {', '.join(missing)}"
                )
            for index, raw in enumerate(reader):
                try:
                    rows.append(_row_from_csv(index, raw))
                except MileageCsvError as exc:
                    if strict:
                        raise
                    warnings.append(str(exc))
        return rows, warnings

    def append(self, row: MileageRow) -> int:
        """Append a row atomically (write to temp, fsync, rename). Returns
        the 0-based row index of the new row."""
        self.ensure()
        existing, _ = self.read_all(strict=False)
        new_index = len(existing)

        # Re-render the whole file via temp + rename for atomicity. This is
        # cheap (mileage CSVs are small) and avoids partial writes on crash.
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(CSV_HEADER)
        for r in existing:
            writer.writerow(r.to_csv_row())
        new_row = MileageRow(**{**row.__dict__, "csv_row_index": new_index})
        writer.writerow(new_row.to_csv_row())

        self._atomic_write(out.getvalue())
        return new_index

    def rewrite(self, rows: Iterable[MileageRow]) -> int:
        """Replace the file with ``rows`` (used by deletion paths). Returns
        the new row count."""
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(CSV_HEADER)
        n = 0
        for index, row in enumerate(rows):
            writer.writerow(MileageRow(**{**row.__dict__, "csv_row_index": index}).to_csv_row())
            n += 1
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write(out.getvalue())
        return n

    def _atomic_write(self, contents: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".vehicles.", suffix=".csv", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                fh.write(contents)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass
            os.replace(tmp_path, self.path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def iter_warnings(warnings: Iterable[str]) -> Iterator[str]:
    for w in warnings:
        yield w
