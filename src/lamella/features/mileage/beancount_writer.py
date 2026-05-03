# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date as date_t
from decimal import Decimal
from pathlib import Path

from lamella.features.mileage.service import YearlySummaryRow
from lamella.core.ledger_writer import (
    BeanCheckError,
    WriteError,
    ensure_include_in_main,
    run_bean_check,
)

log = logging.getLogger(__name__)


SUMMARY_HEADER = (
    "; mileage_summary.bean — Managed by Lamella (Phase 5+).\n"
    "; Year-end mileage deductions, one block per (vehicle, entity) pair.\n"
)

EQUITY_ACCOUNT = "Equity:MileageDeductions"

_BLOCK_BEGIN_RE = re.compile(r";; BEGIN year=(\d{4})")
_BLOCK_END_RE = re.compile(r";; END year=(\d{4})")


@dataclass(frozen=True)
class SummaryWriteResult:
    year: int
    rows_written: int
    deduction_total_usd: Decimal
    rate_per_mile: Decimal
    replaced: bool


class MileageSummaryError(RuntimeError):
    pass


def _ensure_summary_file(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(SUMMARY_HEADER, encoding="utf-8")


def _equity_account_opened(main_bean: Path) -> bool:
    """Cheap text scan for an `open Equity:MileageDeductions` directive
    anywhere in the ledger tree. Avoids re-parsing the whole ledger; we
    accept some false negatives because the surface error message points
    the user to add the line themselves."""
    target = EQUITY_ACCOUNT
    try:
        roots = [main_bean] + [main_bean.parent / p for p in main_bean.parent.glob("*.bean")]
    except OSError:
        return False
    seen: set[Path] = set()
    for root in roots:
        try:
            real = root.resolve()
        except OSError:
            continue
        if real in seen:
            continue
        seen.add(real)
        try:
            text = real.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("open ") and target in stripped:
                return True
            if " open " in stripped and target in stripped:
                return True
    return False


def _render_block(year: int, rows: list[YearlySummaryRow], rate: Decimal) -> str:
    """Render a `;; BEGIN year=YYYY` ... `;; END year=YYYY` block. Inside,
    emit one balanced transaction per (vehicle, entity) pair."""
    lines: list[str] = [f";; BEGIN year={year}"]
    on = date_t(year, 12, 31).isoformat()
    for row in rows:
        narration = (
            f"Mileage deduction — {row.vehicle} ({row.entity}) "
            f"{row.miles:.1f} mi @ ${rate:.3f}/mi"
        )
        lines.append(f'{on} * "Mileage" "{narration}"')
        lines.append(f'  lamella-mileage-vehicle: "{row.vehicle}"')
        lines.append(f'  lamella-mileage-entity: "{row.entity}"')
        lines.append(f'  lamella-mileage-miles: {row.miles:.2f}')
        # Business / commuting / personal breakdown. Only emit a
        # metadata key when the value is non-zero so personal-only
        # deploys don't clutter the file with zero rows.
        if row.business_miles:
            lines.append(
                f'  lamella-mileage-business-miles: {row.business_miles:.2f}'
            )
        if row.commuting_miles:
            lines.append(
                f'  lamella-mileage-commuting-miles: {row.commuting_miles:.2f}'
            )
        if row.personal_miles:
            lines.append(
                f'  lamella-mileage-personal-miles: {row.personal_miles:.2f}'
            )
        lines.append(f'  lamella-mileage-rate: {rate:.3f}')
        lines.append(f'  Expenses:{row.entity}:Mileage  {row.deduction_usd:.2f} USD')
        lines.append(f'  {EQUITY_ACCOUNT}  -{row.deduction_usd:.2f} USD')
        lines.append("")
    lines.append(f";; END year={year}")
    return "\n".join(lines) + "\n"


def _replace_year_block(existing: str, year: int, block: str) -> tuple[str, bool]:
    """Replace the `BEGIN year=YYYY ... END year=YYYY` block, returning
    (new_text, replaced). If no existing block, append at the end."""
    begin_marker = f";; BEGIN year={year}"
    end_marker = f";; END year={year}"
    begin_idx = existing.find(begin_marker)
    if begin_idx == -1:
        # Append.
        suffix = "" if existing.endswith("\n") else "\n"
        return existing + suffix + "\n" + block, False
    end_idx = existing.find(end_marker, begin_idx)
    if end_idx == -1:
        raise MileageSummaryError(
            f"mileage_summary.bean: BEGIN year={year} without matching END"
        )
    # Walk to the end of the END line.
    end_line_end = existing.find("\n", end_idx)
    if end_line_end == -1:
        end_line_end = len(existing)
    else:
        end_line_end += 1  # include trailing newline
    return existing[:begin_idx] + block + existing[end_line_end:], True


class MileageBeancountWriter:
    """Writes year-end mileage summary blocks to mileage_summary.bean.

    Idempotent: a second call for the same year replaces only that year's
    block. Runs ``bean-check`` after every write and reverts on failure.
    """

    def __init__(
        self,
        *,
        main_bean: Path,
        summary_path: Path,
        run_check: bool = True,
    ):
        self.main_bean = main_bean
        self.summary_path = summary_path
        self.run_check = run_check

    def write_year(
        self,
        *,
        year: int,
        rows: list[YearlySummaryRow],
        rate_per_mile: Decimal,
    ) -> SummaryWriteResult:
        if not rows:
            raise MileageSummaryError(f"no mileage rows for year {year}")
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")
        if not _equity_account_opened(self.main_bean):
            raise MileageSummaryError(
                f"{EQUITY_ACCOUNT} is not opened in the ledger. Add a line "
                f"like `2024-01-01 open {EQUITY_ACCOUNT} USD` and try again."
            )

        summary_existed_before = self.summary_path.exists()
        _ensure_summary_file(self.summary_path)
        backup_summary = self.summary_path.read_bytes()
        backup_main = self.main_bean.read_bytes()

        try:
            ensure_include_in_main(self.main_bean, self.summary_path)
            existing = self.summary_path.read_text(encoding="utf-8")
            block = _render_block(year, rows, rate_per_mile)
            new_text, replaced = _replace_year_block(existing, year, block)
            self._atomic_overwrite(self.summary_path, new_text)
            if self.run_check:
                run_bean_check(self.main_bean)
        except (BeanCheckError, MileageSummaryError, OSError):
            if summary_existed_before:
                self.summary_path.write_bytes(backup_summary)
            else:
                self.summary_path.unlink(missing_ok=True)
            self.main_bean.write_bytes(backup_main)
            raise

        # ADR-0022: deduction_total_usd is Decimal. YearlySummaryRow.deduction_usd
        # is still float upstream (out of this worker's scope); coerce via str
        # to avoid binary-float artefacts.
        deduction_total = Decimal(
            str(round(sum(r.deduction_usd for r in rows), 2))
        )
        return SummaryWriteResult(
            year=year,
            rows_written=len(rows),
            deduction_total_usd=deduction_total,
            rate_per_mile=rate_per_mile,
            replaced=replaced,
        )

    def _atomic_overwrite(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".mileage_summary.", suffix=".bean", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
