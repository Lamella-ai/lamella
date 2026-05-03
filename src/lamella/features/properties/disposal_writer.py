# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Property disposal writer.

Mirrors :mod:`lamella.features.vehicles.disposal_writer`. A property
disposal is a real ledger transaction (not a custom directive) — the
vehicle/property left the user's books on a specific date, real money
moved (or didn't, for a gift), and a gain/loss / disposition plug
captures the gap between book value and proceeds.

Use case: outright sale to a third party (vehicle/property leaves the
user's books entirely). Distinct from intercompany transfer (handled
by :mod:`lamella.features.properties.transfer_writer`) and rename
(handled in routes/properties.py).

Writes one block per disposal into ``connector_overrides.bean``:

```
2026-04-24 * "Property disposal" "<narration>" #lamella-property-disposal
  lamella-disposal-id: "<uuid>"
  lamella-disposal-property: "<slug>"
  lamella-disposal-date: "..."
  lamella-disposal-type: "sale"
  Assets:<Entity>:Property:<slug>          -<book_value>
  <proceeds_account>                        +<proceeds_amount>
  <gain_loss_account>                       <signed plug>
```

The plug closes the math: ``proceeds - book_value`` posts to whatever
account the user selected for gain/loss. Beancount Income accounts
naturally carry credit (negative) balances, so the writer flips the
sign when the user picks an Income account so a positive gap reads
as a gain on the income statement.

Bookkeeper-not-tax: the system records the sale event the user
states. Section 1245/1250 recapture characterization, capital-gain
treatment, basis adjustments — all CPA territory.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, date as date_t, datetime
from decimal import Decimal
from pathlib import Path

from lamella.core.ledger_writer import (
    BeanCheckError,
    WriteError,
    capture_bean_check,
    ensure_include_in_main,
    run_bean_check_vs_baseline,
)
from lamella.features.rules.overrides import ensure_overrides_exists

log = logging.getLogger(__name__)


DISPOSAL_TAG = "#lamella-property-disposal"


VALID_DISPOSAL_TYPES = {
    "sale", "gift", "scrap", "transfer-out", "other",
}


@dataclass(frozen=True)
class PropertyDisposalDraft:
    disposal_id: str
    property_slug: str
    property_display_name: str | None
    disposal_date: date_t
    disposal_type: str
    proceeds_amount: Decimal
    proceeds_account: str
    asset_account: str
    asset_amount_out: Decimal            # book value at disposal, positive
    gain_loss_account: str
    gain_loss_amount: Decimal            # signed; sign already flipped for Income
    buyer_or_party: str | None = None
    notes: str | None = None


def new_disposal_id() -> str:
    return uuid.uuid4().hex


def _esc(s: str | None) -> str:
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _fmt(d: Decimal) -> str:
    return f"{d.quantize(Decimal('0.01')):.2f}"


def render_disposal_block(draft: PropertyDisposalDraft) -> str:
    narration = (
        f"Property disposal — "
        f"{draft.property_display_name or draft.property_slug} "
        f"({draft.disposal_type})"
    )
    if draft.buyer_or_party:
        narration += f" — {draft.buyer_or_party}"

    lines = [
        "",
        f'{draft.disposal_date.isoformat()} * "Property disposal" '
        f'"{_esc(narration)}" {DISPOSAL_TAG}',
        f'  lamella-disposal-id: "{draft.disposal_id}"',
        f'  lamella-disposal-property: "{draft.property_slug}"',
        f'  lamella-disposal-date: "{draft.disposal_date.isoformat()}"',
        f'  lamella-disposal-type: "{draft.disposal_type}"',
        f'  lamella-modified-at: "{datetime.now(UTC).isoformat(timespec="seconds")}"',
    ]
    if draft.buyer_or_party:
        lines.append(f'  lamella-disposal-party: "{_esc(draft.buyer_or_party)}"')
    if draft.notes:
        lines.append(f'  lamella-disposal-notes: "{_esc(draft.notes)}"')
    lines.append(f"  {draft.asset_account}  -{_fmt(draft.asset_amount_out)} USD")
    lines.append(
        f"  {draft.proceeds_account}  {_fmt(draft.proceeds_amount)} USD"
    )
    lines.append(
        f"  {draft.gain_loss_account}  {_fmt(draft.gain_loss_amount)} USD"
    )
    return "\n".join(lines) + "\n"


def compute_gain_loss(
    *, proceeds: Decimal, book_value: Decimal,
) -> Decimal:
    """``proceeds - book_value``. Positive = gain, negative = loss.
    Bookkeeping figure only — the CPA decides §1245/1250 character."""
    return proceeds - book_value


def _append_with_check(
    *,
    main_bean: Path,
    overrides_path: Path,
    block: str,
    skip_check: bool = False,
) -> None:
    if not main_bean.exists():
        raise WriteError(f"main.bean not found at {main_bean}")
    backup_main = main_bean.read_bytes()
    backup_ov = overrides_path.read_bytes() if overrides_path.exists() else None
    _, baseline = capture_bean_check(main_bean) if not skip_check else (0, "")
    ensure_overrides_exists(overrides_path)
    ensure_include_in_main(main_bean, overrides_path)
    with overrides_path.open("a", encoding="utf-8") as fh:
        fh.write(block)
    if skip_check:
        return
    try:
        run_bean_check_vs_baseline(main_bean, baseline)
    except BeanCheckError:
        main_bean.write_bytes(backup_main)
        if backup_ov is None:
            overrides_path.unlink(missing_ok=True)
        else:
            overrides_path.write_bytes(backup_ov)
        raise


def write_disposal(
    *,
    draft: PropertyDisposalDraft,
    main_bean: Path,
    overrides_path: Path,
    skip_check: bool = False,
) -> None:
    if draft.disposal_type not in VALID_DISPOSAL_TYPES:
        raise WriteError(
            f"invalid disposal_type {draft.disposal_type!r} — "
            f"must be one of {sorted(VALID_DISPOSAL_TYPES)}"
        )
    block = render_disposal_block(draft)
    _append_with_check(
        main_bean=main_bean,
        overrides_path=overrides_path,
        block=block,
        skip_check=skip_check,
    )
