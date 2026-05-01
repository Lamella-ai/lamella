# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicle disposal writer.

Writes disposal transactions into `connector_overrides.bean` following
the same snapshot → append → bean-check-vs-baseline → rollback
contract used by the loan-funding writer. A disposal is a real
ledger transaction (not a `custom` directive) because it moves money:
book value out of the asset account, proceeds in, plug the gain/loss.

Every transaction carries the `#lamella-vehicle-disposal` tag and enough
`lamella-disposal-*` metadata to reconstruct the SQLite row if the cache
is wiped (Phase 7 reconstruct).

**Revoke-and-rewrite.** User edits to a committed disposal never
rewrite the original transaction in place — we'd be lying about
what was known when. Instead:
  1. `write_revoke(...)` appends a reversing transaction (every
     amount negated) with `lamella-disposal-revokes: <original_id>` and
     a fresh `lamella-disposal-id` for the revoke row itself.
  2. The caller then creates a new replacement with `write_disposal`
     and its own fresh `lamella-disposal-id`.

The pair stays atomic in the ledger because each side is a real
transaction — running balances stay honest; audit is preserved.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date as date_t
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


DISPOSAL_TAG = "#lamella-vehicle-disposal"


VALID_DISPOSAL_TYPES = {
    "sale", "trade-in", "total-loss", "gift", "scrap", "other",
}


@dataclass(frozen=True)
class DisposalDraft:
    """The set of facts needed to render a disposal transaction. Built
    from the preview form and re-rendered at commit time so the
    preview flow stays stateless."""
    disposal_id: str
    vehicle_slug: str
    vehicle_display_name: str | None
    disposal_date: date_t
    disposal_type: str
    proceeds_amount: Decimal
    proceeds_account: str
    asset_account: str
    asset_amount_out: Decimal            # cost basis at disposal, positive
    gain_loss_account: str
    gain_loss_amount: Decimal            # signed; + income, - expense
    buyer_or_party: str | None = None
    notes: str | None = None


def new_disposal_id() -> str:
    """UUID4 without dashes so it reads nicely in metadata values."""
    return uuid.uuid4().hex


def _esc(s: str | None) -> str:
    """Escape a string for the inside of a Beancount double-quoted
    literal. Backslash and quote only — Beancount doesn't interpret
    other escapes."""
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _fmt_amount(d: Decimal) -> str:
    """Two-decimal fixed-point, no currency suffix."""
    q = d.quantize(Decimal("0.01"))
    return f"{q:.2f}"


def render_disposal_block(draft: DisposalDraft) -> str:
    """Render the ledger text for a disposal transaction. Single
    compound transaction — matches the existing loan-funding pattern,
    keeps the `#lamella-vehicle-disposal` tag + `lamella-disposal-id` atomic.

    Postings:
        Assets:...:Vehicles:{slug}  -<cost_basis_at_disposal>
        <proceeds_account>           +<proceeds_amount>
        <gain_loss_account>          <signed plug to balance>
    """
    narration = (
        f"Vehicle disposal — {draft.vehicle_display_name or draft.vehicle_slug} "
        f"({draft.disposal_type})"
    )
    if draft.buyer_or_party:
        narration += f" — {draft.buyer_or_party}"

    lines = [
        "",
        f'{draft.disposal_date.isoformat()} * "Vehicle disposal" "{_esc(narration)}" {DISPOSAL_TAG}',
        f'  lamella-disposal-id: "{draft.disposal_id}"',
        f'  lamella-disposal-vehicle: "{draft.vehicle_slug}"',
        f'  lamella-disposal-date: "{draft.disposal_date.isoformat()}"',
        f'  lamella-disposal-type: "{draft.disposal_type}"',
    ]
    if draft.buyer_or_party:
        lines.append(f'  lamella-disposal-party: "{_esc(draft.buyer_or_party)}"')
    if draft.notes:
        lines.append(f'  lamella-disposal-notes: "{_esc(draft.notes)}"')
    # Book value out of the asset account.
    lines.append(
        f"  {draft.asset_account}  -{_fmt_amount(draft.asset_amount_out)} USD"
    )
    # Proceeds into the destination account.
    lines.append(
        f"  {draft.proceeds_account}  {_fmt_amount(draft.proceeds_amount)} USD"
    )
    # Signed plug — positive on an Income account reads as a gain,
    # positive on an Expense account reads as a loss.
    lines.append(
        f"  {draft.gain_loss_account}  {_fmt_amount(draft.gain_loss_amount)} USD"
    )
    return "\n".join(lines) + "\n"


def render_revoke_block(
    *,
    revoke_id: str,
    original: DisposalDraft,
    revoke_date: date_t | None = None,
) -> str:
    """Render a reversing transaction for `original`. Every amount is
    negated; `lamella-disposal-revokes` points back at the original. The
    revoke is its own transaction with its own `lamella-disposal-id`.

    `revoke_date` defaults to today — the ledger shouldn't be rewriting
    history; the revoke records "on this date we backed out the
    earlier disposal because of a correction."
    """
    rdate = revoke_date or date_t.today()
    narration = (
        f"Vehicle disposal revoked — {original.vehicle_display_name or original.vehicle_slug}"
    )
    lines = [
        "",
        f'{rdate.isoformat()} * "Vehicle disposal revoked" "{_esc(narration)}" {DISPOSAL_TAG}',
        f'  lamella-disposal-id: "{revoke_id}"',
        f'  lamella-disposal-revokes: "{original.disposal_id}"',
        f'  lamella-disposal-vehicle: "{original.vehicle_slug}"',
        f'  lamella-disposal-date: "{rdate.isoformat()}"',
        f'  lamella-disposal-type: "{original.disposal_type}"',
        # Negate every posting.
        f"  {original.asset_account}  {_fmt_amount(original.asset_amount_out)} USD",
        f"  {original.proceeds_account}  -{_fmt_amount(original.proceeds_amount)} USD",
        f"  {original.gain_loss_account}  {_fmt_amount(-original.gain_loss_amount)} USD",
    ]
    return "\n".join(lines) + "\n"


def _append_with_check(
    *,
    main_bean: Path,
    overrides_path: Path,
    block: str,
    skip_check: bool = False,
) -> None:
    """Append `block` to `overrides_path` and bean-check. On check
    failure, revert both files to their pre-append bytes. Mirrors the
    loan-funding writer flow."""
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
    draft: DisposalDraft,
    main_bean: Path,
    overrides_path: Path,
    skip_check: bool = False,
) -> None:
    """Append the disposal block to the overrides file and bean-check
    (unless skip_check). Raises BeanCheckError on a regression; both
    files are rolled back to their pre-write bytes before the raise."""
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


def write_revoke(
    *,
    revoke_id: str,
    original: DisposalDraft,
    main_bean: Path,
    overrides_path: Path,
    revoke_date: date_t | None = None,
    skip_check: bool = False,
) -> None:
    """Append a reversing transaction for `original`. The caller
    provides `revoke_id` (a fresh UUID) which becomes the revoke
    transaction's own lamella-disposal-id."""
    block = render_revoke_block(
        revoke_id=revoke_id,
        original=original,
        revoke_date=revoke_date,
    )
    _append_with_check(
        main_bean=main_bean,
        overrides_path=overrides_path,
        block=block,
        skip_check=skip_check,
    )


def compute_gain_loss(
    *,
    proceeds: Decimal,
    adjusted_basis: Decimal,
    accumulated_depreciation: Decimal,
) -> Decimal:
    """Informational gain/loss computed from user-entered basis and
    depreciation. A positive result is a gain (goes to an income
    account); negative is a loss (goes to an expense account).

    This is NOT an authoritative tax determination — Section 1245
    recapture rules characterize part of the gain as ordinary income
    rather than capital gain, and that split depends on depreciation
    history. The preview surfaces this number plainly labeled as a
    worksheet figure; the user's CPA decides how to characterize it.
    """
    remaining_basis = adjusted_basis - accumulated_depreciation
    return proceeds - remaining_basis
