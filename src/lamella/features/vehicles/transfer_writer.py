# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Vehicle transfer (intercompany) writer.

A *transfer* is an entity-to-entity handoff where the user owns both
sides — Personal → Acme, Acme → Personal, or Acme → Beta. The vehicle
moves to the target entity's books; old entity records a disposition,
new entity records an acquisition. Two ledger transactions, written
atomically through :func:`lamella.features.setup.recovery.recovery_write_envelope`
so any bean-check regression rolls both sides back together.

Distinct from :mod:`lamella.features.vehicles.disposal_writer`:

* Disposal = the vehicle leaves the user's books entirely (sold to
  outside party / scrapped / gifted to a third party).
* Transfer = the vehicle stays under the user's control, just changes
  which of their entities owns it.

Distinct from :func:`lamella.web.routes.vehicles.vehicle_change_ownership_rename`:

* Rename = the user labeled the vehicle on the wrong entity from day
  one. Books get rewritten; no real-world event happened.
* Transfer = a real event happened on a specific date; pre-event
  history stays attached to the old entity, post-event history attaches
  to the new entity.

Posting structure — see ``FEATURE_SETUP_4.2_4.3_QUESTIONS.md`` for the
design discussion that drove these choices. Slug embedded in every
scaffolded path so a CPA reading the books can tell at a glance which
vehicle a sub-account ties to:

* ``Assets:<Old>:Vehicle:<Slug>:SaleClearing`` — incoming-cash clearing
  on the selling entity (only scaffolded when cash > 0).
* ``Equity:<Old>:Vehicle:<Slug>:SaleEquity`` — equity portion of the
  disposition (only when equity > 0).
* ``Equity:<Old>:Vehicle:<Slug>:SaleRecapture`` — gap between book
  value and (cash + equity) when the user-entered transaction value
  doesn't match book value. Sign matters: positive = gain/recapture,
  negative = loss. CPA reconciles.
* ``Assets:<New>:Vehicle:<Slug>`` — the asset itself on the receiving
  entity.
* ``Assets:<New>:Vehicle:<Slug>:PurchaseClearing`` — outgoing-cash
  clearing on the buying entity (only when cash > 0).
* ``Equity:<New>:Vehicle:<Slug>:PurchaseEquity`` — equity portion of
  the acquisition (only when equity > 0).
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, date as date_t, datetime
from decimal import Decimal
from pathlib import Path

log = logging.getLogger(__name__)


TRANSFER_DISPOSAL_TAG = "#lamella-vehicle-transfer-out"
TRANSFER_ACQUISITION_TAG = "#lamella-vehicle-transfer-in"


@dataclass(frozen=True)
class TransferDraft:
    """Facts describing one intercompany transfer."""
    transfer_id: str
    vehicle_slug: str
    vehicle_display_name: str | None
    transfer_date: date_t
    old_entity: str
    new_entity: str
    book_value: Decimal           # current NBV on old entity, positive
    cash_amount: Decimal          # real-money portion, ≥ 0
    equity_amount: Decimal        # owner-equity portion, ≥ 0
    new_basis: Decimal            # asset basis on new entity (carryover/sale-price/explicit)
    notes: str | None = None


def new_transfer_id() -> str:
    return uuid.uuid4().hex


def _esc(s: str | None) -> str:
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _fmt(d: Decimal) -> str:
    return f"{d.quantize(Decimal('0.01')):.2f}"


def vehicle_asset_path(entity: str, slug: str) -> str:
    return f"Assets:{entity}:Vehicle:{slug}"


def disposal_chart_paths(
    *, entity: str, slug: str,
    cash: bool, equity: bool, recapture: bool,
) -> list[str]:
    """Old-entity scaffolded sub-accounts. Caller passes flags so we
    only open what's actually used in the specific transfer."""
    out: list[str] = []
    base = f"Assets:{entity}:Vehicle:{slug}"
    eq_base = f"Equity:{entity}:Vehicle:{slug}"
    if cash:
        out.append(f"{base}:SaleClearing")
    if equity:
        out.append(f"{eq_base}:SaleEquity")
    if recapture:
        out.append(f"{eq_base}:SaleRecapture")
    return out


def acquisition_chart_paths(
    *, entity: str, slug: str, cash: bool, equity: bool,
) -> list[str]:
    """New-entity scaffolded sub-accounts (the asset itself is opened
    via :func:`registry.vehicle_companion.ensure_vehicle_chart` along
    with the canonical operating chart)."""
    out: list[str] = []
    base = f"Assets:{entity}:Vehicle:{slug}"
    eq_base = f"Equity:{entity}:Vehicle:{slug}"
    if cash:
        out.append(f"{base}:PurchaseClearing")
    if equity:
        out.append(f"{eq_base}:PurchaseEquity")
    return out


def render_disposal_block(draft: TransferDraft) -> str:
    """OLD-entity transaction: vehicle leaves at book value. Cash leg
    lands in SaleClearing (reconciled when SimpleFIN brings the
    deposit). Equity leg posts directly to SaleEquity. Any gap between
    book value and (cash + equity) plugs to SaleRecapture for the CPA
    to reconcile."""
    asset = vehicle_asset_path(draft.old_entity, draft.vehicle_slug)
    eq_base = f"Equity:{draft.old_entity}:Vehicle:{draft.vehicle_slug}"
    cash_path = f"{asset}:SaleClearing"
    eq_path = f"{eq_base}:SaleEquity"
    rec_path = f"{eq_base}:SaleRecapture"

    transaction_value = draft.cash_amount + draft.equity_amount
    # Sign per Beancount equity convention: equity accounts naturally
    # carry credit (negative) balances when the owner has gained value.
    # SaleRecapture > 0 → loss (debit equity, owner is out of pocket).
    # SaleRecapture < 0 → gain / recapturable income (credit equity).
    # Math: -book_value (asset out) + cash + equity + recapture = 0
    # ⇒ recapture = book_value - cash - equity = book_value - transaction_value.
    recapture = draft.book_value - transaction_value

    narration = (
        f"Vehicle transfer — "
        f"{draft.vehicle_display_name or draft.vehicle_slug} "
        f"→ {draft.new_entity}"
    )
    lines = [
        "",
        f'{draft.transfer_date.isoformat()} * "Vehicle transfer out" '
        f'"{_esc(narration)}" {TRANSFER_DISPOSAL_TAG}',
        f'  lamella-transfer-id: "{draft.transfer_id}"',
        f'  lamella-transfer-vehicle: "{draft.vehicle_slug}"',
        f'  lamella-transfer-from: "{draft.old_entity}"',
        f'  lamella-transfer-to: "{draft.new_entity}"',
        f'  lamella-transfer-date: "{draft.transfer_date.isoformat()}"',
        f'  lamella-transfer-cash: "{_fmt(draft.cash_amount)}"',
        f'  lamella-transfer-equity: "{_fmt(draft.equity_amount)}"',
        f'  lamella-modified-at: "{datetime.now(UTC).isoformat(timespec="seconds")}"',
    ]
    if draft.notes:
        lines.append(f'  lamella-transfer-notes: "{_esc(draft.notes)}"')
    lines.append(f"  {asset}  -{_fmt(draft.book_value)} USD")
    if draft.cash_amount > 0:
        lines.append(f"  {cash_path}  {_fmt(draft.cash_amount)} USD")
    if draft.equity_amount > 0:
        lines.append(f"  {eq_path}  {_fmt(draft.equity_amount)} USD")
    if recapture != 0:
        lines.append(f"  {rec_path}  {_fmt(recapture)} USD")
    return "\n".join(lines) + "\n"


def render_acquisition_block(draft: TransferDraft) -> str:
    """NEW-entity transaction: vehicle arrives at ``new_basis``. Cash
    leg lands in PurchaseClearing (reconciled when SimpleFIN brings the
    matching withdrawal). Equity leg posts to PurchaseEquity."""
    asset = vehicle_asset_path(draft.new_entity, draft.vehicle_slug)
    eq_base = f"Equity:{draft.new_entity}:Vehicle:{draft.vehicle_slug}"
    cash_path = f"{asset}:PurchaseClearing"
    eq_path = f"{eq_base}:PurchaseEquity"

    # Acquisition postings sum to zero.  asset + cash + equity = 0.
    # asset is positive (debit). cash + equity must equal -asset's
    # value, i.e. the transaction value contributed by the acquirer.
    # In the user-facing case where new_basis equals cash + equity,
    # the postings sum cleanly.  When new_basis differs (carryover
    # NBV != transaction value), a ``PurchaseBasisAdjustment`` plug
    # captures the mismatch — typically the §1031-style carryover
    # case where the asset records at NBV but the equity contribution
    # is the higher transaction value.
    transaction_value = draft.cash_amount + draft.equity_amount
    basis_adjustment = transaction_value - draft.new_basis

    narration = (
        f"Vehicle transfer — "
        f"{draft.vehicle_display_name or draft.vehicle_slug} "
        f"← {draft.old_entity}"
    )
    lines = [
        "",
        f'{draft.transfer_date.isoformat()} * "Vehicle transfer in" '
        f'"{_esc(narration)}" {TRANSFER_ACQUISITION_TAG}',
        f'  lamella-transfer-id: "{draft.transfer_id}"',
        f'  lamella-transfer-vehicle: "{draft.vehicle_slug}"',
        f'  lamella-transfer-from: "{draft.old_entity}"',
        f'  lamella-transfer-to: "{draft.new_entity}"',
        f'  lamella-transfer-date: "{draft.transfer_date.isoformat()}"',
        f'  lamella-transfer-basis: "{_fmt(draft.new_basis)}"',
        f'  lamella-modified-at: "{datetime.now(UTC).isoformat(timespec="seconds")}"',
    ]
    if draft.notes:
        lines.append(f'  lamella-transfer-notes: "{_esc(draft.notes)}"')
    lines.append(f"  {asset}  {_fmt(draft.new_basis)} USD")
    if draft.cash_amount > 0:
        lines.append(f"  {cash_path}  -{_fmt(draft.cash_amount)} USD")
    if draft.equity_amount > 0:
        lines.append(f"  {eq_path}  -{_fmt(draft.equity_amount)} USD")
    if basis_adjustment != 0:
        # Posts to a basis-adjustment plug under the new vehicle's
        # equity tree. Carryover NBV with higher transaction value
        # produces a positive adjustment (extra equity contributed
        # over recorded basis); reverse for the opposite case.
        adj_path = f"{eq_base}:PurchaseBasisAdjustment"
        lines.append(f"  {adj_path}  -{_fmt(basis_adjustment)} USD")
    return "\n".join(lines) + "\n"


def required_open_paths(draft: TransferDraft) -> list[str]:
    """Every account this transfer will post to that needs an Open
    directive — caller checks against current opens and writes only the
    missing ones via :class:`registry.accounts_writer.AccountsWriter`."""
    paths = [vehicle_asset_path(draft.old_entity, draft.vehicle_slug)]
    paths.extend(disposal_chart_paths(
        entity=draft.old_entity, slug=draft.vehicle_slug,
        cash=draft.cash_amount > 0,
        equity=draft.equity_amount > 0,
        recapture=(draft.cash_amount + draft.equity_amount) != draft.book_value,
    ))
    paths.append(vehicle_asset_path(draft.new_entity, draft.vehicle_slug))
    paths.extend(acquisition_chart_paths(
        entity=draft.new_entity, slug=draft.vehicle_slug,
        cash=draft.cash_amount > 0,
        equity=draft.equity_amount > 0,
    ))
    transaction_value = draft.cash_amount + draft.equity_amount
    if transaction_value != draft.new_basis:
        eq_base = f"Equity:{draft.new_entity}:Vehicle:{draft.vehicle_slug}"
        paths.append(f"{eq_base}:PurchaseBasisAdjustment")
    return paths
