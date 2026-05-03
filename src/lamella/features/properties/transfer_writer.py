# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Property transfer (intercompany) writer.

Mirrors :mod:`lamella.features.vehicles.transfer_writer`. Real-estate
transfer between two entities the user owns: disposal entry on the
old entity + acquisition entry on the new, written atomically through
:func:`lamella.features.setup.recovery.recovery_write_envelope`.

Per the bookkeeper-not-tax product directive (see
``FEATURE_SETUP_4.2_4.3_QUESTIONS.md``): the system records facts the
user states. §1031 like-kind exchange vs. taxable sale vs. owner
contribution / §721 partnership-capital treatment is the CPA's call.
The ``SaleRecapture`` plug is the CPA-touchpoint when transaction
value differs from book value.

Slug-embedded paths so a CPA can read the chart of accounts and tie
sub-entries to the property they relate to:

* ``Assets:<Old>:Property:<Slug>:SaleClearing`` — incoming-cash
  clearing on the selling entity (only when cash > 0).
* ``Equity:<Old>:Property:<Slug>:SaleEquity`` — equity portion of the
  disposition (only when equity > 0).
* ``Equity:<Old>:Property:<Slug>:SaleRecapture`` — gap between book
  value and (cash + equity). Sign matters: negative = gain (Beancount
  equity convention), positive = loss.
* ``Assets:<New>:Property:<Slug>`` — asset on the receiving entity.
* ``Assets:<New>:Property:<Slug>:PurchaseClearing`` — outgoing-cash
  clearing on the buying entity.
* ``Equity:<New>:Property:<Slug>:PurchaseEquity`` — equity portion of
  acquisition.
* ``Equity:<New>:Property:<Slug>:PurchaseBasisAdjustment`` — only
  posts when chosen new basis differs from transaction value
  (carryover NBV case).
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, date as date_t, datetime
from decimal import Decimal

log = logging.getLogger(__name__)


TRANSFER_DISPOSAL_TAG = "#lamella-property-transfer-out"
TRANSFER_ACQUISITION_TAG = "#lamella-property-transfer-in"


@dataclass(frozen=True)
class PropertyTransferDraft:
    transfer_id: str
    property_slug: str
    property_display_name: str | None
    transfer_date: date_t
    old_entity: str
    new_entity: str
    book_value: Decimal
    cash_amount: Decimal
    equity_amount: Decimal
    new_basis: Decimal
    notes: str | None = None


def new_transfer_id() -> str:
    return uuid.uuid4().hex


def _esc(s: str | None) -> str:
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _fmt(d: Decimal) -> str:
    return f"{d.quantize(Decimal('0.01')):.2f}"


def property_asset_path(entity: str, slug: str) -> str:
    return f"Assets:{entity}:Property:{slug}"


def disposal_chart_paths(
    *, entity: str, slug: str,
    cash: bool, equity: bool, recapture: bool,
) -> list[str]:
    out: list[str] = []
    base = f"Assets:{entity}:Property:{slug}"
    eq_base = f"Equity:{entity}:Property:{slug}"
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
    out: list[str] = []
    base = f"Assets:{entity}:Property:{slug}"
    eq_base = f"Equity:{entity}:Property:{slug}"
    if cash:
        out.append(f"{base}:PurchaseClearing")
    if equity:
        out.append(f"{eq_base}:PurchaseEquity")
    return out


def render_disposal_block(draft: PropertyTransferDraft) -> str:
    asset = property_asset_path(draft.old_entity, draft.property_slug)
    eq_base = f"Equity:{draft.old_entity}:Property:{draft.property_slug}"
    cash_path = f"{asset}:SaleClearing"
    eq_path = f"{eq_base}:SaleEquity"
    rec_path = f"{eq_base}:SaleRecapture"

    transaction_value = draft.cash_amount + draft.equity_amount
    # Beancount equity convention: gain credits equity → negative;
    # loss debits → positive. Math: -book + cash + equity + recap = 0.
    recapture = draft.book_value - transaction_value

    narration = (
        f"Property transfer — "
        f"{draft.property_display_name or draft.property_slug} "
        f"→ {draft.new_entity}"
    )
    lines = [
        "",
        f'{draft.transfer_date.isoformat()} * "Property transfer out" '
        f'"{_esc(narration)}" {TRANSFER_DISPOSAL_TAG}',
        f'  lamella-transfer-id: "{draft.transfer_id}"',
        f'  lamella-transfer-property: "{draft.property_slug}"',
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


def render_acquisition_block(draft: PropertyTransferDraft) -> str:
    asset = property_asset_path(draft.new_entity, draft.property_slug)
    eq_base = f"Equity:{draft.new_entity}:Property:{draft.property_slug}"
    cash_path = f"{asset}:PurchaseClearing"
    eq_path = f"{eq_base}:PurchaseEquity"

    transaction_value = draft.cash_amount + draft.equity_amount
    basis_adjustment = transaction_value - draft.new_basis

    narration = (
        f"Property transfer — "
        f"{draft.property_display_name or draft.property_slug} "
        f"← {draft.old_entity}"
    )
    lines = [
        "",
        f'{draft.transfer_date.isoformat()} * "Property transfer in" '
        f'"{_esc(narration)}" {TRANSFER_ACQUISITION_TAG}',
        f'  lamella-transfer-id: "{draft.transfer_id}"',
        f'  lamella-transfer-property: "{draft.property_slug}"',
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
        adj_path = f"{eq_base}:PurchaseBasisAdjustment"
        lines.append(f"  {adj_path}  -{_fmt(basis_adjustment)} USD")
    return "\n".join(lines) + "\n"


def required_open_paths(draft: PropertyTransferDraft) -> list[str]:
    paths = [property_asset_path(draft.old_entity, draft.property_slug)]
    paths.extend(disposal_chart_paths(
        entity=draft.old_entity, slug=draft.property_slug,
        cash=draft.cash_amount > 0,
        equity=draft.equity_amount > 0,
        recapture=(draft.cash_amount + draft.equity_amount) != draft.book_value,
    ))
    paths.append(property_asset_path(draft.new_entity, draft.property_slug))
    paths.extend(acquisition_chart_paths(
        entity=draft.new_entity, slug=draft.property_slug,
        cash=draft.cash_amount > 0,
        equity=draft.equity_amount > 0,
    ))
    transaction_value = draft.cash_amount + draft.equity_amount
    if transaction_value != draft.new_basis:
        eq_base = f"Equity:{draft.new_entity}:Property:{draft.property_slug}"
        paths.append(f"{eq_base}:PurchaseBasisAdjustment")
    return paths
