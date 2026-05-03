# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0044: matcher → Paperless writeback of the four canonical
``Lamella_*`` custom fields.

When the receipt matcher links a Paperless document to a ledger txn
with high confidence, four pieces of context want to flow back to
Paperless so the user can search the document store by entity,
category, txn-id, or payment account without consulting Lamella:

| Field name         | Source on the matched txn                   |
|--------------------|---------------------------------------------|
| ``Lamella_Entity`` | entity slug (second segment of an account)  |
| ``Lamella_Category``| full Beancount account path (e.g. ``Expenses:AcmeCoLLC:OfficeSupplies``) |
| ``Lamella_TXN``    | ``lamella-txn-id`` UUIDv7 (per ADR-0019)    |
| ``Lamella_Account``| display name of the payment account         |

This module is the bridge between :class:`DocumentLinker` (which
writes the link directive into ``connector_links.bean``) and
:class:`PaperlessClient` (which talks HTTP to Paperless). It pulls
the values off the matched ``Transaction`` and hands them to the
client's :meth:`writeback_lamella_fields` after going through the
``ensure_lamella_writeback_fields`` idempotent-create step.

ADR-0044 contract:

* **Field creation failures must NOT block matching.** This module
  catches errors from ``ensure_*`` / ``writeback_*`` and logs them.
  The match (the receipt-link directive) is already on disk by the
  time we get here.
* **Writeback is gated by the same confidence threshold as the
  link itself.** The matcher decides; this module just executes.
* **No non-`Lamella_` fields are written.** The client-side
  :class:`InvalidWritebackFieldError` enforces this; we shouldn't
  ever trip it because every name we send is one of the four
  constants from :data:`LAMELLA_WRITEBACK_FIELD_NAMES`.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING, Any

from beancount.core.data import Transaction

from lamella.adapters.paperless.client import (
    InvalidWritebackFieldError,
    PaperlessClient,
    PaperlessError,
)
from lamella.core.beancount_io.txn_hash import txn_hash as compute_hash
from lamella.core.identity import get_txn_id
from lamella.features.paperless_bridge.lamella_namespace import (
    FIELD_ACCOUNT,
    FIELD_CATEGORY,
    FIELD_ENTITY,
    FIELD_TXN,
)

if TYPE_CHECKING:
    from lamella.core.beancount_io import LedgerReader
    from lamella.core.config import Settings

log = logging.getLogger(__name__)


# Beancount account roots. The "entity" segment is the SECOND part of
# an entity-first account path (ADR-0007: ``Expenses:Entity:Category``).
# The first part is always one of these roots.
_ACCOUNT_ROOTS: frozenset[str] = frozenset({
    "Assets", "Liabilities", "Equity", "Income", "Expenses",
})


def _entity_slug_from_txn(txn: Transaction) -> str | None:
    """Return the entity slug for ``txn``, derived from the postings'
    account paths per ADR-0007's ``<Root>:<Entity>:<...>`` shape.

    Walks every posting once and returns the first non-empty entity
    segment found. Postings on a system-managed root (``Equity``,
    ``Income`` for some entities) carry the same entity, so any one
    answer is correct as long as the txn is internally consistent.

    Returns ``None`` when no posting has a recognizable entity (the
    canonical "FIXME stage 0" case where everything routes through
    ``Expenses:FIXME``). The writeback gracefully omits the field.
    """
    for p in txn.postings or ():
        acct = p.account or ""
        if not acct:
            continue
        parts = acct.split(":")
        if len(parts) < 2:
            continue
        root, entity = parts[0], parts[1]
        if root not in _ACCOUNT_ROOTS:
            continue
        if not entity or entity == "FIXME":
            continue
        return entity
    return None


def _category_account_from_txn(txn: Transaction) -> str | None:
    """Return the full Beancount account path that should land in
    ``Lamella_Category``. Per ADR-0044 the category is the booked
    expense (or income) account — e.g.
    ``Expenses:AcmeCoLLC:OfficeSupplies``. The payment leg
    (``Assets:`` / ``Liabilities:``) is captured separately by
    :func:`_payment_account_from_txn` and lands in
    ``Lamella_Account``.

    Picks the highest-magnitude ``Expenses`` posting first; falls
    back to ``Income`` only when there's no expense leg (refund
    receipts, rebate inflows). Equity / Assets / Liabilities are
    deliberately NOT considered here — those are payment legs, not
    categories.
    """
    best_acct: str | None = None
    best_amount: Any = None
    # First-pass: the dominant Expenses posting.
    for p in txn.postings or ():
        acct = p.account or ""
        if not acct:
            continue
        if acct.split(":", 1)[0] != "Expenses":
            continue
        if p.units is None or p.units.number is None:
            continue
        try:
            mag = abs(p.units.number)
        except Exception:  # noqa: BLE001
            continue
        if best_amount is None or mag > best_amount:
            best_amount = mag
            best_acct = acct
    if best_acct is not None:
        return best_acct
    # Second-pass: an Income leg (refund receipt). Lower magnitude
    # rules so a tiny rebate posting still gets attributed.
    for p in txn.postings or ():
        acct = p.account or ""
        if not acct:
            continue
        if acct.split(":", 1)[0] != "Income":
            continue
        if p.units is None or p.units.number is None:
            continue
        try:
            mag = abs(p.units.number)
        except Exception:  # noqa: BLE001
            continue
        if best_amount is None or mag > best_amount:
            best_amount = mag
            best_acct = acct
    return best_acct


def _payment_account_from_txn(txn: Transaction) -> str | None:
    """Return the Beancount account path of the posting that
    represents the payment leg — typically the credit-card or
    checking-account posting.

    Picks the first ``Assets:`` or ``Liabilities:`` posting on the
    txn. Real-world receipts usually have exactly one (the card or
    the checking account). When multiple exist (a transfer), the
    first one is acceptable; the matcher only writes back receipts
    that link to a real expense, so the dominant payment leg is
    deterministic per txn.
    """
    for p in txn.postings or ():
        acct = p.account or ""
        if not acct:
            continue
        root = acct.split(":", 1)[0]
        if root in ("Assets", "Liabilities"):
            return acct
    return None


def _payment_account_display(
    conn: sqlite3.Connection | None, account_path: str | None,
) -> str | None:
    """Resolve a payment-account path to the human-friendly display
    name that lands in ``Lamella_Account``.

    Strategy:
      1. If a sqlite3 connection is available, defer to
         :func:`lamella.core.registry.alias.alias_for` — that's the
         project-wide source of truth for "what does this account
         look like in the UI". Picks up
         ``accounts_meta.display_name`` when set, else the
         heuristic pretty-format.
      2. Otherwise (test fixtures, headless callers), fall back to
         the raw account path. The user still sees a useful value
         in Paperless; just less polished than the UI version.
    """
    if not account_path:
        return None
    if conn is not None:
        try:
            from lamella.core.registry.alias import alias_for
            display = alias_for(conn, account_path)
            if display:
                return display
        except Exception as exc:  # noqa: BLE001 — never block a match
            log.info(
                "ADR-0044: alias_for(%r) failed; using raw path: %s",
                account_path, exc,
            )
    return account_path


def build_writeback_values(
    txn: Transaction,
    *,
    conn: sqlite3.Connection | None = None,
) -> dict[str, str]:
    """Build the ``{field_name: value}`` dict for a successful match.

    Empty / None pieces are dropped — the writeback writes only the
    fields it has real values for, never blank strings (a blank
    Paperless field is indistinguishable from "user cleared it").

    Callers pass the matched ``Transaction`` plus an optional sqlite
    connection (to resolve the payment-account display name via
    :mod:`lamella.core.registry.alias`).
    """
    out: dict[str, str] = {}
    entity = _entity_slug_from_txn(txn)
    if entity:
        out[FIELD_ENTITY] = entity
    category = _category_account_from_txn(txn)
    if category:
        out[FIELD_CATEGORY] = category
    txn_id = get_txn_id(txn)
    if txn_id:
        out[FIELD_TXN] = txn_id
    payment_acct = _payment_account_from_txn(txn)
    payment_display = _payment_account_display(conn, payment_acct)
    if payment_display:
        out[FIELD_ACCOUNT] = payment_display
    return out


async def write_match_fields(
    *,
    client: PaperlessClient,
    paperless_id: int,
    txn: Transaction,
    conn: sqlite3.Connection | None = None,
) -> dict[str, str]:
    """Execute the post-match writeback for one ``(paperless_id, txn)``
    pair. Returns the values actually written (may be empty when the
    txn lacks usable signals; never raises into the caller).

    Failure modes that are caught and logged here (so the caller sees
    a successful match even if Paperless is unreachable):

    * ``PaperlessError`` from the field-listing or PATCH call —
      Paperless is down, returned 4xx/5xx, etc. The match stays on
      disk; next sweep re-runs the writeback.
    * ``InvalidWritebackFieldError`` — a programming error inside
      this module would let one through. Logged at WARNING with the
      offending name; caller still sees ``{}``.

    Per ADR-0044, this MUST NOT propagate exceptions: the receipt
    link is already committed to ``connector_links.bean`` by the
    time we get here, and the writeback is a best-effort follow-on.
    """
    values = build_writeback_values(txn, conn=conn)
    if not values:
        log.info(
            "ADR-0044: doc %d has no usable writeback values from txn %s "
            "(no entity / category / txn-id / payment account) — "
            "skipping writeback.",
            paperless_id, get_txn_id(txn) or "(no txn-id)",
        )
        return {}
    try:
        await client.writeback_lamella_fields(
            paperless_id, values=values, ensure_fields=True,
        )
    except InvalidWritebackFieldError as exc:
        # Programming error — caught so the caller doesn't lose
        # the match. Surfaced loudly in the log.
        log.warning(
            "ADR-0044: invalid writeback field name reached "
            "PaperlessClient (this is a bug in writeback.py): %s",
            exc,
        )
        return {}
    except PaperlessError as exc:
        log.warning(
            "ADR-0044: writeback to Paperless doc %d failed — match "
            "stands, writeback will be retried next round: %s",
            paperless_id, exc,
        )
        return {}
    return values


async def writeback_after_link(
    *,
    paperless_id: int,
    txn_hash: str,
    settings: "Settings",
    reader: "LedgerReader",
    conn: sqlite3.Connection | None,
) -> dict[str, str]:
    """Best-effort post-link writeback of the four ADR-0044 fields,
    invoked from the user-facing manual-attach routes after
    :meth:`DocumentLinker.link` succeeds.

    The matcher sweep paths (``auto_match.py`` / ``hunt.py``) already
    wire :func:`write_match_fields` directly because they hold a
    long-lived ``PaperlessClient`` for the whole sweep. The manual
    routes don't — they spin up a one-shot client per attach. This
    helper hides that bookkeeping so each route just calls
    ``await writeback_after_link(...)`` once.

    Resolution rules:

    * ``txn_hash`` may be either a Beancount content hash (legacy /
      ledger-row paths) or a ``lamella-txn-id`` UUIDv7 (the
      ``/txn/{token}`` paths). We match on either: first by computing
      ``txn_hash(entry)`` and comparing, then by walking
      ``lamella-txn-id`` + ``lamella-txn-id-alias-N`` meta keys.
    * Skips silently when Paperless isn't configured. ADR-0044
      writeback is the canonical contract; it is only off when the
      whole integration is off.
    * Catches every exception path (Paperless down, ledger missing
      the txn, etc.) and returns ``{}``. The link is the user-visible
      action; the writeback is supplemental and must never break it.

    Returns the dict of fields actually written, or ``{}`` when no
    writeback happened (Paperless off, txn not in ledger, no usable
    fields, or transient failure).
    """
    if not settings.paperless_configured:
        return {}

    # Find the matching txn. Try content-hash first (cheap, the
    # common case); then fall back to lamella-txn-id + aliases.
    target = (txn_hash or "").lower()
    if not target:
        return {}
    try:
        ledger = reader.load()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "ADR-0044 manual writeback: ledger load failed for doc "
            "%d / txn %s: %s",
            paperless_id, txn_hash[:12], exc,
        )
        return {}

    matched: Transaction | None = None
    for entry in ledger.entries:
        if not isinstance(entry, Transaction):
            continue
        try:
            if compute_hash(entry).lower() == target:
                matched = entry
                break
        except Exception:  # noqa: BLE001
            pass
        primary = get_txn_id(entry)
        if primary and primary.lower() == target:
            matched = entry
            break
        meta = getattr(entry, "meta", None) or {}
        alias_hit = False
        for k, v in meta.items():
            if not isinstance(k, str):
                continue
            if k.startswith("lamella-txn-id-alias-") and v:
                if str(v).lower() == target:
                    alias_hit = True
                    break
        if alias_hit:
            matched = entry
            break

    if matched is None:
        log.info(
            "ADR-0044 manual writeback: no ledger txn matched "
            "id=%s for doc %d — skipping writeback (link still "
            "stands).",
            txn_hash[:12], paperless_id,
        )
        return {}

    client: PaperlessClient | None = None
    try:
        client = PaperlessClient(
            base_url=settings.paperless_url,  # type: ignore[arg-type]
            api_token=settings.paperless_api_token.get_secret_value(),  # type: ignore[union-attr]
            extra_headers=settings.paperless_extra_headers(),
        )
        return await write_match_fields(
            client=client,
            paperless_id=int(paperless_id),
            txn=matched,
            conn=conn,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "ADR-0044 manual writeback: unexpected failure for doc "
            "%d / txn %s: %s",
            paperless_id, txn_hash[:12], exc,
        )
        return {}
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                pass
