# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for the ``custom "receipt-dismissed"`` directive.

Dismissals are part of the reconstruct contract: a user acknowledging
"this transaction never gets a receipt" is state, not cache. It has to
survive a DB delete. We stamp each dismissal as a directive in
``connector_links.bean`` (co-located with receipt links — all
receipt-related ledger state lives in one file).

``txn_hash`` invalidation on edit is intentional: an edited
transaction's hash changes, the dismissal stops matching, and the
txn re-surfaces in the review queue with the "previously dismissed"
context so the user can re-dismiss with one click. See
``needs_queue.previously_dismissed_for`` for the re-surface path.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    append_custom_directive,
    custom_arg,
    custom_meta,
    read_custom_directives,
)

log = logging.getLogger(__name__)


CONNECTOR_LINKS_HEADER = "; Managed by Lamella. Do not hand-edit.\n"


def append_dismissal(
    *,
    connector_links: Path,
    main_bean: Path,
    txn_hash: str,
    reason: str | None = None,
    dismissed_by: str = "user",
    dismissed_at: datetime | None = None,
    backfilled: bool = False,
    run_check: bool = True,
) -> str:
    ts = dismissed_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-dismissed-at": ts,
        "lamella-dismissed-by": dismissed_by,
    }
    if reason:
        meta["lamella-reason"] = reason
    if backfilled:
        meta["lamella-backfilled"] = True
    return append_custom_directive(
        target=connector_links,
        main_bean=main_bean,
        header=CONNECTOR_LINKS_HEADER,
        directive_date=ts.date() if isinstance(ts, datetime) else date.today(),
        directive_type="receipt-dismissed",
        args=[txn_hash],
        meta=meta,
        run_check=run_check,
    )


def append_dismissal_revoke(
    *,
    connector_links: Path,
    main_bean: Path,
    txn_hash: str,
    revoked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    """Record that a previous dismissal is no longer active. Append-only
    semantics: we never delete a dismissal directive, we stamp a
    matching revoke so the ledger preserves history. Reconstruct's
    read path considers a txn dismissed iff the most recent directive
    for its hash is a dismissal (not a revoke)."""
    ts = revoked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    return append_custom_directive(
        target=connector_links,
        main_bean=main_bean,
        header=CONNECTOR_LINKS_HEADER,
        directive_date=ts.date(),
        directive_type="receipt-dismissal-revoked",
        args=[txn_hash],
        meta={"lamella-revoked-at": ts},
        run_check=run_check,
    )


def read_dismissals_from_entries(entries) -> list[dict]:
    """Parse every ``custom "receipt-dismissed"`` directive in the
    loaded ledger entries and filter by matching revokes. A dismissal
    is active iff the most recent dismissal/revoke directive for its
    txn_hash is a dismissal. Returns rows ready for upsert into the
    ``receipt_dismissals`` cache.
    """
    # Walk both directive types in load order; for each txn_hash, the
    # last entry wins.
    state: dict[str, dict | None] = {}  # txn_hash -> row or None (revoked)
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in ("receipt-dismissed", "receipt-dismissal-revoked"):
            continue
        txn_hash = custom_arg(entry, 0)
        if not isinstance(txn_hash, str) or not txn_hash:
            continue
        if entry.type == "receipt-dismissal-revoked":
            state[txn_hash] = None
            continue
        reason = custom_meta(entry, "lamella-reason")
        dismissed_by = custom_meta(entry, "lamella-dismissed-by") or "user"
        dismissed_at = custom_meta(entry, "lamella-dismissed-at")
        if isinstance(dismissed_at, datetime):
            ts_iso = dismissed_at.isoformat(sep=" ", timespec="seconds")
        elif isinstance(dismissed_at, date):
            ts_iso = datetime.combine(
                dismissed_at, datetime.min.time()
            ).isoformat(sep=" ", timespec="seconds")
        elif isinstance(dismissed_at, str):
            ts_iso = dismissed_at
        else:
            ts_iso = datetime.combine(
                entry.date, datetime.min.time()
            ).isoformat(sep=" ", timespec="seconds")
        state[txn_hash] = {
            "txn_hash": txn_hash,
            "reason": (reason if isinstance(reason, str) and reason else None),
            "dismissed_by": (
                dismissed_by if isinstance(dismissed_by, str) else "user"
            ),
            "dismissed_at": ts_iso,
        }
    return [row for row in state.values() if row is not None]
