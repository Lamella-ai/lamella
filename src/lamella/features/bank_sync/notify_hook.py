# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging

from lamella.ports.notification import NotificationEvent, Priority
from lamella.features.notifications.dispatcher import Dispatcher
from lamella.features.bank_sync.ingest import IngestResult, LargeFixmeEvent

log = logging.getLogger(__name__)


def _build_event(fixme: LargeFixmeEvent) -> NotificationEvent:
    merchant = fixme.merchant or "(unknown merchant)"
    body = (
        f"${fixme.amount:.2f} at {merchant} on {fixme.posted_date.isoformat()} "
        f"from {fixme.source_account} — needs review."
    )
    # Deep-link into the review queue using the txn id as the hash anchor.
    url = f"/review#txn={fixme.txn_id}"
    return NotificationEvent(
        dedup_key=f"fixme:{fixme.txn_id}",
        priority=Priority.WARN,
        title=f"Large FIXME: ${fixme.amount:.2f}",
        body=body,
        url=url,
    )


async def dispatch_large_fixmes(
    *,
    dispatcher: Dispatcher,
    result: IngestResult,
) -> int:
    """Fan the ingest's large-FIXME events through the dispatcher. Returns
    the number of events considered (not necessarily delivered — dedup and
    rate-limit drops still count as considered)."""
    count = 0
    for fixme in result.large_fixmes:
        try:
            await dispatcher.send(_build_event(fixme))
        except Exception as exc:  # noqa: BLE001
            log.warning("large-FIXME notify failed for %s: %s", fixme.txn_id, exc)
        count += 1
    return count
