from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    append_custom_directive,
    custom_arg,
    custom_meta,
)
from lamella.features.receipts.directive_types import (
    DIRECTIVE_LINK_BLOCKED_NEW,
    DIRECTIVE_LINK_BLOCK_REVOKED_NEW,
    DIRECTIVE_TYPES_ALL_LINK_BLOCK,
    DIRECTIVE_TYPES_LINK_BLOCK_REVOKED,
)

CONNECTOR_LINKS_HEADER = "; Managed by Lamella. Do not hand-edit.\n"


def append_link_block(
    *,
    connector_links: Path,
    main_bean: Path,
    txn_hash: str,
    paperless_id: int,
    reason: str | None = None,
    blocked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    ts = blocked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta = {"lamella-paperless-id": int(paperless_id), "lamella-blocked-at": ts}
    if reason:
        meta["lamella-reason"] = reason
    return append_custom_directive(
        target=connector_links,
        main_bean=main_bean,
        header=CONNECTOR_LINKS_HEADER,
        directive_date=ts.date() if isinstance(ts, datetime) else date.today(),
        directive_type=DIRECTIVE_LINK_BLOCKED_NEW,
        args=[txn_hash],
        meta=meta,
        run_check=run_check,
    )


def append_link_block_revoke(
    *,
    connector_links: Path,
    main_bean: Path,
    txn_hash: str,
    paperless_id: int,
    revoked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    ts = revoked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    return append_custom_directive(
        target=connector_links,
        main_bean=main_bean,
        header=CONNECTOR_LINKS_HEADER,
        directive_date=ts.date(),
        directive_type=DIRECTIVE_LINK_BLOCK_REVOKED_NEW,
        args=[txn_hash],
        meta={"lamella-paperless-id": int(paperless_id), "lamella-revoked-at": ts},
        run_check=run_check,
    )


def append_doc_deleted_tombstone(
    *,
    connector_links: Path,
    main_bean: Path,
    paperless_id: int,
    purged_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    """Write a ``paperless-doc-deleted`` custom directive to
    ``connector_links.bean``.

    This is the Beancount-layer tombstone for a Paperless document
    confirmed deleted after the dangling-link gate (3 consecutive 404s
    + 7-day cooldown). The directive is the source of truth for
    reconstruct; the ``paperless_deleted_docs`` SQLite table is the
    fast-query cache.

    A future sync that re-encounters this ``paperless_id`` (e.g. the
    user re-uploads a document that gets the same integer ID — unlikely
    but possible) will NOT silently re-ingest it if the tombstone
    exists. The sync skips rows present in ``paperless_deleted_docs``.
    """
    ts = purged_at or datetime.now(timezone.utc).replace(tzinfo=None)
    return append_custom_directive(
        target=connector_links,
        main_bean=main_bean,
        header=CONNECTOR_LINKS_HEADER,
        directive_date=ts.date() if isinstance(ts, datetime) else date.today(),
        directive_type="paperless-doc-deleted",
        args=[],
        meta={
            "lamella-paperless-id": int(paperless_id),
            "lamella-purged-at": ts,
        },
        run_check=run_check,
    )


def read_link_blocks_from_entries(entries) -> list[dict]:
    """Read link-block directives. Both v3 (receipt-link-blocked /
    receipt-link-block-revoked) and v4 (document-link-blocked /
    document-link-block-revoked) vocabularies are accepted per
    ADR-0061. The active set is the union of both — they are
    semantically the same directive, just renamed.
    """
    state: dict[tuple[str, int], dict | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in DIRECTIVE_TYPES_ALL_LINK_BLOCK:
            continue
        txn_hash = custom_arg(entry, 0)
        if not isinstance(txn_hash, str) or not txn_hash:
            continue
        pid_raw = custom_meta(entry, "lamella-paperless-id")
        try:
            pid = int(pid_raw)
        except Exception:
            continue
        key = (txn_hash, pid)
        if entry.type in DIRECTIVE_TYPES_LINK_BLOCK_REVOKED:
            state[key] = None
            continue
        reason = custom_meta(entry, "lamella-reason")
        state[key] = {
            "txn_hash": txn_hash,
            "paperless_id": pid,
            "reason": reason if isinstance(reason, str) and reason else None,
        }
    return [r for r in state.values() if r is not None]
