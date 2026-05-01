from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    append_custom_directive,
    custom_arg,
    custom_meta,
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
        directive_type="receipt-link-blocked",
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
        directive_type="receipt-link-block-revoked",
        args=[txn_hash],
        meta={"lamella-paperless-id": int(paperless_id), "lamella-revoked-at": ts},
        run_check=run_check,
    )


def read_link_blocks_from_entries(entries) -> list[dict]:
    state: dict[tuple[str, int], dict | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in ("receipt-link-blocked", "receipt-link-block-revoked"):
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
        if entry.type == "receipt-link-block-revoked":
            state[key] = None
            continue
        reason = custom_meta(entry, "lamella-reason")
        state[key] = {
            "txn_hash": txn_hash,
            "paperless_id": pid,
            "reason": reason if isinstance(reason, str) and reason else None,
        }
    return [r for r in state.values() if r is not None]
