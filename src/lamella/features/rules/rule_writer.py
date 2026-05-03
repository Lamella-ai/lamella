# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "classification-rule"`` directives and their
matching revokes.

State that lives in the ledger:
  * The rule itself: pattern + target + created-by + added-at.
  * Revoke directives for deleted rules (append-only).

State that does NOT go in the ledger (these are cache):
  * hit_count, last_used, confidence mutations from bumps / demotions /
    promotions. These are derivable by scanning ledger transactions
    against the rule; rewriting a directive every time a rule fires
    would flood the file.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from beancount.core.data import Custom

from lamella.core.transform.custom_directive import (
    Account,
    append_custom_directive,
    custom_arg,
    custom_meta,
)

log = logging.getLogger(__name__)


CONNECTOR_RULES_HEADER = (
    "; connector_rules.bean — classification rules written by Lamella.\n"
    "; One custom \"classification-rule\" directive per rule. Do not hand-edit;\n"
    "; use the /rules UI — every write runs bean-check and reverts on error.\n"
)


def append_rule(
    *,
    connector_rules: Path,
    main_bean: Path,
    pattern_type: str,
    pattern_value: str,
    target_account: str,
    card_account: str | None = None,
    created_by: str = "user",
    added_at: datetime | None = None,
    backfilled: bool = False,
    run_check: bool = True,
) -> str:
    """Append a ``custom "classification-rule"`` directive. Idempotent
    from the reconstruct point of view — duplicates are filtered at
    read time by (pattern_type, pattern_value, card_account,
    target_account).

    ``added_at`` should be the original user-decision timestamp when
    available (e.g. from a SQLite ``created_at`` column during a
    one-shot migration). When it's omitted we stamp *now* and set
    ``lamella-backfilled: TRUE`` so future-you can distinguish rules that
    were stamped at teach time from those retroactively migrated.
    """
    if added_at is None:
        ts = datetime.now(timezone.utc).replace(tzinfo=None)
        is_backfill = backfilled
    else:
        ts = added_at
        is_backfill = backfilled
    meta: dict = {
        "lamella-target-account": Account(target_account),
        "lamella-pattern-type": pattern_type,
        "lamella-added-at": ts,
        "lamella-created-by": created_by,
    }
    if card_account:
        meta["lamella-card-account"] = Account(card_account)
    if is_backfill:
        meta["lamella-backfilled"] = True
    return append_custom_directive(
        target=connector_rules,
        main_bean=main_bean,
        header=CONNECTOR_RULES_HEADER,
        directive_date=ts.date(),
        directive_type="classification-rule",
        args=[pattern_value],
        meta=meta,
        run_check=run_check,
    )


def append_rule_revoke(
    *,
    connector_rules: Path,
    main_bean: Path,
    pattern_type: str,
    pattern_value: str,
    target_account: str,
    card_account: str | None = None,
    revoked_at: datetime | None = None,
    run_check: bool = True,
) -> str:
    """Mark a rule as revoked. Append-only; we never rewrite the
    original directive. Reconstruct filters out rules whose latest
    directive is a revoke."""
    ts = revoked_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta: dict = {
        "lamella-target-account": Account(target_account),
        "lamella-pattern-type": pattern_type,
        "lamella-revoked-at": ts,
    }
    if card_account:
        meta["lamella-card-account"] = Account(card_account)
    return append_custom_directive(
        target=connector_rules,
        main_bean=main_bean,
        header=CONNECTOR_RULES_HEADER,
        directive_date=ts.date(),
        directive_type="classification-rule-revoked",
        args=[pattern_value],
        meta=meta,
        run_check=run_check,
    )


def _account_to_str(value) -> str | None:
    """Beancount parses bare accounts as colon-separated lists; strings
    come back as str. Normalize both."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return ":".join(str(v) for v in value)
    return str(value)


def read_rules_from_entries(entries) -> list[dict]:
    """Return the active rules defined on the ledger, filtering any
    that have a later matching revoke. Active-rule identity is the
    tuple (pattern_type, pattern_value, card_account, target_account).
    Returns rows ready for upsert into ``classification_rules``.
    """
    # Walk in load order; the last directive for a given identity wins.
    state: dict[tuple, dict | None] = {}
    for entry in entries:
        if not isinstance(entry, Custom):
            continue
        if entry.type not in ("classification-rule", "classification-rule-revoked"):
            continue
        pattern_value = custom_arg(entry, 0)
        if not isinstance(pattern_value, str) or not pattern_value:
            continue
        pattern_type = custom_meta(entry, "lamella-pattern-type")
        if not isinstance(pattern_type, str) or not pattern_type:
            continue
        target = _account_to_str(custom_meta(entry, "lamella-target-account"))
        card = _account_to_str(custom_meta(entry, "lamella-card-account"))
        key = (pattern_type, pattern_value, card, target)
        if entry.type == "classification-rule-revoked":
            state[key] = None
            continue
        created_by = custom_meta(entry, "lamella-created-by") or "user"
        added_at = custom_meta(entry, "lamella-added-at")
        if isinstance(added_at, datetime):
            added_iso = added_at.isoformat(sep=" ", timespec="seconds")
        elif isinstance(added_at, date):
            added_iso = datetime.combine(
                added_at, datetime.min.time()
            ).isoformat(sep=" ", timespec="seconds")
        elif isinstance(added_at, str):
            added_iso = added_at
        else:
            added_iso = datetime.combine(
                entry.date, datetime.min.time()
            ).isoformat(sep=" ", timespec="seconds")
        state[key] = {
            "pattern_type": pattern_type,
            "pattern_value": pattern_value,
            "card_account": card,
            "target_account": target,
            "created_by": str(created_by),
            "added_at": added_iso,
        }
    return [row for row in state.values() if row is not None]
