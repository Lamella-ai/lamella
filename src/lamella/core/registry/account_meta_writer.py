# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Writer for ``custom "account-meta"`` directives — the extended
field round-trip for the ``accounts_meta`` table.

Before this, ``accounts_meta.kind`` was already round-tripped via
kind_writer's ``custom "account-kind"`` directive. But the 185
rows carry additional user-labeled fields — display_name,
institution, last_four, entity_slug, simplefin_account_id, notes —
that were SQLite-only. A DB wipe or fresh install against an
existing ledger lost all of them.

Directive shape:

    2000-01-01 custom "account-meta" Assets:ZetaGen:Mercury:Checking
      lamella-display-name:    "Mercury Checking 1234"
      lamella-institution:     "Bank Three"
      lamella-last-four:       "1234"
      lamella-entity-slug:     "ZetaGen"
      lamella-simplefin-id:    "ACT_..."
      lamella-notes:           "main operating account"
      lamella-modified-at:     "2026-04-24T10:23:00-06:00"

Similar to entity_writer: one directive per account, each write
strips the prior block for the same account path so we stay
one-block-per-account. Reconstruct step reads every block and
upserts.
"""
from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    ensure_include_in_main,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)


def _q(s: str | None) -> str:
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _strip_existing_account_meta_blocks(text: str, account_path: str) -> str:
    """Remove any existing ``custom "account-meta" <path>`` block.

    Beancount 3.x renders the account argument UNQUOTED (it's a
    valid Account token). We handle both forms — quoted +
    unquoted — just in case.
    """
    escaped = re.escape(account_path)
    pattern = re.compile(
        r'(?:^|\n)(\d{4}-\d{2}-\d{2})\s+custom\s+"account-meta"\s+'
        r'(?:"' + escaped + r'"|' + escaped + r')'
        r'\s*\n(?:[ \t]+[^\n]*\n)*',
        re.MULTILINE,
    )
    return pattern.sub("\n", text)


def append_account_meta_directive(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    display_name: str | None = None,
    institution: str | None = None,
    last_four: str | None = None,
    entity_slug: str | None = None,
    simplefin_account_id: str | None = None,
    notes: str | None = None,
    run_check: bool = True,
) -> None:
    """Write (or rewrite) the ``custom "account-meta"`` block.

    Unspecified/None fields are omitted from the written directive —
    reconstruct's reader treats a missing key as "don't overwrite the
    existing DB value" via COALESCE on upsert. So setting one field
    doesn't wipe the others.
    """
    if not account_path:
        raise ValueError("account_path required")
    if not main_bean.exists():
        raise FileNotFoundError(f"main.bean not found at {main_bean}")

    backup_main = main_bean.read_bytes()
    config_existed = connector_config.exists()
    backup_config = connector_config.read_bytes() if config_existed else None

    baseline_output = ""
    if run_check:
        _, baseline_output = capture_bean_check(main_bean)

    if not config_existed:
        connector_config.parent.mkdir(parents=True, exist_ok=True)
        connector_config.write_text(
            "; connector_config.bean — configuration state written by Lamella.\n"
            "; Paperless field-role mappings and UI-persisted settings live here.\n"
            "; Do not hand-edit; use the /settings pages.\n\n",
            encoding="utf-8",
        )
    ensure_include_in_main(main_bean, connector_config)

    current = connector_config.read_text(encoding="utf-8")
    stripped = _strip_existing_account_meta_blocks(current, account_path)

    when = "2000-01-01"
    modified_at = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [
        f'{when} custom "account-meta" {account_path}',
    ]
    if display_name and display_name.strip():
        lines.append(f'  lamella-display-name: "{_q(display_name.strip())}"')
    if institution and institution.strip():
        lines.append(f'  lamella-institution: "{_q(institution.strip())}"')
    if last_four and last_four.strip():
        lines.append(f'  lamella-last-four: "{_q(last_four.strip())}"')
    if entity_slug and entity_slug.strip():
        lines.append(f'  lamella-entity-slug: "{_q(entity_slug.strip())}"')
    if simplefin_account_id and simplefin_account_id.strip():
        lines.append(f'  lamella-simplefin-id: "{_q(simplefin_account_id.strip())}"')
    if notes and notes.strip():
        lines.append(f'  lamella-notes: "{_q(notes.strip())}"')
    lines.append(f'  lamella-modified-at: "{modified_at}"')
    block = "\n" + "\n".join(lines) + "\n"

    new_text = stripped.rstrip() + "\n" + block
    try:
        connector_config.write_text(new_text, encoding="utf-8")
        if run_check:
            run_bean_check_vs_baseline(main_bean, baseline_output)
    except BeanCheckError:
        main_bean.write_bytes(backup_main)
        if backup_config is None:
            connector_config.unlink(missing_ok=True)
        else:
            connector_config.write_bytes(backup_config)
        raise


def append_account_meta_deleted(
    *,
    connector_config: Path,
    main_bean: Path,
    account_path: str,
    run_check: bool = True,
) -> None:
    """Append a ``custom "account-meta-deleted"`` tombstone directive.

    Phase 1.4. ``seed_accounts_meta`` walks every Open directive and
    re-INSERTs an accounts_meta row via INSERT OR IGNORE. If the user
    deletes a labeled accounts_meta row but the Open directive
    survives (the closed-account case is the normal flow — we set
    ``closed_on`` rather than DELETE — but admin paths and
    cleanup-stale-meta DO hard-delete), the next boot resurrects the
    row with all the user-set fields (kind, entity_slug, institution,
    last_four) reset to NULL.

    The tombstone prevents that. The reconstruct reader at
    ``read_account_meta_directives`` honors the tombstone too, so a
    rebuilt DB doesn't recreate user-deleted account_meta rows.

    Mirrors ``append_entity_deleted`` and ``append_vehicle_deleted``.
    The argument is the full account_path (unquoted Account token);
    no metadata beyond lamella-deleted-at — once tombstoned, the row is
    gone, no other state to preserve.
    """
    from lamella.core.transform.custom_directive import (
        Account,
        append_custom_directive,
    )
    from datetime import date as _date_t
    if not account_path:
        raise ValueError("account_path required")
    if not main_bean.exists():
        raise FileNotFoundError(f"main.bean not found at {main_bean}")
    today = _date_t.today()
    append_custom_directive(
        target=connector_config,
        main_bean=main_bean,
        header=(
            "; connector_config.bean — configuration state written by Lamella.\n"
            "; Paperless field-role mappings and UI-persisted settings live here.\n"
            "; Do not hand-edit; use the /settings pages.\n\n"
        ),
        directive_date=today,
        directive_type="account-meta-deleted",
        args=[Account(account_path)],
        meta={"lamella-deleted-at": datetime.now(UTC).isoformat(timespec="seconds")},
        run_check=run_check,
    )


def read_deleted_account_paths(entries) -> set[str]:
    """Return account paths whose latest directive is an
    ``account-meta-deleted`` tombstone. Used by both the reconstruct
    reader (drop tombstoned rows from the rebuilt DB) and
    ``seed_accounts_meta`` (filter tombstoned paths out of the boot-
    time auto-seed)."""
    from beancount.core.data import Custom
    deleted: set[str] = set()
    for e in entries:
        if not isinstance(e, Custom):
            continue
        if e.type != "account-meta-deleted":
            continue
        if not e.values:
            continue
        raw = e.values[0]
        if hasattr(raw, "value"):
            raw = raw.value
        path = str(raw).strip()
        if path:
            deleted.add(path)
    return deleted


def read_account_meta_directives(entries) -> list[dict]:
    """Parse every ``custom "account-meta"`` directive, return dicts
    ready for SQL upsert. Tolerates both the Account (unquoted) and
    string-literal (quoted) argument shapes.

    Phase 1.4: drops paths carrying an ``account-meta-deleted``
    tombstone — same one-way contract as the entity / vehicle /
    property tombstone readers.
    """
    from beancount.core.data import Custom
    deleted = read_deleted_account_paths(entries)
    out: list[dict] = []
    for e in entries:
        if not isinstance(e, Custom):
            continue
        if e.type != "account-meta":
            continue
        if not e.values:
            continue
        raw = e.values[0]
        if hasattr(raw, "value"):
            raw = raw.value
        account_path = str(raw).strip()
        if not account_path:
            continue
        if account_path in deleted:
            continue
        meta = e.meta or {}
        row = {
            "account_path": account_path,
            "display_name": _meta_get(meta, "lamella-display-name"),
            "institution": _meta_get(meta, "lamella-institution"),
            "last_four": _meta_get(meta, "lamella-last-four"),
            "entity_slug": _meta_get(meta, "lamella-entity-slug"),
            "simplefin_account_id": _meta_get(meta, "lamella-simplefin-id"),
            "notes": _meta_get(meta, "lamella-notes"),
        }
        out.append(row)
    return out


def _meta_get(meta: dict, key: str) -> str | None:
    if key not in meta:
        return None
    v = meta[key]
    if hasattr(v, "value"):
        v = v.value
    if v is None:
        return None
    s = str(v).strip()
    return s or None
