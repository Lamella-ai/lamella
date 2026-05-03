# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0046 Phase 2 — synthetic counterpart replacement.

Phase 1 (commit a7349e0) emits the four ``lamella-synthetic-*`` meta
keys on the destination posting when a user classifies a transfer-
suspect single-leg row to an Assets:/Liabilities: target. Phase 2 is
the matcher half: when an incoming bank-feed row matches an existing
synthetic leg's (account, signed amount, date within window), the
matcher replaces the synthetic leg in place with real source meta
instead of staging a duplicate.

The original transaction's ``lamella-txn-id`` stays stable across the
swap. The four synthetic-* keys come off; ``lamella-source-N`` /
``lamella-source-reference-id-N`` paired keys go on. The bank feed
sees the row as "already imported" and dedup is preserved.

Wired by ``ingest._load_account``: before staging a new SimpleFIN row,
call ``find_replaceable_synthetic_match`` against the loaded ledger.
On match, call ``replace_synthetic_in_place`` and skip the stage step.
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from beancount.core.data import Transaction


log = logging.getLogger(__name__)


_TXN_HEADER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\s+[*!]")
_LAMELLA_TXN_ID_RE = re.compile(r'^\s+lamella-txn-id:\s*"([^"]+)"')
_POSTING_LINE_RE = re.compile(r"^\s{2,}([A-Z][A-Za-z0-9:_\-]*)\s+")
_SYNTHETIC_META_RE = re.compile(r"^\s+lamella-synthetic[A-Za-z\-]*\s*:")


def find_replaceable_synthetic_match(
    entries: Iterable,
    *,
    account: str,
    amount: Decimal,
    posted_date: date,
    window_days: int = 5,
) -> dict | None:
    """Walk ``entries`` looking for a synthetic counterpart leg whose
    posting matches ``(account, amount, posted_date)``.

    Returns a dict ``{lamella_txn_id, posting_account, posting_amount}``
    on first match, or ``None`` if no replaceable synthetic leg exists
    for this row.

    Match criteria (ADR-0046 Phase 2, same-account branch):
      * Posting account equals ``account``
      * Posting amount equals ``amount`` (signed; the synthetic leg
        was emitted with the same sign the bank feed will report)
      * Transaction date within ``window_days`` of ``posted_date``
      * Posting carries ``lamella-synthetic-replaceable: TRUE``
        (or the literal string ``"TRUE"``; Beancount parses both)

    A ``False`` value on the replaceable flag means the user manually
    confirmed the synthetic leg as the truth — those are not auto-
    replaced even if a real row eventually arrives.
    """
    target_amount = Decimal(amount)
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        # Date window check is cheap; do it first.
        try:
            delta = abs((entry.date - posted_date).days)
        except Exception:  # noqa: BLE001
            continue
        if delta > window_days:
            continue
        for posting in entry.postings:
            if posting.account != account:
                continue
            if posting.units is None or posting.units.number is None:
                continue
            if Decimal(posting.units.number) != target_amount:
                continue
            meta = getattr(posting, "meta", None) or {}
            replaceable = meta.get("lamella-synthetic-replaceable")
            # Beancount renders TRUE/FALSE bare; loader returns Python
            # bool. Older fixtures or hand-edits may surface as the
            # string "TRUE" — accept both.
            if replaceable is True or (
                isinstance(replaceable, str)
                and replaceable.strip().upper() == "TRUE"
            ):
                txn_meta = getattr(entry, "meta", None) or {}
                txn_id = txn_meta.get("lamella-txn-id")
                if not txn_id:
                    continue
                return {
                    "lamella_txn_id": str(txn_id),
                    "posting_account": posting.account,
                    "posting_amount": Decimal(posting.units.number),
                }
    return None


def find_loose_synthetic_match(
    entries: Iterable,
    *,
    amount: Decimal,
    posted_date: date,
    exclude_account: str,
    window_days: int = 5,
) -> dict | None:
    """ADR-0046 Phase 3 — looser matcher for the wrong-account case.

    Same window + amount criteria as the strict matcher, but matches
    synthetic legs whose posting is on a DIFFERENT account from
    ``exclude_account``. Returns the first hit (or ``None``) so /review
    can surface "We thought this was a transfer to X — actually Y?"
    when the user picked the wrong destination on Phase 1.

    Returned dict carries ``lamella_txn_id``, ``synthetic_account``
    (the wrong guess), and ``synthetic_amount``. The /review surface
    uses these to render the prompt; a confirm POST then runs an
    account-rewrite + a synthetic→real meta swap.
    """
    target_amount = Decimal(amount)
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        try:
            delta = abs((entry.date - posted_date).days)
        except Exception:  # noqa: BLE001
            continue
        if delta > window_days:
            continue
        for posting in entry.postings:
            if posting.account == exclude_account:
                continue
            if posting.units is None or posting.units.number is None:
                continue
            if Decimal(posting.units.number) != target_amount:
                continue
            meta = getattr(posting, "meta", None) or {}
            replaceable = meta.get("lamella-synthetic-replaceable")
            if replaceable is True or (
                isinstance(replaceable, str)
                and replaceable.strip().upper() == "TRUE"
            ):
                txn_meta = getattr(entry, "meta", None) or {}
                txn_id = txn_meta.get("lamella-txn-id")
                if not txn_id:
                    continue
                return {
                    "lamella_txn_id": str(txn_id),
                    "synthetic_account": posting.account,
                    "synthetic_amount": Decimal(posting.units.number),
                }
    return None


def _next_source_index(posting_meta_lines: list[str]) -> int:
    """Scan a posting's existing meta lines and return the next free
    ``lamella-source-N`` index. Synthetic legs have no source meta yet,
    so this typically returns 0 — but we don't assume that."""
    used: set[int] = set()
    pat = re.compile(r"^\s+lamella-source-(\d+)\s*:")
    for line in posting_meta_lines:
        m = pat.match(line)
        if m:
            used.add(int(m.group(1)))
    n = 0
    while n in used:
        n += 1
    return n


def replace_synthetic_in_place(
    *,
    bean_file: Path,
    lamella_txn_id: str,
    posting_account: str,
    source: str,
    source_reference_id: str,
) -> bool:
    """Rewrite ``bean_file`` in place: find the transaction block whose
    txn-meta ``lamella-txn-id`` equals ``lamella_txn_id``, locate the
    posting line for ``posting_account``, strip every
    ``lamella-synthetic-*`` meta line under it, and add paired
    ``lamella-source-N`` / ``lamella-source-reference-id-N`` lines at
    the next free index.

    Returns ``True`` when the file was modified, ``False`` if the
    transaction block (or the synthetic posting under it) couldn't be
    found.

    Mirrors the block-scan pattern used by ``_stamp_alias_on_ledger``;
    no Beancount round-trip — raw text only — so ordering, comments,
    and unrelated meta survive intact.
    """
    if not bean_file.exists():
        return False
    text = bean_file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        # Collect the whole transaction block (header + meta + postings
        # until the next header or EOF).
        block_start = i
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        # Does this block carry the right lamella-txn-id?
        block_txn_id: str | None = None
        for bl in block_lines:
            m = _LAMELLA_TXN_ID_RE.match(bl)
            if m:
                block_txn_id = m.group(1)
                break
        if block_txn_id != lamella_txn_id:
            out_lines.extend(block_lines)
            continue
        # Found the block. Walk its lines, locate the synthetic posting,
        # rewrite it in place.
        new_block = _rewrite_synthetic_posting(
            block_lines,
            posting_account=posting_account,
            source=source,
            source_reference_id=source_reference_id,
        )
        if new_block != block_lines:
            modified = True
        out_lines.extend(new_block)
    if modified:
        bean_file.write_text("".join(out_lines), encoding="utf-8")
    return modified


def _rewrite_synthetic_posting(
    block_lines: list[str],
    *,
    posting_account: str,
    source: str,
    source_reference_id: str,
) -> list[str]:
    """Within one transaction's lines, find the posting for
    ``posting_account`` and swap its synthetic-* meta for real source
    meta. Returns a new list; if the posting can't be located, returns
    the original list unchanged."""
    # Locate the posting line by account match. Account is the first
    # capital-letter run after at least 2 spaces.
    target_idx: int | None = None
    for idx, bl in enumerate(block_lines):
        m = _POSTING_LINE_RE.match(bl)
        if m and m.group(1) == posting_account:
            target_idx = idx
            break
    if target_idx is None:
        return block_lines
    # Collect the meta lines that belong to this posting (subsequent
    # indented lines that aren't a fresh posting start).
    meta_start = target_idx + 1
    meta_end = meta_start
    while meta_end < len(block_lines):
        bl = block_lines[meta_end]
        if _POSTING_LINE_RE.match(bl):
            break
        # Only continuation lines matter (indented, blank lines too end
        # the transaction).
        if bl.strip() == "":
            break
        meta_end += 1
    posting_meta = block_lines[meta_start:meta_end]
    # Filter out every synthetic-* meta line. Keep everything else.
    cleaned_meta = [
        bl for bl in posting_meta
        if not _SYNTHETIC_META_RE.match(bl)
    ]
    if cleaned_meta == posting_meta:
        # No synthetic meta on this posting → nothing to replace.
        return block_lines
    # Pick the next free source index relative to surviving meta.
    n = _next_source_index(cleaned_meta)
    # Use the same indent the synthetic meta lines used — typically
    # four spaces. Read it off the first stripped meta line if present;
    # otherwise default to four spaces.
    indent = "    "
    for bl in posting_meta:
        if bl.startswith("    "):
            indent = bl[: len(bl) - len(bl.lstrip(" "))]
            break
    new_meta_lines = list(cleaned_meta)
    src_line = f'{indent}lamella-source-{n}: "{_q(source)}"\n'
    ref_line = (
        f'{indent}lamella-source-reference-id-{n}: '
        f'"{_q(source_reference_id)}"\n'
    )
    new_meta_lines.extend([src_line, ref_line])
    new_block = (
        block_lines[: meta_start]
        + new_meta_lines
        + block_lines[meta_end:]
    )
    return new_block


def rewrite_synthetic_account_in_place(
    *,
    bean_file: Path,
    lamella_txn_id: str,
    wrong_account: str,
    right_account: str,
    source: str,
    source_reference_id: str,
) -> bool:
    """ADR-0046 Phase 3b — wrong-account confirm.

    Locate the synthetic posting on ``wrong_account`` inside the txn
    block whose ``lamella-txn-id`` matches, change the account from
    ``wrong_account`` to ``right_account``, AND swap the synthetic-*
    meta for paired ``lamella-source-N`` / ``lamella-source-reference-
    id-N`` lines in one pass.

    Returns ``True`` on a successful rewrite, ``False`` when the txn
    block or matching synthetic posting could not be found. Idempotent
    in the sense that a re-run after the rewrite has happened won't
    find the wrong-account posting (because it was renamed) and will
    return ``False``.

    The caller is responsible for ``bean-check``-validating the
    rewrite afterwards. On bean-check failure the caller should
    restore from snapshot — typical pattern is the connector's
    snapshot/restore wrapper that already exists for the strict
    matcher path.
    """
    if not bean_file.exists():
        return False
    text = bean_file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        block_txn_id: str | None = None
        for bl in block_lines:
            m = _LAMELLA_TXN_ID_RE.match(bl)
            if m:
                block_txn_id = m.group(1)
                break
        if block_txn_id != lamella_txn_id:
            out_lines.extend(block_lines)
            continue
        new_block = _rewrite_synthetic_account_block(
            block_lines,
            wrong_account=wrong_account,
            right_account=right_account,
            source=source,
            source_reference_id=source_reference_id,
        )
        if new_block != block_lines:
            modified = True
        out_lines.extend(new_block)
    if modified:
        bean_file.write_text("".join(out_lines), encoding="utf-8")
    return modified


def _rewrite_synthetic_account_block(
    block_lines: list[str],
    *,
    wrong_account: str,
    right_account: str,
    source: str,
    source_reference_id: str,
) -> list[str]:
    """Within one txn block, rename the posting on ``wrong_account`` to
    ``right_account`` and swap its synthetic-* meta for real source
    meta. Returns a new list; if the wrong-account posting can't be
    located, returns the original list unchanged."""
    target_idx: int | None = None
    for idx, bl in enumerate(block_lines):
        m = _POSTING_LINE_RE.match(bl)
        if m and m.group(1) == wrong_account:
            target_idx = idx
            break
    if target_idx is None:
        return block_lines
    # Rewrite the posting line: replace the first occurrence of the
    # wrong account with the right account. Use str.replace with
    # count=1 to preserve any subsequent text exactly (amount, currency,
    # comment etc.). The leading indent is preserved because we only
    # touch the account run.
    posting_line = block_lines[target_idx]
    new_posting_line = posting_line.replace(wrong_account, right_account, 1)
    if new_posting_line == posting_line:
        return block_lines
    # Now do the meta swap, identical to the strict path's behavior
    # except keyed on the NEW account (since the line was just
    # renamed, the meta block underneath stays adjacent to the
    # renamed posting).
    rewritten = list(block_lines)
    rewritten[target_idx] = new_posting_line
    # Walk the meta range underneath this posting.
    meta_start = target_idx + 1
    meta_end = meta_start
    while meta_end < len(rewritten):
        bl = rewritten[meta_end]
        if _POSTING_LINE_RE.match(bl):
            break
        if bl.strip() == "":
            break
        meta_end += 1
    posting_meta = rewritten[meta_start:meta_end]
    cleaned_meta = [
        bl for bl in posting_meta
        if not _SYNTHETIC_META_RE.match(bl)
    ]
    n = _next_source_index(cleaned_meta)
    indent = "    "
    for bl in posting_meta:
        if bl.startswith("    "):
            indent = bl[: len(bl) - len(bl.lstrip(" "))]
            break
    new_meta_lines = list(cleaned_meta)
    src_line = f'{indent}lamella-source-{n}: "{_q(source)}"\n'
    ref_line = (
        f'{indent}lamella-source-reference-id-{n}: '
        f'"{_q(source_reference_id)}"\n'
    )
    new_meta_lines.extend([src_line, ref_line])
    new_block = (
        rewritten[: meta_start]
        + new_meta_lines
        + rewritten[meta_end:]
    )
    return new_block


def promote_synthetic_to_confirmed(
    *,
    bean_file: Path,
    lamella_txn_id: str,
    posting_account: str,
) -> bool:
    """ADR-0046 Phase 4 — flip a synthetic leg's replaceable flag from
    TRUE to FALSE and bump confidence to ``"confirmed"``.

    Used by the /audit "Promote to confirmed" button when a user has
    manually verified the synthetic leg matches their real-world
    record (e.g. they reconciled a PayPal statement and confirmed the
    counterpart they authored is correct). After promotion the matcher
    no longer auto-replaces this leg even if a real bank-feed row
    eventually arrives — the user has taken ownership.

    Returns ``True`` when the file was modified, ``False`` when the
    transaction or posting couldn't be located. Idempotent: running
    twice on an already-promoted leg returns ``False`` and leaves
    the file unchanged."""
    if not bean_file.exists():
        return False
    text = bean_file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        block_txn_id: str | None = None
        for bl in block_lines:
            m = _LAMELLA_TXN_ID_RE.match(bl)
            if m:
                block_txn_id = m.group(1)
                break
        if block_txn_id != lamella_txn_id:
            out_lines.extend(block_lines)
            continue
        new_block = _flip_replaceable_in_posting(
            block_lines, posting_account=posting_account,
        )
        if new_block != block_lines:
            modified = True
        out_lines.extend(new_block)
    if modified:
        bean_file.write_text("".join(out_lines), encoding="utf-8")
    return modified


def demote_synthetic_to_replaceable(
    *,
    bean_file: Path,
    lamella_txn_id: str,
    posting_account: str,
) -> bool:
    """ADR-0046 Phase 4b — inverse of :func:`promote_synthetic_to_confirmed`.

    Flips ``lamella-synthetic-replaceable`` from FALSE back to TRUE and
    drops confidence back to ``"guessed"`` so the strict matcher will
    auto-replace the leg again when a real bank-feed row arrives.

    Used by the /audit Demote button when the user realizes they
    promoted prematurely. Idempotent: a no-op when the leg is already
    replaceable."""
    if not bean_file.exists():
        return False
    text = bean_file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        block_txn_id: str | None = None
        for bl in block_lines:
            m = _LAMELLA_TXN_ID_RE.match(bl)
            if m:
                block_txn_id = m.group(1)
                break
        if block_txn_id != lamella_txn_id:
            out_lines.extend(block_lines)
            continue
        new_block = _flip_replaceable_in_posting(
            block_lines, posting_account=posting_account, target_value="TRUE",
            target_confidence="guessed",
        )
        if new_block != block_lines:
            modified = True
        out_lines.extend(new_block)
    if modified:
        bean_file.write_text("".join(out_lines), encoding="utf-8")
    return modified


def _flip_replaceable_in_posting(
    block_lines: list[str], *, posting_account: str,
    target_value: str = "FALSE",
    target_confidence: str = "confirmed",
) -> list[str]:
    """Within one txn block, find the posting for ``posting_account``
    and rewrite its synthetic-replaceable line to ``target_value`` +
    confidence line to ``target_confidence``. No-op if synthetic-
    replaceable already matches the target value.

    Defaults are the promote direction (TRUE→FALSE, guessed→confirmed);
    pass ``target_value="TRUE"`` + ``target_confidence="guessed"`` for
    the demote direction."""
    target_idx: int | None = None
    for idx, bl in enumerate(block_lines):
        m = _POSTING_LINE_RE.match(bl)
        if m and m.group(1) == posting_account:
            target_idx = idx
            break
    if target_idx is None:
        return block_lines
    meta_start = target_idx + 1
    meta_end = meta_start
    while meta_end < len(block_lines):
        bl = block_lines[meta_end]
        if _POSTING_LINE_RE.match(bl):
            break
        if bl.strip() == "":
            break
        meta_end += 1
    meta = block_lines[meta_start:meta_end]
    new_meta: list[str] = []
    changed = False
    repl_re = re.compile(r"^(\s+lamella-synthetic-replaceable:\s*)(TRUE|FALSE)\b")
    conf_re = re.compile(r"^(\s+lamella-synthetic-confidence:\s*)\"[^\"]*\"")
    for bl in meta:
        m_repl = repl_re.match(bl)
        if m_repl:
            if m_repl.group(2) == target_value:
                new_meta.append(bl)
            else:
                new_meta.append(f"{m_repl.group(1)}{target_value}\n")
                changed = True
            continue
        m_conf = conf_re.match(bl)
        if m_conf:
            new_meta.append(f'{m_conf.group(1)}"{target_confidence}"\n')
            # Confidence change alone counts as modified only if the
            # replaceable flag was also TRUE; idempotency is governed
            # by the replaceable line, not confidence.
            continue
        new_meta.append(bl)
    if not changed:
        return block_lines
    return (
        block_lines[:meta_start] + new_meta + block_lines[meta_end:]
    )


def _q(value: str) -> str:
    """Mirror writer._q: escape backslash + double-quote for Beancount
    string literals."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def append_source_paired_meta_in_place(
    *,
    bean_file: Path,
    lamella_txn_id: str,
    posting_account: str,
    source: str,
    source_reference_id: str,
    source_description: str | None = None,
) -> bool:
    """ADR-0058 confirm-as-dup writer + ADR-0019 paired source meta.

    Locate the transaction block whose ``lamella-txn-id`` equals
    ``lamella_txn_id``, find the posting on ``posting_account``, and
    append a fresh ``lamella-source-N`` / ``lamella-source-reference-
    id-N`` (and optional ``lamella-source-description-N``) triplet at
    the next free index N. Existing source pairs are left untouched —
    this is APPEND, not replace.

    Used by ``/review/duplicates/{id}/confirm`` when the matched ledger
    entry exists on disk: the new source becomes another observation
    on that bank-side leg, exactly the shape ADR-0019 + ADR-0059
    designed for. Same algorithm a fresh-source promotion would use
    if it landed on an entry that already had a source pair.

    Returns True on a successful write, False when the txn block can't
    be found by ``lamella-txn-id`` or no matching posting line exists.
    Idempotent in the sense that re-running with the same
    ``source`` + ``source_reference_id`` would APPEND again — callers
    that want at-most-once semantics should check the existing meta
    first via ``find_source_reference``.
    """
    if not bean_file.exists():
        return False
    text = bean_file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        # Collect the whole transaction block (header + meta +
        # postings until the next header or EOF).
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        # Match by lamella-txn-id.
        block_txn_id: str | None = None
        for bl in block_lines:
            m = _LAMELLA_TXN_ID_RE.match(bl)
            if m:
                block_txn_id = m.group(1)
                break
        if block_txn_id != lamella_txn_id:
            out_lines.extend(block_lines)
            continue
        # Found the block; append paired meta to the right posting.
        new_block = _append_paired_meta_to_posting(
            block_lines,
            posting_account=posting_account,
            source=source,
            source_reference_id=source_reference_id,
            source_description=source_description,
        )
        if new_block != block_lines:
            modified = True
        out_lines.extend(new_block)
    if modified:
        bean_file.write_text("".join(out_lines), encoding="utf-8")
    return modified


def _append_paired_meta_to_posting(
    block_lines: list[str],
    *,
    posting_account: str,
    source: str,
    source_reference_id: str,
    source_description: str | None,
) -> list[str]:
    """Inside one transaction's lines, find the posting on
    ``posting_account`` and append a fresh paired-source triplet at
    the next free index, preserving the indent of the existing meta.

    Returns a new list; the original is unchanged when the posting
    line can't be located (defensive — prefer no-op over partial
    write)."""
    target_idx: int | None = None
    for idx, bl in enumerate(block_lines):
        m = _POSTING_LINE_RE.match(bl)
        if m and m.group(1) == posting_account:
            target_idx = idx
            break
    if target_idx is None:
        return block_lines
    # Collect the meta lines belonging to this posting (subsequent
    # indented lines that aren't a fresh posting start or blank line).
    meta_start = target_idx + 1
    meta_end = meta_start
    while meta_end < len(block_lines):
        bl = block_lines[meta_end]
        if _POSTING_LINE_RE.match(bl):
            break
        if bl.strip() == "":
            break
        meta_end += 1
    posting_meta = block_lines[meta_start:meta_end]
    # Pick the next free source index relative to existing meta.
    n = _next_source_index(posting_meta)
    indent = "    "
    for bl in posting_meta:
        if bl.startswith("    "):
            indent = bl[: len(bl) - len(bl.lstrip(" "))]
            break
    appended = [
        f'{indent}lamella-source-{n}: "{_q(source)}"\n',
        (
            f'{indent}lamella-source-reference-id-{n}: '
            f'"{_q(source_reference_id)}"\n'
        ),
    ]
    if source_description:
        appended.append(
            f'{indent}lamella-source-description-{n}: '
            f'"{_q(source_description)}"\n'
        )
    new_block = (
        block_lines[:meta_end]
        + appended
        + block_lines[meta_end:]
    )
    return new_block


_HEADER_NARRATION_RE = re.compile(
    r'^(?P<prefix>\d{4}-\d{2}-\d{2}\s+[*!]\s*'
    r'(?:"[^"]*"\s+)?)'   # optional payee
    r'"(?P<narration>(?:[^"\\]|\\.)*)"'
    r'(?P<rest>.*)$',
)


def rewrite_narration_in_place(
    *,
    bean_file: Path,
    lamella_txn_id: str,
    new_narration: str,
    mark_synthesized: bool = True,
) -> bool:
    """ADR-0059 — rewrite the canonical txn-level narration on the
    transaction whose ``lamella-txn-id`` matches.

    Used by the confirm-as-dup writer (after a new
    ``lamella-source-N`` paired meta lands on a posting, the
    narration may want re-synthesizing to combine the new source's
    text) and by the promote path (when no manual narration is
    set, pick a synthesized line).

    When ``mark_synthesized=True`` (the default), also adds or
    refreshes the ``lamella-narration-synthesized: TRUE`` marker at
    txn-meta level. Adds it once; doesn't duplicate if it's already
    present. The marker tells future synthesis passes "this
    narration is mine to rewrite"; if the user edits the narration
    by hand later, they should drop the marker (or set it to
    FALSE) to opt out of further synthesis.

    Returns ``True`` if the file was modified, ``False`` when the
    txn-id wasn't found or the narration is already equal.

    Refuses to write when ``new_narration`` contains a literal
    newline — Beancount narrations are single-line."""
    if "\n" in new_narration or "\r" in new_narration:
        return False
    if not bean_file.exists():
        return False
    text = bean_file.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    out_lines: list[str] = []
    modified = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _TXN_HEADER_RE.match(line):
            out_lines.append(line)
            i += 1
            continue
        # Collect block.
        block_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if _TXN_HEADER_RE.match(nxt):
                break
            block_lines.append(nxt)
            i += 1
        # Match by lamella-txn-id.
        block_txn_id: str | None = None
        for bl in block_lines:
            m = _LAMELLA_TXN_ID_RE.match(bl)
            if m:
                block_txn_id = m.group(1)
                break
        if block_txn_id != lamella_txn_id:
            out_lines.extend(block_lines)
            continue
        # Rewrite the header narration.
        header = block_lines[0]
        header_match = _HEADER_NARRATION_RE.match(header.rstrip("\n"))
        if header_match is None:
            out_lines.extend(block_lines)
            continue
        existing = header_match.group("narration")
        if existing == _q(new_narration) and not mark_synthesized:
            out_lines.extend(block_lines)
            continue
        new_header = (
            f'{header_match.group("prefix")}'
            f'"{_q(new_narration)}"'
            f'{header_match.group("rest")}'
        )
        # Preserve trailing newline.
        if header.endswith("\n"):
            new_header += "\n"
        block_lines[0] = new_header
        # Marker. Idempotent: skip if already present in any form.
        if mark_synthesized:
            has_marker = any(
                bl.lstrip().startswith(
                    "lamella-narration-synthesized:"
                )
                for bl in block_lines
            )
            if not has_marker:
                # Insert after txn-id meta (which is conventionally
                # the first txn-meta line). Find the first
                # txn-meta line that's NOT the header to anchor.
                insert_at = 1
                for idx in range(1, len(block_lines)):
                    bl = block_lines[idx]
                    if _POSTING_LINE_RE.match(bl):
                        insert_at = idx
                        break
                block_lines.insert(
                    insert_at,
                    "  lamella-narration-synthesized: TRUE\n",
                )
        modified = True
        out_lines.extend(block_lines)
    if modified:
        bean_file.write_text("".join(out_lines), encoding="utf-8")
    return modified
