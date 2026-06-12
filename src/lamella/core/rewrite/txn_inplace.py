# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Rewrite a single FIXME posting to a real account, in place.

The existing override-based categorize (``OverrideWriter``) layers a
correction on top of the raw txn. That's safe but leaves the raw
``Expenses:FIXME`` in the source file, which confuses the UI and
accumulates audit-trail blocks a user almost never wants to read.

This module does the surgery the user actually expects: find the
FIXME posting in its source file and rewrite the account path to
the chosen target. Runs under the same discipline as every other
connector write:

  * Backup the file to ``.pre-inplace-<ISO-timestamp>/`` before
    any write.
  * Line-level text edit preserves whitespace, comments, and
    posting meta exactly.
  * Amount sanity-check: the line we're about to rewrite must be a
    posting on the given account with the expected amount.
    Prevents accidental overwrites of a different posting if the
    txn's line number has shifted since load.
  * bean-check vs. baseline after the write. A new error triggers
    a rollback from the backup. The snapshot restore is
    byte-identical.
  * Refuses paths outside ``ledger_dir`` or under archive/reboot
    directories.

Scope: the caller supplies the absolute file path + line number
(from the beancount parser's posting meta), the expected account
and amount, and the new target. The function returns the pre/post
text diff, or raises on validation failure.
"""
from __future__ import annotations

import logging
import re
import shutil
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Tuple

from lamella.core.ledger_writer import (
    BeanCheckError,
    capture_bean_check,
    run_bean_check_vs_baseline,
)
from lamella.core.transform.normalize_txn_identity import (
    normalize_one_transaction_in_lines,
)

log = logging.getLogger(__name__)


def _opportunistic_normalize(
    lines: list[str], txn_start_line: int,
) -> list[str]:
    """Best-effort on-touch identity normalization. Any time we rewrite
    a transaction's posting block we also clean up its identity meta
    (mint ``lamella-txn-id`` if missing, migrate legacy txn-level
    source keys to posting-level paired source meta). Failures here
    are logged but never block the actual rewrite — the post-write
    ``bean-check`` is the load-bearing guard.

    This is what makes the schema migration self-healing: legacy
    entries converge to the new shape as the user categorizes /
    edits them, no bulk run required.
    """
    try:
        new_lines, _changed = normalize_one_transaction_in_lines(
            lines, txn_start_line - 1,
        )
        return new_lines
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "on-touch identity normalization at line %d failed; "
            "leaving identity meta as-is. detail: %s",
            txn_start_line, exc,
        )
        return lines


class InPlaceRewriteError(RuntimeError):
    """Raised when the in-place rewrite can't proceed safely.

    Every path: either leaves the file byte-identical to its pre-call
    state, or raises this. Never a partial write."""


# Matches a posting line like:
#   "  Expenses:FIXME   42.17 USD"
# or "  Expenses:FIXME  -42.17 USD"
# or "  Expenses:FIXME   42.17 USD  ; comment"
# Groups:
#   1: leading whitespace
#   2: account path (what we replace)
#   3: remainder (spaces + amount + currency + optional comment)
_POSTING_LINE_RE = re.compile(
    r"^(\s+)([A-Z][A-Za-z0-9:_\-]+)(\s+.*)$"
)


def _is_safe_path(path: Path, ledger_dir: Path) -> bool:
    """Reject paths that must not be touched: outside ledger_dir,
    under an archive / reboot / backup directory, or a symlink."""
    try:
        resolved = path.resolve()
        ledger_resolved = ledger_dir.resolve()
    except OSError:
        return False
    try:
        resolved.relative_to(ledger_resolved)
    except ValueError:
        return False
    if path.is_symlink():
        return False
    forbidden_dirs = ("_archive", ".reboot", ".pre-inplace", ".pre-reboot")
    for part in resolved.parts:
        for bad in forbidden_dirs:
            if part == bad or part.startswith(bad):
                return False
    return True


def _backup(path: Path, ledger_dir: Path) -> Path:
    """Copy ``path`` to a timestamped backup directory under the
    ledger root. Returns the backup path."""
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    backup_root = ledger_dir / f".pre-inplace-{ts}"
    backup_root.mkdir(parents=True, exist_ok=True)
    rel = path.relative_to(ledger_dir)
    dest = backup_root / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dest)
    return dest


def _parse_decimal(text: str) -> Decimal | None:
    """Pull the first signed decimal number out of a string."""
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except Exception:  # noqa: BLE001
        return None


def _find_posting_line(
    lines: list[str],
    *,
    txn_start_line: int,
    old_account: str,
    expected_amount: Decimal | None,
) -> int:
    """Locate the 1-indexed line number of the target posting.

    Search starts at the transaction's declared start line and walks
    forward while the line is still part of the same transaction
    (indented continuation). Matches the first posting whose account
    equals ``old_account`` AND whose amount equals
    ``expected_amount`` (within Decimal precision). The amount check
    is what guards against rewriting the wrong row if line numbers
    have shifted.
    """
    # 0-indexed list → 1-indexed lineno.
    i = txn_start_line - 1
    if i < 0 or i >= len(lines):
        raise InPlaceRewriteError(
            f"txn start line {txn_start_line} is out of range for the file"
        )

    # Walk forward; stop when we hit a line that clearly isn't part
    # of the transaction (no leading whitespace + not blank).
    j = i + 1
    while j < len(lines):
        line = lines[j]
        stripped = line.strip()
        # Blank line ends the transaction block.
        if not stripped:
            break
        # Un-indented non-blank line = start of next entry.
        if not line.startswith((" ", "\t")):
            break
        m = _POSTING_LINE_RE.match(line)
        if m:
            acct = m.group(2)
            rest = m.group(3)
            if acct == old_account:
                if expected_amount is None:
                    return j + 1
                amt = _parse_decimal(rest)
                if amt is not None and amt == expected_amount:
                    return j + 1
        j += 1
    raise InPlaceRewriteError(
        f"no posting line found in txn at line {txn_start_line} "
        f"matching account={old_account!r} amount={expected_amount}"
    )


def _rewrite_line(
    line: str, *, old_account: str, new_account: str,
) -> str:
    """Replace the account path in a posting line, preserving all
    surrounding whitespace + amount + trailing comment + newline."""
    # Separate the trailing newline (if any) so the regex doesn't
    # have to deal with it — then reattach. Without this, lines
    # from splitlines(keepends=True) lose their \n in reconstruction.
    if line.endswith("\r\n"):
        newline = "\r\n"
        body = line[:-2]
    elif line.endswith("\n"):
        newline = "\n"
        body = line[:-1]
    else:
        newline = ""
        body = line
    m = _POSTING_LINE_RE.match(body)
    if not m or m.group(2) != old_account:
        raise InPlaceRewriteError(
            f"line does not match expected posting format: {line!r}"
        )
    return f"{m.group(1)}{new_account}{m.group(3)}{newline}"


def rewrite_fixme_to_account(
    *,
    source_file: Path,
    line_number: int,
    old_account: str,
    new_account: str,
    expected_amount: Decimal | None,
    ledger_dir: Path,
    main_bean: Path,
    run_check: bool = True,
) -> Tuple[str, str]:
    """Rewrite a single posting in ``source_file`` from
    ``old_account`` → ``new_account``. Returns (pre_text, post_text)
    for diff purposes.

    ``line_number`` is the beancount-parsed transaction start line
    (1-indexed). The actual posting may be a few lines below it;
    the scanner walks forward from that anchor.

    ``expected_amount`` — when set, require the posting's amount to
    match. Skip the check with ``None`` only if the caller has
    already validated (used in tests).

    Raises :class:`InPlaceRewriteError` on validation or bean-check
    failure. On failure the file is restored from the pre-call
    snapshot, so the ledger is byte-identical to the state before
    the call."""
    source_file = Path(source_file)
    ledger_dir = Path(ledger_dir)
    main_bean = Path(main_bean)

    if not _is_safe_path(source_file, ledger_dir):
        raise InPlaceRewriteError(
            f"refusing to rewrite {source_file} — outside ledger_dir, "
            "under an archive/reboot/backup dir, or a symlink"
        )
    if not source_file.exists():
        raise InPlaceRewriteError(f"source file not found: {source_file}")
    if not main_bean.exists():
        raise InPlaceRewriteError(f"main.bean not found: {main_bean}")

    original_text = source_file.read_text(encoding="utf-8")
    lines = original_text.splitlines(keepends=True)

    target_lineno = _find_posting_line(
        lines,
        txn_start_line=line_number,
        old_account=old_account,
        expected_amount=expected_amount,
    )

    new_line = _rewrite_line(
        lines[target_lineno - 1],
        old_account=old_account,
        new_account=new_account,
    )

    _, baseline = (capture_bean_check(main_bean) if run_check else (0, ""))

    backup_path = _backup(source_file, ledger_dir)
    try:
        lines[target_lineno - 1] = new_line
        # Opportunistic identity normalization on the same txn.
        # Position-stable for the line we just rewrote (single-line
        # replacement); the normalizer may insert/delete lines
        # elsewhere in the txn block but we no longer depend on
        # target_lineno after this point.
        lines = _opportunistic_normalize(lines, line_number)
        new_text = "".join(lines)
        source_file.write_text(new_text, encoding="utf-8")

        if run_check:
            try:
                run_bean_check_vs_baseline(main_bean, baseline)
            except BeanCheckError as exc:
                # Roll back from the backup.
                shutil.copy2(backup_path, source_file)
                raise InPlaceRewriteError(
                    f"bean-check rejected the rewrite; file restored. "
                    f"detail: {exc}"
                ) from exc
    except InPlaceRewriteError:
        raise
    except Exception as exc:  # noqa: BLE001
        # Any other failure — restore + re-raise as our error type.
        try:
            shutil.copy2(backup_path, source_file)
        except Exception:  # noqa: BLE001
            pass
        raise InPlaceRewriteError(
            f"rewrite failed; file restored. detail: {exc}"
        ) from exc

    return original_text, source_file.read_text(encoding="utf-8")


def _find_posting_block(
    lines: list[str], *, txn_start_line: int,
) -> tuple[int, int, str]:
    """Locate the contiguous block of posting lines for the txn
    that starts at ``txn_start_line``. Walks forward until a blank
    line, an un-indented line, or end-of-file. Skips meta lines
    (indented `key: value` patterns) — those stay in place when the
    posting block is replaced.

    Returns ``(first_posting_lineno, last_posting_lineno, indent)``,
    all 1-indexed, inclusive on both ends. ``indent`` is the
    leading-whitespace string from the first posting line so new
    postings can match it.

    Raises :class:`InPlaceRewriteError` if the txn has no posting
    lines (would mean txn_start_line points at a non-transaction
    entry, or at a transaction that lost its postings).
    """
    i = txn_start_line - 1
    if i < 0 or i >= len(lines):
        raise InPlaceRewriteError(
            f"txn start line {txn_start_line} is out of range"
        )
    first_posting = -1
    last_posting = -1
    indent = "  "
    j = i + 1
    while j < len(lines):
        line = lines[j]
        stripped = line.strip()
        if not stripped:
            break
        if not line.startswith((" ", "\t")):
            break
        m = _POSTING_LINE_RE.match(line)
        if m:
            if first_posting < 0:
                first_posting = j + 1
                indent = m.group(1)
            last_posting = j + 1
        # Non-posting indented lines (meta `key: value`) are part of
        # the txn block but we don't include them in the posting
        # range — they're left in place.
        j += 1
    if first_posting < 0:
        raise InPlaceRewriteError(
            f"txn at line {txn_start_line} has no posting lines"
        )
    return first_posting, last_posting, indent


def rewrite_txn_postings(
    *,
    source_file: Path,
    txn_start_line: int,
    new_postings: list[tuple[str, Decimal, str]],
    expected_old_accounts: list[str] | None = None,
    extra_meta: list[tuple[str, str]] | None = None,
    ledger_dir: Path,
    main_bean: Path,
    run_check: bool = True,
) -> Tuple[str, str]:
    """General M → N posting-block rewriter.

    Replaces the *entire* contiguous block of posting lines on the
    transaction at ``txn_start_line`` with ``new_postings``. Useful
    when the structural change isn't a single FIXME → split (which
    `rewrite_fixme_to_multiple_postings` already covers) but a
    full restructure: undo-and-redo a split, change every leg,
    rewrite an intercompany 4-leg as 2-leg, etc.

    Args:
      source_file: file holding the txn.
      txn_start_line: 1-indexed line of the txn header (the
        ``YYYY-MM-DD * "..."`` line).
      new_postings: list of ``(account, signed_amount, currency)``
        tuples. Their signed amounts MUST sum to zero (Beancount
        balance constraint). The function refuses the write
        otherwise.
      expected_old_accounts: optional safety net — if provided,
        the existing posting block must contain exactly these
        account names (order-insensitive). Guards against
        line-number drift between parse and edit.
      extra_meta: optional list of ``(key, value)`` pairs to stamp
        as transaction-level meta on lines inserted right after
        the txn header (and before the first posting). Existing
        meta is preserved in place — extra_meta is additive.
      ledger_dir / main_bean / run_check: same discipline as the
        single-line rewriter.

    Returns ``(pre_text, post_text)``.

    Raises :class:`InPlaceRewriteError` on any validation failure;
    on failure the file is restored byte-identical from the
    pre-call snapshot."""
    source_file = Path(source_file)
    ledger_dir = Path(ledger_dir)
    main_bean = Path(main_bean)

    if not new_postings:
        raise InPlaceRewriteError(
            "new_postings must contain at least one posting"
        )
    posting_sum = sum(
        (amt for _, amt, _ in new_postings), Decimal("0"),
    )
    if posting_sum != Decimal("0"):
        raise InPlaceRewriteError(
            f"new postings sum to {posting_sum} — Beancount requires "
            "all postings to balance to zero. Check signs + amounts."
        )

    if not _is_safe_path(source_file, ledger_dir):
        raise InPlaceRewriteError(
            f"refusing to rewrite {source_file} — outside ledger_dir, "
            "under an archive/reboot/backup dir, or a symlink"
        )
    if not source_file.exists():
        raise InPlaceRewriteError(f"source file not found: {source_file}")
    if not main_bean.exists():
        raise InPlaceRewriteError(f"main.bean not found: {main_bean}")

    original_text = source_file.read_text(encoding="utf-8")
    lines = original_text.splitlines(keepends=True)

    first_lineno, last_lineno, indent = _find_posting_block(
        lines, txn_start_line=txn_start_line,
    )

    # Optional safety check on the existing accounts.
    if expected_old_accounts is not None:
        existing_accounts: list[str] = []
        for j in range(first_lineno - 1, last_lineno):
            m = _POSTING_LINE_RE.match(lines[j])
            if m:
                existing_accounts.append(m.group(2))
        if sorted(existing_accounts) != sorted(expected_old_accounts):
            raise InPlaceRewriteError(
                f"existing posting block accounts don't match expected. "
                f"got {sorted(existing_accounts)}, "
                f"expected {sorted(expected_old_accounts)}"
            )

    # Determine line-ending style from the first old posting line.
    sample = lines[first_lineno - 1]
    if sample.endswith("\r\n"):
        newline_str = "\r\n"
    elif sample.endswith("\n"):
        newline_str = "\n"
    else:
        newline_str = ""

    new_posting_lines: list[str] = [
        f"{indent}{acct}  {amt:.2f} {ccy}{newline_str}"
        for acct, amt, ccy in new_postings
    ]

    # Optional extra meta lines inserted right after the header.
    new_meta_lines: list[str] = []
    if extra_meta:
        for key, value in extra_meta:
            # Quote string values; numeric / decimal values left bare.
            if isinstance(value, (int, float, Decimal)):
                rendered = str(value)
            else:
                rendered = f'"{value}"'
            new_meta_lines.append(
                f"{indent}{key}: {rendered}{newline_str}"
            )

    _, baseline = (capture_bean_check(main_bean) if run_check else (0, ""))

    backup_path = _backup(source_file, ledger_dir)
    try:
        # Replace the posting block. If extra_meta, also insert
        # meta lines BEFORE the new postings (right after the
        # header). For simplicity we put meta at the start of the
        # block, which means it lands between any existing meta
        # and the postings — that's fine for beancount.
        replacement = new_meta_lines + new_posting_lines
        lines[first_lineno - 1: last_lineno] = replacement
        # Opportunistic identity normalization on the same txn.
        # The header line index is unchanged (we replaced posting
        # lines, not the header), so the normalizer can locate the
        # txn block from txn_start_line as the caller passed it.
        lines = _opportunistic_normalize(lines, txn_start_line)
        new_text = "".join(lines)
        source_file.write_text(new_text, encoding="utf-8")

        if run_check:
            try:
                run_bean_check_vs_baseline(main_bean, baseline)
            except BeanCheckError as exc:
                shutil.copy2(backup_path, source_file)
                raise InPlaceRewriteError(
                    f"bean-check rejected the M→N rewrite; file restored. "
                    f"detail: {exc}"
                ) from exc
    except InPlaceRewriteError:
        raise
    except Exception as exc:  # noqa: BLE001
        try:
            shutil.copy2(backup_path, source_file)
        except Exception:  # noqa: BLE001
            pass
        raise InPlaceRewriteError(
            f"M→N rewrite failed; file restored. detail: {exc}"
        ) from exc

    return original_text, source_file.read_text(encoding="utf-8")


def _format_split_lines(
    *,
    indent: str,
    splits: list[tuple[str, Decimal]],
    currency: str,
    newline: str,
) -> list[str]:
    """Format N posting lines, all using ``indent`` and ``newline``,
    matching the convention of the FIXME line being replaced. Each
    split is ``(account, signed_amount)`` — caller decides sign so
    the new lines preserve the source posting's sign convention."""
    out: list[str] = []
    for acct, amt in splits:
        # Two-space gap between account and amount mirrors the
        # convention beancount.printer uses; bean-check is
        # whitespace-tolerant either way.
        out.append(
            f"{indent}{acct}  {amt:.2f} {currency}{newline}"
        )
    return out


def rewrite_fixme_to_multiple_postings(
    *,
    source_file: Path,
    line_number: int,
    old_account: str,
    splits: list[tuple[str, Decimal]],
    expected_amount: Decimal | None,
    currency: str,
    ledger_dir: Path,
    main_bean: Path,
    run_check: bool = True,
) -> Tuple[str, str]:
    """Replace one FIXME posting line with N posting lines that sum
    to the original amount. Same backup → bean-check → rollback
    discipline as :func:`rewrite_fixme_to_account`. Returns
    ``(pre_text, post_text)``.

    ``splits`` is a list of ``(account, signed_amount)`` tuples. The
    sum of amounts MUST equal the original posting's signed amount —
    the function verifies this before touching the file. Indentation
    and line ending are copied from the source line so the new lines
    visually match the rest of the transaction.

    The new lines are inserted at the position of the original FIXME
    line, replacing it. No trailing comment or per-posting meta from
    the FIXME line is preserved (those described the FIXME state
    and don't apply to the resolved postings).

    Raises :class:`InPlaceRewriteError` on validation failure
    (path safety, sum mismatch, missing file, bean-check rejection,
    etc.). On any failure the file is restored byte-identical from
    the pre-call snapshot.
    """
    source_file = Path(source_file)
    ledger_dir = Path(ledger_dir)
    main_bean = Path(main_bean)

    if not splits:
        raise InPlaceRewriteError(
            "splits must contain at least one (account, amount) entry"
        )
    # Sum-equals-original check. The caller passes signed amounts;
    # we compare against the signed expected_amount when available.
    splits_sum = sum((amt for _, amt in splits), Decimal("0"))
    if expected_amount is not None and splits_sum != expected_amount:
        raise InPlaceRewriteError(
            f"split amounts sum to {splits_sum}, expected "
            f"{expected_amount} to preserve transaction balance"
        )

    if not _is_safe_path(source_file, ledger_dir):
        raise InPlaceRewriteError(
            f"refusing to rewrite {source_file} — outside ledger_dir, "
            "under an archive/reboot/backup dir, or a symlink"
        )
    if not source_file.exists():
        raise InPlaceRewriteError(f"source file not found: {source_file}")
    if not main_bean.exists():
        raise InPlaceRewriteError(f"main.bean not found: {main_bean}")

    original_text = source_file.read_text(encoding="utf-8")
    lines = original_text.splitlines(keepends=True)

    target_lineno = _find_posting_line(
        lines,
        txn_start_line=line_number,
        old_account=old_account,
        expected_amount=expected_amount,
    )

    # Pull indentation + newline style off the line we're replacing
    # so the new lines look native to the transaction.
    target_line = lines[target_lineno - 1]
    if target_line.endswith("\r\n"):
        newline_str = "\r\n"
        body = target_line[:-2]
    elif target_line.endswith("\n"):
        newline_str = "\n"
        body = target_line[:-1]
    else:
        newline_str = ""
        body = target_line
    m = _POSTING_LINE_RE.match(body)
    if not m:
        raise InPlaceRewriteError(
            f"FIXME line does not match expected posting format: "
            f"{target_line!r}"
        )
    indent = m.group(1)

    new_lines = _format_split_lines(
        indent=indent, splits=splits,
        currency=currency, newline=newline_str,
    )

    _, baseline = (capture_bean_check(main_bean) if run_check else (0, ""))

    backup_path = _backup(source_file, ledger_dir)
    try:
        # Replace the single FIXME line with the new posting lines.
        lines[target_lineno - 1: target_lineno] = new_lines
        # Opportunistic identity normalization on the same txn.
        lines = _opportunistic_normalize(lines, line_number)
        new_text = "".join(lines)
        source_file.write_text(new_text, encoding="utf-8")

        if run_check:
            try:
                run_bean_check_vs_baseline(main_bean, baseline)
            except BeanCheckError as exc:
                shutil.copy2(backup_path, source_file)
                raise InPlaceRewriteError(
                    f"bean-check rejected the multi-leg rewrite; "
                    f"file restored. detail: {exc}"
                ) from exc
    except InPlaceRewriteError:
        raise
    except Exception as exc:  # noqa: BLE001
        try:
            shutil.copy2(backup_path, source_file)
        except Exception:  # noqa: BLE001
            pass
        raise InPlaceRewriteError(
            f"multi-leg rewrite failed; file restored. detail: {exc}"
        ) from exc

    return original_text, source_file.read_text(encoding="utf-8")
