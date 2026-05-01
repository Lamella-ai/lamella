# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from lamella.core.identity import (
    REF_KEY,
    SOURCE_KEY,
    STAGING_SOURCE_NAMES,
    TXN_ID_KEY,
    mint_txn_id,
)
from lamella.core.ledger_writer import (
    BeanCheckError,
    WriteError,
    ensure_include_in_main,
    run_bean_check,
)

log = logging.getLogger(__name__)


SIMPLEFIN_HEADER = (
    "; simplefin_transactions.bean — Managed by Lamella (Phase 4+).\n"
    "; Do not hand-edit; the Connector is the sole writer.\n"
)


class InvalidSourceError(WriteError):
    """Raised when ``lamella-source`` value on a staged-txn directive
    is not in the closed STAGING_SOURCE_NAMES enum (ADR-0043b §2).
    Pre-write hook: rejected before any file mutation."""

    def __init__(self, source: str):
        super().__init__(
            f"invalid staging source {source!r}; "
            f"allowed values: {sorted(STAGING_SOURCE_NAMES)}"
        )
        self.source = source


class StagedDirectiveNotFoundError(WriteError):
    """Raised when ``promote_staged_txn`` cannot locate a
    ``custom "staged-txn"`` directive matching the given
    ``lamella-txn-id`` in the target file. Either the row was
    already promoted (idempotency violation), or the directive
    was hand-edited away. Caller decides whether to surface as
    a 404 or as recovery."""

    def __init__(self, lamella_txn_id: str, file_path: Path):
        super().__init__(
            f"no `custom \"staged-txn\"` directive with "
            f"lamella-txn-id={lamella_txn_id!r} in {file_path}"
        )
        self.lamella_txn_id = lamella_txn_id
        self.file_path = file_path


@dataclass(frozen=True)
class PendingEntry:
    """One transaction ready to render. FIXME-bound entries use
    ``target_account = 'Expenses:FIXME'`` (or the per-entity variant)."""

    date: date
    simplefin_id: str
    payee: str | None
    narration: str | None
    amount: Decimal  # signed as emitted by SimpleFIN (positive = credit to account)
    currency: str
    source_account: str
    target_account: str
    ai_classified: bool = False
    ai_decision_id: int | None = None
    rule_id: int | None = None
    # NEXTGEN Phase B: links the pending entry back to the unified
    # staging row that spawned it. Used by ingest after
    # ``append_entries`` succeeds to mark the staged row promoted.
    staged_id: int | None = None
    # Immutable identity (UUIDv7) carried over from the staged row so
    # the on-disk lamella-txn-id matches what /txn/{token} resolved to
    # while the row was still in staging. None means "mint fresh on
    # render" (legacy callers, tests, ingest paths that bypass staging).
    lamella_txn_id: str | None = None
    # ADR-0046 Phase 1: when the user classifies a transfer-suspect
    # single-leg row to an Assets:/Liabilities: target, the destination
    # leg is "Lamella-authored" — there's no real bank source for it
    # yet. The matcher (Phase 2+) replaces this leg in place when the
    # genuine other half arrives. ``synthetic_kind`` is the provenance
    # tag (e.g. ``"user-classified-counterpart"``); when None, no
    # synthetic-* meta is emitted on the destination posting.
    synthetic_kind: str | None = None
    synthetic_confidence: str | None = None  # "guessed" | "likely" | "confirmed"
    synthetic_replaceable: bool = True
    # Refund-of-expense link: when this PendingEntry is a refund the
    # user routed against a previously-classified expense, this carries
    # the original expense's ``lamella-txn-id``. ``render_entry`` stamps
    # it as ``lamella-refund-of`` at the txn-meta level so the bidirectional
    # /txn page link can be rebuilt by walking the ledger. The link is
    # the source of truth — SQLite indexing of these links is a cache.
    refund_of_txn_id: str | None = None
    # ADR-0059 — the source's verbatim description text for this leg.
    # Written to the bank-side posting as ``lamella-source-description-0``
    # at first emit so the source's phrasing survives on disk even
    # when the canonical narration is later synthesized from multiple
    # sources. Optional — not every source carries useful description
    # text; when absent, the writer skips the description line.
    source_description: str | None = None


def ensure_simplefin_file_exists(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(SIMPLEFIN_HEADER, encoding="utf-8")


def _fsync_file(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDWR)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _q(value: str | None) -> str:
    if value is None:
        return ""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def render_entry(entry: PendingEntry) -> str:
    """Render one Beancount transaction. We keep the formatting stable and
    deterministic — same inputs always produce the same bytes.

    Schema (Phase 7b of NORMALIZE_TXN_IDENTITY.md — writer emits the
    new format only):
      * Transaction meta: ``lamella-txn-id`` (UUIDv7 lineage minted at
        emit time).
      * Source-side (first) posting meta: paired indexed source keys
        ``lamella-source-0: "simplefin"`` + ``lamella-source-reference-id-0:
        "<id>"``. Every reader resolves the SimpleFIN id via
        ``identity.find_source_reference(entry, "simplefin")``; the
        cleaner / alias-injector accept both formats in raw text.

    Legacy on-disk content carrying ``lamella-simplefin-id`` at txn
    level still parses transparently via
    ``_legacy_meta.normalize_entries`` (mirrors down to first posting).
    New writes are clean.
    """
    amt = Decimal(entry.amount)
    # SimpleFIN reports "amount" signed from the *account's* POV: negative
    # means money leaving the account. For the source leg we use the raw
    # signed amount; the target leg gets the opposite sign so the txn
    # sums to zero.
    source_amt = amt
    target_amt = -amt

    payee = _q(entry.payee) if entry.payee else ""
    narration = _q(entry.narration or "")

    header = f'{entry.date.isoformat()} *'
    if payee:
        header += f' "{payee}"'
    header += f' "{narration}"'

    # Reuse the staged row's identity when present so /txn/{token}
    # resolves to the same URL pre- and post-promotion. Mint fresh
    # only for legacy / test paths that bypass staging.
    txn_id_value = entry.lamella_txn_id or mint_txn_id()
    lines = [
        header,
        f'  {TXN_ID_KEY}: "{txn_id_value}"',
    ]
    if entry.ai_classified:
        lines.append("  lamella-ai-classified: TRUE")
    if entry.ai_decision_id is not None:
        lines.append(f'  lamella-ai-decision-id: "{entry.ai_decision_id}"')
    if entry.rule_id is not None:
        lines.append(f'  lamella-rule-id: "{entry.rule_id}"')
    # Refund-of-expense link — bidirectional /txn page lookup walks
    # the ledger for any txn carrying this meta whose value matches
    # the original's lamella-txn-id. Stamped only when the user (or
    # a future auto-detector) explicitly accepted a refund-of match;
    # absent for ordinary deposits / transfers.
    if entry.refund_of_txn_id:
        lines.append(
            f'  lamella-refund-of: "{_q(entry.refund_of_txn_id)}"'
        )
    # Source-side posting (bank/card account) — paired indexed source
    # meta is the canonical post-normalization location for provenance.
    # ADR-0059 — when the source carried a description text, persist
    # it verbatim alongside the source / source-reference-id pair so
    # the source's phrasing survives even after the txn-level
    # narration gets re-synthesized from multiple sources.
    lines.append(f"  {entry.source_account}  {source_amt:.2f} {entry.currency}")
    lines.append(f'    {SOURCE_KEY}-0: "simplefin"')
    lines.append(f'    {REF_KEY}-0: "{_q(entry.simplefin_id)}"')
    if entry.source_description:
        lines.append(
            f'    lamella-source-description-0: '
            f'"{_q(entry.source_description)}"'
        )
    # Target leg — synthesized by us, no source provenance.
    lines.append(f"  {entry.target_account}  {target_amt:.2f} {entry.currency}")
    # ADR-0046 Phase 1: when the destination leg is a Lamella-authored
    # counterpart (user picked an Assets:/Liabilities: target on a
    # transfer-suspect single-leg row), tag it synthetic so the matcher
    # can replace it in place when the real other half arrives.
    if entry.synthetic_kind:
        # TZ-aware UTC timestamp per ADR-0023; isoformat with seconds
        # precision so the value is stable / human-readable in the
        # ledger and on /audit.
        decided_at = (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
        )
        lines.append(
            f'    lamella-synthetic: "{_q(entry.synthetic_kind)}"'
        )
        lines.append(
            f'    lamella-synthetic-confidence: '
            f'"{_q(entry.synthetic_confidence or "guessed")}"'
        )
        lines.append(
            f'    lamella-synthetic-replaceable: '
            f'{"TRUE" if entry.synthetic_replaceable else "FALSE"}'
        )
        lines.append(
            f'    lamella-synthetic-decided-at: "{decided_at}"'
        )
    return "\n" + "\n".join(lines) + "\n"


def render_staged_txn_directive(
    entry: PendingEntry,
    *,
    source: str = "simplefin",
) -> str:
    """Render one ``custom "staged-txn"`` directive per ADR-0043 + ADR-0043b.

    No monetary posting, no balance-sheet impact — the directive is a
    metadata-only audit anchor. The corresponding ``staged_transactions``
    row in SQLite holds the ingest-time payload as a cache; the directive
    is the source of truth that survives a DB wipe.

    Required directive shape (ADR-0043b §1–§3):

        YYYY-MM-DD custom "staged-txn"
          lamella-txn-id: "<uuidv7>"
          lamella-source: "simplefin"
          lamella-source-reference-id: "<id>"
          lamella-txn-date: YYYY-MM-DD
          lamella-txn-amount: <signed-decimal> <currency>
          lamella-source-account: "<account-path>"
          lamella-txn-narration: "<payee / description>"

    Sign convention (ADR-0043b §3): ``lamella-txn-amount`` matches
    ``PendingEntry.amount`` POV — negative = money leaving the source
    account.

    ``source`` defaults to ``"simplefin"`` because this writer's primary
    caller is the bank-sync ingest path. Other ingest paths (CSV,
    paste, reboot) pass their own value. Raises ``InvalidSourceError``
    if the value is not in STAGING_SOURCE_NAMES.
    """
    if source not in STAGING_SOURCE_NAMES:
        raise InvalidSourceError(source)
    txn_id_value = entry.lamella_txn_id or mint_txn_id()
    amt = Decimal(entry.amount)
    narration_q = _q(entry.narration or entry.payee or "")
    lines = [
        f'\n{entry.date.isoformat()} custom "staged-txn" "{source}"',
        f'  {TXN_ID_KEY}: "{txn_id_value}"',
        f'  lamella-source: "{source}"',
        f'  lamella-source-reference-id: "{_q(entry.simplefin_id)}"',
        f'  lamella-txn-date: {entry.date.isoformat()}',
        f'  lamella-txn-amount: {amt:.2f} {entry.currency}',
        f'  lamella-source-account: "{_q(entry.source_account)}"',
        f'  lamella-txn-narration: "{narration_q}"',
    ]
    return "\n".join(lines) + "\n"


def render_staged_txn_promoted_directive(
    entry: PendingEntry,
    *,
    source: str = "simplefin",
    promoted_at: str,
    promoted_by: str,
    promoted_rule_id: str | None = None,
    promoted_ai_model: str | None = None,
) -> str:
    """Render the ``custom "staged-txn-promoted"`` audit anchor that
    replaces a ``staged-txn`` directive in-place at promotion time per
    ADR-0043b §4. Carries the original directive fields verbatim PLUS
    the supplemental promotion meta:

      * ``lamella-promoted-at`` (REQUIRED, ISO-8601 TZ-aware UTC)
      * ``lamella-promoted-by`` (REQUIRED, "rule"|"ai"|"manual")
      * ``lamella-promoted-rule-id`` (iff promoted-by == "rule")
      * ``lamella-promoted-ai-model`` (iff promoted-by == "ai")
    """
    if source not in STAGING_SOURCE_NAMES:
        raise InvalidSourceError(source)
    if promoted_by not in {"rule", "ai", "manual"}:
        raise WriteError(
            f"invalid promoted_by {promoted_by!r}; "
            "expected one of: rule, ai, manual"
        )
    txn_id_value = entry.lamella_txn_id or mint_txn_id()
    amt = Decimal(entry.amount)
    narration_q = _q(entry.narration or entry.payee or "")
    lines = [
        f'\n{entry.date.isoformat()} custom "staged-txn-promoted" "{source}"',
        f'  {TXN_ID_KEY}: "{txn_id_value}"',
        f'  lamella-source: "{source}"',
        f'  lamella-source-reference-id: "{_q(entry.simplefin_id)}"',
        f'  lamella-txn-date: {entry.date.isoformat()}',
        f'  lamella-txn-amount: {amt:.2f} {entry.currency}',
        f'  lamella-source-account: "{_q(entry.source_account)}"',
        f'  lamella-txn-narration: "{narration_q}"',
        f'  lamella-promoted-at: "{promoted_at}"',
        f'  lamella-promoted-by: "{promoted_by}"',
    ]
    if promoted_by == "rule" and promoted_rule_id:
        lines.append(f'  lamella-promoted-rule-id: "{_q(promoted_rule_id)}"')
    if promoted_by == "ai" and promoted_ai_model:
        lines.append(f'  lamella-promoted-ai-model: "{_q(promoted_ai_model)}"')
    return "\n".join(lines) + "\n"


def _replace_staged_directive(
    text: str,
    *,
    target_lamella_txn_id: str,
    promoted_block: str,
) -> tuple[str, bool]:
    """Find a ``custom "staged-txn"`` directive whose ``lamella-txn-id``
    meta matches ``target_lamella_txn_id`` and replace the entire
    directive block (header + indented meta lines until the next
    non-indented line) with ``promoted_block``. Returns
    ``(new_text, found)``.

    Pure text operation — no parsing, no I/O. Used by P3's
    ``SimpleFINWriter.promote_staged_txn``.

    Block boundaries:
      * Start: a line matching ``YYYY-MM-DD custom "staged-txn" "...``.
      * End: the line BEFORE the next directive header or EOF (next
        directive header is detected by leading-non-whitespace and a
        date-shape ``YYYY-MM-DD`` prefix). All indented (`  ...`)
        meta lines and contiguous blank lines after the header
        are absorbed into the block.
    """
    lines = text.splitlines(keepends=True)
    import re
    header_re = re.compile(
        r'^\d{4}-\d{2}-\d{2}\s+custom\s+"staged-txn"\s+"[^"]+"\s*$'
    )
    next_directive_re = re.compile(r'^\d{4}-\d{2}-\d{2}\s')
    txn_id_re = re.compile(
        r'^\s+lamella-txn-id:\s*"([^"]+)"\s*$'
    )

    # First pass: locate every staged-txn header line (start indices)
    # and read its meta block to find the lamella-txn-id; pick the
    # block whose id matches.
    n = len(lines)
    start_idx = -1
    end_idx = -1
    for i in range(n):
        line = lines[i].rstrip("\n").rstrip("\r")
        if not header_re.match(line):
            continue
        # Walk forward over indented meta until next directive header
        # (date-prefixed) or EOF.
        block_txn_id: str | None = None
        j = i + 1
        while j < n:
            peek = lines[j]
            if next_directive_re.match(peek):
                break
            stripped = peek.rstrip("\n").rstrip("\r")
            if stripped == "":
                j += 1
                continue
            if not (peek.startswith(" ") or peek.startswith("\t")):
                break
            m = txn_id_re.match(peek.rstrip("\n").rstrip("\r"))
            if m:
                block_txn_id = m.group(1)
            j += 1
        if block_txn_id == target_lamella_txn_id:
            start_idx = i
            # Walk back past blank lines that immediately precede the
            # header so the replacement doesn't leave double blanks.
            while start_idx > 0 and lines[start_idx - 1].strip() == "":
                start_idx -= 1
            end_idx = j
            break

    if start_idx < 0:
        return text, False

    # Trim trailing newlines from the replacement so the resulting
    # text doesn't accumulate a triple-newline.
    new_block = promoted_block
    if not new_block.endswith("\n"):
        new_block += "\n"
    new_text = "".join(lines[:start_idx]) + new_block + "".join(lines[end_idx:])
    return new_text, True


class SimpleFINWriter:
    """Append classified SimpleFIN entries to ``simplefin_transactions.bean``.

    Contract (from PHASE_4_PLAN.md "Writer discipline"):

    1. Take a file lock on the ledger directory.
    2. Record pre-write size.
    3. Append entries atomically.
    4. Run ``bean-check main.bean``.
    5. On failure, truncate back to pre-write size; raise ``BeanCheckFailed``.
    6. On success, best-effort commit to git (failures logged, not raised).

    The writer does NOT batch across accounts — callers feed one account's
    worth of entries per ``append_entries`` call.
    """

    def __init__(
        self,
        *,
        main_bean: Path,
        simplefin_path: Path,
        run_check: bool = True,
        run_git_commit: bool = False,
        lock_timeout: float = 30.0,
    ):
        self.main_bean = main_bean
        self.simplefin_path = simplefin_path
        self.run_check = run_check
        self.run_git_commit = run_git_commit
        self.lock_timeout = lock_timeout

    def _lock_path(self) -> Path:
        return self.simplefin_path.parent / ".lamella.lock"

    def _acquire_lock(self) -> Path:
        lock = self._lock_path()
        lock.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.lock_timeout
        while True:
            try:
                fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode("ascii"))
                os.close(fd)
                return lock
            except FileExistsError:
                if time.monotonic() >= deadline:
                    raise WriteError(
                        f"could not acquire writer lock at {lock} within {self.lock_timeout}s"
                    )
                time.sleep(0.1)

    def _release_lock(self, lock: Path) -> None:
        try:
            lock.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("failed to release writer lock %s: %s", lock, exc)

    def append_entries(
        self,
        entries: Iterable[PendingEntry],
        *,
        target_path: Path | None = None,
        commit_message: str | None = None,
    ) -> int:
        """Append ``entries`` to ``target_path`` (default:
        ``simplefin_transactions.bean``). Returns the number of entries
        written. On bean-check failure, truncates back and raises
        ``BeanCheckError``.

        ``target_path`` overrides the default so shadow mode can write to
        ``simplefin_transactions.connector_preview.bean`` without risking
        the real ledger."""
        dest = target_path or self.simplefin_path
        pending = list(entries)
        if not pending:
            return 0

        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        lock = self._acquire_lock()
        try:
            ensure_simplefin_file_exists(dest)
            # Only tie the file into main.bean when we're writing the real
            # file. The shadow preview MUST NOT be included.
            if dest == self.simplefin_path:
                ensure_include_in_main(self.main_bean, dest)

            pre_size = dest.stat().st_size
            backup_main = self.main_bean.read_bytes()

            rendered = "".join(render_entry(e) for e in pending)
            with dest.open("a", encoding="utf-8") as fh:
                fh.write(rendered)
                fh.flush()
            _fsync_file(dest)

            if self.run_check and dest == self.simplefin_path:
                try:
                    run_bean_check(self.main_bean)
                except BeanCheckError:
                    # Truncate back to pre-write size; restore main.bean in
                    # case ensure_include_in_main mutated it.
                    with dest.open("rb+") as fh:
                        fh.truncate(pre_size)
                    self.main_bean.write_bytes(backup_main)
                    raise
        finally:
            self._release_lock(lock)

        if self.run_git_commit and dest == self.simplefin_path:
            self._best_effort_git_commit(dest, commit_message or "simplefin: ingest")

        return len(pending)

    def append_split_entry(
        self,
        *,
        txn_date: date,
        simplefin_id: str,
        source_account: str,
        source_amount: Decimal,
        splits: list[tuple[str, Decimal]],
        narration: str | None = None,
        payee: str | None = None,
        currency: str = "USD",
        extra_meta: dict | None = None,
        target_path: Path | None = None,
        commit_message: str | None = None,
        lamella_txn_id: str | None = None,
    ) -> str:
        """Append a SINGLE multi-leg classified SimpleFIN transaction.

        Unlike ``append_entries`` (which writes 2-leg PendingEntry
        records), this writes one transaction with N target legs
        offset by one source leg — the shape WP6's post-ingest
        auto-classify needs for loan payments (principal + interest
        + escrow + tax + insurance all from one SimpleFIN inflow).

        Same lock / bean-check / rollback discipline as
        ``append_entries``. `extra_meta` stamps additional lamella-*
        keys between the simplefin-id meta and the postings — used
        by WP6 for the lamella-loan-autoclass-* tier / decision-id.

        Signs: `source_amount` is signed from SimpleFIN's POV
        (negative for money leaving a checking account), and `splits`
        are rendered with opposite signs so the transaction sums
        to zero. The caller is expected to have already run
        plan_from_facts() so splits total |source_amount|.
        """
        if not splits:
            raise WriteError("splits cannot be empty")
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        dest = target_path or self.simplefin_path
        total_splits = sum((amt for _, amt in splits), Decimal("0"))
        # Defensive: abs of splits must match abs of source within a cent.
        if abs(abs(total_splits) - abs(source_amount)) > Decimal("0.02"):
            raise WriteError(
                f"splits total {total_splits:.2f} does not balance source "
                f"{source_amount:.2f}"
            )

        payee_q = _q(payee) if payee else ""
        narration_q = _q(narration or "")
        header = f"{txn_date.isoformat()} *"
        if payee_q:
            header += f' "{payee_q}"'
        header += f' "{narration_q}"'

        # Carry over the staged row's identity when supplied; mint
        # fresh only when this writer is invoked outside the staging
        # promotion path (legacy / direct-write callers).
        txn_id_value = lamella_txn_id or mint_txn_id()
        lines = [
            header,
            f'  {TXN_ID_KEY}: "{txn_id_value}"',
        ]
        for k, v in (extra_meta or {}).items():
            lines.append(f'  {k}: "{_q(str(v))}"')
        # Source leg preserves SimpleFIN sign; splits are rendered
        # with the opposite sign so the transaction sums to zero.
        lines.append(f"  {source_account}  {source_amount:.2f} {currency}")
        lines.append(f'    {SOURCE_KEY}-0: "simplefin"')
        lines.append(f'    {REF_KEY}-0: "{_q(simplefin_id)}"')
        # Flip split sign: if source is negative (money out), splits
        # must be positive (money into loan accounts).
        split_sign = Decimal("1") if source_amount < 0 else Decimal("-1")
        for acct, amt in splits:
            lines.append(f"  {acct}  {(split_sign * amt):.2f} {currency}")
        rendered = "\n" + "\n".join(lines) + "\n"

        lock = self._acquire_lock()
        try:
            ensure_simplefin_file_exists(dest)
            if dest == self.simplefin_path:
                ensure_include_in_main(self.main_bean, dest)

            pre_size = dest.stat().st_size
            backup_main = self.main_bean.read_bytes()

            with dest.open("a", encoding="utf-8") as fh:
                fh.write(rendered)
                fh.flush()
            _fsync_file(dest)

            if self.run_check and dest == self.simplefin_path:
                try:
                    run_bean_check(self.main_bean)
                except BeanCheckError:
                    with dest.open("rb+") as fh:
                        fh.truncate(pre_size)
                    self.main_bean.write_bytes(backup_main)
                    raise
        finally:
            self._release_lock(lock)

        if self.run_git_commit and dest == self.simplefin_path:
            self._best_effort_git_commit(
                dest, commit_message or "simplefin: loan auto-classify",
            )

        return rendered

    def append_staged_txn_directives(
        self,
        entries: Iterable[PendingEntry],
        *,
        source: str = "simplefin",
        target_path: Path | None = None,
        commit_message: str | None = None,
    ) -> int:
        """Append one ``custom "staged-txn"`` directive per entry to
        ``target_path`` (default: simplefin_transactions.bean). No
        balanced txn is emitted — the directive is metadata-only per
        ADR-0043 / ADR-0043b. Same lock + bean-check + rollback
        discipline as ``append_entries``.

        Pre-write validation (ADR-0043b Mitigation): every entry's
        ``source`` is checked against STAGING_SOURCE_NAMES; an
        ``InvalidSourceError`` aborts the entire batch before any file
        write. Same fail-closed posture the FIXME guard had.

        Returns the number of directives written.

        Post-write count assertion: directive count in the file equals
        len(entries) at exit. The migration plan calls this out as a
        non-optional invariant — a writer bug that drops a directive
        on the floor would silently fail the round-trip with the
        ``staged_transactions`` row count.
        """
        dest = target_path or self.simplefin_path
        pending = list(entries)
        if not pending:
            return 0
        if source not in STAGING_SOURCE_NAMES:
            raise InvalidSourceError(source)
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        lock = self._acquire_lock()
        try:
            ensure_simplefin_file_exists(dest)
            if dest == self.simplefin_path:
                ensure_include_in_main(self.main_bean, dest)

            pre_size = dest.stat().st_size
            backup_main = self.main_bean.read_bytes()

            rendered = "".join(
                render_staged_txn_directive(e, source=source) for e in pending
            )
            with dest.open("a", encoding="utf-8") as fh:
                fh.write(rendered)
                fh.flush()
            _fsync_file(dest)

            if self.run_check and dest == self.simplefin_path:
                try:
                    run_bean_check(self.main_bean)
                except BeanCheckError:
                    with dest.open("rb+") as fh:
                        fh.truncate(pre_size)
                    self.main_bean.write_bytes(backup_main)
                    raise

            # Post-write count invariant. Walk the file's tail (only the
            # bytes added in this call) and confirm one staged-txn header
            # per entry — catches a renderer regression that would
            # produce 0 lines or a corrupt block.
            self._assert_staged_txn_count(dest, pre_size, len(pending))
        finally:
            self._release_lock(lock)

        if self.run_git_commit and dest == self.simplefin_path:
            self._best_effort_git_commit(
                dest, commit_message or f"{source}: stage directives",
            )

        return len(pending)

    def promote_staged_txn(
        self,
        *,
        promoted_entry: PendingEntry,
        promoted_by: str,
        source: str = "simplefin",
        promoted_rule_id: str | None = None,
        promoted_ai_model: str | None = None,
        target_path: Path | None = None,
    ) -> str:
        """ADR-0043 P3 — atomic two-part promotion write.

        In one lock acquisition + one bean-check pass:
          (a) replace the ``custom "staged-txn"`` directive whose
              ``lamella-txn-id`` matches ``promoted_entry.lamella_txn_id``
              with a ``custom "staged-txn-promoted"`` directive carrying
              the audit-trail meta (promoted_at / promoted_by /
              optional rule_id / optional ai_model); and
          (b) append a real balanced transaction whose target leg is
              ``promoted_entry.target_account``.

        Sign convention: ``promoted_entry.amount`` is signed from the
        source-account POV (negative = debit) — same as
        ``PendingEntry.amount`` semantics throughout bank_sync.

        Lock + rollback discipline: snapshot both files before the
        write. On bean-check failure or any exception, restore both
        files byte-for-byte. The two edits are coupled: either both
        land or neither does.

        Returns ``promoted_entry.lamella_txn_id`` — the txn lineage
        id that survived the staging → promotion bridge unchanged
        (per ADR-0043b §1).

        Raises:
          * ``StagedDirectiveNotFoundError`` if the directive can't
            be located in the file (already promoted, or hand-edited).
          * ``InvalidSourceError`` if ``source`` is not in
            STAGING_SOURCE_NAMES.
          * ``WriteError`` if ``promoted_by`` is not in
            {"rule", "ai", "manual"}.
          * ``BeanCheckError`` if the post-write ledger fails
            bean-check (file restored before the raise).
        """
        if source not in STAGING_SOURCE_NAMES:
            raise InvalidSourceError(source)
        if promoted_by not in {"rule", "ai", "manual"}:
            raise WriteError(
                f"invalid promoted_by {promoted_by!r}; "
                "expected one of: rule, ai, manual"
            )
        if not promoted_entry.lamella_txn_id:
            raise WriteError(
                "promote_staged_txn requires promoted_entry.lamella_txn_id "
                "to identify the directive"
            )
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        dest = target_path or self.simplefin_path
        if not dest.exists():
            raise StagedDirectiveNotFoundError(
                promoted_entry.lamella_txn_id, dest,
            )

        promoted_at = (
            datetime.now(timezone.utc).isoformat(timespec="seconds")
        )

        # Read once under the lock so the directive scan + replacement
        # see the same bytes the appender writes back.
        lock = self._acquire_lock()
        try:
            backup_main = self.main_bean.read_bytes()
            backup_dest = dest.read_bytes()
            try:
                original_text = backup_dest.decode("utf-8")
                replaced_text, found = _replace_staged_directive(
                    original_text,
                    target_lamella_txn_id=promoted_entry.lamella_txn_id,
                    promoted_block=render_staged_txn_promoted_directive(
                        promoted_entry,
                        source=source,
                        promoted_at=promoted_at,
                        promoted_by=promoted_by,
                        promoted_rule_id=promoted_rule_id,
                        promoted_ai_model=promoted_ai_model,
                    ),
                )
                if not found:
                    raise StagedDirectiveNotFoundError(
                        promoted_entry.lamella_txn_id, dest,
                    )
                # Append the balanced txn rendered the standard way —
                # render_entry handles all the lamella-* posting meta,
                # synthetic-leg tagging, etc., the same way regular
                # ingest does.
                balanced = render_entry(promoted_entry)
                if not replaced_text.endswith("\n"):
                    replaced_text += "\n"
                final_text = replaced_text + balanced
                dest.write_text(final_text, encoding="utf-8")
                _fsync_file(dest)

                if self.run_check and dest == self.simplefin_path:
                    run_bean_check(self.main_bean)
            except (BeanCheckError, StagedDirectiveNotFoundError, Exception):
                # Restore both files byte-for-byte before propagating.
                # The directive replacement and the balanced txn append
                # are coupled — either both stay or neither.
                dest.write_bytes(backup_dest)
                self.main_bean.write_bytes(backup_main)
                raise
        finally:
            self._release_lock(lock)

        return promoted_entry.lamella_txn_id

    @staticmethod
    def _assert_staged_txn_count(
        path: Path, pre_size: int, expected: int,
    ) -> None:
        """Read the bytes appended in this write and count the
        ``custom "staged-txn"`` headers. Raises WriteError if the
        count differs from ``expected`` — a sentinel that the
        renderer or appender skipped a record."""
        with path.open("rb") as fh:
            fh.seek(pre_size)
            tail = fh.read().decode("utf-8", errors="replace")
        # Match `YYYY-MM-DD custom "staged-txn"` at line start. We
        # deliberately do NOT match the `-promoted` variant — the
        # invariant is for the freshly-written staged-txn directives,
        # not promoted ones (those replace the un-promoted form via a
        # different code path).
        import re
        count = len(re.findall(
            r'^\d{4}-\d{2}-\d{2} custom "staged-txn" "[^"]+"$',
            tail,
            re.MULTILINE,
        ))
        if count != expected:
            raise WriteError(
                f"staged-txn count assertion failed: wrote {count} "
                f"directives, expected {expected} (file: {path.name})"
            )

    def _best_effort_git_commit(self, path: Path, message: str) -> None:
        cwd = path.parent
        try:
            subprocess.run(
                ["git", "add", str(path.name)],
                cwd=str(cwd),
                check=True,
                capture_output=True,
                timeout=10,
            )
            subprocess.run(
                ["git", "commit", "-m", message],
                cwd=str(cwd),
                check=True,
                capture_output=True,
                timeout=10,
            )
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            log.warning("git commit skipped: %s", exc)
