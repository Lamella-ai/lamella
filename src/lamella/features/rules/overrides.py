# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from lamella.core.fs import validate_safe_path
from lamella.core.ledger_writer import (
    BeanCheckError,
    WriteError,
    capture_bean_check,
    ensure_include_in_main,
    run_bean_check,
    run_bean_check_vs_baseline,
)

log = logging.getLogger(__name__)

OVERRIDES_HEADER = (
    "; connector_overrides.bean — Managed by Lamella.\n"
    "; Each block moves a FIXME posting onto the user-chosen target account.\n"
    "; Do not hand-edit; rewrite via the review UI.\n"
)


def ensure_overrides_exists(overrides: Path) -> None:
    if overrides.exists():
        return
    safe = validate_safe_path(overrides, allowed_roots=[overrides.parent])
    safe.parent.mkdir(parents=True, exist_ok=True)
    safe.write_text(OVERRIDES_HEADER, encoding="utf-8")


def _render_modified_at(modified_at: datetime) -> str:
    return modified_at.isoformat(timespec="seconds")


def _esc(s: str) -> str:
    """Escape for a beancount double-quoted string literal.

    Narrations from SimpleFIN / merchant feeds frequently include
    literal double-quote characters (`3-1/2" Coaxial Loudspeakers`,
    `26"+23" Wiper Blades`). Writing those verbatim produces an
    unterminated string → the whole ledger fails bean-check on next
    load → the app redirects every route to /setup and refuses to
    unblock until the user hand-edits the file. Must escape.
    """
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _override_block(
    *,
    txn_date: date,
    txn_hash: str,
    amount: Decimal,
    from_account: str,
    to_account: str,
    modified_at: datetime | None = None,
    currency: str = "USD",
    narration: str = "FIXME override",
    extra_meta: dict | None = None,
) -> str:
    """Build a 2-leg override block routing ``from_account`` →
    ``to_account``. ``extra_meta`` accepts a dict of additional
    ``lamella-*`` keys stamped on the txn between modified-at and the
    postings — the refund-of path uses this to thread
    ``lamella-refund-of: "<original-lamella-txn-id>"`` without growing
    a parallel writer surface for one new key."""
    amt = Decimal(amount)
    if modified_at is None:
        modified_at = datetime.now(UTC)
    lines = [
        f'\n{txn_date.isoformat()} * "{_esc(narration)}" #lamella-override\n',
        f'  lamella-override-of: "{txn_hash}"\n',
        f'  lamella-modified-at: "{_render_modified_at(modified_at)}"\n',
    ]
    for k, v in (extra_meta or {}).items():
        lines.append(f'  {k}: "{_esc(str(v))}"\n')
    lines.append(f"  {from_account}  {(-amt):.2f} {currency}\n")
    lines.append(f"  {to_account}  {amt:.2f} {currency}\n")
    return "".join(lines)


def _intercompany_override_block(
    *,
    txn_date: date,
    txn_hash: str,
    paying_entity: str,
    owning_entity: str,
    card_account: str,
    expense_account: str,
    amount: Decimal,
    modified_at: datetime | None = None,
    currency: str = "USD",
    narration: str = "wrong-card intercompany correction",
) -> str:
    """Phase G5 four-leg block per LEDGER_LAYOUT §6.5.

    Structure:

    * Card side (paying entity): -amount — the account that was
      physically charged, preserved.
    * DueFrom (paying entity): +amount — the receivable owed back.
    * Expense (owning entity): +amount — the real expense.
    * DueTo (owning entity): -amount — the payable owed to paying.

    Globally zero-sum; each entity internally balances (card + DF
    for paying = 0; exp + DT for owning = 0). Intercompany
    reports read DueFrom / DueTo to produce who-owes-whom
    balances.
    """
    amt = Decimal(amount)
    abs_amt = amt.copy_abs()
    if modified_at is None:
        modified_at = datetime.now(UTC)
    due_from = f"Assets:{paying_entity}:DueFrom:{owning_entity}"
    due_to = f"Liabilities:{owning_entity}:DueTo:{paying_entity}"
    lines = [
        f'\n{txn_date.isoformat()} * "{_esc(narration)}" #lamella-override #lamella-intercompany\n',
        f'  lamella-override-of:     "{txn_hash}"\n',
        f'  lamella-modified-at:     "{_render_modified_at(modified_at)}"\n',
        f"  lamella-intercompany:    TRUE\n",
        f'  lamella-paying-entity:   "{paying_entity}"\n',
        f'  lamella-owning-entity:   "{owning_entity}"\n',
        f"  {card_account}  {(-abs_amt):.2f} {currency}\n",
        f"  {due_from}  {abs_amt:.2f} {currency}\n",
        f"  {expense_account}  {abs_amt:.2f} {currency}\n",
        f"  {due_to}  {(-abs_amt):.2f} {currency}\n",
    ]
    return "".join(lines)


def _transfer_pair_leg_block(
    *,
    txn_date: date,
    txn_hash: str,
    amount: Decimal,
    from_account: str,
    clearing_account: str,
    pair_id: str,
    partner_hash: str,
    modified_at: datetime | None = None,
    currency: str = "USD",
    narration: str = "transfer pair leg",
) -> str:
    """One side of a cross-date transfer pair. The amount moves from
    the FIXME leg to the clearing account so the two legs net to zero
    on the clearing account once both pair legs are written.

    Carries ``lamella-transfer-pair-id`` + ``lamella-transfer-partner-of`` so
    a reader can re-derive the pairing (and the undo flow can find
    both halves)."""
    amt = Decimal(amount)
    if modified_at is None:
        modified_at = datetime.now(UTC)
    return (
        f'\n{txn_date.isoformat()} * "{_esc(narration)}" #lamella-override #lamella-transfer-pair\n'
        f'  lamella-override-of:        "{txn_hash}"\n'
        f'  lamella-modified-at:        "{_render_modified_at(modified_at)}"\n'
        f'  lamella-transfer-pair-id:   "{pair_id}"\n'
        f'  lamella-transfer-partner-of: "{partner_hash}"\n'
        f"  {from_account}  {(-amt):.2f} {currency}\n"
        f"  {clearing_account}  {amt:.2f} {currency}\n"
    )


def _split_override_block(
    *,
    txn_date: date,
    txn_hash: str,
    from_account: str,
    splits: list[tuple[str, Decimal]],
    modified_at: datetime | None = None,
    currency: str = "USD",
    narration: str = "FIXME override (split)",
    extra_meta: dict | None = None,
) -> str:
    """Build a block with N target legs. Splits must sum to a nonzero
    total; the from_account leg offsets the sum.

    `extra_meta` is an optional dict of ``lamella-*`` keys stamped onto
    the transaction after lamella-override-of / lamella-modified-at. Values
    are converted with ``str(...)`` and then wrapped in quotes — so
    pass primitive types (str / Decimal / bool / date). WP6 uses
    this for the lamella-loan-autoclass-* tier/overflow/decision-id
    meta.
    """
    if modified_at is None:
        modified_at = datetime.now(UTC)
    total = sum((amt for _, amt in splits), Decimal("0"))
    lines = [
        f'\n{txn_date.isoformat()} * "{_esc(narration)}" #lamella-override\n',
        f'  lamella-override-of: "{txn_hash}"\n',
        f'  lamella-modified-at: "{_render_modified_at(modified_at)}"\n',
    ]
    for k, v in (extra_meta or {}).items():
        # Escape quotes in values so servicer strings don't break the
        # block the same way they break narrations (see b380b42).
        lines.append(f'  {k}: "{_esc(str(v))}"\n')
    lines.append(f"  {from_account}  {(-total):.2f} {currency}\n")
    for acct, amt in splits:
        lines.append(f"  {acct}  {Decimal(amt):.2f} {currency}\n")
    return "".join(lines)


def _strip_override_blocks_by_hash_in_text(
    contents: str, target_hash: str,
) -> tuple[str, int]:
    """Pure-string version of the strip-by-hash logic. Returns
    ``(new_contents, removed_count)``. Shared between the on-disk
    helper below and the in-memory batch writer so both code paths
    share a single block-parsing implementation.
    """
    needles = (
        f'lamella-override-of: "{target_hash}"',
        f'override-of: "{target_hash}"',
        # The pair-leg block uses a wider indent on the key for
        # legibility — search both shapes.
        f'lamella-override-of:        "{target_hash}"',
        f'lamella-override-of:     "{target_hash}"',
    )
    blocks: list[str] = []
    removed = 0
    lines = contents.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(r"^\d{4}-\d{2}-\d{2}\s+\*", line):
            block_lines = [line]
            i += 1
            while i < len(lines) and (
                lines[i].startswith("  ") or lines[i].strip() == ""
            ):
                if re.match(r"^\d{4}-\d{2}-\d{2}\s+\*", lines[i]):
                    break
                block_lines.append(lines[i])
                i += 1
            block_text = "".join(block_lines)
            if any(n in block_text for n in needles):
                removed += 1
                continue
            blocks.append(block_text.rstrip("\n") + "\n")
        else:
            blocks.append(line)
            i += 1
    return "".join(blocks), removed


def _strip_override_blocks_by_hash(overrides_path: Path, target_hash: str) -> int:
    """Remove every override block whose ``lamella-override-of`` (or legacy
    ``override-of``) matches ``target_hash`` from the overrides file.
    Returns the number of blocks removed.

    Shared by ``OverrideWriter.append``/``append_split`` (idempotent
    re-submit protection) and ``rewrite_without_hash`` (undo flow).
    Does NOT run bean-check — callers are responsible, since the
    natural next step is usually an append or a no-op.
    """
    if not overrides_path.exists():
        return 0
    contents = overrides_path.read_text(encoding="utf-8")
    new_contents, removed = _strip_override_blocks_by_hash_in_text(
        contents, target_hash,
    )
    if removed:
        safe = validate_safe_path(
            overrides_path, allowed_roots=[overrides_path.parent]
        )
        safe.write_text(new_contents, encoding="utf-8")
    return removed


class OverrideWriter:
    """Append zero-sum corrections to `connector_overrides.bean`.

    Each override moves the FIXME amount off `Expenses:FIXME` (or whichever
    leaf-FIXME account the original txn posted to) and onto the target
    account the user chose. `bean-check` runs after every write; on failure
    the append is reverted.

    When constructed with a SQLite ``conn``, every successful write also
    upserts ``txn_classification_modified`` so the calendar's
    dirty-since-reviewed query sees the change in the same request. When
    ``conn`` is ``None`` (tests, ad-hoc tooling), only the ledger is
    touched — the cache self-heals on next boot via
    ``calendar.classification_modified.rebuild_from_entries``.
    """

    def __init__(
        self,
        *,
        main_bean: Path,
        overrides: Path,
        run_check: bool = True,
        conn: sqlite3.Connection | None = None,
    ):
        # ADR-0030: validate both paths land inside the ledger directory
        # before any write call captures them. Both files live alongside
        # main.bean so the ledger dir is the natural allowed root.
        ledger_dir = main_bean.parent
        self.main_bean = validate_safe_path(
            main_bean, allowed_roots=[ledger_dir]
        )
        self.overrides = validate_safe_path(
            overrides, allowed_roots=[ledger_dir]
        )
        self.run_check = run_check
        self.conn = conn

    def _bump_cache(self, *, txn_hash: str, txn_date: date, modified_at: datetime) -> None:
        if self.conn is None:
            return
        try:
            from lamella.features.calendar.classification_modified import bump
            bump(
                self.conn,
                txn_hash=txn_hash,
                txn_date=txn_date,
                modified_at=modified_at,
            )
        except Exception as exc:  # noqa: BLE001
            # Ledger write already succeeded; missing cache row recovers at
            # next boot. Never fail the override on a cache hiccup.
            log.warning("txn_classification_modified bump failed: %s", exc)

    def append(
        self,
        *,
        txn_date: date,
        txn_hash: str,
        amount: Decimal,
        from_account: str,
        to_account: str,
        currency: str = "USD",
        narration: str = "FIXME override",
        replace_existing: bool = True,
        extra_meta: dict | None = None,
    ) -> str:
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        backup_main = self.main_bean.read_bytes()
        overrides_existed = self.overrides.exists()
        backup_overrides = self.overrides.read_bytes() if overrides_existed else None

        _, baseline_output = capture_bean_check(self.main_bean) if self.run_check else (0, "")

        ensure_overrides_exists(self.overrides)
        ensure_include_in_main(self.main_bean, self.overrides)

        # Idempotency: wipe any pre-existing override blocks for this
        # same txn_hash before appending. Without this, re-submitting
        # the same form stacks a new override on top of the old one —
        # every subsequent bean-check pass counts both, doubling the
        # posting on the target account. Snapshot-restore below puts
        # the ledger back on any failure.
        if replace_existing and self.overrides.exists():
            _strip_override_blocks_by_hash(self.overrides, txn_hash)

        modified_at = datetime.now(UTC)
        block = _override_block(
            txn_date=txn_date,
            txn_hash=txn_hash,
            amount=amount,
            from_account=from_account,
            to_account=to_account,
            modified_at=modified_at,
            currency=currency,
            narration=narration,
            extra_meta=extra_meta,
        )
        with self.overrides.open("a", encoding="utf-8") as fh:
            fh.write(block)

        if self.run_check:
            try:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
            except BeanCheckError:
                self.main_bean.write_bytes(backup_main)
                if backup_overrides is None:
                    self.overrides.unlink(missing_ok=True)
                else:
                    self.overrides.write_bytes(backup_overrides)
                raise

        self._bump_cache(
            txn_hash=txn_hash,
            txn_date=txn_date,
            modified_at=modified_at,
        )
        return block

    def append_batch(
        self,
        rows: list[dict],
        *,
        on_row_staged=None,
        replace_existing: bool = True,
    ) -> tuple[int, list[str], list[str]]:
        """Append N override blocks under a SINGLE backup → bean-check →
        revert envelope. ~100x cheaper than calling ``append`` in a
        loop because the slow part (``bean-check``) runs once instead
        of once per row.

        ``rows`` is a list of dicts with keys
        ``{txn_date (date), txn_hash, amount (Decimal), from_account,
        to_account, currency, narration}``. Per-row narration falls
        back to ``"FIXME override"``.

        ``on_row_staged(idx, total, row)`` is called as each block is
        rendered into the in-memory buffer — fast (~µs per call) so
        the modal sees rapid progress before the single trailing
        bean-check.

        Returns ``(applied_count, applied_blocks, skipped_reasons)``.
        ``skipped_reasons`` is currently always empty — every passed
        row goes into the batch — but reserved so callers don't need
        to change shape if pre-write validation moves in here.

        On bean-check failure both files are restored byte-identical
        from the in-memory backup and ``BeanCheckError`` is raised —
        the caller MUST treat the whole batch as not applied. Cache
        bumps only fire after the bean-check passes, so a failed
        batch leaves no SQLite drift either.
        """
        if not rows:
            return 0, [], []
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        backup_main = self.main_bean.read_bytes()
        overrides_existed = self.overrides.exists()
        backup_overrides = self.overrides.read_bytes() if overrides_existed else None

        _, baseline_output = (
            capture_bean_check(self.main_bean) if self.run_check else (0, "")
        )

        ensure_overrides_exists(self.overrides)
        ensure_include_in_main(self.main_bean, self.overrides)

        overrides_text = self.overrides.read_text(encoding="utf-8")

        if replace_existing:
            for row in rows:
                overrides_text, _ = _strip_override_blocks_by_hash_in_text(
                    overrides_text, row["txn_hash"],
                )

        total = len(rows)
        cache_bumps: list[tuple[str, date, datetime]] = []
        rendered_blocks: list[str] = []
        for idx, row in enumerate(rows):
            modified_at = datetime.now(UTC)
            block = _override_block(
                txn_date=row["txn_date"],
                txn_hash=row["txn_hash"],
                amount=Decimal(row["amount"]),
                from_account=row["from_account"],
                to_account=row["to_account"],
                modified_at=modified_at,
                currency=row.get("currency") or "USD",
                narration=row.get("narration") or "FIXME override",
            )
            rendered_blocks.append(block)
            cache_bumps.append((row["txn_hash"], row["txn_date"], modified_at))
            if on_row_staged is not None:
                on_row_staged(idx, total, row)

        new_overrides = overrides_text + "".join(rendered_blocks)

        try:
            self.overrides.write_text(new_overrides, encoding="utf-8")
            if self.run_check:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
        except BeanCheckError:
            self.main_bean.write_bytes(backup_main)
            if backup_overrides is None:
                self.overrides.unlink(missing_ok=True)
            else:
                self.overrides.write_bytes(backup_overrides)
            raise
        except Exception:
            self.main_bean.write_bytes(backup_main)
            if backup_overrides is None:
                self.overrides.unlink(missing_ok=True)
            else:
                self.overrides.write_bytes(backup_overrides)
            raise

        for txn_hash, txn_date, modified_at in cache_bumps:
            self._bump_cache(
                txn_hash=txn_hash,
                txn_date=txn_date,
                modified_at=modified_at,
            )
        return len(rows), rendered_blocks, []

    def append_intercompany(
        self,
        *,
        txn_date: date,
        txn_hash: str,
        paying_entity: str,
        owning_entity: str,
        card_account: str,
        expense_account: str,
        amount: Decimal,
        currency: str = "USD",
        narration: str = "wrong-card intercompany correction",
    ) -> str:
        """Phase G5 — append a four-leg intercompany override.

        Produces the canonical shape specified in
        ``docs/specs/LEDGER_LAYOUT.md`` §6.5: card side stays on the
        charging entity, a receivable lands on the paying entity,
        the real expense lands on the owning entity, and a payable
        lands on the owning entity. Net-zero globally; each entity
        internally balances.

        ``Assets:<PayingEntity>:DueFrom:<OwingEntity>`` and
        ``Liabilities:<OwingEntity>:DueTo:<PayingEntity>`` must
        already be open in the ledger — callers that work from the
        review UI scaffold them via ``AccountsWriter.write_opens``
        before invoking this method. If the accounts aren't open,
        bean-check will reject the write and the snapshot rollback
        restores the pre-call state byte-for-byte.

        ``paying_entity`` must match the second path segment of
        ``card_account`` (the card belongs to the paying entity).
        ``owning_entity`` must match the second path segment of
        ``expense_account`` (the expense belongs to the owning
        entity). The caller is responsible for that invariant;
        this writer doesn't re-derive it.
        """
        if paying_entity == owning_entity:
            raise WriteError(
                "paying_entity equals owning_entity — this isn't "
                "intercompany, use append() instead"
            )
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        backup_main = self.main_bean.read_bytes()
        overrides_existed = self.overrides.exists()
        backup_overrides = self.overrides.read_bytes() if overrides_existed else None

        _, baseline_output = capture_bean_check(self.main_bean) if self.run_check else (0, "")

        ensure_overrides_exists(self.overrides)
        ensure_include_in_main(self.main_bean, self.overrides)

        modified_at = datetime.now(UTC)
        block = _intercompany_override_block(
            txn_date=txn_date,
            txn_hash=txn_hash,
            paying_entity=paying_entity,
            owning_entity=owning_entity,
            card_account=card_account,
            expense_account=expense_account,
            amount=amount,
            modified_at=modified_at,
            currency=currency,
            narration=narration,
        )
        with self.overrides.open("a", encoding="utf-8") as fh:
            fh.write(block)

        if self.run_check:
            try:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
            except BeanCheckError:
                self.main_bean.write_bytes(backup_main)
                if backup_overrides is None:
                    self.overrides.unlink(missing_ok=True)
                else:
                    self.overrides.write_bytes(backup_overrides)
                raise

        self._bump_cache(
            txn_hash=txn_hash,
            txn_date=txn_date,
            modified_at=modified_at,
        )
        return block

    def append_transfer_pair_leg(
        self,
        *,
        txn_date: date,
        txn_hash: str,
        amount: Decimal,
        from_account: str,
        clearing_account: str,
        pair_id: str,
        partner_hash: str,
        currency: str = "USD",
        narration: str = "transfer pair leg",
    ) -> str:
        """Write one side of a cross-date transfer pair, routing the
        FIXME leg to ``clearing_account``. Caller is responsible for
        writing the partner side with the same ``pair_id`` (and
        ``partner_hash`` crossed) so the clearing account nets to
        zero once both legs land.
        """
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        backup_main = self.main_bean.read_bytes()
        overrides_existed = self.overrides.exists()
        backup_overrides = self.overrides.read_bytes() if overrides_existed else None

        _, baseline_output = capture_bean_check(self.main_bean) if self.run_check else (0, "")

        ensure_overrides_exists(self.overrides)
        ensure_include_in_main(self.main_bean, self.overrides)

        modified_at = datetime.now(UTC)
        block = _transfer_pair_leg_block(
            txn_date=txn_date,
            txn_hash=txn_hash,
            amount=amount,
            from_account=from_account,
            clearing_account=clearing_account,
            pair_id=pair_id,
            partner_hash=partner_hash,
            modified_at=modified_at,
            currency=currency,
            narration=narration,
        )
        with self.overrides.open("a", encoding="utf-8") as fh:
            fh.write(block)

        if self.run_check:
            try:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
            except BeanCheckError:
                self.main_bean.write_bytes(backup_main)
                if backup_overrides is None:
                    self.overrides.unlink(missing_ok=True)
                else:
                    self.overrides.write_bytes(backup_overrides)
                raise

        self._bump_cache(
            txn_hash=txn_hash,
            txn_date=txn_date,
            modified_at=modified_at,
        )
        return block

    def append_split(
        self,
        *,
        txn_date: date,
        txn_hash: str,
        from_account: str,
        splits: list[tuple[str, Decimal]],
        currency: str = "USD",
        narration: str = "FIXME override (split)",
        replace_existing: bool = True,
        extra_meta: dict | None = None,
    ) -> str:
        """Append a multi-leg override. `splits` is a list of
        (target_account, amount) pairs that sum to the original amount.

        ``replace_existing`` (default True) wipes any pre-existing
        override block for this ``txn_hash`` before appending. This
        is critical for handlers like the mortgage record-payment
        form — without it, each re-submit stacks a new override,
        doubling (or tripling) the posting on the target account
        even though the UI acts like the operation is idempotent.

        ``extra_meta`` is passed through to ``_split_override_block``
        to stamp additional lamella-* keys on the override (e.g., the
        WP6 lamella-loan-autoclass-tier / overflow / decision-id meta).
        """
        if not splits:
            raise WriteError("splits cannot be empty")
        if not self.main_bean.exists():
            raise WriteError(f"main.bean not found at {self.main_bean}")

        backup_main = self.main_bean.read_bytes()
        overrides_existed = self.overrides.exists()
        backup_overrides = self.overrides.read_bytes() if overrides_existed else None

        _, baseline_output = capture_bean_check(self.main_bean) if self.run_check else (0, "")

        ensure_overrides_exists(self.overrides)
        ensure_include_in_main(self.main_bean, self.overrides)

        if replace_existing and self.overrides.exists():
            _strip_override_blocks_by_hash(self.overrides, txn_hash)

        modified_at = datetime.now(UTC)
        block = _split_override_block(
            txn_date=txn_date,
            txn_hash=txn_hash,
            from_account=from_account,
            splits=splits,
            modified_at=modified_at,
            currency=currency,
            narration=narration,
            extra_meta=extra_meta,
        )
        with self.overrides.open("a", encoding="utf-8") as fh:
            fh.write(block)

        if self.run_check:
            try:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
            except BeanCheckError:
                self.main_bean.write_bytes(backup_main)
                if backup_overrides is None:
                    self.overrides.unlink(missing_ok=True)
                else:
                    self.overrides.write_bytes(backup_overrides)
                raise

        self._bump_cache(
            txn_hash=txn_hash,
            txn_date=txn_date,
            modified_at=modified_at,
        )
        return block

    def rewrite_without_hash(self, txn_hash: str) -> int:
        """Remove every override block for ``txn_hash`` from the
        overrides file. Used by the undo flow. Runs bean-check and
        reverts on failure; returns the number of blocks removed.

        Shares its block-parsing logic with
        :func:`_strip_override_blocks_by_hash` so the wider-indent
        metadata variants (``lamella-override-of:     "…"`` from the
        intercompany + transfer-pair writers) match cleanly."""
        if not self.overrides.exists():
            return 0
        backup_main = self.main_bean.read_bytes()
        backup_overrides = self.overrides.read_bytes()
        _, baseline_output = capture_bean_check(self.main_bean) if self.run_check else (0, "")
        try:
            removed = _strip_override_blocks_by_hash(self.overrides, txn_hash)
            if self.run_check:
                run_bean_check_vs_baseline(self.main_bean, baseline_output)
        except BeanCheckError:
            self.main_bean.write_bytes(backup_main)
            self.overrides.write_bytes(backup_overrides)
            raise
        return removed
