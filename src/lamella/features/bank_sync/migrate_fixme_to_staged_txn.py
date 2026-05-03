# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0043 P6 — bulk-rewrite legacy FIXME postings to ``custom
"staged-txn"`` directives.

Pre-C1 versions of Lamella wrote a balanced txn with a FIXME leg
whenever an unclassified bank row landed. Post-C1, the same row is
deferred — no ledger write — until the user classifies it. ADR-0043
formalizes that the deferred state lives as a metadata-only ``custom
"staged-txn"`` directive instead of as a FIXME leg. This module
takes existing FIXME-leg transactions and converts them.

Eligibility (per migration plan §5):

  A txn is ELIGIBLE for migration iff:
    * It has a posting whose account leaf is ``FIXME``.
    * It has at least one posting with a paired
      ``lamella-source-N`` + ``lamella-source-reference-id-N`` meta
      whose source value is in STAGING_SOURCE_NAMES (i.e. it
      genuinely came from one of the four ingest paths).
    * It is NOT a loan / intercompany construction (loan groups
      have a non-FIXME structural target — those FIXME legs are
      replaced inline by the loan code path, not rewritten here).

Output:
    For each eligible txn, the entire balanced transaction is replaced
    in-place with a ``custom "staged-txn"`` directive carrying the
    same ``lamella-txn-id`` lineage. The user re-classifies the row
    via /inbox.

Failure mode:
    Snapshot every connector-owned ``.bean`` file before any edit.
    On any per-file bean-check failure, restore that file from
    snapshot and skip its remaining txns; continue with the next
    file. Per ADR-0036 the migration must not be all-or-nothing —
    a single corrupt file shouldn't block migrating the others.

Non-goals (deferred):
    * Loan-claim FIXME-leg replacement (loan-classify code path
      handles those inline).
    * Bare ``Expenses:FIXME`` (no entity prefix) findings — those
      are flagged separately by the recovery system per ADR-0043
      Legacy compatibility section.
"""
from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from lamella.core.ledger_writer import BeanCheckError, run_bean_check

log = logging.getLogger(__name__)


# Account-leaf check (handles both `Expenses:FIXME` and entity-prefixed
# `Expenses:Acme:FIXME`). Mirrors features/rules/scanner.py:_is_fixme.
def _is_fixme_account(account: str) -> bool:
    return account.split(":")[-1].upper() == "FIXME"


@dataclass
class MigrationReport:
    """Returned by ``migrate_fixme_to_staged_txn``. ``dry_run=True``
    populates ``would_migrate`` without touching disk."""

    files_scanned: int = 0
    files_modified: int = 0
    txns_migrated: int = 0
    txns_skipped_loan_pattern: int = 0
    txns_skipped_no_source: int = 0
    bean_check_failures: list[str] = field(default_factory=list)
    snapshot_dir: Path | None = None
    dry_run: bool = True

    @property
    def would_migrate(self) -> int:
        """Convenience alias used by dry-run callers."""
        return self.txns_migrated


# --- Eligibility detection (works on raw ledger entries) -------------------

def _eligible_txns(entries) -> list:
    """Walk parsed beancount entries and return the Transaction
    instances that are ELIGIBLE per the rules above. Pure read."""
    from beancount.core.data import Transaction
    out = []
    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        # Has a FIXME leg?
        fixme_legs = [
            p for p in entry.postings
            if _is_fixme_account(p.account)
        ]
        if not fixme_legs:
            continue
        # Loan-pattern check: if any non-FIXME posting has an
        # `Assets:` or `Liabilities:` account that isn't the bank
        # source AND there's loan-related meta on the txn, treat
        # as a loan construction and skip. We approximate by
        # checking for any `lamella-loan-*` meta on the txn.
        meta = entry.meta or {}
        if any(k.startswith("lamella-loan-") for k in meta.keys()):
            continue
        # Has a source-bearing posting? Look for paired indexed
        # source meta on any posting.
        has_source = False
        for posting in entry.postings:
            pmeta = posting.meta or {}
            for k in pmeta.keys():
                if k.startswith("lamella-source-") and not k.startswith(
                    "lamella-source-account"
                ):
                    has_source = True
                    break
            if has_source:
                break
        if not has_source:
            continue
        out.append(entry)
    return out


# --- Per-txn rewrite — text-level so we don't have to round-trip beancount

# Match a balanced-txn block whose first line carries a date + payee/narration
# and whose body contains ``lamella-txn-id: "<the id>"``. Use the txn-id as
# the unique anchor — it's UUIDv7-stable across reads.
_BALANCED_TXN_BLOCK = re.compile(
    r'^\d{4}-\d{2}-\d{2} \*[^\n]*\n'
    r'(?:[ \t]+[^\n]*\n)+',
    re.MULTILINE,
)


def _replace_txn_block_with_directive(
    text: str, *, lamella_txn_id: str, directive_block: str,
) -> tuple[str, bool]:
    """Find the txn block carrying ``lamella-txn-id`` and replace it
    with ``directive_block``. Returns ``(new_text, found)``.

    Pure text op — works against the raw .bean file without
    re-rendering. The lamella-txn-id is unique per txn, so a single
    match is the expected case.
    """
    for m in _BALANCED_TXN_BLOCK.finditer(text):
        block = m.group(0)
        if f'lamella-txn-id: "{lamella_txn_id}"' not in block:
            continue
        # Replace the block (preserve a single trailing newline boundary).
        new_block = directive_block
        if not new_block.endswith("\n"):
            new_block += "\n"
        return text[:m.start()] + new_block + text[m.end():], True
    return text, False


def _build_directive_for_txn(txn) -> str | None:
    """Render a ``custom "staged-txn"`` directive that captures the
    classified fields of a legacy FIXME txn.

    Strategy: pull the bank-side posting's source value from indexed
    meta; the source-account is its account; the amount is its
    posting amount (signed from the bank account's POV — same
    convention as PendingEntry / ADR-0043b §3); the narration is the
    txn's payee + narration; the lamella-txn-id is the txn meta value.
    """
    from lamella.core.identity import (
        REF_KEY,
        SOURCE_KEY,
        STAGING_SOURCE_NAMES,
        TXN_ID_KEY,
    )
    meta = txn.meta or {}
    txn_id = meta.get(TXN_ID_KEY)
    if not txn_id:
        return None
    # Pick the bank-side posting: the one with paired indexed source meta.
    bank_posting = None
    source_value = None
    source_ref_id = None
    for posting in txn.postings:
        pmeta = posting.meta or {}
        # Walk indices 0..N
        for i in range(20):  # generous cap; real txns have 1-2 sources
            src = pmeta.get(f"{SOURCE_KEY}-{i}")
            ref = pmeta.get(f"{REF_KEY}-{i}")
            if src and ref:
                source_value = str(src)
                source_ref_id = str(ref)
                bank_posting = posting
                break
        if bank_posting is not None:
            break
    if bank_posting is None or not source_value or not source_ref_id:
        return None
    if source_value not in STAGING_SOURCE_NAMES:
        return None
    # Amount on the bank posting is signed from its POV. Render as a
    # bare beancount Amount.
    units = bank_posting.units
    if units is None or units.number is None:
        return None
    amount = Decimal(units.number)
    currency = units.currency or "USD"
    payee = (txn.payee or "").replace("\\", "\\\\").replace('"', '\\"')
    narration = (txn.narration or "").replace("\\", "\\\\").replace('"', '\\"')
    narration_combined = (
        f"{payee} - {narration}".strip(" -") if payee else narration
    )
    source_account = bank_posting.account
    lines = [
        f'\n{txn.date.isoformat()} custom "staged-txn" "{source_value}"',
        f'  {TXN_ID_KEY}: "{txn_id}"',
        f'  lamella-source: "{source_value}"',
        f'  lamella-source-reference-id: "{source_ref_id}"',
        f'  lamella-txn-date: {txn.date.isoformat()}',
        f'  lamella-txn-amount: {amount:.2f} {currency}',
        f'  lamella-source-account: "{source_account}"',
        f'  lamella-txn-narration: "{narration_combined}"',
    ]
    return "\n".join(lines) + "\n"


# --- Snapshot-aware bulk migration ----------------------------------------

def _now_iso() -> str:
    # microseconds precision so a second migration call in the same
    # second doesn't collide on the snapshot dirname (matters in tests
    # and in retry-on-failure flows).
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def migrate_fixme_to_staged_txn(
    *,
    ledger_dir: Path,
    connector_files: list[Path],
    main_bean: Path,
    dry_run: bool = True,
) -> MigrationReport:
    """Walk every file in ``connector_files``, find eligible FIXME txns,
    and rewrite them as ``custom "staged-txn"`` directives.

    ``dry_run=True`` returns a count without touching disk.
    ``dry_run=False`` snapshots ``ledger_dir`` to
    ``ledger_dir.parent/.pre-migrate-0043-<ts>/``, applies, runs
    bean-check, and rolls back any failing file.

    Returns a ``MigrationReport`` summarizing the outcome.
    """
    from beancount import loader

    report = MigrationReport(dry_run=dry_run)

    # --- Snapshot (only on apply) ---
    if not dry_run:
        ts = _now_iso().replace(":", "-")
        snapshot = ledger_dir.parent / f".pre-migrate-0043-{ts}"
        snapshot.mkdir(parents=True, exist_ok=False)
        for cf in connector_files:
            if cf.exists():
                shutil.copy2(cf, snapshot / cf.name)
        report.snapshot_dir = snapshot
        log.info("0043 migration snapshot: %s", snapshot)

    # Parse the full ledger once (via main.bean which `include`s every
    # connector file). Each entry carries source info indicating which
    # file it came from, so we filter eligible txns to the connector
    # file we're processing — but parse + bean-check the whole graph.
    ledger_entries, ledger_errors, _ = loader.load_file(str(main_bean))
    if ledger_errors:
        # Tolerate pre-existing baseline errors. The ones we care about
        # are post-rewrite errors caught by run_bean_check below.
        log.info(
            "0043 migration: ledger has %d pre-existing parse warning(s) — "
            "tolerated",
            len(ledger_errors),
        )

    # --- Per-file work ---
    for connector_file in connector_files:
        if not connector_file.exists():
            continue
        report.files_scanned += 1

        # Filter ledger entries to the ones whose source is this
        # connector file. beancount entries carry their source filename
        # in entry.meta["filename"].
        cf_str = str(connector_file)
        cf_entries = [
            e for e in ledger_entries
            if (e.meta or {}).get("filename") == cf_str
        ]
        if not cf_entries:
            continue

        eligible = _eligible_txns(cf_entries)
        if not eligible:
            continue

        if dry_run:
            report.txns_migrated += len(eligible)
            continue

        # Apply: read the file's bytes, replace each txn block, write
        # back, then bean-check the parent main.bean. On bean-check
        # failure, restore from snapshot and stop processing this file.
        original_text = connector_file.read_text(encoding="utf-8")
        new_text = original_text
        per_file_count = 0
        for txn in eligible:
            txn_id = (txn.meta or {}).get("lamella-txn-id")
            if not txn_id:
                continue
            directive = _build_directive_for_txn(txn)
            if directive is None:
                report.txns_skipped_no_source += 1
                continue
            new_text, found = _replace_txn_block_with_directive(
                new_text,
                lamella_txn_id=str(txn_id),
                directive_block=directive,
            )
            if found:
                per_file_count += 1
        if per_file_count == 0:
            continue
        connector_file.write_text(new_text, encoding="utf-8")

        # Bean-check the result. Failure → restore from snapshot.
        try:
            run_bean_check(main_bean)
        except BeanCheckError as exc:
            log.warning(
                "0043 migration: bean-check failed after %s — restoring",
                connector_file.name,
            )
            assert report.snapshot_dir is not None
            shutil.copy2(
                report.snapshot_dir / connector_file.name, connector_file,
            )
            report.bean_check_failures.append(
                f"{connector_file.name}: {exc}"
            )
            continue

        report.files_modified += 1
        report.txns_migrated += per_file_count

    return report


# --- CLI entry point -------------------------------------------------------

def _cli(argv: list[str] | None = None) -> int:
    """``python -m lamella.features.bank_sync.migrate_fixme_to_staged_txn``"""
    import argparse
    parser = argparse.ArgumentParser(
        description=(
            "ADR-0043 P6 — migrate legacy FIXME postings to "
            "custom \"staged-txn\" directives. Default is dry-run; "
            "pass --apply to write."
        )
    )
    parser.add_argument(
        "--ledger-dir", type=Path, required=True,
        help="Path to the ledger directory (parent of main.bean)",
    )
    parser.add_argument(
        "--connector-files", type=Path, nargs="+", default=None,
        help=(
            "Connector-owned .bean files to scan. Defaults to "
            "<ledger-dir>/simplefin_transactions.bean."
        ),
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually write. Default is dry-run.",
    )
    args = parser.parse_args(argv)

    main_bean = args.ledger_dir / "main.bean"
    if not main_bean.exists():
        print(f"main.bean not found at {main_bean}")
        return 2
    cf = args.connector_files or [
        args.ledger_dir / "simplefin_transactions.bean"
    ]

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = migrate_fixme_to_staged_txn(
        ledger_dir=args.ledger_dir,
        connector_files=cf,
        main_bean=main_bean,
        dry_run=not args.apply,
    )
    print(
        f"{'DRY RUN' if report.dry_run else 'APPLIED'}: "
        f"{report.txns_migrated} txn(s) "
        f"across {report.files_scanned} file(s); "
        f"{report.files_modified} file(s) modified; "
        f"{len(report.bean_check_failures)} bean-check failure(s)"
    )
    if report.snapshot_dir:
        print(f"snapshot: {report.snapshot_dir}")
    if report.bean_check_failures:
        for line in report.bean_check_failures:
            print(f"  - {line}")
    return 0 if not report.bean_check_failures else 1


if __name__ == "__main__":
    raise SystemExit(_cli())
