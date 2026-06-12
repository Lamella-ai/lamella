# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Reboot re-ingestion — NEXTGEN.md Phase E.

The user-facing "onboard my existing books" workflow: parse every
transaction from the existing Beancount files, stage each onto the
unified surface with ``source='reboot'``, then run the same
downstream pipeline (matcher, duplicate detector, classifier)
over historical data.

**Non-destructive by default.** Phase E1 ships the ingest half:
historical transactions become visible to the matcher + duplicate
detector without touching any ledger bytes. Phase E2 adds the
file-side workflow:

  1. Write a cleaned copy of each ledger file to ``.reboot/``
     (never the originals).
  2. Show the user a per-file diff.
  3. On explicit Apply, copy originals to
     ``.pre-reboot-<timestamp>/`` backups and move the cleaned
     files into place.
  4. Rollback is available for as long as the backup dir exists.

Phase E3 adds rule mining + AI-assisted reclassification.

The reboot scan is **idempotent**: re-running it upserts each
staged row in place via ``(source, source_ref_hash)``. Orphans
(rows whose source line no longer exists in the ledger) are left
as stale data rather than cleared, so downstream pair records
keep their references.
"""
from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from beancount.core.data import Transaction

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.identity import get_txn_id
from lamella.features.import_.staging.intake import content_fingerprint
from lamella.features.import_.staging.service import StagingService

log = logging.getLogger(__name__)

__all__ = [
    "DuplicateGroup",
    "RebootResult",
    "RebootService",
    "scan_ledger",
    # Typed-envelope extract helpers (ADR-0057 §1) — exposed so the
    # round-trip property test and any future serializer can build
    # against the same shape the service writes.
    "_typed_meta_value",
    "_typed_meta_list",
    "_capture_amount",
    "_capture_cost",
]


@dataclass(frozen=True)
class DuplicateGroup:
    """Two or more staged rows sharing a content fingerprint — the
    signal that the ledger has historical double-imports.

    Reuses the same ``content_fingerprint`` algorithm as Phase D1.1
    intake. One source of truth for "these two records are the same
    real-world transaction" across all intake paths (SimpleFIN, CSV,
    paste, reboot). When the user confirms a group as duplicates in
    review, Phase E2's retrofit pass stamps ``lamella-source-ref`` onto
    the canonical ledger entry and marks the others for cleanup.
    """
    fingerprint: str
    members: tuple[tuple[int, str, str | None, str | None], ...]
    # each member: (staged_id, source, file, lineno)


@dataclass
class RebootResult:
    """Summary of a reboot scan."""
    total_txns: int = 0
    staged: int = 0
    skipped: int = 0
    already_reboot: int = 0            # txns already tagged lamella-source='reboot'
    files_covered: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    duplicate_groups: list[DuplicateGroup] = field(default_factory=list)

    @property
    def duplicates_total(self) -> int:
        """Count of individual rows flagged across all duplicate
        groups — not group count. Useful for "N rows look like
        historical double-imports" summary lines."""
        return sum(len(g.members) for g in self.duplicate_groups)


# --- helpers ------------------------------------------------------------


# Internal Beancount meta keys that aren't user-authored content. The
# parser stuffs `filename`, `lineno`, and dunder keys into entry.meta;
# they're carried as `source_ref` separately and would round-trip wrong
# if re-emitted as user metadata. Keep this in sync with anything
# reader.py / loader plugins inject.
_INTERNAL_META_KEYS = frozenset({"filename", "lineno"})


def _typed_meta_value(v: Any) -> dict[str, Any]:
    """Map a Beancount metadata value onto the round-trip-safe envelope
    described in ADR-0057 §1: ``{"type": <tag>, "value": <jsonable>}``.

    The tag tells the serializer how to re-emit per LEDGER_LAYOUT.md
    §6.3 (booleans bare ``TRUE`` / ``FALSE``, dates bare ``YYYY-MM-DD``,
    amounts bare ``<n> <ccy>``, strings double-quoted, numbers bare).
    Without it, a stage-then-emit cycle can't distinguish the literal
    string ``"TRUE"`` from the boolean ``TRUE``.

    Order matters: ``bool`` is a subclass of ``int`` in Python, so the
    bool check must come first.
    """
    if isinstance(v, bool):
        return {"type": "boolean", "value": v}
    if isinstance(v, int):
        return {"type": "integer", "value": v}
    if isinstance(v, datetime.date):
        # date and datetime — Beancount metadata uses date.
        return {"type": "date", "value": v.isoformat()}
    if isinstance(v, Decimal):
        return {"type": "decimal", "value": str(v)}
    # Beancount Amount is a NamedTuple(number=Decimal, currency=str).
    if (
        hasattr(v, "number")
        and hasattr(v, "currency")
        and hasattr(v, "_fields")
    ):
        return {
            "type": "amount",
            "value": {
                "number": (
                    str(v.number) if v.number is not None else None
                ),
                "currency": v.currency,
            },
        }
    if isinstance(v, str):
        return {"type": "string", "value": v}
    # Fallback: stringify but tag it so the serializer doesn't emit a
    # bare token where it should emit a quoted string. A type the
    # extract layer doesn't recognize is a bug, not a silent coercion;
    # the unknown tag makes that visible to round-trip property tests.
    return {"type": "unknown", "value": str(v), "python_type": type(v).__name__}


def _typed_meta_list(meta: dict | None) -> list[dict[str, Any]]:
    """Convert a Beancount ``entry.meta`` / ``posting.meta`` dict into
    the ordered, typed list form the staging row stores.

    Drops the parser-injected ``filename`` / ``lineno`` keys (they're
    captured separately as ``source_ref``) and dunder keys (Beancount
    internals like ``__tolerances__``). User-authored ``lamella-*`` /
    ``bcg-*`` / arbitrary keys all flow through unchanged.
    """
    if not meta:
        return []
    result = []
    for k, v in meta.items():
        if not isinstance(k, str):
            continue
        if k.startswith("__") or k in _INTERNAL_META_KEYS:
            continue
        envelope = _typed_meta_value(v)
        result.append({"key": k, **envelope})
    return result


def _capture_amount(amt: Any) -> dict[str, Any] | None:
    """Capture a Beancount ``Amount`` (or any object exposing ``number``
    / ``currency``) as a JSON-safe dict. Used for posting prices."""
    if amt is None:
        return None
    number = getattr(amt, "number", None)
    return {
        "number": str(number) if number is not None else None,
        "currency": getattr(amt, "currency", None),
    }


def _capture_cost(cost: Any) -> dict[str, Any] | None:
    """Capture a Beancount ``Cost`` (post-booking) or ``CostSpec``
    (pre-booking) as a JSON-safe dict.

    The two shapes differ — ``Cost`` has ``number`` while ``CostSpec``
    has ``number_per`` / ``number_total`` / ``merge`` — so we walk a
    superset of attributes and capture whatever's present. Keeps the
    extract layer agnostic to whether booking ran.
    """
    if cost is None:
        return None
    out: dict[str, Any] = {}
    for attr in (
        "number",
        "number_per",
        "number_total",
        "currency",
        "date",
        "label",
        "merge",
    ):
        v = getattr(cost, attr, None)
        if v is None:
            continue
        if isinstance(v, datetime.date):
            v = v.isoformat()
        elif isinstance(v, Decimal):
            v = str(v)
        out[attr] = v
    return out


def _first_real_posting(txn: Transaction) -> tuple[str | None, Decimal | None, str]:
    """Pick a representative leg: first posting with a non-null amount.

    Returns (account, amount, currency). Falls back to (None, None, 'USD')
    when no posting has a concrete amount — rare but legal in Beancount
    (pure elidable transactions where the parser infers the residual).
    """
    for p in txn.postings or []:
        if p.units is not None and p.units.number is not None:
            return (
                p.account,
                Decimal(str(p.units.number)),
                p.units.currency or "USD",
            )
    return None, None, "USD"


def _txn_file_and_line(txn: Transaction) -> tuple[str | None, int | None]:
    meta = getattr(txn, "meta", None) or {}
    filename = meta.get("filename")
    lineno = meta.get("lineno")
    if isinstance(filename, str) and filename.startswith("<"):
        # Synthetic entries from plugins (auto_accounts, implicit_prices)
        # — not real lines in a user file. Skip.
        return None, None
    return (
        str(filename) if filename else None,
        int(lineno) if lineno is not None else None,
    )


def _already_reboot_tagged(txn: Transaction) -> bool:
    """True if this transaction was written by a prior reboot apply
    pass (it carries the ``lamella-source`` metadata). Doesn't affect
    re-scans — those just upsert — but future code can use the flag
    to decide whether to re-classify."""
    meta = getattr(txn, "meta", None) or {}
    return str(meta.get("lamella-source", "")).lower() == "reboot"


def _is_already_classified(txn: Transaction) -> bool:
    """True when every posting on the txn has a non-FIXME leaf
    account — the transaction is fully resolved and doesn't need to
    surface on the review queue.

    The reboot scan's original contract ("stage every ledger txn so
    the matcher can see history") imported already-classified rows
    too, which surfaced as "duplicates" alongside the live ledger
    entry. This predicate is the gate the scan now consults to skip
    them. Entries with at least one FIXME leg, or with synthetic-
    counterpart metadata that needs replacement, still get staged.
    """
    postings = txn.postings or ()
    if not postings:
        # Elidable-residual entry; no useful work for the review queue.
        return True
    for p in postings:
        leaf = (p.account or "").split(":")[-1].upper()
        if leaf == "FIXME":
            return False
    return True


# --- service ------------------------------------------------------------


class RebootService:
    """Runs the reboot scan + (eventually) the file-side apply/rollback.

    Phase E1 shipped: ``scan_ledger`` stages every ledger transaction
    onto the unified staging surface. File rewriting lands in E2.
    """

    def __init__(self, conn, staging: StagingService | None = None):
        self.conn = conn
        self.staging = staging or StagingService(conn)

    def scan_ledger(
        self,
        reader: LedgerReader,
        *,
        session_id: str | None = None,
        force_reload: bool = True,
        detect_duplicates: bool = True,
        progress_callback=None,
        include_classified: bool = True,
    ) -> RebootResult:
        """Parse every transaction in the ledger and stage it.

        ``source_ref`` = ``{"file": <path>, "lineno": <int>}`` which
        gives the row a stable identity across re-scans. The scan is
        idempotent: a transaction at the same file+line will upsert
        in place rather than double-insert.

        When ``detect_duplicates`` is True (the default), after the
        stage pass we run a content-fingerprint collision check over
        all staged rows (not just reboot) using the same
        ``content_fingerprint`` algorithm that Phase D1.1 intake uses
        for paste-dup detection. One dedup system, not two.
        """
        if session_id is None:
            import secrets
            session_id = "reboot-" + secrets.token_hex(6)

        result = RebootResult()
        loaded = reader.load(force=force_reload)
        files_seen: set[str] = set()
        total_txns = sum(
            1 for e in loaded.entries if isinstance(e, Transaction)
        )
        if progress_callback is not None:
            try:
                progress_callback("total", total_txns, None)
            except Exception:  # noqa: BLE001
                pass

        for entry in loaded.entries:
            if not isinstance(entry, Transaction):
                continue
            result.total_txns += 1
            # Throttle: callback every 100 txns so a 5k-ledger scan
            # emits ~50 events to the modal, not 5,000.
            if progress_callback is not None and result.total_txns % 100 == 0:
                try:
                    progress_callback("progress", result.total_txns, total_txns)
                except Exception:  # noqa: BLE001
                    pass

            filename, lineno = _txn_file_and_line(entry)
            if filename is None or lineno is None:
                result.skipped += 1
                continue
            files_seen.add(filename)

            # Default behavior: skip already-classified entries so the
            # review queue doesn't surface them as "duplicates" of the
            # live ledger entry. Callers that need historical context
            # for the matcher (rare) can pass include_classified=True.
            if not include_classified and _is_already_classified(entry):
                result.skipped += 1
                continue

            if _already_reboot_tagged(entry):
                # Still upsert — preserves lifecycle idempotency — but
                # count separately so the UI can say "N already on the
                # staging surface from a prior reboot."
                result.already_reboot += 1

            account, amount, currency = _first_real_posting(entry)
            if amount is None:
                # Elidable-residual txn with no concrete leg: nothing
                # useful to land on the matcher's surface.
                result.skipped += 1
                continue

            try:
                # Reuse the existing ledger entry's lamella-txn-id so
                # /txn/{id} resolves to the same URL pre- and
                # post-promotion. v3 guarantees every entry has one;
                # legacy ledgers that reach this code without v3 fall
                # back to a fresh mint via the service default.
                existing_lineage = get_txn_id(entry)
                # ADR-0057 §1 typed envelope: every captured value
                # carries its Beancount type so the re-emit serializer
                # can honor LEDGER_LAYOUT.md §6.3 type rules. Round-trip
                # safe — booleans don't degrade to "TRUE", dates don't
                # degrade to ISO strings, paired source meta on the
                # bank-side posting doesn't get dropped on re-emit.
                # Forward-fix: stamp ``account_path`` directly into
                # source_ref when the first real posting points at an
                # Assets:/Liabilities: leg. The staging classify path
                # falls through to a generic ``account_path`` resolver
                # branch, so newly-staged reboot rows resolve without
                # needing the raw envelope. Older rows still resolve
                # via the ``raw["representative_account"]`` fallback in
                # ``_resolve_account_path``.
                _source_ref: dict[str, object] = {
                    "file": filename,
                    "lineno": lineno,
                }
                if isinstance(account, str) and account.startswith(
                    ("Assets:", "Liabilities:"),
                ):
                    _source_ref["account_path"] = account
                self.staging.stage(
                    source="reboot",
                    source_ref=_source_ref,
                    session_id=session_id,
                    posting_date=entry.date.isoformat(),
                    amount=amount,
                    currency=currency,
                    payee=entry.payee,
                    description=entry.narration,
                    lamella_txn_id=existing_lineage,
                    raw={
                        "flag": entry.flag,
                        "tags": (
                            sorted(entry.tags) if entry.tags else []
                        ),
                        "links": (
                            sorted(entry.links) if entry.links else []
                        ),
                        "txn_meta": _typed_meta_list(entry.meta),
                        "representative_account": account,
                        "leg_count": len(entry.postings or []),
                        "postings": [
                            {
                                "account": p.account,
                                "amount": (
                                    str(p.units.number)
                                    if p.units and p.units.number is not None
                                    else None
                                ),
                                "currency": (
                                    p.units.currency
                                    if p.units else None
                                ),
                                "cost": _capture_cost(p.cost),
                                "price": _capture_amount(p.price),
                                "flag": p.flag,
                                "meta": _typed_meta_list(p.meta),
                            }
                            for p in (entry.postings or [])
                        ],
                    },
                )
                result.staged += 1
            except Exception as exc:  # noqa: BLE001
                result.errors.append(
                    f"{filename}:{lineno}: {type(exc).__name__}: {exc}"
                )
                result.skipped += 1

        result.files_covered = sorted(files_seen)
        if detect_duplicates:
            result.duplicate_groups = self._find_duplicate_groups()
        log.info(
            "reboot scan session=%s: total=%d staged=%d skipped=%d "
            "already_reboot=%d files=%d dup_groups=%d dup_rows=%d",
            session_id, result.total_txns, result.staged,
            result.skipped, result.already_reboot,
            len(result.files_covered), len(result.duplicate_groups),
            result.duplicates_total,
        )
        return result

    def _find_duplicate_groups(self) -> list[DuplicateGroup]:
        """Scan the entire staging surface for rows sharing a content
        fingerprint. Reuses ``content_fingerprint`` (Phase D1.1) so
        the algorithm is identical across all intake paths — one dedup
        system for reboot-vs-reboot (historical double-imports),
        reboot-vs-simplefin (ledger had the SimpleFIN row plus a
        manually-written copy), and every other cross-source pair.

        A group is only reported when it has ≥ 2 members. Single-row
        "groups" aren't duplicates.

        Cross-source false-positive filter: a SimpleFIN row that was
        imported, classified, and wrote a ledger entry will collide
        with the reboot scan's re-observation of that same ledger
        entry — same date / amount / description by construction.
        That's the SAME event seen twice through two intake paths,
        not a duplicate. Drop groups that are purely
        ``{N promoted non-reboot rows} + {1 reboot row}`` because the
        reboot row is just the scanner re-observing what the other
        source already produced. Real duplicate signals — multiple
        rows from the same source (paste-dup), or multiple still-pending
        rows needing user action — are preserved.
        """
        from decimal import InvalidOperation
        # Exclude dismissed rows: once retrofitted, a row has done its
        # job — the authoritative identity is now on the ledger line
        # via lamella-source-ref. Keeping dismissed rows in the group scan
        # would cause retrofitted groups to keep showing up, which
        # defeats the "resolve this once, forever" contract.
        rows = self.conn.execute(
            "SELECT id, source, source_ref, status, posting_date, amount, "
            "description FROM staged_transactions "
            "WHERE status != 'dismissed'"
        ).fetchall()
        by_fp: dict[str, list[dict]] = {}
        for r in rows:
            try:
                amt = Decimal(r["amount"])
            except (InvalidOperation, ValueError):
                continue
            fp = content_fingerprint(
                posting_date=r["posting_date"],
                amount=amt,
                description=r["description"],
            )
            ref = json.loads(r["source_ref"]) if r["source_ref"] else {}
            file = ref.get("file") if isinstance(ref, dict) else None
            lineno = ref.get("lineno") if isinstance(ref, dict) else None
            by_fp.setdefault(fp, []).append({
                "id": int(r["id"]),
                "source": r["source"],
                "status": r["status"],
                "file": file,
                "lineno": int(lineno) if lineno else None,
            })
        groups: list[DuplicateGroup] = []
        for fp, members in by_fp.items():
            if len(members) < 2:
                continue
            reboot_rows = [m for m in members if m["source"] == "reboot"]
            other_rows = [m for m in members if m["source"] != "reboot"]
            # Cross-source false positive: 1 reboot + ≥1 promoted non-reboot
            # rows, with NO other reboot members. This is the integrity
            # scan re-observing a ledger entry that the other source
            # produced — same event, two intake paths.
            if (
                len(reboot_rows) == 1
                and other_rows
                and all(m["status"] == "promoted" for m in other_rows)
            ):
                continue
            groups.append(DuplicateGroup(
                fingerprint=fp,
                members=tuple(
                    (m["id"], m["source"], m["file"], m["lineno"])
                    for m in members
                ),
            ))
        # Stable ordering: groups by size desc, then fingerprint.
        groups.sort(key=lambda g: (-len(g.members), g.fingerprint))
        return groups


def scan_ledger(
    conn,
    reader: LedgerReader,
    *,
    session_id: str | None = None,
) -> RebootResult:
    """Functional wrapper for callers that don't want the service."""
    return RebootService(conn).scan_ledger(reader, session_id=session_id)
