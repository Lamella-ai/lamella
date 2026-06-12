# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Emit connector_imports/<year>.bean from the importer pipeline.

Adapted from importer_bundle/importers/emit_bean.py. Key differences:

  * Output goes to `${LEDGER_DIR}/connector_imports/` (Connector-owned),
    NOT Cowork's `historical/`. The `connector_imports/_all.bean` is an
    include-of-includes referenced by a one-time `include` line in
    `main.bean`. Phase 1's write discipline is preserved: take a snapshot
    of every touched file, run `bean-check`, revert on failure.
  * Runs per-upload (scoped by import_id) instead of sweeping the whole DB.
  * No hardcoded global state (WORK_DB, TAXES_ROOT); paths are injected.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

from lamella.core.identity import (
    REF_KEY,
    SOURCE_KEY,
    TXN_ID_KEY,
    mint_txn_id,
)
from lamella.core.ledger_writer import (
    BeanCheckError,
    run_bean_check,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source -> asset/liability account mapping (verbatim from bundle)
# ---------------------------------------------------------------------------

PAYMENT_METHOD_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"ACME LLC CHECKING", re.I), "Assets:Acme:BankOne:Checking"),
    (re.compile(r"ACME LLC EIDL", re.I), "Assets:Acme:BankOne:EIDL"),
    (re.compile(r"ACME LLC VISA\s+\.+7777", re.I), "Liabilities:Acme:BankOne:AffiliateD"),
    (re.compile(r"ACME LLC VISA\s+\.+8888", re.I), "Liabilities:Acme:BankOne:BusinessElite"),
    (re.compile(r"ACME LLC VISA", re.I), "Liabilities:Acme:BankOne:Credit"),
    (re.compile(r"ACME LINE OF CRED", re.I), "Liabilities:Acme:BankOne:LineOfCredit"),
    (re.compile(r"WIDGET CO LLC CHECKING", re.I), "Assets:WidgetCo:BankOne:Checking"),
    (re.compile(r"WIDGET CO LLC EIDL", re.I), "Assets:WidgetCo:BankOne:EIDL"),
    (re.compile(r"WIDGET CO LLC VISA", re.I), "Liabilities:WidgetCo:BankOne:Credit"),
    (re.compile(r"CNC LINE OF CREDIT", re.I), "Liabilities:WidgetCo:BankOne:LineOfCredit"),
    (re.compile(r"RENTALS CHECKING", re.I), "Assets:Rentals:BankOne:Checking"),
    (re.compile(r"RENTALS EIDL", re.I), "Assets:Rentals:BankOne:EIDL"),
    (re.compile(r"RENTALS VISA", re.I), "Liabilities:Rentals:BankOne:Credit"),
    (re.compile(r"PRIME CHECKING", re.I), "Assets:Personal:BankOne:PrimeChecking"),
    (re.compile(r"CHECK WRITING", re.I), "Assets:Personal:BankOne:CheckWriting"),
    (re.compile(r"VISA SIGNATURE", re.I), "Liabilities:Personal:BankOne:VisaSignature"),
    (re.compile(r"Credit Card\s+\.+5555", re.I), "Liabilities:Personal:BankOne:VisaSignature"),
    (re.compile(r"Debit Card/ATM", re.I), "Assets:Personal:BankOne:PrimeChecking"),
    (re.compile(r"Credit Card\s+\.+6666", re.I), "Liabilities:Personal:BankOne:Platinum"),
    (re.compile(r"Credit Card", re.I), "Liabilities:Personal:BankOne:Credit"),
]


def _entity_from_path(path: str | None) -> str | None:
    if not path:
        return None
    p = path.lower()
    if "acme" in p or "acme co" in p:
        return "Acme"
    if "cnc xyz" in p or "widgetco" in p:
        return "WidgetCo"
    if "rentals" in p or "rentals" in p:
        return "Rentals"
    if "farm co" in p:
        return "FarmCo"
    if "consulting" in p:
        return "Consulting"
    if "genco" in p:
        return "ThetaCo"
    if "personal" in p:
        return "Personal"
    return None


_BEAN_VALID_CHAR = re.compile(r"[^A-Za-z0-9:-]")


def sanitize_account(acct: str | None) -> str:
    if not acct:
        return "Expenses:Uncategorized"
    segs: list[str] = []
    for seg in acct.split(":"):
        s = re.sub(r"[_\u2013\u2014\-\s/,&.]+", " ", seg)
        s = re.sub(r"[^A-Za-z0-9 ]", "", s)
        parts = [p for p in s.split() if p]
        if not parts:
            segs.append("Unknown")
            continue
        pc = "".join(p[:1].upper() + p[1:] for p in parts)
        if not pc or not pc[0].isalpha():
            pc = "X" + pc
        pc = pc[:1].upper() + pc[1:]
        segs.append(pc)
    return ":".join(segs)


def source_account(
    source_class: str,
    entity_hint: str | None,
    path: str | None,
    payment_method: str | None,
    raw_json_str: str | None,
) -> str:
    entity = entity_hint or _entity_from_path(path) or "Personal"
    if source_class == "wf_annotated":
        if payment_method:
            for pat, acct in PAYMENT_METHOD_PATTERNS:
                if pat.search(payment_method):
                    return acct
        return f"Assets:{entity}:BankOne:Checking"
    if source_class == "paypal":
        try:
            rj = json.loads(raw_json_str) if raw_json_str else {}
        except Exception:
            rj = {}
        ent = rj.get("_entity") or entity
        return f"Assets:{ent}:PayPal:Cash"
    if source_class == "amazon_seller":
        return f"Assets:{entity}:Amazon:SellerBalance"
    if source_class == "amazon_merch":
        return "Assets:Acme:Amazon:MerchBalance"
    if source_class == "amazon_purchases":
        return "Liabilities:Personal:AmazonOrders"
    if source_class == "amex":
        return "Liabilities:Personal:Amex"
    if source_class == "costco_citibank":
        return "Liabilities:Personal:Warehouse ClubCiti"
    if source_class == "chase":
        return "Liabilities:Personal:ChaseCredit"
    if source_class == "ebay":
        return f"Assets:{entity}:Ebay:Balance"
    if source_class == "eidl":
        return f"Liabilities:{entity}:EIDL"
    # generic_csv/generic_xlsx: defer to payment_method hints, else Personal checking
    if source_class in ("generic_csv", "generic_xlsx"):
        if payment_method:
            for pat, acct in PAYMENT_METHOD_PATTERNS:
                if pat.search(payment_method):
                    return acct
        return f"Assets:{entity}:Imported"
    return f"Assets:{entity}:Unknown"


# ---------------------------------------------------------------------------
# Bean line emission
# ---------------------------------------------------------------------------

def fmt_amount(amt: Decimal | None) -> str:
    if amt is None:
        return "0.00"
    if abs(amt) < Decimal("0.005"):
        return "0.00"
    return "{:.2f}".format(amt)


def escape_str(s: str | None, maxlen: int | None = None) -> str:
    if s is None:
        return ""
    s = str(s).replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    if maxlen and len(s) > maxlen:
        s = s[:maxlen].rstrip()
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    return s


def render_transaction(
    row: dict,
    source_acct: str,
    counter_acct: str,
    *,
    pair_kind: str | None = None,
    import_id: int | None = None,
) -> list[str]:
    """Render one importer-sourced transaction.

    Schema (Phase 7 of NORMALIZE_TXN_IDENTITY.md — writer emits the
    new format only):
      * Transaction meta: ``lamella-txn-id`` (UUIDv7 lineage). When
        the input ``row`` carries a ``cat_lamella_txn_id`` (pre-minted
        at categorize time, persisted on the categorizations row by
        migration 055) we reuse it so the AI ``input_ref`` and the
        on-disk identity are the same value end-to-end. Falls back
        to a fresh mint for paths that bypass categorize (legacy
        callers, tests).
      * Plus ``lamella-import-memo`` when the source's memo column
        adds info beyond the description (user-visible content, not
        an identifier — kept).
      * Source-side (first) posting meta: paired indexed source keys
        ``lamella-source-0: "csv"`` + ``lamella-source-reference-id-0:
        "<id>"``. Reference id is the source-provided ``transaction_id``
        when present; otherwise the natural-key hash of (date, amount,
        payee, description) so reconstruct-from-ledger can recompute it.

    Retired identifiers (no longer emitted): ``lamella-import-id``
    (a SQLite PK — reconstruct-violation), ``lamella-import-source``
    (free-form ``source=X row=Y`` debug), ``lamella-import-txn-id``
    (replaced by the paired source meta on the source-side posting).
    Legacy on-disk content carrying these keys still parses cleanly
    via ``_legacy_meta.normalize_entries``; the ``import_id`` argument
    is accepted but unused — kept for callsite compatibility.
    """
    date = row["date"]
    amt = row["amount"]
    if amt is not None and not isinstance(amt, Decimal):
        # row["amount"] may arrive as the raw TEXT column value from the
        # SQLite read in build_postings/_render_chunk; coerce to Decimal
        # so downstream comparisons and formatting are precision-clean.
        amt = Decimal(str(amt))
    if date is None or amt is None or abs(amt) < Decimal("0.005"):
        return []
    payee = escape_str(row["payee"] or "", maxlen=200)
    narration = escape_str(row["description"] or row["memo"] or "", maxlen=200)
    lines: list[str] = []
    lines.append(f'{date} * "{payee}" "{narration}"')
    # Reuse the lineage minted at categorize time so ai_decisions's
    # input_ref and the on-disk lamella-txn-id are the same value.
    # Mint fresh only when the row arrives without one (callers that
    # bypass categorize, mostly tests).
    lineage = (row.get("cat_lamella_txn_id") or "").strip() or mint_txn_id()
    lines.append(f'  {TXN_ID_KEY}: "{lineage}"')
    if row.get("memo") and row.get("memo") != row.get("description"):
        lines.append(f'  lamella-import-memo: "{escape_str(row["memo"], maxlen=200)}"')
    # Resolve canonical source-reference-id: prefer the source-provided
    # transaction_id; fall back to a natural-key hash so reconstruct
    # from a wiped DB still produces stable identity.
    csv_ref = row.get("transaction_id")
    if csv_ref:
        canonical_ref = escape_str(csv_ref, maxlen=100)
    else:
        canonical_ref = _natural_key_hash(
            date, amt, row.get("payee"), row.get("description"),
        )
    # Source-side leg + paired indexed source meta.
    lines.append(
        "  {:50s} {:>12s} USD".format(source_acct, fmt_amount(amt))
    )
    lines.append(f'    {SOURCE_KEY}-0: "csv"')
    lines.append(f'    {REF_KEY}-0: "{canonical_ref}"')
    # Counter leg — synthesized by us, no source provenance.
    if pair_kind == "transfer":
        lines.append(
            "  {:50s} {:>12s} USD".format(
                "Assets:Clearing:Transfers", fmt_amount(-amt)
            )
        )
    else:
        lines.append("  {:50s} {:>12s} USD".format(counter_acct, fmt_amount(-amt)))
    lines.append("")
    return lines


def _natural_key_hash(
    date_value,
    amount: Decimal | None,
    payee: str | None,
    description: str | None,
) -> str:
    """SHA256 of the four fields most uniquely identifying a CSV row
    when the source provides no native transaction id. Reconstruct-
    stable: same content always produces the same hash.

    Prefixed with ``nk-`` so a reader can tell at a glance that this
    is a synthesized id rather than a source-provided one.
    """
    import hashlib
    h = hashlib.sha256()
    h.update(str(date_value).encode("utf-8"))
    h.update(b"|")
    h.update(fmt_amount(amount).encode("utf-8"))
    h.update(b"|")
    h.update((payee or "").encode("utf-8"))
    h.update(b"|")
    h.update((description or "").encode("utf-8"))
    return f"nk-{h.hexdigest()[:32]}"


# ---------------------------------------------------------------------------
# Posting population
# ---------------------------------------------------------------------------

def build_postings(conn: sqlite3.Connection, import_id: int) -> int:
    """Populate txn_postings from surviving raw_rows in this upload.

    Skips rows whose classifications.status is 'deduped' or 'skipped'.
    For transfer pairs, the B-side still posts via Assets:Clearing:Transfers.
    For duplicate pairs, only the A-side posts.

    Returns the number of txn_postings rows written.
    """
    pair_info: dict[int, tuple[str, str]] = {}
    for rp in conn.execute(
        """
        SELECT rp.row_a_id, rp.row_b_id, rp.kind
          FROM row_pairs rp
         WHERE rp.row_a_id IN (
                SELECT r.id FROM raw_rows r
                  JOIN sources s ON s.id = r.source_id
                 WHERE s.upload_id = ?)
            OR rp.row_b_id IN (
                SELECT r.id FROM raw_rows r
                  JOIN sources s ON s.id = r.source_id
                 WHERE s.upload_id = ?)
        """,
        (import_id, import_id),
    ):
        pair_info[int(rp["row_a_id"])] = (rp["kind"], "a")
        pair_info[int(rp["row_b_id"])] = (rp["kind"], "b")

    rows = conn.execute(
        """
        SELECT rr.id, rr.source_id, rr.row_num, rr.date, rr.amount,
               rr.payee, rr.description, rr.memo, rr.payment_method,
               rr.transaction_id, rr.raw_json,
               s.source_class, s.path, s.year, s.entity AS source_entity,
               cat.account AS counter_account, cat.entity AS cat_entity,
               cat.lamella_txn_id AS cat_lamella_txn_id,
               COALESCE(cls.status, 'imported') AS status,
               st.status AS staged_status
          FROM raw_rows rr
          JOIN sources s ON s.id = rr.source_id
          LEFT JOIN categorizations cat ON cat.raw_row_id = rr.id
          LEFT JOIN classifications cls ON cls.raw_row_id = rr.id
          LEFT JOIN staged_transactions st
                 ON st.source = 'csv'
                AND json_extract(st.source_ref, '$.raw_row_id') = rr.id
         WHERE s.upload_id = ?
           AND rr.date IS NOT NULL
         ORDER BY rr.date, rr.id
        """,
        (import_id,),
    ).fetchall()

    conn.execute(
        """
        DELETE FROM txn_postings WHERE raw_row_id IN (
            SELECT r.id FROM raw_rows r
              JOIN sources s ON s.id = r.source_id
             WHERE s.upload_id = ?)
        """,
        (import_id,),
    )

    emitted = 0
    for r in rows:
        if r["status"] in ("deduped", "skipped", "zero"):
            continue
        # NEXTGEN Phase C2b: the transfer writer already emitted this
        # row as one leg of a balanced cross-source transaction in
        # connector_transfers.bean. Skip here to avoid double-writing.
        if r["staged_status"] == "promoted":
            continue
        pi = pair_info.get(int(r["id"]))
        if pi and pi[0] == "duplicate" and pi[1] == "b":
            continue
        entity_hint = r["cat_entity"] or r["source_entity"]
        src = sanitize_account(
            source_account(
                r["source_class"],
                entity_hint,
                r["path"],
                r["payment_method"],
                r["raw_json"],
            )
        )
        counter = sanitize_account(r["counter_account"] or "Expenses:Uncategorized")
        pair_kind = pi[0] if pi else None
        other = counter if pair_kind != "transfer" else "Assets:Clearing:Transfers"
        # raw_rows.amount and txn_postings.amount are TEXT (post-migration
        # 057); read as Decimal and bind both legs as canonical strings.
        amt = Decimal(str(r["amount"]))
        amt_text = str(amt)
        neg_amt_text = str(-amt)
        conn.execute(
            """INSERT INTO txn_postings (raw_row_id, leg_idx, account, amount, currency)
                   VALUES (?, 0, ?, ?, 'USD')""",
            (int(r["id"]), src, amt_text),
        )
        conn.execute(
            """INSERT INTO txn_postings (raw_row_id, leg_idx, account, amount, currency)
                   VALUES (?, 1, ?, ?, 'USD')""",
            (int(r["id"]), other, neg_amt_text),
        )
        emitted += 1
    return emitted


# ---------------------------------------------------------------------------
# File writes with bean-check + rollback
# ---------------------------------------------------------------------------

CONNECTOR_IMPORTS_README = (
    "; Managed by Lamella (Phase 7 spreadsheet imports).\n"
    "; Do not hand-edit — re-run the import UI instead.\n"
)

ALL_INCLUDES_HEADER = (
    "; Generated by Lamella. Imports every per-year file in this dir.\n"
)


@dataclass
class EmitResult:
    import_id: int
    per_year: dict[str, int] = field(default_factory=dict)   # year -> txn count
    touched_files: list[Path] = field(default_factory=list)
    main_include_added: bool = False


def _render_chunk(
    conn: sqlite3.Connection,
    import_id: int,
) -> dict[str, list[str]]:
    pair_info: dict[int, tuple[str, str]] = {}
    for rp in conn.execute(
        """
        SELECT rp.row_a_id, rp.row_b_id, rp.kind
          FROM row_pairs rp
         WHERE rp.row_a_id IN (
                SELECT r.id FROM raw_rows r
                  JOIN sources s ON s.id = r.source_id
                 WHERE s.upload_id = ?)
            OR rp.row_b_id IN (
                SELECT r.id FROM raw_rows r
                  JOIN sources s ON s.id = r.source_id
                 WHERE s.upload_id = ?)
        """,
        (import_id, import_id),
    ):
        pair_info[int(rp["row_a_id"])] = (rp["kind"], "a")
        pair_info[int(rp["row_b_id"])] = (rp["kind"], "b")

    rows = conn.execute(
        """
        SELECT rr.id, rr.source_id, rr.row_num, rr.date, rr.amount,
               rr.payee, rr.description, rr.memo, rr.payment_method,
               rr.transaction_id, rr.raw_json,
               s.source_class, s.path, s.year, s.entity AS source_entity,
               cat.account AS counter_account, cat.entity AS cat_entity,
               cat.lamella_txn_id AS cat_lamella_txn_id,
               COALESCE(cls.status, 'imported') AS status,
               st.status AS staged_status
          FROM raw_rows rr
          JOIN sources s ON s.id = rr.source_id
          LEFT JOIN categorizations cat ON cat.raw_row_id = rr.id
          LEFT JOIN classifications cls ON cls.raw_row_id = rr.id
          LEFT JOIN staged_transactions st
                 ON st.source = 'csv'
                AND json_extract(st.source_ref, '$.raw_row_id') = rr.id
         WHERE s.upload_id = ?
           AND rr.date IS NOT NULL
         ORDER BY rr.date, rr.id
        """,
        (import_id,),
    ).fetchall()

    by_year: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        if r["status"] in ("deduped", "skipped", "zero"):
            continue
        # NEXTGEN Phase C2b: skip rows the transfer writer already
        # emitted as part of a balanced cross-source txn.
        if r["staged_status"] == "promoted":
            continue
        pi = pair_info.get(int(r["id"]))
        if pi and pi[0] == "duplicate" and pi[1] == "b":
            continue
        entity_hint = r["cat_entity"] or r["source_entity"]
        src = sanitize_account(
            source_account(
                r["source_class"],
                entity_hint,
                r["path"],
                r["payment_method"],
                r["raw_json"],
            )
        )
        counter = sanitize_account(
            r["counter_account"] or "Expenses:Uncategorized"
        )
        year = (r["date"] or "")[:4] or "unknown"
        lines = render_transaction(
            dict(r),
            src,
            counter,
            pair_kind=pi[0] if pi else None,
            import_id=import_id,
        )
        by_year[year].extend(lines)
    return dict(by_year)


def _atomic_append(path: Path, extra: str) -> bytes | None:
    """Append `extra` to `path` (creating it if needed). Returns the
    pre-write bytes (None if the file did not exist) so the caller can
    restore on rollback.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    before = path.read_bytes() if path.exists() else None
    if before is None:
        path.write_text(CONNECTOR_IMPORTS_README + extra, encoding="utf-8")
    else:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(extra)
    return before


def _ensure_all_includes(
    all_bean: Path, year_files: Iterable[Path]
) -> bytes | None:
    """Write `_all.bean` so it includes every per-year file. Idempotent.
    Returns pre-write bytes (None if file didn't exist).
    """
    all_bean.parent.mkdir(parents=True, exist_ok=True)
    before = all_bean.read_bytes() if all_bean.exists() else None
    existing = all_bean.read_text(encoding="utf-8") if before is not None else ""
    lines_to_add: list[str] = []
    for yf in sorted(set(year_files), key=lambda p: p.name):
        needle = f'include "{yf.name}"'
        if needle not in existing:
            lines_to_add.append(needle + "\n")
    if before is None:
        all_bean.write_text(ALL_INCLUDES_HEADER + "".join(lines_to_add), encoding="utf-8")
    elif lines_to_add:
        with all_bean.open("a", encoding="utf-8") as fh:
            fh.write("".join(lines_to_add))
    return before


def _ensure_main_include(main_bean: Path, all_bean: Path) -> tuple[bool, bytes | None]:
    """Append `include "connector_imports/_all.bean"` to main.bean if missing.
    Returns (was_modified, pre-write bytes)."""
    if not main_bean.exists():
        raise FileNotFoundError(f"main.bean not found at {main_bean}")
    before = main_bean.read_bytes()
    existing = before.decode("utf-8")
    # Reference via relative path "connector_imports/_all.bean" so main.bean can
    # stay agnostic of absolute location.
    rel = f"{all_bean.parent.name}/{all_bean.name}"
    needle = f'include "{rel}"'
    if needle in existing:
        return False, before
    suffix = "" if existing.endswith("\n") else "\n"
    addition = f'{suffix}\n; Added by Lamella (Phase 7)\n{needle}\n'
    main_bean.write_text(existing + addition, encoding="utf-8")
    return True, before


def emit_to_ledger(
    conn: sqlite3.Connection,
    *,
    import_id: int,
    main_bean: Path,
    output_dir: Path,
    run_check: bool = True,
) -> EmitResult:
    """Render the import's chunks to `output_dir/<year>.bean`, ensure the
    include tree, then run `bean-check` against `main_bean`.

    Rollback discipline: before touching anything we snapshot every file we
    will write; on `bean-check` failure we restore bytes (or unlink if the
    file did not exist).
    """
    chunks = _render_chunk(conn, import_id)
    result = EmitResult(import_id=import_id)
    if not chunks:
        log.info("emit: import_id=%d no rows to write", import_id)
        return result

    output_dir.mkdir(parents=True, exist_ok=True)
    all_bean = output_dir / "_all.bean"

    # Track pre-write state for rollback.
    year_backups: list[tuple[Path, bytes | None]] = []
    year_files: list[Path] = []
    try:
        # 1. Per-year files.
        for year, lines in sorted(chunks.items()):
            target = output_dir / f"{year}.bean"
            header = (
                f";; connector_imports/{year}.bean — imports\n"
                f";; Generated: {time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
                f";; Import ID: {import_id}\n"
                f";; Transactions in this chunk: {sum(1 for ln in lines if ln and ln[:1].isdigit())}\n"
                "\n"
            )
            before = _atomic_append(target, header + "\n".join(lines) + ("\n" if lines else ""))
            year_backups.append((target, before))
            year_files.append(target)
            result.per_year[year] = sum(1 for ln in lines if ln and ln[:1].isdigit())
            result.touched_files.append(target)

        # 2. _all.bean include-of-includes.
        all_before = _ensure_all_includes(all_bean, year_files)
        year_backups.append((all_bean, all_before))
        result.touched_files.append(all_bean)

        # 3. main.bean one-time include.
        main_modified, main_before = _ensure_main_include(main_bean, all_bean)
        result.main_include_added = main_modified

        # 4. bean-check.
        if run_check:
            try:
                run_bean_check(main_bean)
            except BeanCheckError:
                # Revert every file we touched.
                if main_modified:
                    main_bean.write_bytes(main_before)
                for path, before in year_backups:
                    if before is None:
                        path.unlink(missing_ok=True)
                    else:
                        path.write_bytes(before)
                raise

    except Exception:
        # Any unexpected error: same rollback discipline.
        for path, before in year_backups:
            if before is None:
                path.unlink(missing_ok=True)
            else:
                try:
                    path.write_bytes(before)
                except Exception:  # noqa: BLE001
                    log.exception("rollback failed for %s", path)
        raise

    # 5. bean_output audit rows.
    for year, count in result.per_year.items():
        # Resolve raw_row_ids for this year in this import.
        rows = conn.execute(
            """
            SELECT rr.id FROM raw_rows rr
              JOIN sources s ON s.id = rr.source_id
              LEFT JOIN classifications cls ON cls.raw_row_id = rr.id
             WHERE s.upload_id = ? AND substr(rr.date, 1, 4) = ?
               AND COALESCE(cls.status, 'imported') NOT IN ('deduped', 'skipped', 'zero')
            """,
            (import_id, year),
        ).fetchall()
        path = output_dir / f"{year}.bean"
        for r in rows:
            conn.execute(
                """INSERT INTO bean_output (raw_row_id, year, written_to)
                    VALUES (?, ?, ?)""",
                (int(r["id"]), int(year) if year.isdigit() else 0, str(path)),
            )

    # 6. NEXTGEN commit-time lifecycle mirror: flip every staged row
    # this emit actually wrote into the ledger from 'classified' →
    # 'promoted' with a pointer to the file it landed in. Previously
    # these stayed in 'classified' forever, so staged_transactions
    # grew unbounded and 'is this row already in the ledger?' stayed
    # expensive to answer. Now it's a cheap (source, source_ref_hash)
    # status check.
    try:
        from lamella.features.import_.staging.service import StagingService
        svc = StagingService(conn)
        for year, count in result.per_year.items():
            path = output_dir / f"{year}.bean"
            promoted_rows = conn.execute(
                """
                SELECT st.id AS staged_id
                  FROM raw_rows rr
                  JOIN sources s ON s.id = rr.source_id
                  LEFT JOIN classifications cls ON cls.raw_row_id = rr.id
                  JOIN staged_transactions st
                       ON st.source = 'csv'
                      AND json_extract(st.source_ref, '$.raw_row_id') = rr.id
                 WHERE s.upload_id = ?
                   AND substr(rr.date, 1, 4) = ?
                   AND COALESCE(cls.status, 'imported') NOT IN ('deduped', 'skipped', 'zero')
                   AND st.status IN ('new', 'classified', 'matched')
                """,
                (import_id, year),
            ).fetchall()
            for r in promoted_rows:
                try:
                    svc.mark_promoted(
                        int(r["staged_id"]),
                        promoted_to_file=str(path),
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "emit: mark_promoted(%s) failed: %s",
                        r["staged_id"], exc,
                    )
    except Exception as exc:  # noqa: BLE001
        # Non-fatal — emit already succeeded. Log and let reconcile fix.
        log.warning("emit: commit-time staging lifecycle mirror failed: %s", exc)

    return result
