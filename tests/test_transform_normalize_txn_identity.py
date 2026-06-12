# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 4 transform end-to-end. Builds a tiny ledger on disk with the
pre-normalization shape (legacy txn-level source keys, no lineage),
runs the transform, and asserts:

  * Lineage minted on every transaction lacking one.
  * Legacy ``lamella-simplefin-id`` / ``simplefin-id`` /
    ``lamella-import-txn-id`` migrated to paired indexed source meta on
    the source-side posting.
  * Retired keys (``lamella-import-id`` / ``lamella-import-source``)
    dropped entirely.
  * Idempotent on re-run (zero diff).
  * Dry-run produces a plan but writes nothing.
  * ``ai_decisions.input_ref`` rewritten to lineage when resolvable.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lamella.core.config import Settings
from lamella.core.identity import REF_KEY, SOURCE_KEY, TXN_ID_KEY
from lamella.core.transform.normalize_txn_identity import (
    _BackfillMaps,
    _looks_like_uuid,
    plan_file,
    run,
)


_LEDGER_HEAD = (
    'option "title" "Test"\n'
    'option "operating_currency" "USD"\n'
    "\n"
    '1900-01-01 open Assets:Acme:Checking USD\n'
    '1900-01-01 open Liabilities:Acme:Card USD\n'
    '1900-01-01 open Expenses:Acme:Supplies USD\n'
    '1900-01-01 open Expenses:Acme:Office USD\n'
    "\n"
)


def _write_main(ledger_dir: Path, includes: list[str]) -> Path:
    main = ledger_dir / "main.bean"
    body = _LEDGER_HEAD
    for inc in includes:
        body += f'include "{inc}"\n'
    main.write_text(body, encoding="utf-8")
    return main


def _ledger_with(text: str, *, ledger_dir: Path, name: str) -> Path:
    """Create ``ledger_dir/<name>.bean`` with the given txn body and an
    accompanying ``main.bean`` that includes it."""
    target = ledger_dir / f"{name}.bean"
    target.write_text(text, encoding="utf-8")
    _write_main(ledger_dir, [f"{name}.bean"])
    return target


@pytest.fixture
def empty_ledger(tmp_path: Path) -> Path:
    d = tmp_path / "ledger"
    d.mkdir()
    return d


@pytest.fixture
def settings(tmp_path: Path, empty_ledger: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        ledger_dir=empty_ledger,
    )


# ---------------------------------------------------------------------------
# Schema migration cases
# ---------------------------------------------------------------------------

def test_mints_lineage_on_txn_lacking_one(empty_ledger, settings):
    """Bare manual entry with no lineage gets a UUIDv7 stamped at txn
    meta, and nothing else changes."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="manual")

    result = run(settings, apply=True, run_check=False, db_conn=None)
    assert result.applied
    assert result.lineage_minted == 1
    assert result.simplefin_migrated == 0
    assert result.csv_migrated == 0

    out = (empty_ledger / "manual.bean").read_text(encoding="utf-8")
    # The lineage line was inserted right under the header.
    assert f'  {TXN_ID_KEY}:' in out
    # Pull the value and confirm it's UUID-shaped.
    line = next(ln for ln in out.splitlines() if TXN_ID_KEY in ln)
    val = line.split('"')[1]
    assert _looks_like_uuid(val)
    # Postings preserved verbatim.
    assert "Liabilities:Acme:Card  -42.17 USD" in out
    assert "Expenses:Acme:Supplies  42.17 USD" in out


def test_migrates_simplefin_id_to_first_posting_paired_source(
    empty_ledger, settings,
):
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-A1"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="simplefin_txns")

    result = run(settings, apply=True, run_check=False, db_conn=None)
    assert result.simplefin_migrated == 1
    assert result.lineage_minted == 1

    out = (empty_ledger / "simplefin_txns.bean").read_text(encoding="utf-8")
    # Legacy txn-level key is gone.
    assert "lamella-simplefin-id" not in out
    # Paired indexed source meta lives on the bank-side posting now.
    assert f'{SOURCE_KEY}-0: "simplefin"' in out
    assert f'{REF_KEY}-0: "TRN-A1"' in out
    # And lineage was minted at the txn level.
    assert f'  {TXN_ID_KEY}:' in out


def test_migrates_bare_simplefin_id(empty_ledger, settings):
    """Pre-prefix-era entries used the bare ``simplefin-id`` key. The
    transform must accept it interchangeably with the prefixed form."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  simplefin-id: "TRN-OLD"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="legacy_simplefin")

    run(settings, apply=True, run_check=False, db_conn=None)
    out = (empty_ledger / "legacy_simplefin.bean").read_text(encoding="utf-8")
    assert "simplefin-id:" not in out  # both bare AND lamella- prefix gone
    assert f'{SOURCE_KEY}-0: "simplefin"' in out
    assert f'{REF_KEY}-0: "TRN-OLD"' in out


def test_migrates_csv_import_txn_id_and_drops_retired_keys(
    empty_ledger, settings,
):
    body = (
        '2026-04-15 * "Office Supplies" "Pens"\n'
        '  lamella-import-id: "42"\n'
        '  lamella-import-txn-id: "CSV-99"\n'
        '  lamella-import-source: "source=acme-bank-csv row=99"\n'
        '  lamella-import-memo: "kept across transform"\n'
        '  Liabilities:Acme:Card  -8.50 USD\n'
        '  Expenses:Acme:Office   8.50 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="csv_txns")

    result = run(settings, apply=True, run_check=False, db_conn=None)
    assert result.csv_migrated == 1
    assert result.lineage_minted == 1

    out = (empty_ledger / "csv_txns.bean").read_text(encoding="utf-8")
    # Retired txn-level keys gone.
    assert "lamella-import-id" not in out
    assert "lamella-import-txn-id" not in out
    assert "lamella-import-source" not in out
    # Memo survives — it's user-facing content, not an identifier.
    assert 'lamella-import-memo: "kept across transform"' in out
    # Paired indexed csv source landed on first posting.
    assert f'{SOURCE_KEY}-0: "csv"' in out
    assert f'{REF_KEY}-0: "CSV-99"' in out


def test_idempotent_on_already_normalized_entry(empty_ledger, settings):
    """An entry already in the post-normalization shape produces no
    diff. Re-running the transform on a clean ledger is a no-op."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        f'  {TXN_ID_KEY}: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        f'    {SOURCE_KEY}-0: "simplefin"\n'
        f'    {REF_KEY}-0: "TRN-A1"\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    target = _ledger_with(body, ledger_dir=empty_ledger, name="normalized")

    result = run(settings, apply=True, run_check=False, db_conn=None)
    assert result.files_changed == 0
    assert result.lineage_minted == 0
    assert result.simplefin_migrated == 0
    # And the file is byte-identical.
    assert target.read_text(encoding="utf-8") == body


def test_dry_run_writes_nothing(empty_ledger, settings):
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-DRY"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    target = _ledger_with(body, ledger_dir=empty_ledger, name="dry")
    pre = target.read_text(encoding="utf-8")

    result = run(settings, apply=False, run_check=False, db_conn=None)
    assert result.applied is False
    assert result.files_changed == 1
    assert result.lineage_minted == 1
    # File untouched.
    assert target.read_text(encoding="utf-8") == pre


def test_does_not_double_stamp_when_posting_already_has_source(
    empty_ledger, settings,
):
    """If the bank-side posting already carries the same SimpleFIN
    source pair (e.g. partial pre-normalization run), the txn-level
    legacy key still drops but no duplicate posting-meta line is
    inserted."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-DUP"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        f'    {SOURCE_KEY}-0: "simplefin"\n'
        f'    {REF_KEY}-0: "TRN-DUP"\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="already_partial")

    run(settings, apply=True, run_check=False, db_conn=None)
    out = (empty_ledger / "already_partial.bean").read_text(encoding="utf-8")
    # The legacy key is gone but only ONE simplefin pair exists.
    assert "lamella-simplefin-id" not in out
    assert out.count(f'{SOURCE_KEY}-0: "simplefin"') == 1
    assert out.count(f'{REF_KEY}-0: "TRN-DUP"') == 1


def test_appends_at_next_free_index_when_posting_already_has_a_source(
    empty_ledger, settings,
):
    """Cross-source case: the posting already has a CSV source at -0
    when we discover a new SimpleFIN id at the txn level. The new pair
    should land at -1, dense from the existing -0."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-NEW"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        f'    {SOURCE_KEY}-0: "csv"\n'
        f'    {REF_KEY}-0: "CSV-99"\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="cross_source")

    run(settings, apply=True, run_check=False, db_conn=None)
    out = (empty_ledger / "cross_source.bean").read_text(encoding="utf-8")
    assert "lamella-simplefin-id" not in out
    assert f'{SOURCE_KEY}-0: "csv"' in out
    assert f'{SOURCE_KEY}-1: "simplefin"' in out
    assert f'{REF_KEY}-1: "TRN-NEW"' in out


def test_skips_snapshot_directories(empty_ledger, settings):
    """Files under ``.pre-normalize-*`` (our own snapshots) and
    ``_archive*`` (user archive) must be ignored — editing them would
    confuse subsequent rollback paths."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-SNAPSHOT"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    snap_dir = empty_ledger / ".pre-normalize-20260101T000000"
    snap_dir.mkdir()
    (snap_dir / "ghost.bean").write_text(body, encoding="utf-8")
    archive_dir = empty_ledger / "_archive_2025"
    archive_dir.mkdir()
    (archive_dir / "old.bean").write_text(body, encoding="utf-8")
    # Plus one real file the transform SHOULD touch.
    _ledger_with(body, ledger_dir=empty_ledger, name="real")

    result = run(settings, apply=True, run_check=False, db_conn=None)
    assert result.files_changed == 1
    assert (snap_dir / "ghost.bean").read_text(encoding="utf-8") == body
    assert (archive_dir / "old.bean").read_text(encoding="utf-8") == body


def test_apply_creates_snapshot_directory(empty_ledger, settings):
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-X"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    target = _ledger_with(body, ledger_dir=empty_ledger, name="snap_me")

    result = run(settings, apply=True, run_check=False, db_conn=None)
    assert result.snapshot_dir is not None
    assert result.snapshot_dir.exists()
    # Snapshot lives under the ledger root with the .pre-normalize-* prefix.
    assert result.snapshot_dir.name.startswith(".pre-normalize-")
    # Snapshot copy is byte-identical to the pre-write content.
    snap_copy = result.snapshot_dir / "snap_me.bean"
    assert snap_copy.exists()
    assert snap_copy.read_text(encoding="utf-8") == body
    # And the live file actually changed.
    assert target.read_text(encoding="utf-8") != body


def test_full_round_trip_loads_under_normalize_entries(empty_ledger, settings):
    """Sanity: after the transform, parsing the file through the
    standard at-load normalizer produces a transaction with a non-None
    lineage and the SimpleFIN source on the first posting."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-RT"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="round_trip")

    run(settings, apply=True, run_check=False, db_conn=None)

    from beancount.core.data import Transaction
    from beancount.parser import parser as bparser
    from lamella.utils._legacy_meta import normalize_entries
    from lamella.core.identity import get_txn_id, iter_sources

    target = empty_ledger / "round_trip.bean"
    raw, _, _ = bparser.parse_file(str(target))
    entries = normalize_entries(raw)
    txns = [e for e in entries if isinstance(e, Transaction)]
    assert len(txns) == 1
    txn = txns[0]
    assert get_txn_id(txn) is not None
    pairs = list(iter_sources(txn.postings[0].meta))
    assert ("simplefin", "TRN-RT") in pairs


# ---------------------------------------------------------------------------
# AI decisions backfill
# ---------------------------------------------------------------------------

def _make_decisions_table(conn: sqlite3.Connection) -> None:
    """Minimal schema mirroring the runtime DB shape, just enough for
    the backfill test to round-trip through the same code path."""
    conn.execute(
        """
        CREATE TABLE ai_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_type TEXT NOT NULL,
            input_ref TEXT NOT NULL,
            model TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            result TEXT,
            user_corrected INTEGER DEFAULT 0,
            user_correction TEXT
        )
        """
    )
    conn.commit()


def _seed(conn: sqlite3.Connection, input_ref: str) -> int:
    cur = conn.execute(
        "INSERT INTO ai_decisions (decision_type, input_ref, model) "
        "VALUES (?, ?, ?)",
        ("classify_txn", input_ref, "anthropic/claude-haiku-4.5"),
    )
    conn.commit()
    return int(cur.lastrowid)


def test_backfill_rewrites_simplefin_id_to_lineage(empty_ledger, settings):
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-BACK1"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="for_backfill")

    conn = sqlite3.connect(":memory:")
    _make_decisions_table(conn)
    decision_id = _seed(conn, "TRN-BACK1")

    result = run(settings, apply=True, run_check=False, db_conn=conn)
    assert result.ai_decisions_backfilled == 1
    new_ref = conn.execute(
        "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,)
    ).fetchone()[0]
    assert _looks_like_uuid(new_ref)
    conn.close()


def test_backfill_rewrites_txn_hash_to_lineage(empty_ledger, settings):
    """Decisions logged post-promotion under the entry's ``txn_hash``
    get rewritten to the lineage too — that's the most common shape."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="for_hash_backfill")

    # Compute the txn_hash the same way the AI client would have.
    from beancount.core.data import Transaction
    from beancount.parser import parser as bparser
    from lamella.core.beancount_io.txn_hash import txn_hash

    raw, _, _ = bparser.parse_file(
        str(empty_ledger / "for_hash_backfill.bean")
    )
    txn = next(e for e in raw if isinstance(e, Transaction))
    th = txn_hash(txn)

    conn = sqlite3.connect(":memory:")
    _make_decisions_table(conn)
    decision_id = _seed(conn, th)

    result = run(settings, apply=True, run_check=False, db_conn=conn)
    assert result.ai_decisions_backfilled == 1
    new_ref = conn.execute(
        "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,)
    ).fetchone()[0]
    assert _looks_like_uuid(new_ref)
    assert new_ref != th
    conn.close()


def test_backfill_rewrites_importer_composite_to_lineage(
    empty_ledger, settings,
):
    body = (
        '2026-04-15 * "Office Supplies" "Pens"\n'
        '  lamella-import-id: "42"\n'
        '  lamella-import-txn-id: "CSV-99"\n'
        '  lamella-import-source: "source=acme-bank-csv row=99"\n'
        '  Liabilities:Acme:Card  -8.50 USD\n'
        '  Expenses:Acme:Office   8.50 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="for_csv_backfill")

    conn = sqlite3.connect(":memory:")
    _make_decisions_table(conn)
    decision_id = _seed(conn, "import:acme-bank-csv:row:99")

    result = run(settings, apply=True, run_check=False, db_conn=conn)
    assert result.ai_decisions_backfilled == 1
    new_ref = conn.execute(
        "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,)
    ).fetchone()[0]
    assert _looks_like_uuid(new_ref)
    conn.close()


def test_backfill_leaves_unresolvable_ref_alone(empty_ledger, settings):
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-EXIST"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="unresolvable")

    conn = sqlite3.connect(":memory:")
    _make_decisions_table(conn)
    decision_id = _seed(conn, "TRN-NOT-IN-LEDGER")

    result = run(settings, apply=True, run_check=False, db_conn=conn)
    # The unrelated row is left alone; only the resolvable one (none here)
    # would have been touched.
    assert result.ai_decisions_backfilled == 0
    new_ref = conn.execute(
        "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,)
    ).fetchone()[0]
    assert new_ref == "TRN-NOT-IN-LEDGER"
    conn.close()


def test_backfill_skips_already_uuid_refs(empty_ledger, settings):
    """A decision logged after Phase 3 already carries a UUID lineage.
    The backfill must not touch it."""
    body = (
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n'
    )
    _ledger_with(body, ledger_dir=empty_ledger, name="already_uuid")

    conn = sqlite3.connect(":memory:")
    _make_decisions_table(conn)
    pre = "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"
    decision_id = _seed(conn, pre)

    result = run(settings, apply=True, run_check=False, db_conn=conn)
    assert result.ai_decisions_backfilled == 0
    after = conn.execute(
        "SELECT input_ref FROM ai_decisions WHERE id = ?", (decision_id,)
    ).fetchone()[0]
    assert after == pre
    conn.close()


# ---------------------------------------------------------------------------
# plan_file unit coverage
# ---------------------------------------------------------------------------

def test_plan_file_returns_none_when_already_normalized(tmp_path):
    p = tmp_path / "clean.bean"
    p.write_text(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        f'  {TXN_ID_KEY}: "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        f'    {SOURCE_KEY}-0: "simplefin"\n'
        f'    {REF_KEY}-0: "TRN-CLEAN"\n'
        '  Expenses:Acme:Supplies  42.17 USD\n',
        encoding="utf-8",
    )
    backfill = _BackfillMaps()
    assert plan_file(p, backfill=backfill) is None


def test_plan_file_returns_change_for_legacy_entry(tmp_path):
    p = tmp_path / "legacy.bean"
    p.write_text(
        '2026-04-15 * "Hardware Store" "Supplies"\n'
        '  lamella-simplefin-id: "TRN-LEG"\n'
        '  Liabilities:Acme:Card  -42.17 USD\n'
        '  Expenses:Acme:Supplies  42.17 USD\n',
        encoding="utf-8",
    )
    backfill = _BackfillMaps()
    change = plan_file(p, backfill=backfill)
    assert change is not None
    assert change.n_changed_txns == 1
    assert change.n_lineage_minted == 1
    assert change.n_simplefin_migrated == 1
    # The backfill map captured the migration target.
    assert "TRN-LEG" in backfill.simplefin_to_lineage
    lineage = backfill.simplefin_to_lineage["TRN-LEG"]
    assert _looks_like_uuid(lineage)
