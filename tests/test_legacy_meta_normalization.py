# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Tests for ``lamella._legacy_meta.normalize_entries``.

The rebrand renames every metadata key, transaction tag, and Custom
directive type from the ``bcg-`` namespace to the ``lamella-``
namespace. New writes only emit ``lamella-`` strings, but every
deployed ledger still carries ``bcg-`` content on disk. The
normalizer rewrites those references to ``lamella-`` at load time
so downstream code only ever sees the new prefix.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from beancount import loader
from beancount.core.data import Custom, Transaction

from lamella.utils._legacy_meta import normalize_entries


def _write(path: Path, body: str) -> Path:
    main = path / "main.bean"
    main.write_text(body, encoding="utf-8")
    return main


def test_renames_meta_keys_in_postings_and_top_level(tmp_path: Path):
    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  bcg-simplefin-id: "sf-1"\n'
        '  bcg-ai-classified: TRUE\n'
        '  Assets:Bank      -10.00 USD\n'
        '    bcg-rule-id: "rule-7"\n'
        '  Expenses:Food     10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert "lamella-simplefin-id" in txn.meta
    assert "lamella-ai-classified" in txn.meta
    assert "bcg-simplefin-id" not in txn.meta
    posting_meta = next(p.meta for p in txn.postings if p.meta and "lamella-rule-id" in p.meta)
    assert posting_meta["lamella-rule-id"] == "rule-7"


def test_renames_custom_directive_type(tmp_path: Path):
    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2026-01-01 custom "bcg-ledger-version" "1"\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    custom = next(e for e in entries if isinstance(e, Custom))
    assert custom.type == "lamella-ledger-version"


def test_renames_transaction_tags(tmp_path: Path):
    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Equity:Open USD\n'
        '2026-04-15 * "" "override block" #bcg-override #bcg-loan-funding #user-tag\n'
        '  Assets:Bank   100.00 USD\n'
        '  Equity:Open  -100.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert "lamella-override" in txn.tags
    assert "lamella-loan-funding" in txn.tags
    assert "user-tag" in txn.tags  # untouched
    assert "bcg-override" not in txn.tags


def test_collision_keeps_new_key(tmp_path: Path):
    """When both legacy and new names exist on the same dict, the
    new key wins and the legacy key is dropped — operator's ``apply``
    of the on-disk transform must converge on the new prefix."""
    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  bcg-simplefin-id: "legacy"\n'
        '  lamella-simplefin-id: "new"\n'
        '  Assets:Bank      -10.00 USD\n'
        '  Expenses:Food     10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert txn.meta["lamella-simplefin-id"] == "new"
    assert "bcg-simplefin-id" not in txn.meta


def test_modern_ledger_unchanged_passes_through(tmp_path: Path):
    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2026-01-01 custom "lamella-ledger-version" "1"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch" #lamella-override\n'
        '  lamella-simplefin-id: "sf-2"\n'
        '  Assets:Bank      -5.00 USD\n'
        '  Expenses:Food     5.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries_after = normalize_entries(entries)
    # Same length, same content — no rewrites needed.
    assert len(entries_after) == len(entries)
    txn = next(e for e in entries_after if isinstance(e, Transaction))
    # Legacy txn-level key stays in place (Phase 1 is additive — Phase 4
    # transform is what drops it on disk; existing readers keep working).
    assert "lamella-simplefin-id" in txn.meta
    assert "lamella-override" in txn.tags


# ─── identity normalization (Phase 1 additive layer) ──────────────


def test_simplefin_legacy_key_mirrored_to_first_posting(tmp_path: Path):
    """Phase 1 mirrors the transaction-level legacy SimpleFIN id down
    to the source-side (first) posting as paired indexed source meta.
    The legacy key STAYS on txn meta so existing readers keep working."""
    from lamella.core.identity import iter_sources

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Liabilities:Card USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  lamella-simplefin-id: "TRN-X"\n'
        '  Liabilities:Card  -10.00 USD\n'
        '  Expenses:Food      10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    # Legacy key still present (Phase 1 additive).
    assert txn.meta.get("lamella-simplefin-id") == "TRN-X"
    # Mirror lives on the first posting (source side per writer convention).
    src_pairs = list(iter_sources(txn.postings[0].meta))
    assert ("simplefin", "TRN-X") in src_pairs
    # Second posting (synthesized expense leg) has no source meta.
    assert list(iter_sources(txn.postings[1].meta)) == []


def test_bare_simplefin_id_mirrored(tmp_path: Path):
    """Pre-prefix-era bare ``simplefin-id`` (no ``lamella-`` prefix)
    is mirrored to posting-level source meta the same way."""
    from lamella.core.identity import iter_sources

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Liabilities:Card USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  simplefin-id: "TRN-Y"\n'
        '  Liabilities:Card  -10.00 USD\n'
        '  Expenses:Food      10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert ("simplefin", "TRN-Y") in list(iter_sources(txn.postings[0].meta))


def test_importer_legacy_pair_mirrored_as_csv_source(tmp_path: Path):
    """``lamella-import-id`` (a SQLite PK on its own) + ``lamella-
    import-txn-id`` (the source-provided id worth keeping) → mirrored
    as a ``csv`` source on the first posting."""
    from lamella.core.identity import iter_sources

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Liabilities:Card USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  lamella-import-id: "42"\n'
        '  lamella-import-txn-id: "AMZ-ORDER-99"\n'
        '  Liabilities:Card  -10.00 USD\n'
        '  Expenses:Food      10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert ("csv", "AMZ-ORDER-99") in list(iter_sources(txn.postings[0].meta))


def test_orphan_lamella_import_id_does_not_create_source(tmp_path: Path):
    """``lamella-import-id`` alone is a SQLite PK and reconstruct-
    unsafe — without its companion ``lamella-import-txn-id`` it
    contributes no source meta (the value is dropped, not migrated)."""
    from lamella.core.identity import iter_sources

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Liabilities:Card USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  lamella-import-id: "42"\n'
        '  Liabilities:Card  -10.00 USD\n'
        '  Expenses:Food      10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    # No csv source emitted.
    src_pairs = list(iter_sources(txn.postings[0].meta))
    assert all(src != "csv" for src, _ in src_pairs)


def test_bare_posting_source_pair_folded_to_indexed(tmp_path: Path):
    """A hand-edited posting carrying the bare un-indexed pair gets
    folded to the indexed canonical form by the normalizer."""
    from lamella.core.identity import REF_KEY, SOURCE_KEY

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  Assets:Bank      -5.00 USD\n'
        '    lamella-source: "simplefin"\n'
        '    lamella-source-reference-id: "TRN-Z"\n'
        '  Expenses:Food     5.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    posting_meta = txn.postings[0].meta
    assert posting_meta.get(f"{SOURCE_KEY}-0") == "simplefin"
    assert posting_meta.get(f"{REF_KEY}-0") == "TRN-Z"
    # Bare keys consumed.
    assert SOURCE_KEY not in posting_meta
    assert REF_KEY not in posting_meta


def test_lineage_id_NOT_auto_minted_at_parse_time(tmp_path: Path):
    """Phase 1 must not silently mint lineage ids on every entry.
    Doing so would make every txn appear Lamella-managed to the
    bootstrap classifier and the ``main._ledger_has_bcg_content``
    check, both of which use "any lamella-* key" as the heuristic.
    Lineage is stamped only by writers + transform + lazy-mint
    helpers, never as a side effect of reading the ledger."""
    from lamella.core.identity import TXN_ID_KEY

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  Assets:Bank      -5.00 USD\n'
        '  Expenses:Food     5.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert TXN_ID_KEY not in txn.meta


def test_lineage_id_preserved_when_already_present(tmp_path: Path):
    from lamella.core.identity import TXN_ID_KEY

    existing_id = "0190fe22-7c10-7000-8000-000000000001"
    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        f'  {TXN_ID_KEY}: "{existing_id}"\n'
        '  Assets:Bank      -5.00 USD\n'
        '  Expenses:Food     5.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert txn.meta[TXN_ID_KEY] == existing_id


def test_normalization_is_idempotent(tmp_path: Path):
    """Running normalize_entries twice produces the same result —
    source mirrors are deduped by stamp_source's idempotency guard."""
    from lamella.core.identity import iter_sources

    main = _write(
        tmp_path,
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Liabilities:Card USD\n'
        '2020-01-01 open Expenses:Food USD\n'
        '2026-04-15 * "lunch"\n'
        '  lamella-simplefin-id: "TRN-X"\n'
        '  Liabilities:Card  -10.00 USD\n'
        '  Expenses:Food      10.00 USD\n',
    )
    entries, _, _ = loader.load_file(str(main))
    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    first_sources = list(iter_sources(txn.postings[0].meta))

    entries = normalize_entries(entries)
    txn = next(e for e in entries if isinstance(e, Transaction))
    assert list(iter_sources(txn.postings[0].meta)) == first_sources
