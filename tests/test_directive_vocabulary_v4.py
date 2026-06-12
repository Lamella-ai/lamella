# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0061: directive vocabulary v3 → v4.

Three contracts to verify:

1. **Writers emit document-* only.** Calls to the writer modules
   (`linker.py`, `dismissals_writer.py`, `link_block_writer.py`,
   `backfill_hash._amendment_block`) produce ``document-*``
   directives — never ``receipt-*``.

2. **Readers accept both vocabularies.** The reconstruct read paths
   (``read_dismissals_from_entries``, ``read_link_blocks_from_entries``)
   parse both the legacy ``receipt-*`` shapes and the new
   ``document-*`` shapes.

3. **Mixed ledgers parse correctly.** A ledger that contains a mix
   of legacy receipt-* directives and new document-* directives
   produces the correct active set (last-write-wins on the same
   txn_hash, regardless of vocabulary).
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest
from beancount import loader

from lamella.features.receipts.directive_types import (
    DIRECTIVE_DISMISSAL_REVOKED_NEW,
    DIRECTIVE_DISMISSED_NEW,
    DIRECTIVE_LINK_BLOCKED_NEW,
    DIRECTIVE_LINK_BLOCK_REVOKED_NEW,
    DIRECTIVE_LINK_HASH_BACKFILL_NEW,
    DIRECTIVE_LINK_NEW,
    LEGACY_TO_NEW,
)


# ---------------------------------------------------------------------
# Constants are wired correctly
# ---------------------------------------------------------------------


def test_constants_are_document_prefixed():
    assert DIRECTIVE_LINK_NEW == "document-link"
    assert DIRECTIVE_LINK_HASH_BACKFILL_NEW == "document-link-hash-backfill"
    assert DIRECTIVE_DISMISSED_NEW == "document-dismissed"
    assert DIRECTIVE_DISMISSAL_REVOKED_NEW == "document-dismissal-revoked"
    assert DIRECTIVE_LINK_BLOCKED_NEW == "document-link-blocked"
    assert DIRECTIVE_LINK_BLOCK_REVOKED_NEW == "document-link-block-revoked"


def test_legacy_to_new_map_has_six_entries():
    # Six legacy → new pairs covering link, hash-backfill,
    # dismissed/revoked, link-blocked/revoked.
    assert len(LEGACY_TO_NEW) == 6
    assert LEGACY_TO_NEW["receipt-link"] == "document-link"
    assert LEGACY_TO_NEW["receipt-dismissed"] == "document-dismissed"
    assert LEGACY_TO_NEW["receipt-link-blocked"] == "document-link-blocked"


# ---------------------------------------------------------------------
# Writer contract: emit document-* only
# ---------------------------------------------------------------------


def _stub_main_bean(tmp_path: Path) -> Path:
    """Minimal main.bean that includes connector_links so beancount
    parses without errors."""
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        'option "title" "test"\n'
        'plugin "beancount.plugins.auto_accounts"\n'
        '\n'
        'include "connector_links.bean"\n',
        encoding="utf-8",
    )
    (tmp_path / "connector_links.bean").write_text(
        "; Managed by Lamella. Do not hand-edit.\n", encoding="utf-8",
    )
    return main


def test_dismissals_writer_emits_document_dismissed(tmp_path: Path):
    from lamella.features.receipts.dismissals_writer import append_dismissal

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    append_dismissal(
        connector_links=links,
        main_bean=main,
        txn_hash="01900000-0000-7000-8000-aaaaaaaaaaaa",
        dismissed_at=datetime(2026, 5, 1, 12, 0, 0),
        run_check=False,
    )
    text = links.read_text(encoding="utf-8")
    assert 'custom "document-dismissed"' in text
    assert 'custom "receipt-dismissed"' not in text


def test_dismissal_revoke_writer_emits_document_revoked(tmp_path: Path):
    from lamella.features.receipts.dismissals_writer import (
        append_dismissal_revoke,
    )

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    append_dismissal_revoke(
        connector_links=links,
        main_bean=main,
        txn_hash="01900000-0000-7000-8000-aaaaaaaaaaaa",
        revoked_at=datetime(2026, 5, 2, 12, 0, 0),
        run_check=False,
    )
    text = links.read_text(encoding="utf-8")
    assert 'custom "document-dismissal-revoked"' in text
    assert 'custom "receipt-dismissal-revoked"' not in text


def test_link_block_writer_emits_document_blocked(tmp_path: Path):
    from lamella.features.receipts.link_block_writer import append_link_block

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    append_link_block(
        connector_links=links,
        main_bean=main,
        txn_hash="01900000-0000-7000-8000-aaaaaaaaaaaa",
        paperless_id=42,
        run_check=False,
    )
    text = links.read_text(encoding="utf-8")
    assert 'custom "document-link-blocked"' in text
    assert 'custom "receipt-link-blocked"' not in text


def test_link_block_revoke_writer_emits_document_revoked(tmp_path: Path):
    from lamella.features.receipts.link_block_writer import (
        append_link_block_revoke,
    )

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    append_link_block_revoke(
        connector_links=links,
        main_bean=main,
        txn_hash="01900000-0000-7000-8000-aaaaaaaaaaaa",
        paperless_id=42,
        run_check=False,
    )
    text = links.read_text(encoding="utf-8")
    assert 'custom "document-link-block-revoked"' in text
    assert 'custom "receipt-link-block-revoked"' not in text


def test_linker_emits_document_link(tmp_path: Path):
    """DocumentLinker.link writes document-link going forward."""
    import sqlite3
    from decimal import Decimal

    from lamella.core.db import connect, migrate
    from lamella.features.receipts.linker import DocumentLinker

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    conn = connect(Path(":memory:"))
    migrate(conn)
    linker = DocumentLinker(
        conn=conn, main_bean=main, connector_links=links, run_check=False,
    )
    linker.link(
        paperless_id=42,
        txn_hash="01900000-0000-7000-8000-aaaaaaaaaaaa",
        txn_date=date(2026, 1, 15),
        txn_amount=Decimal("12.34"),
        match_method="auto_sweep",
        match_confidence=0.95,
    )
    text = links.read_text(encoding="utf-8")
    assert 'custom "document-link"' in text
    assert 'custom "receipt-link"' not in text


def test_backfill_hash_emits_document_link_hash_backfill():
    """The backfill amendment block emits document-* directive type."""
    from lamella.core.transform.backfill_hash import (
        BackfillRow,
        _amendment_block,
    )

    row = BackfillRow(
        link_id=1,
        paperless_id=42,
        txn_hash="01900000-0000-7000-8000-aaaaaaaaaaaa",
        resolved_hash="md5:abc",
        resolved_url="http://example.com/doc/42",
    )
    block = _amendment_block(row, today="2026-05-02")
    assert 'custom "document-link-hash-backfill"' in block
    assert 'custom "receipt-link-hash-backfill"' not in block


# ---------------------------------------------------------------------
# Reader contract: accept both vocabularies
# ---------------------------------------------------------------------


def test_dismissals_reader_accepts_legacy_receipt_dismissed(tmp_path: Path):
    """A v3 ledger with legacy receipt-dismissed directives must
    still produce active dismissal rows when read by the v4 reader.
    """
    from lamella.features.receipts.dismissals_writer import (
        read_dismissals_from_entries,
    )

    main = _stub_main_bean(tmp_path)
    (tmp_path / "connector_links.bean").write_text(
        "; Managed by Lamella. Do not hand-edit.\n"
        '\n'
        '2026-01-15 custom "receipt-dismissed" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-dismissed-by: "user"\n'
        '  lamella-dismissed-at: 2026-01-15\n',
        encoding="utf-8",
    )
    entries, _errors, _options = loader.load_file(str(main))
    rows = read_dismissals_from_entries(entries)
    assert len(rows) == 1
    assert rows[0]["txn_hash"] == "01900000-0000-7000-8000-aaaaaaaaaaaa"


def test_dismissals_reader_accepts_new_document_dismissed(tmp_path: Path):
    from lamella.features.receipts.dismissals_writer import (
        read_dismissals_from_entries,
    )

    main = _stub_main_bean(tmp_path)
    (tmp_path / "connector_links.bean").write_text(
        "; Managed by Lamella. Do not hand-edit.\n"
        '\n'
        '2026-01-15 custom "document-dismissed" "01900000-0000-7000-8000-bbbbbbbbbbbb"\n'
        '  lamella-dismissed-by: "user"\n'
        '  lamella-dismissed-at: 2026-01-15\n',
        encoding="utf-8",
    )
    entries, _errors, _options = loader.load_file(str(main))
    rows = read_dismissals_from_entries(entries)
    assert len(rows) == 1
    assert rows[0]["txn_hash"] == "01900000-0000-7000-8000-bbbbbbbbbbbb"


def test_dismissals_reader_mixed_vocabulary_last_write_wins(tmp_path: Path):
    """A txn dismissed under receipt-dismissed and then revoked under
    document-dismissal-revoked should be inactive (last write wins
    across vocabularies).
    """
    from lamella.features.receipts.dismissals_writer import (
        read_dismissals_from_entries,
    )

    main = _stub_main_bean(tmp_path)
    (tmp_path / "connector_links.bean").write_text(
        "; Managed by Lamella. Do not hand-edit.\n"
        '\n'
        '2026-01-15 custom "receipt-dismissed" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-dismissed-by: "user"\n'
        '  lamella-dismissed-at: 2026-01-15\n'
        '\n'
        # New-vocabulary revoke must cancel the legacy-vocabulary dismissal.
        '2026-01-20 custom "document-dismissal-revoked" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-revoked-at: 2026-01-20\n',
        encoding="utf-8",
    )
    entries, _errors, _options = loader.load_file(str(main))
    rows = read_dismissals_from_entries(entries)
    assert rows == []


def test_link_blocks_reader_mixed_vocabulary(tmp_path: Path):
    """Same last-write-wins contract for link blocks."""
    from lamella.features.receipts.link_block_writer import (
        read_link_blocks_from_entries,
    )

    main = _stub_main_bean(tmp_path)
    (tmp_path / "connector_links.bean").write_text(
        "; Managed by Lamella. Do not hand-edit.\n"
        '\n'
        '2026-01-15 custom "receipt-link-blocked" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-paperless-id: 42\n'
        '\n'
        '2026-02-15 custom "document-link-blocked" "01900000-0000-7000-8000-bbbbbbbbbbbb"\n'
        '  lamella-paperless-id: 99\n',
        encoding="utf-8",
    )
    entries, _errors, _options = loader.load_file(str(main))
    rows = read_link_blocks_from_entries(entries)
    assert len(rows) == 2
    pids = sorted(r["paperless_id"] for r in rows)
    assert pids == [42, 99]


# ---------------------------------------------------------------------
# remove_document_link removes both legacy AND new shapes
# ---------------------------------------------------------------------


def test_remove_receipt_link_removes_legacy_block(tmp_path: Path):
    """A legacy receipt-link block written by v3 software must be
    removable by the v4 unlink path. Otherwise unlinking a pre-v4
    pair would silently no-op.
    """
    from lamella.features.receipts.linker import remove_document_link

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    links.write_text(
        "; Managed by Lamella. Do not hand-edit.\n"
        '\n'
        '2026-01-15 custom "receipt-link" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-paperless-id: 42\n'
        '  lamella-match-method: "auto_sweep"\n'
        '  lamella-match-confidence: 0.95\n',
        encoding="utf-8",
    )
    removed = remove_document_link(
        main_bean=main,
        connector_links=links,
        txn_id="01900000-0000-7000-8000-aaaaaaaaaaaa",
        paperless_id=42,
        run_check=False,
    )
    assert removed is True
    text = links.read_text(encoding="utf-8")
    assert 'receipt-link' not in text


def test_remove_receipt_link_removes_new_block(tmp_path: Path):
    from lamella.features.receipts.linker import remove_document_link

    main = _stub_main_bean(tmp_path)
    links = tmp_path / "connector_links.bean"
    links.write_text(
        "; Managed by Lamella. Do not hand-edit.\n"
        '\n'
        '2026-01-15 custom "document-link" "01900000-0000-7000-8000-aaaaaaaaaaaa"\n'
        '  lamella-paperless-id: 42\n'
        '  lamella-match-method: "auto_sweep"\n'
        '  lamella-match-confidence: 0.95\n',
        encoding="utf-8",
    )
    removed = remove_document_link(
        main_bean=main,
        connector_links=links,
        txn_id="01900000-0000-7000-8000-aaaaaaaaaaaa",
        paperless_id=42,
        run_check=False,
    )
    assert removed is True
    text = links.read_text(encoding="utf-8")
    assert 'document-link' not in text


# ---------------------------------------------------------------------
# Classifier recognizes both vocabularies
# ---------------------------------------------------------------------


def test_classifier_owns_both_vocabularies():
    from lamella.core.bootstrap.classifier import OWNED_CUSTOM_TYPES

    # New vocabulary
    assert "document-link" in OWNED_CUSTOM_TYPES
    assert "document-dismissed" in OWNED_CUSTOM_TYPES
    assert "document-link-blocked" in OWNED_CUSTOM_TYPES
    # Legacy vocabulary still recognized for import classification
    assert "receipt-link" in OWNED_CUSTOM_TYPES
    assert "receipt-dismissed" in OWNED_CUSTOM_TYPES
    assert "receipt-link-blocked" in OWNED_CUSTOM_TYPES


def test_latest_ledger_version_is_four():
    from lamella.core.bootstrap.detection import LATEST_LEDGER_VERSION

    assert LATEST_LEDGER_VERSION == 4
