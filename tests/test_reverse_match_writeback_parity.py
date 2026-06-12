# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0063 §7: side-effect parity.

After a reverse-direction (doc -> txn) auto-link, the document
carries the same ledger directive shape as a forward-direction
(txn -> doc) link. Both routes flow through DocumentLinker.link()
which doesn't know which direction discovered the pair — so the
on-disk artifacts are identical.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from beancount.core import data
from beancount.core.amount import Amount

from lamella.core.db import connect, migrate
from lamella.core.beancount_io.txn_hash import txn_hash
from lamella.features.receipts.linker import DocumentLinker


@pytest.fixture
def db(tmp_path: Path):
    conn = connect(tmp_path / "test.sqlite")
    migrate(conn)
    yield conn
    conn.close()


@pytest.fixture
def ledger(tmp_path: Path):
    """Set up an empty ledger pair (main + connector_links)."""
    main = tmp_path / "main.bean"
    main.write_text('option "name_assets" "Assets"\n')
    connector_links = tmp_path / "connector_links.bean"
    return main, connector_links


def _make_txn():
    amt = Decimal("42.00")
    return data.Transaction(
        meta={"filename": "test", "lineno": 1},
        date=date(2026, 4, 17),
        flag="*",
        payee="Hardware Store",
        narration="HARDWARE STORE",
        tags=set(),
        links=set(),
        postings=[
            data.Posting(
                account="Expenses:Personal:Hardware",
                units=Amount(amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
            data.Posting(
                account="Assets:Personal:Checking",
                units=Amount(-amt, "USD"),
                cost=None, price=None, flag=None, meta=None,
            ),
        ],
    )


def _link_via_method(db, ledger, *, match_method: str):
    """Drive DocumentLinker.link with the given match_method label
    (mimicking either forward or reverse caller). Returns the
    connector_links text after the write."""
    main, connector_links = ledger
    txn = _make_txn()
    h = txn_hash(txn)
    linker = DocumentLinker(
        conn=db,
        main_bean=main,
        connector_links=connector_links,
        run_check=False,  # skip bean-check in this isolated test
    )
    linker.link(
        paperless_id=999,
        txn_hash=h,
        txn_date=txn.date,
        txn_amount=Decimal("42.00"),
        match_method=match_method,
        match_confidence=0.95,
        paperless_hash="md5:abc123",
        paperless_url="https://paperless.test/documents/999/",
    )
    return connector_links.read_text(encoding="utf-8"), h


def test_reverse_link_directive_matches_forward_link_shape(db, ledger):
    """Both directions emit the same custom "document-link" directive
    structure. The only difference is the lamella-match-method line
    (one says forward, the other says reverse) — every other field
    is identical given identical inputs."""
    main, connector_links = ledger

    # Forward-direction write (txn -> doc).
    forward_text, forward_hash = _link_via_method(
        db, ledger, match_method="auto_sweep",
    )

    # Reset for the reverse-direction write so the second write
    # doesn't see the first's link.
    connector_links.unlink(missing_ok=True)
    db.execute("DELETE FROM document_links")
    db.commit()

    reverse_text, reverse_hash = _link_via_method(
        db, ledger, match_method="auto_reverse_sweep",
    )

    # The directive HEAD shape is identical.
    assert 'custom "document-link"' in forward_text
    assert 'custom "document-link"' in reverse_text

    # The same metadata fields appear on both sides.
    for required in (
        "lamella-paperless-id: 999",
        'lamella-paperless-hash: "md5:abc123"',
        'lamella-paperless-url: "https://paperless.test/documents/999/"',
        "lamella-match-confidence: 0.95",
        "lamella-txn-date: 2026-04-17",
        "lamella-txn-amount: 42.00 USD",
    ):
        assert required in forward_text, f"forward missing: {required}"
        assert required in reverse_text, f"reverse missing: {required}"

    # The match-method label is the only on-disk distinction.
    assert 'lamella-match-method: "auto_sweep"' in forward_text
    assert 'lamella-match-method: "auto_reverse_sweep"' in reverse_text


def test_reverse_link_writes_same_db_row_shape(db, ledger):
    """The document_links DB row is shaped identically — same
    columns populated, same types — regardless of which direction
    triggered the write."""
    _link_via_method(db, ledger, match_method="auto_reverse_sweep")
    rows = db.execute(
        "SELECT paperless_id, paperless_hash, txn_hash, txn_date, "
        "       txn_amount, match_method, match_confidence "
        "FROM document_links"
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["paperless_id"] == 999
    assert row["paperless_hash"] == "md5:abc123"
    assert row["match_method"] == "auto_reverse_sweep"
    # ADR-0022: match_confidence is stored as REAL; compare numerically.
    assert abs(row["match_confidence"] - 0.95) < 1e-9
    # ADR-0022: txn_amount is TEXT (Decimal canonical).
    assert row["txn_amount"] == "42.00"
    assert row["txn_date"] == "2026-04-17"
