# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 2 of NORMALIZE_TXN_IDENTITY.md — importer emit writes the
new identity schema: lineage at txn level, paired indexed source on
the bank-side posting. Legacy txn-level keys remain for read-side
compat until Phase 6.
"""
from __future__ import annotations

import re
from datetime import date

from lamella.features.import_.emit import _natural_key_hash, render_transaction


def _row(**overrides) -> dict:
    base = {
        "date": date(2026, 4, 15),
        "amount": 42.17,
        "payee": "Hardware Store",
        "description": "Supplies",
        "memo": "",
        "source_id": 7,
        "row_num": 3,
        "transaction_id": "AMZ-ORDER-99",
    }
    base.update(overrides)
    return base


def test_render_transaction_emits_lineage_id():
    lines = render_transaction(_row(), "Liabilities:Acme:Card", "Expenses:FIXME", import_id=42)
    body = "\n".join(lines)
    assert "lamella-txn-id:" in body
    # Distinct id per emit.
    a = "\n".join(render_transaction(_row(), "Liabilities:Acme:Card", "Expenses:FIXME", import_id=42))
    b = "\n".join(render_transaction(_row(), "Liabilities:Acme:Card", "Expenses:FIXME", import_id=42))
    ids = re.findall(r'lamella-txn-id: "([^"]+)"', a + b)
    assert len(ids) == 2 and ids[0] != ids[1]


def test_render_transaction_emits_csv_source_on_bank_posting_with_real_id():
    """When the CSV row provides a ``transaction_id``, the paired
    indexed source uses it directly — durable, source-of-truth id."""
    lines = render_transaction(
        _row(transaction_id="AMZ-ORDER-99"),
        "Liabilities:Acme:Card",
        "Expenses:FIXME",
        import_id=42,
    )
    body = "\n".join(lines)
    assert 'lamella-source-0: "csv"' in body
    assert 'lamella-source-reference-id-0: "AMZ-ORDER-99"' in body


def test_render_transaction_falls_back_to_natural_key_hash():
    """When the CSV has no ``transaction_id`` (common for plain bank
    exports), the importer synthesizes a reconstruct-stable hash of
    (date, amount, payee, description) — same content always yields
    the same id."""
    row = _row(transaction_id=None)
    lines_a = render_transaction(row, "Assets:Bank", "Expenses:FIXME", import_id=42)
    lines_b = render_transaction(row, "Assets:Bank", "Expenses:FIXME", import_id=42)
    body_a = "\n".join(lines_a)
    body_b = "\n".join(lines_b)
    refs_a = re.findall(r'lamella-source-reference-id-0: "([^"]+)"', body_a)
    refs_b = re.findall(r'lamella-source-reference-id-0: "([^"]+)"', body_b)
    assert refs_a == refs_b, "natural-key hash must be deterministic"
    assert refs_a[0].startswith("nk-"), "fallback ref id should be prefixed nk-"


def test_natural_key_hash_changes_when_content_changes():
    h1 = _natural_key_hash(date(2026, 4, 15), 42.17, "Acme", "Supplies")
    h2 = _natural_key_hash(date(2026, 4, 15), 42.17, "Acme", "Different desc")
    h3 = _natural_key_hash(date(2026, 4, 16), 42.17, "Acme", "Supplies")
    h4 = _natural_key_hash(date(2026, 4, 15), 42.18, "Acme", "Supplies")
    assert len({h1, h2, h3, h4}) == 4


def test_render_transaction_does_not_emit_retired_legacy_keys():
    """Phase 7 of NORMALIZE_TXN_IDENTITY: writer no longer emits the
    retired identifier keys (``lamella-import-id`` is a SQLite PK,
    ``lamella-import-source`` is debug, ``lamella-import-txn-id`` is
    replaced by posting-level paired source meta). Legacy on-disk
    content carrying these still parses transparently via
    ``_legacy_meta.normalize_entries``; new writes are clean."""
    lines = render_transaction(
        _row(memo="bank-side memo distinct from description"),
        "Assets:Bank", "Expenses:FIXME", import_id=42,
    )
    body = "\n".join(lines)
    assert "lamella-import-id" not in body
    assert "lamella-import-txn-id" not in body
    assert "lamella-import-source" not in body
    # The user-content memo key is preserved — it carries the bank
    # memo column, not an identifier.
    assert "lamella-import-memo:" in body
    # And the new schema is what's actually written.
    assert "lamella-txn-id:" in body
    assert 'lamella-source-0: "csv"' in body
    assert 'lamella-source-reference-id-0: "AMZ-ORDER-99"' in body


def test_render_transaction_parses_under_beancount_loader(tmp_path):
    """End-to-end sanity: the rendered output must be valid Beancount.
    A bad escape or indent in the new posting meta lines would 500
    every importer write."""
    from beancount import loader

    lines = render_transaction(
        _row(transaction_id='id with "quotes"'),
        "Assets:Bank",
        "Expenses:FIXME",
        import_id=42,
    )
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        '2020-01-01 open Assets:Bank USD\n'
        '2020-01-01 open Expenses:FIXME USD\n'
        + "\n".join(lines),
        encoding="utf-8",
    )
    entries, errors, _ = loader.load_file(str(main))
    assert not errors, errors
    txns = [e for e in entries if e.__class__.__name__ == "Transaction"]
    assert len(txns) == 1
    assert "lamella-txn-id" in txns[0].meta
    bank_posting = txns[0].postings[0]
    assert bank_posting.meta is not None
    assert bank_posting.meta.get("lamella-source-0") == "csv"
