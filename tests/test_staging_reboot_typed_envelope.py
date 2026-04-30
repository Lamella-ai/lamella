# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0057 §1 round-trip property test for the typed-envelope extract.

The reboot flow is a round-trip ETL whose validation step is a
per-transaction property:

    serialize(parse(serialize(parse(text)))) == serialize(parse(text))

Step one of that property is the *extract*: parsing every captured
value into a typed envelope so the eventual serializer can re-emit it
in canonical form (LEDGER_LAYOUT.md §6.3). This test exercises the
extract layer directly — it doesn't yet test the serializer (a later
ADR-0057 follow-up) — but it locks in the contract the serializer will
build against.

What we assert:
- Booleans, integers, decimals, dates, and strings round-trip with
  their Beancount type intact.
- Posting-level metadata (the paired ``lamella-source-N`` /
  ``lamella-source-reference-id-N`` keys ADR-0019 cares about) is
  captured per-posting, not flattened onto the txn or dropped.
- ``flag``, ``tags``, ``links``, and per-posting ``cost`` / ``price``
  / ``flag`` are captured.
- Internal Beancount keys (``filename`` / ``lineno`` / dunder) are NOT
  carried into the envelope — they're ``source_ref`` data.
"""
from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging.reboot import (
    RebootService,
    _capture_amount,
    _capture_cost,
    _typed_meta_list,
    _typed_meta_value,
)


# --- pure helpers -------------------------------------------------------


class TestTypedMetaValue:
    """``_typed_meta_value`` is the per-value type-tagger. Order
    matters in the implementation (bool is a subclass of int in
    Python) so we exercise both paths separately."""

    def test_boolean_true_keeps_type(self):
        assert _typed_meta_value(True) == {
            "type": "boolean", "value": True
        }

    def test_boolean_false_keeps_type(self):
        # Distinct from integer 0 — the serializer must emit ``FALSE``,
        # not ``0``.
        out = _typed_meta_value(False)
        assert out["type"] == "boolean"
        assert out["value"] is False

    def test_integer_not_boolean(self):
        # Regression guard: bool is a subclass of int. The check order
        # must be bool-first.
        out = _typed_meta_value(2025)
        assert out == {"type": "integer", "value": 2025}

    def test_decimal_serializes_as_string(self):
        # Decimal can't go through json.dumps natively; the envelope
        # stringifies but tags as decimal so the serializer knows to
        # emit a bare number, not a quoted string.
        out = _typed_meta_value(Decimal("1.50"))
        assert out == {"type": "decimal", "value": "1.50"}

    def test_date_iso_format(self):
        out = _typed_meta_value(datetime.date(2025, 6, 15))
        assert out == {"type": "date", "value": "2025-06-15"}

    def test_string_passthrough(self):
        assert _typed_meta_value("hello") == {
            "type": "string", "value": "hello"
        }

    def test_unknown_type_tagged(self):
        # An unrecognized Python type must surface — silently
        # stringifying loses round-trip fidelity. Tag with 'unknown'
        # so the property test can detect regressions.
        class Weird:
            def __str__(self):
                return "weird"

        out = _typed_meta_value(Weird())
        assert out["type"] == "unknown"
        assert out["value"] == "weird"
        assert out["python_type"] == "Weird"


class TestTypedMetaList:
    """``_typed_meta_list`` walks an entry/posting meta dict and
    drops parser-injected keys."""

    def test_empty_meta_yields_empty_list(self):
        assert _typed_meta_list({}) == []
        assert _typed_meta_list(None) == []

    def test_filename_and_lineno_are_dropped(self):
        # These come from the parser, not from user-authored metadata.
        # They live in source_ref, not the typed envelope.
        out = _typed_meta_list({
            "filename": "/some/file.bean",
            "lineno": 42,
            "lamella-txn-id": "abc",
        })
        keys = [item["key"] for item in out]
        assert "filename" not in keys
        assert "lineno" not in keys
        assert "lamella-txn-id" in keys

    def test_dunder_keys_are_dropped(self):
        # Beancount internals (e.g. __tolerances__) shouldn't surface
        # in the user-visible metadata layer.
        out = _typed_meta_list({"__internal__": "skip", "real": "keep"})
        keys = [item["key"] for item in out]
        assert "__internal__" not in keys
        assert "real" in keys

    def test_preserves_insertion_order(self):
        # The serializer emits meta in source order; the envelope must
        # carry that order.
        out = _typed_meta_list({
            "z": "1",
            "a": "2",
            "m": "3",
        })
        assert [item["key"] for item in out] == ["z", "a", "m"]

    def test_each_entry_carries_full_envelope(self):
        out = _typed_meta_list({
            "tax-year": 2025,
            "reimbursable": True,
            "purchase-date": datetime.date(2025, 6, 15),
            "amount-spent": Decimal("99.99"),
            "memo": "office supplies",
        })
        by_key = {e["key"]: e for e in out}
        assert by_key["tax-year"] == {
            "key": "tax-year", "type": "integer", "value": 2025,
        }
        assert by_key["reimbursable"] == {
            "key": "reimbursable", "type": "boolean", "value": True,
        }
        assert by_key["purchase-date"] == {
            "key": "purchase-date", "type": "date",
            "value": "2025-06-15",
        }
        assert by_key["amount-spent"] == {
            "key": "amount-spent", "type": "decimal", "value": "99.99",
        }
        assert by_key["memo"] == {
            "key": "memo", "type": "string", "value": "office supplies",
        }


class TestCaptureAmount:
    def test_none_returns_none(self):
        assert _capture_amount(None) is None

    def test_amount_with_value(self):
        # Synthesize a duck-typed Amount; we don't import the real
        # type because the helper is intentionally
        # structurally-typed (it has to handle Cost / CostSpec /
        # Amount uniformly).
        from beancount.core.amount import Amount
        a = Amount(Decimal("1.25"), "USD")
        assert _capture_amount(a) == {
            "number": "1.25", "currency": "USD"
        }


class TestCaptureCost:
    def test_none_returns_none(self):
        assert _capture_cost(None) is None

    def test_cost_with_full_fields(self):
        from beancount.core.position import Cost
        c = Cost(
            number=Decimal("100.00"),
            currency="USD",
            date=datetime.date(2025, 1, 1),
            label="lot-1",
        )
        out = _capture_cost(c)
        assert out["number"] == "100.00"
        assert out["currency"] == "USD"
        assert out["date"] == "2025-01-01"
        assert out["label"] == "lot-1"


# --- end-to-end against a real ledger ----------------------------------


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _write_ledger_with_typed_meta(dir_: Path) -> Path:
    """Build a ledger with txn-level + posting-level metadata in every
    Beancount type the envelope must support, plus tags / links /
    cost. Mirrors the shape a hand-written or foreign-tool ledger
    might land in for the reboot extract to chew on.
    """
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Liabilities:Card USD\n"
        "2020-01-01 open Expenses:Food USD\n"
        "\n"
        '2025-06-15 * "Coffee Shop" "Decaf and a scone" #tag-one ^link-one\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-000000000001"\n'
        '  tax-year: 2025\n'
        '  reimbursable: TRUE\n'
        '  purchase-date: 2025-06-15\n'
        '  memo: "office supplies"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-AAA"\n'
        '  Expenses:Food      12.50 USD\n',
        encoding="utf-8",
    )
    return main


def test_extract_captures_typed_txn_and_posting_meta(conn, tmp_path):
    """ADR-0057 §1: a real reboot scan stages a row whose ``raw_json``
    carries the typed envelope. Drives the production extract path
    end-to-end: parse → stage → read back → assert."""
    main = _write_ledger_with_typed_meta(tmp_path)
    reader = LedgerReader(main_bean=main)
    svc = RebootService(conn)
    result = svc.scan_ledger(reader, detect_duplicates=False)
    assert result.staged == 1, (
        f"expected exactly one staged row; got {result.staged} "
        f"(errors={result.errors})"
    )

    row = conn.execute(
        "SELECT raw_json FROM staged_transactions WHERE source = 'reboot'"
    ).fetchone()
    assert row is not None, "reboot scan didn't stage a row"
    raw = json.loads(row["raw_json"])

    # Header-line capture.
    assert raw["flag"] == "*"
    assert raw["tags"] == ["tag-one"]
    assert raw["links"] == ["link-one"]

    # Txn-level meta — every type tagged correctly.
    by_key = {item["key"]: item for item in raw["txn_meta"]}

    # Internal parser keys must be filtered out.
    assert "filename" not in by_key
    assert "lineno" not in by_key

    assert by_key["lamella-txn-id"]["type"] == "string"
    assert by_key["lamella-txn-id"]["value"] == (
        "0190f000-0000-7000-8000-000000000001"
    )
    # Beancount parses bare numbers as Decimal — there is no integer
    # type at the parser layer. The envelope's ``integer`` tag is for
    # values set programmatically from Python (cleaner pipeline,
    # tests). The serializer renders both as a bare number, so the
    # round-trip property holds either way.
    assert by_key["tax-year"]["type"] == "decimal"
    assert by_key["tax-year"]["value"] == "2025"
    assert by_key["reimbursable"]["type"] == "boolean"
    assert by_key["reimbursable"]["value"] is True
    assert by_key["purchase-date"]["type"] == "date"
    assert by_key["purchase-date"]["value"] == "2025-06-15"
    assert by_key["memo"]["type"] == "string"
    assert by_key["memo"]["value"] == "office supplies"

    # Posting-level capture — paired source meta survives.
    postings = raw["postings"]
    assert len(postings) == 2
    card = next(
        p for p in postings if p["account"] == "Liabilities:Card"
    )
    food = next(
        p for p in postings if p["account"] == "Expenses:Food"
    )

    card_meta = {item["key"]: item for item in card["meta"]}
    assert card_meta["lamella-source-0"]["type"] == "string"
    assert card_meta["lamella-source-0"]["value"] == "simplefin"
    assert card_meta["lamella-source-reference-id-0"]["type"] == "string"
    assert (
        card_meta["lamella-source-reference-id-0"]["value"] == "TRN-AAA"
    )

    # The Expenses leg has no posting-level meta — the envelope must
    # still be a list (empty), not None/missing.
    assert food["meta"] == []

    # Posting amount captures preserve sign + currency.
    assert card["amount"] == "-12.50"
    assert card["currency"] == "USD"
    assert food["amount"] == "12.50"
    assert food["currency"] == "USD"


def test_extract_captures_posting_cost(conn, tmp_path):
    """Lots — ``Posting.cost`` — must be captured so the eventual
    re-emit serializer can reproduce the lot lifetime correctly."""
    main = tmp_path / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Brokerage:AAPL AAPL\n"
        "2020-01-01 open Assets:Bank USD\n"
        "\n"
        '2025-03-01 * "Buy AAPL"\n'
        '  Assets:Brokerage:AAPL    10 AAPL {180.00 USD}\n'
        '  Assets:Bank          -1800.00 USD\n',
        encoding="utf-8",
    )
    reader = LedgerReader(main_bean=main)
    svc = RebootService(conn)
    result = svc.scan_ledger(reader, detect_duplicates=False)
    assert result.staged == 1, (
        f"errors={result.errors}; result={result}"
    )

    row = conn.execute(
        "SELECT raw_json FROM staged_transactions WHERE source = 'reboot'"
    ).fetchone()
    raw = json.loads(row["raw_json"])
    aapl_leg = next(
        p for p in raw["postings"]
        if p["account"] == "Assets:Brokerage:AAPL"
    )
    assert aapl_leg["cost"] is not None, (
        "lot cost was dropped during extract"
    )
    # Beancount books the lot — Cost has number/currency/date.
    cost = aapl_leg["cost"]
    assert cost["number"] == "180.00"
    assert cost["currency"] == "USD"
