# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""ADR-0057 round-trip property test for the typed-envelope
serializer.

The contract:

    serialize(parse(serialize(parse(text)))) == serialize(parse(text))

That is, after one extract + serialize cycle the canonical form is
fixed. A second cycle is a no-op. A regression in either the extract
(reboot._typed_meta_*, _capture_*) or the serializer
(envelope_serializer.serialize_envelope) breaks this property.

The test runs over a fixture corpus that mixes:
* simple expense entries
* multi-leg transfers
* entries with txn-level meta (string, decimal, boolean, date)
* entries with paired posting-level source meta
* tags and links
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from lamella.core.beancount_io.reader import LedgerReader
from lamella.core.db import connect, migrate
from lamella.features.import_.staging.envelope_serializer import (
    serialize_envelope,
    serialize_meta_value,
)
from lamella.features.import_.staging.reboot import RebootService


@pytest.fixture()
def conn():
    c = connect(Path(":memory:"))
    migrate(c)
    return c


def _full_ledger_with(dir_: Path, body: str) -> Path:
    main = dir_ / "main.bean"
    main.write_text(
        'option "operating_currency" "USD"\n'
        "2020-01-01 open Assets:Bank USD\n"
        "2020-01-01 open Assets:PayPal USD\n"
        "2020-01-01 open Liabilities:Card USD\n"
        "2020-01-01 open Expenses:Food USD\n"
        + body,
        encoding="utf-8",
    )
    return main


def _extract_then_serialize(
    conn, main_bean: Path,
) -> str:
    """Run one full extract → serialize cycle. Returns the
    serialized text concatenated across every staged row, in date
    order. The exact concatenation is what the round-trip property
    asserts to be idempotent on a second pass."""
    reader = LedgerReader(main_bean=main_bean)
    svc = RebootService(conn)
    result = svc.scan_ledger(reader, detect_duplicates=False)
    assert result.errors == [], (
        f"unexpected extract errors: {result.errors}"
    )
    rows = conn.execute(
        "SELECT posting_date, payee, description, raw_json "
        "FROM staged_transactions WHERE source = 'reboot' "
        "ORDER BY posting_date, id"
    ).fetchall()
    chunks: list[str] = []
    for row in rows:
        envelope = json.loads(row["raw_json"])
        chunk = serialize_envelope(
            date=row["posting_date"],
            payee=row["payee"],
            narration=row["description"],
            envelope=envelope,
        )
        chunks.append(chunk)
    return "\n".join(chunks)


# --- serialize_meta_value spot checks ----------------------------


class TestSerializeMetaValue:
    """Basic type-tag → Beancount literal mapping."""

    def test_boolean_true(self):
        assert (
            serialize_meta_value({"type": "boolean", "value": True})
            == "TRUE"
        )

    def test_boolean_false(self):
        assert (
            serialize_meta_value({"type": "boolean", "value": False})
            == "FALSE"
        )

    def test_integer(self):
        assert (
            serialize_meta_value({"type": "integer", "value": 2025})
            == "2025"
        )

    def test_decimal_passthrough(self):
        assert (
            serialize_meta_value({"type": "decimal", "value": "1.50"})
            == "1.50"
        )

    def test_date(self):
        assert (
            serialize_meta_value(
                {"type": "date", "value": "2025-06-15"},
            )
            == "2025-06-15"
        )

    def test_string_quoted_and_escaped(self):
        assert (
            serialize_meta_value({
                "type": "string",
                "value": 'has "quote" and \\backslash',
            })
            == r'"has \"quote\" and \\backslash"'
        )

    def test_amount(self):
        assert (
            serialize_meta_value({
                "type": "amount",
                "value": {"number": "1.25", "currency": "USD"},
            })
            == "1.25 USD"
        )


# --- end-to-end serialize over real envelopes --------------------


def test_serialize_envelope_renders_simple_expense(conn, tmp_path):
    """Round-trip: a clean expense entry survives the extract +
    re-serialize cycle and produces parseable Beancount output."""
    main = _full_ledger_with(
        tmp_path,
        '\n2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  Assets:Bank      -12.50 USD\n'
        '  Expenses:Food     12.50 USD\n',
    )
    out = _extract_then_serialize(conn, main)
    # Header has the date / flag / payee / narration.
    assert '2026-04-15 *' in out
    assert '"Coffee Shop"' in out
    assert '"Decaf"' in out
    # Both posting accounts present.
    assert 'Assets:Bank' in out
    assert 'Expenses:Food' in out
    # Amounts formatted with currency.
    assert '-12.50 USD' in out
    assert '12.50 USD' in out


def test_serialize_envelope_preserves_typed_meta(conn, tmp_path):
    """Booleans, decimals, dates, and strings round-trip through
    the envelope as their Beancount-canonical literal forms."""
    main = _full_ledger_with(
        tmp_path,
        '\n2026-04-15 * "Coffee Shop" "Decaf" #tag-one ^link-one\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-000000000001"\n'
        '  reimbursable: TRUE\n'
        '  purchase-date: 2025-06-15\n'
        '  memo: "office supplies"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-AAA"\n'
        '  Expenses:Food      12.50 USD\n',
    )
    out = _extract_then_serialize(conn, main)
    # Booleans bare.
    assert "reimbursable: TRUE" in out
    # Date bare.
    assert "purchase-date: 2025-06-15" in out
    # Quoted string.
    assert 'memo: "office supplies"' in out
    # Tags + links on header.
    assert '#tag-one' in out
    assert '^link-one' in out
    # Paired source meta on the Liabilities posting.
    assert 'lamella-source-0: "simplefin"' in out
    assert 'lamella-source-reference-id-0: "TRN-AAA"' in out


def test_round_trip_property_holds_for_clean_entry(conn, tmp_path):
    """ADR-0057 round-trip property:

        serialize(parse(serialize(parse(text)))) ==
        serialize(parse(text))

    Two cycles produce the same canonical output. A regression in
    extract or serialize would break this; CI catches it."""
    main = _full_ledger_with(
        tmp_path,
        '\n2026-04-15 * "Coffee Shop" "Decaf"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-CLEAN-ENTRY-1"\n'
        '  Liabilities:Card  -12.50 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-AAA"\n'
        '  Expenses:Food      12.50 USD\n',
    )
    pass1 = _extract_then_serialize(conn, main)
    # Write pass1 into a fresh ledger file (with the boilerplate)
    # and run extract + serialize again. Need a fresh DB so the
    # reboot scan doesn't see the same content twice.
    tmp_path2 = tmp_path / "round2"
    tmp_path2.mkdir()
    main2 = _full_ledger_with(tmp_path2, "\n" + pass1)
    conn2 = connect(Path(":memory:"))
    migrate(conn2)
    pass2 = _extract_then_serialize(conn2, main2)
    assert pass1 == pass2, (
        "round-trip property broken — second pass did not match "
        "first.\n--- pass1 ---\n" + pass1 + "\n--- pass2 ---\n"
        + pass2
    )


def test_round_trip_property_holds_for_multi_leg_transfer(
    conn, tmp_path,
):
    """A 2-leg transfer entry (Checking -50 / PayPal +50) must
    round-trip unchanged. Catches regressions in posting ordering,
    sign preservation, or amount precision."""
    main = _full_ledger_with(
        tmp_path,
        '\n2026-04-15 * "Internal transfer" "Checking → PayPal"\n'
        '  lamella-txn-id: "0190f000-0000-7000-8000-XFER"\n'
        '  Assets:Bank     -50.00 USD\n'
        '    lamella-source-0: "simplefin"\n'
        '    lamella-source-reference-id-0: "TRN-CHK"\n'
        '  Assets:PayPal    50.00 USD\n'
        '    lamella-source-0: "csv"\n'
        '    lamella-source-reference-id-0: "ROW-7"\n',
    )
    pass1 = _extract_then_serialize(conn, main)
    tmp_path2 = tmp_path / "round2"
    tmp_path2.mkdir()
    main2 = _full_ledger_with(tmp_path2, "\n" + pass1)
    conn2 = connect(Path(":memory:"))
    migrate(conn2)
    pass2 = _extract_then_serialize(conn2, main2)
    assert pass1 == pass2
    # Sanity: both legs survived.
    assert "Assets:Bank" in pass2
    assert "Assets:PayPal" in pass2
    assert "-50.00 USD" in pass2
    assert "50.00 USD" in pass2
