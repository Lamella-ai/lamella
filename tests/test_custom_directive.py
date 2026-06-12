# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from lamella.core.transform.custom_directive import (
    Account,
    Amount,
    render_directive,
)


def test_minimal_directive_no_args_no_meta():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="x",
    )
    assert out == '\n2026-04-21 custom "x"\n'


def test_directive_with_string_and_account_args():
    out = render_directive(
        directive_date=date(2026, 3, 15),
        directive_type="recurring-confirmed",
        args=["Streaming", Account("Expenses:Personal:Subscriptions:Streaming")],
    )
    assert out == (
        '\n2026-03-15 custom "recurring-confirmed" '
        '"Streaming" Expenses:Personal:Subscriptions:Streaming\n'
    )


def test_directive_with_amount_arg():
    out = render_directive(
        directive_date=date(2026, 1, 1),
        directive_type="budget",
        args=[
            Account("Expenses:Personal:Food"),
            "USD",
            "monthly",
            Amount(Decimal("600.00"), "USD"),
        ],
    )
    assert 'Expenses:Personal:Food "USD" "monthly" 600.00 USD' in out


def test_metadata_keys_must_be_bcg_prefixed():
    with pytest.raises(ValueError, match="must be lamella-"):
        render_directive(
            directive_date=date(2026, 4, 21),
            directive_type="x",
            meta={"rogue-key": "v"},
        )


def test_metadata_renders_bool_as_bare_token():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="paperless-field",
        args=[42, "vendor"],
        meta={"lamella-auto-assigned": False},
    )
    # Must be bare FALSE, NOT "FALSE".
    assert "  lamella-auto-assigned: FALSE\n" in out
    assert '"FALSE"' not in out


def test_metadata_renders_strings_quoted():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="x",
        meta={"lamella-reason": "cash tip"},
    )
    assert '  lamella-reason: "cash tip"\n' in out


def test_metadata_renders_account_bare():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="x",
        meta={"lamella-target-account": Account("Expenses:Personal:Food")},
    )
    assert "  lamella-target-account: Expenses:Personal:Food\n" in out
    assert '"Expenses:Personal:Food"' not in out


def test_metadata_renders_amount_with_currency():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="x",
        meta={"lamella-amount-hint": Amount(Decimal("14.99"), "USD")},
    )
    assert "  lamella-amount-hint: 14.99 USD\n" in out


def test_metadata_renders_datetime_quoted_iso():
    dt = datetime(2026, 4, 21, 14, 32, 7)
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="x",
        meta={"lamella-confirmed-at": dt},
    )
    assert '  lamella-confirmed-at: "2026-04-21T14:32:07"\n' in out


def test_metadata_renders_date_bare():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="x",
        meta={"lamella-effective-on": date(2026, 1, 1)},
    )
    assert "  lamella-effective-on: 2026-01-01\n" in out


def test_string_quoting_handles_special_chars():
    out = render_directive(
        directive_date=date(2026, 4, 21),
        directive_type="classification-rule",
        args=['grocer "whole foods"\\backslash'],
    )
    assert r'"grocer \"whole foods\"\\backslash"' in out
