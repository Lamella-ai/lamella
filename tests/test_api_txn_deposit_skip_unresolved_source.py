# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware
# financial intelligence
# https://lamella.ai

"""The ``/api/txn/{ref}/ask-ai`` deposit-skip preflight uses the
*universal* sign convention — positive bank-side amount = deposit
(skip AI), negative = run AI to classify as expense. Same rule for
asset and liability sources, verified against actual ledger writes
(``Liabilities:CC -9.49`` for charges, ``+20.95`` for refunds).

Earlier code branched the rule on ``accounts_meta.kind`` and the
account-path root, with the liability convention *inverted* from the
asset one. That inversion silently routed every CC charge to the
deposit panel (no AI) and every CC refund to the AI's expense
whitelist — exactly the user-reported bug ("negative money treated
as deposit", "refund treated as expense"). The fix removed the
account-kind branching entirely; these tests pin the simplification
so it doesn't get reintroduced.
"""
from __future__ import annotations

import inspect


def _deposit_block() -> str:
    """Return the substring of api_txn_ask_ai's deposit-skip preflight
    — from ``_is_deposit = False`` to ``if _is_deposit:``. Limits
    later assertions to the relevant block so unrelated occurrences in
    the file (e.g., the refund-candidate lookup) don't false-match."""
    import lamella.web.routes.api_txn as api_txn_mod
    src = inspect.getsource(api_txn_mod.api_txn_ask_ai)
    start = src.index("_is_deposit = False")
    end = src.index("if _is_deposit:", start)
    return src[start:end]


class TestDepositDetectionUsesUniversalConvention:
    def test_no_account_kind_branching(self):
        """The deposit-skip preflight must not branch on
        accounts_meta.kind — the universal convention applies
        regardless of whether the source is checking, savings, or a
        credit card."""
        block = _deposit_block()
        assert "credit_card" not in block
        assert "line_of_credit" not in block
        assert "_is_liability_kind" not in block

    def test_no_account_path_root_inversion(self):
        """The deposit-skip preflight must not inspect the source
        path root to flip the sign comparison. Earlier code used
        ``startswith("Liabilities:")`` to invert the convention,
        which is the bug we removed."""
        block = _deposit_block()
        assert 'startswith("Liabilities:")' not in block, (
            "Deposit-skip preflight must not branch on the source "
            "account path root — universal convention applies"
        )

    def test_staged_path_uses_simple_positive_check(self):
        """The staged branch sets ``_is_deposit = _amt > 0`` directly
        — no account-kind / path resolution needed."""
        block = _deposit_block()
        assert "_is_deposit = _amt > 0" in block, (
            "Staged-path deposit detection must use the universal "
            "positive-amount check; got block:\n" + block
        )

    def test_ledger_path_uses_simple_negative_fixme_check(self):
        """The ledger branch sets ``_is_deposit = _fixme_amt < 0``
        directly — FIXME leg has the opposite sign of the bank side,
        so negative FIXME = positive bank-side = deposit."""
        block = _deposit_block()
        assert "_is_deposit = _fixme_amt < 0" in block, (
            "Ledger-path deposit detection must use the universal "
            "negative-FIXME check; got block:\n" + block
        )
