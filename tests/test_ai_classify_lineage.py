# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Phase 3 of NORMALIZE_TXN_IDENTITY.md — AI classify decisions key
off the entry's ``lamella-txn-id`` (lineage) when present, falling
back to the Beancount ``txn_hash`` for legacy pre-Phase-4 entries.

These tests pin the input_ref selection logic at every classify call
site without exercising the full AI cascade — they patch the AI client
so the test asserts what's *passed* to the model, not what comes back.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from lamella.features.ai_cascade.classify import propose_account
from lamella.features.ai_cascade.context import TxnForClassify


def _txn(
    *,
    lineage: str | None = None,
    txn_hash: str = "deadbeef" * 5,
) -> TxnForClassify:
    return TxnForClassify(
        date=date(2026, 4, 15),
        amount=Decimal("42.17"),
        currency="USD",
        payee="Hardware Store",
        narration="Supplies",
        card_account="Liabilities:Acme:Card",
        fixme_account="Expenses:Acme:FIXME",
        txn_hash=txn_hash,
        lamella_txn_id=lineage,
    )


def _mock_client(*, low_confidence: bool = False) -> MagicMock:
    """Build a stub OpenRouter client whose .chat returns a parseable
    AIResult with the requested confidence."""
    from lamella.adapters.openrouter.client import AIResult

    response = MagicMock()
    response.target_account = "Expenses:Acme:Supplies"
    response.confidence = 0.3 if low_confidence else 0.9
    response.reasoning = ""
    response.intercompany_flag = False
    response.owning_entity = None

    result = AIResult(
        data=response,
        decision_id=1,
        prompt_tokens=10,
        completion_tokens=10,
        model="anthropic/claude-haiku-4.5",
        cached=False,
    )
    client = MagicMock()
    client.chat = AsyncMock(return_value=result)
    return client


@pytest.mark.asyncio
async def test_propose_account_uses_lineage_when_present():
    client = _mock_client()
    txn = _txn(lineage="0190fe22-7c10-7000-8000-aaaaaaaaaaaa")
    await propose_account(
        client,
        txn=txn,
        similar=[],
        valid_accounts=["Expenses:Acme:Supplies"],
        entity="Acme",
    )
    call_kwargs = client.chat.await_args.kwargs
    assert call_kwargs["input_ref"] == "0190fe22-7c10-7000-8000-aaaaaaaaaaaa"
    assert call_kwargs["decision_type"] == "classify_txn"


@pytest.mark.asyncio
async def test_propose_account_falls_back_to_txn_hash_when_no_lineage():
    """Pre-Phase-4 legacy entries don't carry a lineage id. Until the
    transform stamps them, the classifier must still log a usable
    input_ref (the content hash) so AI history pins to the entry."""
    client = _mock_client()
    txn = _txn(lineage=None, txn_hash="abc123def456" + "0" * 28)
    await propose_account(
        client,
        txn=txn,
        similar=[],
        valid_accounts=["Expenses:Acme:Supplies"],
        entity="Acme",
    )
    assert client.chat.await_args.kwargs["input_ref"] == "abc123def456" + "0" * 28


@pytest.mark.asyncio
async def test_propose_account_fallback_model_uses_same_input_ref():
    """Both primary + fallback calls log under the same input_ref so
    the per-txn history page sees them as a coherent thread, not two
    disconnected events."""
    client = _mock_client(low_confidence=True)
    txn = _txn(lineage="0190fe22-7c10-7000-8000-bbbbbbbbbbbb")
    await propose_account(
        client,
        txn=txn,
        similar=[],
        valid_accounts=["Expenses:Acme:Supplies"],
        entity="Acme",
        fallback_model="anthropic/claude-opus-4-7",
        fallback_threshold=0.7,
    )
    # Two calls — both with the same lineage input_ref.
    assert client.chat.await_count == 2
    refs = [c.kwargs["input_ref"] for c in client.chat.await_args_list]
    assert refs == [
        "0190fe22-7c10-7000-8000-bbbbbbbbbbbb",
        "0190fe22-7c10-7000-8000-bbbbbbbbbbbb",
    ]


def test_extract_fixme_txn_populates_lineage_from_meta():
    """``extract_fixme_txn`` is the bridge from Beancount entries into
    ``TxnForClassify``. It must read ``lamella-txn-id`` off the
    transaction meta so propose_account sees the lineage."""
    from beancount.core import data as bdata
    from beancount.core.amount import Amount
    from beancount.core.number import D

    from lamella.features.ai_cascade.context import extract_fixme_txn

    posting_card = bdata.Posting(
        account="Liabilities:Acme:Card",
        units=Amount(D("-42.17"), "USD"),
        cost=None, price=None, flag=None, meta={},
    )
    posting_fixme = bdata.Posting(
        account="Expenses:Acme:FIXME",
        units=Amount(D("42.17"), "USD"),
        cost=None, price=None, flag=None, meta={},
    )
    txn = bdata.Transaction(
        meta={
            "filename": "<test>", "lineno": 1,
            "lamella-txn-id": "0190fe22-7c10-7000-8000-cccccccccccc",
        },
        date=date(2026, 4, 15),
        flag="*", payee="Acme", narration="thing",
        tags=frozenset(), links=frozenset(),
        postings=[posting_card, posting_fixme],
    )
    result = extract_fixme_txn(txn)
    assert result is not None
    assert result.lamella_txn_id == "0190fe22-7c10-7000-8000-cccccccccccc"


def test_extract_fixme_txn_lineage_none_when_meta_absent():
    from beancount.core import data as bdata
    from beancount.core.amount import Amount
    from beancount.core.number import D

    from lamella.features.ai_cascade.context import extract_fixme_txn

    posting_card = bdata.Posting(
        account="Liabilities:Acme:Card",
        units=Amount(D("-42.17"), "USD"),
        cost=None, price=None, flag=None, meta={},
    )
    posting_fixme = bdata.Posting(
        account="Expenses:Acme:FIXME",
        units=Amount(D("42.17"), "USD"),
        cost=None, price=None, flag=None, meta={},
    )
    txn = bdata.Transaction(
        meta={"filename": "<test>", "lineno": 1},
        date=date(2026, 4, 15),
        flag="*", payee="Acme", narration="thing",
        tags=frozenset(), links=frozenset(),
        postings=[posting_card, posting_fixme],
    )
    result = extract_fixme_txn(txn)
    assert result is not None
    assert result.lamella_txn_id is None
