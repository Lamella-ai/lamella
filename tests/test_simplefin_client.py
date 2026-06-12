# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from lamella.adapters.simplefin.client import (
    SimpleFINAuthError,
    SimpleFINClient,
    SimpleFINError,
    _split_access_url,
)

FIXTURES = Path(__file__).parent / "fixtures" / "simplefin"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_split_access_url_happy_path():
    base, auth = _split_access_url("https://user:pw@bridge.example/simplefin")
    assert base == "https://bridge.example/simplefin"
    assert auth.startswith("Basic ")


def test_split_access_url_rejects_missing_creds():
    with pytest.raises(SimpleFINAuthError):
        _split_access_url("https://bridge.example/simplefin")


async def test_fetch_accounts_happy_path():
    client = SimpleFINClient(access_url="https://user:pw@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        route = mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        response = await client.fetch_accounts(lookback_days=14)
    await client.aclose()

    assert route.called
    assert len(response.accounts) == 2
    total_txns = sum(len(a.transactions) for a in response.accounts)
    assert total_txns == 10


async def test_fetch_accounts_auth_error_raises():
    client = SimpleFINClient(access_url="https://user:pw@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(401, text="bad creds")
        with pytest.raises(SimpleFINAuthError):
            await client.fetch_accounts(lookback_days=14)
    await client.aclose()


async def test_fetch_accounts_5xx_retries_then_fails():
    client = SimpleFINClient(access_url="https://user:pw@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        route = mock.get("/accounts").respond(502, text="gateway")
        with pytest.raises((SimpleFINError, httpx.HTTPError)):
            await client.fetch_accounts(lookback_days=14)
    await client.aclose()
    assert route.call_count >= 2  # tenacity retried


async def test_posted_epoch_converts_to_date():
    client = SimpleFINClient(access_url="https://user:pw@bridge.example/simplefin")
    with respx.mock(base_url="https://bridge.example/simplefin") as mock:
        mock.get("/accounts").respond(200, json=_load("two_accounts_ten_txns.json"))
        response = await client.fetch_accounts(lookback_days=14)
    await client.aclose()

    txn = response.accounts[0].transactions[0]
    # sf-2001 posted=1744243200 → 2025-04-10 UTC (chosen to match fixture ledger range)
    assert txn.posted_date.year == 2025
