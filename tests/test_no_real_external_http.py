# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Proves the conftest network-safety fixture actually rejects
unmocked HTTP calls to paid / external services. If this test
fails it means a future change weakened the guardrail and a
production AI bill is one bug away."""
from __future__ import annotations

import asyncio

import httpx
import pytest


def test_unmocked_openrouter_call_is_rejected():
    """A direct httpx call to openrouter.ai — no test-level mock —
    must raise. The conftest autouse respx context refuses any
    unmatched request to a paid host."""

    async def _go():
        async with httpx.AsyncClient() as client:
            await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                json={"messages": []},
            )

    with pytest.raises(Exception):
        asyncio.run(_go())


def test_unmocked_anthropic_call_is_rejected():
    """Same protection for anthropic.com — guards against tests
    that bypass OpenRouter and talk to Claude directly."""

    async def _go():
        async with httpx.AsyncClient() as client:
            await client.post(
                "https://api.anthropic.com/v1/messages",
                json={"messages": []},
            )

    with pytest.raises(Exception):
        asyncio.run(_go())


def test_test_local_calls_pass_through(app_client):
    """TestClient and other local in-process calls aren't
    intercepted — the safety net only refuses paid external
    hosts."""
    r = app_client.get("/healthz")
    assert r.status_code == 200
