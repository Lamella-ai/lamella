# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx
from httpx import Response

from lamella.ports.notification import NotificationEvent, Priority
from lamella.adapters.pushover.client import PushoverNotifier


FIXTURES = Path(__file__).parent / "fixtures" / "notify"


async def test_disabled_without_credentials():
    n = PushoverNotifier(user_key=None, api_token=None)
    assert n.enabled() is False
    res = await n.send(_event())
    assert res.ok is False


@respx.mock
async def test_success_path_sends_priority_and_url():
    n = PushoverNotifier(user_key="u", api_token="t")
    payload = json.loads((FIXTURES / "pushover_success.json").read_text(encoding="utf-8"))
    route = respx.post("https://api.pushover.net/1/messages.json").mock(
        return_value=Response(200, json=payload)
    )
    res = await n.send(_event(priority=Priority.URGENT, url="/review"))
    await n.aclose()
    assert res.ok is True
    request = route.calls.last.request
    body = request.content.decode("utf-8")
    assert "priority=1" in body
    assert "url=%2Freview" in body
    assert "user=u" in body
    assert "token=t" in body


@respx.mock
async def test_rate_limit_response_returns_failure():
    n = PushoverNotifier(user_key="u", api_token="t")
    payload = json.loads((FIXTURES / "pushover_ratelimited.json").read_text(encoding="utf-8"))
    respx.post("https://api.pushover.net/1/messages.json").mock(
        return_value=Response(200, json=payload)
    )
    res = await n.send(_event())
    await n.aclose()
    assert res.ok is False
    assert "monthly message limit" in (res.error or "")


@respx.mock
async def test_http_429_is_failure():
    n = PushoverNotifier(user_key="u", api_token="t")
    respx.post("https://api.pushover.net/1/messages.json").mock(
        return_value=Response(429, text="Too Many Requests")
    )
    res = await n.send(_event())
    await n.aclose()
    assert res.ok is False
    assert "429" in (res.error or "")


def _event(priority: Priority = Priority.INFO, url: str | None = None) -> NotificationEvent:
    return NotificationEvent(
        dedup_key="k",
        priority=priority,
        title="title",
        body="body",
        url=url,
    )
