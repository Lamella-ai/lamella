# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import pytest
import respx
from httpx import Response

from lamella.ports.notification import NotificationEvent, Priority
from lamella.adapters.ntfy.client import NtfyNotifier


async def test_disabled_when_no_topic():
    n = NtfyNotifier(base_url="https://ntfy.sh", topic=None)
    assert n.enabled() is False
    res = await n.send(_event())
    assert res.ok is False
    assert "not configured" in (res.error or "")


@respx.mock
async def test_post_to_topic_with_priority_and_click():
    n = NtfyNotifier(base_url="https://ntfy.example", topic="my-topic", token="abc")
    route = respx.post("https://ntfy.example/my-topic").mock(
        return_value=Response(200, json={"id": "x"})
    )
    res = await n.send(_event(priority=Priority.URGENT, url="/review#txn=foo"))
    await n.aclose()
    assert res.ok is True
    assert route.called
    request = route.calls.last.request
    assert request.headers["Title"] == "title"
    assert request.headers["Priority"] == "5"
    assert request.headers["Click"] == "/review#txn=foo"
    assert request.headers["Authorization"] == "Bearer abc"


@respx.mock
async def test_http_error_returns_failure():
    n = NtfyNotifier(base_url="https://ntfy.example", topic="my-topic")
    respx.post("https://ntfy.example/my-topic").mock(return_value=Response(500, text="boom"))
    res = await n.send(_event())
    await n.aclose()
    assert res.ok is False
    assert "500" in (res.error or "")


def _event(priority: Priority = Priority.INFO, url: str | None = None) -> NotificationEvent:
    return NotificationEvent(
        dedup_key="k",
        priority=priority,
        title="title",
        body="body",
        url=url,
    )
