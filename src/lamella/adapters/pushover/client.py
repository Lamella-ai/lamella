# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from lamella.ports.notification import (
    Channel,
    NotificationEvent,
    Notifier,
    NotifierResult,
    Priority,
)

log = logging.getLogger(__name__)


# ADR-0027: 3 attempts max, exponential backoff (2-10s), retry only on
# transient network/timeout errors (NOT on 4xx — Pushover's 429 rate
# limit is handled by the caller as a non-retryable failure). reraise=True
# so the original httpx exception bubbles up after exhaustion.
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(
        (httpx.TimeoutException, httpx.NetworkError)
    ),
    reraise=True,
)
async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    data: dict[str, object],
) -> httpx.Response:
    return await client.post(url, data=data)


# Pushover priority: -2..2. We intentionally avoid 2 ("emergency /
# acknowledge") because the system is single-user; URGENT at 1 still makes
# the phone buzz loudly but doesn't demand user action.
_PUSHOVER_PRIORITY = {
    Priority.INFO: -1,
    Priority.WARN: 0,
    Priority.URGENT: 1,
}


class PushoverNotifier(Notifier):
    channel = Channel.PUSHOVER

    def __init__(
        self,
        *,
        user_key: str | None,
        api_token: str | None,
        api_url: str = "https://api.pushover.net/1/messages.json",
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,  # ADR-0027: 30s hard timeout
    ):
        self.user_key = (user_key or "").strip() or None
        self.api_token = (api_token or "").strip() or None
        self.api_url = api_url
        self._client = client
        self._owns_client = client is None
        self.timeout = timeout

    def enabled(self) -> bool:
        return bool(self.user_key and self.api_token)

    def _client_or_new(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, event: NotificationEvent) -> NotifierResult:
        if not self.enabled():
            return NotifierResult(ok=False, error="pushover: not configured")
        data: dict[str, object] = {
            "token": self.api_token,
            "user": self.user_key,
            "title": event.title,
            "message": event.body,
            "priority": _PUSHOVER_PRIORITY.get(event.priority, 0),
        }
        if event.url:
            data["url"] = event.url
        try:
            client = self._client_or_new()
            response = await _post_with_retry(client, self.api_url, data=data)
        except httpx.HTTPError as exc:
            return NotifierResult(
                ok=False, error=f"pushover: {type(exc).__name__}: {exc}"
            )
        if response.status_code == 429:
            return NotifierResult(ok=False, error="pushover: rate limited (HTTP 429)")
        if response.status_code >= 400:
            return NotifierResult(
                ok=False,
                error=f"pushover: HTTP {response.status_code}: {response.text[:200]}",
            )
        # Pushover returns {"status": 1} on success; anything else → failure.
        try:
            payload = response.json()
        except ValueError:
            return NotifierResult(ok=False, error="pushover: non-JSON response")
        if payload.get("status") != 1:
            errors = payload.get("errors") or [payload.get("error") or "unknown"]
            return NotifierResult(
                ok=False, error=f"pushover: {'; '.join(str(e) for e in errors)}"
            )
        return NotifierResult(ok=True)
