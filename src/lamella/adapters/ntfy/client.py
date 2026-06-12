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
# transient network/timeout errors (NOT on 4xx — those are caller bugs
# that retrying won't fix). reraise=True so the original httpx exception
# bubbles up after exhaustion; the caller's existing except-block keeps
# working unchanged.
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
    content: bytes,
    headers: dict[str, str],
) -> httpx.Response:
    return await client.post(url, content=content, headers=headers)


# ntfy priority encoding: 1 (min) .. 5 (max). Default is 3.
_NTFY_PRIORITY = {
    Priority.INFO: "2",
    Priority.WARN: "4",
    Priority.URGENT: "5",
}


class NtfyNotifier(Notifier):
    channel = Channel.NTFY

    def __init__(
        self,
        *,
        base_url: str,
        topic: str | None,
        token: str | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,  # ADR-0027: 30s hard timeout
    ):
        self.base_url = (base_url or "https://ntfy.sh").rstrip("/")
        self.topic = (topic or "").strip() or None
        self.token = (token or "").strip() or None
        self._client = client
        self._owns_client = client is None
        self.timeout = timeout

    def enabled(self) -> bool:
        return self.topic is not None

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
            return NotifierResult(ok=False, error="ntfy: not configured")
        url = f"{self.base_url}/{self.topic}"
        headers = {
            "Title": event.title,
            "Priority": _NTFY_PRIORITY.get(event.priority, "3"),
        }
        if event.url:
            headers["Click"] = event.url
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            client = self._client_or_new()
            response = await _post_with_retry(
                client,
                url,
                content=event.body.encode("utf-8"),
                headers=headers,
            )
        except httpx.HTTPError as exc:
            return NotifierResult(ok=False, error=f"ntfy: {type(exc).__name__}: {exc}")
        if response.status_code >= 400:
            return NotifierResult(
                ok=False,
                error=f"ntfy: HTTP {response.status_code}: {response.text[:200]}",
            )
        return NotifierResult(ok=True)
