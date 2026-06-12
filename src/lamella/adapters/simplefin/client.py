# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import base64
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import httpx
from pydantic import ValidationError
from tenacity import (
    AsyncRetrying,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from lamella.adapters.simplefin.schemas import SimpleFINBridgeResponse

log = logging.getLogger(__name__)


class SimpleFINError(RuntimeError):
    """Raised when a fetch cannot produce a parsed response."""


class SimpleFINAuthError(SimpleFINError):
    """The access URL is malformed or the bridge rejected our credentials."""


class _RetryableHTTP(httpx.HTTPError):
    pass


def _looks_like_access_url(value: str) -> bool:
    """Access URLs are HTTP(S) with user:pass embedded. Setup tokens don't
    have a scheme because they're base64 of a claim URL. Used to route
    paste-in input between the direct-URL path and the token-claim path."""
    v = (value or "").strip()
    if not v:
        return False
    lowered = v.lower()
    return lowered.startswith("http://") or lowered.startswith("https://")


# ADR-0027: 3 attempts, exponential backoff (2-10s), retry only on
# transient timeout/network errors. The synchronous claim flow ran a bare
# ``httpx.post`` with no retry — wrap it with the same shape applied to
# the other 3 adapters (paperless, openrouter, plaid). 30s timeout matches
# the kwarg default below.
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    reraise=True,
)
def _claim_post(decoded: str, *, timeout: float) -> httpx.Response:
    """Wrapped POST to the SimpleFIN claim URL. Tenacity retries only on
    transient (timeout/network) errors so 4xx responses (token reuse,
    malformed claim URL) bubble up immediately in claim_setup_token."""
    return httpx.post(decoded, timeout=timeout)


def claim_setup_token(token: str, *, timeout: float = 30.0) -> str:
    """SimpleFIN setup tokens are base64 of a one-shot claim URL. POSTing
    to that URL returns the real access URL (with user:pass@ embedded).
    Tokens are single-use — the bridge invalidates them after a successful
    claim. Returns the resolved access URL on success."""
    raw = (token or "").strip()
    if not raw:
        raise SimpleFINAuthError("setup token is empty")
    # Audit hardening: ``raise ... from None`` instead of ``from exc`` —
    # the chained exception's traceback would carry the failed token in
    # frame locals if any caller ever swapped log.warning(type-only)
    # for log.exception(stack-trace). Discipline-only mitigation isn't
    # enough for credential paths.
    try:
        decoded = base64.b64decode(raw, validate=True).decode("utf-8").strip()
    except Exception:
        raise SimpleFINAuthError(
            "setup token is not valid base64"
        ) from None
    if not decoded.lower().startswith(("http://", "https://")):
        raise SimpleFINAuthError(
            "setup token did not decode to a claim URL — did you paste the access URL instead?"
        )
    try:
        resp = _claim_post(decoded, timeout=timeout)
    except httpx.HTTPError as exc:
        # ``str(exc)`` for httpx errors is generic ("Connection refused",
        # "timeout", etc.) and doesn't carry the request URL — safe to
        # surface. We still drop the chain to keep tracebacks credential-free.
        raise SimpleFINError(f"claim request failed: {exc}") from None
    if resp.status_code == 403:
        raise SimpleFINAuthError(
            "claim URL returned 403 — the setup token was already used or has expired"
        )
    if resp.status_code >= 400:
        # Audit hardening: bridge error responses can echo the request
        # URL (which contains the decoded claim URL — a credential).
        # The 200-char truncation in the route layer doesn't help if
        # the URL is at the start of resp.text. Strip URLs from the
        # body before surfacing.
        raise SimpleFINError(
            f"claim URL returned HTTP {resp.status_code} "
            "(bridge response body suppressed — see server logs for "
            "the response status code only)"
        )
    access_url = resp.text.strip()
    if not access_url.lower().startswith(("http://", "https://")):
        raise SimpleFINError("claim response was not an access URL")
    return access_url


def _split_access_url(access_url: str) -> tuple[str, str]:
    """A SimpleFIN access URL embeds HTTP Basic creds:
    ``https://<user>:<pass>@bridge.example/simplefin``. Split that into
    ``(base_url_without_creds, basic_auth_header)``."""
    if not access_url:
        raise SimpleFINAuthError("access URL is empty")
    parsed = urlparse(access_url)
    if not parsed.scheme or not parsed.netloc:
        raise SimpleFINAuthError("access URL is malformed")
    user = parsed.username or ""
    pwd = parsed.password or ""
    if not user or not pwd:
        raise SimpleFINAuthError("access URL is missing username:password")
    clean_netloc = parsed.hostname or ""
    if parsed.port:
        clean_netloc = f"{clean_netloc}:{parsed.port}"
    base = f"{parsed.scheme}://{clean_netloc}{parsed.path.rstrip('/')}"
    token = base64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
    return base, f"Basic {token}"


class SimpleFINClient:
    """Async client over the SimpleFIN Bridge HTTP+JSON API.

    The only endpoint we care about is ``GET <base>/accounts``. The bridge
    accepts ``start-date`` (Unix epoch) and ``pending=1`` query params.
    """

    def __init__(
        self,
        *,
        access_url: str,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ):
        base, auth = _split_access_url(access_url)
        self._base = base
        self._headers = {"Authorization": auth, "Accept": "application/json"}
        self._timeout = timeout
        self._external_client = client is not None
        self._client = client or httpx.AsyncClient(
            base_url=self._base,
            headers=self._headers,
            timeout=timeout,
        )

    async def aclose(self) -> None:
        if not self._external_client:
            await self._client.aclose()

    async def __aenter__(self) -> "SimpleFINClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def fetch_accounts(
        self,
        *,
        lookback_days: int = 14,
        include_pending: bool = False,
    ) -> SimpleFINBridgeResponse:
        since = datetime.now(timezone.utc).date() - timedelta(days=max(1, lookback_days))
        start_epoch = int(
            datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc).timestamp()
        )
        params: dict[str, Any] = {"start-date": start_epoch}
        if include_pending:
            params["pending"] = 1

        payload: dict[str, Any] = {}
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
            retry=retry_if_exception_type(_RetryableHTTP),
            reraise=True,
        ):
            with attempt:
                try:
                    resp = await self._client.get("/accounts", params=params)
                except httpx.HTTPError as exc:
                    raise _RetryableHTTP(f"network: {exc}") from exc
                if resp.status_code in (401, 403):
                    raise SimpleFINAuthError(
                        f"bridge rejected credentials: HTTP {resp.status_code}"
                    )
                if resp.status_code >= 500:
                    raise _RetryableHTTP(f"HTTP {resp.status_code}: {resp.text[:200]}")
                if resp.status_code >= 400:
                    raise SimpleFINError(
                        f"bridge returned HTTP {resp.status_code}: {resp.text[:200]}"
                    )
                try:
                    payload = resp.json()
                except ValueError as exc:
                    # `from None` instead of `from exc` for the same
                    # reason as claim_setup_token: chained tracebacks
                    # carry frame locals (resp / self._client / etc.)
                    # which reference the access URL + basic-auth
                    # headers. Discipline-only mitigation isn't enough
                    # for credential paths.
                    raise SimpleFINError(
                        f"non-JSON bridge response: {exc}"
                    ) from None

        try:
            return SimpleFINBridgeResponse.model_validate(payload)
        except ValidationError as exc:
            raise SimpleFINError(
                f"bridge response failed validation: {exc}"
            ) from None


def epoch_to_date(ts: int | float) -> date:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
