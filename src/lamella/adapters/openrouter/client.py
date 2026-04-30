# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

import httpx
from pydantic import BaseModel, ValidationError
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Lazy import: importing lamella.features.ai_cascade.decisions at module load
# triggers the ai_cascade/__init__.py which in turn imports from this module
# (openrouter.client) — a circular import that only fails when openrouter.client
# is the first module loaded (i.e. running tests in isolation where AIError is
# the entry point). Resolved by deferring `CACHED_MODEL_SENTINEL` and
# `DECISION_TYPES` lookups to call sites inside `chat()` (Python caches the
# submodule after first import) and keeping `DecisionsLog` under TYPE_CHECKING
# (annotations are strings thanks to `from __future__ import annotations`).
if TYPE_CHECKING:
    from lamella.features.ai_cascade.decisions import DecisionsLog

log = logging.getLogger(__name__)

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
SCHEMA_NAME = "lamella_response"

T = TypeVar("T", bound=BaseModel)


class AIError(RuntimeError):
    """Raised when a decision cannot be produced (bad response, exhausted
    retries, schema failure after repair). Callers catch this and degrade
    to Phase-2 behavior."""


class AIBudgetExhausted(AIError):
    """Soft cap (AI_MAX_MONTHLY_SPEND_USD) exceeded — caller should fall
    back to rule-only behavior and surface a visible banner."""


@dataclass(frozen=True)
class AIResult(Generic[T]):
    data: T
    decision_id: int
    prompt_tokens: int
    completion_tokens: int
    model: str
    cached: bool


@dataclass(frozen=True)
class CachedResult(Generic[T]):
    data: T
    decision_id: int


def _hash_prompt(
    model: str,
    system: str,
    user: str,
    schema_name: str,
    images: list[tuple[bytes, str]] | None = None,
    *,
    schema_fingerprint: str = "",
) -> str:
    """Cache key for an AI decision.

    ``schema_fingerprint`` carries a stable identifier for the actual
    schema CONTENT so a code-side change to the response schema (e.g.,
    removing a field) invalidates cached entries. Without it, the
    schema_name alone keeps the same hash even when the field set
    changes — old cached responses then replay with stale fields and
    surface bugs we already fixed (the recent ``payment_last_four``
    drift was exactly this: schema dropped the field, but cache key
    didn't change, so the old response with the field kept replaying).
    """
    h = hashlib.sha256()
    h.update(model.encode("utf-8"))
    h.update(b"\x00")
    h.update(system.encode("utf-8"))
    h.update(b"\x00")
    h.update(user.encode("utf-8"))
    h.update(b"\x00")
    h.update(schema_name.encode("utf-8"))
    if schema_fingerprint:
        h.update(b"\x00")
        h.update(schema_fingerprint.encode("utf-8"))
    if images:
        for blob, mime in images:
            h.update(b"\x00")
            h.update(mime.encode("utf-8"))
            h.update(b"\x00")
            # Hash the image bytes so a different image means a
            # different cache key.
            h.update(hashlib.sha256(blob).digest())
    return h.hexdigest()


_UNSUPPORTED_NUMBER_KEYS = frozenset({
    "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
})


def _unfence_json(content: str) -> str:
    """Strip Markdown code-fence wrappers and any trailing prose so
    a plain ``json.loads`` succeeds on schemaless-retry replies.

    The response_format=json_schema path returns clean JSON, but the
    schemaless fallback (used when an Anthropic provider rejects the
    schema upstream with HTTP 400) returns whatever shape the model
    produced. Common shapes:
      ```json\n{...}\n```
      ```\n{...}\n```
      Sure! Here's the JSON:\n{...}
    Strategy: trim, peel a single ``...`` fence with optional
    language tag, and if there's still trailing/leading text outside
    the outermost JSON object, slice from the first ``{`` to the
    matching final ``}``. Returns the original string when nothing
    fence-y is detected so well-formed input is untouched.
    """
    s = content.strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl > 0:
            s = s[first_nl + 1 :]
        if s.endswith("```"):
            s = s[: -3]
        s = s.strip()
    # If the model wrapped the JSON in prose, slice to the brace span.
    if not (s.startswith("{") or s.startswith("[")):
        first = s.find("{")
        last = s.rfind("}")
        if first >= 0 and last > first:
            s = s[first : last + 1]
    return s


def _strip_anthropic_unsupported(schema: Any) -> Any:
    # Anthropic (via Bedrock) rejects numeric min/max and multipleOf in
    # grammar-constrained structured outputs with:
    #   "output_config.format.schema: For 'number' type, properties
    #    maximum, minimum are not supported".
    # Pydantic Field(ge=, le=) generates exactly those keys. We still
    # validate the response through pydantic after the fact, so dropping
    # the constraints from the wire schema only loses a soft hint.
    if isinstance(schema, dict):
        t = schema.get("type")
        cleaned = {
            k: _strip_anthropic_unsupported(v)
            for k, v in schema.items()
            if not (t in ("number", "integer") and k in _UNSUPPORTED_NUMBER_KEYS)
        }
        return cleaned
    if isinstance(schema, list):
        return [_strip_anthropic_unsupported(v) for v in schema]
    return schema


def _schema_for(model: type[BaseModel], *, strict: bool = True) -> dict[str, Any]:
    raw = _strip_anthropic_unsupported(model.model_json_schema())
    # OpenRouter's json_schema format requires a top-level object schema.
    return {
        "name": SCHEMA_NAME,
        "strict": strict,
        "schema": raw,
    }


# Decision types whose schema trips Anthropic's strict-mode regex
# compiler ("output_config.format.schema: Invalid regex in pattern
# field: Quantifier '?' without preceding element"). Pydantic emits
# these schemas without any explicit `pattern` keys, so the bad
# regex is synthesized internally by Anthropic when it translates
# the schema into a grammar-constrained sampler — most likely from
# the three-way `anyOf: [number, string, null]` union pydantic
# produces for `Decimal | None`, or from `additionalProperties` on
# an open dict. We still validate the response against the pydantic
# model after it comes back, so turning off API-level strict costs
# nothing; the schema just becomes an advisory hint to the model.
_STRICT_SCHEMA_OPT_OUTS: frozenset[str] = frozenset({
    "receipt_verify",
})


# Decision types where Anthropic-via-Bedrock has consistently rejected
# the ``response_format: json_schema`` payload upstream (HTTP 400 with
# "Provider returned error"). The schemaless-retry fallback in chat()
# recovers, but only after burning a full request round-trip on the
# doomed first call. For these decision types we skip the schema
# entirely and inline a JSON shape hint in the system prompt from the
# start, halving end-to-end latency. The pydantic validator on the
# response side catches any non-conformant output.
_SKIP_RESPONSE_FORMAT: frozenset[str] = frozenset({
    "receipt_verify",
})


class OpenRouterClient:
    """Thin async wrapper over OpenRouter's OpenAI-compatible chat API.

    Every call logs an `ai_decisions` row — happy path, cache hit, and
    permanent failure alike — so the audit trail is the ground truth.
    """

    def __init__(
        self,
        *,
        api_key: str,
        default_model: str,
        decisions: DecisionsLog,
        cache_ttl_hours: int = 24,
        base_url: str = OPENROUTER_BASE_URL,
        timeout: float = 30.0,
        app_url: str | None = None,
        app_title: str | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        self._api_key = api_key
        self._default_model = default_model
        self.decisions = decisions
        self.cache_ttl_hours = cache_ttl_hours
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if app_url:
            headers["HTTP-Referer"] = app_url
        if app_title:
            headers["X-Title"] = app_title
        self._external_client = client is not None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout,
        )

    async def aclose(self) -> None:
        if not self._external_client:
            await self._client.aclose()

    async def chat(
        self,
        *,
        decision_type: Literal[
            "classify_txn",
            "match_receipt",
            "parse_note",
            "rule_promotion",
            "column_map",
            "receipt_verify",
            "receipt_enrich",
            "draft_description",
            "summarize_day",
            "audit_day",
        ],
        input_ref: str,
        system: str,
        user: str,
        schema: type[T],
        model: str | None = None,
        temperature: float = 0.1,
        images: list[tuple[bytes, str]] | None = None,
    ) -> AIResult[T]:
        # Lazy import to break the openrouter.client <-> ai_cascade circular
        # import (see top-of-module note). Python caches the submodule, so
        # this is a single dict lookup after the first call.
        from lamella.features.ai_cascade.decisions import (
            CACHED_MODEL_SENTINEL,
            DECISION_TYPES,
        )

        if decision_type not in DECISION_TYPES:
            raise ValueError(f"unknown decision_type: {decision_type!r}")
        use_model = model or self._default_model

        # Stable fingerprint of the schema's actual field set so any
        # code-side change to the response model invalidates cache
        # entries. Sorted to be deterministic across pydantic
        # serializations. Truncated SHA-256 keeps the hash bounded.
        schema_fp = hashlib.sha256(
            json.dumps(schema.model_json_schema(), sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        prompt_hash = _hash_prompt(
            use_model, system, user, schema.__name__, images=images,
            schema_fingerprint=schema_fp,
        )

        cached = self.decisions.find_cache_hit(
            prompt_hash=prompt_hash,
            ttl_hours=self.cache_ttl_hours,
            decision_type=decision_type,
        )
        if cached is not None:
            try:
                data = schema.model_validate(cached.result)
            except ValidationError:
                data = None
            if data is not None:
                log.info("ai cache hit for %s (hash=%s...)", decision_type, prompt_hash[:8])
                decision_id = self.decisions.log(
                    decision_type=decision_type,
                    input_ref=input_ref,
                    model=CACHED_MODEL_SENTINEL,
                    prompt_tokens=0,
                    completion_tokens=0,
                    prompt_hash=prompt_hash,
                    prompt_system=system,
                    prompt_user=user,
                    result=cached.result,
                )
                return AIResult(
                    data=data,
                    decision_id=decision_id,
                    prompt_tokens=0,
                    completion_tokens=0,
                    model=CACHED_MODEL_SENTINEL,
                    cached=True,
                )

        if images:
            # OpenRouter / OpenAI-compatible vision shape: the user
            # message's content becomes a list of content blocks,
            # each either an image_url (data URL with base64) or a
            # text block. System message stays a plain string.
            import base64
            user_content: list[dict[str, Any]] = []
            for blob, mime in images:
                b64 = base64.b64encode(blob).decode("ascii")
                user_content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{mime};base64,{b64}"},
                })
            user_content.append({"type": "text", "text": user})
            messages: list[dict[str, Any]] = [
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ]
        else:
            messages = [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        use_strict = decision_type not in _STRICT_SCHEMA_OPT_OUTS
        # Skip ``response_format`` entirely for decision types that
        # consistently fail the schema-as-grammar compile upstream
        # (Anthropic via Bedrock — see _SKIP_RESPONSE_FORMAT). Inline
        # a JSON shape hint as a system message instead. Without this
        # every receipt_verify call burns a full failed round-trip on
        # the doomed schema-format request before falling back, which
        # was visibly doubling end-to-end latency to 3-4 minutes.
        if decision_type in _SKIP_RESPONSE_FORMAT:
            schema_hint = (
                "\n\n---\n"
                "Reply with ONLY a single JSON object matching this shape, "
                "no markdown fences, no commentary:\n"
                + json.dumps(_schema_for(schema, strict=False).get("schema", {}), indent=2)[:4000]
            )
            # Append the schema hint to the EXISTING system message
            # rather than prepending a second one. Keeps messages[1]
            # as the user prompt, which is the shape callers (and
            # tests) expect across the codebase.
            messages = [{**messages[0], "content": messages[0]["content"] + schema_hint}, *messages[1:]]
            body = {
                "model": use_model,
                "temperature": temperature,
                "messages": messages,
            }
        else:
            body = {
                "model": use_model,
                "temperature": temperature,
                "messages": messages,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": _schema_for(schema, strict=use_strict),
                },
            }

        try:
            payload = await self._post_chat(body)
        except httpx.HTTPError as exc:
            self._log_error(
                decision_type=decision_type,
                input_ref=input_ref,
                model=use_model,
                prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                error=f"network: {exc}",
            )
            raise AIError(f"openrouter request failed: {exc}") from exc

        status = payload.get("__status", 200)
        if status >= 400:
            # Try one repair on 4xx if it's a schema/validation-shaped error.
            first_detail = _format_error_detail(payload.get("__body", {}))
            repair_note = payload.get("__body", {}).get("error", {}).get("message", "")
            # Detect "Provider returned error" pattern. OpenRouter returns
            # this when the underlying model provider (e.g. Anthropic via
            # Bedrock) rejects the request shape itself, not the assistant
            # response. The most common cause for receipt_verify is the
            # ``response_format: json_schema`` payload tripping Anthropic's
            # schema-to-grammar compiler on nested object schemas (the
            # ``ReceiptVerification`` model has ``FieldConfidences`` and
            # ``list[VerifyLineItem]``). Re-issuing the SAME body is
            # guaranteed to fail the same way; a schemaless retry, where
            # the model emits plain JSON in its text reply that we parse
            # ourselves, is the only escape hatch that actually works.
            provider_400 = (
                status < 500
                and isinstance(repair_note, str)
                and "Provider returned error" in repair_note
            )
            if provider_400 and "response_format" in body:
                schemaless_body = {
                    k: v for k, v in body.items() if k != "response_format"
                }
                # Append a system message instructing the model to emit
                # ONLY the JSON object matching the original schema, no
                # prose, no fences. The pydantic model's own field shape
                # is included as a hint via the schema name + JSON shape
                # so the model has the contract.
                schema_hint = (
                    "The response_format constraint was rejected upstream. "
                    "Reply with ONLY a single JSON object matching this shape, "
                    "no markdown fences, no commentary:\n"
                    + json.dumps(_schema_for(schema, strict=False).get("schema", {}), indent=2)[:4000]
                )
                schemaless_body["messages"] = list(schemaless_body["messages"]) + [
                    {"role": "system", "content": schema_hint}
                ]
                try:
                    payload = await self._post_chat(schemaless_body)
                except httpx.HTTPError as exc:
                    self._log_error(
                        decision_type=decision_type,
                        input_ref=input_ref,
                        model=use_model,
                        prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                        error=f"schemaless retry network: {exc} | first: {first_detail}",
                    )
                    raise AIError(f"openrouter schemaless retry failed: {exc}") from exc
                if payload.get("__status", 200) >= 400:
                    schemaless_detail = _format_error_detail(payload.get("__body", {}))
                    self._log_error(
                        decision_type=decision_type,
                        input_ref=input_ref,
                        model=use_model,
                        prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                        error=(
                            f"HTTP {payload.get('__status')} after schemaless retry | "
                            f"first: {first_detail} | retry: {schemaless_detail}"
                        ),
                    )
                    raise AIError(
                        f"openrouter returned {payload.get('__status')} after schemaless retry"
                    )
                # Schemaless retry succeeded; fall through to response
                # parsing. The schema validator below will catch any
                # non-JSON or schema-mismatched output.
            elif status < 500:
                try:
                    payload = await self._post_chat(
                        self._with_repair_system(body, repair_note)
                    )
                except httpx.HTTPError as exc:
                    self._log_error(
                        decision_type=decision_type,
                        input_ref=input_ref,
                        model=use_model,
                        prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                        error=f"repair network: {exc} | first: {first_detail}",
                    )
                    raise AIError(f"openrouter repair failed: {exc}") from exc
                if payload.get("__status", 200) >= 400:
                    second_detail = _format_error_detail(payload.get("__body", {}))
                    self._log_error(
                        decision_type=decision_type,
                        input_ref=input_ref,
                        model=use_model,
                        prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                        error=(
                            f"HTTP {payload.get('__status')} after repair | "
                            f"first: {first_detail} | retry: {second_detail}"
                        ),
                    )
                    raise AIError(
                        f"openrouter returned {payload.get('__status')} after repair"
                    )
            else:
                self._log_error(
                    decision_type=decision_type,
                    input_ref=input_ref,
                    model=use_model,
                    prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                    error=f"HTTP {status}: {first_detail}",
                )
                raise AIError(f"openrouter returned {status}")

        response = payload.get("__body", {})
        # A 200 response can still carry an upstream error envelope
        # (provider timeout, rate limit, invalid region). OpenRouter
        # frequently wraps Anthropic's "Provider returned error |
        # code=400" rejection inside a 200-with-error-envelope shape
        # rather than surfacing as HTTP 400, which is how receipt
        # verify Tier 1 / Tier 2 looked broken for days: the schema-
        # less retry above only fired on status >= 400, so this path
        # raised straight to the caller without retrying. Detect the
        # same provider-level rejection here and re-issue without
        # ``response_format`` so the model can emit the JSON in plain
        # text and we parse it locally.
        if isinstance(response, dict) and isinstance(response.get("error"), dict):
            detail = _format_error_detail(response)
            err_msg = response["error"].get("message") or ""
            provider_400 = (
                isinstance(err_msg, str)
                and "Provider returned error" in err_msg
                and "response_format" in body
            )
            if provider_400:
                schemaless_body = {
                    k: v for k, v in body.items() if k != "response_format"
                }
                schema_hint = (
                    "The response_format constraint was rejected upstream. "
                    "Reply with ONLY a single JSON object matching this shape, "
                    "no markdown fences, no commentary:\n"
                    + json.dumps(_schema_for(schema, strict=False).get("schema", {}), indent=2)[:4000]
                )
                schemaless_body["messages"] = list(schemaless_body["messages"]) + [
                    {"role": "system", "content": schema_hint}
                ]
                try:
                    payload = await self._post_chat(schemaless_body)
                except httpx.HTTPError as exc:
                    self._log_error(
                        decision_type=decision_type,
                        input_ref=input_ref,
                        model=use_model,
                        prompt_hash=prompt_hash,
                        system_prompt=system,
                        user_prompt=user,
                        error=f"schemaless retry network (200-envelope path): {exc} | first: {detail}",
                    )
                    raise AIError(f"openrouter schemaless retry failed: {exc}") from exc
                response = payload.get("__body", {})
                if (
                    payload.get("__status", 200) >= 400
                    or (isinstance(response, dict) and isinstance(response.get("error"), dict))
                ):
                    second_detail = _format_error_detail(response)
                    self._log_error(
                        decision_type=decision_type,
                        input_ref=input_ref,
                        model=use_model,
                        prompt_hash=prompt_hash,
                        system_prompt=system,
                        user_prompt=user,
                        error=(
                            f"upstream after schemaless retry | first: {detail} | "
                            f"retry: {second_detail}"
                        ),
                    )
                    raise AIError(f"upstream provider error after schemaless retry: {second_detail}")
                # Schemaless retry succeeded; fall through to parse.
            else:
                self._log_error(
                    decision_type=decision_type,
                    input_ref=input_ref,
                    model=use_model,
                    prompt_hash=prompt_hash,
                    system_prompt=system,
                    user_prompt=user,
                    error=f"upstream: {detail}",
                )
                raise AIError(f"upstream provider error: {detail}")
        try:
            content, prompt_tokens, completion_tokens = _extract_content(response)
        except ValueError as exc:
            self._log_error(
                decision_type=decision_type,
                input_ref=input_ref,
                model=use_model,
                prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                error=str(exc),
            )
            raise AIError(str(exc))

        try:
            parsed = (
                json.loads(_unfence_json(content)) if isinstance(content, str)
                else content
            )
            data = schema.model_validate(parsed)
        except (ValueError, ValidationError) as exc:
            # Second-chance repair against the schema.
            try:
                repaired_body = self._with_repair_system(
                    body, f"Previous response did not match schema: {exc}"
                )
                payload = await self._post_chat(repaired_body)
            except httpx.HTTPError as net:
                self._log_error(
                    decision_type=decision_type,
                    input_ref=input_ref,
                    model=use_model,
                    prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                    error=f"schema repair network: {net}",
                )
                raise AIError(f"schema repair failed: {net}") from net
            try:
                content, prompt_tokens, completion_tokens = _extract_content(
                    payload.get("__body", {})
                )
                parsed = (
                json.loads(_unfence_json(content)) if isinstance(content, str)
                else content
            )
                data = schema.model_validate(parsed)
            except (ValueError, ValidationError) as exc2:
                self._log_error(
                    decision_type=decision_type,
                    input_ref=input_ref,
                    model=use_model,
                    prompt_hash=prompt_hash,
                system_prompt=system,
                user_prompt=user,
                    error=f"schema: {exc2}",
                )
                raise AIError(f"schema validation failed: {exc2}") from exc2

        decision_id = self.decisions.log(
            decision_type=decision_type,
            input_ref=input_ref,
            model=use_model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            prompt_hash=prompt_hash,
            prompt_system=system,
            prompt_user=user,
            result=data.model_dump(mode="json"),
        )
        return AIResult(
            data=data,
            decision_id=decision_id,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=use_model,
            cached=False,
        )

    async def _post_chat(self, body: dict[str, Any]) -> dict[str, Any]:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
            retry=retry_if_exception_type(_RetryableHTTP),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.post("/chat/completions", json=body)
                if resp.status_code >= 500:
                    raise _RetryableHTTP(
                        f"HTTP {resp.status_code}: {resp.text[:200]}"
                    )
                try:
                    parsed = resp.json()
                except ValueError:
                    parsed = {"error": {"message": resp.text}}
                # OpenRouter frequently returns HTTP 200 with an
                # error body when the upstream provider (Anthropic,
                # OpenAI, etc.) failed. Shape:
                #   {"error": {"message": "Provider returned error",
                #              "code": 502, ...}, "choices": []}
                # Treat a 5xx upstream code embedded in a 200 like
                # a real 5xx so we actually retry through tenacity
                # rather than crashing later on "no choices".
                if resp.status_code < 400 and isinstance(parsed, dict):
                    err = parsed.get("error")
                    if isinstance(err, dict):
                        upstream = err.get("code")
                        if isinstance(upstream, int) and upstream >= 500:
                            raise _RetryableHTTP(
                                f"upstream {upstream}: "
                                f"{_format_error_detail(parsed)[:200]}"
                            )
                return {"__status": resp.status_code, "__body": parsed}
        # Unreachable; AsyncRetrying raises if all attempts fail.
        raise AIError("retry loop exited without a response")

    @staticmethod
    def _with_repair_system(body: dict[str, Any], note: str) -> dict[str, Any]:
        new_body = dict(body)
        messages = list(body.get("messages", []))
        repair_msg = {
            "role": "system",
            "content": (
                "Your previous reply was rejected. "
                "Return ONLY valid JSON matching the json_schema. "
                f"Error: {note[:500]}"
            ),
        }
        new_body["messages"] = messages + [repair_msg]
        return new_body

    def _log_error(
        self,
        *,
        decision_type: str,
        input_ref: str,
        model: str,
        prompt_hash: str,
        error: str,
        system_prompt: str | None = None,
        user_prompt: str | None = None,
    ) -> None:
        # Capture the exact system + user strings on error rows too.
        # Without these the user sees "(pre-capture era — not stored)"
        # in the AI-decisions detail page even for fresh failures, which
        # makes provider-rejection bugs un-debuggable. The decisions
        # store accepts the prompts as nullable, so historical rows
        # stay valid.
        try:
            self.decisions.log(
                decision_type=decision_type,
                input_ref=input_ref,
                model=model,
                prompt_tokens=0,
                completion_tokens=0,
                prompt_hash=prompt_hash,
                prompt_system=system_prompt,
                prompt_user=user_prompt,
                result={"error": error},
            )
        except Exception:  # never let logging failure propagate
            log.exception("failed to log ai decision error")


class _RetryableHTTP(httpx.HTTPError):
    pass


def _format_error_detail(body: dict[str, Any]) -> str:
    """Flatten an OpenRouter error body into a single readable string.
    The top-level `error.message` is often generic ("Provider returned
    error"); the actionable text lives in `error.metadata.raw` (the
    upstream provider's own JSON) or `error.metadata`. We concatenate
    whatever is present so the audit log shows the real reason.
    """
    if not isinstance(body, dict):
        return f"non-dict body: {body!r}"[:800]
    err = body.get("error") or {}
    parts: list[str] = []
    msg = err.get("message")
    if msg:
        parts.append(str(msg))
    code = err.get("code")
    if code is not None:
        parts.append(f"code={code}")
    meta = err.get("metadata")
    if meta:
        raw = meta.get("raw") if isinstance(meta, dict) else None
        if raw:
            parts.append(f"raw={str(raw)[:600]}")
        else:
            parts.append(f"metadata={json.dumps(meta)[:600]}")
    if not parts:
        # Fall back to dumping the first ~400 chars of the body so
        # something shows up even on unexpected shapes.
        return json.dumps(body)[:800]
    return " | ".join(parts)


def _extract_content(body: dict[str, Any]) -> tuple[str, int, int]:
    choices = body.get("choices") or []
    if not choices:
        raise ValueError(f"no choices in response: {body}")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if content is None:
        # Some tool/structured outputs put the payload in `message.parsed`.
        content = message.get("parsed")
    if content is None:
        raise ValueError(f"no content in message: {message}")
    usage = body.get("usage") or {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    return content, prompt_tokens, completion_tokens
