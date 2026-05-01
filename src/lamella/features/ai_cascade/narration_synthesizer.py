# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Narration synthesizer — ADR-0059.

When a transaction has multiple source observations whose
descriptions diverge, the canonical txn-level narration shouldn't
be "whichever source landed first." It should be a coherent line
that summarizes what the event actually was, drawn from every
source's phrasing.

This module defines:

* :class:`SynthesisInput` — the structured shape passed to a
  synthesizer (signed amount, currency, source-side account,
  per-source observations).
* :class:`NarrationSynthesizer` — the port. Two implementations:

  * :class:`DeterministicNarrationSynthesizer` — used by tests and
    by deployments that haven't enabled an AI adapter. Picks the
    longest source description as the canonical, prefixed with the
    payee when one is available. No AI cost; predictable output.
  * :class:`HaikuNarrationSynthesizer` — production adapter that
    calls Haiku via the existing ``ai_cascade`` machinery. Falls
    back to the deterministic adapter on any error so the
    promotion path never blocks on AI latency / outage.

The synthesizer is invoked at promote time (when the staged row
becomes a ledger entry) and at confirm-as-dup time (when a new
source observation lands on an existing entry). User-edited
narrations are sticky — see ``LAMELLA_NARRATION_SYNTHESIZED_KEY``
below for the marker writeback.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable, Protocol


log = logging.getLogger(__name__)


# Txn-level meta key marking a narration as synthesizer-owned.
# When TRUE, a future synthesis pass may rewrite it. When FALSE
# or absent, the user has edited it; the synthesizer leaves it
# alone forever.
LAMELLA_NARRATION_SYNTHESIZED_KEY = "lamella-narration-synthesized"


@dataclass(frozen=True)
class SourceObservation:
    """One source's view of the event — the per-source data the
    synthesizer reads to produce the canonical narration."""
    source: str          # 'simplefin' | 'csv' | 'paste' | 'reboot' | …
    reference_id: str | None
    description: str | None
    payee: str | None = None


@dataclass(frozen=True)
class SynthesisInput:
    """Everything a synthesizer needs to compose one narration line.

    Date is intentionally NOT part of the input: narrations are
    timeless ("Coffee Shop — Decaf", not "On 2026-04-15 …"). The
    date is already on the entry's header line.
    """
    signed_amount: Decimal
    currency: str
    source_account: str
    target_account: str | None
    observations: tuple[SourceObservation, ...]
    # The user's existing narration if any. The synthesizer may
    # use it as a hint but won't keep it verbatim unless explicitly
    # told to (see SynthesisInput.allow_overwrite).
    existing_narration: str | None = None


@dataclass
class SynthesisResult:
    """Output of a synthesis call."""
    narration: str
    # Where the narration came from — useful for the audit trail
    # ("Haiku synthesized from 3 sources", "deterministic
    # fallback after Haiku timeout"). Free-form; the writer just
    # logs it.
    rationale: str = ""


class NarrationSynthesizer(Protocol):
    """The narration-synthesizer port. Sync by design — the
    promotion path is already off the request hot path, so adding
    an async hop here adds complexity without latency wins."""

    def synthesize(
        self, input_: SynthesisInput,
    ) -> SynthesisResult:  # pragma: no cover (Protocol)
        ...


class DeterministicNarrationSynthesizer:
    """Never calls an LLM. Picks the longest source description as
    the canonical narration; falls back to a payee-only line; falls
    back to the source-account-derived account-name when no source
    text exists.

    Useful for tests, deployments that disable AI entirely, and as
    the safety fallback when ``HaikuNarrationSynthesizer`` fails.
    """

    def synthesize(
        self, input_: SynthesisInput,
    ) -> SynthesisResult:
        descriptions = [
            (o.description or "").strip()
            for o in input_.observations
            if o.description and o.description.strip()
        ]
        payees = [
            (o.payee or "").strip()
            for o in input_.observations
            if o.payee and o.payee.strip()
        ]
        if descriptions:
            chosen = max(descriptions, key=len)
            return SynthesisResult(
                narration=chosen,
                rationale=(
                    "deterministic: longest of "
                    f"{len(descriptions)} source description(s)"
                ),
            )
        if payees:
            # Pick the first non-empty payee; payees are usually
            # already canonical merchant names.
            chosen = payees[0]
            return SynthesisResult(
                narration=chosen,
                rationale="deterministic: payee fallback",
            )
        # Nothing useful from any source — emit a placeholder the
        # user can recognize and overwrite.
        return SynthesisResult(
            narration="(no narration)",
            rationale="deterministic: no source text available",
        )


class HaikuNarrationSynthesizer:
    """Production adapter: builds a small prompt for Haiku
    summarizing every source's phrasing into one canonical line.

    On any error (rate limit, network, malformed response) falls
    back to ``DeterministicNarrationSynthesizer.synthesize`` so the
    promotion / confirm path never hard-blocks on the AI."""

    SYSTEM_PROMPT = (
        "You compose one short transaction narration from multiple "
        "source observations. Output one line, plain text, no "
        "markdown, ≤ 80 characters. Prefer the merchant name. "
        "Combine signal across observations; do not invent details "
        "not present in the inputs. Output JUST the narration text."
    )

    def __init__(self, ai_service):
        self.ai_service = ai_service
        self._fallback = DeterministicNarrationSynthesizer()

    def synthesize(
        self, input_: SynthesisInput,
    ) -> SynthesisResult:
        if self.ai_service is None or not getattr(
            self.ai_service, "enabled", False,
        ):
            return self._fallback.synthesize(input_)
        # Build a compact user prompt from the observations.
        lines = [
            f"signed amount: {input_.signed_amount} {input_.currency}",
            f"source account: {input_.source_account}",
        ]
        if input_.target_account:
            lines.append(f"target account: {input_.target_account}")
        for obs in input_.observations:
            lines.append(
                f"- source={obs.source} "
                f"payee={obs.payee or '∅'} "
                f"description={obs.description or '∅'}"
            )
        if input_.existing_narration:
            lines.append(
                f"existing user-set narration: "
                f"{input_.existing_narration!r}"
            )
        user_prompt = "\n".join(lines)
        try:
            client = self.ai_service.new_client()
            if client is None:
                return self._fallback.synthesize(input_)
            # The narration synthesizer is a small structured-output
            # call. We rely on the existing chat client; the result
            # type isn't a Pydantic schema because we just want a
            # string.
            import asyncio
            text = asyncio.run(
                self._call_haiku(client, user_prompt)
            )
            text = (text or "").strip().splitlines()
            line = (text[0] if text else "").strip()
            if not line:
                return self._fallback.synthesize(input_)
            return SynthesisResult(
                narration=line[:120],
                rationale=(
                    "haiku synthesis from "
                    f"{len(input_.observations)} source(s)"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "narration synthesis: Haiku call failed (%s) — "
                "falling back to deterministic", exc,
            )
            return self._fallback.synthesize(input_)

    async def _call_haiku(self, client, user_prompt: str) -> str:
        """Single-line Haiku call for narration. Uses chat without
        a Pydantic schema since the contract is plain text."""
        # The OpenRouterClient's chat() requires schema kwarg; for
        # free-text we use raw_chat() if available, else fall back.
        raw = getattr(client, "raw_chat", None)
        if raw is None:
            return ""
        return await raw(
            decision_type="synthesize_narration",
            input_ref="narration",
            system=self.SYSTEM_PROMPT,
            user=user_prompt,
            model=self.ai_service.model_for("synthesize_narration"),
        )


def build_synthesis_input(
    *,
    signed_amount: Decimal,
    currency: str,
    source_account: str,
    target_account: str | None,
    observations: Iterable[SourceObservation],
    existing_narration: str | None = None,
) -> SynthesisInput:
    """Builder convenience — coerces ``observations`` to a tuple
    and applies sane defaults so call sites don't repeat ceremony."""
    return SynthesisInput(
        signed_amount=signed_amount,
        currency=currency,
        source_account=source_account,
        target_account=target_account,
        observations=tuple(observations),
        existing_narration=existing_narration,
    )


__all__ = [
    "LAMELLA_NARRATION_SYNTHESIZED_KEY",
    "SourceObservation",
    "SynthesisInput",
    "SynthesisResult",
    "NarrationSynthesizer",
    "DeterministicNarrationSynthesizer",
    "HaikuNarrationSynthesizer",
    "build_synthesis_input",
]
