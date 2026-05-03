# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""AI provider port — abstracts OpenRouter-style chat completion.

The concrete adapter today is :mod:`lamella.adapters.openrouter.client`.
Stage-2 may add Anthropic-direct, OpenAI-direct, or Bedrock adapters.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AIProviderPort(Protocol):
    """Chat-completion contract.

    Adapters implement ``chat`` for unstructured completions and
    ``complete_with_schema`` for JSON-schema-validated responses.
    Both must enforce the Lamella budget guardrails (token caps + cost
    accounting) before returning.
    """

    async def chat(self, *args: Any, **kwargs: Any) -> Any: ...

    async def complete_with_schema(self, *args: Any, **kwargs: Any) -> Any: ...
