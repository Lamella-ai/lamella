# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from lamella.adapters.openrouter.client import OpenRouterClient
from lamella.features.ai_cascade.decisions import DecisionsLog
from lamella.features.ai_cascade.gating import ConfidenceGate
from lamella.core.config import Settings
from lamella.core.settings.store import AppSettingsStore

log = logging.getLogger(__name__)


DEFAULT_PROMPT_PRICE_PER_1K = 0.001  # conservative; user overridable
DEFAULT_COMPLETION_PRICE_PER_1K = 0.005


class AIService:
    """Facade: `new_client()`, `decisions()`, `model_for(decision_type)`,
    `spend_cap_reached()`. Routes and background jobs consume this, not
    `OpenRouterClient` directly, so the spend-cap and per-decision-type
    model overrides live in one place."""

    def __init__(
        self,
        *,
        settings: Settings,
        conn: sqlite3.Connection,
        gate: ConfidenceGate | None = None,
    ):
        self.settings = settings
        self.conn = conn
        self.decisions = DecisionsLog(conn)
        self.settings_store = AppSettingsStore(conn)
        self.gate = gate or ConfidenceGate()

    @property
    def enabled(self) -> bool:
        return self.settings.ai_enabled

    def model_for(self, decision_type: str) -> str:
        override_key = f"openrouter_model_{decision_type}"
        override = self.settings_store.get(override_key)
        if override:
            return override
        global_override = self.settings_store.get("openrouter_model")
        return global_override or self.settings.openrouter_model

    def fallback_model_for(self, decision_type: str) -> str | None:
        """Return the escalation model for a decision type, or None
        when the cascade is disabled or no fallback is configured.

        Lookup order mirrors `model_for`: per-decision-type app_settings
        override → global `openrouter_model_fallback` app_settings override
        → config default. When the resolved fallback equals the primary
        model, returns None (no point retrying with the same model).
        """
        if not self.fallback_enabled:
            return None
        override_key = f"openrouter_model_{decision_type}_fallback"
        override = self.settings_store.get(override_key)
        if override:
            resolved = override
        else:
            global_override = self.settings_store.get("openrouter_model_fallback")
            resolved = global_override or self.settings.openrouter_model_fallback
        if not resolved:
            return None
        if resolved == self.model_for(decision_type):
            return None
        return resolved

    @property
    def fallback_enabled(self) -> bool:
        raw = self.settings_store.get("ai_fallback_enabled")
        if raw is None:
            return bool(self.settings.ai_fallback_enabled)
        return str(raw).strip().lower() not in ("0", "false", "no", "off")

    def fallback_threshold(self) -> float:
        raw = self.settings_store.get("ai_fallback_confidence_threshold")
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
        return float(self.settings.ai_fallback_confidence_threshold)

    def vision_model(self) -> str:
        """Resolve the vision-capable model used for Paperless
        receipt-verify calls. Per-decision-type override (via
        `openrouter_model_receipt_verify` in app_settings) takes
        precedence; otherwise config default. Deliberately NOT
        using `model_for("receipt_verify")` — vision calls are
        expensive and we don't want the two-agent cascade logic
        retrying them."""
        override = self.settings_store.get("openrouter_model_receipt_verify")
        if override:
            return override
        return self.settings.openrouter_model_receipt_verify

    def ocr_text_receipt_verify_model(self) -> str:
        """Resolve the cheap text-only model used for the first
        tier of receipt verification. Reads OCR'd content and
        extracts structured fields — no image, so Haiku-class is
        fine. Distinct from `vision_model()` so a user setting the
        expensive vision override doesn't accidentally force the
        cheap tier onto it. Falls back to the primary
        `openrouter_model` when no override is set."""
        override = self.settings_store.get(
            "openrouter_model_receipt_verify_ocr"
        )
        if override:
            return override
        global_override = self.settings_store.get("openrouter_model")
        return global_override or self.settings.openrouter_model

    def price_prompt_per_1k(self) -> float:
        raw = self.settings_store.get("ai_price_usd_per_1k_prompt")
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
        return DEFAULT_PROMPT_PRICE_PER_1K

    def price_completion_per_1k(self) -> float:
        raw = self.settings_store.get("ai_price_usd_per_1k_completion")
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
        return DEFAULT_COMPLETION_PRICE_PER_1K

    def monthly_cap_usd(self) -> float:
        raw = self.settings_store.get("ai_max_monthly_spend_usd")
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
        return float(self.settings.ai_max_monthly_spend_usd or 0.0)

    def month_start(self) -> datetime:
        now = datetime.now(timezone.utc)
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def spend_cap_reached(self) -> bool:
        cap = self.monthly_cap_usd()
        if cap <= 0:
            return False
        summary = self.cost_summary()
        return summary["cost_usd"] >= cap

    def cost_summary(self) -> dict[str, Any]:
        return self.decisions.cost_summary(
            since=self.month_start(),
            prompt_price_per_1k=self.price_prompt_per_1k(),
            completion_price_per_1k=self.price_completion_per_1k(),
        )

    def new_client(self) -> OpenRouterClient | None:
        """Build a fresh OpenRouterClient, or None if AI is disabled or
        spending is over the monthly cap. Caller is responsible for
        awaiting `aclose()`."""
        if not self.enabled:
            return None
        if self.spend_cap_reached():
            log.info("ai: monthly spend cap reached — skipping client creation")
            return None
        key = self.settings.openrouter_api_key.get_secret_value()  # type: ignore[union-attr]
        return OpenRouterClient(
            api_key=key,
            default_model=self.settings.openrouter_model,
            decisions=self.decisions,
            cache_ttl_hours=self.settings.ai_cache_ttl_hours,
            app_url=self.settings.openrouter_app_url,
            app_title=self.settings.openrouter_app_title,
        )
