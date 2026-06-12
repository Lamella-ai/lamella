# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable

log = logging.getLogger(__name__)


EDITABLE_KEYS: frozenset[str] = frozenset(
    {
        "paperless_url",
        "paperless_api_token",
        # Phase 3 — AI model + spend cap editable from /settings.
        "openrouter_model",
        "openrouter_model_classify_txn",
        "openrouter_model_match_receipt",
        "openrouter_model_parse_note",
        "ai_max_monthly_spend_usd",
        "ai_price_usd_per_1k_prompt",
        "ai_price_usd_per_1k_completion",
        # Two-agent cascade — Haiku primary, Opus fallback on low
        # confidence. All three keys are runtime-editable.
        "openrouter_model_fallback",
        "openrouter_model_classify_txn_fallback",
        "ai_fallback_confidence_threshold",
        "ai_fallback_enabled",
        # Paperless verify-and-writeback (Slice A/B/C).
        "paperless_writeback_enabled",
        "openrouter_model_receipt_verify",
        # Vector-search toggle (Phase H).
        "ai_vector_search_enabled",
        # Paperless sync tuning.
        "paperless_sync_interval_hours",
        "paperless_sync_lookback_days",
        # Paperless document-type semantics used by receipts filtering
        # and matcher exclusion (receipt / invoice / ignore by type id).
        "paperless_doc_type_roles",
        # Receipt-required threshold (needs-receipt queue).
        "receipt_required_threshold_usd",
        # Phase 4 — SimpleFIN takeover.
        "simplefin_access_url",
        "simplefin_mode",
        "simplefin_fetch_interval_hours",
        "simplefin_lookback_days",
        # /setup/simplefin recovery wrapper: stamped on Skip click,
        # 7-day suppression of the recovery-progress finding.
        "simplefin_dismissed_at",
        # Phase 5 — notifications + mileage.
        "ntfy_base_url",
        "ntfy_topic",
        "ntfy_token",
        "pushover_user_key",
        "pushover_api_token",
        "mileage_rate",
        # NOTE: `mileage.vehicles` (comma-separated) was removed —
        # vehicles now come exclusively from the registry at
        # /settings/vehicles. Leaving the key here as a no-op was
        # confusing; it's off the editable list and the UI for it is
        # gone. Existing rows in app_settings are ignored by the
        # mileage page.
        "notify_digest_day",
        "notify_min_fixme_usd",
        # Phase 6 — reports + budgets + recurring.
        "audit_max_receipt_bytes",
        "budget_alert_channels",
        "recurring_scan_window_days",
        "recurring_min_occurrences",
        "estimated_tax_flat_rate",
        # Phase 7 — spreadsheet import with AI column mapping.
        "import_retention_days",
        "import_max_upload_bytes",
        "import_ai_column_map_model",
        "import_ai_confidence_threshold",
        # ADR-0064 — Paperless namespace migration completion flag.
        # Set to "1" by the lifespan wiring after a clean run; unset
        # / "0" means the migration runs on next boot. Editable so an
        # operator can force a re-run from /settings/data-integrity
        # by clearing it (e.g. after restoring a Paperless backup
        # that re-introduced legacy-named tags).
        "paperless_namespace_migration_completed",
        # Default Open-directive date for system-scaffolded accounts.
        # ISO YYYY-MM-DD. Default 1900-01-01 means "this account
        # accepts any historical transactions you import." Users
        # who want to go even further back can change this. Users
        # who want an honest opening date for a specific loan or
        # mortgage can set one per-account in the wizard modal —
        # that override flows through draft.opening_date and lands
        # on the per-account Open directive instead of the default.
        "account_default_open_date",
        # Number formatting locale — en_US or en_EU. Editable from
        # /settings/general; controls thousands/decimal separators
        # on every D.money() call across the app.
        "number_locale",
        # IANA timezone name; controls rendering of every user-facing
        # timestamp via the |local_ts filter. Validated against
        # zoneinfo on write; an invalid stored value falls back to
        # UTC per the same rule in core/config.py:57.
        "app_tz",
    }
)


class AppSettingsStore:
    """Key-value overlay over the `app_settings` table. Values written here
    take precedence over env vars at read time.

    When ``connector_config_path`` and ``main_bean_path`` are set,
    ``set()`` also stamps a ``custom "setting"`` directive into the
    ledger (non-secret keys only, per the naming-convention rule in
    ``settings_writer.is_secret_key``). This is the step-6 dual-write
    so settings survive a DB delete.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        connector_config_path: Path | None = None,
        main_bean_path: Path | None = None,
    ):
        self.conn = conn
        self._connector_config_path = connector_config_path
        self._main_bean_path = main_bean_path

    def get(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set(self, key: str, value: str) -> None:
        if not key:
            raise ValueError("app_settings key must not be empty")
        if key not in EDITABLE_KEYS:
            raise ValueError(f"app_settings key {key!r} is not editable at runtime")
        self._maybe_stamp_to_ledger(key, value)
        self.conn.execute(
            """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )

    def delete(self, key: str) -> None:
        self._maybe_stamp_unset_to_ledger(key)
        self.conn.execute("DELETE FROM app_settings WHERE key = ?", (key,))

    def _maybe_stamp_to_ledger(self, key: str, value: str) -> None:
        if self._connector_config_path is None or self._main_bean_path is None:
            return
        # Local imports avoid circular imports at module load.
        from lamella.core.ledger_writer import BeanCheckError
        from lamella.core.settings.writer import append_setting, is_secret_key

        if is_secret_key(key):
            return
        try:
            append_setting(
                connector_config=self._connector_config_path,
                main_bean=self._main_bean_path,
                key=key,
                value=value,
            )
        except BeanCheckError as exc:
            # Non-fatal: log and continue. The SQLite write is still the
            # primary contract for the request; reconstruct will pick up
            # the stamp on the next successful write.
            log.warning("settings ledger stamp failed for %s: %s", key, exc)

    def _maybe_stamp_unset_to_ledger(self, key: str) -> None:
        if self._connector_config_path is None or self._main_bean_path is None:
            return
        from lamella.core.ledger_writer import BeanCheckError
        from lamella.core.settings.writer import (
            append_setting_unset,
            is_secret_key,
        )

        if is_secret_key(key):
            return
        try:
            append_setting_unset(
                connector_config=self._connector_config_path,
                main_bean=self._main_bean_path,
                key=key,
            )
        except BeanCheckError as exc:
            log.warning("settings ledger unset failed for %s: %s", key, exc)

    def all(self, keys: Iterable[str] | None = None) -> dict[str, str]:
        if keys is None:
            rows = self.conn.execute(
                "SELECT key, value FROM app_settings"
            ).fetchall()
        else:
            keys_list = list(keys)
            if not keys_list:
                return {}
            placeholders = ",".join("?" * len(keys_list))
            rows = self.conn.execute(
                f"SELECT key, value FROM app_settings WHERE key IN ({placeholders})",
                tuple(keys_list),
            ).fetchall()
        return {row["key"]: row["value"] for row in rows}
