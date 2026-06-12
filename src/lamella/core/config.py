# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

from __future__ import annotations

import os
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Pydantic reads this field from env var DATA_DIR (case-insensitive,
    # no env_prefix is configured on this class). The documented user-
    # facing name is LAMELLA_DATA_DIR — _legacy_env.apply_env_aliases()
    # copies LAMELLA_DATA_DIR (and the legacy CONNECTOR_DATA_DIR /
    # LAMELLA_CONNECTOR_DATA_DIR names) into DATA_DIR so pydantic
    # picks them up here. Old field name `connector_data_dir` is kept
    # as a deprecated read-only property below.
    data_dir: Path = Field(default=Path("/data"))
    ledger_dir: Path = Field(default=Path("/ledger"))

    paperless_url: str | None = Field(default=None)
    paperless_api_token: SecretStr | None = Field(default=None)
    # Optional Cloudflare Access service-token pair — required when the
    # Paperless host sits behind Cloudflare Zero Trust / Access. Without
    # these, every API call gets 302'd to cloudflareaccess.com and the
    # Paperless token can't be checked.
    paperless_cf_access_client_id: SecretStr | None = Field(default=None)
    paperless_cf_access_client_secret: SecretStr | None = Field(default=None)

    port: int = Field(default=8080)
    # ADR-0050 — bind defaults to loopback for non-Docker runs. Docker
    # entrypoint sets HOST=0.0.0.0 explicitly because the container's
    # network namespace is isolated; the operator's port mapping is the
    # real exposure boundary. Outside Docker, defaulting to 0.0.0.0 is
    # how people accidentally publish their financial data to the
    # internet, so we make the safe choice the default.
    host: str = Field(default="127.0.0.1")
    log_level: str = Field(default="INFO")
    # Calendar + date-from-timestamp derivation. IANA tz name;
    # must resolve via zoneinfo. An invalid value falls back to
    # UTC with a log warning.
    app_tz: str = Field(default="UTC")

    # Phase 3 — OpenRouter / AI.
    openrouter_api_key: SecretStr | None = Field(default=None)
    openrouter_model: str = Field(default="anthropic/claude-haiku-4.5")
    openrouter_app_url: str | None = Field(default=None)
    openrouter_app_title: str | None = Field(default="lamella")
    ai_cache_ttl_hours: int = Field(default=24)
    ai_max_monthly_spend_usd: float = Field(default=0.0)
    # Phase H — vector-search over resolved transactions + user
    # corrections. ON by default; sentence-transformers is a core
    # dependency so the feature works out of the box. Falls back
    # to the substring path only if the model fails to load or the
    # import is somehow broken — never silently on a fresh install.
    ai_vector_search_enabled: bool = Field(default=True)
    ai_vector_model_name: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2"
    )
    ai_vector_correction_weight: float = Field(default=2.0)
    # Two-agent cascade. The primary model (`openrouter_model`) is a
    # cheap workhorse (Haiku). When its confidence on a classify
    # comes back below `ai_fallback_confidence_threshold`, we retry
    # once with `openrouter_model_fallback` (a stronger model) so
    # the hard cases get a second opinion without doubling cost on
    # the easy ones. Set fallback to None or equal to the primary
    # to disable.
    openrouter_model_fallback: str | None = Field(
        default="anthropic/claude-opus-4.7"
    )
    ai_fallback_confidence_threshold: float = Field(default=0.60)
    ai_fallback_enabled: bool = Field(default=True)
    # Paperless verify-and-writeback. Disabled by default because
    # it makes authenticated WRITES to the user's Paperless. Opt
    # in from /settings once the first few corrections look right.
    # Vision model defaults to Opus (strong at OCR + self-honest
    # about confidence); any vision-capable OpenRouter model
    # works — per-user configurable in /settings.
    paperless_writeback_enabled: bool = Field(default=False)
    # ADR-0063 — reverse-direction (document -> txn) auto-link. After
    # each Paperless sync, walk extracted-but-unlinked docs and
    # auto-link any whose top-candidate score >= AUTO_LINK_THRESHOLD
    # (0.90) AND the second-place candidate trails by at least
    # ``paperless_auto_link_min_confidence_gap``. Default ON because
    # the threshold + gap together make the false-positive rate
    # vanishingly small (and the link is easily reversible from
    # /txn/<token>).
    paperless_auto_link_enabled: bool = Field(default=True)
    paperless_auto_link_min_confidence_gap: float = Field(default=0.10)
    # ADR-0043 / ADR-0043b — staged-txn directives. When True, the
    # bank-sync defer path writes a `custom "staged-txn"` directive
    # to the connector-owned .bean file alongside the staged_transactions
    # row (the directive is the source of truth; the SQLite row is the
    # cache). Default OFF in v0.3.1 — turn on per-user via /settings
    # once you've confirmed the migration is safe on your ledger.
    # A follow-up release will flip the default to True after the
    # soak window confirms zero new FIXMEs in real ledgers; a later
    # release removes the legacy FIXME-emission code entirely.
    enable_staged_txn_directives: bool = Field(default=False)
    openrouter_model_receipt_verify: str = Field(
        default="anthropic/claude-opus-4.7"
    )

    # Phase 4 — SimpleFIN takeover.
    simplefin_access_url: SecretStr | None = Field(default=None)
    simplefin_mode: str = Field(default="disabled")  # disabled | shadow | active
    simplefin_fetch_interval_hours: int = Field(default=6)
    simplefin_lookback_days: int = Field(default=14)
    simplefin_account_map_path: Path | None = Field(default=None)

    # Default Open-directive date for accounts the system scaffolds
    # (wizard scaffolds, category trees, vehicle/property charts).
    # Beancount requires Open.date <= every posting against the
    # account; if we use today's date and the user later imports
    # historical transactions, bean-check rejects them as "inactive
    # account at the time of transaction." Default 1900-01-01 is
    # an unambiguous "this account exists for any history you
    # might import" marker - covers accounts opened in any era.
    # Users who want a more honest date can override per-account
    # in the bank-account modal or change this setting globally.
    account_default_open_date: str = Field(default="1900-01-01")

    # Phase 5 — notifications + mileage.
    ntfy_base_url: str = Field(default="https://ntfy.sh")
    ntfy_topic: str | None = Field(default=None)
    ntfy_token: SecretStr | None = Field(default=None)
    pushover_user_key: SecretStr | None = Field(default=None)
    pushover_api_token: SecretStr | None = Field(default=None)
    # ADR-0022: tax-relevant rate multiplied with miles to produce a
    # Schedule-C dollar deduction; stored as Decimal so the rate→$
    # path is float-free end-to-end. Pydantic v2 coerces string/int
    # env-var values into Decimal natively.
    mileage_rate: Decimal = Field(default=Decimal("0.67"))
    mileage_csv_path: Path | None = Field(default=None)
    notify_digest_day: str = Field(default="Monday")
    # ADR-0022: money threshold compared against transaction amounts;
    # Decimal so the comparison side stays in the money type.
    notify_min_fixme_usd: Decimal = Field(default=Decimal("500"))

    # Phase 6 — PDF reports + budgets + recurring.
    reports_output_dir: Path | None = Field(default=None)
    audit_max_receipt_bytes: int = Field(default=10_000_000)
    budget_alert_channels: str = Field(default="")  # CSV; empty = all enabled
    recurring_scan_window_days: int = Field(default=540)
    recurring_min_occurrences: int = Field(default=3)
    # ADR-0022: tax rate multiplied with money to produce estimated
    # quarterly tax owed; Decimal so the multiplication stays in
    # money precision.
    estimated_tax_flat_rate: Decimal = Field(default=Decimal("0.25"))

    # Phase 8 — txn-first receipt workflow.
    # ADR-0022: dollar threshold compared against expense amounts.
    receipt_required_threshold_usd: Decimal = Field(default=Decimal("75"))

    # Number formatting locale. Controls thousands + decimal
    # separators in every D.money() / T.summary() amount across the
    # app. Two values today:
    #   - "en_US" → 1,234.56  (default)
    #   - "en_EU" → 1.234,56
    # Set via env LAMELLA_NUMBER_LOCALE or in /settings/general.
    # Why a Settings field, not just an env var: locale is a UX
    # preference, not a deploy-time constant — the operator should
    # be able to flip it without restarting the container.
    number_locale: str = Field(default="en_US")
    paperless_sync_interval_hours: int = Field(default=6)
    # ADR-0062 — tag-driven workflow engine. ``enabled`` gates the
    # scheduler entirely; ``interval_minutes`` controls the cadence
    # of the periodic doc-tag-workflow tick. Date-sanity bounds are
    # the defaults applied when the scheduler runs the
    # date_sanity_check rule autonomously; the on-demand UI accepts
    # per-invocation overrides.
    paperless_workflow_enabled: bool = Field(default=True)
    paperless_workflow_interval_minutes: int = Field(default=60)
    paperless_date_sanity_min_year: int = Field(default=2000)
    paperless_date_sanity_max_date_offset_days: int = Field(default=0)
    # ADR-0064 — one-time Paperless namespace migration from
    # ``Lamella_X`` (legacy underscore) to ``Lamella:X`` (canonical
    # colon). False on first boot after the upgrade; the lifespan
    # wiring runs the migration and flips this to True after a clean
    # run. Subsequent boots skip the migration entirely. The flag is
    # intentionally a Settings field so the Setup status panel can
    # surface "Paperless namespace migrated" as a one-line
    # observability breadcrumb.
    paperless_namespace_migration_completed: bool = Field(default=False)
    # Reboot ingest — disabled by default. The scan stages EVERY
    # ledger transaction onto the unified staging surface, including
    # already-classified ones, which can surface "duplicates" on the
    # review queue and confuse users. Until the flow is properly
    # isolated (skip already-classified entries, preserve all
    # metadata, atomic backup-before-write, recovery-on-failure),
    # operators must opt in via env LAMELLA_REBOOT_INGEST_ENABLED=1
    # or the /settings/data-integrity force-enable. The /settings
    # surface explains the destructive failure modes inline.
    reboot_ingest_enabled: bool = Field(default=False)
    # Paperless sync lookback. Default raised to 3650 (10 years)
    # so first-run picks up receipts older than 2 years — most
    # self-employed users have receipts spanning the full business
    # lifetime they want to classify against. Lower it for a
    # smaller sync surface via `/settings`.
    paperless_sync_lookback_days: int = Field(default=3650)

    # Phase 7 — spreadsheet import with AI column mapping.
    import_upload_dir: Path | None = Field(default=None)
    import_ledger_output_dir: Path | None = Field(default=None)
    import_retention_days: int = Field(default=90)
    import_max_upload_bytes: int = Field(default=50_000_000)
    import_ai_column_map_model: str | None = Field(default=None)
    import_ai_confidence_threshold: float = Field(default=0.7)

    # ADR-0050 — optional authentication.
    #
    # Auth is OFF by default; setting AUTH_USERNAME (and exactly one
    # of AUTH_PASSWORD or AUTH_PASSWORD_HASH) on first run BOOTSTRAPS
    # a single user into the `users` table. Subsequent runs ignore
    # these env vars — credentials are durable in SQLite and the user
    # changes them via /account/password. The session cookie holds a
    # signed session_id; the server-side `auth_sessions` row carries
    # the user binding, expiry, and revocation state.
    auth_username: str | None = Field(default=None)
    auth_password: SecretStr | None = Field(default=None)
    auth_password_hash: SecretStr | None = Field(default=None)
    # Cookie-signing secret. If unset, the auth bootstrap auto-creates
    # data_dir/.session-secret on first start and persists it across
    # restarts. Override via env to share a secret across replicas in
    # SaaS-day deployments.
    auth_session_secret: SecretStr | None = Field(default=None)
    auth_session_days: int = Field(default=30)
    # Per-user lockout (depth-in-defense; the proxy layer is the real
    # rate-limit boundary). Five failures inside fifteen minutes locks
    # the user for fifteen minutes; success during the lockout window
    # is still rejected; the counter resets on success outside it.
    auth_lockout_threshold: int = Field(default=5)
    auth_lockout_window_minutes: int = Field(default=15)
    auth_lockout_duration_minutes: int = Field(default=15)

    @property
    def connector_data_dir(self) -> Path:
        """Deprecated alias for :attr:`data_dir` — read-only.

        Pre-rebrand the field was named ``connector_data_dir`` and was
        read from env var ``CONNECTOR_DATA_DIR``. Both names continue
        to resolve via this property + ``_legacy_env.apply_env_aliases``;
        new code should reference ``settings.data_dir`` directly.
        """
        return self.data_dir

    @property
    def db_path(self) -> Path:
        # New default: lamella.sqlite. Existing deploys carry
        # beancounter-glue.sqlite — lamella.bootstrap.db_migrate
        # renames the legacy file in-place at startup before any
        # migrations run, so this property can return the new
        # name unconditionally.
        return self.data_dir / "lamella.sqlite"

    @property
    def legacy_db_path(self) -> Path:
        """Legacy SQLite filename from the beancounter-glue era.
        Used by the rename-on-startup shim (see ``main.py`` lifespan).
        Drop together with the env-var deprecation shim."""
        return self.data_dir / "beancounter-glue.sqlite"

    @property
    def backups_dir(self) -> Path:
        return self.data_dir / "backups"

    @property
    def ledger_main(self) -> Path:
        return self.ledger_dir / "main.bean"

    @property
    def connector_links_path(self) -> Path:
        return self.ledger_dir / "connector_links.bean"

    @property
    def connector_overrides_path(self) -> Path:
        return self.ledger_dir / "connector_overrides.bean"

    @property
    def connector_accounts_path(self) -> Path:
        return self.ledger_dir / "connector_accounts.bean"

    @property
    def simplefin_transactions_path(self) -> Path:
        return self.ledger_dir / "simplefin_transactions.bean"

    @property
    def simplefin_preview_path(self) -> Path:
        return self.ledger_dir / "simplefin_transactions.connector_preview.bean"

    @property
    def reports_output_resolved(self) -> Path:
        if self.reports_output_dir is not None:
            return self.reports_output_dir
        return self.data_dir / "reports"

    @property
    def mileage_csv_resolved(self) -> Path:
        if self.mileage_csv_path is not None:
            return self.mileage_csv_path
        return self.ledger_dir / "mileage" / "vehicles.csv"

    @property
    def mileage_summary_path(self) -> Path:
        return self.ledger_dir / "mileage_summary.bean"

    @property
    def connector_rules_path(self) -> Path:
        return self.ledger_dir / "connector_rules.bean"

    @property
    def connector_budgets_path(self) -> Path:
        return self.ledger_dir / "connector_budgets.bean"

    @property
    def connector_config_path(self) -> Path:
        return self.ledger_dir / "connector_config.bean"

    @property
    def connector_transfers_path(self) -> Path:
        return self.ledger_dir / "connector_transfers.bean"

    @property
    def ntfy_enabled(self) -> bool:
        return bool(self.ntfy_topic)

    @property
    def pushover_enabled(self) -> bool:
        return bool(
            self.pushover_user_key
            and self.pushover_user_key.get_secret_value()
            and self.pushover_api_token
            and self.pushover_api_token.get_secret_value()
        )

    @property
    def simplefin_account_map_resolved(self) -> Path:
        if self.simplefin_account_map_path is not None:
            return self.simplefin_account_map_path
        return self.ledger_dir / "simplefin_account_map.yml"

    @property
    def config_dir(self) -> Path:
        # In the image the package lives inside the venv, so the repo-relative parents[3]
        # walk doesn't land on /app/config. The Docker image sets LAMELLA_CONFIG_DIR
        # (canonical) and pre-rebrand installs use CONNECTOR_CONFIG_DIR; route through
        # read_env so both work AND a legacy-only deploy logs a one-shot deprecation.
        # Fall through to the repo layout for dev runs.
        from lamella.utils._legacy_env import read_env
        override = (
            read_env("LAMELLA_CONFIG_DIR")
            or os.environ.get("CONFIG_DIR")
        )
        if override:
            return Path(override)
        return Path(__file__).resolve().parents[3] / "config"

    @property
    def import_upload_dir_resolved(self) -> Path:
        if self.import_upload_dir is not None:
            return self.import_upload_dir
        return self.data_dir / "imports"

    @property
    def import_ledger_output_dir_resolved(self) -> Path:
        if self.import_ledger_output_dir is not None:
            return self.import_ledger_output_dir
        return self.ledger_dir / "connector_imports"

    @property
    def import_ai_column_map_model_resolved(self) -> str:
        return self.import_ai_column_map_model or self.openrouter_model

    @property
    def schedule_c_lines_path(self) -> Path:
        return self.config_dir / "schedule_c_lines.yml"

    @property
    def schedule_f_lines_path(self) -> Path:
        return self.config_dir / "schedule_f_lines.yml"

    @property
    def personal_categories_path(self) -> Path:
        """Schedule A + common personal-living categories for the
        Personal entity. Used for any entity whose tax_schedule is
        'A' or 'Personal', or whose slug is Personal without a
        business schedule. Mirrors the shape of the Schedule C/F
        yaml so ``scaffold_paths_for_entity`` can consume it too."""
        return self.config_dir / "personal_categories.yml"

    @property
    def paperless_configured(self) -> bool:
        return bool(self.paperless_url and self.paperless_api_token)

    def paperless_extra_headers(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if self.paperless_cf_access_client_id and self.paperless_cf_access_client_secret:
            out["CF-Access-Client-Id"] = self.paperless_cf_access_client_id.get_secret_value()
            out["CF-Access-Client-Secret"] = self.paperless_cf_access_client_secret.get_secret_value()
        return out

    @property
    def auth_enabled(self) -> bool:
        """Auth is on when bootstrap creds are present OR a user row
        already exists. The bootstrap-only signal is what we have at
        Settings load time; the helper is conservative and treats
        either env-var (username + (password OR password_hash)) as
        "auth enabled". Runtime callers that need ground truth read
        from the users table via auth.bootstrap.has_users()."""
        if not self.auth_username:
            return False
        return bool(self.auth_password or self.auth_password_hash)

    @property
    def session_secret_path(self) -> Path:
        """Per-install auto-generated session secret. Created on first
        startup if AUTH_SESSION_SECRET is unset; persisted across
        restarts so long-lived sessions keep validating."""
        return self.data_dir / ".session-secret"

    @property
    def ai_enabled(self) -> bool:
        return self.openrouter_api_key is not None and bool(
            self.openrouter_api_key.get_secret_value()
        )

    def masked_paperless_token(self) -> str:
        if not self.paperless_api_token:
            return ""
        value = self.paperless_api_token.get_secret_value()
        if len(value) <= 4:
            return "*" * len(value)
        return "*" * (len(value) - 4) + value[-4:]

    def apply_kv_overrides(self, overrides: dict[str, str]) -> None:
        """Overlay values from the `app_settings` KV table on top of env vars.

        Only keys listed in `AppSettingsStore.EDITABLE_KEYS` are honored;
        unknown keys are ignored."""
        if "paperless_url" in overrides:
            self.paperless_url = overrides["paperless_url"] or None
        if "paperless_api_token" in overrides:
            value = overrides["paperless_api_token"]
            self.paperless_api_token = SecretStr(value) if value else None
        if "openrouter_model" in overrides and overrides["openrouter_model"]:
            self.openrouter_model = overrides["openrouter_model"]
        if "ai_max_monthly_spend_usd" in overrides:
            try:
                self.ai_max_monthly_spend_usd = float(
                    overrides["ai_max_monthly_spend_usd"] or 0
                )
            except ValueError:
                pass
        if "simplefin_access_url" in overrides:
            value = overrides["simplefin_access_url"]
            self.simplefin_access_url = SecretStr(value) if value else None
        if "simplefin_mode" in overrides:
            mode = (overrides["simplefin_mode"] or "").strip().lower()
            if mode in {"disabled", "shadow", "active"}:
                self.simplefin_mode = mode
        if "simplefin_fetch_interval_hours" in overrides:
            try:
                self.simplefin_fetch_interval_hours = max(
                    1, int(overrides["simplefin_fetch_interval_hours"] or 6)
                )
            except ValueError:
                pass
        if "simplefin_lookback_days" in overrides:
            try:
                self.simplefin_lookback_days = max(
                    1, int(overrides["simplefin_lookback_days"] or 14)
                )
            except ValueError:
                pass
        if "ntfy_base_url" in overrides and overrides["ntfy_base_url"]:
            self.ntfy_base_url = overrides["ntfy_base_url"].strip()
        if "ntfy_topic" in overrides:
            self.ntfy_topic = (overrides["ntfy_topic"] or "").strip() or None
        if "ntfy_token" in overrides:
            value = overrides["ntfy_token"]
            self.ntfy_token = SecretStr(value) if value else None
        if "pushover_user_key" in overrides:
            value = overrides["pushover_user_key"]
            self.pushover_user_key = SecretStr(value) if value else None
        if "pushover_api_token" in overrides:
            value = overrides["pushover_api_token"]
            self.pushover_api_token = SecretStr(value) if value else None
        if "mileage_rate" in overrides:
            # ADR-0022: mileage_rate is Decimal; parse user override
            # as Decimal to keep the field type honest.
            try:
                self.mileage_rate = Decimal(str(overrides["mileage_rate"] or 0))
            except (ValueError, ArithmeticError):
                pass
        if "notify_min_fixme_usd" in overrides:
            # ADR-0022: notify_min_fixme_usd is a money threshold
            # (Decimal); keep the override path on the same type.
            try:
                self.notify_min_fixme_usd = Decimal(
                    str(overrides["notify_min_fixme_usd"] or 0)
                )
            except (ValueError, ArithmeticError):
                pass
        if "notify_digest_day" in overrides and overrides["notify_digest_day"]:
            self.notify_digest_day = overrides["notify_digest_day"].strip()
        if "audit_max_receipt_bytes" in overrides:
            try:
                self.audit_max_receipt_bytes = max(
                    1, int(overrides["audit_max_receipt_bytes"] or 10_000_000)
                )
            except ValueError:
                pass
        if "budget_alert_channels" in overrides:
            self.budget_alert_channels = (
                overrides["budget_alert_channels"] or ""
            ).strip()
        if "recurring_scan_window_days" in overrides:
            try:
                self.recurring_scan_window_days = max(
                    30, int(overrides["recurring_scan_window_days"] or 540)
                )
            except ValueError:
                pass
        if "recurring_min_occurrences" in overrides:
            try:
                self.recurring_min_occurrences = max(
                    2, int(overrides["recurring_min_occurrences"] or 3)
                )
            except ValueError:
                pass
        if "estimated_tax_flat_rate" in overrides:
            # ADR-0022: parse the user-supplied override as Decimal so
            # the field type stays honest. Decimal(str(...)) is the
            # safe extraction pattern — accepts ints/floats too.
            try:
                self.estimated_tax_flat_rate = Decimal(
                    str(overrides["estimated_tax_flat_rate"] or 0)
                )
            except (ValueError, ArithmeticError):
                pass
        if "import_retention_days" in overrides:
            try:
                self.import_retention_days = max(
                    1, int(overrides["import_retention_days"] or 90)
                )
            except ValueError:
                pass
        if "import_max_upload_bytes" in overrides:
            try:
                self.import_max_upload_bytes = max(
                    1_000_000, int(overrides["import_max_upload_bytes"] or 50_000_000)
                )
            except ValueError:
                pass
        if "import_ai_column_map_model" in overrides:
            value = (overrides["import_ai_column_map_model"] or "").strip()
            self.import_ai_column_map_model = value or None
        if "import_ai_confidence_threshold" in overrides:
            try:
                self.import_ai_confidence_threshold = float(
                    overrides["import_ai_confidence_threshold"] or 0.7
                )
            except ValueError:
                pass
        if "number_locale" in overrides:
            value = (overrides["number_locale"] or "").strip()
            if value in ("en_US", "en_EU"):
                self.number_locale = value
        if "receipt_required_threshold_usd" in overrides:
            # ADR-0022: receipt threshold is a dollar value (Decimal).
            try:
                self.receipt_required_threshold_usd = Decimal(
                    str(overrides["receipt_required_threshold_usd"] or "75")
                )
            except (ValueError, ArithmeticError):
                pass
        if "paperless_sync_interval_hours" in overrides:
            try:
                self.paperless_sync_interval_hours = max(
                    1, int(overrides["paperless_sync_interval_hours"] or 6)
                )
            except ValueError:
                pass
        if "paperless_sync_lookback_days" in overrides:
            try:
                self.paperless_sync_lookback_days = max(
                    1, int(overrides["paperless_sync_lookback_days"] or 3650)
                )
            except ValueError:
                pass
        if "openrouter_model_fallback" in overrides:
            value = (overrides["openrouter_model_fallback"] or "").strip()
            self.openrouter_model_fallback = value or None
        if "ai_fallback_confidence_threshold" in overrides:
            try:
                self.ai_fallback_confidence_threshold = float(
                    overrides["ai_fallback_confidence_threshold"] or 0.60
                )
            except ValueError:
                pass
        if "ai_fallback_enabled" in overrides:
            raw = str(overrides["ai_fallback_enabled"] or "").strip().lower()
            self.ai_fallback_enabled = raw not in ("0", "false", "no", "off")
        if "paperless_writeback_enabled" in overrides:
            raw = str(overrides["paperless_writeback_enabled"] or "").strip().lower()
            self.paperless_writeback_enabled = raw not in ("0", "false", "no", "off")
        if "paperless_namespace_migration_completed" in overrides:
            # ADR-0064 — flipped to "1" by the lifespan wiring after a
            # clean migration run. Anything that parses as "true" maps
            # to True; the empty string / "0" / unset keeps the default
            # False so the migration retries on next boot.
            raw = str(
                overrides["paperless_namespace_migration_completed"] or ""
            ).strip().lower()
            self.paperless_namespace_migration_completed = raw not in (
                "", "0", "false", "no", "off",
            )
        if "openrouter_model_receipt_verify" in overrides:
            value = (overrides["openrouter_model_receipt_verify"] or "").strip()
            if value:
                self.openrouter_model_receipt_verify = value
        if "app_tz" in overrides:
            import zoneinfo as _zoneinfo
            import logging as _logging
            value = (overrides["app_tz"] or "").strip()
            if value:
                try:
                    _zoneinfo.ZoneInfo(value)
                    self.app_tz = value
                except _zoneinfo.ZoneInfoNotFoundError:
                    _logging.getLogger(__name__).warning(
                        "app_tz override %r is not a valid IANA timezone; "
                        "keeping current value (%r)",
                        value,
                        self.app_tz,
                    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
