# Configuration reference

Every environment variable Lamella reads at boot, organized by what it
unlocks. Settings marked **(runtime)** are also editable from
`/settings` after the app is running and persist in the SQLite cache.

## Core paths

| Name | Default | Purpose |
| --- | --- | --- |
| `LAMELLA_DATA_DIR` | `/data` | SQLite + nightly backups. Legacy `CONNECTOR_DATA_DIR` and `LAMELLA_CONNECTOR_DATA_DIR` are still accepted via deprecation shim. |
| `LEDGER_DIR` | `/ledger` | Beancount ledger root. Container expects `main.bean` at this root. |
| `LAMELLA_MIGRATIONS_DIR` | `/app/migrations` | SQL migration files. You should not need to override. |
| `LAMELLA_CONFIG_DIR` | `/app/config` | Config templates. You should not need to override. |
| `PORT` | `8080` | HTTP port the app binds inside the container. |
| `HOST` | `127.0.0.1` (non-Docker) / `0.0.0.0` (in image) | Bind address. |
| `LOG_LEVEL` | `INFO` | Python logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`. |

## Authentication (ADR-0050)

Optional, but **strongly recommended**. Without auth, the app refuses
to bind to a non-loopback interface without a loud startup banner.

| Name | Default | Purpose |
| --- | --- | --- |
| `AUTH_USERNAME` | *(unset)* | First-run username to bootstrap. Ignored after the first user exists. |
| `AUTH_PASSWORD` | *(unset)* | First-run password. Hashed with Argon2id and discarded; env var is no longer read. |
| `AUTH_PASSWORD_HASH` | *(unset)* | Use a pre-computed Argon2id hash instead of plaintext. Only one of `AUTH_PASSWORD` / `AUTH_PASSWORD_HASH` should be set. |
| `AUTH_SESSION_SECRET` | *(auto)* | HMAC key for signed session cookies. Auto-generated to `${LAMELLA_DATA_DIR}/.session-secret` on first boot. Do not delete that file unless you want every existing session invalidated. |
| `AUTH_SESSION_DAYS` | `30` | Session lifetime. After expiry the user signs in again. |
| `AUTH_LOCKOUT_THRESHOLD` | `5` | Failed-login attempts before lockout. |
| `AUTH_LOCKOUT_WINDOW_MINUTES` | `15` | Window the failed attempts must fall inside. |
| `AUTH_LOCKOUT_DURATION_MINUTES` | `15` | How long the lockout lasts. |

## Paperless-ngx (optional)

| Name | Default | Purpose |
| --- | --- | --- |
| `PAPERLESS_URL` | *(unset)* | Base URL of your Paperless-ngx instance. |
| `PAPERLESS_API_TOKEN` | *(unset)* | API token. Generate one in Paperless under Profile → API. |
| `PAPERLESS_WRITEBACK_ENABLED` | `0` | Enable AI-correction writeback. Off-by-default for safety. **(runtime)** |

## OpenRouter / AI classification (optional)

| Name | Default | Purpose |
| --- | --- | --- |
| `OPENROUTER_API_KEY` | *(unset)* | Without this, classification is rule-only. |
| `OPENROUTER_MODEL` | `anthropic/claude-haiku-4.5` | Primary classify model. **(runtime)** |
| `OPENROUTER_MODEL_FALLBACK` | `anthropic/claude-opus-4.7` | Fires when primary returns < threshold confidence. **(runtime)** |
| `AI_FALLBACK_CONFIDENCE_THRESHOLD` | `0.60` | The cascade trigger. **(runtime)** |
| `AI_FALLBACK_ENABLED` | `1` | `0` disables the cascade entirely. **(runtime)** |
| `AI_VECTOR_SEARCH_ENABLED` | `1` | Vector similar-txn context. **(runtime)** |
| `AI_MAX_MONTHLY_SPEND_USD` | `0` | Hard spending cap. `0` = unlimited. **(runtime)** |
| `OPENROUTER_MODEL_RECEIPT_VERIFY` | `anthropic/claude-opus-4.7` | Vision model for `/receipts/verify`. **(runtime)** |

## SimpleFIN (optional bank sync)

| Name | Default | Purpose |
| --- | --- | --- |
| `SIMPLEFIN_ACCESS_URL` | *(unset)* | SimpleFIN bridge URL with `user:pass@`. Also editable on `/simplefin`. |
| `SIMPLEFIN_MODE` | `disabled` | `disabled` / `shadow` / `active`. Shadow logs without writing. |
| `SIMPLEFIN_FETCH_INTERVAL_HOURS` | `6` | Scheduled fetch interval. |
| `SIMPLEFIN_LOOKBACK_DAYS` | `14` | Window requested from the bridge. |
| `SIMPLEFIN_ACCOUNT_MAP_PATH` | `${LEDGER_DIR}/simplefin_account_map.yml` | Maps SimpleFIN account ids to Beancount source accounts. Copy `simplefin_account_map.yml.example` and edit. |

## Receipt thresholds

| Name | Default | Purpose |
| --- | --- | --- |
| `RECEIPT_REQUIRED_THRESHOLD_USD` | `75` | Transactions ≥ this amount get the "receipt required" badge (IRS-aligned default). **(runtime)** |

## Number formatting

| Name | Default | Purpose |
| --- | --- | --- |
| `LAMELLA_NUMBER_LOCALE` | `en_US` | Controls thousands + decimal separators on every money display. Two values today: `en_US` (`1,234.56`) and `en_EU` (`1.234,56`). Affects `D.money()` and every transaction summary across the app. **(runtime)** |

## Volumes

The container expects two bind mounts:

- **`/data`**: writable. SQLite DB, backups, audit logs, the
  auto-generated session secret. Do not delete this without backing
  up first.
- **`/ledger`**: writable. The Beancount ledger root. Connector-owned
  files (`connector_overrides.bean`, `connector_links.bean`,
  `simplefin_transactions.bean`, etc.) get appended to your `main.bean`
  include list on first write. Other ledger files are read-only from
  the app's perspective.

The Docker image runs as a non-root `app` user. If you map host
volumes, ensure they're writable by the in-image uid (1000 by default;
use `PUID` / `PGID` env vars to override; see `docker-compose.unraid.yml`
for an example).

## Where settings actually live

- **Boot-time only**: paths, ports, log level, auth bootstrap.
  Reading `.env` once at process start. Changing them requires a
  container restart.
- **Runtime, persisted in SQLite**: AI model choice, fallback
  threshold, monthly spend cap, Paperless writeback toggle. The
  `/settings` page is the source of truth; env vars seed the initial
  values but the DB owns them after first save.
- **Auto-generated, on disk**: the session HMAC secret in
  `${LAMELLA_DATA_DIR}/.session-secret`. Persists across restarts.

## Legacy compatibility

These names still work via the deprecation shim:

- `CONNECTOR_DATA_DIR` → `LAMELLA_DATA_DIR`
- `CONNECTOR_OVERRIDES_DIR` → derived from `LEDGER_DIR`
- `BCG_SKIP_DISCOVERY_GUARD` → `LAMELLA_SKIP_DISCOVERY_GUARD`
- `bcg-*` metadata keys are read transparently; never written
  (see ADR-0003).

You'll see one DeprecationWarning per legacy name in the logs. Migrate
when convenient.
