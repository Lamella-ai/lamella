---
audience: agents
read-cost-target: 100 lines
authority: informative
status: Active Development
cross-refs: docs/adr/0050-optional-authentication.md, docs/adr/0025-logs-identify-entities-not-values.md, docs/adr/0029-sql-parameterized-subprocess-list.md
last-derived-from-code: 2026-04-27
---
# Authentication

## Summary

Optional, opt-in authentication for self-hosted Lamella. Bootstrap from
env vars on first run, durable in SQLite afterwards. Argon2id passwords,
DB-backed sessions, double-submit CSRF, per-user lockout, append-only
audit log. Single-tenant with `account_id=1` injected everywhere;
the account-id-aware shape leaves room for future multi-user mode.

## Owned routes

| Method | Path | Handler | File:line |
|---|---|---|---|
| GET  | `/login` | `login_get` | `src/lamella/web/auth/routes.py:96` |
| POST | `/login` | `login_post` | `src/lamella/web/auth/routes.py:104` |
| POST | `/logout` | `logout_post` | `src/lamella/web/auth/routes.py:218` |
| GET  | `/account/password` | `password_form_get` | `src/lamella/web/auth/routes.py:243` |
| POST | `/account/password` | `password_form_post` | `src/lamella/web/auth/routes.py:259` |

## Owned templates

- `src/lamella/web/templates/auth/_layout.html`: minimal standalone shell
- `src/lamella/web/templates/auth/login.html`
- `src/lamella/web/templates/auth/password.html`

## Owned source files

- `src/lamella/web/auth/passwords.py`: argon2id hash + verify, DUMMY_HASH
- `src/lamella/web/auth/sessions.py`: DB-backed session ops + signing
- `src/lamella/web/auth/csrf.py`: double-submit token check
- `src/lamella/web/auth/events.py`: auth event log writer
- `src/lamella/web/auth/lockout.py`: per-user lockout state machine
- `src/lamella/web/auth/bootstrap.py`: env-var → first user, session secret
- `src/lamella/web/auth/dependencies.py`: `current_user`, `current_tenant`
- `src/lamella/web/auth/middleware.py`: auth + security middleware
- `src/lamella/web/auth/routes.py`: login / logout / password change
- `src/lamella/web/auth/user.py`: `User` and `Tenant` value objects

## Owned tests

- `tests/test_auth.py`: bootstrap + login flow + bypass list + HTMX
- `tests/test_auth_security.py`: argon2 properties, signing, lockout, headers

## Owned migrations

- `migrations/060_auth_tables.sql`: `accounts`, `users`, `auth_sessions`,
  `auth_events`, `password_reset_tokens`, `mfa_secrets`. Single-row seed
  for `accounts(id=1, name='local')`.

## ADR compliance

- **[ADR-0050](../adr/0050-optional-authentication.md)**, this feature.
- **ADR-0001 (ledger as source of truth):** auth state is a disposable
  cache. Blowing away SQLite and re-bootstrapping from env vars is a
  supported recovery path.
- **ADR-0025 (logs identify entities, never values):** `auth_events`
  records `user_id`, never the attempted username/password.
- **ADR-0029 (SQL parameterized):** every auth query uses bound
  parameters; no f-string SQL.
- **ADR-0017 (example data policy):** test fixtures use
  `admin` / `bootstrap-pass-2026` placeholders.

## Configuration

Environment variables consumed by `Settings` (`src/lamella/core/config.py`):

| Var | Default | Effect |
|---|---|---|
| `HOST` | `127.0.0.1` | Bind address (non-Docker). |
| `AUTH_USERNAME` | unset | Bootstrap username. Triggers user-row insert when users table is empty. |
| `AUTH_PASSWORD` | unset | Bootstrap plaintext password (hashed on first read). |
| `AUTH_PASSWORD_HASH` | unset | Bootstrap pre-hashed password (Argon2id encoded). Takes precedence. |
| `AUTH_SESSION_SECRET` | auto | Cookie-signing secret. Auto-generates `<data_dir>/.session-secret` when unset. |
| `AUTH_SESSION_DAYS` | 30 | Session lifetime (rolled forward on activity). |
| `AUTH_LOCKOUT_THRESHOLD` | 5 | Failed logins before lock. |
| `AUTH_LOCKOUT_WINDOW_MINUTES` | 15 | Window over which failures count. |
| `AUTH_LOCKOUT_DURATION_MINUTES` | 15 | Lock duration once tripped. |

## Current state

- ✅ Bootstrap from env, durable in SQLite afterwards.
- ✅ Argon2id passwords, opportunistic rehash on parameter strengthening.
- ✅ DB-backed sessions, signed cookie wraps the session_id.
- ✅ HTMX-aware unauth response (`HX-Redirect` header, no JSON body).
- ✅ Double-submit CSRF for state-changing routes.
- ✅ Per-user lockout (5/15min/15min default).
- ✅ Append-only `auth_events` audit log.
- ✅ Security headers everywhere; tightened CSP on `/login` + `/account/*`.
- ✅ `request.state.user` and `request.state.tenant` injected always.
- ✅ Default bind change (127.0.0.1 non-Docker; Docker entrypoint sets 0.0.0.0).
- ✅ Loud startup banner when bind is non-loopback AND auth unset.

## Gaps (deferred, no UI today)

- **Forgot password / email reset.** Schema exists
  (`password_reset_tokens`); no `/forgot` route. Single-user recovery
  is "stop server, set `AUTH_PASSWORD` env, restart" until a future multi-user release.
- **MFA / TOTP.** Schema exists (`mfa_secrets`); login flow has the
  "credentials verified → login complete" boundary that a future multi-user release
  TOTP slots into.
- **Active-sessions UI.** `auth_sessions` carries IP/UA/last_seen; no
  /account/sessions surface yet.
- **Rate-limiting beyond per-user lockout.** The proxy layer
  (Cloudflare, Tailscale, fail2ban) is the intended IP-based defense.
- **Per-account data isolation.** `account_id` is plumbed through
  `request.state`; data queries do not yet filter on it. a future multi-user release
  cutover is a separate work-stream, see ADR-0050 §11.
