# ADR-0050: Optional authentication with financial-grade defaults

- **Status:** Accepted
- **Date:** 2026-04-27
- **Related:** [ADR-0007](0007-entity-first-account-hierarchy.md),
  [ADR-0025](0025-logs-identify-entities-not-values.md),
  [ADR-0029](0029-sql-parameterized-subprocess-list.md),
  [ADR-0030](0030-file-operations-validate-allowed-roots.md)

## Context

Lamella has been running without authentication. The product
intent is "your laptop or NAS, single user, behind your network."
Adjacent self-hosted Beancount tools (Fava, lazybeancount) take the
same position: auth is the operator's job, configure a reverse proxy.

That position is defensible for a CLI-style tool. It is not defensible
for a web application that holds the user's full financial history.
The failure mode is asymmetric: misconfiguring a router or running
`docker run -p 0.0.0.0:5000:5000` once exposes the entire ledger to
the internet. Reviewers, employers, accountants, and real adversaries
all read the same surface.

Two further pressures push us toward owning the auth boundary:

1. **Financial product expectations.** Users handing financial software
   their bank-feed credentials, receipt photos, and tax-relevant
   classifications expect the application to defend itself, not to
   delegate that to deployment hygiene.

2. **Future-proof account model.** Even single-operator deployments
   benefit from an account-id-aware data layer: it makes per-user
   audit trails and any future multi-user mode (family member,
   accountant read-only access) a no-op rather than a migration.
   The cost of the account-shaped abstraction today is negligible.

This ADR records the auth design that ships now: optional, opt-in
via env var, financial-grade defaults.

## Decision

### 1. Default bind is loopback; auth is opt-in but strongly nudged

The non-Docker default for `HOST` is `127.0.0.1`. Local-only is the
only configuration that works without auth. Docker continues to bind
`0.0.0.0` inside the container because the container's network
namespace is isolated; the operator's port mapping (`-p 8080:8080` vs
`-p 127.0.0.1:8080:8080` vs none) is the real exposure control.

If the configured `HOST` is non-loopback AND no auth credentials are
configured, the application emits a multi-line warning banner at
startup. The application continues to start; the operator made a
choice and the logs preserve a record that the choice was visible.

### 2. Bootstrap from env vars; durable state is in SQLite

Auth credentials enter the system through env vars on first run and
become inert thereafter. The shape:

| Env var | Mode |
|---|---|
| `AUTH_USERNAME` + `AUTH_PASSWORD` | Plaintext bootstrap. Hashed on first read into the `users` table. |
| `AUTH_USERNAME` + `AUTH_PASSWORD_HASH` | Pre-hashed bootstrap. Inserted as-is. |
| neither | Auth disabled. Single-user `User("admin", account_id=1)` injected on every request. |

On startup, if the `users` table is empty AND env-var credentials are
set, the bootstrap helper inserts one user. On subsequent starts the
table already has a row; env vars are ignored. Users change their
password through the in-app `/account/password` flow, which is the
only path that mutates the `users` table once bootstrapped.

This is the same shape future multi-user mode uses, registration upserts users, the
auth check runs against the table, except today there's a single user
and the "registration" is `docker-compose up`. Migration to future multi-user mode is
"add a real registration flow" not "rewrite authentication."

### 3. Argon2id passwords from day one

Passwords are hashed with **Argon2id** (`argon2-cffi`). Bcrypt is
acceptable; argon2id is the OWASP / NIST SP 800-63B current
recommendation and the cost of choosing it now is one Python
dependency. Migrating from bcrypt to argon2id later forces a
mandatory password reset for every user. There is no reason to
inherit that debt.

Plaintext-from-env-var (`AUTH_PASSWORD`) is hashed via Argon2id on
first read; the env var value is never stored.

### 4. DB-backed sessions; cookie holds an opaque session id

Sessions live in `auth_sessions(session_id, user_id, account_id,
created_at, last_seen, expires_at, ua, ip, revoked_at)`. The cookie
contains the `session_id` only, signed with `itsdangerous` so a
forged cookie fails validation before a DB lookup.

Server-side state is the load-bearing choice. It buys:

- **Logout that actually logs out.** Signed-cookie-only sessions can
  only expire; they cannot be revoked. Setting `revoked_at` makes the
  cookie inert immediately on the next request.
- **Password change kills all sessions.** Routine post-incident hygiene.
- **Active-sessions UI later.** future multi-user mode-day surface; today the data is
  written and unused.
- **Session rotation on login** (anti session-fixation). Every
  successful credential check issues a new `session_id` and
  invalidates the old one.

Session lifetime defaults to 30 days (`AUTH_SESSION_DAYS`); rolled
forward by `last_seen` on every authenticated request.

### 5. Cookie security flags

- `HttpOnly` always (no JS access).
- `SameSite=Lax` (Strict breaks `next` redirect flow; Lax is the
  modern default and matches OWASP guidance for session cookies).
- `Secure` when the request indicates HTTPS (via `X-Forwarded-Proto`
  if behind a proxy, or the request's own scheme).
- Cookie name uses the `__Host-` prefix when `Secure` is set.
  Binds the cookie to the host with no `Domain` attribute, defeats
  subdomain takeover and cross-host injection.

### 6. CSRF protection is required

Every state-changing route (POST, PUT, DELETE, PATCH) is CSRF-protected
via the **double-submit cookie** pattern: a CSRF token is generated
per session, exposed in the response (HTMX response header
`X-CSRF-Token` and template global `csrf_token`), and required as
either a hidden form input (`_csrf`) or an `X-CSRF-Token` request
header. GET routes are exempt. The login route is exempt because the
session does not yet exist; it relies on credential check + same-site
cookie semantics. Logout requires CSRF (it is destructive).

For HTMX, the base layout sets `hx-headers='{"X-CSRF-Token": "..."}'`
on `<body>` so every htmx-driven POST carries the token without
per-form wiring.

### 7. Constant-time credential check; generic errors

The credential-check code path:

1. Look up user by username.
2. If not found, verify against a fixed `DUMMY_HASH` (so the response
   time of "user not found" matches "wrong password").
3. Constant-time compare on the verification result.
4. Return single generic message: *"Invalid username or password."*

No username-existence oracle. No timing oracle.

### 8. Per-user lockout

Sustained failed-login attempts trigger a per-username lockout: 5
failures within 15 minutes locks the user for 15 minutes. Counter
lives on the `users` row (`failed_login_count`,
`locked_until`) and is reset on successful login.

This is depth-in-defense, not the only line. The proxy-layer rate
limit (Cloudflare, Tailscale, fail2ban) is the real defense for an
internet-facing deployment. The lockout makes a credential-stuffing
script noisy in the logs and forces the attacker to slow down.

### 9. Audit log

Every credential event is recorded in `auth_events(id, ts, user_id,
event_type, ip, ua, success, detail)`. Event types: `login_success`,
`login_failure`, `logout`, `password_change`, `lockout`,
`bootstrap_user_created`. `user_id` is nullable for failed-unknown-user
attempts (we don't echo the attempted username into the table; see
ADR-0025).

Append-only. Pruning is operator-initiated; defaults to no pruning.

### 10. Security headers on every response

- `X-Content-Type-Options: nosniff`
- `Referrer-Policy: strict-origin-when-cross-origin`
- `X-Frame-Options: DENY` on `/login`, `/account/*` (clickjacking
  defense; the rest of the app is allowed to run in iframes for
  legitimate embed use cases).
- `Strict-Transport-Security: max-age=31536000` when HTTPS detected.
- Content-Security-Policy on `/login` and `/account/password`
  (no inline scripts, no third-party origins, default-src 'self').

### 11. Account scoping shape

The auth middleware injects two values onto `request.state`:

- `request.state.user: User(id, username, account_id, role)`:
  authenticated user.
- `request.state.account: Account(id, name)`: the account/account the
  user belongs to.

In single-account mode, `account_id == 1` everywhere. The shape is
present so that:

- Routes that take `current_user: User = Depends(...)` continue to
  work in future multi-user mode without source changes.
- Account-scoped queries can be migrated to take a `Account` parameter
  one route at a time, without a flag-day.

Today's enforcement: dependency exists, all reads are unscoped (only
one account). future multi-user mode-day cutover: query helper takes `Account`, every
read filters on `account_id`. The cutover is a separate, deliberate
work-stream, not implicit in this ADR.

### 12. Bypass list

Routes exempt from the auth gate:

- `GET /healthz`
- `GET /login`
- `POST /login`
- `GET /static/*`

Everything else requires an authenticated session when auth is
configured. `GET /favicon.ico` is served from `/static`.

### 13. HTMX-aware unauth response

When auth is required and the cookie is absent or invalid:

- HTML request → 302 to `/login?next=<original-path>`.
- HTMX request → 200 with `HX-Redirect: /login?next=<original-path>`
  (browser follows it; HTMX semantics).
- JSON / API request → 401 with `{"detail": "authentication required"}`.

### 14. Stub tables for future multi-user mode-day features

Empty / single-row tables exist now so future multi-user mode-day work is incremental:

- `accounts(id, name, created_at)`: single row `(1, 'local', ...)`.
- `password_reset_tokens(token_hash, user_id, expires_at, used_at)`:
  empty until `/forgot-password` ships.
- `mfa_secrets(user_id, secret_encrypted, enabled, last_used_at)`:
  empty until TOTP UI ships.

The schema exists. The helpers exist. The UI surfaces don't. future multi-user mode-day
work is wiring UI to existing backends, not creating both.

## Consequences

- **A new dependency footprint.** `argon2-cffi`, `itsdangerous`. Both
  are mature, widely deployed, and small.
- **The dev-server bind changes.** Existing dev workflows that hit the
  app from a LAN device must set `HOST=0.0.0.0` explicitly. Docker is
  unaffected. The change is justified because every "I accidentally
  exposed my server" failure is unrecoverable.
- **CSRF tokens become a template / HTMX cross-cut.** All forms that
  POST need a `{{ csrf_token }}` hidden input or a CSRF-aware HTMX
  attribute. Documented in `docs/specs/AUTH.md`. Tests assert 403 on
  tokenless POSTs.
- **The `users` / `auth_sessions` / `auth_events` tables exist on every
  install.** They are tiny. Recovery from a deleted DB still works
  via re-bootstrap from env vars. Per ADR-0001, the ledger is the
  source of truth; auth state is a disposable cache, by design.
- **Account scoping is deferred work.** The shape is in place; the
  enforcement is a future multi-user mode-day project. This ADR does NOT promise
  multi-user isolation today.
- **No `/forgot-password`, no MFA UI today.** Documented as known gaps.
  Single-account: stop the server, set `AUTH_PASSWORD` env, restart.
- **No proxy-layer rate limit shipped.** Per-user lockout is in;
  IP-based throttling is left to the deployment.

## Alternatives considered

1. **No auth, document a reverse-proxy recipe.** Fava-style. Rejected
   for the asymmetric-failure-mode reason: the cost of misconfiguration
   is total exposure of financial data.

2. **HTTP Basic auth.** One line of middleware. Rejected: bad UX
   (browser-popup, no real logout, mobile-unfriendly), no extension
   point for future multi-user mode (no session, no per-request user context, no
   audit shape).

3. **Signed-cookie sessions only (no DB).** Smaller footprint.
   Rejected: server-side revocation is impossible. Logout becomes a
   client-side cookie clear that any cached browser tab can replay.
   Password change cannot kill other sessions. The savings (one
   table) do not justify the lost defenses.

4. **Bcrypt instead of argon2id.** Acceptable. Rejected because the
   migration from bcrypt → argon2id later forces a password reset
   per user, and there is no reason to inherit that debt for a
   single-line dependency change today.

5. **Roll our own session signing.** Rejected. `itsdangerous` is
   maintained, well-audited, and the right size (small).

6. **Scrypt passwords.** Acceptable. Argon2id won the OWASP / NIST
   shootout for new deployments; we follow the recommendation rather
   than hand-tuning scrypt cost params.

7. **Multi-factor auth on day one.** Rejected. TOTP UI is several
   hundred lines of code that single-user, self-hosted deployments
   gain little from while the proxy layer remains the actual
   internet-facing surface. The schema slot exists; future multi-user mode-day adds it.

8. **Per-IP rate limiting.** Rejected for v1. In-memory state is
   meaningless against a determined attacker; persistent state is
   work that produces near-zero defense beyond what a real proxy
   already provides. Fail2ban-style tooling reads the auth log and
   does this better.

## Audit checklist

For every new state-changing route:

1. Does it depend on `current_user`?
2. Does it require a CSRF token?
3. If it reads account-scoped data, does it depend on `current_account`?
4. Does the test suite assert 401 / 302 when unauthenticated?
5. Does the test suite assert 403 when CSRF token is missing?

For every new credential-touching code path:

1. Does it use `passwords.verify(...)` (constant-time)?
2. Does it record an `auth_event` for both success and failure?
3. Does it surface generic error messages (no username-enumeration
   oracle)?
