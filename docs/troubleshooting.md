# Troubleshooting

The shortlist of things that commonly go wrong on a fresh install,
how to diagnose them, and how to fix them.

## "CSRF token invalid or missing" on every form

**Symptom**: signed in fine, but every action (link receipt, dismiss,
classify) returns `{"detail":"CSRF token invalid or missing"}`.

**Cause**: an unusually old browser tab was loaded *before* you signed
in, so it doesn't have the CSRF cookie that gets minted on
authenticated responses. Or, more rarely, a third-party browser
extension is stripping cookies.

**Fix**: hard-refresh the tab (Cmd/Ctrl-Shift-R). The auth middleware
mints a fresh CSRF token on every authenticated response and the
site-wide hook in `base.html` reads it from the cookie before each
HTMX request, so a fresh page load always has a valid token. If the
problem persists across hard-refreshes, check that the browser is
storing cookies for your origin (DevTools → Application → Cookies);
in particular the `lamella_csrf` cookie should be present.

## Container exits at boot, log shows `KeyError: 'getpwuid(): uid not found'`

**Symptom**: in self-hosted setups using `PUID` / `PGID` to map the
container user to a non-1000 uid, the app crashes on first vector
index access.

**Cause**: `sentence-transformers` and `huggingface_hub` call
`getpwuid()` to resolve `$HOME`, which fails when the runtime uid
isn't in `/etc/passwd` inside the container.

**Fix**: the image already sets `HOME`, `HF_HOME`, `TRANSFORMERS_CACHE`,
and `SENTENCE_TRANSFORMERS_HOME` explicitly to avoid the lookup. If
you're using a custom Dockerfile, replicate those env vars (see the
top-level `Dockerfile` for the canonical values).

## Signed in but immediately bounced back to `/login`

**Symptom**: login works, redirect lands on `/`, then the next click
sends you back to `/login`.

**Cause**: usually the `lamella_session` cookie isn't sticking. Two
common reasons:

1. **You're behind a TLS-terminating proxy that doesn't set
   `X-Forwarded-Proto: https`.** The auth middleware uses the
   `__Host-lamella_session` cookie name (with the `Secure` flag) when
   it sees HTTPS, and `lamella_session` (no Secure) otherwise. If the
   proxy drops the X-Forwarded-Proto header, the app sees HTTP, sets
   a non-Secure cookie, and the browser refuses to send a non-Secure
   cookie back over HTTPS. **Fix**: configure the proxy to forward
   `X-Forwarded-Proto`.
2. **System clock skew >5 min** between your client and the host.
   Cookie max-age timing gets weird. **Fix**: NTP.

## "Looks like the wrong-account half of an existing transfer" banner won't go away

**Symptom**: a staged transaction has a teal banner saying it's the
wrong-account half of a synthetic transfer, and clicking either button
doesn't dismiss it.

**Cause**: the marker in `staged_transactions.synthetic_match_meta` is
out of sync with the ledger state. Most often, the synthetic
counterpart got cleaned up by a different code path before the user
clicked.

**Fix**: click "No, classify normally". That clears the marker even if
the rewrite half is no longer applicable. The staged row then proceeds
through the regular classify flow.

## Paperless thumbnails 404

**Symptom**: receipts appear on `/receipts/needed`, but every
thumbnail is a broken image.

**Cause**: most often, `PAPERLESS_API_TOKEN` is wrong or revoked.
Sometimes the Paperless instance has rotated its index and the doc
ids in your local cache are stale.

**Fix**:

1. Check `/status`. If the Paperless card is red, the message tells
   you why (auth fail, network, etc.).
2. Visit `/settings`, scroll to Paperless, and click
   "Re-sync from scratch". This rebuilds the local doc index from a
   full crawl.

## "AI is over its monthly spend cap"

**Symptom**: classification falls back to rule-only mode; classify
buttons report "AI cap reached."

**Cause**: `AI_MAX_MONTHLY_SPEND_USD` is enforced; you've hit it.

**Fix**: bump the cap on `/settings` (the change is live; no restart
needed) or set it to `0` for unlimited. The cap exists so a bug doesn't
empty your OpenRouter balance overnight; tune it but don't disable
without monitoring.

## Tests pass locally but fail in CI / on a clone

**Symptom**: `pytest` is mostly red on a fresh clone.

**Cause**: known. The test suite has a baseline of pre-existing
failures from rapid-iteration coupling between fixtures and code. See
the production-readiness caveat in `README.md`.

**Practical contract**: run `pytest -k <suite>` for the area you're
touching. Full-suite green is on the post-launch backlog.

## "fava: command not found" in the container logs

**Symptom**: docker logs show fava failing to start during boot.

**Cause**: nothing serious. Fava is the secondary read-only ledger
viewer on port 5003; the primary app is uvicorn on 8080. Fava is
optional. Failures here don't affect Lamella's main UI.

**Fix**: ignore unless you specifically rely on fava. If you do, check
the fava log under `/tmp/fava.log` inside the container.

## Session secret got deleted, everyone is logged out

**Symptom**: deleted `${LAMELLA_DATA_DIR}/.session-secret` (or wiped
the data volume). Every existing user session is now invalid.

**Cause**: working as intended. The session signing key is the secret
that proves a session cookie wasn't tampered with; rotating it
invalidates everything.

**Fix**: sign in again. If you need a stable secret across restarts,
either restore the file from a backup, or set `AUTH_SESSION_SECRET`
explicitly via env var (the file-based path is just the default for
zero-config installs).

## `bean-check` is failing on a connector-owned file I didn't touch

**Symptom**: a write succeeds but `bean-check` reports new errors
afterwards, and the app rolls the write back.

**Cause**: this is the safety net working. ADR-0004 mandates
`bean-check` after every ledger write, with baseline subtraction so
pre-existing errors don't cause a revert; only *new* errors do.

**Fix**: read the actual error in the log. Common culprits:

- An account was used that isn't open. Open it with an `open`
  directive at an earlier date.
- A posting amount doesn't balance with the rest of the txn (the
  classifier wrote a one-sided entry from corrupt input).
- A custom directive used a key name your plugin allowlist doesn't
  recognize.

The roll-back is automatic; your ledger is unchanged. Fix the issue
and re-run the action that triggered the write.

## Where else to look

- `/status`: first line of triage. Each card maps to a subsystem
  and tells you in plain English why it's not green.
- `/audit`: see what the AI classified recently and how confidently.
  Disagreements with the rule book surface here.
- `/settings/backups`: recent backups and on-demand snapshot button.
- Container logs (`docker compose logs -f lamella`): verbose at
  `LOG_LEVEL=DEBUG` for tracing one specific thing.
