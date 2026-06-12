# Security Policy

Lamella is financial software. It sits between your bank-feed sources
and your authoritative Beancount ledger. Security issues affect real
money, real receipts, and the integrity of the books people will hand
to a tax preparer. We take them seriously.

## Reporting a vulnerability

**Do not open a public GitHub issue for security reports.** Public
disclosure before a fix is available puts every Lamella operator at
risk while the fix is being prepared.

Email: **security@lamella.ai**

Include:

- A description of the issue and the impact you observed.
- Steps to reproduce, minimum working example if you have one.
- The version / commit SHA you tested against (run
  `git rev-parse HEAD` in your clone, or check the footer of the
  Lamella web UI).
- Whether you have already disclosed the issue elsewhere.

You will receive an acknowledgement within 72 hours. We will work
with you on a coordinated disclosure timeline; the default is up to
90 days from initial report to public disclosure, shorter if the
issue is actively being exploited.

## Scope

In scope:

- The Lamella application code in this repository (`src/lamella/`).
- The published Docker images at
  `ghcr.io/<owner>/lamella:*` and `…/lamella-base:*`.
- The `docker-compose.yml` and `docker-compose.unraid.yml` deploy
  templates.
- Dependencies pinned in `pyproject.toml` when the vulnerability is
  exploitable through Lamella's use of them (versus a generic
  upstream advisory).

Out of scope:

- Self-hosted deployments that have removed the documented hardening
  (read-only rootfs, `cap_drop: ALL`, `no-new-privileges`,
  loopback-only port binding behind a tunnel/access-proxy). If you
  found an issue that requires removing those, the report is welcome
  but it is not a Lamella vulnerability; your deploy posture is.
- Third-party services Lamella integrates with (SimpleFIN, Paperless,
  OpenRouter, etc.). Report those upstream.

## What counts as a vulnerability

- Authentication / authorization bypass (Lamella's
  single-user-trust-the-tunnel model is documented; bypassing the
  tunnel-or-equivalent assumption is a deploy-template issue, not a
  Lamella issue).
- Remote code execution, command injection, SQL injection, path
  traversal in any code path reachable via HTTP, the file watcher,
  or background jobs.
- Cross-entity data leaks (a query intended to return data for
  Entity A returning Entity B's data instead).
- Logged sensitive data. Payee names, transaction amounts, account
  paths leaking into logs is treated as a vulnerability per
  [ADR-0025](docs/adr/0025-logs-identify-never-expose-values.md).
- Secrets disclosure. API keys, OpenRouter tokens, SimpleFIN URLs,
  Paperless tokens leaking into logs, errors, or UI.

## Hardening defaults

The published `docker-compose.yml` ships with:

- `read_only: true` on the rootfs (writes go to the volume mounts
  + small explicit tmpfs entries for `/tmp`, `/run`, `/var/run`,
  `/var/cache`).
- `cap_drop: [ALL]`. Lamella never binds privileged ports or opens
  raw sockets.
- `security_opt: [no-new-privileges:true]`. Blocks setuid escalation
  paths inside the container.
- Ports bound to `127.0.0.1` only. Lamella expects to be reached
  through a tunnel + access-proxy (Cloudflare Tunnel + Access,
  Tailscale, etc.), not directly from the LAN.

Removing any of these is your call as an operator, but be aware
that the security posture assumes they are in place.
