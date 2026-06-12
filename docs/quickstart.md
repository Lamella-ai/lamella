# Quick start

The five-minute path from "I cloned the repo" or "I read about this on
GitHub" to "I'm logged in and the app is running on my machine."

## What you'll have when you're done

- A Docker container running on `localhost:8080` (or wherever you
  bind it).
- A signed-in account, password hashed with Argon2id, session in a
  signed cookie.
- An empty Beancount ledger at `./ledger/` and a SQLite database at
  `./data/lamella.sqlite3`.
- An app that knows what it doesn't know yet. Every integration is
  off by default until you give it credentials.

## Prerequisites

- **Docker + Docker Compose**. Anything from the last two years works.
  `docker compose version` should print `v2.x` or newer.
- **About 2 GB of disk** for the image. The base image carries
  PyTorch + sentence-transformers, which is heavy but local AI
  classification needs them.
- **A directory to mount as `./ledger/`**. Either an empty directory
  (the in-app setup wizard writes a starter ledger) or one with an
  existing `main.bean`.

## Run it

```bash
mkdir -p ~/lamella/{data,ledger}
cd ~/lamella
curl -fsSL https://raw.githubusercontent.com/lamella-ai/lamella/main/docker-compose.yml -o docker-compose.yml
curl -fsSL https://raw.githubusercontent.com/lamella-ai/lamella/main/.env.example -o .env
```

Edit `.env`:

```bash
AUTH_USERNAME=you
AUTH_PASSWORD=pick-something-strong
```

Then:

```bash
docker compose up -d
docker compose logs -f lamella   # watch boot
```

Open `http://localhost:8080`. You should land on `/login`. Sign in
with the credentials you just set. The plaintext password gets hashed
with Argon2id on first read and the env var is then ignored. To
change it later, use `/account/password` inside the app.

## What to do next, in order

These are the steps that turn a cold install into a working
classifier. None of them are gates (you can skip ahead and come
back), but skipping them means worse classifications until you do
them.

1. **`/setup/check`**. Fix every red row. The check validates
   entities, accounts, vehicles, properties, loans, Paperless field
   mappings, and classification rules against the live ledger. You
   want this green before you start classifying transactions.

2. **`/status`**. Every card on this page maps to a system that
   either is, or isn't, configured. Yellow cards mean "intentionally
   off"; red cards mean "configured but failing." Aim for all green
   or yellow.

3. **`/settings/entities`**. Write an entity description for each
   active business or self. The 🪄 Generate draft button proposes one
   from your transaction history if any exists. The description gets
   rendered into every classify prompt for that entity, so vague
   descriptions produce vague classifications.

4. **`/settings/account-descriptions`**. Same idea for accounts. The
   ⛏ Mine button proposes sub-categories for sprawling catchall
   accounts; useful when "Expenses:Misc" has 400 transactions in it.

5. **`/audit`**. Pick a small (10-20) random sample first. Each
   Accept is a user-correction that the vector index will weight 2×
   over the original ledger entry. The classifier learns from
   disagreements.

## When something doesn't work

- See [docs/troubleshooting.md](troubleshooting.md) for known issues
  and fixes.
- See [docs/configuration.md](configuration.md) for every env var
  and what it controls.
- See [docs/features/](features/) for per-feature current-state docs
  derived from the live source tree.

## When you're ready to go past localhost

The defaults bind to `127.0.0.1`. Loopback is the only zero-config
posture that's safe without auth. Any of these is fine:

- **Cloudflare Tunnel** in front, with **Cloudflare Access** doing
  the auth. The bundled in-app auth becomes depth-in-defense.
- **Tailscale** with ACLs scoped to your devices.
- **Reverse proxy** on a private LAN with the in-app auth on.

For internet-facing deployments, the in-app auth is **not** a
substitute for an authenticating proxy. Always have one upstream.

See [ADR-0050](adr/0050-optional-authentication.md) for the full
threat model.
