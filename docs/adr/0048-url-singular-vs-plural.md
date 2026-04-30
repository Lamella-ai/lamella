# ADR-0048: URL singular vs plural conventions

- **Status:** Accepted
- **Date:** 2026-04-27
- **Related:** [ADR-0047](0047-settings-is-a-dashboard.md)

## Context

URL naming has drifted as routes accumulated. Most resource-collection
paths are plural (`/accounts`, `/vehicles`, `/businesses`, `/receipts`,
`/budgets`, `/reports`, `/loans` post-promotion), but a few inconsistent
exceptions exist (`/note`, `/audit`, `/recurring`, `/inbox`,
`/intake`, `/import`, `/dashboard`). New routes have no rule to follow
and the inconsistency is going to compound when the project goes public.

## Decision

URL paths follow these conventions:

### Plural for resource collections

A path that lists, creates, or filters multiple instances of the same
shape uses the plural noun.

- `/accounts`, `/accounts/{path}`, `/accounts/{path}/edit`
- `/vehicles`, `/vehicles/{slug}`, `/vehicles/{slug}/dispose`
- `/businesses`, `/businesses/{slug}`
- `/entities`, `/entities/{slug}` (post-promotion)
- `/loans`, `/loans/{slug}` (post-promotion)
- `/properties`, `/properties/{slug}` (post-promotion)
- `/receipts`, `/receipts/{id}/link`
- `/notes`, `/notes/{id}` (post-rename)
- `/budgets`, `/reports`, `/projects`, `/rules`
- `/ai/logs`, `/ai/suggestions`
- `/txn/{token}` is an outlier: `/txn` is short for transaction and the
  detail URL is the heaviest-traffic page on the site. Keeping it short
  is intentional; the listing page is `/transactions`.

### Singular for utilities, dashboards, and single-instance concepts

A path that represents one thing, a process, a dashboard, a single
identity, a sweep, uses singular.

- `/dashboard`, `/inbox`, `/calendar`, `/audit`
- `/setup`, `/setup/recovery`, `/setup/welcome`
- `/import` (one import workflow), `/intake` (one intake workflow)
- `/recurring` (one recurring scan), `/search` (one search box)
- `/status`, `/notifications`
- `/ai/cost`, `/ai/decisions/{id}` (specific decision detail)

### Carve-out: verb-shaped destinations

Some paths are deliberately singular because the user is performing a **verb**
on a single conceptual destination, not browsing a collection of items.
`/inbox`, `/audit`, and `/card` are examples: the user is "doing inbox
processing", "doing an audit", or "viewing the card view". These are
activities, not lists of resources. The plural would be semantically wrong
(`/inboxes`, `/audits`, `/cards` would imply a list of multiple inboxes,
audit runs, or card-view configurations).

The rule of thumb: if the path feels like a verb or a named workspace, keep
it singular. If the path lists multiple instances of the same entity shape,
use plural. Plural URLs are reserved for noun collections (`/transactions`,
`/loans`, `/properties`, `/vehicles`, `/notes`).

### Sub-paths inherit the rule

`/accounts/{path}/edit` is fine; the parent is plural, the leaf is a
verb. `/businesses/{slug}/transactions` is fine; both are plural
collections. `/vehicles/{slug}/dispose` is fine; `dispose` is a verb.

### Settings sub-pages

`/settings/<area>` follows the rule based on what `<area>` means.

- `/settings/accounts`: plural (account collection editor).
- `/settings/entities`: pre-promotion path; redirects to `/entities`.
- `/settings/loans`: pre-promotion path; redirects to `/loans`.
- `/settings/data-integrity`: singular (one process).
- `/settings/backups`: plural (collection of backups).
- `/settings/rewrite`: singular (one config screen).

## Consequences

- **Predictable URLs**: when somebody adds a new resource, plural is the
  default and reviewers can flag deviations.
- **One renaming pass**: legacy singular collections that should be
  plural get renamed and 301-aliased. Today that's `/note` → `/notes`.
  No mass churn after this ADR.
- **Detail routes don't add prefixes**: keep `/<plural>/{id}` rather
  than `/{singular}/{id}`. `/notes/{id}` not `/note/{id}`.

## Migration

Sweep-and-redirect for existing inconsistencies:

| Old | New | Type |
|---|---|---|
| `/note` | `/notes` | 301 |
| `/note/{id}` | `/notes/{id}` | 301 (when implemented) |
| `/ai/audit` | `/ai/logs` | already done in commit 602f89f |

Future renames follow the same pattern: rename + 301 alias + sweep
inbound links. Never delete the old route until soak.

## Alternatives considered

1. **All plural, all the time.** Rejected: `/dashboard` becoming
   `/dashboards` reads worse than the singular for a one-off page.
2. **All singular, REST-style with method semantics.** Rejected: this
   is not a JSON API; the templates' GET-vs-POST shape doesn't map to
   verb endpoints well.
3. **Leave drift in place.** Rejected: a 5-minute ADR now beats
   tribal-knowledge URL design forever.
