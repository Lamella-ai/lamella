# ADR-0064: Paperless Namespace Uses Colon Separator (`Lamella:X`)

- **Status:** Accepted (2026-05-02)
- **Date:** 2026-05-02
- **Author:** AJ Quick
- **Related:** [ADR-0003](0003-lamella-metadata-namespace.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0061](0061-documents-abstraction-and-ledger-v4.md), [ADR-0062](0062-tag-driven-workflow-engine.md)
- **Supersedes:** ADR-0044's separator choice (the `Lamella` namespace itself stays — only the separator flips from `_` to `:`)

## Context

ADR-0044 reserved a single grep-able namespace (`Lamella_*`,
PascalCase, underscore-separated) for Lamella's Paperless custom
fields. ADR-0061 §8 extended that reservation to Paperless tags.
ADR-0062 §1 then defined the five canonical workflow state tags
under that umbrella:

- `Lamella_AwaitingExtraction`
- `Lamella_Extracted`
- `Lamella_NeedsReview`
- `Lamella_DateAnomaly`
- `Lamella_Linked`

The choice of `_` as the separator was an unforced error. The
Paperless community convention — the form most third-party scripts,
docker-compose templates, and tutorial repos use — is colon-separated
(`Lamella:Vendor`, `Lamella:AwaitingExtraction`). Colons render
cleaner in the Paperless UI (which already groups colon-separated
tags as a visual hierarchy in some themes), are clearer when read
aloud, and don't collide with the underscore convention many people
use for "this is a single multi-word identifier" inside a single
namespace level.

The cost of staying on underscores is permanent friction with every
operator who already runs Paperless: documentation that doesn't match
their other automation, and tag/field names that look out of place
next to community-shipped tooling. The cost of flipping is one
in-place rename pass against existing Paperless instances and a
backwards-compatible read shim during the transition.

This is not the same kind of change as the `bcg-*` → `lamella-*`
metadata rename. **Beancount metadata (`lamella-*` kebab-case per
ADR-0003) is not affected.** That namespace is colon-illegal at the
Beancount syntax level and is the user-facing source of truth — its
shape is correct as-is. Only the Paperless-side identifiers (custom
fields and tags) move.

## Decision

All Lamella-managed Paperless tag names and custom field names use
the `Lamella:X` form (colon-separated, PascalCase suffix). The
namespace itself — the leading `Lamella` token reserved by ADR-0044
— is unchanged; only the separator flips.

### 1. Concrete name changes

| Kind | Legacy (`Lamella_X`) | Canonical (`Lamella:X`) |
|---|---|---|
| Custom field (ADR-0044) | `Lamella_Entity` | `Lamella:Entity` |
| Custom field (ADR-0044) | `Lamella_Category` | `Lamella:Category` |
| Custom field (ADR-0044) | `Lamella_TXN` | `Lamella:TXN` |
| Custom field (ADR-0044) | `Lamella_Account` | `Lamella:Account` |
| Tag (ADR-0062) | `Lamella_AwaitingExtraction` | `Lamella:AwaitingExtraction` |
| Tag (ADR-0062) | `Lamella_Extracted` | `Lamella:Extracted` |
| Tag (ADR-0062) | `Lamella_NeedsReview` | `Lamella:NeedsReview` |
| Tag (ADR-0062) | `Lamella_DateAnomaly` | `Lamella:DateAnomaly` |
| Tag (ADR-0062) | `Lamella_Linked` | `Lamella:Linked` |

### 2. Read/write asymmetry

**Writes always use the canonical (colon) form.** Every
`ensure_tag` / `ensure_lamella_writeback_fields` /
`writeback_lamella_fields` call against Paperless writes the
colon name only. A non-Lamella name (anything not starting with
`Lamella:` or `Lamella_`) still raises
`InvalidWritebackFieldError` before any HTTP call — the namespace
defense from ADR-0044 is preserved verbatim, just widened to accept
either separator on the input side.

**Reads accept both forms.** A new helper module
`paperless_bridge/lamella_namespace.py` exposes a small surface:

```python
LAMELLA_NAMESPACE_PREFIX_NEW = "Lamella:"
LAMELLA_NAMESPACE_PREFIX_LEGACY = "Lamella_"

def canonical_name(suffix: str) -> str:
    """Return the canonical Lamella:<suffix> name."""

def legacy_name(suffix: str) -> str:
    """Return the legacy Lamella_<suffix> name (backwards-compat reads)."""

def is_lamella_name(name: str) -> bool:
    """True if name carries either Lamella separator."""

def to_canonical(name: str) -> str:
    """Rewrite a Lamella_X name to Lamella:X. Idempotent on already-canonical names."""

def name_variants(suffix: str) -> tuple[str, str]:
    """Return (canonical, legacy) name pair for backwards-compat lookups."""
```

Helpers that look up tags or custom fields by name try the canonical
form first, then fall back to the legacy form. A user with a partial
or paused install — Paperless `Lamella_Vendor` field present, no
colon equivalent yet — keeps working without intervention.

### 3. Startup migration

A new module `paperless_bridge/namespace_migration.py` runs once at
startup (gated by the `paperless_namespace_migration_completed`
setting) and renames every `Lamella_X` tag/field in Paperless to
`Lamella:X`:

1. **Tags.** For each tag whose name matches `Lamella_X`:
   - If a `Lamella:X` tag already exists: for every document tagged
     `Lamella_X`, also tag `Lamella:X` (idempotent), then untag
     `Lamella_X`. Then delete the `Lamella_X` tag.
   - Otherwise: PATCH the tag's name from `Lamella_X` to `Lamella:X`
     in place. On 4xx fall through to the copy + remove path.

2. **Custom fields.** Same logic. In-place name PATCH first;
   copy-and-remove fallback if Paperless rejects the rename
   (Paperless's API support for renaming a custom field varies
   across versions).

The migration is idempotent — once all `Lamella_X` tags/fields are
gone, the function is a no-op. After a successful run (no errors),
the `paperless_namespace_migration_completed` setting flips to
`True` and subsequent boots skip the work entirely.

### 4. What this ADR does NOT change

- **Beancount metadata** (`lamella-*` kebab per ADR-0003). Colons
  are not valid in Beancount metadata keys; the kebab form is also
  correct as the ledger-side convention. No changes there.
- **The `Lamella` namespace itself.** The reserved leading token
  doesn't change. ADR-0044's grep-ability claim still holds — search
  Paperless for `Lamella` and find both fields and tags.
- **The four ADR-0044 writeback fields' purpose or content.** Only
  the names flip; entity slug, category, txn-id, payment-account
  display — same payloads, same write triggers, same confidence gates.
- **The five ADR-0062 canonical workflow tags' semantics.** Only
  the names flip; the state machine is identical.

## Consequences

### Positive

- **Convention alignment.** Lamella's Paperless surface looks like
  every other community-shipped Paperless integration. Operators
  who copy-paste from Paperless tutorials don't hit a stylistic
  discontinuity.
- **Visual hierarchy in Paperless.** Some Paperless themes group
  colon-separated tags into a tree view; underscore tags are a flat
  list. Users get a free `Lamella:` group label.
- **Backwards-compatible reads** mean no flag day. A user who
  upgrades while their Paperless is unreachable, or who applies the
  upgrade before the migration's first tick has finished, continues
  to see correctly-tagged documents.
- **One-shot migration.** A user with thousands of legacy-tagged
  documents takes one boot to migrate; subsequent boots cost zero
  Paperless API calls for the migration.

### Negative / Costs

- **Existing on-disk Paperless state must be migrated.** A user who
  has already pulled in legacy `Lamella_*` tags and field values
  needs the migration pass to run end-to-end before old names
  disappear. The migration is best-effort but on a failure (Paperless
  outage, permission error) the next boot retries.
- **Code paths must touch every `Lamella_X` literal.** A grep-and-
  replace across ~15 files (mostly Paperless-bridge module + a few
  HTML templates with copy describing the names).
- **Documentation churn.** ADR-0044 and ADR-0062 get banner notes;
  `docs/features/paperless-bridge.md` mentions the new shape.

### Mitigations

- The migration is wrapped in try/except inside `main.py` lifespan
  so a Paperless outage at boot never breaks startup.
- The settings flag means a successful migration is a one-time
  event; restarts during normal operation cost nothing.
- The backwards-compat read shim means a half-migrated state is
  still functional — a doc tagged with the legacy name still gets
  filtered by selectors looking for the canonical name (or vice
  versa).
- `lamella_namespace.is_lamella_name(...)` keeps the namespace
  defense ADR-0044 establishes — non-namespaced fields are still
  user-owned, the matcher still refuses to write them.

## Compliance

- Grep `src/lamella/` for `"Lamella_` (with the underscore). After
  this ADR lands, the only occurrences must be in
  `lamella_namespace.py` (the `LAMELLA_NAMESPACE_PREFIX_LEGACY`
  constant and the legacy tag/field name list used by the migration)
  or in `namespace_migration.py` (the rewrite implementation).
  Test files that specifically exercise the backwards-compat read
  path may also reference legacy names. New writes MUST use
  `"Lamella:"`.
- The Paperless writer asserts that every writeback field name
  carries either the canonical or the legacy `Lamella` prefix. Any
  name without a `Lamella` namespace prefix raises
  `InvalidWritebackFieldError` before any HTTP call.
- The bootstrap path (`bootstrap_canonical_tags`) writes only
  canonical names. The migration path is the only thing that PATCHes
  legacy names to canonical names.

## References

- [ADR-0044](0044-paperless-lamella-custom-fields.md): the original
  custom-field namespace reservation. This ADR supersedes the
  separator choice; the namespace itself is preserved.
- [ADR-0062](0062-tag-driven-workflow-engine.md): the five canonical
  workflow tags this ADR renames.
- [ADR-0061](0061-documents-abstraction-and-ledger-v4.md) §8: the
  reservation extension to tags. The colon separator applies equally
  to fields and tags.
- [ADR-0003](0003-lamella-metadata-namespace.md): Beancount metadata
  namespace. **Not affected** — Beancount keys remain `lamella-*`
  kebab-case.
- [ADR-0027](0027-http-tenacity-timeout.md): the migration's HTTP
  calls go through the same tenacity wrapper as every other Paperless
  call.
