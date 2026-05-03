---
audience: agents implementing user-facing copy
read-cost-target: 150 lines
authority: implementation cookbook (informative)
cross-refs: docs/adr/0041-display-names-everywhere.md
---

# UI Language Cookbook

Implementation reference for ADR-0041 (display names everywhere). The ADR
sets the rule; this document shows what the renderer functions look like
and what the translation table is.

## Renderer functions

TARGET: `src/lamella/display.py` (after Phase 8 src/ refactor:
`src/lamella/core/display.py`). The functions below specify the intended
signatures and behavior. Initial implementation: add `display_account` and
`humanize_slug` first; migrate templates one feature at a time.

```python
def display_account(
    account_path: str,
    *,
    in_entity_context: str | None = None,
) -> str:
    """Convert a Beancount account path to its user-facing display name.

    Lookup order:
      1. `accounts_meta.display_name` for the exact path.
      2. `humanize_slug()` on the last two segments joined by ": ".
      3. Falls back gracefully — never raises.

    When `in_entity_context` matches the entity segment of the path,
    the entity prefix is omitted from the returned string.

    Examples (in_entity_context=None):
      "Liabilities:Acme:BankOne:Card"  → "Acme: Bank One Card"
      "Assets:Personal:Chase:Checking"    → "Personal: Chase Checking"
      "Expenses:Acme:Office:Supplies"     → "Acme: Office Supplies"

    Examples (in_entity_context="Acme"):
      "Liabilities:Acme:BankOne:Card"  → "Bank One Card"
      "Expenses:Acme:Office:Supplies"     → "Office Supplies"
    """

def display_entity(entity_slug: str) -> str:
    """Look up `entities.display_name`; fall back to `humanize_slug(entity_slug)`."""

def display_vehicle(vehicle_slug: str, entity_slug: str) -> str:
    """Look up vehicle display name scoped to entity; fall back to humanize_slug."""

def display_property(property_slug: str, entity_slug: str) -> str:
    """Look up property display name scoped to entity; fall back to humanize_slug."""

def display_loan(loan_slug: str, entity_slug: str) -> str:
    """Look up loan display name scoped to entity; fall back to humanize_slug."""

def display_project(project_slug: str, entity_slug: str) -> str:
    """Look up project display name scoped to entity; fall back to humanize_slug."""

def humanize_slug(slug: str) -> str:
    """Fallback for missing display names.

    Splits CamelCase words, replaces dashes and underscores with spaces,
    strips leading version prefixes (e.g. "V2008").

    Examples:
      "BankOneChecking"  → "Bank One Checking"
      "north-rental"        → "North Rental"
      "V2008WorkSUV"        → "V2008 Work SUV"
      "office_supplies"     → "Office Supplies"
    """

def narrate_transaction(txn) -> str:
    """Render a Beancount transaction as a natural-language sentence.

    Uses display names throughout. Picks the verb based on posting structure:

    Transfer (2 postings, both Asset or Liability roots, same entity):
      "Transferred $184.22 from Chase Checking to Bank One Card"

    Expense (one Asset/Liability negative + one Expenses positive):
      "Spent $42.00 at Example LLC for Office Supplies,
       charged to Bank One Card"

    Income (one Asset/Liability positive + one Income negative):
      "Received $1,200.00 from Example LLC,
       deposited to Chase Checking"

    Multi-leg / other:
      "Posted $… — {payee or narration}"   (safe fallback)

    TARGET — implement after the simpler renderers land.
    """
```

## Translation table

User-facing translations of Beancount account roots (per ADR-0041). These
govern the narrative verbs and category nouns used in UI copy, notification
messages, and `narrate_transaction()`.

| Beancount root | Default narrative term | Context-specific refinement |
|---|---|---|
| `Assets:` | "money you have" | Segment 3 → "Checking", "Savings", "Brokerage", "Cash" |
| `Liabilities:` | "money you owe" | Segment 3 → "Credit Card", "Loan", "Mortgage" |
| `Equity:` | "owner contributions" | Segment 3 → "Capital", "Draws", "Retained Earnings", "Mileage Deduction" |
| `Income:` | "earnings" | Segment 3 → the income type (e.g. "Consulting", "Interest") |
| `Expenses:` | "spending" | Segments 3+ → the category display name |

Segment numbering: `Root:Entity:Type:Subcategory` (1-indexed). "Segment 3"
is the `Type` slot in the entity-first hierarchy.

## Display name fallback rules

Applied in order. The first match wins:

1. **DB lookup**: query the appropriate display-name table
   (`accounts_meta`, `entities`, `vehicles`, `properties`, `loans`,
   `projects`) for the exact slug or path.
2. **Slug humanization**: call `humanize_slug()` on the slug or the
   last 1 to 2 segments of the account path.
3. **Entity prefix stripping**: when `in_entity_context` matches the
   account's entity segment, strip it from the display string.
4. **NEVER** show the raw slug, raw account path, or a SQLite PK to a
   user. If all lookups fail, `humanize_slug` must produce something
   readable; it never returns empty.

## When to use renderer functions

| Surface | Use renderer? | Reason |
|---|---|---|
| HTML template variables | YES | Every user-visible label |
| Notification messages (ntfy, Pushover) | YES | Users read these |
| Job event log messages | YES | Visible in the job modal |
| Review-queue AI proposal display | YES | User picks from these |
| Error messages bubbling to the UI | YES | User reads them |
| Server-side log messages | NO | Logs use raw paths (ADR-0025) |
| AI prompt context | NO | AI needs structural account paths |
| `ai_decisions` audit row content | NO | Audit captures raw structural data |
| Settings / configuration screens | NO | User is configuring the structural identifier |
| Power-user "Show raw" toggle view | NO | Toggle intentionally reveals raw paths |
| Account autocomplete picker label | YES | Visible label shown to user |
| Account autocomplete picker submitted value | NO | Must submit the canonical account path |

## Jinja integration

Register `display_account`, `display_entity`, and `humanize_slug` as
Jinja globals so templates call them without imports:

```python
# In app startup (e.g. lamella/app.py or lamella/web/templates.py):
from lamella.display import display_account, display_entity, humanize_slug

templates.env.globals.update(
    display_account=display_account,
    display_entity=display_entity,
    humanize_slug=humanize_slug,
)
```

Template usage:

```jinja
{# entity context collapses the entity prefix #}
<td>{{ display_account(row.account, in_entity_context=entity_slug) }}</td>

{# top-level table — show the full "Entity: Type" label #}
<td>{{ display_account(row.account) }}</td>

{# entity chip label #}
{{ D.chip(display_entity(row.entity_slug), color=row.entity_color,
          letter=row.entity_slug[0]|upper) }}
```

## Implementation phasing

| Step | Deliverable | Status |
|---|---|---|
| 1 | `humanize_slug()` in `display.py` | TARGET |
| 2 | `display_account()` + `display_entity()` in `display.py` | TARGET |
| 3 | Register as Jinja globals | TARGET |
| 4 | Migrate review-queue, card, and staging surfaces | TARGET |
| 5 | Migrate notifications and job event log | TARGET |
| 6 | `display_vehicle`, `display_property`, `display_loan`, `display_project` | TARGET |
| 7 | `narrate_transaction()` | TARGET (implement last; most complex) |

Migrate one surface at a time. Each migration is a self-contained PR that
only touches one feature area and its tests. Do not attempt a cross-cutting
rename of all templates in one pass; it's untestable at that scale.

## Anti-patterns (forbidden)

| Anti-pattern | Why | Correct alternative |
|---|---|---|
| `{{ row.account }}` in a user-visible cell | Shows raw account path | `{{ display_account(row.account) }}` |
| `{{ entity.slug }}` as a user-facing label | Shows slug, not name | `{{ display_entity(entity.slug) }}` |
| Constructing display names in route handlers | Business logic in the wrong layer | Call renderer from template or from a view-model helper |
| Passing display names into AI prompts | AI needs structural paths for routing | Pass raw account paths; display names are UI-only |
| Hardcoding account path segments as display strings | Breaks when ledger evolves | Derive from `display_account()` |
