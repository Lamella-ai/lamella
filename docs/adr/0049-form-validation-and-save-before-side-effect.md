# ADR-0049: Form validation + save-before-side-effect

- **Status:** Accepted
- **Date:** 2026-04-27
- **Related:** [ADR-0011](0011-autocomplete-everywhere.md),
  [ADR-0047](0047-settings-is-a-dashboard.md)

## Context

A pair of related bugs surfaced during pre-release polish on the
`/entities` dashboard:

1. **Free-text fields where a fixed enum was expected.** The
   `entity_type` input was rendered as `<input type="text" list="…">`,
   which lets the browser autocomplete suggestions but accepts arbitrary
   typed values. The user could clear the suggested option and submit
   "whatever". The route accepted the value and the row stored garbage.
   Downstream code (the dashboard's bucket grouping) silently bucketed
   anything-not-recognized into "Other".

2. **Side-effect buttons firing on stale form state.** The legacy bulk
   editor had a "Scaffold accounts" button next to a `<select>` that
   chose Schedule C vs F. The user could change the schedule from C to F,
   then click Scaffold without saving. The scaffold ran on the *old*
   schedule because it didn't read from the unsaved form. Same shape:
   "Apply Repairs" on `/setup/recovery` ran on an empty draft state
   because the page didn't seed default-apply drafts on render.

The first is a validation gap. The second is a coherence gap: a
side-effect button assumed the row was saved, but the user only edited
the form. Both are reusable patterns we want to enforce going forward.

## Decision

### 1. Picker shape follows the cardinality of the value set

| Value set | UI element | Rationale |
|---|---|---|
| Fixed small enum (≤ ~20 known values) | `<select required>` with `<option>` per choice | The browser refuses any value not in the list. No client-side typo path. |
| Long ledger-derived list (accounts, entities-as-pickers, vehicles, projects) | `<input type="text" list="…">` with `<datalist>` | ADR-0011: typing-while-finding is faster than scrolling a 200-row select. |
| Free-form text | `<input type="text">` | Display names, addresses, notes. |

`<input list="…">` is **not a substitute for `<select>`**. A datalist
augments a free-text field with suggestions; it does not constrain the
value. Use `<select>` whenever the value set is fixed and small.

`entity_type`, `tax_schedule`, `property_type` (when extended to a
fixed list), `disposal_type`, `loan_type` etc. are all small fixed
enums and use `<select>`. Account paths, entity slugs as picker values,
vehicle slugs are ledger-derived and use `<datalist>`.

### 2. Server-side validation is mandatory for fixed-set fields

The `<select>` blocks the casual typo path. It does NOT block:

- A `curl` POST.
- A stale browser tab whose `<option>` list was rendered before a
  schema change.
- A future XSS payload.
- Anybody bypassing the form via the developer console.

Every route that accepts a fixed-enum field validates against the
canonical set and raises 400 when the value is unrecognized. The
canonical set lives in one place and is imported wherever it's needed:

```python
from lamella.core.registry.entity_structure import ENTITY_TYPES
valid = {t[0] for t in ENTITY_TYPES}
if entity_type and entity_type not in valid:
    raise HTTPException(
        status_code=400,
        detail=f"unknown entity_type {entity_type!r}; "
               f"expected one of {sorted(t for t in valid if t)}",
    )
```

Tests cover the 400-on-bad-value path. The test suite is the regression
tripwire for "did somebody add a new option to ENTITY_TYPES and forget
to expose it on the form."

### 3. Save before any side-effect button fires

A side-effect button (Scaffold, Generate, Apply, Promote, etc.) MUST NOT
read the form's unsaved state. Either:

- **(a) The side-effect button is its own form / its own POST.** It
  reads from the database / ledger, not from sibling form inputs. If it
  needs the latest values, those values came from a prior save.

- **(b) The side-effect runs *as part of* the save POST.** The form's
  "Save and scaffold" button submits to the canonical save handler with
  an extra `&also_scaffold=1` flag (or equivalent). The handler does
  the save first, then the side-effect, atomically.

Pattern (a) is preferred when the side-effect is rare or expensive.
Pattern (b) is preferred when "save and X" is the obvious user
intent for that button.

Concretely:
- `/entities/{slug}/scaffold` reads the persisted `tax_schedule`. The
  user must save first.
- `/setup/recovery`'s Apply Repairs button operates on the persisted
  `setup_repair_state.findings` rows. Render-time seeds default-apply
  drafts so an unedited "Apply Repairs" actually has drafts to apply.
- `/loans/wizard` is the explicit (b) pattern. The wizard's form data
  is captured into a session, then the final commit creates loan +
  schedule + opens together.

### 4. Don't show side-effect buttons next to unsaved form fields

When the same screen has an editable form AND a side-effect button,
visually separate them so it's obvious the side-effect doesn't read
the form's working copy. Section heading + explicit "Save first" copy
near the side-effect button. The wizard-style modal pattern (per
[ADR-0047](0047-settings-is-a-dashboard.md)) sidesteps this: the modal
is the editor; side-effects sit on the focused detail page in their
own section.

### 5. Sentinel input for the no-op save

A user who lands on a per-entity edit modal and clicks Save without
changing anything must still get a clean save (idempotent UPSERT, no
400). The form carries the slug as a hidden input; the rest of the
fields are pre-filled from the database row. Posting them back is a
no-op write that returns the same card partial. The modal closes, the
card swaps to itself, no harm.

## Consequences

- **`<select>` shows up where it didn't before.** That's correct for
  fixed enums. ADR-0011 still governs ledger-derived pickers.
- **Server-side validation is duplicate work in 99% of cases** (the
  `<select>` already filtered). It catches the remaining 1% that
  matters: API clients, stale tabs, security probes.
- **Side-effect buttons get less convenient.** The user has to save
  before scaffolding. Trade-off, no more "I clicked Scaffold and it
  used the wrong schedule" reports.
- **Existing routes that accepted free-text where a fixed set was
  expected need fixing.** Inventoried at the time of this ADR:
  - `entity_type` on `/settings/entities`: fixed in commit 098c0f2
  - `tax_schedule` on `/settings/entities`: fixed in commit 098c0f2
  - `disposal_type` on `/vehicles/{slug}/dispose`: already validates
  - `loan_type` on `/settings/loans`: already a `<select>`
  - `fuel_type` on vehicle save: currently silently coerces unknown
    values to NULL; acceptable (the field is optional and unrecognized
    is the same as not set), but flagged for audit.

## Audit checklist

For every route that accepts a fixed-enum form field:

1. Does the form use `<select>` (not `<input list="…">`)?
2. Is the canonical enum imported from one source of truth (e.g.,
   `lamella.core.registry.*` not a hand-typed list in the template)?
3. Does the route validate the value against the canonical set and
   raise 400 on unknown?
4. Is there a test that asserts (3)?

For every side-effect button (Scaffold, Apply, Generate, Promote):

1. Does the button live on its own form, posting to its own endpoint?
2. Does that endpoint read from the database / ledger, not the
   sibling form inputs?
3. If the button is "save and X", does the route do save → X
   atomically (rolling back save on X failure when reasonable)?
4. Is there a test that asserts the button works on un-edited rows
   (no implicit-empty-form trap)?

## Alternatives considered

1. **Trust the `<select>`, skip server validation.** Rejected; works
   in the browser flow, fails on every other client. The cost of the
   server check is one set lookup per request.
2. **Make every save-and-X button submit the form first via JS.**
   Rejected; adds a JS dependency to a feature that should work
   without JS, and it doesn't help curl callers.
3. **Block free-text + autocomplete on long lists too.** Rejected;
   ADR-0011 is correct for ledger-derived pickers; that's a different
   trade-off (typing speed vs constraint).
