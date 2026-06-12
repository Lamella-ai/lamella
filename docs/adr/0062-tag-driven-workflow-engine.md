# ADR-0062: Tag-Driven Workflow Engine for Paperless Documents

- **Status:** Accepted (2026-05-02)
- **Date:** 2026-05-02
- **Author:** AJ Quick
- **Related:** [ADR-0006](0006-long-running-ops-as-jobs.md), [ADR-0016](0016-paperless-writeback-policy.md), [ADR-0027](0027-http-tenacity-timeout.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0053](0053-paperless-read-precedence-and-self-test.md), [ADR-0055](0055-prompts-must-be-generalized.md), [ADR-0061](0061-documents-abstraction-and-ledger-v4.md), [ADR-0064](0064-lamella-paperless-namespace-colon.md)
- **Depends on:** ADR-0061

> **Tag names use the colon separator per
> [ADR-0064](0064-lamella-paperless-namespace-colon.md) (2026-05-02).**
> The five canonical tags are now `Lamella:AwaitingExtraction`,
> `Lamella:Extracted`, `Lamella:NeedsReview`, `Lamella:DateAnomaly`,
> `Lamella:Linked`. Reads accept both forms; writes use the colon
> form. The body of this ADR is otherwise unchanged — every reference
> to `Lamella_X` should be read as `Lamella:X`.

> **§3 amended by [ADR-0065](0065-user-defined-tag-bindings.md) (2026-05-02).**
> The hardcoded `DEFAULT_RULES` are removed from the scheduler tick; rules are now
> user-defined via `custom "lamella-tag-binding"` directives in `connector_config.bean`
> and loaded at runtime from the `tag_workflow_bindings` DB cache. Empty bindings =
> workflows do nothing on fresh install — the user opts in via
> `/settings/paperless-workflows`. The five canonical state tags (`Lamella:Extracted`,
> `Lamella:NeedsReview`, `Lamella:DateAnomaly`, `Lamella:Linked`,
> `Lamella:AwaitingExtraction`) remain system-managed. `DEFAULT_RULES` is kept in code
> for backward-compatibility with the on-demand trigger endpoint and existing tests.

## Context

Paperless document processing in Lamella is manual today. The user
clicks "Verify" on a single document, the AI extracts fields, the
result lands in `paperless_writeback_log`, and that's it. There is
no way to say "every new document needs date extraction" or "every
document with a date older than the year 2000 needs human review."
The flow is one document at a time, every time, with no
automation.

The Paperless API already provides everything an automation engine
needs:

- `iter_documents({"tags": [id]})` lists documents with a specific
  tag, fully paginated (`adapters/paperless/client.py:194`).
- `ensure_tag(name)` is idempotent (`client.py:374`).
- `patch_document(doc_id, tags=[...])` updates the tag list
  (`client.py:319`) — albeit with replacement semantics, not
  additive.
- `list_tags()` enumerates all tags (`client.py:360`) — though
  unmemoized, so a naive automation would re-fetch on every rule
  evaluation.

The scheduler infrastructure also exists. APScheduler is wired in
`main.py` lifespan (`main.py:1167`) and already runs a periodic
`_run_paperless_sync` job on `IntervalTrigger(hours=...)`. Adding
a second periodic job is a copy-paste of three lines.

The audit infrastructure also exists.
`paperless_writeback_log` (`paperless_writebacks.py:99`) is a
queryable table with `kind`, `dedup_key`, `payload_json`, and
`ai_decision_id` columns. The `/paperless/writebacks` page renders
it as a browsable diff log. New automation events fit this table
without schema change — only new `kind` values.

What does not exist:

- A rule abstraction that says "find documents matching X, run
  action Y, on success apply tag Z, on anomaly apply tag W."
- A trigger model that distinguishes scheduled rules (run on a
  cadence) from on-demand rules (run when the user clicks a
  button).
- A naming convention for the tags that mark workflow state.
- An anomaly review queue UI distinct from the existing writeback
  log.
- A date-sanity check (impossibly old or future dates).

The user requirement is: define automated workflows based on
document tags. Find all documents missing a certain tag, process
them through AI to extract dates and other info, then apply the
tag when done. Flag documents with dates that don't make sense.
Some workflows should run on a schedule; others should be
triggerable on demand. Keep an audit trail of what the AI did to
each document.

## Decision

Lamella adds a code-defined tag-driven workflow engine that runs
periodic and on-demand jobs against Paperless using the
`Lamella_*` tag namespace reserved by ADR-0061. Workflows are
expressed as `WorkflowRule` Python dataclasses in a new module
`paperless_bridge/tag_workflow.py`. Audit rows go into the
existing `paperless_writeback_log` with new `kind` values. A new
review-queue page surfaces anomaly-flagged documents.

### 1. Tag namespace and canonical state tags

Five canonical tags are reserved under the `Lamella_*` namespace:

| Tag name | Meaning |
|---|---|
| `Lamella_AwaitingExtraction` | Document needs AI field extraction. Applied at sync time to any new document that lacks `Lamella_Extracted`. |
| `Lamella_Extracted` | AI extraction completed and fields meet the confidence threshold. Set by the extraction workflow on success. |
| `Lamella_NeedsReview` | AI extraction completed but at least one field fell below the confidence threshold. Set by the extraction workflow on low confidence. |
| `Lamella_DateAnomaly` | The extracted document date is outside the configured sanity bounds (e.g. before year 2000 or after today). Set by the date-sanity workflow. |
| `Lamella_Linked` | Document has been linked to a transaction. Set by the linker (already exists in DB; this ADR mirrors it as a Paperless tag). |

These tags are state markers, not user-facing categorization. Users
should not edit them by hand; the workflows own them. (Users can,
however, see them in Paperless searches, which is the point.)

`PaperlessClient.ensure_tag(name)` is called at engine startup to
create any missing tags idempotently.

### 2. WorkflowRule abstraction

```python
@dataclass(frozen=True)
class WorkflowRule:
    name: str                                  # stable id, e.g. "extract_missing_fields"
    description: str                           # one-line user-facing
    trigger: Literal["scheduled", "on_demand"]
    schedule: IntervalTrigger | None           # required iff trigger=scheduled
    selector: DocumentSelector                 # which docs match
    action: WorkflowAction                     # what to run
    on_success: list[TagOp]                    # tag ops if action returns ok
    on_anomaly: list[TagOp]                    # tag ops if action flags anomaly
    on_error: list[TagOp]                      # tag ops if action raises
```

`DocumentSelector` is one of:

- `MissingTag(tag_name: str)` — docs without this tag
- `HasTag(tag_name: str)` — docs with this tag
- `MissingField(field_name: str)` — docs whose Paperless custom
  field is null/missing
- `And(*selectors)`, `Or(*selectors)`, `Not(selector)` —
  composition

`WorkflowAction` is one of:

- `ExtractFields(document_type_aware=True)` — runs the
  ADR-0061 extraction cascade
- `DateSanityCheck(min_year: int, max_date_offset_days: int)` —
  flags impossibly-old or future-dated docs
- `LinkToLedger()` — runs the ADR-0063 reverse matcher
  (composes with that ADR)

`TagOp` is one of:

- `ApplyTag(name: str)`
- `RemoveTag(name: str)`

Rules live as a Python list in
`paperless_bridge/tag_workflow.py::DEFAULT_RULES`. They are
**code-defined for v1**: source-controlled, reviewable in PR,
testable. Migration to DB-defined rules is deferred until a
multi-user need arises (see "What this ADR does not decide").

### 3. Default rule set

The v1 default rules are:

1. **`extract_missing_fields`** — scheduled, hourly. Selector:
   `MissingTag("Lamella_Extracted") AND
   MissingTag("Lamella_NeedsReview")`. Action:
   `ExtractFields(document_type_aware=True)`. On success: apply
   `Lamella_Extracted`. On anomaly (low confidence): apply
   `Lamella_NeedsReview`.

2. **`date_sanity_sweep`** — on-demand only. Selector:
   `HasTag("Lamella_Extracted") AND
   NOT HasTag("Lamella_DateAnomaly")`. Action:
   `DateSanityCheck(min_year=2000, max_date_offset_days=0)`. On
   anomaly: apply `Lamella_DateAnomaly`. The bounds are settable
   per-invocation in the trigger UI so the user can ad-hoc widen
   them ("scan for 1900–1980").

3. **`link_to_ledger`** — composes with ADR-0063, scheduled (same
   cadence as the Paperless sync). Selector:
   `HasTag("Lamella_Extracted") AND NOT HasTag("Lamella_Linked")`.
   Action: `LinkToLedger()`. On success: apply `Lamella_Linked`.

### 4. Engine: scheduling and on-demand triggering

A new function `_run_doc_tag_workflow(app)` is added to `main.py`,
mirroring `_run_paperless_sync` (`main.py:702`). It is registered
in the lifespan startup with `IntervalTrigger(minutes=...)` (the
interval is a setting; default 60 minutes). The job evaluates all
rules where `trigger="scheduled"` in source-order, processing each
rule's matched documents serially.

On-demand rules expose a button in `/settings/paperless` that
POSTs to `/paperless/workflows/{rule_name}/run`. The POST kicks
off a background job using the existing `JobRunner` infrastructure
(ADR-0006), with progress events emitted to a modal mirroring the
verify flow. The same job machinery handles scheduled and
on-demand runs; the only difference is who fires it.

### 5. Concurrency, idempotency, and rate limits

Two safeguards prevent runaway behavior:

- **Single-instance per rule.** APScheduler is configured with
  `max_instances=1, coalesce=True` per rule (same pattern as
  `_paperless_sync_job`). A long-running rule cannot stack
  invocations.
- **Per-document idempotency.** Before running an action, the
  engine checks whether the document already has the rule's
  `on_success` tag — if so, skip. This makes scheduled re-runs
  safe even if a tag op briefly fails to apply.

The Paperless API is rate-limited via the existing tenacity
wrapper (ADR-0027). The engine adds a per-rule batch size cap
(default 50 documents per run) to prevent a single tick from
swamping the API.

### 6. PaperlessClient additions

Two helpers are added to `PaperlessClient`:

- **`add_tag(doc_id: int, tag_id: int) -> None`** — idempotent
  add-single-tag, implemented as read-`get_document` →
  union-merge → `patch_document(tags=union)`. Wraps the
  replacement-semantics gotcha so callers never get it wrong.
- **`remove_tag(doc_id: int, tag_id: int) -> None`** — symmetric
  remove.

`list_tags()` is given an in-memory TTL cache (default 60s) on
the client instance. The cache is invalidated whenever
`ensure_tag` creates a new tag. This eliminates the
re-fetch-per-rule-evaluation cost.

### 7. Audit trail

Each workflow execution writes a row to `paperless_writeback_log`
with one of three new `kind` values:

- `workflow_action` — action ran, recorded the action, the
  result, and the tag ops applied
- `workflow_anomaly` — action flagged the document as anomalous
  (e.g. date out of bounds, low extraction confidence)
- `workflow_error` — action raised; recorded the exception and
  the `on_error` tag ops applied

The `payload_json` includes: rule name, action type, before/after
tag set, action result summary, AI decision id (if applicable),
duration. The existing `/paperless/writebacks` page filters on
the new `kind` values via query string (`?kind=workflow_action`).

### 8. Anomaly review queue

A new page `/paperless/anomalies` filters
`paperless_writeback_log` to rows where `kind = 'workflow_anomaly'`
and where the document still carries the anomaly tag (i.e. not yet
resolved). Per-row actions:

- **Open in Paperless** — link-out to the document in Paperless
  (uses `lamella-paperless-url`)
- **Re-run extraction** — POSTs to
  `/paperless/{doc_id}/verify` (existing route)
- **Mark resolved** — removes the anomaly tag, writes a
  `workflow_action` audit row noting manual resolution

There is no shell-command escape hatch and no Python plugin
system. Manual fixes happen in Paperless itself; the anomaly tag
is the queue.

### 9. Settings

Two new settings are added:

- `paperless_workflow_interval_minutes` (default 60) — cadence for
  scheduled rules
- `paperless_date_sanity_min_year` (default 2000) — default
  for the on-demand date-sanity sweep
- `paperless_date_sanity_max_date_offset_days` (default 0) —
  same; 0 means "no future dates allowed"

The on-demand trigger UI also accepts per-invocation overrides for
the date-sanity bounds.

## Why this works

- **Code-defined rules pay no UI tax in v1.** The user is one
  person. Source-controlled rules are diffable, testable, and
  reviewable. Adding a CRUD UI with persistence, validation, and
  history would be more code than the engine itself for zero
  current benefit. The migration path to DB-defined rules is
  straightforward (same dataclass, hydrated from a table) when a
  second user need arises.

- **Triggerable, not automatic-only.** The user explicitly called
  out that not every workflow should run on a cadence — date
  anomalies are a manual sweep with adjustable bounds. The
  trigger model handles both with one abstraction.

- **The audit table is the queue.** The same
  `paperless_writeback_log` infrastructure already powers the
  `/paperless/writebacks` page. Filtering on `kind` reuses the
  rendering, the schema, and the user's mental model. The
  anomaly review queue is a filtered view, not a new system.

- **The `Lamella_*` tag namespace is grep-able.** Combined with
  ADR-0044's custom-field reservation, the user can search
  Paperless for `Lamella_` and see both fields and state tags.
  The naming is uniform.

- **Idempotency at every layer.** `ensure_tag` is idempotent.
  `add_tag` is idempotent. The pre-action check (skip if
  on_success tag already present) is idempotent. Scheduled re-runs
  cannot corrupt state.

- **No shell-command escape hatch.** A configurable shell hook is
  a security and maintenance hole even on a single-user
  self-hosted box. The review queue plus the existing manual
  re-run action covers every concrete use case the user
  described. If a true shell escape becomes necessary later, it
  is one row-action away in the anomaly queue UI, with an
  explicit allowlist.

- **The cascade respects ADR-0055.** `ExtractFields` reuses the
  document-type-aware prompt cascade from ADR-0061. Type-specific
  fields (PO number, line items) are append-only conditional
  blocks. No prompt edits to fix one failure mode.

## Compliance checks

This ADR is satisfied iff:

1. `paperless_bridge/tag_workflow.py` exists and exports
   `WorkflowRule`, `DEFAULT_RULES`, and `run_rule(rule, conn,
   client)`.
2. The five canonical tags are created at engine startup via
   `ensure_tag`. (Test:
   `tests/test_tag_workflow_bootstrap.py`.)
3. `_run_doc_tag_workflow` is registered in `main.py` lifespan
   with `IntervalTrigger`, `max_instances=1`, `coalesce=True`.
4. The on-demand UI at `/settings/paperless` exposes a button for
   each `trigger="on_demand"` rule.
5. `PaperlessClient.add_tag` and `remove_tag` exist and are
   idempotent. `list_tags()` is memoized with a 60s TTL. (Test:
   `tests/test_paperless_client_tag_ops.py`.)
6. Each workflow execution writes a `paperless_writeback_log` row
   with the new `kind` values. (Test:
   `tests/test_tag_workflow_audit.py`.)
7. `/paperless/anomalies` exists, filters by unresolved anomaly
   tags, and supports the three per-row actions in §8.
8. The default rule set from §3 is registered.

## What this ADR does not decide

- DB-defined rules with a CRUD UI. Deferred to v2 when needed.
- Custom shell-command hooks. Rejected.
- Per-document-type-specific rule sets (e.g. "for invoices, also
  extract PO number"). The action handles type-conditioning
  internally per ADR-0061; no separate rule per type.
- The reverse doc→txn matching algorithm. That is ADR-0063.
  `LinkToLedger` is a thin adapter to that matcher.
