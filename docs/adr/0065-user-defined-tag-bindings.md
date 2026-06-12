# ADR-0065: User-Defined Tag→Action Bindings

- **Status:** Accepted (2026-05-02)
- **Date:** 2026-05-02
- **Author:** AJ Quick
- **Related:** [ADR-0001](0001-ledger-as-source-of-truth.md), [ADR-0015](0015-reconstruct-capability-invariant.md), [ADR-0026](0026-migrations-forward-only.md), [ADR-0044](0044-paperless-lamella-custom-fields.md), [ADR-0061](0061-documents-abstraction-and-ledger-v4.md), [ADR-0062](0062-tag-driven-workflow-engine.md), [ADR-0064](0064-lamella-paperless-namespace-colon.md)
- **Amends:** [ADR-0062](0062-tag-driven-workflow-engine.md) §3 (hardcoded DEFAULT_RULES)

## Context

ADR-0062 shipped the tag-driven workflow engine with hardcoded `DEFAULT_RULES` in
`tag_workflow.py`. The `extract_missing_fields` rule's selector was:

```python
DocumentSelector(
    must_have_tags=(),
    must_not_have_tags=(TAG_EXTRACTED, TAG_NEEDS_REVIEW),
)
```

This matches **every document** that doesn't already carry `Lamella:Extracted` or
`Lamella:NeedsReview`. On a fresh install connected to a Paperless instance with
thousands of existing documents, the first scheduler tick would auto-apply
`Lamella:AwaitingExtraction` to all of them and start firing AI extraction on the
entire corpus — without the user asking for it. This is wrong.

The engine needs an opt-in model: workflows do nothing by default, and the user
explicitly creates bindings to activate automation.

## Decision

Bindings are user-controlled, persisted as `custom "lamella-tag-binding"` directives
in `connector_config.bean`, cached in the `tag_workflow_bindings` DB table, and
reconstructed via reconstruct step 26.

**Empty bindings = workflows do nothing.** The scheduler tick calls
`load_runtime_rules(conn)`, which reads the bindings table. If the table is empty
(fresh install, no user-created bindings), the tick is a no-op. No documents are
touched, no AI is fired. The user opts in by creating a binding via
`/settings/paperless-workflows`.

### Directive vocabulary

```
; create or update a binding (last-write-wins per tag_name)
2026-05-02 custom "lamella-tag-binding" "Lamella:Process" "extract_fields"
  lamella-enabled: TRUE
  lamella-config-json: ""
  lamella-created-at: "2026-05-02T14:30:00"

; revoke a binding (marks it inactive; history preserved)
2026-05-02 custom "lamella-tag-binding-revoked" "Lamella:Process"
  lamella-revoked-at: "2026-05-02T14:31:00"
```

The first positional arg is the trigger tag name. The second (for binding only) is the
action name. Metadata carries enabled state, optional action config JSON, and the
creation timestamp.

### Source of truth: `connector_config.bean`

Bindings are config, not link state, so they belong in `connector_config.bean`
alongside other user-defined configuration (settings, entities, loan definitions, etc.),
not in `connector_links.bean` which is reserved for document↔transaction link state.

### DB cache: `tag_workflow_bindings` table

```sql
CREATE TABLE tag_workflow_bindings (
    tag_name      TEXT PRIMARY KEY,
    action_name   TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    config_json   TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
```

This table is a reconstructible cache per ADR-0001. It is never the source of truth.

### Reconstruct step 26

`step26_tag_bindings.py` implements the ADR-0015 invariant: after a DB rebuild,
`tag_workflow_bindings` is repopulated from the ledger directives, producing the same
active-binding state as before the wipe.

### Runtime rule loading

The scheduler tick calls `load_runtime_rules(conn)` instead of iterating
`DEFAULT_RULES`. Each enabled binding becomes one `WorkflowRule` with:

- `selector.must_have_tags = (binding.tag_name,)` — only docs carrying the
  trigger tag are processed.
- `selector.must_not_have_tags = (completion_tag,)` — docs already tagged with
  the completion tag are skipped (idempotency).
- `on_success = (RemoveTag(trigger_tag), AddTag(completion_tag))` — on success,
  the trigger tag is removed so the doc can't be reprocessed, and the state tag
  is applied.

### Five canonical state tags remain system-managed

`CANONICAL_TAGS` in `tag_workflow.py` and `bootstrap_canonical_tags` at startup are
unchanged. `Lamella:AwaitingExtraction`, `Lamella:Extracted`, `Lamella:NeedsReview`,
`Lamella:DateAnomaly`, and `Lamella:Linked` continue to be created idempotently in
Paperless at every boot. Bindings are pure user-controlled trigger surface; the state
tags themselves are system-managed.

## Consequences

### Positive

- **No surprise automation.** A fresh install connected to an existing Paperless
  corpus does nothing until the user creates a binding.
- **Full user control.** The user decides which Paperless tags trigger which actions.
  Any Paperless tag can be a trigger — not just `Lamella:*` tags.
- **Survives DB rebuild.** Bindings are ledger directives; step26 rebuilds the cache
  from scratch. ADR-0015 satisfied.
- **Append-only history.** Revoke directives preserve the full binding history in the
  ledger for auditability.

### Negative / trade-offs

- Users need to explicitly create at least one binding before any workflow automation
  fires. There is no "default configuration" for new installs — the settings UI
  (Worker K) must guide the user through creating their first binding.
- The on-demand trigger endpoint (`POST /paperless/workflows/{rule_name}/run`) still
  uses `DEFAULT_RULES` for backward compat with existing tests. Worker K will need
  to surface user-defined rules separately or extend the endpoint to accept binding
  names.

## Compliance

| ADR   | Requirement                         | How satisfied                                         |
|-------|-------------------------------------|-------------------------------------------------------|
| 0001  | Ledger is source of truth           | Directives in `connector_config.bean`                 |
| 0015  | Reconstruct capability invariant    | step26 rebuilds `tag_workflow_bindings` from ledger   |
| 0026  | Migrations are forward-only         | Migration 068 adds table; no destructive DDL          |
| 0003  | `lamella-*` metadata namespace      | All directive meta keys are `lamella-*` prefixed      |
| 0004  | Bean-check after every write        | `append_binding` uses `append_custom_directive`       |
