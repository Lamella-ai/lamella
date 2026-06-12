# ADR-0014: Memory, Write Everything, List to Recall

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Memory: write everything, search via list"), [ADR-0012](0012-multi-agent-via-ruflo-mcp.md), [ADR-0013](0013-workers-verify-own-commits.md)

## Context

In a swarm run, workers produce findings: file paths read,
patterns found, decisions made, SHAs committed. Without a
structured store, those findings exist only in the worker's local
context window. The coordinator cannot access them without asking
the worker to re-summarize, which is lossy and slow. Other workers
that need the same findings re-discover them independently.

ruflo memory provides `memory_store` and `memory_list` as the
shared state layer for swarm runs. A stored entry is retrievable
by any agent in the swarm.

`mcp__ruflo__memory_search` is currently broken upstream. It
returns 0 results despite stored entries. `mcp__ruflo__memory_list`
also rejects slash-separated namespaces with `namespace contains
invalid characters`. Until both are fixed, recall is via
`mcp__ruflo__memory_retrieve` with the exact namespace + key.

## Decision

Every swarm worker MUST write its findings to ruflo memory via
`mcp__ruflo__memory_store`.

Namespace convention: `lamella/<domain>/<topic>`. Examples:
- `lamella/docs-restructure/phase3/adr-0010` key `draft-v1`
- `lamella/swarm-run/findings/route-coverage`

Normative obligations:

- Worker MUST call `memory_store` with all findings before
  reporting done. A done report without prior `memory_store` calls
  is non-compliant unless the worker produced no findings.
- Worker MUST use the `lamella/<domain>/<topic>` namespace pattern.
  Flat keys without a namespace are non-compliant.
- Coordinator MUST recall using `memory_retrieve(namespace, key)`
  with the exact pair, NOT `memory_search` or `memory_list` until
  the upstream bugs are fixed.
- Worker MAY store intermediate findings as `draft-v1`, `draft-v2`
  etc. to preserve history.

## Consequences

### Positive
- All worker findings are accessible to the coordinator and to
  peer workers via direct retrieve calls.
- The namespace prefix pattern groups findings by domain; with
  upstream fixes, list-by-prefix recall becomes possible.
- Audit trail: memory entries can be reviewed after the swarm
  completes without re-running agents.

### Negative / Costs
- `memory_search` is broken upstream as of 2026-04-26. Semantic
  recall (find entries similar to a query) is not available.
- `memory_list` rejects slash namespaces. Coordinators must track
  the exact (namespace, key) pairs workers used.
- Workers must serialize findings to string or JSON before storing.
  Large structured outputs require explicit truncation or chunking.
- No TTL is set by default; stale entries accumulate. Namespace
  discipline (namespacing by swarm run or date) is the mitigation.

### Mitigations
- Track upstream `memory_search` and `memory_list` fixes. When
  fixed, update this ADR and remove the prohibition.
- Namespace runs by swarm id or date (e.g.
  `lamella/swarm-<id>/...`) to avoid cross-run collisions.
- Coordinator records the (namespace, key) pairs each worker
  reports back, enabling later retrieve without listing.

## Compliance

- **Process:** any worker done report MUST be accompanied by at
  least one `memory_store` call per finding domain. Workers that
  produced findings without storing them are non-compliant.
- **Review:** coordinator inspects `memory_retrieve` output for
  each reported key after swarm close; missing entries from a
  worker that reported "done" trigger a re-verify per
  [ADR-0013](0013-workers-verify-own-commits.md).

## References

- CLAUDE.md §"Memory: write everything, search via list"
- [ADR-0012](0012-multi-agent-via-ruflo-mcp.md)
- [ADR-0013](0013-workers-verify-own-commits.md)
