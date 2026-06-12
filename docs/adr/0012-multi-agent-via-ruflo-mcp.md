# ADR-0012: Multi-Agent Work Dispatches via ruflo MCP, Not Task

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Multi-agent dispatch: ruflo MCP, not Task"), [ADR-0013](0013-workers-verify-own-commits.md), [ADR-0014](0014-memory-write-everything-list-to-recall.md)

## Context

Claude Code's built-in Task tool can spawn subagents. For a single
delegated subtask, that tool is adequate. For N>1 parallel workers
(a swarm decomposing a feature into independent tracks), the Task
tool has no shared memory, no coordinator visibility into individual
worker state, and no structured handoff protocol. Each Task call is
an isolated event; the coordinator cannot poll worker progress,
share context between agents, or receive structured reports.

ruflo MCP provides `swarm_init`, `agent_spawn`, and `memory_store`
as a structured multi-agent layer. Workers write findings to a
shared namespace. The coordinator can list or retrieve those
entries to synthesize results. This is the infrastructure this
docs-restructure swarm itself used.

Reaching for Task with N>1 subagents duplicates the swarm pattern
in a less capable primitive, which produces coordination gaps:
workers that collide on the same file, workers that produce
contradictory results neither the coordinator nor the other workers
see, and done reports that cannot be verified against shared state.

## Decision

Multi-worker parallel work (N > 1 subagents) MUST use ruflo MCP:

- `mcp__ruflo__swarm_init` to initialize the swarm.
- `mcp__ruflo__agent_spawn` to dispatch workers.
- `mcp__ruflo__memory_store` for workers to publish findings.
- `mcp__ruflo__memory_list` for the coordinator to aggregate results.

Claude Code's Task tool is acceptable ONLY for trivial single-agent
delegation (N = 1, no shared state needed, no verification required).

Normative obligations:

- MUST NOT use N>1 Task calls as a substitute for swarm dispatch.
- MUST initialize a named swarm before spawning workers.
- MUST ensure every worker writes its findings to ruflo memory
  (see [ADR-0014](0014-memory-write-everything-list-to-recall.md)).
- MAY use Task for a single self-contained subtask that produces
  no output other agents depend on.

If ruflo MCP tools are not loaded, MUST call `ToolSearch` with
`select:mcp__ruflo__swarm_init,mcp__ruflo__agent_spawn,mcp__ruflo__memory_store`
before proceeding.

## Consequences

### Positive
- Coordinator has full visibility into all worker state via the
  shared memory namespace.
- Workers can read each other's findings without coordinator relay.
- Structured handoff protocol (see [ADR-0013](0013-workers-verify-own-commits.md))
  is enforced at the swarm layer.

### Negative / Costs
- ruflo MCP must be loaded and healthy before a swarm starts. If
  the MCP server is unavailable, multi-agent work blocks entirely.
- Swarm overhead (init, spawn, memory writes) is non-trivial for
  tasks that would be fast as a single Task call. The N>1 threshold
  is the selector.

### Mitigations
- `ToolSearch` fallback ensures tools are loaded before use.
- For swarm health checks: `mcp__ruflo__swarm_health` before dispatch.

## Compliance

- **Process:** any PR that introduces N>1 Task calls in agent
  orchestration code MUST be flagged in review. Add comment
  explaining why the swarm pattern is not appropriate, or convert.
- **Review:** swarm init and agent spawn calls must appear in pairs
  per swarm run. Orphaned spawns without a parent swarm are a
  violation.

## References

- CLAUDE.md §"Multi-agent dispatch: ruflo MCP, not Task"
- [ADR-0013](0013-workers-verify-own-commits.md)
- [ADR-0014](0014-memory-write-everything-list-to-recall.md)
