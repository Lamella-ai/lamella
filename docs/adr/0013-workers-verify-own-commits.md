# ADR-0013: Workers Verify Their Own Commits

- **Status:** Accepted
- **Date:** 2026-04-26
- **Author:** AJ Quick
- **Related:** `CLAUDE.md` ("Workers verify their own commits"), [ADR-0012](0012-multi-agent-via-ruflo-mcp.md)

## Context

A swarm worker that reports "done" without evidence is unverifiable.
The coordinator has no way to distinguish:

1. Worker completed all file edits and committed them.
2. Worker completed edits but the commit failed silently.
3. Worker hallucinated completion without making any changes.

In swarms with multiple workers writing to the same branch, an
unverified "done" from one worker can cause the coordinator to
proceed as if state has landed when it hasn't. Subsequent workers
that depend on that state then operate on stale context.

The minimal verifiable artifact is the git commit SHA. A worker
that has truly committed work can always produce `git log -1 --format=%H`.
A worker that produced files but failed to commit will produce a
different SHA, or the same SHA as before the run.

## Decision

Every swarm worker that produces file changes MUST verify its
own commit before reporting done.

Worker obligations:

1. After completing all file edits, run:
   ```
   git log -1 --format=%H
   ```
2. Include the output SHA in the done report.
3. If commit failed (pre-commit hook rejection, merge conflict,
   permission error), report that state explicitly. MUST NOT report
   success without a commit.

Coordinator obligations:

1. After receiving a done report, run `git log --format=%H` and
   verify the reported SHA appears in the log.
2. If the SHA is absent: mark the worker's item as incomplete.
   Do NOT count it done. Re-queue or escalate.
3. If the SHA is present but the expected files are absent from
   `git show <SHA>`: same result, mark incomplete.

Normative obligations:

- Worker MUST include `git log -1 --format=%H` output verbatim in
  the done report.
- Worker that produced files but could not commit MUST report
  "produced files, commit failed", never as success.
- Coordinator MUST NOT mark an item complete without a verified SHA
  in `git log`.

## Consequences

### Positive
- Done reports are machine-checkable in one command.
- Partial work (edits without commit) surfaces before the swarm
  closes, not after the orchestrator discovers missing files.
- The audit trail in `git log` matches the swarm's claimed progress.

### Negative / Costs
- Workers must run an extra git command per task. Overhead is
  negligible but the discipline requires it even for trivial edits.
- If the coordinator's branch has diverged (force-push, rebase by
  another party), the SHA check may produce false negatives.
  Coordinators operating on shared branches should verify against
  `git log --all`.

### Mitigations
- Workers publish the SHA to ruflo memory alongside other findings
  (see [ADR-0014](0014-memory-write-everything-list-to-recall.md))
  so the coordinator has a retrievable record.

## Compliance

- **Process:** done reports without a SHA field are non-compliant.
  Coordinator rejects them and re-queries the worker.
- **Format:** done report MUST include a field `commit_sha` or
  equivalent. SHA pattern: `[0-9a-f]{40}`.

## References

- CLAUDE.md §"Workers verify their own commits"
- [ADR-0012](0012-multi-agent-via-ruflo-mcp.md)
- [ADR-0014](0014-memory-write-everything-list-to-recall.md)
