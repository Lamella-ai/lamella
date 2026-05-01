# ADR-0030: File Operations Validate Paths Against Allowed Roots (Project-Wide)

- **Status:** Accepted
- **Date:** 2026-04-27
- **Author:** AJ Quick
- **Related:** [ADR-0002](0002-in-place-rewrites-default.md), [ADR-0004](0004-bean-check-after-every-write.md), `src/lamella/rewrite/txn_inplace.py`

## Context

ADR-0002 introduced path safety for the in-place rewrite system: all
writes must be under `ledger_dir`, not under archive/backup
subdirectories, and not through symlinks. That rule lives only in
`rewrite/txn_inplace.py` and the snapshot machinery.

Since ADR-0002, additional write paths have appeared: the Paperless
writeback system, the SimpleFIN writer, the CSV importer, the audit
log, and the job system all write files to paths that are at least
partially derived from configuration or runtime data. None of those
paths are validated against the rules that `txn_inplace.py` enforces.

A misconfigured `LAMELLA_DATA_DIR` or a crafted Paperless document
field could direct a write outside the intended directory tree. The
failure mode is silent data corruption or data placed outside the
container's expected mount points.

The Phase 7 violation scan found 4 write-mode `open()` calls. None
take raw user-supplied paths today, all derive from configured paths.
Low real risk, but they should still flow through the validator going
forward to defend against future changes.

## Decision

Path safety from ADR-0002 is promoted to a project-wide rule. Any
write to a file path that is not a string literal MUST go through a
validated path check before the write.

The canonical implementation is a single function:

```python
lamella.fs.validate_safe_path(candidate: Path, allowed_roots: list[Path]) -> Path
```

It MUST:

1. Resolve the candidate to an absolute path (`.resolve(strict=False)`).
2. Raise `ValueError` if the resolved path does not start with at
   least one of `allowed_roots` (after resolving each root too).
3. Raise `ValueError` if the candidate or any component of its path
   is a symlink (`Path.is_symlink()` on each parent).
4. Raise `ValueError` if the path is under a backup directory pattern:
   `.pre-inplace-*/`, `.bak`, `.backup`. These are reserved for
   snapshot machinery. External callers do not write to them.

Allowed roots are determined by configuration at call time:
`ledger_dir`, `data_dir`, or other `LAMELLA_*`-configured directories.
Hardcoded OS paths (`/tmp`, `/etc`) are never allowed roots.

Write calls covered: `open(path, 'w')`, `open(path, 'wb')`,
`Path.write_text(...)`, `Path.write_bytes(...)`, `shutil.copy(...)`,
`shutil.move(...)`. Read calls are not in scope. A path traversal on
a read is a confidentiality concern, not a data-integrity concern, and
is a separate threat model.

## Consequences

### Positive
- A misconfigured data directory that points outside the container's
  mounts is caught before the write, not after the damage is done.
- Symlink attacks, placing a symlink inside `ledger_dir` that points
  outside, are refused at write time.
- The backup directory reservation means snapshot machinery is the
  sole owner of `.pre-inplace-*` directories. No other code can
  accidentally write into a snapshot slot.

### Negative / Costs
- Every new write path must import and call `validate_safe_path`. The
  compliance check catches omissions, but it adds friction to writing
  new file-output code.
- `strict=False` in `Path.resolve()` means the path is validated
  against its intended location, not its current location. If an
  intermediate directory is replaced with a symlink after the check
  but before the write, the validator does not catch it (TOCTOU). This
  risk is acceptable in a single-container, single-user deployment.

### Mitigations
- `validate_safe_path` is a one-import, one-call addition to any write
  site. The function signature is designed to be hard to call
  incorrectly.
- The ADR-0004 snapshot machinery already owns the backup naming
  convention. Formalizing it here prevents accidental name collisions.

## Compliance

How `/adr-check` detects violations:

- **Write without validation:** AST scan `src/lamella/` for `open(`,
  `Path(...).write_text(`, `Path(...).write_bytes(`, `shutil.copy(`,
  `shutil.move(` where the path argument is not a string literal
  (`Constant` AST node) AND the enclosing function does not contain a
  call to `validate_safe_path` before the write. Flag every hit.
- **Writes to backup dirs:** grep `src/lamella/` for string literals
  containing `.pre-inplace-` or `.bak` or `.backup` in write context.
  Only the snapshot module (`rewrite/snapshot.py` or equivalent) is
  allowed to reference these patterns.

## References

- [ADR-0002](0002-in-place-rewrites-default.md): in-place rewrites
  (original path safety rule for `txn_inplace.py`)
- [ADR-0004](0004-bean-check-after-every-write.md): snapshot+restore
  (uses backup directory naming convention)
- `src/lamella/rewrite/txn_inplace.py`: reference implementation of
  path validation (pre-ADR-0030 shape; to be refactored to call shared
  helper)
- `src/lamella/fs.py` (proposed): `validate_safe_path` implementation
  home
