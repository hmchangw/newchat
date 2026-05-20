---
description: Two-pass Go comment cleanup — delete WHAT-comments, shorten what remains to max 2 lines (target 65% 1-line)
argument-hint: [path]
---

You are cleaning up Go comments per the project's CLAUDE.md guidance and the 2-line / 65% rule.

This command produces a single edit pass — no incremental report file, no chapter commits. Use `superpowers:verification-before-completion` for the final verification step before reporting success.

# Scope

- If `$ARGUMENTS` is empty: operate on `.go` files changed on this branch. Source list = `git diff --name-only main...HEAD` ∪ tracked `.go` files in `git status --porcelain`.
- If `$ARGUMENTS` is a path: operate on `.go` files under that path (recursively).

Never touch generated files (`*_gen.go`, files with `// Code generated …`, `mock_*_test.go`).

# Pass 1 — DELETE pass

For every comment in scope, decide:

**Delete** when any of the following holds:
- It explains WHAT the code does and well-named identifiers already convey the same.
- It restates the function signature or obvious behavior.
- It references the current task / fix / PR ("added for feature X", "fixes #123", "handles the case from the Y flow") — that belongs in the PR description, not in code.
- It is a `// TODO` / `// FIXME` without an owner or ticket reference AND the surrounding code does not actually require the TODO.
- It is a `// removed XYZ` tombstone for code that no longer exists.
- It is commented-out code.

**Keep** when:
- It explains WHY — hidden constraint, subtle invariant, workaround for a specific bug, behavior that would surprise a reader.
- It is a godoc comment on an exported identifier (Go convention requires these — proceed to Pass 2 to shorten if verbose).
- It is a compiler directive: `//go:generate`, `//go:build`, `//nolint…`, `// #nosec …`.

# Pass 2 — SHORTEN pass

For each surviving comment:
- Hard cap: **max 2 lines** per comment block. Rewrite to fit.
- Target: **≥ 65% of comments in the touched set are 1 line**. After shortening, if you are below 65%, look for 2-line comments where the second line is filler and collapse them.
- Preserve information content — never drop a WHY detail to hit the quota.
- For godoc on exported identifiers: keep the first line in standard godoc form (`// FuncName does X.`). Add a second line only if it captures a non-obvious constraint.

# Verify (mandatory — do not skip)

After both passes:
1. `make fmt`
2. `make lint`
3. `make test` — scope: if your edits stayed inside a single service, `make test SERVICE=<name>`; otherwise full suite.

If any step fails, fix the failures or revert the affected file. Do not report success until all three pass.

# Final chat output (5–8 lines, nothing else)

- Files touched (count)
- Comments deleted (count)
- Comments shortened (count)
- Final 1-line ratio: `<X> / <total> = <Y>%`
- `make fmt` / `make lint` / `make test` status (pass/fail)
