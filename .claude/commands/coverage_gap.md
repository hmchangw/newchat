---
description: Find Go functions below the 80% coverage floor and add TDD-written tests to close the gap
argument-hint: [service-directory-name]
---

You are closing the test-coverage gap. CLAUDE.md Section 4 sets a **hard 80% minimum** for all packages — this command finds violations and writes tests for them following the Red-Green-Refactor TDD cycle.

# Scope

- If `$ARGUMENTS` is a service directory name: operate on that service only.
- If `$ARGUMENTS` is empty: operate on every top-level directory containing `main.go` that has files changed on this branch (`git diff --name-only main...HEAD`). If no services were touched by the branch, ask the user to pass an explicit service name — do not silently fall back to the full repo.

# Step 1 — Measure

For each service in scope:
1. Run `make test SERVICE=<svc>` to verify the existing suite passes. If it fails, stop and report — do not write new tests on top of broken ones.
2. Run `go test -coverprofile=/tmp/cov-<svc>.out ./<svc>/...` and `go tool cover -func=/tmp/cov-<svc>.out` (acceptable read-only audit step, same exception used in `/production_readiness`).
3. Parse the per-function output. Build `GAP_FUNCTIONS` = functions with coverage `< 80%` or `0%`.
4. Skip from `GAP_FUNCTIONS`:
   - Files matching `*_gen.go`, `mock_*.go`, or containing the `// Code generated …` header.
   - `func main()` and trivial constructors (single-statement return, no logic).
   - Test files themselves (`*_test.go`).

# Step 2 — Group by file and dispatch TDD agents (parallel)

Group `GAP_FUNCTIONS` by file. Dispatch one agent per file in parallel (single message, multiple Agent tool calls).

Each agent must:
- Read `CLAUDE.md` (Sections 3 and 4) and the per-service file organization conventions.
- Read the target file in full + the existing sibling `*_test.go` to learn the test style (table-driven, mock setup helpers, naming) and avoid duplication.
- Read any imported `pkg/` types the gap functions depend on.
- Invoke `superpowers:test-driven-development` for the workflow.
- For each gap function:
  - Write Red tests first — covering **happy path, error paths, edge cases** (empty collections, boundary conditions, invalid input) per CLAUDE.md Section 4.
  - Use **table-driven style** when multiple scenarios share the same logic, with descriptive sub-test names via `t.Run`.
  - Use `testify/assert` + `testify/require` and `go.uber.org/mock` mocks where already present in the package.
  - Confirm tests are meaningful — they must actually exercise the function, not just call mocks and assert mock interactions. If you're only asserting on mock calls, the test is wrong.
  - Run `go test -run <new-test-name> -race ./<package>` to confirm pass.
- If a function cannot be tested without implementation changes (untestable dependency, missing seam, real network call): **do NOT bend the test or modify production code**. Skip it and add an entry to a `BLOCKERS` list with `file:line` + reason.
- Never edit production code in this command. Tests only.
- Commit per file: `git commit -m "test(<svc>): cover <function-list-truncated>"` with the standard `Co-Authored-By: Claude` trailer (via HEREDOC).
- Return ≤ 400 words: functions covered, lines added, blockers found.

# Step 3 — Verify uplift

After all agents return:
1. Re-run `make test SERVICE=<svc>` to confirm everything passes including the new tests.
2. Re-run `go test -coverprofile=/tmp/cov-<svc>.out ./<svc>/...` and `go tool cover -func=/tmp/cov-<svc>.out`. Capture the new total %.
3. If the new total is still `< 80%`, surface this in the final output — the remaining gap is in the BLOCKERS list and needs a human (likely requires refactoring for testability).

# Final chat output (5–15 lines)

For each service in scope, one block:
- Service name
- Coverage before → after (numeric %)
- Functions covered (count)
- Files touched (count)
- Blockers (count + brief description of the worst one, if any)

End with a final line: `<service> now meets the 80% floor` OR `<service> still below 80% — see blockers above; manual refactoring needed`.

# Notes

- The pre-commit hook runs `make lint` and `make test` on every `.go`-touching commit. Per-file commits trigger this each time. That is the intended cost.
- If a commit fails the pre-commit hook, the agent must fix the failure and create a NEW commit (never `--amend`, never `--no-verify`).
