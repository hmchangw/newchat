---
description: Multi-expert review of the current branch (or a PR), per-service + global lenses
argument-hint: [PR-number]
---

You are running the multi-expert branch review.

# Preconditions

1. Verify the working tree under `docs/reviews/` is clean. If dirty, ask the user to commit or stash those changes first. Other uncommitted changes are fine — they are part of what is being reviewed.
2. Determine the diff source:
   - If `$ARGUMENTS` is empty: review the current branch against `main`. Diff = `git diff main...HEAD` + `git diff` (working tree) + `git status --porcelain`. Capture current branch name via `git rev-parse --abbrev-ref HEAD`.
   - If `$ARGUMENTS` is a number (PR mode): use `gh pr diff $ARGUMENTS` for the diff and `gh pr view $ARGUMENTS --json headRefName,baseRefName,title` for metadata. **In PR mode the per-service generalists are SKIPPED** — they need access to the PR head-branch files, which would require switching the local working tree. Only the 5 global lenses run. To get the full review on a PR, run `gh pr checkout $ARGUMENTS` first and then invoke `/branch_review` with no args.
3. Detect touched services: any top-level directory at the repo root containing a `main.go` whose files appear in the diff. Build the list `TOUCHED_SERVICES`. Also note whether any `pkg/` files changed.
4. If `TOUCHED_SERVICES` is empty AND no `pkg/` files changed, print `No reviewable Go code changes detected` and stop.
5. Resolve today's date in `YYYY-MM-DD`. Compute the report path:
   - Branch mode: `docs/reviews/branch-<sanitized-branch>-<date>.md` (replace `/` in branch names with `-`).
   - PR mode: `docs/reviews/pr-$ARGUMENTS-<date>.md`.
   - If that file already exists, append `-2`, `-3`, etc. until you find a free name.

# Dispatch agents (parallel — single message, all Agent tool calls together)

**N per-service generalists** (one per touched service). Each must:
- Read `CLAUDE.md` first.
- Read that service's `main.go`, `handler.go`, `store.go`, and every other file in the directory.
- Read the diff scoped to that service.
- Judge, with `file:line` evidence:
  - (a) **Diff correctness** against the service's existing conventions
  - (b) **Scope drift / refactor-readiness** — has the service grown beyond its original purpose, are responsibilities still cohesive, does it need a split
  - (c) **Abstraction changes** — are new interfaces/types/helpers in the diff justified, are existing ones still earning their keep, any signs of premature abstraction
  - (d) **Design coherence** — does the diff fit the service's stated job or bolt on unrelated concerns
  - (e) **Project-pattern adherence** — uses `pkg/subject` builders (not raw `fmt.Sprintf`), `pkg/stream` configs (not inline), `pkg/idgen` for IDs, JetStream consumer pattern matches existing services (high-throughput: `cons.Messages()` + semaphore; sequential: `cons.Consume()`), cross-site events use the outbox pattern, new event structs in `pkg/model` have `Timestamp int64` set at the publish site.
  - (f) **Client-API doc rule** — if the diff modifies a handler whose subject begins with `chat.user.{account}.…` or `chat.user.{account}.room.{roomID}.{siteID}.msg.send`, OR any HTTP route in `auth-service`: verify `docs/client-api.md` is modified in the same diff. If not, emit a `critical` finding ("client-API doc not updated in same PR — CLAUDE.md Section 5 hard rule violation").
- Return ≤ 600 words, one section per concern. Tag each individual finding with `critical` / `high` / `medium` / `low` / `nitpick`.

**5 global expert lenses** over the full branch diff. Each returns ≤ 600 words with findings tagged with one of `critical` / `high` / `medium` / `low` / `nitpick`, each with `file:line` evidence. Agents may use `superpowers:requesting-code-review` patterns where relevant.

1. **Go expert** — idioms, error wrapping, naming, struct tags, CLAUDE.md Section 3 compliance, best practices a senior reviewer at a top Go shop would call out.
2. **Test-automation expert** — TDD compliance for new code, coverage of error paths, table-driven structure, mock vs integration choice, missing test cases, `-race` usage. **TDD heuristic**: for each new exported function in the diff, check there is a corresponding new test in `*_test.go` within the same diff. If not → `high` finding ("no test added for new code — TDD violation per CLAUDE.md Section 4"). **Mock staleness**: if the diff modifies any `store.go` interface, run `make generate` and check for resulting diff. If yes → `high` finding ("mocks stale; run `make generate`"). Revert any `make generate` diff after checking.
3. **Bug & security finder** — correctness bugs, race conditions, injection, leaked secrets, unsafe defaults, silently swallowed errors, missing input validation at boundaries. **Run `make sast` (gosec + govulncheck + semgrep)** and surface any medium+ findings introduced by this branch as `critical` (security-critical) or `high`. SAST is a blocking CI gate per CLAUDE.md Section 5.
4. **Performance expert** — hot-path allocations, N+1 DB queries, goroutine leaks, blocking calls in NATS handlers, missing batching/pagination, sync primitive choice, missing pagination/caching.
5. **Observability expert** — `log/slog` JSON usage, request-ID propagation via `context.Context`, OTel spans on new handlers, Prometheus metrics on new code paths, no logging of secrets or full payloads (CLAUDE.md Section 3 "Logging" + "Request Logging & Tracing").

# Synthesize the report — chapter by chapter, commit per chapter

Write the report **incrementally**. Each chapter is its own commit. Do not skip the commit step, do not batch chapters into one commit.

For each chapter:
1. Draft the chapter content from the relevant agent output.
2. Append to the report file (create the file on chapter 1).
3. `git add <report-path>`
4. `git commit -m "review(branch): <chapter-name>"` (use `review(pr-$ARGUMENTS): ...` in PR mode)

Chapter order:
1. **Header + executive summary** — branch name (or PR title + number), date, base branch, services touched, count of findings by severity (`critical` / `high` / `medium` / `low` / `nitpick`), top-line risk assessment.
2. **One chapter per touched service** — the per-service generalist's full output. Title each chapter `Service: <name>`.
3. **Go expert**
4. **Test-automation**
5. **Bug & security**
6. **Performance**
7. **Observability**
8. **Prioritized action list** — top 5–10 items across all sections, ordered by severity (`critical` first, then `high`, etc.) then impact ÷ effort. Each item: severity tag, action, file:line, why.

# Final chat output

After the last commit, print 5–10 lines, nothing else:
- Branch (or PR) + base
- Services touched (count + names)
- Counts: critical / high / medium / low / nitpick
- Path to the report file
