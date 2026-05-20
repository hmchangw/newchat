---
description: Multi-expert production-readiness audit of a Go service
argument-hint: <service-directory-name>
---

You are running the production-readiness audit for the service named `$ARGUMENTS`.

# Preconditions

1. If `$ARGUMENTS` is empty, print `Usage: /production_readiness <service-directory-name>` and stop.
2. Verify `$ARGUMENTS` is a top-level directory in the repo. If not, list candidate service directories (top-level dirs containing `main.go`) and stop.
3. Verify the working tree under `docs/reviews/` is clean (no uncommitted changes there). If dirty, ask the user to commit or stash those changes first. Other uncommitted changes are fine.
4. Resolve today's date in `YYYY-MM-DD`. Compute the report path: `docs/reviews/$ARGUMENTS-readiness-<date>.md`. If that file already exists, append `-2`, `-3`, etc. until you find a free name.

# Dispatch experts (parallel — single message, six Agent tool calls)

Each agent must:
- Read `CLAUDE.md` (project guidelines) before judging anything.
- Read the service directory `$ARGUMENTS/` in full.
- Read the `pkg/` subpackages the service imports.
- Score its dimension on a 1–5 scale.
- Return: score, top findings with `file:line` evidence, and 3–7 concrete recommendations. Each individual finding and recommendation is tagged with one of `critical` / `high` / `medium` / `low` / `nitpick`. ≤ 800 words.
- Agents may use `superpowers:requesting-code-review` patterns where relevant.
- Judge against industry best practices for Go microservices at scale (what a senior reviewer at a top Go shop would call out). Do not invoke Uber/ByteDance internal source — those are not accessible.

The 6 experts:

1. **Go code quality** — idioms, error wrapping (`fmt.Errorf("...: %w", err)`), naming, struct tags, package layout, CLAUDE.md Section 3 compliance. **Also run `make sast` (gosec + govulncheck + semgrep) against the repo** and fold any medium+ findings touching `$ARGUMENTS/` into this dimension as `high` (or `critical` for security-critical). SAST is a blocking CI gate per CLAUDE.md Section 5.
2. **Architecture** — boundaries, interfaces defined in consumer, dependency injection, handler/store/main separation. NATS subject design uses `pkg/subject` builders (never raw `fmt.Sprintf`), stream configs come from `pkg/stream`, `BOOTSTRAP_STREAMS` opt-in convention is respected (services must not create streams in production), correct JetStream consumer pattern (`cons.Messages()` + semaphore for high-throughput, `cons.Consume()` for sequential), outbox/inbox federation pattern with `inbox-worker` as the sole INBOX owner.
3. **Test coverage** — TDD compliance, unit coverage of error paths, integration test depth, table-driven structure, mock vs real-dependency choice. Run `make test SERVICE=$ARGUMENTS` to verify tests pass; then run `go test -coverprofile=/tmp/cov.out ./$ARGUMENTS/...` and `go tool cover -func=/tmp/cov.out` to compute numeric coverage (read-only audit step). **Apply the 80% coverage floor from CLAUDE.md Section 4: if coverage <80%, floor this dimension at score 2 and emit a `high` finding (`coverage below repo minimum 80%, currently X%`); if <60%, emit `critical` and floor at score 1.** Also run `make generate` and check for resulting diff — if mocks regenerate, emit a `high` finding ("mocks are stale; run `make generate`"). Revert any `make generate` diff after checking.
4. **Maintainability** — file size and complexity, ease of adding a new feature, clarity of responsibilities, signs the service is refactor-ready (grown past its original purpose, leaky abstractions, dead code).
5. **Integration** — NATS subject usage matches `pkg/subject` builders (never raw `fmt.Sprintf`), JetStream consumer pattern correctness, cross-service contracts via `pkg/model` event structs (each must have `Timestamp int64` set at publish site), outbox subject pattern `outbox.{siteID}.to.{destSiteID}.{eventType}`, INBOX sourcing only owned by `inbox-worker`, bucketed Cassandra tables aligned on `MESSAGE_BUCKET_HOURS`, IDs generated via `pkg/idgen` (not ad-hoc). If the service has client-facing handlers (subjects matching `chat.user.{account}.…` or HTTP routes in `auth-service`), `docs/client-api.md` must document them — flag mismatches as `high`.
6. **Performance** — hot-path allocations, N+1 queries, goroutine leaks, blocking calls in NATS handlers, missing batching/pagination, sync primitive choice, caching opportunities.

# Synthesize the report — chapter by chapter, commit per chapter

Write the report **incrementally**. Each chapter is its own commit. Do not skip the commit step, do not batch chapters into one commit.

For each chapter:
1. Draft the chapter content from the relevant expert output.
2. Append to the report file (create the file on chapter 1).
3. `git add docs/reviews/$ARGUMENTS-readiness-<date>.md`
4. `git commit -m "review($ARGUMENTS): <chapter-name>"`

Chapter order:
1. **Header + executive summary** — service name, date, current branch, overall score (avg of 6), one-paragraph TL;DR, table of dimension scores, count of findings by severity (`critical` / `high` / `medium` / `low` / `nitpick`).
2. **Code quality**
3. **Architecture**
4. **Test coverage**
5. **Maintainability**
6. **Integration**
7. **Performance**
8. **Prioritized action list** — top 5–10 actions across all dimensions, ordered by severity (`critical` first) then impact ÷ effort. Each item: severity tag, action, dimension, file:line, why.

Each dimension chapter contains: score, evidence (`file:line`), recommendations.

# Final chat output

After the last commit, print 5–10 lines, nothing else:
- Overall score
- Highest-risk dimension and one-line summary
- Path to the report file
