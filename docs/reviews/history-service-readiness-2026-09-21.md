# history-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1dfa2f3` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The read path is engineered with unusual care — every Mongo read projects, bucket walks are bounded and adaptive, errcode tiering is correct throughout, subjects come from `pkg/subject` and every publish stamps `Timestamp` — and four of six dimensions score 4. It still cannot merge under CLAUDE.md as it stands. Unit coverage is 55.1% because the entire store layer lives behind `//go:build integration` and CI never passes the tag, so 19 integration files (1,900 lines in `write_integration_test.go` alone) gate nothing; the same pipeline's Validate step runs `go build ./history-service/`, which has no Go files under the `cmd/`+`internal/` layout, so it either fails every run or is not gating at all. That layout is itself the only one in the tree and contradicts the CLAUDE.md sub-package exception that names this service as its exemplar. Two contract issues need a decision rather than a patch: `msg.thread` returns replies newest-first while both the canonical doc and the derived view promise oldest-first, and every post-commit canonical publish (edit, delete, pin, react) is fire-and-forget — a lost publish leaves fan-out, search and the preview silently stale with no OUTBOX-style retry. Maintainability is the soft spot: a 342-line hand-wired `main()`, a 9-method `RoomRepository` that leaks preview writes into every cache and breaker decorator, and config validation split across two packages.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 2 high, 17 medium, 26 low, 9 nitpick (55 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] CI Validate stage cannot build this service — `history-service/deploy/azure-pipelines.yml:47` — the step is `go build -o /dev/null ./$(SERVICE_NAME)/`, but the entrypoint is `cmd/main.go` and the service root holds no `.go` files. Ran `go build -o /dev/null ./history-service/` on this tree: `no Go files in .../history-service`. The line is a copy of the flat-layout template used by every other service; the sanctioned sub-package layout was adopted without adjusting it, so the pipeline either fails every run or is not actually gating.
- [medium] Log-AND-return on a client-facing handler path — `history-service/internal/service/reactions.go:58-59` — `slog.WarnContext(c, "react: actor not found", ...)` immediately followed by `return nil, fmt.Errorf(...)`; the router's `Classify` logs the returned error again, double-logging exactly what CLAUDE.md §6 "Never log AND return" forbids. The raw wrap also collapses a "subscribed account has no user doc" case to `internal`; if that state is reachable it deserves a typed errcode, if not the log is dead.
- [medium] Warn-level logs on the read hot path bypass context correlation — `history-service/internal/service/messages.go:406` (`readFloorInto`), `messages.go:749,754` (`publishCanonicalBestEffort`), `threads.go:374`, `migration.go:150,156` — ctx-less `slog.Warn` hands `context.Background()` to `logctx.Handler.Handle(ctx, r)` (`pkg/logctx/handler.go:68`), so the request-ID/log-values attached via `c.WithLogValues` (`pkg/natsrouter/context.go:187-188`) and the trace ID never reach these lines. `messages.go:406` and `threads.go:374` carry no `request_id` at all; the others hand-add `request_id` but still lose trace correlation. CLAUDE.md §3 requires the request ID "in all log lines".
- [low] Store methods return bare `err` — `history-service/internal/mongorepo/subscription.go:45` (`return nil, false, err`), `subscription.go:32-35` (`GetSubscription` passes `FindOne`'s error straight through), `mongorepo/app.go:31` (`return "", err`). Only `mongoutil.Collection.FindOne`'s generic "finding subscriptions: %w" (`pkg/mongoutil/collection.go:50`) survives; the method-level "what was I doing" wrap CLAUDE.md §3 mandates is absent, unlike the sibling `room.go:206,218` which wrap correctly.
- [low] Log-AND-return inside the scan helper — `history-service/internal/cassrepo/utils.go:224-226` — `structScan` builds the unmapped-column error, logs it at Warn, then returns it; every caller wraps and returns it upward to the router, which logs again.
- [low] Silently discarded close error — `history-service/internal/cassrepo/pin.go:347` — `_ = iter.Close()` on the scan-error path with no comment; CLAUDE.md §3 requires a comment when an error is intentionally dropped (contrast `rooms.go:137`, which does comment its discard).
- [nitpick] Misattached doc comment — `history-service/internal/readcache/readcache.go:139-148` — the paragraph describing `getOrLoad` (load semantics, cancellation behavior) sits directly above `func (c *ttlCache[V]) remove`, so godoc attaches it to `remove` and `getOrLoad` is undocumented.
- [nitpick] Dynamic slog key — `history-service/cmd/main.go:56` — `slog.Error("invalid config", c.name, c.value)` uses the env-var name as the attribute key, so the field name changes per failure; `"name", c.name, "value", c.value` keeps the schema stable for log queries.

### Recommendations

- [medium] Change the CI build step to `go build -o /dev/null ./$(SERVICE_NAME)/...` (or `./$(SERVICE_NAME)/cmd/`) — `history-service/deploy/azure-pipelines.yml:47` — restores a passing Validate stage; the Dockerfile at `deploy/Dockerfile:7` already builds `./history-service/cmd/`, so aligning on `/...` also future-proofs other sub-package services (`user-service` is flat today, but the template will bite the next one).
- [medium] Drop the `slog.WarnContext` at `reactions.go:58` and let the returned error carry the fact; if "actor not found for a subscribed account" is a legitimate state, return `errcode.NotFound(..., WithReason(...))` instead of a raw wrap — one log line, correct wire category.
- [medium] Switch the ctx-less `slog.Warn` calls to `slog.WarnContext(ctx, ...)` — `messages.go:406,749,754`, `threads.go:374`, `migration.go:150,156` — `ctx`/`c` is in scope at every site; this restores request-ID and trace correlation for free through `logctx` and removes the hand-rolled `request_id` fields.
- [low] Wrap at the store boundary — `mongorepo/subscription.go:32-35,45`, `mongorepo/app.go:31` — e.g. `fmt.Errorf("get history shared since for %s/%s: %w", account, roomID, err)`, matching `room.go`.
- [low] Remove the `slog.Warn` in `structScan` (`cassrepo/utils.go:225`) — the error already names the column and type and every caller propagates it.
- [low] Comment the discarded close at `cassrepo/pin.go:347` (`// scan error takes precedence; close error is secondary`) or fold it with `errors.Join`.
- [nitpick] Move the `getOrLoad` doc block below `remove` in `readcache.go:139-148`.
