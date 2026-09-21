# notification-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `2155c60` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.2, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

notification-worker's fan-out core is well engineered — error tiering, sonic on the hot path, projected reads, `jsretry.Settle` everywhere, and consumer-defined interfaces that make gates easy to swap — but four independent reviewers converged on the same shutdown bug and the same bootstrap defect. The ROOMS invalidation consumer goroutine is tracked by no WaitGroup, and shutdown calls `invalIter.Stop()` then immediately `close(invalCh)`, so a message already returned by `Next()` sends on a closed channel and panics, skipping the NATS drain and every database disconnect. The dev bootstrap hands `bootstrapStreams` the `.created` leaf instead of the `pkg/stream` schema, so with `BOOTSTRAP_STREAMS=true` a notification-worker restart narrows the shared MESSAGES-CANONICAL stream and history-service's edit/delete/pin publishes fail until another service re-widens it (production is unaffected). Beyond those: `PRESENCE_RPC_ENABLED=true` calls a snapshot RPC that no service serves, so DND suppression can never engage and the contracts doc still says "flip once live"; that same ops doc tells operators to provision a 5-minute duplicate window when the service refuses to start under 1 hour; the second consumer is hand-rolled inline in a 359-line `main()` bypassing `DurableConsumerDefaults`; per-message dependency timeouts sum well past `AckWait` with no overall deadline; `MONGO_URI` is defaulted while `WithDegradedStart` removes the fail-fast backstop; and the room-meta L2 tier is built with a nil breaker, unlike both sibling hot-path services. Coverage is 59.5% only because `main` holds 30% of the statements — everything else sits at 84.8% — but the consume loop, invalidation loop and shutdown sequence have no test at any tier, and the history parent fetcher is a verbatim copy of broadcast-worker's.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 3 high, 18 medium, 20 low, 10 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

