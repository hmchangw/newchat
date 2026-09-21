# inbox-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1209ff9` (base `main`)  
**Overall score:** 2.7 / 5 (baseline 2026-09-01: 2.8, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

inbox-worker is the sole owner of the INBOX stream and the destination end of cross-site federation; its handler logic, settle/retry discipline and subject usage are clean, but three reviewers independently converged on the same two structural defects. First, `room_renamed` is routed to the concurrent pool rather than the sequential membership lane, so the FIFO ordering that the origin-side OUTBOX lane exists to preserve is discarded at the destination: a rename that lands before the `member_added` it depends on matches zero documents and is silently lost, and the new subscription is created with the stale name. Second, the membership lane channel is sized from the raw `CONSUMER_MAX_ACK_PENDING` env value (1024 floor) while the consumer itself is silently promoted to 10000 in the same default case, so the documented "dispatcher never blocks" invariant is false and a membership backlog of more than 1024 events stalls every other event type behind the single sequential Mongo writer. Around those, `FindUsersByAccounts` fetches whole user documents with no projection on the membership hot path, `BADGE_CACHE_TTL` is re-declared per service instead of owned by `pkg/badgecache`, the store implementation lives in `main.go` rather than `store_mongo.go`, the generated mock is dead in favour of a hand-written stub that has no error hooks for 14 store-error branches, and the unit profile sits at 48.1% because every store method is integration-only and CI never runs the integration tier. The per-process-only sequential lane also means the add/remove resurrection race returns at more than one replica, which nothing enforces.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 10 high, 15 medium, 18 low, 5 nitpick (49 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

