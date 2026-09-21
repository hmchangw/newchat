# room-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `421c933` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.3, Δ -0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The ROOMS consumer pattern, OUTBOX ordered-lane federation, `pkg/subject` usage, dedup-id helpers and shutdown order all match CLAUDE.md, and code quality is strong. The high finding is a client contract break: `publishAsyncJobResult` emits `status:"error"` for any non-nil error before `HandleJetStreamMsg` NAKs a transient one for retry, so a Mongo blip tells the client the job failed and minutes later that the same `requestId` succeeded — `docs/client-api.md` describes the result as terminal. Performance carries the real cost: `GetRoom`, `ListByRoom` and `GetSubscription` fetch whole documents (the first drags the room's private key on every DM redelivery), the Teams reconcile does a per-member `GetUser` and a per-member synchronous PubAck where a batch call already exists, no per-job deadline exists so a wedged Mongo call leaks a `MAX_WORKERS` slot, and the writer's own L1 room-meta cache is never invalidated after a rename. The ROOMS durable has no `FilterSubjects`, so every mute toggle from room-service is a wasted delivery plus a WARN. Coverage is 62.7%, and the `HandleJetStreamMsg` dispatcher — the only production entry point — is 0% at every layer.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 4 |
| Performance | 3 |

**Findings by severity:** 0 critical, 5 high, 22 medium, 19 low, 6 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

