# search-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `6c6670e` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.3, Δ -0.3)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

search-service is a tidy NATS request/reply front end over Elasticsearch whose enrichment, caching and admission-control layers are well built, but it ships two client-facing prototypes as if they were finished: `search.users` proxies to a third-party HR endpoint whose path, body and response shape are admitted guesses (three `TODO(searchUsers-thirdparty)` markers, yet `USERS_API_URL` is required and the RPC is registered and documented), and `search.apps` discards the caller's `account` so every name-matching app is returned unscoped, with the planned fix written as two `$lookup` stages that CLAUDE.md forbids. The `search.apps` aggregation is also the only Mongo read without a projection. Four reviewers independently flagged that the per-request timeout is declared twice (`SEARCH_REQUEST_TIMEOUT` and the router guard's `REQUEST_TIMEOUT`, both 10s, both applied), that the `RoomInfoClient` is poked into the handler after construction, and that the service creates indexes on `apps` and `subscriptions` collections other services own while treating `users` as verify-only. On the performance side the message-search body grows unbounded with the caller's restricted-room count and client-supplied `roomIds`, `offset` is never capped against ES's 10 000 result window, and the restricted-rooms access map sits in Valkey for five minutes with no invalidation path. Coverage is 68.9% overall and 79.5% under the pipeline's own `main.go` filter, so the service misses its own 80% gate by half a point; the generated mocks are dead code in favour of hand-written fakes, and `room_client.go` has no test of any kind.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 5 high, 22 medium, 22 low, 7 nitpick (56 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.

