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


## 2. Code quality — score 3

### Evidence

- [high] `search.apps` aggregation has no projection — `search-service/query_apps.go:51-55` — Pipeline is `$match → $skip → $limit`; `SearchAppsByName` (`store_mongo.go:146-154`) decodes whole `apps` documents. CLAUDE.md §6: "every find/aggregation MUST specify an explicit projection". The other three Mongo reads (`store_mongo.go:61,89,116`) do project.
- [medium] Degraded-path WARN logs are emitted without context, so they carry neither `request_id` nor trace correlation — `handler.go:261`, `handler.go:291`, `enrich.go:44`, `enrich.go:91`, `enrich.go:100`, `enrich.go:173` — All six use `slog.Warn` instead of `slog.WarnContext(ctx, …)`. The o11y base handler derives trace IDs from ctx (`o11y/internal/log/handler.go:71`); `logEmptyResult` (`handler.go:194-204`) shows the intended pattern. CLAUDE.md §3: include the request ID "in all log lines". These fire precisely during Valkey/Mongo/room-service brown-outs.
- [medium] Live `search.apps` RPC accepts `account` and discards it; access guard unimplemented — `query_apps.go:49` (`_ = account`) — Every name-matching app is returned to any caller. Documented as "Planned behavior" (`docs/client-api.md:4419`) but registered (`handler.go:76`). The TODO also plans two `$lookup` stages, forbidden by CLAUDE.md §6 without a documented exception. Security dimension should weigh the authorization impact.
- [medium] `search.users` ships a placeholder wire contract on a live RPC — `users_client.go:21-23`, `:40-43`, `:59-60` — Three `TODO(searchUsers-thirdparty)` markers: body, path (`/search`) and response shape are guesses, yet `USERS_API_URL` is `required` (`main.go:58`) and the handler is registered (`handler.go:77`).
- [low] Generated mockgen mocks are dead code; tests use hand-rolled fakes — `mock_store_test.go` (10 `NewMock*` ctors, referenced by no other file); `handler_test.go:25-60` (`fakeStore`), `:644-660` (`fakeUsers`) — CLAUDE.md §4 says mock with `go.uber.org/mock`; the 291-line file is regenerated for zero consumers.
- [low] `RoomInfoClient` injected by field assignment after construction — `main.go:289` (`handler.room = newRoomClient(nc)`) vs `handler.go:57` — CLAUDE.md §3: dependencies injected via constructor; `enrich.go:158` then nil-guards `h.room`.
- [low] `newHandler` mutates the caller's `*handlerConfig` while defaulting — `handler.go:57-70` — Defaults are written through the pointer, then the struct is copied; the caller's config silently changes.
- [low] Redundant `request_id` re-passed to `WithLogValues` in every handler — `handler.go:95,147,218,304,340` — `natsrouter.RequestID()` already sets it (`pkg/natsrouter/middleware.go:52`, doc: "handlers don't need to re-pass it").
- [nitpick] Dead `errors.As` special-case — `handler.go:164-171` — `errcode.Classify` walks the chain (`pkg/errcode/classify.go:17`), so a plain `%w` wrap classifies identically.
- [nitpick] Bare `return err` pass-through — `metrics.go:157-160` — CLAUDE.md §3 says never return bare `err`; harmless since the callee wraps, but inconsistent with the file.

### Recommendations

- [high] Add a terminal `$project` limited to the fields `SearchAppsResponse` serialises — `query_apps.go:51-55` — MUST compliance; stops shipping full app documents (assistant config etc.) over the wire.
- [medium] Implement the `search.apps` access guard as a separate `subscriptions` query rather than the planned `$lookup` pair — `query_apps.go:38-49` — Closes the caller-scoping gap without a forbidden server-side join.
- [medium] Switch the six degraded-path warnings to `slog.WarnContext(ctx, …)` and drop the manual `request_id` args — `handler.go:261,291`, `enrich.go:44,91,100,173` — Restores trace/request correlation on the lines that matter during outages.
- [medium] Finish `httpUsersClient` against the real third-party spec, or gate `search.users` registration behind a flag until it exists — `users_client.go`, `handler.go:77` — A guessed contract on a required URL fails at first call looking like an upstream outage.
- [low] Pick one test-double strategy: consume the mockgen mocks in `handler_test.go`, or delete the `//go:generate` directive and the generated file — `store.go:11`, `mock_store_test.go`.
- [low] Inject `RoomInfoClient` through `newHandler` and take `handlerConfig` by value — `handler.go:57`, `main.go:289`.
- [low] Convert the ten `TestHandler_SearchApps_*` and eight `TestHandler_SearchRooms_*` single-scenario functions to table-driven subtests — `handler_test.go:495-620` — Only 2 `t.Run` across 986 lines.
