# admin-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `8eaf4a7` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.2, Δ +0.1)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

A well-structured admin HTTP surface — routes in `routes.go`, consumer-defined store, explicit projections on every read, no `$lookup`, streamed uploads with body caps, the documented HTTP shutdown order — with one high-severity security gap that two reviewers reached independently: password reset, self-service password change and account deactivation revoke sessions through a hand-rolled `DeleteMany` inside the transaction that returns no ids, so unlike the explicit revoke endpoints they never call `sessioncache.Bust*`, and a reset or deactivated user's token keeps authenticating from the consumer-side cache for the refresh window (~67 min at defaults, indefinitely during a Mongo outage). Two operational gaps follow it: `authenticate` collapses every `FindByHash` error to 401, so a Mongo outage logs the whole console out and pages as auth failures, and `user_account_updated`/`user_permissions_updated` publish straight to remote INBOX lanes with no OUTBOX retry, so a permission revoke that misses a down peer stays effective there until an operator runs resync. `SearchUsers` executes two unanchored case-insensitive regex scans over every site's users per console keystroke, with no sort, so pages repeat rows under HR-sync writes. Coverage is 68.2% — 93% outside the integration-only store — but five handlers lack the malformed-body test CLAUDE.md requires and nothing enforces the floor in any pipeline.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 12 medium, 21 low, 9 nitpick (46 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] Password reset / self change-password / deactivate revoke sessions in Mongo but never bust the session cache — `admin-service/handler.go:701`, `admin-service/login.go:201`, `admin-service/handler.go:483` — `UpdateUserPasswordAndRevoke` and `DeactivateAndRevoke` (`store_mongo.go:376`, `:402`) `DeleteMany` the shared sessions collection inside the transaction and return no ids, so unlike `revokeAllSessions`/`revokeSession` (`handler.go:599`, `:620`) no `sessioncache.Bust*` follows. `pkg/sessioncache/sessioncache.go:119-121` and the service's own `revokebust_test.go:27-32` state the consequence: a revoked token keeps authenticating from cache (~67 min at the default refresh window, indefinitely during a Mongo outage) at the consumer that caches sessions (`botplatform-service/store_mongo.go`). A forced reset or account deactivation is exactly the case where that matters.
- [medium] Any `FindByHash` failure collapses to 401 `invalid session token` — `admin-service/middleware.go:424-430` — `session.ErrNotFound` exists (`pkg/session/session.go:49`) precisely so "a caller cannot mistake an outage for an invalid token" (`pkg/sessioncache/sessioncache.go:135-137`), but the middleware never `errors.Is`-checks it. A Mongo outage therefore logs every admin request at INFO as an auth failure (`Classify` logs `Unauthenticated` at Info, `pkg/errcode/classify.go:44-50`) and the console logs everyone out. `middleware_test.go:87-91` only pins the `ErrNotFound` path, so the outage behaviour is not a deliberate contract.
- [low] Request ID reaches the `Classify` boundary log in only two handler files — `admin-service/login.go:59`, `admin-service/room_onduty.go:349` seed `errcode.WithLogValues(ctx, "request_id", …)`; every handler in `handler.go` (e.g. `:297`, `:379`, `:670`), `permissions.go:139`, `rooms.go:261`, `client_update.go:296` use the bare request ctx. `ginutil.RequestID` stores the id in ctx (`pkg/ginutil/middleware.go:24-25`) but nothing in `pkg/obs` copies it onto slog lines, so "request failed" lines from most endpoints carry only the trace id, contrary to CLAUDE.md "include in all log lines".
- [low] Bare `err` returns — `admin-service/config.go:278`, `admin-service/store_mongo.go:346`, `admin-service/store_mongo.go:409` — `return Config{}, err` / `return err` / `return nil, err` with no context, against the CLAUDE.md "never return bare err" rule (callers wrap, so no information is lost, but the pattern is inconsistent with the rest of the file).
- [low] `run()` calls `slog.Error` + `os.Exit(1)` instead of returning — `admin-service/main.go:79-81` — the only exit inside `run()`; it bypasses the `run() error` contract and skips `obsShutdown`, whereas the neighbouring config/connect failures return wrapped errors.
- [low] Outbound HTTP drives `net/http` directly — `admin-service/client_update.go:39-43`, `:81`, `:90` — CLAUDE.md says "never `net/http` client directly". The deviation is justified inline (resty v2 buffers non-replayable bodies) and the transport still comes from `restyutil`, but the exception lives only in a comment; nothing in `CLAUDE.md` or `pkg/restyutil` sanctions it, so a future lint/semgrep rule on `http.NewRequestWithContext` would flag it.
- [nitpick] `nowMillis` is a mutable package-level var "so tests can stub" — `admin-service/handler.go:77-79` — no test references it (grep across `admin-service/*_test.go`: zero hits), and sibling code uses `time.Now().UTC()` directly (`permissions.go:176`, `handler.go:266`). Dead indirection plus a global that would race if tests ever went parallel.
- [nitpick] Same failure class logged at different levels — audit append failure: Warn at `login.go:216`, Error at `handler.go:267` and `permissions.go:286`; INBOX publish failure: Warn at `handler.go:215`, Error at `permissions.go:384`. Alert rules keyed on level will see half the events.
- [nitpick] `fmt.Errorf` with constant strings — `admin-service/room_onduty.go:411`, `:428` — `errors.New` is the idiom; harmless but inconsistent with `permissions.go:86`.

### Recommendations

- [high] Return the deleted session `_id`s from `UpdateUserPasswordAndRevoke`/`DeactivateAndRevoke` (find-then-DeleteMany inside the same transaction, as `session.MongoStore.DeleteBeyondCap` does) and call `sessioncache.BustMany` at `handler.go:701`, `handler.go:483`, `login.go:201` — closes the revoked-token window the service already closes for explicit revokes; add a `revokebust_test.go` case per path.
- [medium] In `authenticate` (`middleware.go:424`) branch on `errors.Is(err, session.ErrNotFound)` → 401; anything else → `fmt.Errorf("find session: %w", err)` so it classifies as `internal` (ERROR log, 500) — an outage then looks like an outage; add a table row with a non-sentinel error to `middleware_test.go`.
- [low] Seed `request_id` once, either in `applyBaseMiddleware` (`main.go:42`) by wrapping the request ctx with `errcode.WithLogValues` after `ginutil.RequestID()`, or as a `ginutil` middleware shared by all Gin services — removes the per-handler drift and makes every `Classify` line correlatable.
- [low] Wrap the three bare returns (`config.go:278`, `store_mongo.go:346`, `:409`) with `%w` context, and make `main.go:79` `return fmt.Errorf("parse mongo read preference: %w", err)`.
- [low] Either promote the streaming-upload exception into `pkg/restyutil` (e.g. a `StreamingClient` helper) or record it in CLAUDE.md next to the Resty rule so the `net/http` usage in `client_update.go` is a sanctioned pattern rather than a per-file comment.
- [nitpick] Drop `nowMillis` in favour of `time.Now().UTC().UnixMilli()` (or actually stub it in tests), and align the audit/publish failure log levels to one choice per class.

## 3. Architecture — score 3

### Evidence

- [high] Transactional session revokes bypass `pkg/session` and drop the cache-bust contract, so revoked tokens keep authenticating from cache — `admin-service/store_mongo.go:375` and `:401` — `UpdateUserPasswordAndRevoke`/`DeactivateAndRevoke` open `session.Collection` directly with a hand-copied filter and `DeleteMany`, returning no IDs. `pkg/session/session.go:36-44` exists precisely so callers can hand deleted `_id`s to `sessioncache.BustMany`; the only bust sites are `handler.go:599`, `:620` and `login.go:130`. The admin password reset (`handler.go:701`), deactivation (`handler.go:483`) and self change-password (`login.go:201`) never bust, so a deactivated user's token works until the cache refresh window elapses (indefinitely during a Mongo outage, per `revokebust_test.go:27-32`). Same-client driver-v2 transaction ctx would let the `session.Store` methods join the transaction; the duplication was unnecessary.
- [medium] Cross-site permission and account replication publishes straight into remote INBOX lanes, skipping the OUTBOX durability lane — `admin-service/permissions.go:382`, `admin-service/handler.go:214` — `h.publish` is a bare `js.PublishMsg` (`main.go:110-115`). A failed gateway publish is reported as `syncFailures` and depends on a human calling `/permissions/resync`; a permission *revoke* that fails to land stays effective at the remote site until then. CLAUDE.md routes `room-service`'s request/reply cross-site events through OUTBOX so exactly this is durably retried. `InboxUserPermissionsUpdated`/`InboxUserAccountUpdated` are in neither `pkg/outbox` partition (`pkg/outbox/outbox.go:22-58`).
- [medium] Request ID reaches `Classify`'s "request failed" log line in only three of eighteen handlers — `admin-service/login.go:59`, `:164`, `admin-service/room_onduty.go:47` seed `errcode.WithLogValues(... "request_id")` by hand; every other handler passes `c.Request.Context()` bare. `errhttp.Write` → `Classify` logs via `loggerFrom(ctx)` (`pkg/errcode/classify.go:40`) and `ginutil.RequestID` (`pkg/ginutil/middleware.go:22-25`) never seeds that logger, so most error lines lack the correlation id CLAUDE.md requires on all log lines. This is an entry-point concern done per-handler.
- [low] `os.Exit(1)` inside `run()` — `admin-service/main.go:80` — every other startup failure returns an error through `run() error` (`main.go:26-29`); this one exits directly, skipping `obsShutdown` and the single exit path.
- [low] Optional Valkey dependency is assigned post-construction despite an option mechanism existing — `admin-service/main.go:125` sets `h.valkey` while `handlerOption`/`withVersionUploader` (`handler.go:70-75`) is the constructor-DI seam added for exactly this; tests mirror the mutation (`revokebust_test.go:23`).
- [low] `readyz` reflects Mongo only — `admin-service/handler.go:657-665` — the duty toggle (`room_onduty.go:94`) and every fanout depend on NATS; a lost NATS connection leaves the pod Ready. `nc.IsConnected()` is a free check.
- [low] `http.Server` has no `ReadHeaderTimeout`/`IdleTimeout` — `admin-service/main.go:142-147` — idle keep-alive connections never expire server-side; `upload-service/main.go:205` and `search-service/main.go:315-318` set both.

### Recommendations

- [high] Make the two transactional store methods return deleted session IDs (or take a `session.Store` and call `DeleteForAccount`/`DeleteForAccountExcept` under the transaction ctx) and `sessioncache.BustMany` at `handler.go:483`, `:701` and `login.go:201` — closes the stale-token window; add a `revokebust_test.go` case per path.
- [medium] Add `InboxUserPermissionsUpdated`/`InboxUserAccountUpdated` to `pkg/outbox.ConcurrentEventTypes` and publish via `outbox.Publish` — `permissions.go:382`, `handler.go:214` — turns manual resync into at-least-once delivery; keep `syncFailures` only for the local OUTBOX write.
- [medium] Seed the errcode logger once in `applyBaseMiddleware` (or `ginutil.RequestID`) with `errcode.WithLogValues(ctx, "request_id", id)` and delete the three per-handler copies — `main.go:42-48`, `login.go:59`, `:164`, `room_onduty.go:47`.
- [low] Replace `os.Exit(1)` with `return fmt.Errorf("parse mongo read preference: %w", err)` — `main.go:80`.
- [low] Add `withValkey(vk)` as a `handlerOption` and pass it in `newHandler` — `main.go:117-128`.
- [low] Check `nc.IsConnected()` in `readyz` alongside the Mongo ping — `handler.go:657`.
- [low] Set `ReadHeaderTimeout: 5s` and `IdleTimeout: 60s` on the server — `main.go:142`.

## 4. Test coverage — score 2

### Evidence

- [high] coverage below repo minimum 80%, currently 68.2% — `admin-service/store_mongo.go:27` — 754/1106 statements. The shortfall is structural: `store_mongo.go` is 0/227 (every method has an integration test, but `//go:build integration` keeps them out of the unit profile) and `main.go:50` `run` is 0/71. Excluding those two files the rest is 749/803 = 93.3%.
- [high] Five handlers have no malformed-body test, which CLAUDE.md §4 requires for every handler — `admin-service/handler.go:383` — the `ShouldBindJSON` error branches at `handler.go:383` (createUser), `handler.go:456` (updateUser), `handler.go:674` (setPassword), `permissions.go:143` (createPermissions) and `permissions.go:462` (resyncPermissions) are all 0 in the profile; every test body is a marshaled `map[string]any`, so bind never fails. Only setRoomOnDuty covers it (`room_onduty_test.go:187-212`).
- [medium] Reachable store-error / not-found branches are untested — `admin-service/handler.go:428` — createUser's non-`ErrAccountExists` store error → 500 (`handler.go:428`); updateUser's `DeactivateAndRevoke` → `ErrUserNotFound` → 404 (`handler.go:485`); setRoomOnDuty `reply == nil` → 500 (`room_onduty.go:108`, `fakeRoomRPC` can return `reply:nil, err:nil` but no test does); createPermissions' `seen` guard for an applicant/approver that repeats a subject (`permissions.go:213`) — the behaviour the comment documents has no test.
- [medium] The 80% floor is not enforced anywhere and the service pipeline never runs the store — `admin-service/deploy/azure-pipelines.yml:687` — that step runs `go test ./admin-service/...` with no `-tags integration`, so `store_mongo.go` is never executed there; it writes `coverage-admin-service.out` and nothing reads it. `make test` (`Makefile:114-118`) has no `-coverprofile`/threshold either; `tools/coveragecheck` is wired only for loadgen (`Makefile:153-161`). Integration runs only in `.github/workflows/ci.yml:296-332`, and only for affected targets.
- [low] Order-coupled integration subtests — `admin-service/integration_test.go:505-514` — "DeleteForAccount removes all" relies on the previous subtest having deleted hash-a ("hash-b remains at this point"); `TestIntegration_UpdateUser` subtests (`integration_test.go:208-267`) mutate the same seeded user in sequence. Running one subtest via `-run` sees a different fixture. CLAUDE.md test-independence rule.
- [low] Timing-sensitive tests run in the default `make test` (no `-short`) — `admin-service/login_test.go:1336` — asserts a ≤3x spread across three bcrypt-cost-10 requests; `client_update_test.go:649-669` (200ms budget vs 700ms uploader), `:907-928` (300ms stalled body), `:752-785` (48 MiB relay must stay under a 24 MiB HeapAlloc peak sampled on a 10ms ticker, `pkg/testutil/memtest.go:71`). All green today, all load-sensitive.
- [low] `parsePaging` invalid input is never asserted — `admin-service/handler.go:279-291` — no row sends `page=0`, `page=abc` or `limit=-1`; the fall-through to defaults is only hit implicitly. `main.go:77-81` calls `os.Exit(1)` inside `run()`, making that config branch untestable.
- [nitpick] httptest handlers write plain locals that the test reads after `Do` returns — `admin-service/client_update_test.go:155-160` (also `:186-190`, `:727-729`) — while `fakeUploader` guards the same pattern with a mutex (`:298-300`). Not a happens-before edge the race detector tracks.
- [nitpick] `TestLoginAndChangePasswordEndToEnd` — `admin-service/integration_test.go:1306` — lacks the `TestIntegration_` prefix every sibling uses.

### Recommendations

- [high] Add a raw `{"broken":` row to each of the five handler tables — `handler_test.go:110`, `:561`, `:1190`, `permissions_test.go:212`, `:1135` — closes the §4 MUST gap in ~25 lines; tables need a `rawBody string` field or a small `doRaw` helper like `room_onduty_test.go:96`.
- [high] Add the four missing branch rows: createUser generic store error → 500, updateUser deactivate → 404, setRoomOnDuty `reply:nil`, createPermissions applicant==subject/approver — `handler.go:428`, `:485`, `room_onduty.go:108`, `permissions.go:213` — each is a real production path with a distinct wire outcome.
- [medium] Enforce the floor: add `-coverprofile` plus `go run ./tools/coveragecheck -min 80` to `make test` or `ci.yml` — `Makefile:114-118` — and decide explicitly how integration coverage counts: either merge a `-tags integration` profile (as `data-migration/oplog-connector/deploy/azure-pipelines.yml:48` already does) or exclude `store_mongo.go`/`main.go` with a written rationale. Today the number is unenforced and misleading.
- [medium] Run the store in the service pipeline: add `-tags integration` (or a second job) to `admin-service/deploy/azure-pipelines.yml:687` — otherwise a `store_mongo.go` regression only surfaces in the GitHub matrix when the target is "affected".
- [low] Make integration subtests self-contained — `integration_test.go:420-525`, `:191-268` — seed a distinct account per subtest so any `-run` subset sees the fixture it expects.
- [low] Gate the timing tests — `login_test.go:1266`, `client_update_test.go:752` — run `make test` with `-short`, or widen the tolerances; a CI-load flake here blocks every unrelated PR.
