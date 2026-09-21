# teams-room-inspector — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1f1d314` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.3, Δ +0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-inspector` is the smallest and cleanest service in this audit: nine files, 774 lines, one read-only HTTP endpoint that answers `teams-room-verify` with "does this room exist and how many subscriptions does it have". It scores 4 on five of six dimensions — every one but coverage — with zero TODOs, no function over 51 lines, correct `pkg/errcode` Tier-1 usage behind a single `errhttp.Write` adapter, explicit projections, no `$lookup`, a request-body cap, and exactly two Mongo queries for any batch size up to the 500-id cap. `handler.go`, `routes.go` and `newServer` are at 100% coverage. Its 3.2 is almost entirely a coverage artefact plus a set of shared-knob omissions.

The one finding every reviewer raised independently is the **room-id derivation, duplicated by hand across two binaries**. Both `handler.go:68` and `room-worker/teamsroomcreate.go:62` inline `idgen.DeterministicID([]byte(chatID))`, and both carry a comment saying the two must be kept in step by hand. The repo already has the right pattern next door — `pkg/teamsmigrate.EmployeeIDFromGraphID` wraps exactly this call so HR-sync and migration cannot drift. Drift here is silent and self-sustaining: every chat comes back `missing_room`, `teams-room-verify` logs mismatches forever, `needVerify` never clears, and the flagged set grows without bound while both jobs report healthy runs. The existing test cannot catch it, because it recomputes the expected id with the same call rather than pinning a literal.

Three shared knobs the rest of the fleet mounts are missing here, and they compound into the same failure. `mongoutil.PoolConfig` is not mounted, so the client keeps the driver's 30-second server-selection default instead of the repo's 2 seconds — a quiet Mongo becomes a 30-second hang rather than a fast, reportable failure. `ginutil.TimeoutConfig` is not wired either, so the handler context has no deadline at all: `http.Server`'s Read/WriteTimeout bound the socket, not the query. Together, an unreachable secondary pins every handler goroutine and its pooled connection for the caller's whole 30-second budget, and since the caller runs 8 concurrent batches, one slow site consumes an entire verify pass. Five sibling Gin services mount both.

Two contract gaps worth noting. The store filters on `_id`/`roomId` only, although the service is configured with `SITE_ID` and rooms are replicated to non-owning sites carrying the origin site's id — so a chat asked at the wrong site is answered `roomExists: true` with a partial subscription count rather than "not here", and the misroute guard is advisory only. And neither end of the only service-to-service hop in the pipeline propagates `X-Request-ID` or `traceparent`, so `ginutil.RequestID` mints a fresh id per call and a mismatch logged by the verifier can never be joined to the inspector's access log for the same batch. The inspector's extraction side is correct; what is missing is the caller's half of the contract.

Coverage is 47.7%, the lowest of the Teams family, and all 46 uncovered statements sit in `main.go` (29) and `store_mongo.go` (17). The store's tests exist, are fully compliant, and run in no pipeline. `run()` is 10.3% because the dial/serve/shutdown wiring was never given the seam that `newServer` was — extracting it closes roughly 26 of the 46 and lifts the package over the floor on its own.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 1 critical, 2 high, 14 medium, 13 low, 7 nitpick (37 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] The chat-id → room-id derivation is copy-pasted across two services with no shared helper — `teams-room-inspector/handler.go:68` and `room-worker/teamsroomcreate.go:62` — both call `idgen.DeterministicID([]byte(chatID))` inline, and the comments on both sides (`handler.go:44-46`, `teamsroomcreate.go:56-59`) state the two "must be changed together" by hand. `pkg/teamsmigrate` already hosts exactly this kind of derivation for the sibling case (`EmployeeIDFromGraphID`, `pkg/teamsmigrate/teamsmigrate.go:62-67`), so the right home exists and was not used. A divergence is silent: every chat reports `roomExists:false` and the verifier just logs mismatches forever.
- [medium] No request deadline on the Mongo path — `teams-room-inspector/handler.go:71` passes `c.Request.Context()` (no timeout) into a find plus an aggregate over up to 500 ids (`store_mongo.go:47,56`). `newServer` wires no `ginutil.Timeout`/`TimeoutConfig` (`main.go:50-57`) although that shared knob exists and peers mount it (`tcard-service/main.go:120`, `botplatform-service/main.go:124`), and `mongoutil.ConnectRead` sets no client operation timeout (`pkg/mongoutil/mongo.go:271-284`). `http.Server.WriteTimeout` closes the connection but does not cancel the query, so a stalled secondary parks goroutines and connections for the whole 25s drain.
- [low] The `errcode` log line carries no request id — `handler.go:53,57,61,73` call `errhttp.Write`, which runs `errcode.Classify`; that logs through `loggerFrom(ctx)` = `slog.Default()` (`pkg/errcode/classify.go:40`) and nothing put `request_id` on that logger. Peers call `errcode.WithLogValues(ctx, "request_id", c.GetString("request_id"))` at handler entry (`auth-service/handler.go:136`, `botplatform-service/handler.go:89`). CLAUDE.md §3 asks for the correlation id in all log lines; here `request failed` and the access log line (`pkg/ginutil/middleware.go:67-74`) must be joined by trace id instead.
- [low] Sentinel compared with `!=` instead of `errors.Is` — `main.go:115` (`err != http.ErrServerClosed`). Not a live bug (`ListenAndServe` returns it unwrapped) and the repo is split, but the newer peers this file was modelled on use `errors.Is` (`admin-service/main.go:174`, `media-service/main.go:143`); `errorlint` is not enabled in `.golangci.yml`, so nothing catches a future wrapping change.
- [nitpick] The bind error is discarded and every rejection collapses to one message — `handler.go:52-55` — `err` from `ShouldBindJSON` is never used and no `errcode.WithCause(err)` is attached, so a malformed body, a wrong type and a `MaxBytesReader` cutoff all log and return the identical `decode verify request`. Discarding it matches house style (`admin-service/permissions.go:143`), but here it is the only diagnosis the caller's CronJob gets.
- [nitpick] An oversized body answers 400, not 413 — `handler.go:50-55` — `http.MaxBytesReader` is correctly installed, but its error is indistinguishable from a parse failure at the `ShouldBindJSON` boundary, and `handler_test.go:120` pins 400. A `*http.MaxBytesError` check would let the caller tell "batch too big" from "bad JSON".

### Recommendations

- [medium] Move the derivation into `pkg/teamsmigrate` as `RoomIDFromChatID(chatID string) string` and call it from both `handler.go:68` and `room-worker/teamsroomcreate.go:62` — turns a hand-maintained cross-service invariant into a single function with one test, beside the sibling `EmployeeIDFromGraphID`.
- [medium] Mount the shared HTTP timeout knob (`ginutil.TimeoutConfig` field + `cfg.HTTP.Middleware()`) in `newServer` — `main.go:50-57` — bounds the verify handler so a slow secondary sheds load instead of accumulating goroutines; keep it under the 30s `WriteTimeout`.
- [low] Add `ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))` at the top of `HandleVerify` — `handler.go:48` — one line, matches auth/botplatform, and makes the `request failed` record self-correlating.
- [low] Attach the bind error as a server-only cause: `errcode.BadRequest("decode verify request", errcode.WithCause(err))` — `handler.go:53` — the cause is never serialized (`pkg/errcode/error.go`, `classify.go:19-21` logs it as `underlying`), so the operator learns why a batch 400'd at no client-exposure cost.
- [low] Use `errors.Is(err, http.ErrServerClosed)` — `main.go:115` — aligns with the peer mains and survives any future wrapping.
- [nitpick] Distinguish the oversized-body case with `errors.As(err, &maxErr)` (`*http.MaxBytesError`) and return 413 — `handler.go:52` — lets `teams-room-verify` detect a misconfigured `VERIFY_BATCH_SIZE` rather than reading it as malformed JSON.

## 3. Architecture — score 4

### Evidence

- [medium] The chat-id → room-id derivation is duplicated across two binaries with no shared helper — `teams-room-inspector/handler.go:68` vs `room-worker/teamsroomcreate.go:62`. Both call `idgen.DeterministicID([]byte(chatID))` inline and both carry hand-written "must be changed together" comments (`handler.go:44-46`, `teamsroomcreate.go:50-55`). The repo already has the right pattern for exactly this problem — `teamsmigrate.EmployeeIDFromGraphID` (`pkg/teamsmigrate/teamsmigrate.go:66`) wraps the same call so HR-sync and migration cannot drift. Drift here is silent and self-sustaining: every chat reports `missing_room` (`teams-room-verify/runner.go:152-154`), `MarkVerified` never runs, and the flagged set grows unbounded.
- [medium] The store is not site-scoped although the service is configured with `SITE_ID` — `teams-room-inspector/store_mongo.go:47` and `:57` filter on `_id`/`roomId` only. Rooms and subscriptions are replicated to non-owning sites (`inbox-worker/main.go:144-154` `UpsertRoom`, `:133` `CreateSubscription`) and both carry the *origin* site's `siteId` (`room-worker/handler.go:1530`), so a chat asked at the wrong site can be answered `RoomExists=true` with a partial subscription count rather than "not here". `cfg.SiteID` is only echoed into the response (`handler.go:78`), never pushed into the query, so the misroute guard is advisory only.
- [medium] The Mongo client is wired without the shared pool knob — `teams-room-inspector/main.go:86-87` calls `mongoutil.ConnectRead(...)` with observability but no `mongoutil.WithPool`. Over 30 services mount `Pool mongoutil.PoolConfig`, which carries `MONGO_SERVER_SELECTION_TIMEOUT` default 2s and the pool ceiling (`pkg/mongoutil/poolconfig.go:24-50`); without it this long-lived HTTP server takes the driver's 30s selection default and an untunable pool, and the handler passes an undeadlined `c.Request.Context()` straight through (`handler.go:48,71`), so a quiet Mongo parks every verify call for the caller's whole 30s budget (`teams-room-verify/main.go:28`).
- [low] No startup assertion for the index this read-only service depends on — `store_mongo.go:31-36` has no `EnsureIndexes`. The `$match {roomId: {$in: …}}` at `store_mongo.go:57` is served by `roomId_1_u.account_1`, owned by room-service; every other reader of that index warns at startup (`user-service/mongorepo/subscriptions.go:80`, `bot-room-service/store_mongo.go:36`, `inbox-worker/main.go:563`). A dropped index degrades a 500-id batch to a collection scan with nothing in the logs.
- [low] The shutdown goroutine has no termination path when the listener fails — `main.go:101-117`: if `ListenAndServe` returns immediately (port in use), `run` returns at `:116` while the `shutdown.Wait` goroutine still blocks on its signal channel and `mongoutil.Disconnect` never runs. Cosmetic (the process exits), and the same shape exists fleet-wide (`client-update-service/main.go:89-104`), but it is a goroutine without a clear exit per CLAUDE.md §3.

### Recommendations

- [medium] Extract the derivation into `pkg/teamsmigrate` (e.g. `RoomIDFromTeamsChatID(chatID)`) and call it from both `room-worker/teamsroomcreate.go:62` and `teams-room-inspector/handler.go:68` — the mapping becomes one edit instead of a comment-enforced contract across two deployables.
- [medium] Pass `siteID` into `newMongoStore` (`main.go:92`) and add `"siteId": s.siteID` to both filters (`store_mongo.go:47,57`) — a wrong-site question then answers "room missing" (a reportable mismatch) instead of a plausible-looking partial count.
- [medium] Add `Pool mongoutil.PoolConfig` to `Config` (`main.go:29-39`), call `cfg.Pool.Validate()` after parse, and pass `mongoutil.WithPool(cfg.Pool)` at `main.go:86` — brings the 2s server-selection bound and an explicit pool ceiling in line with the rest of the fleet.
- [low] Add an `EnsureIndexes(ctx)` on `mongoStore` calling `mongoutil.WarnMissingIndexes(ctx, s.subs.Raw(), "roomId_1_u.account_1")`, invoked from `run()` after the store is built — matches `inbox-worker/main.go:563` and makes a missing index visible before it becomes a latency incident.
- [low] Give the shutdown goroutine an exit: use a cancellable context / `shutdown.WaitOn` with a channel the listen-failure path can close (`main.go:101-117`), so Mongo is disconnected on every exit path.
- [nitpick] Move the inline per-request body cap (`handler.go:25,50`) behind a small shared helper if a second internal endpoint ever appears; today one call site is fine.

## 4. Test coverage — score 1

### Evidence

- [critical] coverage below repo minimum 80%, currently 47.7% — `teams-room-inspector` package (42/88 stmts, `ok github.com/hmchangw/chat/teams-room-inspector 47.7%`) — under the 60% line, so critical per the dimension rule. All 46 uncovered statements sit in `main.go` (29) and `store_mongo.go` (17); `handler.go`, `routes.go` and `newServer` are at 100%.
- [high] `run()` is 10.3% covered — `teams-room-inspector/main.go:69` — only the config-parse branch is exercised (`main_test.go:21`). Untested: `obs.Init` failure (`main.go:77-80`), `mongoutil.ConnectRead` failure (`main.go:86-90`), store/handler wiring (`main.go:92`), and the whole shutdown closure — `srv.Shutdown` → `mongoutil.Disconnect` → `obsShutdown` ordering at `main.go:104-112`. There is no injectable seam, unlike `newServer`, which was deliberately extracted for testability (`main.go:48-50`).
- [high] the Teams chat-id → room-id derivation is asserted tautologically — `teams-room-inspector/handler_test.go:46-48` — the test recomputes `idgen.DeterministicID([]byte(chatID))` rather than pinning a literal expected id, and `pkg/idgen/idgen_test.go:46` also pins no fixed value (only length, alphabet, determinism). `handler.go:44-46` states the derivation must be kept in step by hand with `room-worker/teamsroomcreate.go:62`; a change to `idgen.DeterministicID` (`pkg/idgen/idgen.go:86`) would leave every test green while the inspector reported every existing room missing, and `teams-room-verify` acts on that answer. This is the service's single most load-bearing invariant and nothing guards it.
- [medium] `mongoStore.RoomStates` has 0% coverage in CI — `teams-room-inspector/store_mongo.go:41` (16 stmts) and `newMongoStore` `store_mongo.go:31` — its only tests are `integration_test.go:18,45`, and `deploy/azure-pipelines.yml:45` runs `go test ./teams-room-inspector/...` with no `-tags=integration`. Repo-wide none of the 29 `*/deploy/azure-pipelines.yml` files run integration tests, so this is the house norm rather than a local slip, but the effect here is that the service's only DB code is never executed by any pipeline.
- [medium] the pipeline writes `coverage.out` but never evaluates it — `teams-room-inspector/deploy/azure-pipelines.yml:45` — no `go tool cover -func` threshold step, so the CLAUDE.md §4 80% floor is unenforced and a regression below 47.7% would go green.
- [low] the immediate listen-failure branch is untested — `teams-room-inspector/main.go:115-117` — returning there skips `<-shutdownDone`, so `mongoutil.Disconnect` (`main.go:108`) and `obsShutdown` (`main.go:111`) never run. Harmless in practice because `main` then `os.Exit(1)`s, but it is a live, untested branch that the same seam would cover.
- [low] store integration tests cover only the happy path and empty input — `teams-room-inspector/integration_test.go:18,45` — nothing exercises the error branches (`store_mongo.go:49-51`, `:60-62`) or a full `maxChatIDsPerRequest`-sized batch (500 ids), so the `$in` fan-out at the documented boundary is unverified against a real Mongo.
- [nitpick] no unit case asserts the handler's orphan shape — `teams-room-inspector/handler.go:84-94` — `roomExists:false` with `subscriptionCount > 0` is the drift signal the caller acts on; it is covered store-side (`integration_test.go:41`) but never through `HandleVerify`.

Test hygiene is otherwise good: table-driven invalid-input cases with `t.Run` (`handler_test.go:111-129`), a fresh `gomock.NewController` per test with no shared mutable state, helpers confined to `_test.go` (`handler_test.go:22,30`), `//go:build integration` + `TestMain` → `testutil.RunTests` (`integration_test.go:1,16`) and containers from `pkg/testutil` only (`integration_test.go:19`).

### Recommendations

- [critical] Extract the dial/serve/shutdown wiring out of `run()` into a testable constructor the way `newServer` already is — `main.go:69` — unit-testing the resulting seam closes ~26 of the 46 uncovered statements and lifts the package over the 80% floor on its own.
- [high] Add a golden-value test pinning `idgen.DeterministicID` output for a fixed Teams chat id to a literal string, referenced from both `teams-room-inspector` and `room-worker` — `handler_test.go:46`, `pkg/idgen/idgen_test.go:46` — turns the hand-maintained cross-service invariant into a failing test instead of a silent site-wide false "room missing".
- [medium] Add an integration stage (`go test -tags=integration ./teams-room-inspector/...`) on a Docker-enabled agent — `deploy/azure-pipelines.yml:45` — today the store's only tests never execute in CI.
- [medium] Gate the Validate stage on `go tool cover -func=coverage.out` ≥ 80% — `deploy/azure-pipelines.yml:45` — makes the CLAUDE.md §4 floor enforceable rather than aspirational.
- [low] Extend `integration_test.go:18` into a table covering a 500-id batch and the two store error branches (`store_mongo.go:49`, `:60`).
- [low] Cover the immediate-`ListenAndServe`-failure path and assert Mongo/obs cleanup still runs — `main.go:115`.
- [nitpick] Add a `HandleVerify` case for the orphan-subscription shape — `handler.go:84`.

## 5. Maintainability — score 4

### Evidence

- [medium] The chat-id → room-id derivation is duplicated across two services and synced only by comment — `teams-room-inspector/handler.go:68` — the identical `idgen.DeterministicID([]byte(chatID))` lives at `room-worker/teamsroomcreate.go:62`, and both files carry a hand-written "must be kept in step" note (`handler.go:44-46`, `teamsroomcreate.go:56-60`). Nothing links them at compile time or in a test; `pkg/teamsmigrate/teamsmigrate.go:66` (`EmployeeIDFromGraphID`) is the established home for exactly this kind of derivation. A change on one side makes every chat report `roomExists=false` and `teams-room-verify` mismatch forever instead of failing loudly.
- [low] Two coupled timeouts live in two services as unlinked magic numbers — `teams-room-inspector/main.go:63` — `WriteTimeout: 30s` is exactly `teams-room-verify/main.go:28`'s `inspectorTimeout = 30s`, for a batch capped at 500 ids (`pkg/model/teams.go:110`). Neither constant references the other, so raising the client bound to ride out a slow site truncates the response server-side instead.
- [low] `verifyRequestBodyMaxBytes` encodes an untested sizing assumption — `teams-room-inspector/handler.go:25` — the cap is `maxChatIDsPerRequest*256 + 4KiB`, justified by "each well under 256 bytes". `handler_test.go:108` only exercises a body *over* the cap; no test asserts the other direction, that a full 500-id batch of realistic Graph ids still binds. Raising `TeamsRoomVerifyMaxChatIDs` or meeting longer ids would 400 every batch silently.
- [low] Mongo pool sizing is not an operator knob, diverging from the house pattern — `teams-room-inspector/main.go:86-87` — `ConnectRead` is called with `WithObservability` only, no `Pool mongoutil.PoolConfig` field on `Config` (contrast `room-service/main.go:57`, `media-service/config.go:68`, `teams-chat-sync/main.go:30`). Tuning is only reachable through the URI string.
- [nitpick] Design doc predates the body cap — `docs/superpowers/specs/2026-08-01-teams-room-verify-design.md:190-200` — the handler section describes binding and the 1–500 check but not `http.MaxBytesReader` (`handler.go:50`), the one piece of handler behaviour a reader would not predict from the doc.
- [nitpick] `RoomState.UserCount` is plumbed end-to-end but drives no decision — `teams-room-inspector/store.go:13`, `handler.go:93` — it is deliberate diagnostic context (`pkg/model/teams.go:119-129`) and `teams-room-verify` compares only `SubscriptionCount`; worth keeping, but it reads like logic.

### Recommendations

- [medium] Extract `teamsmigrate.RoomIDFromChatID(chatID string) string` and call it from both `teams-room-inspector/handler.go:68` and `room-worker/teamsroomcreate.go:62`, with a golden chat-id→room-id test in `pkg/teamsmigrate` — turns the only cross-service invariant here from a comment into something a compiler and a test defend.
- [low] Give the 30s inspector/verifier pair one owner: either a shared constant beside `TeamsRoomVerifyMaxChatIDs` in `pkg/model/teams.go:110`, or reciprocal comments at `main.go:63` and `teams-room-verify/main.go:28` — stops a one-sided timeout bump from silently truncating replies.
- [low] Add a handler test that posts a full `maxChatIDsPerRequest` batch of maximum-length Graph ids and expects 200 — `handler_test.go:97` — pins the 256-byte assumption behind `verifyRequestBodyMaxBytes` so a future cap change fails a test rather than production.
- [low] Mount `Pool mongoutil.PoolConfig` on `Config` and pass `mongoutil.WithPool(cfg.Pool)` — `main.go:30-39,86` — brings the service in line with the rest of the fleet and makes its read pool tunable without editing the URI.
- [nitpick] Refresh §3 of `docs/superpowers/specs/2026-08-01-teams-room-verify-design.md` with the request-body cap — the doc is otherwise an accurate, useful map of this service and is worth keeping that way.

### Reviewer notes

- Size/complexity checks are clean and do not merit findings: nine Go files, 774 lines total, largest file `handler_test.go` (149); longest functions are `run` (`main.go:69-120`, 51 lines) and `HandleVerify` (`handler.go:47-97`, 50) — both far under the 80-line bar. Zero TODO/FIXME/HACK, no dead code, no in-service duplication. `go vet ./teams-room-inspector/...` is clean. A new engineer could safely modify this in a day; the only trap is the out-of-service derivation above.
- Per sast-summary.md, `govulncheck` and the semgrep registry packs were blocked by egress policy; not re-run, and no SAST finding falls under this service.
- Branch hygiene the synthesizer may want to carry: the branch already contains nine `docs/reviews/*.md` files, including a prior `teams-room-inspector-readiness-2026-08-31.md` whose maintainability findings overlap mine. CLAUDE.md §5 requires deleting everything under `docs/reviews/` before the PR is opened.
