# teams-room-verify — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `724d2ae` (base `main`)  
**Overall score:** 3.5 / 5 (baseline 2026-09-01: 3.5, Δ +0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

`teams-room-verify` is the audit stage of the Teams room pipeline: it reads chats flagged `needVerify`, asks each site's `teams-room-inspector` what the room actually looks like, and clears the flag when the two agree. It is well-made — 4s in code quality, architecture, maintainability and performance, zero TODOs, no file over 260 lines, a consumer-defined two-method store, and 100% statement coverage on `runner.go`, `client.go` and `config.go` with real error-path cases. It lands at 3.5, the highest score of the seven Teams CronJobs. The problem is what it verifies.

**The convergence check compares cardinality, not membership.** `runner.go:156` tests `SubscriptionCount != accountsPresent(c.Members)`, and the wire type cannot do better — `model.TeamsRoomVerifyResult` carries an `int` and no account set. So when `teams-chat-member-sync` swaps member A for member B, the count is unchanged; if `room-worker` has not yet applied the new roster the room still holds A's subscription and none for B, and the audit reports `ok`. A swap is the most common roster change there is. This is also why the verify lane cannot catch the fleet's worst Teams finding: against an empty published roster, 0 subscriptions versus 0 accounts verifies as converged. The fix is to carry `SubscribedAccounts []string` and diff the sets.

Second, the two pipeline stages are not mutually exclusive. `ListChatsNeedingVerify` filters on `needVerify: true` alone and does not even project `needCreateRoom`, so a chat still awaiting (re-)creation gets audited. The `updatedAt` compare-and-set does not close the window, because `teams-room-creation`'s `MarkRoomsCreated` sets `needCreateRoom:false, needVerify:true` *without* bumping `updatedAt` — so a token read before that write still matches. Verify can therefore clear the flag on a room it knows is about to be reconciled. Adding `needCreateRoom: {$ne: true}` to the filter and bumping `updatedAt` on the other side closes both halves.

Third, the flagged set is structurally non-draining. `MarkVerified` is the only writer that ever clears `needVerify`, and every failure path — site missing from `TEAMS_VERIFY_SITE_URLS`, call error, misrouted reply, omitted result, genuine mismatch — leaves the chat flagged with only a WARN. There is no attempt counter, no aging, no dead-letter. Migrated rooms are explicitly live and nothing forbids a native join or leave on one, so any such room mismatches forever. The scan that reloads them is an unbounded full materialisation with no limit, pulling every flagged chat's complete `members` array on every run, and `run` also discards the CAS `ModifiedCount`, so `chats_ok` counts chats *attempted* rather than cleared — in exactly the scenario the CAS exists for, the summary reports a clean run.

Two smaller but compounding gaps: the only HTTP hop in the whole pipeline propagates neither `traceparent` nor `X-Request-ID` (the job never calls `obs.Init`, so the propagator is a no-op, and no run id is minted), which means a mismatch WARN here can never be joined to the inspector log line that produced it; and one dead site starves every healthy one, because batches are grouped per site in input order and fed through a single global semaphore with a 30-second per-call timeout and no circuit-break.

Coverage measures 78.9% — the closest of the family to the floor, and the entire shortfall is `main.go`'s wiring plus a store whose integration tests exist, comply fully with CLAUDE.md §4, and run in no pipeline. Unit plus integration reaches ~86.5% before a single new test is written. The reviewer scored this dimension 3 and explicitly deferred to the rulebook; I applied the `<80%` cap mechanically so the service stays comparable with the other 34.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 5 high, 14 medium, 19 low, 7 nitpick (45 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] SIGTERM does not actually stop the pass — `teams-room-verify/runner.go:78-86` — the batch loop spawns a goroutine for every planned batch with no `ctx.Done()` check, so after cancellation each remaining batch still runs `r.verify` (which fails on the dead ctx) and emits a warn + `failedBatches` stat. This contradicts main.go:68-70's comment that the job "aborts between operations"; on a large backlog a pod deletion produces one warn line per remaining batch and inflates the per-site failure stats.
- [medium] No run-level correlation ID, and none propagated to the inspector — `teams-room-verify/main.go:30-36`, `teams-room-verify/client.go:26-30` — CLAUDE.md §3 "Request Logging & Tracing" requires an ID minted at the entry point, carried in `context.Context` and present on every log line. The sibling jobs do exactly that (`teams-hr-sync/main.go:97-99` mints via `idgen.GenerateRequestID()` + `natsutil.WithRequestID`; `teams-user-sync/main.go:76`), and `portal-service/handler.go:314` forwards `natsutil.RequestIDHeader` on outbound Resty calls. Here every `slog.*Context(ctx, …)` carries nothing, and restyutil's own `request_id` field (`pkg/restyutil/restyutil.go:97`) is always empty — a `teams room verification mismatch` line cannot be joined to the inspector's log for the same call.
- [low] Error message wrapped with the callee's own text — `teams-room-verify/runner.go:67` repeats `store_mongo.go:38` verbatim, yielding `run: list chats needing verify: list chats needing verify: <mongo err>`. CLAUDE.md wants the wrap to describe what *this* function was doing.
- [low] Bare `return err` twice — `teams-room-verify/main.go:61` and `:65` return `validateConfig` / `parseSiteURLs` errors unwrapped. The underlying messages are self-describing, so impact is cosmetic, but it is the literal "never return bare `err`" rule.
- [low] The HTTP path is hardcoded independently at both ends — `teams-room-verify/client.go:13` and `teams-room-inspector/routes.go:9` — while the rest of the contract (request/response types and `TeamsRoomVerifyMaxChatIDs`) is shared in `pkg/model/teams.go:104-146`. A path drift makes every batch 404, which `verifyBatch` records as a failed batch and leaves chats flagged forever — the exact "reads like a healthy run" failure mode the shared cap constant was introduced to prevent.
- [low] Level inversion + double log on every tolerated failure — `pkg/restyutil/restyutil.go:80` logs the transport failure at ERROR, then `teams-room-verify/runner.go:111` logs the same event at WARN. The job deliberately treats an inspector outage as benign (chats re-verify next run), but it still emits an ERROR line that will trip level-based alerting.
- [low] `//nolint:gocritic // rangeValCopy` with a weak rationale — `teams-room-verify/runner.go:240-245` — `gocritic`'s performance tag is genuinely enabled (`.golangci.yml:16-19`) and `model.TeamsChat` is a fat struct copied twice per chat here; the justification ("index-range would be less idiomatic") is contradicted by the same file using `for i := range b.chats` at `runner.go:106` and `:135`.
- [nitpick] `parseSiteURLs` validates only non-emptiness — `teams-room-verify/config.go:43-47` — no `url.Parse`/scheme check, so a missing `http://` or a trailing slash (yielding `//internal/...`) is only discovered as per-batch runtime failures rather than at startup, unlike the numeric knobs which fail fast.
- [nitpick] Redundant loop-variable parameter — `teams-room-verify/runner.go:81` — `go func(b batch)` predates Go 1.22 per-iteration loop vars; the repo is on Go 1.25.

### Recommendations

- [medium] Break the batch loop on cancellation — `runner.go:78-86` — add `if ctx.Err() != nil { break }` (or a `select` on `ctx.Done()` before acquiring the semaphore) and log once that the pass was cut short. Makes the behaviour match main.go's comment and keeps shutdown logs and stats honest.
- [medium] Mint a run ID and propagate it — `main.go:55-71`, `client.go:22-30` — `requestID := idgen.GenerateRequestID()`, `ctx = natsutil.WithRequestID(ctx, requestID)`, and `SetHeader(natsutil.RequestIDHeader, …)` on the Resty request, mirroring `teams-hr-sync/main.go:97-99` and `portal-service/handler.go:314`. Gives one joinable id across the job's logs and the inspector's.
- [low] Move `verifyPath` to `pkg/model/teams.go` beside `TeamsRoomVerifyMaxChatIDs` and have `teams-room-inspector/routes.go:9` register from it — removes a silent-drift failure mode between the two services.
- [low] Re-wrap `runner.go:67` as e.g. `fmt.Errorf("verification pass: %w", err)` and wrap `main.go:61`/`:65` with `fmt.Errorf("validate config: %w", err)` / `fmt.Errorf("site registry: %w", err)`.
- [low] Downgrade or silence the duplicate transport log — either drop the warn at `runner.go:111` or add a restyutil option to suppress `logError` for callers that handle the error themselves — so an inspector outage does not raise ERROR for a condition the job treats as retryable.
- [nitpick] Validate each registry entry with `url.Parse` + scheme check in `config.go:43-47`, so a malformed inspector URL fails at startup like the numeric knobs do.

## 3. Architecture — score 4

### Evidence

- [medium] Service wires no `pkg/obs` SDK — `teams-room-verify/main.go:31` — `main` installs a bare `slog.NewJSONHandler` instead of `obs.Init`, which CLAUDE.md §1 makes the single o11y entry point for every service. The callee half of this same lane does wire it (`teams-room-inspector/main.go:77`), so the verify job emits no traces or metrics and the inspector's spans for verify traffic have no parent. Sibling jobs (`teams-room-creation/main.go:27`) share the gap, so this is pattern-level, not a one-off.
- [medium] Outbound inspector call carries no correlation id or trace context — `teams-room-verify/client.go:26-30` — the request is built with `SetContext/SetBody/SetResult` only: no `X-Request-ID`, no `traceparent`. The inspector mints its own id per request (`teams-room-inspector/main.go:55`, `ginutil.RequestID()`), so a mismatch WARN from `runner.go:178` cannot be joined to the inspector access-log line that produced it. CLAUDE.md §3 requires a correlation id generated at the entry point and propagated.
- [medium] No terminal state or bounded retry for chats that never converge — `teams-room-verify/runner.go:97-103,109-115,121-126` — every failure path (site missing from the registry, call error, misrouted reply, omitted result) leaves the batch flagged, and `MarkVerified` (`store_mongo.go:45`) is the only writer that ever clears `needVerify`. A chat whose site is absent from `TEAMS_VERIFY_SITE_URLS`, or whose room can genuinely never be created, is re-listed and re-batched on every CronJob run forever with only a WARN — no attempt counter, no aging, no dead-letter. The flagged set is monotonically non-draining by construction.
- [medium] Flagged-chat scan is an unbounded full-collection materialization — `teams-room-verify/store_mongo.go:34-36` — `FindMany` has an explicit projection and a stable `_id` sort but no `WithLimit`/cursor, so every flagged chat is decoded into one slice before batching. Combined with the previous finding, peak memory and run time scale with the permanently-stuck backlog rather than with the day's work.
- [low] Shared Mongo pool knob not mounted — `teams-room-verify/config.go:14-30` — `Config` has no `Pool mongoutil.PoolConfig` field, so both the read and write clients (`main.go:74,80`) run on driver defaults. The sibling job mounts it (`teams-room-creation/config.go:19` → `main.go:51,57`). CLAUDE.md §6 declares such a knob once in the owning package and mounts it as a named field.
- [low] Per-site URL registry logic duplicated rather than shared — `teams-room-verify/config.go:35-49` — this re-implements `portal-service/handler.go:32-48` (same JSON map + non-empty validation shape). Two hand-maintained registries now exist, neither cross-checked against `ALL_SITE_IDS`; a missing entry surfaces only at runtime (`runner.go:98`).
- [low] File layout deviates from the documented per-service organization — `teams-room-verify/runner.go:1`, `client.go:1` — no `handler.go`; the pass lives in `runner.go` and the HTTP client in `client.go`. CLAUDE.md §1 sanctions only the sub-package exception for large request/reply services. Consistent with the other teams-* CronJobs, so it is an undocumented convention rather than a divergence.
- [nitpick] Batch loop ignores cancellation — `teams-room-verify/runner.go:78-86` — after SIGTERM the loop still schedules every remaining batch; each fails fast and logs a WARN. Safe (failures leave flags set), just noisy.

### Recommendations

- [medium] Wire `obs.Init` and pass the SDK to `mongoutil.WithObservability` and the Resty client — `main.go:31,74,80,87` — gives the job the same trace/metric surface as its callee and makes the verify lane one trace instead of two.
- [medium] Stamp a per-run id into ctx and send it as `X-Request-ID` (plus trace headers) on the inspector POST — `client.go:26-30` — one grep then joins the job's mismatch WARNs to the inspector's access log.
- [medium] Give the flag a terminal path: add a `verifyAttempts`/`lastVerifyAt` field, escalate to ERROR past a threshold, and stop re-batching beyond it — `runner.go:96-170`, `store_mongo.go:45` — turns a permanently stuck chat into an alert instead of silent unbounded growth.
- [medium] Bound the scan (`mongoutil.WithLimit` plus `_id`-keyset resume, reusing the existing sort) — `store_mongo.go:34-36` — caps per-run memory and run time independent of backlog size.
- [low] Mount `Pool mongoutil.PoolConfig` and pass `mongoutil.WithPool(cfg.Pool)` to both connects — `config.go:14-30`, `main.go:74,80` — aligns with the sibling job and lets ops cap a CronJob that runs alongside live traffic.
- [low] Fail startup when `TEAMS_VERIFY_SITE_URLS` omits a known peer, or lift the registry into a small shared package used by both this job and portal-service — `config.go:35-49` — converts a silent per-run skip into a deploy-time error.

## 4. Test coverage — score 2

### Evidence

- [high] coverage below repo minimum 80%, currently 78.9% (135/171 stmts) — `teams-room-verify/main.go:30` — CLAUDE.md §4 sets an 80% floor. The entire shortfall is two files: `main.go` 8/31 (25.8%) and `store_mongo.go` 0/13. Every other file is 100%: `runner.go` 100/100, `client.go` 11/11, `config.go` 16/16.
- [high] `run()`'s whole dependency-wiring half is 0% — `teams-room-verify/main.go:71` — cov.out shows blocks `71.2,75.16` through `95.2,96.12` all at count 0: signal context, both Mongo connects, `newMongoStore`, `newRunner`, `r.run(ctx)`, and both error wraps. The three `main_test.go` tests all bail in the first 15 lines (parse/validate/parseSiteURLs), so nothing ever proves the store and runner are wired to the right clients, DB name, or timeout. `testutil.MongoURI(t)` (`pkg/testutil/mongo.go:60`) plus an `httptest` inspector makes this directly testable — the pattern the data-migration pipelines describe as "start()/lifecycle … only reachable with real Mongo".
- [high] the only tests for both store methods are integration-tagged and CI never runs them — `teams-room-verify/deploy/azure-pipelines.yml:45` — the step is `go test ./$(SERVICE_DIR)/... -v -race` with no `-tags=integration` and no coverage gate, so `ListChatsNeedingVerify` (`store_mongo.go:31`), `MarkVerified` (`store_mongo.go:45`) and `newMongoStore` (`store_mongo.go:22`) are never executed by any pipeline. `store_mongo_test.go` itself is fully compliant (build tag, `package main`, `testutil.MongoDB`, `TestMain → testutil.RunTests` at line 17) — it is just unreachable. `data-migration/oplog-connector/deploy/azure-pipelines.yml:48-55` already has the fix in-repo.
- [medium] the documented SIGTERM-abort behaviour has no test — `teams-room-verify/runner.go:64` — `main.go:68-71` claims the run "aborts between operations instead of being killed mid-batch", but `run()` never checks `ctx.Err()` or selects on `ctx.Done()` between batches, and no test passes a canceled context. Whatever the real behaviour is (queued batches still dispatch and fail inside `verify`), it is unverified.
- [low] unsynchronized shared state in a concurrent test — `teams-room-verify/runner_test.go:262` — the `DoAndReturn` closure appends to `marked` under no lock, and gomock runs actions *outside* `ctrl.mu` (`go.uber.org/mock@v0.6.0/gomock/controller.go:230-237`). With `MaxWorkers: 4` and two sites this is a data race; `-race` passes today only because site-a's verifier errors before reaching `MarkVerified`. The same pattern at lines 106, 138, 170, 326 is single-batch and safe by accident, not by construction.
- [low] `planBatches` boundary conditions untested — `teams-room-verify/runner_test.go:399` — the one test uses `size: 1` only. Untested: `size >= len(group)`, exact multiples, and `size == 0`, which makes `i += size` (`runner.go:250`) spin forever appending batches. `validateConfig` guards the reachable path, but the function has no test asserting the boundary it depends on.
- [low] the read/write client split is never exercised — `teams-room-verify/store_mongo_test.go:33` — both integration tests call `newMongoStore(db, db)`, so the secondary-read / primary-write separation that `store.go:22-24` and `config.go:10-13` justify is collapsed to one client in every test.
- [nitpick] `disconnect` is 0% — `teams-room-verify/main.go:47` — 3 statements; an end-to-end `run()` integration test picks it up for free.

### Recommendations

- [high] Add `//go:build integration` `TestRun_EndToEnd` driving `run()` against `testutil.MongoURI(t)` and an `httptest` inspector, via `t.Setenv` — covers `main.go:71-96` + `disconnect`, the last 23 uncovered statements.
- [high] Copy the data-migration pipeline shape into `teams-room-verify/deploy/azure-pipelines.yml:45`: `-tags=integration` plus the `go tool cover -func` 80%-floor gate (`data-migration/oplog-connector/deploy/azure-pipelines.yml:48-55`). Unit+integration alone reaches ~86.5% (148/171) before any new test.
- [medium] Add a canceled-context test for `runner.run` — `runner.go:64` — pin the actual abort semantics and, if they are wrong, the `ctx.Err()` check between batches.
- [low] Guard `marked` with a mutex in `runner_test.go:262-267` (and the sibling closures) so the assertions do not depend on one site failing.
- [low] Make `TestPlanBatches` table-driven over sizes 1, group-size, oversize and exact-multiple — `runner_test.go:399`.
- [nitpick] Give one integration test two distinct `*mongo.Database` handles so `newMongoStore`'s read/write split is at least structurally exercised — `store_mongo_test.go:33`.

### Reviewer notes

SYNTHESIZER NOTE: the reviewer scored 3 and explicitly deferred to the dimension rule; the <80% cap has been applied mechanically (3 -> 2) so this service is comparable with the other 34. The finding text is unchanged.
