# teams-room-inspector — Production Readiness Review

**Service:** `teams-room-inspector` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

670 lines across nine files, every function short, file layout and DI exactly per CLAUDE.md, and comments that explain WHY — including the one at `pkg/model/teams.go:104-110` explaining why an exceeded batch cap looks like a healthy run rather than a failure. Two bounded queries, an explicit projection, no `$lookup`, secondary-preferred reads.

The service's exposure is that it is **the read side of a federated verification contract, with no deadline and no index guarantee**. `newServer` sets only `ReadTimeout`/`WriteTimeout`, which bound the socket and do **not** cancel the handler context — the repo's own helper says exactly this — so a stalled Mongo read pins the request goroutine and its pooled connection indefinitely, with no `MaxPoolSize` lever because `mongoutil.PoolConfig` is not mounted either. And the subscriptions aggregation depends on an index owned by a different service without calling `mongoutil.WarnMissingIndexes`, so a dropped index degrades a 500-id batch to a full collection scan **with no signal**.

Coverage is 47.7% — below the critical line, though concentrated in wiring rather than logic.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 1 | 7 | 10 | 2 | **21** |

---

## 2. Go code quality — 4 / 5

Idiomatic, well-commented, correct `errcode` Tier-1 usage and `slog`-only logging; the one real defect is a silently discarded bind error.

### Findings
- `medium` — the `ShouldBindJSON` error is dropped without a comment and without `WithCause`, so a malformed body and a `MaxBytesReader` trip are indistinguishable in server logs — `teams-room-inspector/handler.go:52-55`. CLAUDE.md §3: "Never ignore errors silently — comment if intentionally discarded"; `errcode.Classify` logs a cause once server-side (`pkg/errcode/errhttp/write.go:13-16`) and never serializes it.
- `low` — sentinel compared with `!=` instead of `errors.Is` — `teams-room-inspector/main.go:115`. Peers use `errors.Is` (`media-service/main.go:143`, `admin-service/main.go:174`); tcard/portal/upload share the older form, so this is drift, not a bug.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (egress blocked, per GLOBAL_PREP).
- `nitpick` — `//nolint:gocritic // hugeParam` on `newServer` (`main.go:50`) exists only because the whole `Config` is passed for one field (`cfg.Port`, `main.go:60`).

Correct by inspection: raw `fmt.Errorf("read room states: %w", err)` for infra (`handler.go:73`), named constructors for client errors (`handler.go:53,57,61`), no log-and-return, no `fmt.Println`, store impl unexported (`store_mongo.go:26`), both `json` and `bson` camelCase tags on the wire structs (`pkg/model/teams.go:130-146`).

### Recommendations
- `medium` — attach the bind error: `errcode.BadRequest("decode verify request", errcode.WithCause(err))` (`handler.go:53`). It is not an `*errcode.Error`, so `WithCause` is legal, and a JSON syntax error carries no body content.
- `low` — switch `main.go:115` to `!errors.Is(err, http.ErrServerClosed)`.
- `nitpick` — pass `port string` to `newServer` and delete the `nolint`.

---

## 3. Architecture — 4 / 5

File layout, consumer-side interface, constructor DI and `pkg/shutdown` usage are exactly per CLAUDE.md; two shared-knob configs that the rest of the fleet mounts are simply absent.

### Findings
- `medium` — no `Pool mongoutil.PoolConfig` on the config struct — `teams-room-inspector/main.go:29-39`. CLAUDE.md §6 names it as a knob declared once and mounted as a named field; siblings do (`teams-hr-sync/config.go:45`, `teams-chat-sync/main.go:30`, `media-service/config.go:68`). The inspector runs on driver defaults with no operator lever.
- `medium` — no `ginutil.TimeoutConfig` and no `ginutil.Timeout` on the engine — `main.go:50-65`, `routes.go:7-10`. That type exists precisely to stop per-service re-declaration (`pkg/ginutil/timeoutconfig.go:10-19`). See D6 for the runtime consequence.
- `low` — hand-rolled liveness handler and no `/readyz` — `handler.go:38-40`, `routes.go:8`. `docs/health-probes.md:3-11` says every service serves both via `pkg/health`; several Gin peers also omit `/readyz`, so this is fleet-wide drift the service inherits.
- `low` — the room-id derivation is duplicated against its producer, by hand, and the code says so — `handler.go:44-46,68` vs `room-worker/teamsroomcreate.go:62`. `pkg/teamsmigrate` already hosts the analogous `EmployeeIDFromGraphID` (`pkg/teamsmigrate/teamsmigrate.go:65-67`), so the shared-helper pattern exists and was not used.

N/A by design: no JetStream, so `BOOTSTRAP_STREAMS`/`pkg/stream`/consumer-pattern rules do not apply; `env.ParseAs` with `required,notEmpty` on `MONGO_URI`/`SITE_ID` and `envDefault` elsewhere is correct (`main.go:30-38,70-73`).

### Recommendations
- `medium` — mount `Pool mongoutil.PoolConfig` and pass it through `mongoutil.ConnectRead` (`main.go:86`).
- `medium` — mount `HTTP ginutil.TimeoutConfig`, `Validate()` at load, `r.Use(cfg.HTTP.Middleware())`.
- `low` — extract `RoomIDFromChatID(chatID string) string` into `pkg/teamsmigrate` and call it from both `room-worker/teamsroomcreate.go:62` and `handler.go:68`.

---

## 4. Test coverage — 1 / 5

Unit coverage is **47.7% (42/88 statements)** — below the 60% floor, so a `critical` finding and score 1 per CLAUDE.md §4, even though the deficit is concentrated in wiring rather than logic.

### Findings
- `critical` — 47.7% is under the 60% floor; CLAUDE.md §4 requires ≥80% — service-wide, `coverage_by_service.txt`.
- Composition (from `cov.out`): `handler.go` 29/29 = **100%**, `routes.go` 2/2, `newServer` 100%, `run` 11/40 (`main.go:69`), `store_mongo.go` **0/17** — the store is exercised only under the `integration` tag (`integration_test.go:1,18-51`), which the profile excludes. The percentage is not vanity; it is unit-scope accounting.
- `medium` — both store error branches are unasserted anywhere, unit or integration — `store_mongo.go:49-51` and `:60-62`. The handler's store-error path is covered with a mock (`handler_test.go:132-140`), but the wrapping messages themselves never execute.
- `medium` — the batch-cap boundary is only tested from above: `maxChatIDsPerRequest+1` rejects (`handler_test.go:98`), but exactly 500 is never accepted, so an off-by-one in `handler.go:60` would ship green and permanently 400 every full batch (`pkg/model/teams.go:104-110` explains why that failure mode looks like a healthy run).
- `low` — `run()`'s shutdown path (`main.go:101-113`: `srv.Shutdown` then `mongoutil.Disconnect`) is untested; only the config-parse failure is (`main_test.go:21-25`).
- `low` — duplicate chat ids in one request (two entries mapping to one room id, `handler.go:82-95`) are untested.

Compliant: `package main` tests, `go.uber.org/mock` mocks in an unedited `mock_store_test.go`, no real DB/NATS in unit tests, table-driven invalid-input subtests with descriptive names (`handler_test.go:111-129`), `TestMain(m)` → `testutil.RunTests` and `testutil.MongoDB` for containers (`integration_test.go:16,19`).

### Recommendations
- `high` — add an integration case at exactly `model.TeamsRoomVerifyMaxChatIDs` ids end-to-end; it closes both the boundary gap and the large-`$in` path.
- `medium` — cover `store_mongo.go:49-51,60-62` by pointing the store at a closed/cancelled context (or a dropped collection) and asserting the wrapped messages.
- `medium` — add a handler case for duplicate chat ids asserting one result per requested entry, in order.
- `low` — extract the shutdown sequence from `run()` into a testable func so `main.go:101-113` is reachable from a unit test.

---

## 5. Maintainability — 4 / 5

670 lines across nine files, every function short, comments explain WHY (`main.go:82-85`, `pkg/model/teams.go:104-110`) — the only real hazard is a cross-service invariant maintained by hand.

### Findings
- `low` — the hand-synced room-id derivation (`handler.go:44-46,68` ↔ `room-worker/teamsroomcreate.go:62`) is the one place where an edit in another service silently breaks this one: every chat would report `roomExists=false` and the verifier would log mismatches forever rather than fail.
- `low` — `verifyRequestBodyMaxBytes` hardcodes a 256-byte-per-id assumption (`handler.go:19-25`); the 4 KB slack absorbs it today, but nothing tests or asserts the assumption.
- `nitpick` — `RoomState.UserCount` is read, mapped to `RoomUserCount` and never used in any decision (`store.go:11-15`, `handler.go:93`, `teams-room-verify/runner.go:151-159`); it is deliberate diagnostic context (`pkg/model/teams.go:119-129`), so keep it — just do not mistake it for logic.

No dead code, no duplicated logic inside the service, no file that has outgrown its purpose. Adding a second inspection field is a one-line change in three places.

### Recommendations
- `low` — the `pkg/teamsmigrate` extraction above is the single refactor worth doing; it removes the only hand-synced invariant.
- `nitpick` — assert `verifyRequestBodyMaxBytes` covers a max-size batch of realistic Graph ids in the boundary test recommended in D3.

---

## 6. Integration — 4 / 5

No NATS surface at all, so most integration law is N/A; the HTTP contract with `teams-room-verify` is genuinely shared via `pkg/model` except for the endpoint path.

### Findings
- `medium` — the endpoint path is a bare literal at both ends with no shared constant: `routes.go:9` (`/internal/teams/rooms/verify`) vs `teams-room-verify/client.go:13`. The batch cap *is* shared (`pkg/model/teams.go:110` ← `handler.go:17`), which shows the pattern was available and not applied to the path.
- `low` — `handler_test.go:69` asserts "results must come back in request order", but the consumer matches by chat id (`teams-room-verify/runner.go:128-137`), so the test pins a stronger contract than either side needs; the ordering guarantee is documented (`pkg/model/teams.go:138-140`), so keep it, but know it is now load-bearing only for humans.

Verified correct: absence from `docs/client-api.md` is by design and documented — the subject is not `chat.user.…` and this is not an `auth-service` route (`pkg/model/teams.go:100-102`); no `pkg/subject`, stream, OUTBOX/INBOX, `Timestamp`-on-event, `msgbucket` or `ROOM_KEY_RETIRED_TTL` obligations apply; ids come from `pkg/idgen`, not ad-hoc (`handler.go:68`); the `SiteID` echo guard is honoured by the caller (`main.go:35-37` ↔ `runner.go:121-124`).

### Recommendations
- `medium` — move the path to `pkg/model/teams.go` (e.g. `TeamsRoomVerifyPath`) and reference it from `routes.go:9` and `teams-room-verify/client.go:13`.
- `low` — leave the ordering assertion, but add a comment that the consumer keys by chat id so a future reorder is not read as a breaking change.

---

## 7. Performance — 3 / 5

Two bounded queries, explicit projection, no `$lookup`, secondary-preferred reads — but nothing cancels a slow Mongo read, and a silently missing index would turn every batch into a collection scan.

### Findings
- `high` — no per-request deadline: `newServer` sets only `ReadTimeout`/`WriteTimeout` (`main.go:59-64`), which bound the socket and do **not** cancel the handler context — the repo's own helper says exactly this (`pkg/ginutil/timeout.go:10-14`). A stalled Mongo read at `store_mongo.go:47` or `:56` pins the request goroutine and its pooled connection indefinitely; with no `MaxPoolSize` lever either (D2), pool starvation is the failure mode.
- `medium` — the subscriptions `$match {roomId: {$in: …}}` + `$group` (`store_mongo.go:56-59`) relies on an index owned by another service (`roomId_1_u.account_1`, per `room-service`), yet the service never calls `mongoutil.WarnMissingIndexes` — the established convention for exactly this dependency (`inbox-worker/main.go:560-562`, `user-service/mongorepo/subscriptions.go:75`; helper at `pkg/mongoutil/indexes.go:22`). A dropped or renamed index degrades a 500-id batch to a full scan of `subscriptions`, with no signal.
- `low` — the endpoint is unauthenticated by design (`routes.go:5-6`) with no shed valve; `ginutil.MaxConcurrency` exists for this (`pkg/ginutil/limit.go:20-43`). Low because the only caller is a CronJob.

Positives: `$in` batch bounded at 500 (`handler.go:60`), server-side `$sum` instead of shipping documents (`store_mongo.go:38-40,58`), explicit projection (`store_mongo.go:48`), empty-input short circuit (`store_mongo.go:43-45`), no N+1, no unterminated goroutines (`main.go:96-99,102-113`), no `time.Sleep`, secondary-preferred client (`main.go:82-87`). Absence at a key is the expected miss and handled by zero value, so `mongo.ErrNoDocuments` never applies (`handler.go:84`).

### Recommendations
- `high` — add `ginutil.TimeoutConfig` (default 10s) and `r.Use(cfg.HTTP.Middleware())` so a slow Mongo read is cancelled and its connection released.
- `medium` — call `mongoutil.WarnMissingIndexes(ctx, subs.Raw(), "roomId_1_u.account_1")` in `newMongoStore` (`store_mongo.go:31-36`) so a missing dependency index is visible at startup rather than as latency.
- `medium` — mount `mongoutil.PoolConfig` so pool size and server-selection timeout are operator-tunable for a service that fires 500-id `$in` batches.
- `low` — consider `ginutil.MaxConcurrency` on the verify route; the endpoint has no auth boundary other than network policy.

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Install `ginutil.Timeout` so the handler context actually gets cancelled** | Performance / Arch | `main.go:59-64` sets only socket timeouts; `routes.go:7-10`; `pkg/ginutil/timeout.go:10-14` states the distinction | `ReadTimeout`/`WriteTimeout` bound the socket and **do not cancel the handler context.** A stalled Mongo read at `store_mongo.go:47` or `:56` pins the request goroutine and its pooled connection **indefinitely** — and with no `MaxPoolSize` lever (item 3), pool starvation is the failure mode. |
| 2 | `critical` | **Raise coverage from 47.7% — start with the store error branches and the batch-cap boundary** | Test coverage | 42/88 stmts; `store_mongo.go:49-51`, `:60-62` unasserted anywhere; boundary at `handler.go:60` | Below the 60% critical line. The two most valuable additions are cheap: neither store wrap ever executes in any test, unit or integration; and the batch cap is tested only **from above** (`maxChatIDsPerRequest+1` rejects), so an off-by-one would ship green and **permanently 400 every full batch** — which, per `pkg/model/teams.go:104-110`, looks like a healthy run. |
| 3 | `medium` | **Mount `mongoutil.PoolConfig` and `ginutil.TimeoutConfig`** | Architecture | `main.go:29-39`, `:50-65`; siblings at `teams-hr-sync/config.go:45`, `teams-chat-sync/main.go:30`, `media-service/config.go:68`; `pkg/ginutil/timeoutconfig.go:10-19` | Both types exist to stop per-service re-declaration, and both are simply absent here — the inspector runs on driver defaults **with no operator lever**, which is what turns item 1 into an outage rather than a slow request. |
| 4 | `medium` | **Call `mongoutil.WarnMissingIndexes` for the subscriptions index** | Performance | aggregation `store_mongo.go:56-59`; index `roomId_1_u.account_1` owned by `room-service`; the convention at `inbox-worker/main.go:560-562`, `user-service/mongorepo/subscriptions.go:75`; helper `pkg/mongoutil/indexes.go:22` | A dropped or renamed index degrades a **500-id batch to a full scan of `subscriptions`, with no signal.** The service does not own the index, which is exactly why it should warn about it — this is the established repo convention for a cross-service index dependency. |
| 5 | `medium` | **Stop discarding the `ShouldBindJSON` error** | Code quality | `handler.go:52-55` | Dropped with no comment and no `WithCause`, so **a malformed body and a `MaxBytesReader` trip are indistinguishable in server logs.** CLAUDE.md §3 requires a comment for an intentional discard; `errcode.Classify` logs a cause once server-side and never serializes it, so attaching it is free. |
| 6 | `medium` | **Promote the endpoint path into `pkg/model`** | Integration | `routes.go:9` vs `teams-room-verify/client.go:13` | A bare literal at both ends of a cross-service contract. **The batch cap is already shared this way** (`pkg/model/teams.go:110` ← `handler.go:17`), which shows the pattern was available and simply not applied to the path. A rename compiles cleanly and fails at runtime as a 404 the caller reports as a generic status error. |
| 7 | `medium` | **Add a test asserting exactly `maxChatIDsPerRequest` is accepted** | Test coverage | `handler_test.go:98` covers only the reject side | The complement of item 2's boundary point, and one line. |
| 8 | `medium` | **Add integration coverage for the two store methods** | Test coverage | `store_mongo.go:47`, `:56` | The handler's store-error path is covered with a mock; the store itself, including the aggregation that item 4 is about, is not exercised against real Mongo. |
| 9 | `low` | **Record the cross-service invariant somewhere enforced** | Maintainability | flagged by D4 as the service's only real hazard | The one cross-service invariant here is maintained by hand. Items 4 and 6 each convert part of it into something the compiler or the runtime will complain about. |
| 10 | `low` | **Re-run `make sast-vuln` from an egress-permitted runner** | Code quality | `GLOBAL_PREP.md` | `gosec` and the repo-owned semgrep rules are clean; `govulncheck` and the registry packs could not execute in this environment, so third-party CVE exposure is unverified fleet-wide. |

**Worth stating plainly.** This service is small and correct, and none of the above is a defect in its logic. Every item is about what happens when a dependency it does not own — Mongo, or an index another service creates — misbehaves. That is the right thing to harden in a service whose entire job is to answer truthfully about federation state: an inspector that hangs, or that silently full-scans, makes `teams-room-verify` report a federation problem that does not exist.
