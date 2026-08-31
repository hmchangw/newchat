# teams-room-verify — Production Readiness Review

**Service:** `teams-room-verify` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.5 / 5

A convergence auditor built with real care. The cross-service contract with `teams-room-inspector` is shared through `pkg/model` with the batch cap enforced identically at both ends; the misroute guard is tolerant of older inspectors by design; the query shape matches the partial index `teams-chat-sync` owns and documents for it; and the covered paths are the ones that matter — including the two subtle convergence cases (guest members with empty accounts, duplicate accounts) that would otherwise flag chats forever.

The gaps are operational rather than logical. **The flagged-chat scan is unbounded**: `needVerify` is raised on every room creation, so the flagged set is exactly the backlog that grows when a site's inspector is down — **the failure mode this job exists to detect is also the one that makes it OOM.** **A canceled context does not stop batch dispatch**, so a routine pod eviction emits a warning per remaining batch and a summary indistinguishable from a total federation outage. And `MarkVerified` discards its `BulkWrite` result, so `chats_ok` over-reports every run in which member-sync re-wrote a chat mid-pass — the exact scenario the compare-and-set exists for.

Coverage is 78.9% — **1.1 points** under the floor, entirely `main()` wiring plus a store the integration tests do cover.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 2 | 10 | 10 | 2 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, consistently wrapped errors and disciplined slog usage throughout; deductions are for a missing log-level knob, a doubled error context, and a silently discarded bulk-write result.

### Findings
- `medium` — `BulkWrite`'s result is discarded, so a compare-and-set miss is indistinguishable from a successful clear — `teams-room-verify/store_mongo.go:58`
  `MarkVerified` returns `nil` whether it cleared N chats or 0. The runner has already counted those chats as `ok` (`runner.go:160-161`), so `chats_ok` in the summary (`runner.go:215`) over-reports every run in which member-sync re-wrote a chat mid-pass — the exact scenario the CAS exists for. `mongoutil.BulkResult` carries the matched/modified counts.
- `medium` — no `LOG_LEVEL` config; the JSON handler is built with nil options — `teams-room-verify/main.go:31`
  CLAUDE.md §3 explicitly names log level as config that must have an `envDefault`. Consequence: `restyutil`'s per-request `slog.Debug` (`pkg/restyutil/restyutil.go:75`) is unreachable in production, so there is no way to turn on per-inspector-call tracing during an incident without a rebuild.
- `medium` — no run/correlation ID is generated or put on the context — `teams-room-verify/main.go:71`
  `restyutil` already emits `request_id` when one is on the context (`pkg/restyutil/restyutil.go:97-99`), and CLAUDE.md requires an ID generated at the entry point via `idgen.GenerateRequestID()`. Nothing correlates one CronJob pass's outbound calls, mismatch warnings and summary lines.
- `low` — the same error context is applied twice on the list path — `teams-room-verify/runner.go:67` duplicating `teams-room-verify/store_mongo.go:38`
  Produces `run: list chats needing verify: list chats needing verify: querying teams_chat: …`. CLAUDE.md asks the wrapper to describe what *this* function was doing.
- `low` — `MONGO_PASSWORD`/`MONGO_USERNAME` carry `envDefault:""` — `teams-room-verify/config.go:17-18`
  CLAUDE.md: "never default secrets". House convention across the fleet (`teams-room-creation/config.go:17`), and the empty default is what makes local no-auth Mongo work, so this is a convention-vs-rule conflict worth resolving once fleet-wide rather than here.
- `low` — SAST audit-coverage gap: `gosec` and the 18 repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (blocked egress). Environmental, not a service defect.

### Recommendations
- `medium` — Have `MarkVerified` return `*mongoutil.BulkResult` (or the matched count) and log `chats_cas_missed` in the site summary; the `ok` counter currently lies whenever the CAS does its job.
- `medium` — Add `LogLevel string \`env:"LOG_LEVEL" envDefault:"info"\`` to `Config` and build the handler with `&slog.HandlerOptions{Level: …}`.
- `medium` — Generate one run ID with `idgen.GenerateRequestID()` in `run()` and attach it to `ctx` before `r.run(ctx)`, so resty logs and every `WarnContext` share it.
- `low` — Drop the duplicated `"list chats needing verify: "` prefix at `runner.go:67`; the store already supplies it.

---

---

## 3. Architecture — 4 / 5

Textbook consumer-side store interface, constructor DI and a dependency-free runner; the only real gaps are the missing `pkg/obs.Init` wiring and two filename/knob conventions.

### Findings
- `medium` — the service never calls `pkg/obs.Init`, so it emits no OTel traces or Prometheus metrics — `teams-room-verify/main.go:30-36`
  CLAUDE.md §1 requires each service to wire the o11y SDK once via `pkg/obs.Init`; 20 services do. All five `teams-*` jobs skip it, so this is a family-wide gap, not a one-off — but the effect here is that a CronJob whose entire product is an audit signal exports zero metrics, and `mongoutil.Connect` silently takes its uninstrumented path (`pkg/mongoutil/mongo.go:41-42` runs only when `cfg.obs != nil`).
- `low` — no shared-knob mounts: neither `mongoutil.PoolConfig` nor `mongoutil.BreakerConfig` is mounted as a named field — `teams-room-verify/config.go:14-30`
  Both Mongo clients run on library defaults. Acceptable for a low-QPS job, but CLAUDE.md's named-field pattern is how the pool is made operable at all.
- `low` — the integration test file is named `store_mongo_test.go`, not `integration_test.go` — `teams-room-verify/store_mongo_test.go:1`
  Correct build tag and a correct `TestMain` (`:17`), just off the prescribed per-service filename; `runner.go` likewise stands in for the prescribed `handler.go`, which is reasonable for a job with no request handler.
- `nitpick` — the site registry is a JSON document smuggled through one env var — `teams-room-verify/config.go:24`
  In tension with "no config files", but it has an in-repo precedent (`portal-service/main.go:40`), is validated eagerly (`config.go:35-49`), and is the only way to express sites on different domains. Not a defect.

Positives worth recording: `TeamsChatStore` declares exactly the two methods this consumer needs (`store.go:25-31`); `verifyFunc` is injected as a function field (`client.go:17`) so the runner needs no HTTP; `runConfig` (`runner.go:14-19`) keeps the pass pure; `validateConfig` fails fast before any dial (`main.go:60-62`); config is a typed `caarlos0/env` struct with no `os.Getenv`; the Dockerfile matches the mandated `golang:1.25.13-alpine` → `alpine:3.21` pair and runs non-root (`deploy/Dockerfile:1,9-12`). No NATS/JetStream at all, so the stream-bootstrap, subject-builder and consumer-pattern rules are correctly N/A.

### Recommendations
- `medium` — Wire `pkg/obs.Init` in `main()` and pass the observability option through to both `mongoutil.Connect`/`ConnectRead`; do it for the whole `teams-*` family in one PR.
- `low` — Mount `Pool mongoutil.PoolConfig` (and `Breaker`) as named fields rather than relying on driver defaults.
- `low` — Rename `store_mongo_test.go` → `integration_test.go` to match the per-service layout.

---

---

## 4. Test coverage — 2 / 5

78.9% of 171 statements — 1.1 points under the CLAUDE.md floor, which forces the score to 2, but the shortfall is entirely `main()` wiring plus a store that *is* covered by integration tests, and the business logic is at 100%.

### Findings
- `high` — 78.9% statement coverage, below the mandatory 80% floor — `teams-room-verify` (`coverage_by_service.txt`)
  Score floored at 2 per CLAUDE.md §4. Note how close it is: the entire gap is `main.go:main` 0%, `main.go:disconnect` 0%, `run` 33.3%, and the three `store_mongo.go` methods at 0% in the unit profile.
- `medium` — `store_mongo.go` is 0% in the unit profile and its integration tests do not run in the service's Azure lane — `teams-room-verify/deploy/azure-pipelines.yml:45`
  The step is `go test ./$(SERVICE_DIR)/...` with no `-tags=integration`; peers do pass it (`data-migration/oplog-connector/deploy/azure-pipelines.yml:48`). Mitigated: `.github/workflows/ci.yml:119-140` auto-discovers any dir containing a `//go:build integration` file, so these tests *do* run in the GitHub lane. The Azure lane also runs `go vet` rather than `make lint` and has no SAST stage.
- `medium` — no test covers context cancellation mid-pass — `teams-room-verify/runner.go:78-86`
  SIGTERM is the documented abort path (`main.go:68-71`), yet no test asserts what a canceled run does. The behaviour is untested *and* wrong (see D6).
- `low` — no test asserts the `MaxWorkers` bound is honoured; `TestRunner_BatchesLargeSites` (`runner_test.go:343-361`) produces 3 batches against `MaxWorkers: 4`, so the semaphore never blocks in any test.

Quality is otherwise genuinely high, not vanity: every runner function is 100% (`covfunc.txt`), and the covered paths are the ones that matter — inspector call failure (`runner_test.go:230`), per-site blast-radius isolation (`:247`), misrouted `SiteID` echo (`:287`), omitted-chat-not-treated-as-missing-room (`:312`), `MarkVerified` failure not failing the run (`:384`), plus the two subtle convergence cases that would otherwise flag chats forever — guest members with empty accounts (`:120`) and duplicate accounts (`:156`). Mocks are generated (`mock_store_test.go:1-6`, no diff repo-wide), tests are in `package main`, independent, and touch no real DB or network except `httptest`.

### Recommendations
- `high` — Close the 1.1-point gap where it is cheapest and most meaningful: table-drive `run()`'s early-exit branches in `main_test.go` (currently 33.3%), which needs no Mongo.
- `medium` — Add `-tags=integration` to the Azure pipeline's test step and a `make lint` + `make sast` stage, matching `data-migration/*` and the GitHub lane.
- `medium` — Add `TestRunner_CanceledContextStopsDispatch`: cancel before `run`, assert no inspector call and no `failedBatches` flood.
- `low` — Add a `MaxWorkers: 1` case asserting the semaphore serializes, so the bound is actually exercised.

---

---

## 5. Maintainability — 4 / 5

Small, single-purpose files with unusually good WHY-comments; the one real smell is that `verifyBatch` now carries four failure branches plus the classification loop, and the same rationale is written out three times.

### Findings
- `medium` — `verifyBatch` is 75 lines mixing four abort paths with the per-chat classification — `teams-room-verify/runner.go:96-170`
  The classification decision (`!RoomExists` → missing room; `SubscriptionCount != accountsPresent` → mismatch; else converged) is the service's entire domain logic and is only reachable through batch plumbing, mock store and a fake verifier. Adding a fifth outcome (say "subscriptions exist but room is gone", already flagged as meaningful in `pkg/model/teams.go:135-137`) means editing the middle of this function.
- `low` — the accountsPresent-vs-raw-member-count rationale is written three times — `teams-room-verify/runner.go:144-150`, `:172-176`, `:222-223`
  Three copies drift independently; the doc comment on `accountsPresent` is the right home.
- `low` — `expectedMembers` is computed for every chat but consumed only on the two mismatch branches — `teams-room-verify/runner.go:151`
  Reads as if it participates in the comparison, which is exactly the misreading the surrounding comment spends seven lines preventing.
- `nitpick` — comment volume in `runner.go` runs ~35% of lines. The content is genuinely WHY-oriented and valuable; several blocks would simply read better as doc comments on the functions they describe than as inline paragraphs.

No dead code, no duplicated logic, no leaky abstractions: the store interface exposes nothing Mongo-shaped, and `planBatches`/`accountsPresent` are pure and directly tested.

### Recommendations
- `medium` — Extract `func classify(c *model.TeamsChat, res model.TeamsRoomVerifyResult) (outcome, bool)` returning an enum, and table-test the outcome matrix directly. `verifyBatch` shrinks to plumbing and the domain rule becomes independently testable — this is the refactor I would actually do.
- `low` — Move the guest/duplicate-account rationale to the `accountsPresent` doc comment and leave a one-line pointer at the call site.
- `low` — Compute `expectedMembers` inside `logMismatch` (it already takes `c`), removing the parameter and the confusion.

---

---

## 6. Integration — 4 / 5

The cross-service contract is properly shared through `pkg/model` with the batch cap enforced identically at both ends; the one gap is that the endpoint path itself is hardcoded twice.

### Findings
- `medium` — the verify endpoint path is a private constant duplicated on both sides of the contract — `teams-room-verify/client.go:13` and `teams-room-inspector/routes.go:9`
  Inconsistent with how the *same* contract handles its other invariant: `TeamsRoomVerifyMaxChatIDs` lives in `pkg/model/teams.go:110` precisely so both ends cannot drift (`teams-room-inspector/handler.go:17` consumes it). The path deserves the same treatment; a rename on the inspector compiles cleanly and fails only at runtime, as 404s that this client reports as a generic status error (`client.go:35`).
- `low` — a transient inspector failure has no in-run retry — `teams-room-verify/client.go:26-33`
  One connection blip leaves a whole batch flagged until the next CronJob tick. Defensible by design (the job is idempotent and self-healing, and `runner.go:111-114` says so), but with `MaxWorkers` concurrency and 30s timeouts a single retry would materially cut the flagged backlog.

Correct by inspection, and worth recording as verified rather than assumed:
- **No NATS surface at all** — the service publishes and consumes nothing. The `Timestamp int64` event rule, `pkg/subject` builders, INBOX/OUTBOX lanes and `outbox.Publish` partition membership are all genuinely N/A here; there is no subject built by `fmt.Sprintf` anywhere in the service.
- **`docs/client-api.md` is correctly untouched.** This is an internal service-to-service HTTP contract on `/internal/…`, not a `chat.user.…` RPC, and `pkg/model/teams.go:100-103` states the exclusion explicitly. No drift in the derived views is possible from this service.
- **Wire structs are shared, not re-declared**: `TeamsRoomVerifyRequest`/`Result`/`Response` (`pkg/model/teams.go:115-146`) with both `json` and `bson` tags and round-trip tests (`pkg/model/model_test.go:5421-5501`).
- **The misroute guard is a real contract feature**, tolerant of older inspectors that send an empty `SiteID` (`runner.go:116-126`) — a thoughtful federation-compatibility decision.
- **Query/index alignment across services**: this service's `find({needVerify:true}).sort({_id:1})` (`store_mongo.go:34-36`) is served by the partial index `teams-chat-sync` owns and documents for exactly this shape (`teams-chat-sync/store_mongo.go:62-67`).
- No ID generation in production code; the only `idgen` use is test fixtures (`runner_test.go:75`).

### Recommendations
- `medium` — Promote `verifyPath` to `pkg/model/teams.go` beside `TeamsRoomVerifyMaxChatIDs` and have `teams-room-inspector/routes.go` register from it, so both ends break at compile time on a rename.
- `low` — Add one bounded retry (or `resty`'s `SetRetryCount(1)` with a short backoff) for connection-level failures and 5xx, so a single blip does not defer a whole batch a full CronJob period.
- `low` — Log `RequestedCount`/`FoundCount` from the response alongside the summary; both are already on the wire and unused, and they would expose an inspector that answers with the wrong cardinality.

---
