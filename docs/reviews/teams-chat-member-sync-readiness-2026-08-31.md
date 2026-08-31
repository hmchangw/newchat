# teams-chat-member-sync — Production Readiness Review

**Service:** `teams-chat-member-sync` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

Idiomatic and carefully built — consumer-defined interfaces, constructor DI, `Now` injected for testability, a genuine `-race` exercise on the user cache, batched user resolution memoised run-wide, and correct optimistic-concurrency handling against `teams-chat-sync`'s `updatedAt` watermark. Coverage reads 60.3% only because the unit profile cannot see the integration tests that do cover the store; every business-logic function is at 100%.

The integration dimension is where this service is genuinely weak, and the reason is what sits downstream. `room-worker` treats the `members` list this job writes as **authoritative and deletes every subscription not in it**. Two unguarded paths feed it: **an empty Graph roster is written verbatim and advances the chat**, so one degraded response silently empties a room; and **a member missing from `teams_user` is persisted with an empty `Account`** and the chat is marked done, so that person is permanently omitted with no log, no counter and no retry. Reading `teams_user` through a secondary-preferred client makes the second case more likely, on collections a sibling deliberately keeps on the primary for exactly this reason.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 2 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 4 | 6 | 12 | 2 | **24** |

---

## 2. Go code quality — 4 / 5

Idiomatic, tightly-wrapped errors and disciplined structured logging throughout; the one real defect is a security knob that ships insecure-by-default.

### Findings
- `high` — `GraphTLSInsecureSkipVerify` defaults to **true**, so a deployment that forgets the env var silently disables TLS verification on the connection that POSTs `client_secret` — `teams-chat-member-sync/main.go:44`
  Sibling services disagree: `teams-hr-sync/config.go:28` and `user-presence-service/sync/main.go:43` default `false`; only `teams-chat-sync/main.go:51` and `teams-user-sync/config.go:20` share the true default. The underlying `#nosec G402 … //nolint:gosec` double-suppression in `pkg/msgraph/msgraph.go:257-258` is correct — the defect is the *default*, not the mechanism.
- `low` — SAST audit coverage is incomplete: gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean, but `govulncheck` and the semgrep registry packs could not run (egress blocked, per GLOBAL_PREP). Environmental, not a service defect.
- `nitpick` — `errSuperseded` is a syncer-level control-flow concept declared in `store.go:18`; it reads as a store detail but is consumed only by `syncer.go:144`.

Verified clean: every wrap names what *this* function was doing (`store_mongo.go:39,56,85`; `syncer.go:72,95,127,177,181,185`); no bare `err`, no `error: %w`; `errors.Is(err, errSuperseded)` at `syncer.go:144`, never a string compare; JSON `slog` set once at `main.go:53`; no `fmt.Println`/`log.Print`/`os.Getenv`/`panic` anywhere in the service. **Secrets rule holds**: the service never logs config; `pkg/msgraph/msgraph.go:406` surfaces only the OAuth error code, and `pkg/msgraph/chats.go:156` logs status/Retry-After but explicitly never the token or endpoint. `pkg/errcode` is correctly absent — this is a CronJob with no client boundary.

### Recommendations
- `high` — Flip `envDefault` to `"false"` at `main.go:44` and set `GRAPH_TLS_INSECURE_SKIP_VERIFY=true` only in the on-prem overlay; align the three outlier services in one PR.
- `medium` — Log once at startup at WARN level when `GraphTLSInsecureSkipVerify` is true, so an accidentally-insecure prod run is visible in logs.
- `low` — Move `errSuperseded` to `syncer.go` (or a shared `errors.go`), leaving `store.go` purely the consumer-side contract.
- `low` — Re-run `make sast-vuln` from an environment with `vuln.go.dev` reachable before release sign-off.

---

---

## 3. Architecture — 4 / 5

Clean consumer-defined interfaces, constructor DI, correct shared-knob mounting; deviations from the documented file layout are job-shaped and match fleet precedent.

### Findings
- `low` — CLAUDE.md §6 says "Use `pkg/shutdown.Wait` in every service's `main.go`"; this service uses `signal.NotifyContext` instead (`main.go:88`). CLAUDE.md wins on the letter, but `shutdown.Wait` blocks *waiting* for a signal and is wrong for a run-to-completion job — and every sibling CronJob does the same (`teams-chat-sync/main.go:104`, `teams-hr-sync/main.go:71`, `teams-room-creation/main.go:48`, `teams-room-verify/main.go:71`, `teams-user-sync/main.go:49`). The doc, not the code, should carve out CronJobs.
- `low` — No `handler.go`/`routes.go`/`bootstrap.go`; the orchestration lives in `syncer.go`. Correct for a job with no NATS or HTTP surface (no `nats.go` import at all, so no `BOOTSTRAP_STREAMS`, `pkg/subject` or `pkg/stream` obligation arises), and consistent with `teams-chat-sync`.
- `low` — This service's only scan depends on an index it does not own (`needMemberSync_pending`, created by `teams-chat-sync/store_mongo.go:44-51`). Ownership is deliberate and documented there, but it is a silent deploy-order dependency: run this job against a fresh cluster before `teams-chat-sync` has started and the scan is a COLLSCAN.

Verified: interfaces defined in the consumer with only the needed methods (`store.go:30,52,60`), accept-interfaces/return-structs (`newSyncer` at `syncer.go:31` takes three interfaces, returns `*syncer`); `mongoutil.PoolConfig` mounted as a named field (`main.go:33`) rather than re-declared; all config via `caarlos0/env` with `required,notEmpty` on `MONGO_URI` and `required` on all three Graph credentials, `envDefault` on the rest (`main.go:28-49`); fail-fast via `validateConfig` (`main.go:64-72`); `run()` returns errors so deferred `Disconnect`s run (`main.go:95,101`), with cleanup contexts deliberately detached from the cancelled `ctx`.

### Recommendations
- `low` — Add a CronJob carve-out to CLAUDE.md §6 Graceful Shutdown naming `signal.NotifyContext` as the sanctioned pattern for run-to-completion jobs.
- `low` — Note the `teams-chat-sync`-owned index dependency in this service's package comment so the deploy order is discoverable from here.
- `low` — Consider a `handler.go`→`syncer.go` mapping note in CLAUDE.md §1 so the job layout is explicitly sanctioned rather than tolerated.

---

---

## 4. Test coverage — 2 / 5

60.3% (131 stmts) is under the CLAUDE.md 80% floor, so the score is floored at 2 — but the gap is structural, not a quality gap: every uncovered function is store/wiring code that the integration tests do exercise.

### Findings
- `high` — 60.3% is below the CLAUDE.md §4 80% floor — `teams-chat-member-sync` (coverage_by_service.txt)
- `low` — The entire shortfall is `store_mongo.go` (`newMongoStore`, `ListChatsToSync`, `SetMembersSynced`, `UsersByIDs` all 0.0%) plus `main`/`run` (0.0%), while all business logic is at 100% (`resolve`, `buildMembers`, `run`, `newUserRefCache`) and `syncChat` at 90.9% (covfunc.txt). The store functions ARE covered by `integration_test.go:30,56,86,114`, which the `-covermode=atomic` unit profile excludes. The percentage understates real coverage.
- `medium` — The one uncovered `syncChat` statement is the `build members` failure branch (`syncer.go:181-183`): a `UsersByIDs` failure is tested directly (`syncer_test.go:121`) but never *through* `run`, so nothing pins that a Mongo failure mid-chat marks the chat Failed rather than Superseded.
- `medium` — No test covers a **zero/empty Graph roster**: `run` with `ListChatMembers` returning `nil` writes `members: []` and advances the chat (see D5). The destructive downstream makes this the single most important missing case.
- `low` — No test covers a chat whose `updatedAt` is the zero time (document written without the field): the conditional filter at `store_mongo.go:53` would then never match, stalling that chat permanently as Superseded.
- `low` — Test helpers are scattered across files that do not correspond to any source file: `newTestSyncer` in `syncer_test.go:86`, `wtNow`/`member` in `worker_test.go:18-22`, both consumed by `log_test.go:62-63`. There is no `worker.go` or `log.go`.

Verified good: table-driven with named subtests (`main_test.go:57-78`); `package main` throughout; mocks generated by `go.uber.org/mock` into `mock_store_test.go` per the `//go:generate` at `store.go:12` (repo-wide `make generate` produced zero diff); no real DB/NATS in unit tests; `Now` injected via `syncConfig` (`syncer.go:19`); integration tests correctly tagged `//go:build integration` with `TestMain(m) { testutil.RunTests(m) }` (`integration_test.go:1,18`) and containers from `testutil.MongoDB` — no inline `testcontainers.GenericContainer`. `TestUserRefCache_ConcurrentResolveNoRace` (`syncer_test.go:61`) is a genuine `-race` exercise, not vanity.

### Recommendations
- `high` — Add `TestRun_EmptyGraphRosterIsRejected` pinning that a zero-member Graph response does **not** advance the chat (write the guard first — see D5).
- `medium` — Add `TestRun_BuildMembersFailureFailsChat` to close `syncer.go:181-183` and pin Failed-not-Superseded classification.
- `medium` — Add an integration case for a `teams_chat` doc with no `updatedAt`, asserting whatever behaviour you intend (skip-with-warning is better than silent permanent Superseded).
- `low` — Fold `worker_test.go` and `log_test.go` into `syncer_test.go`, or rename the sources so test files mirror them.
- `low` — Report combined unit+integration coverage in CI for this service so the 60.3% figure stops reading as a business-logic gap.

---
