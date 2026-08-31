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

---

## 5. Maintainability — 4 / 5

1213 lines across 11 files, no function over ~45 lines, comments consistently explain WHY; the only real smells are a leaky counter parameter and test-file naming.

### Findings
- `medium` — `syncChat` takes `sum *summary` purely to do `sum.MembersWritten.Add` (`syncer.go:175,187`), while every other counter is updated by the caller in `run`'s switch (`syncer.go:141-150`). Two places own the tally; adding a third counter means deciding again which side owns it.
- `low` — `run` mixes three responsibilities in one 45-line function: worker-pool construction, dispatch, and outcome classification/logging (`syncer.go:124-169`). Adding one feature — a batch limit, a per-chat timeout, or cancellation-aware dispatch — touches all three.
- `low` — Test helper naming drift: `worker_test.go` and `log_test.go` name no production file (`syncer.go` is the unit under test in both), and `log_test.go` depends on symbols defined in the other two.
- `nitpick` — `teamsUserRef` (`store.go:44`) encodes "unresolved" as its zero value with the semantics documented in three places (`store.go:45-46`, `syncer.go:36`, `syncer.go:87`). A explicit `found bool` or a distinct sentinel would make the D5 finding impossible to write accidentally.

No dead code, no duplicated logic, no leaky abstraction into the Graph SDK (`membersFetcher` at `store.go:60` is a one-method consumer interface). Comment discipline is genuinely strong — `main.go:24-27`, `main.go:85-87`, `syncer.go:36-38`, and `main.go:105-106` all explain rationale, not mechanics.

### Recommendations
- `medium` — Return `len(members)` from `syncChat` and let `run`'s switch own every counter, dropping the `*summary` parameter.
- `low` — Extract the classification switch into `func (s *syncer) classify(chat ChatToSync, err error, sum *summary)`, leaving `run` as load → dispatch → report.
- `low` — Rename `worker_test.go` → `syncer_run_test.go` and merge `log_test.go` into it.
- `nitpick` — Change `userRefCache.resolve` to return `map[string]teamsUserRef` plus an explicit unresolved-id slice, so callers cannot silently treat a miss as an empty identity.

---

---

## 6. Integration — 2 / 5

No NATS surface to get wrong, but the cross-service `teams_chat.members` contract has two unguarded failure modes whose blast radius lands in `room-worker` as silent subscription deletion.

### Findings
- `high` — An **empty Graph roster is written verbatim and advances the chat**. `syncChat` calls `SetMembersSynced` unconditionally (`syncer.go:184`), so `ListChatMembers` returning zero members (a 200 with an empty `value`, e.g. after the app loses access to a chat) stores `members: []` and sets `needCreateRoom: true` (`store_mongo.go:66-71`). `room-worker` then treats that list as authoritative and **deletes every subscription not in it** (`room-worker/teamsroomcreate.go:153-158,176-178`, `DeleteSubscriptionsByAccounts`). One degraded Graph response silently empties a room.
- `high` — A member absent from `teams_user` is persisted with an **empty `Account`** (`store.go:44-47`, `syncer.go:100-106`) and the chat is marked `needMemberSync: false`, so it is never retried. Downstream, `room-worker/teamsroomcreate.go:106-109` skips such a member with a WARN and continues — the person is permanently omitted from the room. This service emits **no log and no counter** for unresolved members; the only summary counters are chats, not members (`syncer.go:160-163`).
- `medium` — `ListChatsToSync` reads `teams_chat` and `teams_user` through a **secondary-preferred** client (`main.go:91`, `store_mongo.go:26-28`), while `teams-chat-sync` deliberately keeps all its teams-collection access on the primary precisely because these collections are freshly populated (`teams-chat-sync/store_mongo.go:16-21`). Replication lag therefore turns a just-created `teams_user` into the permanent empty-account outcome above, and a stale `updatedAt` into a spurious `errSuperseded`.
- `low` — The optimistic-concurrency contract with `teams-chat-sync` is otherwise sound: `updatedAt` is written on every upsert (`pkg/model/teams.go:89`) and the conditional filter at `store_mongo.go:53` correctly returns `errSuperseded` on `MatchedCount == 0`, which `run` classifies as benign (`syncer.go:144-147`).

Not applicable and correctly absent: `pkg/subject` builders, `pkg/stream` configs, OUTBOX/INBOX lanes and `outbox.Publish` partition membership, the `Timestamp int64` event-struct rule, `pkg/msgbucket`, `ROOM_KEY_RETIRED_TTL`, `pkg/idgen` — this service publishes no NATS event and registers no handler (no `nats.go` import). It registers nothing on `chat.user.…` and no `auth-service` HTTP route, so its absence from `docs/client-api.md` and the derived views is correct, not drift. Downstream, `teams-room-creation/runner.go:143` does stamp `Timestamp: now.UTC().UnixMilli()` at the publish site.

### Recommendations
- `high` — Refuse to advance a chat on an empty roster: if `len(raw) == 0`, log WARN, count it, and return without calling `SetMembersSynced` so `needMemberSync` stays true. A room is never legitimately zero-member.
- `high` — Count and log unresolved members per chat (`membersUnresolved` in the run summary at `syncer.go:160`), and decide explicitly whether an unresolved member should block the advance rather than silently persisting an empty `Account`.
- `medium` — Move the `teams_user` resolution lane to the primary client (keep the `teams_chat` scan on the secondary), matching `teams-chat-sync`'s stated reason for primary-only teams-collection reads.
- `medium` — Consider a sanity floor (e.g. refuse a roster that drops below some fraction of the currently stored member count) given `DeleteSubscriptionsByAccounts` is unconditional downstream.

---
