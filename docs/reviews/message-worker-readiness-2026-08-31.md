# message-worker — Production Readiness Review

**Service:** `message-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The sole persister of message history, and it gets the genuinely dangerous part exactly right: **every one of `CLAUDE.md`'s `USING TIMESTAMP` pinning rules is correctly implemented and test-pinned** — plaintext creates pin, encrypted creates do not (a fresh nonce per attempt would make a same-timestamp per-cell conflict permanently undecryptable), tombstones and derived SETs ride the client clock. `handler.go` is 95.1% covered. The federation lane is correct and fully closed end to end. What holds it back: the **thread-reply path is O(N²) per thread** (a full partition rescan for `tcount` on every reply, plus two LWTs and ~10 serial Mongo round-trips), coverage is **56.8%** with `main()` alone accounting for ~46% of the deficit, and **the negative half of the timestamp rule is untested** — adding `USING TIMESTAMP` to either derived SET today passes the whole suite while silently corrupting data.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 12 | 17 | 15 | 6 | **51** |

---

## 2. Go code quality — 4 / 5

Idiomatic, lint-clean Go with disciplined `%w` wrapping, sentinel errors via `errors.Is`, and correct worker-tier `errcode` usage — undercut by dead code, a mock with no `go:generate` hook, one double-log, and three poison paths that discard the underlying parse error.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | `mock_hridentity_test.go` is a `DO NOT EDIT` MockGen file with **no `//go:generate` directive anywhere in the service**, so `make generate` never regenerates it. A change to `HRIdentityStore`/`identityResolver` silently leaves a stale hand-frozen mock. (The repo-wide zero-diff `make generate` is consistent with this — the file is simply **never visited**, not verified up to date) | `mock_hridentity_test.go:6`; `store.go:11` |
| medium | Log-and-return double-log: `migrateOne` logs the save failure at ERROR and returns the same `err`, which `handleBatch` propagates to `jsretry.Settle`, which logs it again. `SettleQuiet` exists precisely for the already-logged case | `teamsbatch.go:140-142`; `pkg/jsretry/jsretry.go:138` |
| medium | All three poison-message paths construct the `errcode` with a literal string and **silently drop the decode error**, so the reason a batch or event is unparseable is never recorded anywhere. Peers wrap it via `errcode.WithCause`, which keeps it server-side only — exactly the intent here | `handler.go:94-98`; `teamsbatch.go:64`, `:71` |
| medium | `reactionShortcode` is **dead across the whole repo** — referenced only by a prose comment and its own test. Because a `_test.go` file calls it, `unused` never fires, so it will rot indefinitely. `_ = tm.Forwarded` is the same shape | `teamstransform.go:113`, `:69` |
| low | `slog.Error` (not `ErrorContext`) on two paths that **have** a live `ctx` and hand-copy `request_id` out of it. Every other log site uses the `…Context` form | `store_cassandra.go:475`, `:490` |
| low | `Mode` is a stringly-typed enum with `"teams"`/`"default"` literals repeated across three files and four decision sites, validated only by an inline string comparison. One typo routes a pod to the wrong stream with no compile-time signal — while the service models closed enums correctly elsewhere | `main.go:84`, `:231`, `:361`; `bootstrap.go:46` |
| low | Two competing optional-dependency idioms in one service: a proper functional option for `NewHandler`, but `newTeamsBatchHandler` takes `injectedMetrics ...*persistenceMetrics` and reads `[0]` — a variadic used as an optional arg, silently accepting two and ignoring the second | `handler.go:49` vs `teamsbatch.go:38-44` |
| low | The publish closure selects its metric labels by branching on `msgID == ""`, coupling the label to a **transport detail** rather than the caller's intent, so a third publish site inherits whichever label its msgID happens to imply | `main.go:205-223` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Mojibake in two doc comments — em-dashes rendered as a stray `â` | `teamstransform.go:72`, `:94` |

### Recommendations
- `medium` — Add `//go:generate mockgen -source=teamssender.go -destination=mock_hridentity_test.go -package=main` so the third mock joins `make generate`.
- `medium` — Replace the log-then-return at `teamsbatch.go:140-142` with a bare return (Settle logs it), or switch `consume` to `jsretry.SettleQuiet`.
- `medium` — Attach `errcode.WithCause(err)` to the three `errcode.BadRequest` poison constructions so the parse failure is classified and logged once server-side.
- `medium` — Delete `reactionShortcode`, its test, and the `_ = tm.Forwarded` placeholder; the existing comments already carry the intent without dead symbols.
- `low` — Introduce `type mode string` with `modeDefault`/`modeTeams` constants and a `parseMode` validator; convert `newTeamsBatchHandler` to the existing option type for one DI idiom service-wide.
- `nitpick` — Switch the two `store_cassandra.go` logs to `ErrorContext`; fix the mojibake.

---

## 3. Architecture — 4 / 5

Boundaries, DI, the federation lane and the high-throughput consumer are all correct and unusually well reasoned; the deductions are config-surface drift and a `handler.go` that has outgrown a single file.

### Verified clean
OUTBOX federation goes **exclusively** through `outbox.Publish` with both event types in `ConcurrentEventTypes`; no direct remote-INBOX publish; no stream creation beyond the mode's own. Subjects always via `pkg/subject` (zero raw `Sprintf("chat…")`). No `os.Getenv`. The high-throughput pattern is `cons.Messages()` + `MAX_WORKERS` semaphore + `PullMaxMessages(2*MaxWorkers)`, not mixed. Shutdown is `pkg/shutdown.Wait` in the documented order.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **`METRICS_ADDR` is parsed into config but never read anywhere in the service** — the real Prometheus listener is `pkg/obs`'s `OTEL_EXPORTER_PROMETHEUS_PORT` (default 2112). An operator who sets `METRICS_ADDR=:9090` gets silence, and the deploy manifests will scrape the wrong port. Confirmed dead by grep (one hit: the declaration) | `main.go:61`; `pkg/obs/obs.go:87-88` |
| medium | `USER_CACHE_SIZE`/`USER_CACHE_TTL` re-declared with their own tag and `envDefault`, violating the shared-knob rule. The same pair is copy-declared in **seven** services. The L2 tier immediately below it (`UserL2 userstore.TTLConfig`) already shows the correct mounted-field shape — the L1 tier never got it | `main.go:56-57` vs `:58` |
| medium | The injected `PublishFunc` **hardcodes its metric labels per transport branch** instead of deriving them from the subject, so `thread_unread_added` and `thread_subscription_upserted` OUTBOX failures are indistinguishable, and the core-NATS branch is unconditionally labelled `OperationThreadTCount` even though the branch is generic. room-worker solves this correctly with `natsmetrics.PublishLabelsFromSubject` | `main.go:211`, `:218`; cf. `room-worker/main.go:245` |
| low | Cross-service MongoDB collection ownership is asserted **only by comment**: this service writes `thread_rooms`/`thread_subscriptions` while the unique index is owned by room-service and only warned about here. Ownership by comment means a room-service index change silently degrades this service's writes at runtime rather than at deploy | `store_mongo.go:31-34`, `:47-49` |
| low | `bootstrapStreams` does not "no-op when `Enabled=false`" as `CLAUDE.md` specifies — the disabled path issues a `js.Stream()` existence RPC and fails startup on a miss. **The behaviour is better than the documented one and is now repo-wide (all 12 `bootstrap.go` files do it), so this is CLAUDE.md drift, not a service defect** — reported because `CLAUDE.md` is binding | `bootstrap.go:44-64` |
| low | `Handler`, `PublishFunc`, `Store`, `ThreadStore`, `CassandraStore`, `HRIdentityStore`, `MessageTransformer` all exported from `package main` although nothing outside the binary can consume them; mockgen does not require it | `store.go:15`, `:32`; `handler.go:32`, `:34` |
| nitpick | `teamsBatchHandler` takes the full 5-method `Store` but calls only `SaveMessage`; the Teams-migration store impl lives in `hridentity.go` rather than `store_mongo.go` | `teamsbatch.go:31`, `:137`; `hridentity.go:16` |

### Recommendations
- `medium` — Delete `MetricsAddr` from the config struct and fix any deploy manifest publishing `:9090`; document `OTEL_EXPORTER_PROMETHEUS_PORT` as the metrics port.
- `medium` — Add a `userstore.CacheConfig{Size, TTL}` beside the existing `TTLConfig` and mount it as a named field here, then in the other six services.
- `medium` — Replace the two hardcoded label pairs with `natsmetrics.PublishLabelsFromSubject(subj)`, matching room-worker.
- `low` — Make the room-service-owned index a startup hard failure (or move ownership here); a warn-only path defers the breakage to first write.
- `low` — Unexport the handler/store types and their constructors; regenerate mocks.
- `low` — Update `CLAUDE.md`'s `BOOTSTRAP_STREAMS` paragraph to describe the verify-or-fail-fast disabled path that all 12 services actually implement.
- `nitpick` — Split `handler.go` (858 lines) into `handler.go` (consume + persist) and `handler_thread.go`.

---

## 4. Test coverage — 1 / 5

Coverage is **56.8% (843 statements)** — under the §4 60% line, so the dimension is floored at 1. `handler.go` is at **95.1%**, meeting the 90% core-logic target; the deficit is one 285-line `main()` plus two dangerous untested rules.

| Sev | Finding | Evidence |
|-----|---------|----------|
| critical | 56.8%, below the §4 60% line | `coverage_by_service.txt` |
| high | `main()` is a single 285-line function holding **181 of the 843 statements at 7.2%** — 168 uncovered statements, **~46% of the entire coverage deficit**, all structurally unreachable from a test. Only `buildConsumerConfig` and `canonicalProcessor` were extracted | `main.go:73` |
| high | **The negative half of the `USING TIMESTAMP` rule is untested.** `store_cassandra_writetime_test.go` asserts creates pin (plaintext) / do not pin (encrypted) / strips ride the client clock — but nothing asserts the **derived SETs must NOT pin**. `CLAUDE.md`: "edits, deletes and derived SETs (`tcount`/`tlm`) MUST NOT [pin], so each stays strictly above the create it supersedes." **Adding `USING TIMESTAMP` to either `UPDATE` today passes the whole suite** while silently letting a redelivered create outrank the tcount it should have bumped | `store_cassandra.go:425` (0%), `:447` (22.2%) |
| high | `UpdateParentMessageThreadRoomID` is **0% in both lanes** — absent from handler mocks and grep-absent from `integration_test.go`. This is the `IF EXISTS` LWT whose own comment says a silent miss "permanently breaks thread reads for that parent". Nothing verifies the not-applied branch, and nothing guards the rule that an LWT can never carry `USING TIMESTAMP` | `store_cassandra.go:464` |
| medium | `teamsBatchHandler.consume` is 0%: both poison-drop branches untested. `newTeamsBatchHandler` is 0% and `canonicalProcessor`'s teams-dispatch branch uncovered, so **the whole Teams migration entry path is unexercised** outside one integration test | `teamsbatch.go:59`, `:38`; `main.go:376` |
| medium | `store_mongo.go` is 0% / 72 statements in the unit profile; most methods are reachable via integration, but `AddReplyAccounts` and `UpsertThreadSubscription` appear in **neither** lane | `store_mongo.go:167`, `:90` |
| medium | `fakeJSMsg` carries a `numDelivered` field documented as seeding backoff selection, but **no table case varies it**, and the assertion is only `assert.Positive(t, nakDelay)`. Backoff *escalation* across redeliveries and the `MaxDeliver`-exhaustion salvage boundary are never asserted — a regression to a fixed 1 ms delay would pass | `handler_test.go:2481`, `:2587` |
| low | `teamsbatch_test.go` hand-rolls `captureStore`/`fakeTransformer`/`echoResolver` rather than using the generated `MockStore` | `teamsbatch_test.go:24`, `:38`, `:53` |
| nitpick | `hridentity.go` and `pretouchJSON` read 0% but are covered by integration / are trivial — profile artefact, not a real gap | — |

**Credit where due:** `handler.go` at 95.1% covers thread fan-out, mention marking, quote reprojection and cross-site publish with error-path cases. The Ack-poison vs Nak-with-backoff distinction *is* table-tested. Integration tests conform exactly: `//go:build integration`, `package main`, `TestMain` → `testutil.RunTests`, containers only from `testutil`, zero inline `GenericContainer`. Every subtest builds a fresh `gomock.NewController(t)` — no shared state, no order dependence. The publish function is injected, so no unit test touches NATS.

### Recommendations
- `high` — Add `TestCassandraStore_DerivedSetsDoNotPinWriteTimestamp` beside the existing writetime tests, driving `countAndSetParentTcount` through the same captured-query seam and asserting the two `tcount`/`tlm` `UPDATE`s contain **no** `USING TIMESTAMP`. **This closes the one rule whose violation is silent and data-corrupting.**
- `high` — Extract `main()`'s wiring into testable seams (`buildStores`, `buildConsumer`, `runConsumeLoop`), following the already-extracted `buildConsumerConfig`/`canonicalProcessor`. This alone moves the package from 56.8% toward ~75% without vanity tests.
- `high` — Cover `UpdateParentMessageThreadRoomID` in integration: applied case, and not-applied against a missing parent, asserting the ERROR log and the returned error.
- `medium` — Add a `teamsBatchHandler.consume` unit table using `fakeJSMsg`: corrupt frame → Ack, malformed request → Ack, infra error → Nak with positive delay.
- `medium` — Add integration cases for `AddReplyAccounts` and `UpsertThreadSubscription` (insert-then-upsert idempotency).
- `medium` — Extend the `HandleJetStreamMsg` table with `numDelivered` variants (1, 3, `MaxDeliver`) asserting the delay grows and exhaustion takes the salvage path.
- `low` — Replace the hand-rolled fakes with the generated `MockStore` so interface drift breaks the build.

---

## 5. Maintainability — 3 / 5

Genuinely well-reasoned code with excellent WHY-comments, but `handler.go`'s thread path and `store_cassandra.go`'s ten hand-maintained INSERT column lists have outgrown single-function/single-file shape, and a one-off Teams migration now rides the same binary behind `MODE`.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | The "build subscription → insert/upsert → federate to owner site" sequence is written out **five times**, differing only in `Insert` vs `Upsert` and which account it targets. The parent/replier pair inside the two thread-reply handlers is otherwise byte-for-byte identical | `handler.go:438-459`, `:508-525`, `:533-541`, `:680-687` |
| high | `processMessage` is 168 lines doing eight things: unmarshal, mention resolve, sender resolve, quote re-projection, parent-createdAt resolution + retry budget, thread room/subs, mention marks, unread fan-out, persist, badge publish. **This is the only entry point to the service's core remit**; every new message feature lands here | `handler.go:92-258` |
| high | **Ten `INSERT` statements across four near-duplicate save paths, each with its own hand-typed column list and matching `?` count.** Adding one message column requires editing up to seven statements and their bind lists, with **no compiler help** — a miscount is a runtime-only failure. `saveThreadMessageEncrypted` (76 lines) is `SaveThreadMessage` with the body columns swapped | `store_cassandra.go:168`, `:179`, `:217`, `:227`, `:260`, `:271`, `:292`, `:338`, `:350`, `:366` |
| medium | `mock_hridentity_test.go` has **no `//go:generate` directive**, so `make generate` never visits it — the repo-wide zero-diff does not cover this mock (see Chapter 2) | `mock_hridentity_test.go:2-6` |
| medium | `main()` is ~275 lines and inlines the publish closure, the Teams wiring and the whole consume loop. Only `canonicalProcessor` was extracted for testability, so the loop, closure and shutdown chain are **untested by construction** — a visible contributor to the 56.8% | `main.go:73-347` |
| medium | The publish closure infers its metric labels from **whether a msgID was passed**, hardcoding `OperationThreadTCount` for every core-NATS publish. These labels encode today's only two call sites; the next core publish is silently reported as a thread-tcount badge | `main.go:211`, `:218` |
| medium | `isMigration bool` is threaded through five functions and guards six branches. A flag argument this deep means **every new thread side effect must remember to ask** | `handler.go:92`, `:207`, `:363`, `:433`, `:533`, `:679` |
| medium | Two unrelated remits in one binary: the Teams migration (~460 production lines with its own store, LRU cache and consumer) selected by `MODE`. **A finished one-off migration is now permanent surface area in the sole persister of message history** | `teamsbatch.go`, `teamssender.go`, `teamstransform.go`, `hridentity.go`; `main.go:36-39`, `:264-278` |
| low | Dead production code: `reactionShortcode` referenced only by its own test; `_ = tm.Forwarded` a no-op keeping an unused field alive | `teamstransform.go:113-131`, `:69` |
| low | `errThreadRoomNotFound` is never distinguished by any handler — only an integration test asserts on it. It reads as a contract the handler honours and does not | `store_mongo.go:19`, `:74` |
| low | `Store`'s signatures take `*cassParticipant`, a Cassandra-UDT type with `cql` tags, so **handler code builds storage-shaped structs** — the persistence encoding leaks into the consumer-owned interface | `store.go:16-18`; `handler.go:836` |
| nitpick | Design rationale now duplicated three ways: `CLAUDE.md` §6, a 38-line `writeTS` doc comment, and a 22-line `stripLegacyPlaintext` comment. Correct today; three places to keep in sync | `store_cassandra.go:82-119`, `:121-142` |

### Recommendations
- `high` — Extract `subscribeAndFederate(ctx, msg, threadRoomID, userID, account, eventSiteID, ownerSiteID, now, write subWriteFunc) error` and collapse the five copies onto it, passing `Insert`/`Upsert` as the varying step.
- `high` — Split `processMessage` into `enrich(evt)`, `persistThreadReply(...)` and `persistChannelMessage(...)`; the retry-budget and quote-reprojection logic already have clean seams.
- `high` — Introduce a single `messageColumns` binder producing (columns, placeholders, values) for the plaintext and encrypted variants, so the four save paths **compose** it instead of restating it ten times.
- `medium` — Add the missing `//go:generate mockgen` to `teamssender.go`.
- `medium` — Move the consume loop and publish closure out of `main()` into `run.go`/`consume.go`, and pass the metric destination/operation explicitly rather than deriving it from `msgID != ""`.
- `medium` — Extract the Teams migration to its own service directory and schedule its deletion; failing that, replace the `isMigration` flag with a no-op `ThreadStore` decorator selected once at wiring time.
- `low` — Delete `reactionShortcode`, its test, and the `_ = tm.Forwarded` no-op.

