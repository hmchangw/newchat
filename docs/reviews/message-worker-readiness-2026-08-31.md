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

---

## 6. Integration — 4 / 5

Every central contract holds — both OUTBOX event types are partitioned, the `USING TIMESTAMP` pinning rules are **exactly right and test-pinned**, bucket math and IDs are correct — and the remaining issues are observability and robustness edges, not broken wires.

### Verified clean — the load-bearing part
**Cassandra pinning matches `CLAUDE.md` exactly.** All five plaintext create INSERTs pin `writeTS(msg.CreatedAt)` (`store_cassandra.go:173`, `:184`, `:265`, `:276`, `:297`); all six **encrypted** create INSERTs are unpinned (a fresh nonce per attempt would otherwise pair one attempt's ciphertext with another's nonce under a single timestamp — permanently undecryptable); the three `stripLegacyPlaintext*` tombstones, the tcount/tlm SETs and the `IF EXISTS` stamps are all unpinned.

**OUTBOX partition membership is fully closed end to end**: `InboxThreadSubscriptionUpserted` and `InboxThreadUnreadAdded` are both in `ConcurrentEventTypes`, consumed by outbox-worker's concurrent lane, and handled at the destination in `inbox-worker`. `MESSAGE_BUCKET_HOURS` is validated > 0, one `msgbucket.Sizer` is threaded through every bucketed statement, and the default 360 matches history-service, bot-message-worker and es-index-migrator. Timestamps are set via `time.Now().UTC().UnixMilli()` at all three publish sites. IDs use `idgen.GenerateUUIDv7()` for ThreadRoom/ThreadSubscription and `idgen.MessageIDFromRequestID` (20-char base62) for Teams. No raw `fmt.Sprintf` subject construction. No `chat.user.` handler, so no `docs/client-api.md` obligation.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **OUTBOX publish failures are labelled `recipient_publish`, not `outbox_publish`.** `pkg/natsmetrics/subject.go:164` maps `chat.outbox.*` correctly, and both sibling producers derive labels that way. message-worker is the only caller of `DestinationOutbox` with a hand-written operation, so **its federation failures fall outside the fleet-wide `outbox_publish` failure series** — the one signal an on-call would query during a cross-site incident | `main.go:218`; cf. `broadcast-worker/main.go:406`, `room-worker/main.go:245` |
| low | `setParentTcountAndTlm` blind-SETs `messages_by_room` at **event-supplied partition coordinates with no `IF EXISTS`**. Those coordinates come from a value the handler explicitly trusts unverified when the gatekeeper ships it. The neighbouring write to the identical coordinates guards with `IF EXISTS` and logs the miss at ERROR *precisely because* a silent miss is unrecoverable. If the trusted value ever drifts (ms truncation, a re-sent event), the tcount SET creates a **phantom row holding only `tcount`/`thread_last_msg_at`**, undetectably | `store_cassandra.go:435-438`; `handler.go:174-176`; cf. `store_cassandra.go:483-493` |
| low | Startup verifies only the stream this mode consumes, never `OUTBOX-{siteID}` it publishes to. A misprovisioned OUTBOX surfaces at the first cross-site thread reply as a NAK loop inside the ~1 h outage retry budget, rather than at deploy | `bootstrap.go:63` |
| low | **Cross-site federation is published before the reply is durably persisted** — `publishThreadSubInboxIfRemote` and `fanOutThreadUnread` both run before `store.SaveThreadMessage`. A Cassandra failure after those publishes leaves remote replicas holding thread-subscription and unread state for a reply that only exists once redelivery re-persists it. The destination applies are idempotent so it converges — but the ordering is publish-then-persist, and the OUTBOX dedup window will not suppress the redelivered copies given the retry budget | `handler.go:216-236` |
| low | The destination dispatches this service's event **by string literal, not the shared constant** — renaming `model.InboxThreadSubscriptionUpserted` would compile cleanly and silently strand message-worker's federated subscriptions at the destination | `inbox-worker/handler.go:244` |
| nitpick | `publishMetrics.Failure` is invoked on every core-NATS publish **including successes**; the siblings deliberately classify only on error | `main.go:207` |

### Recommendations
- `medium` — Replace the hardcoded labels in the publish closure with `natsmetrics.PublishLabelsFromSubject(subj)`, matching room-worker and broadcast-worker, and call `Failure` only when `err != nil`.
- `low` — Add `IF EXISTS` plus an ERROR log to the `messages_by_room` half of `setParentTcountAndTlm`, mirroring `UpdateParentMessageThreadRoomID`, so a bad parent coordinate is observable instead of writing a phantom row.
- `low` — Extend `bootstrapStreams` to verify (not create) `stream.Outbox(siteID)` on the production path, since thread federation now depends on it.
- `low` — Move `fanOutThreadUnread` and the thread-subscription outbox publishes to **after** `SaveThreadMessage` returns, so nothing is federated for a reply that is not yet durable.
- `low` — Change `inbox-worker/handler.go:244` to the `model.InboxThreadSubscriptionUpserted` constant so the cross-service contract is compiler-enforced.

---

## 7. Performance — 3 / 5

The plain-message hot path is genuinely well-tuned (one UnloggedBatch per message, sonic, correct semaphore backpressure, clean `jsretry` discipline), but the **thread-reply path carries an O(N²)-per-thread partition scan, two LWTs and ~10 serial Mongo round-trips per reply.**

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **Every thread reply re-scans the entire `thread_messages_by_thread` partition to derive `tcount`**, so the Nth reply costs O(N) and a thread costs O(N²) overall. A 20k-reply thread walks 20k rows (4 pages at `PageSize(5000)`) per new reply, **holding a `MAX_WORKERS` slot for up to the 15 s `scanTimeout`** | `pkg/threadcount/count.go:41-44`, called unconditionally from `store_cassandra.go:451` |
| high | `UpdateParentMessageThreadRoomID` fires **two LWTs** (`IF EXISTS`, Paxos, ~4 extra round-trips each) on *every subsequent* reply as an idempotent re-stamp, **even though the stamp is immutable after the first reply**. `SaveThreadMessage`'s own comment avoids LWT for its "5–10× Paxos overhead" — and the re-stamp then pays it twice per reply. It only needs to run on thread-room creation or a detected redelivery | `store_cassandra.go:467-470`, `:482-485`, called at `handler.go:572`; cf. `:245-247` |
| high | `MarkThreadSubscriptionMention` issues **two sequential `UpdateOne` calls per mentioned user** inside a per-mention loop, with a cross-site `outbox.Publish` also in-loop. A 20-mention reply is **40 serial Mongo round-trips plus up to 20 publishes before the message is persisted** | `store_mongo.go:120`, `:132`; loop at `handler.go:682`, publish at `:685` |
| medium | `GetThreadRoomByParentMessageID` has **no projection** and decodes the whole `thread_rooms` document, while its caller uses only `.ID` and `.ReplyAccounts` | `store_mongo.go:70-79`; `handler.go:498-499` |
| medium | The parent author is re-resolved on **every** reply — a Cassandra point read plus a user lookup — to recover values that are invariant for the life of the thread and partly already stored in `thread_rooms.replyAccounts` | `handler.go:500`, `:504` |
| medium | `AdvanceThreadSubscriptionLastSeen` is a standalone `UpdateOne` issued immediately beside the replier's own subscription upsert **on the same key** — two round trips where one `$setOnInsert` + `$max` would do | `handler.go:214` → `store_mongo.go:158`, `:93` |
| low | Three debug/flow log sites build their varargs slice and **box their arguments unconditionally**, one of them reached on *every* message. The file demonstrates the correct pattern two lines earlier: the flow log at `:80` is gated behind `logctx.Enabled` precisely because "slog.Log evaluates its args before Enabled runs" | `handler.go:117`, `:163`, `:346` (via `:240`/`:254`) vs `:75` |
| low | The sonic pretouch set is **misaligned with actual sonic use**: `model.InboxEvent` is warmed but never sonic-marshalled here (`pkg/outbox` uses `encoding/json`), while `model.ThreadUnreadAddedEvent`, which *is* sonic-marshalled on the reply path, is not warmed | `pretouch.go:13`; `handler.go:752` |
| low | `MaxWorkers` is unvalidated while `Pool`, `Breaker` and `MessageBucketHours` all get explicit validation — and it sizes both the semaphore and `PullMaxMessages` | `main.go:48` vs `:88-101` |
| nitpick | Clean on the traps: no bare `Nak()`/`NakWithDelay(0)` anywhere; no hardcoded `cc.BackOff`; bucket math is a cheap div/mul via the injected sizer; goroutine termination and shutdown ordering correct | `handler.go:89`; `main.go:360` |

### Recommendations
- `high` — Stop the full-partition `tcount` scan on the add path: maintain the count incrementally (a counter column or a `thread_rooms` field) and reserve `threadcount.CountAndLatest` for the delete path that actually needs soft-delete awareness — or gate the rescan behind a length threshold.
- `high` — Restrict `UpdateParentMessageThreadRoomID` to the first-reply path and to explicit redeliveries (`natsmetrics.DeliveryAttemptFromContext` is already available); drop the two per-reply LWTs from the steady state.
- `high` — Collapse `MarkThreadSubscriptionMention` to one `UpdateOne` per mentionee (fold the guard into a single filter/pipeline update), and batch the loop's writes with `BulkWrite`.
- `medium` — Add an explicit projection to `GetThreadRoomByParentMessageID` selecting only `_id` and `replyAccounts`.
- `medium` — Denormalize `parentAuthorAccount`/`parentAuthorSiteId` onto the `thread_rooms` document at creation, removing the per-reply Cassandra read and user lookup.
- `medium` — Fold `AdvanceThreadSubscriptionLastSeen` into the replier's subscription upsert as a `$max` in the same statement.
- `low` — Gate the three debug log calls behind `logctx.Enabled`; align `pretouchTypes` with the types actually sonic-encoded; validate `MaxWorkers`.

---

## 8. Prioritized action list

| # | Sev | Action | Dimension | Evidence | Why |
|---|-----|--------|-----------|----------|-----|
| 1 | `high` | Add `TestCassandraStore_DerivedSetsDoNotPinWriteTimestamp` asserting the `tcount`/`tlm` UPDATEs carry no `USING TIMESTAMP` | Test coverage | `store_cassandra.go:425`, `:447` | **The one rule whose violation is silent and data-corrupting.** The positive half is test-pinned; the negative half is not, so adding a pin to either derived SET today passes the entire suite while letting a redelivered create outrank the tcount that supersedes it. |
| 2 | `high` | Stop the full-partition `tcount` rescan per reply; maintain the count incrementally | Performance | `pkg/threadcount/count.go:41-44`; `store_cassandra.go:451` | O(N²) per thread. A long thread's Nth reply walks N rows while holding a worker slot for up to 15 s — the service's sharpest scaling cliff. |
| 3 | `high` | Restrict `UpdateParentMessageThreadRoomID` to first-reply and redelivery | Performance | `store_cassandra.go:467-470`, `:482-485` | Two Paxos LWTs on **every** reply to re-stamp an immutable value, in a store whose own comment avoids LWT for its 5–10× overhead. |
| 4 | `high` | Collapse `MarkThreadSubscriptionMention` to one write per mentionee and `BulkWrite` the loop | Performance | `store_mongo.go:120`, `:132`; `handler.go:682` | A 20-mention reply costs 40 serial Mongo round-trips **before the message is persisted**. |
| 5 | `high` | Extract `main()`'s wiring into testable seams | Test coverage / Maintainability | `main.go:73` | 181 statements at 7.2% — ~46% of the entire coverage deficit, and the reason the consume loop, publish closure and shutdown chain are untested by construction. |
| 6 | `high` | Cover `UpdateParentMessageThreadRoomID` in integration, both applied and not-applied | Test coverage | `store_cassandra.go:464` | 0% in both lanes, on the LWT whose own comment says a silent miss "permanently breaks thread reads for that parent". |
| 7 | `high` | Introduce one `messageColumns` binder; collapse the five `subscribeAndFederate` copies; split `processMessage` | Maintainability | `store_cassandra.go:168`…`:366`; `handler.go:92-258` | Ten hand-typed column lists with no compiler help (a miscount is runtime-only), a 168-line core function, and five copies of the subscribe-and-federate sequence. |
| 8 | `medium` | Use `natsmetrics.PublishLabelsFromSubject` in the publish closure | Integration / Architecture | `main.go:211`, `:218` | OUTBOX failures are labelled `recipient_publish`, so this service's federation failures **fall outside the fleet-wide `outbox_publish` series** an on-call would query during a cross-site incident. |
| 9 | `medium` | Delete the dead `METRICS_ADDR` knob and document the real metrics port | Architecture | `main.go:61` | An operator setting it gets silence, and deploy manifests will scrape a port nothing listens on. |
| 10 | `medium` | Extract the Teams migration to its own service, or decorate the flag away | Maintainability | `teamsbatch.go`, `main.go:36-39` | ~460 lines of finished one-off migration is now permanent surface area — with its own store, cache and consumer — inside the **sole persister of message history**, threaded through five functions by an `isMigration` bool. |

### Verdict

**Ship-capable.** This service handles the most dangerous correctness rule in the repo — Cassandra write-timestamp pinning across plaintext, encrypted and tombstone paths — and gets every case right. The work here splits cleanly: item 1 protects that correctness from a future edit, items 2–4 are real scaling cliffs on the thread path that will bite as threads grow, and items 5–7 are what make the rest safe to change.
