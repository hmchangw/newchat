# outbox-worker — Production Readiness Review

**Service:** `outbox-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The sole owner of the OUTBOX stream, and it implements the federation topology CLAUDE.md specifies **exactly**: per-peer concurrent and FIFO lanes, `MaxDeliver=-1`, `MaxAckPending=1` on the ordered lane, `pkg/subject`/`pkg/stream`/`pkg/outbox` throughout, and a design that stays generic over event type — adding a new OUTBOX event type needs zero changes here. The handler is 100% covered and the integration suite genuinely exercises a peer outage.

Three findings matter, and two of them undo the isolation the design exists to provide. **A single shared worker pool serves every peer's concurrent lane**, so one unreachable peer's first-delivery burst can hold all 100 slots for the full 3s publish timeout each and throttle forwarding to healthy peers — the per-destination split holds at the JetStream ack-pending level but not at the pool level. **The ordered lane's in-flight work is outside the shutdown drain** — `cc.Stop()` discards buffered messages without waiting for the running callback, and `Closed()` is never awaited. And **an unset `ALL_SITE_IDS` makes the sole OUTBOX owner a silent no-op**: zero peers, zero consumers, one `slog.Warn`, a health check that stays green off the NATS probe — while producers keep filling the stream. The service's own compose default collapses to exactly that.

Coverage is 36.9%, and the uncovered mass is precisely the durable-retry wiring.

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
| Count | 1 | 8 | 16 | 13 | 7 | **45** |

---

## 2. Go code quality — 4 / 5

Idiomatic, correctly-tiered error handling and disciplined slog/jsretry/jobguard usage throughout; the deductions are a real shutdown gap on the ordered lane, an unrecoverable pump-death path, and a service that silently no-ops on empty `ALL_SITE_IDS`.

### Findings
- `high` — the ordered (FIFO) lanes' in-flight work is not tracked by `wg`, and shutdown calls `cc.Stop()` (not `Drain()`+`<-Closed()`) before `nc.Drain()` — `main.go:149-154`, `main.go:172-174`, `main.go:187`
  Only `drainPool` adds to `wg` (`main.go:204,218`); the `Consume` callbacks run outside it. The service's own integration helper documents this exact race and fixes it correctly — "Stop() only unsubscribes — a callback still awaiting its forward's PubAck then fails with 'nats: connection closed'" (`integration_test.go:56-70`, which uses `cc.Drain()` + `<-cc.Closed()`). Production shutdown does the thing the test calls a race: a membership forward mid-`PublishMsg` at SIGTERM dies un-acked (recovered by redelivery, but the shutdown contract in CLAUDE.md — `iter.Stop()` → `wg.Wait()` → `nc.Drain()` — is not actually honored for half the consumers).

- `medium` — a non-`ErrMsgIteratorClosed` iterator error logs and permanently kills that peer's concurrent pump; the process stays up and healthy — `main.go:208-216`, `main.go:157-159`
  `health.ServeWithPprof` only checks the NATS connection (`natsutil.HealthCheck(nc)`), so a peer whose consumer was deleted or whose iterator died reports green forever while its federation lane is dead. Either re-create the lane or make the failure fatal/visible to the health check; a silent one-peer federation outage is the worst failure shape for this service.

- `medium` — empty/absent `ALL_SITE_IDS` yields zero consumers: the sole OUTBOX owner runs as a no-op while producers keep filling the stream — `main.go:39`, `main.go:123-127`
  `envDefault:""` plus a `slog.Warn` is too soft for the one knob that decides whether the service does anything. `MaxWorkers` gets a fail-fast guard two dozen lines earlier (`main.go:55-58`); this deserves the same, or at minimum `required` semantics in non-dev deployments.

- `low` — permanent-classification errors discard the underlying cause, so the poison-drop log names the class but never the reason — `handler.go:341`, `handler.go:345`
  `errcode.Permanent(errcode.BadRequest("unmarshal outbox event"))` drops the `json` error; `jsretry.settle` logs only `"error", err` (`pkg/jsretry/jsretry.go:129`). `errcode.WithCause(err)` is exactly the sanctioned Tier-1 move here (infra/parse error, not another `*errcode.Error`), and the malformed-subject branch could carry the subject — an OUTBOX subject holds no secret material.

- `low` — `federationForwardTimeout = 3 * time.Second` is a compile-time constant on the one call that talks across a WAN gateway — `handler.go:331`
  Every other timing knob on this path (`AckWait`, backoff, `MaxWorkers`) is env-driven via `stream.ConsumerSettings`; the cross-site publish deadline is the one an operator would most want to raise during a degraded-gateway incident.

- `nitpick` — `time.Sleep(300 * time.Millisecond)` used to assert a negative in the FIFO-outage test — `integration_test.go:335`
  CLAUDE.md bans `time.Sleep` for synchronization; `require.Never` expresses "the ack floor must not advance" without the fixed cost and without being load-flaky.

- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run in this environment (blocked egress), so dependency-CVE exposure for this service is unverified.

Clean on the rest of the D1 checklist: every wrap describes what the calling function was doing (`main.go:93`, `main.go:184`, `handler.go:360`, `bootstrap.go:410,415`), `errors.Is` never string comparison (`main.go:212`), no `fmt.Println`/`log.Println`/`os.Getenv`, no interpolated log fields, no token or message-body logging (the skip-warn logs only ids and a `has_dedup_id` bool — `handler.go:351-353`), `errcode.Permanent` used correctly as the Tier-3 worker-only lever, no log-and-return, subjects built exclusively via `pkg/subject`, and `streamManager` is a properly consumer-side, minimal, unexported interface (`bootstrap.go:390-393`).

### Recommendations
- `high` — track the ordered lanes in `wg` (or switch shutdown step 1 to `cc.Drain()` and wait on `cc.Closed()` alongside `iter.Stop()`), mirroring `stopConsumer` in `integration_test.go:62-70`, so no forward is in flight when `nc.Drain()` runs.
- `medium` — make a dead pump observable: on a non-closed iterator error, either recreate the lane or flip a per-peer readiness flag that `health.ServeWithPprof` fails on, so a one-peer federation stall pages instead of hiding.
- `medium` — fail fast (or require) on an empty peer set in `main.go:123-127`; a no-op OUTBOX owner silently accumulates federation debt.
- `low` — attach `errcode.WithCause(err)` to both permanent classifications in `handler.go:341,345` and include the offending subject, so poison drops are diagnosable from one log line.
- `low` — move `federationForwardTimeout` into `config` with an `envDefault:"3s"`.
- `nitpick` — replace `integration_test.go:335`'s sleep with `require.Never` on the ack-floor assertion.

---

## 3. Architecture — 4 / 5

The mandated federation topology is implemented exactly as CLAUDE.md specifies — per-peer concurrent + FIFO lanes, `MaxDeliver=-1`, `MaxAckPending=1`, `pkg/subject`/`pkg/stream`/`pkg/outbox` used throughout — with two real gaps: the ordered lane is outside the shutdown drain, and the shared worker pool re-couples peers the per-destination consumers were built to isolate.

### Findings
- `medium` — the ordered (FIFO) lane's in-flight handler is not covered by the shutdown drain: `wg` only counts `drainPool`'s pump/message goroutines (`main.go:203-231`); `ocons.Consume(ctx, process)` callbacks are tracked by nothing, and `cc.Stop()` "discards buffered messages" without waiting for the running callback (nats.go v1.50.0 `jetstream/pull.go:58-61`, `:769`). `ConsumeContext.Closed()` is never awaited — `outbox-worker/main.go:173,181-186,187`.
  So `natsutil.Drain(nc)` can run while a membership forward's `js.PublishMsg` (3s timeout, `handler.go:47,58`) is in flight. Not data loss (unacked ⇒ redelivered under `MaxDeliver=-1`), but it produces spurious publish errors and an unclean exit on every deploy — the exact race `drainPool`'s own comment says the design must avoid.
- `medium` — one `sem` of `MAX_WORKERS` is shared by every per-destination concurrent lane — `main.go:114,140` — so the per-peer isolation the design argues for (`main.go:118-124`, `main.go:236-244`) holds at the JetStream ack-pending level but not at the pool level: a down peer's lane pulls up to `2*MaxWorkers` (`main.go:136`) and each forward holds a slot for the full 3s publish timeout (`handler.go:47`), so a single dead peer can occupy all 100 slots and delay healthy peers' forwards. A per-peer sub-budget (or a per-lane semaphore) would make the isolation claim true end to end.
- `medium` — an empty/unset `ALL_SITE_IDS` (`envDefault:""`, `main.go:39`) yields zero peers, therefore zero consumers, and the service only logs a warning and runs on (`main.go:125-127`) while `health.ServeWithPprof` reports healthy off the NATS check alone (`main.go:157-159`). A misconfigured deploy is an indistinguishable-from-healthy no-op that silently accumulates every federated event in OUTBOX. Repo-consistent (`broadcast-worker/main.go:358-363`), but here it disables the service's entire function, not a feature.
- `low` — `main.go` (300 lines) carries the consumer-config builders, `drainPool` and `federationPeers` alongside wiring, while their tests already live in `consumer_config_test.go` and `drainpool_test.go` with no matching source files. CLAUDE.md scopes `main.go` to config/wiring/startup/shutdown — `main.go:203-292` wants to be `consumer.go`.
- `low` — the malformed-event Ack-drop (`handler.go:52-56`) is a silent federation loss surfaced only as a `slog.Warn`; no counter/metric distinguishes it from normal traffic, so a producer regression emitting blank `DedupID`s degrades federation invisibly.
- `nitpick` — no `store.go`/`mock_store_test.go`; correct, not a deviation — `Handler` is a pure NATS→NATS relay holding only an injected `PublishFunc` (`handler.go:22-27`), which is exactly the CLAUDE.md testable-publish-injection pattern.

Verified compliant (no finding): sole OUTBOX ownership — only `outbox-worker` builds consumers on `stream.Outbox` (producers publish via `outbox.Publish` only); `bootstrap.go:44-54` sets only `Name + Subjects` and, when `BOOTSTRAP_STREAMS=false`, *verifies* rather than creates — stronger than required; durables `outbox-worker-{concurrent,ordered}-{dest}` per peer (`main.go:248`); ordered lane filters exactly `member_added|member_removed|room_renamed` on one lane (`pkg/outbox/outbox.go` `OrderedEventTypes`) with `MaxAckPending=1` (`main.go:279`); concurrent lane keeps the default budget; `MaxDeliver=-1` via `stream.WithUnlimitedRedelivery` applied to the *settings* so BackOff isn't clamped (`main.go:247`); subjects only via `subject.Outbox`/`subject.InboxExternal`/`ParseOutbox`; the two filter sets partition the stream and `outbox.Publish` rejects types outside it; config is a typed `caarlos0/env` struct with fail-fast on `MAX_WORKERS<=0`; `jsretry.Settle` + `jobguard.Run`, no bare `Nak()`.

### Recommendations
- `medium` — Track the ordered lane in shutdown: after `cc.Stop()`, select on `cc.Closed()` (or switch to `Drain()` + `Closed()`) inside the existing timeout step, so no forward is in flight when `natsutil.Drain(nc)` runs.
- `medium` — Give each peer its own slice of concurrency (per-lane semaphore of `MaxWorkers/len(peers)` with a floor, or a per-peer cap on the shared pool) so a dead peer cannot consume the whole pool for 3s per message.
- `medium` — Make an empty peer set explicit: either fail fast at startup (`ALL_SITE_IDS` is effectively required for this service) or fail the health check, so a misconfigured deploy cannot report healthy while forwarding nothing.
- `low` — Move `drainPool`, `buildLaneConsumerConfig`/`build{Concurrent,Ordered}ConsumerConfig` and `federationPeers` into `consumer.go`, matching the existing test-file split and CLAUDE.md's `main.go` remit.
- `low` — Emit a counter (o11y metric) for the malformed-event Ack-drop path in `handler.go:52` so producer-side `DedupID`/envelope regressions are alertable rather than log-only.

---

## 4. Test coverage — 1 / 5

Statement coverage is 36.9% — far below the CLAUDE.md 60% critical floor — and the uncovered mass is exactly the durable-retry wiring (message disposition, dedup publish, lane pump), even though the handler-level tests that do exist are genuinely good.

### Findings
- `critical` — Coverage 36.9% (141 stmts), below the 60% floor and the 80% requirement. Structurally: `main.go` holds 118 of the 141 statements and 89 of them are uncovered; `handler.go` + `bootstrap.go` (23 stmts) are at 100% — `outbox-worker/main.go:46`
  For the service that *is* the durable-retry guarantee, the untested 89 statements are the retry/park/forward wiring, not boilerplate.
- `high` — The message-disposition closure `process` — `jobguard.Run` (Ack-on-panic poison drop), `logctx.ConsumeContext`, `jsretry.Settle` (permanent→Ack, transient→NakWithDelay) — is inlined in `main()` and has zero tests — `outbox-worker/main.go:103-108`
  `broadcast-worker` extracts the identical concern as a named `guardedProcessor` covered at 100% (`broadcast-worker/main.go:547`); the precedent exists and is not followed here. Nothing pins that a panic Acks rather than crash-loops, or that a permanent error Ack-poisons rather than parking forever under `MaxDeliver=-1`.
- `high` — The production publish closure — the only site that sets `jetstream.WithMsgID(msgID)`, i.e. the entire cross-site idempotency guarantee — is uncovered, and the integration suite *reimplements* it instead of calling it — `outbox-worker/main.go:90-96`, duplicated at `outbox-worker/integration_test.go:76-81`
  Deleting `WithMsgID` from `main.go` would fail no test in the repo, while every redelivery would then double-apply at the destination.
- `high` — `drainPool` is 50% covered: the only test drives the closed-iterator path — `outbox-worker/drainpool_test.go:124`. Uncovered are the per-message dispatch goroutine, the `MaxWorkers` semaphore bound, and the unexpected-iterator-error branch — `outbox-worker/main.go:212-215`
  That branch logs and `return`s, permanently ending one peer's concurrent lane with the process still healthy and its OUTBOX backlog growing silently. No test asserts any behavior there.
- `medium` — No test proves `DedupID` actually dedups at the destination. The outage test asserts arrival order and `State.Msgs == 2`, but never re-forwards the same DedupID to assert a single stored INBOX message — `outbox-worker/integration_test.go:348-354`. The claim it would verify is stated at `outbox-worker/handler.go:56`.
- `medium` — `time.Sleep(300 * time.Millisecond)` is the synchronization point before the FIFO lane's `AckFloor`/`NumAckPending` assertions — `outbox-worker/integration_test.go:335`
  CLAUDE.md §3 forbids sleep-based synchronization. Under CI load the sleep can expire before first delivery, so `AckFloor == 0` passes vacuously and `NumAckPending == 1` flakes.
- `low` — Startup and shutdown decision points are untestable because they live in `main()` with `os.Exit`: `MAX_WORKERS <= 0` fail-fast (`main.go:55`), the zero-peer "events would sit unconsumed" warn (`main.go:124-127`), the four consumer-creation exits (`main.go:131-153`), and the worker-drain-timeout branch (`main.go:183-185`). `bot-message-worker/main.go:66` shows the `run(ctx) error` split that makes these reachable.
- `nitpick` — Integration uses an in-process `natsserver.NewServer` (`integration_test.go:31-43`) rather than `testutil.NATS(t)`, so `TestMain`'s `testutil.RunTests(m)` reaps nothing. Widespread repo precedent (`broadcast-worker/consumeloop_test.go`, `room-worker/integration_test.go`), and it gives better per-test isolation — noted, not a defect.

Quality of what *is* covered is high, not vanity: `HandleEvent` is 100% with permanent-vs-transient classification asserted via `errcode.IsPermanent` (`handler_test.go:52-94`), the two lane filter sets are golden-listed so a new `pkg/outbox` event type breaks the build (`consumer_config_test.go:264-308`), `federationPeers` is table-driven with descriptive subtests, and integration covers per-destination isolation and FIFO-through-outage. Tests are `package main`, independent (per-test site IDs, per-test embedded server), with no shared mutable state and no store mocks needed (pure relay, `PublishFunc` injected as a field per CLAUDE.md §4).

### Recommendations
- `high` — Extract `process` into a named `guardedProcessor(handler)` mirroring `broadcast-worker/main.go:547`, and unit-test the three dispositions against a fake `jetstream.Msg`: panic→Ack, permanent errcode→Ack, transient→`NakWithDelay` (never bare `Nak`).
- `high` — Extract the publish closure into a package-level `newJSPublish(js) PublishFunc`, call it from both `main.go` and `integration_test.go` in place of `jsPublish`, and assert the forwarded message carries `Nats-Msg-Id == DedupID`.
- `high` — Add a `drainpool_test.go` case that feeds N messages through a stub iterator and asserts (a) every message reaches `process`, (b) in-flight goroutines never exceed the semaphore cap, and (c) a non-`ErrMsgIteratorClosed` error terminates the pump — then decide whether that silent lane death should instead fail the health check.
- `medium` — Add an integration case that publishes the same OUTBOX event twice with one DedupID and asserts the destination INBOX holds exactly one message.
- `medium` — Replace `integration_test.go:335`'s sleep with `require.Eventually` on `NumAckPending == 1`, then assert `AckFloor == 0`.
- `low` — Split `main()` into `main()` + `run(ctx, cfg) error` and a `validate(cfg) error`, making the `MAX_WORKERS`, zero-peer and drain-timeout branches table-testable; this alone moves the bulk of the 89 uncovered statements.

---

## 5. Maintainability — 3 / 5

Small, genuinely well-reasoned service that stays generic over event type (adding a new OUTBOX type needs zero changes here), but `main.go` has outgrown its documented remit — ~110 lines of testable logic and four helper functions live inside the wiring file, and comment volume is drifting toward re-narrating CLAUDE.md.

### Findings
- `high` — `main()` is a 146-line straight-line function (`outbox-worker/main.go:46-192`) that mixes config parse, obs/NATS/JS init, handler construction, the `process` closure, the per-peer lane-creation loop, health server and the 5-stage shutdown. The lane loop (`:130-155`) is real branching logic — two `CreateOrUpdateConsumer` calls, iterator creation, four distinct fatal paths — and none of it is reachable from a test. This is the direct cause of the 36.9% statement coverage: `handler.go`, `bootstrap.go`, `drainPool` and the config builders are all covered; `main()` is not.
- `medium` — File organization drifts from CLAUDE.md's per-service layout. `main.go` owns `drainPool` (`:203-228`), `buildLaneConsumerConfig`/`buildConcurrentConsumerConfig`/`buildOrderedConsumerConfig` (`:242-281`) and `federationPeers` (`:286-299`). Tellingly, the tests are already named for files that do not exist: `outbox-worker/drainpool_test.go` and `outbox-worker/consumer_config_test.go` have no `drainpool.go` / `consumer_config.go` counterpart.
- `medium` — Comment volume in `main.go`: 72 of 300 lines (24%) are comment lines, including blocks of 9, 12, 7 and 9 lines (`main.go:194-202`, `:230-241`, `:257-263`, `:268-276`). The `buildLaneConsumerConfig` header (`:230-241`) restates the per-destination-isolation rationale that `main.go:117-122` already states and that CLAUDE.md §6 states a third time. Three copies of one rationale drift independently.
- `medium` — Two different drop idioms for the same class of defect in one 25-line handler: malformed subject and malformed body return `errcode.Permanent(...)` (`handler.go:41,45`), while a missing `Envelope`/`DedupID` — equally unrecoverable — takes a hand-rolled `slog.WarnContext` + `return nil` (`handler.go:50-55`). The second path bypasses `jsretry`'s poison accounting entirely, so the two undeliverable-event classes look different in logs and metrics for no stated reason.
- `low` — Orphaned doc comment: the block describing `TestIntegration_OutboxRoundTrip` (`integration_test.go:84-88`) is glued directly onto `assertConcurrentLaneForwards`'s own comment (`:89-95`), and the actual test at `:178` is undocumented. A refactoring leftover that now misdescribes the function it sits on.
- `low` — Test comments cite `"finding #1's fix"` and `"finding #2's regression guard"` (`integration_test.go:186`, `:370`). Those findings lived in `docs/reviews/`, which CLAUDE.md §5 requires be deleted before the PR — the references are permanently unresolvable.
- `low` — Removing a peer from `ALL_SITE_IDS` leaves its two durables (`outbox-worker-concurrent-{dest}`, `outbox-worker-ordered-{dest}`) on the stream forever; nothing in `main.go:130-155` deletes consumers for peers no longer configured. Orphan durables accumulate pending state silently across topology changes.
- `low` — `consumer_config_test.go:28-45` asserts the full 13-entry `FilterSubjects` literal. Adding one type to `pkg/outbox.ConcurrentEventTypes` fails a test in a service that legitimately needs no change — a change-detector cost paid at every partition edit.
- `nitpick` — `iters` and `orderedCtxs` (`main.go:128-129`) are parallel slices consumed by two near-identical stop loops (`:169-174`); one `[]func()` of stoppers collapses both.
- `nitpick` — 11 `slog.Error(...); os.Exit(1)` blocks in `main.go`; the 4 inside the peer loop (`:133,138,146,151`) also abandon consumers/iterators already created for earlier peers.

### Recommendations
- `high` — Extract `startLanes(ctx, js, streamName string, cfg config, process func(context.Context, jetstream.Msg), sem chan struct{}, wg *sync.WaitGroup) (stop func(), err error)` into a new `consumer.go`. It returns an error instead of calling `os.Exit`, so the per-peer loop, the dual-consumer creation and the partial-failure path become unit-testable against a fake JetStream (the same trick `bootstrap.go:90-93`'s `streamManager` already uses). This alone converts most of the uncovered statements.
- `medium` — Split `main.go` to match its test file names: `consumer.go` (three config builders + `federationPeers`) and `drainpool.go` (`drainPool`). `main.go` drops to ~150 lines of pure wiring, as CLAUDE.md's file organization specifies.
- `medium` — Collapse the three copies of the per-destination-isolation rationale to one — keep it on `buildLaneConsumerConfig`, reduce `main.go:117-122` and `:194-202` to a one-line pointer. Target the repo's 2-line comment ceiling.
- `medium` — Make the missing-`DedupID`/`Envelope` path return `errcode.Permanent(errcode.BadRequest("outbox event missing dedup id or envelope"))` like its two siblings, and delete the hand-rolled warn; `jsretry` then logs and Ack-poisons all three uniformly.
- `low` — Fix the orphaned comment at `integration_test.go:84-88` (move it onto `:178`) and rewrite the `"finding #1/#2"` references to state the invariant directly.
- `low` — Have `startLanes` list existing `outbox-worker-*` durables and delete those whose destination is absent from `federationPeers`, gated behind the same `Bootstrap.Enabled` flag so production topology stays ops-owned.
- `nitpick` — Replace `iters`/`orderedCtxs` with a single `stops []func()` appended in the loop; the shutdown stage becomes one range.

---

## 6. Integration — 4 / 5

The federation partition, subject builders, and forward lane are correct and integration-tested end-to-end; deductions are for a silent whole-service stall when `ALL_SITE_IDS` is unset, a dedup-window/backoff mismatch, and an untracked ordered-lane shutdown race.

### Findings
- `high` — `ALL_SITE_IDS` unset ⇒ `federationPeers` returns empty ⇒ **zero consumers are created** and every OUTBOX event sits unconsumed until retention deletes it; the only signal is a `slog.Warn` and the health check still reports healthy (`outbox-worker/main.go:123-127`, `main.go:157-159`). The service's own compose default collapses to exactly that (`ALL_SITE_IDS=${ALL_SITE_IDS:-${SITE_ID:-site-local}}` → peers = ∅, `outbox-worker/deploy/docker-compose.yml:18`). Producers derive `destSiteID` from user records, not from this list, so a peer added at a producer but forgotten here fails silently and permanently.
- `medium` — Forward dedup relies on JetStream's stream-level duplicate window, but neither `bootstrapStreams` nor `pkg/stream.Inbox`/`Outbox` sets `Duplicates` (`outbox-worker/bootstrap.go:43-46`, `pkg/stream/stream.go:66-82`), so the server default (2m) applies, while `jsretry.DefaultBackoff`'s tail is 10m (`pkg/jsretry/jsretry.go:52-58`) under `MaxDeliver=-1`. The comment at `outbox-worker/handler.go:56` ("Redelivery is idempotent (DedupID)") is only true inside that window; beyond it the guarantee falls back entirely on inbox-worker's HWM/idempotent writes.
- `medium` — The ordered lane's in-flight callbacks are not covered by shutdown's `wg.Wait()`: only `drainPool`'s pump and per-message goroutines call `wg.Add` (`outbox-worker/main.go:203-227`), while the ordered lane uses `ocons.Consume(ctx, process)` and shutdown calls only `cc.Stop()` (`main.go:149-154`, `main.go:172-174`). `o11ynats.ConsumeContext` exposes `Drain()` and `Closed()` (`o11y@v0.11.0/nats/jetstream.go:248-252`), neither used, so a forward in flight can race `natsutil.Drain` and fail. Not data loss (unacked ⇒ redelivered), but it inverts the FIFO lane's whole point at every rollout.
- `medium` — Producer list in CLAUDE.md §"Outbox" (`CLAUDE.md:273`) omits `bot-room-service`, which publishes `member_added`/`member_removed` through `outbox.Publish` (`bot-room-service/handler.go:676`, `:690`). Binding doc drift on the federation contract.
- `low` — `HandleEvent` derives destination and event type from the subject and never cross-checks `InboxEvent.DestSiteID`/`Type` in the forwarded envelope (`outbox-worker/handler.go:39-59`); inbox-worker never reads `DestSiteID` either. A producer that builds an envelope for site X but a subject for site Y is silently misrouted with no assertion anywhere.
- `low` — `federationForwardTimeout = 3 * time.Second` is a hardcoded const (`outbox-worker/handler.go:31`) rather than a `caarlos0/env` knob; cross-gateway RTT is deployment-dependent.
- `nitpick` — `model.InboxEvent` carries `bson` only on `Timestamp` (`pkg/model/event.go:285-291`), violating the "both `json` and `bson`" struct-tag rule that `OutboxEvent` (`:295-300`) satisfies.

**Verified clean:** the two filter sets are disjoint (`pkg/outbox/outbox_test.go:14-24`) and jointly cover all 16 types any producer emits — room-service (`role_updated`, `subscription_read`, `thread_read`, `thread_read_all`, `room_restricted`, `subscription_mute_toggled`/`favorite_toggled`/`section_moved`/`opened`), room-worker (`member_added`/`member_removed`/`room_renamed`/`member_joinedat_refreshed`), message-worker (`thread_unread_added`, `thread_subscription_upserted`), broadcast-worker (`subscription_mention`), bot-room-service (membership) — with the 5 `user_*` types correctly excluded (user-service publishes direct-to-INBOX). Gaps cannot go silent: `outbox.Publish` rejects any type outside `knownEventTypes` (`pkg/outbox/outbox.go:89-91`), and consumer `FilterSubjects` are built from the same slices (`outbox-worker/main.go:249-252`), so adding a type to a set auto-creates its lane. Subjects are always `pkg/subject` builders — zero raw `fmt.Sprintf` subject construction outside `pkg/subject` — and match CLAUDE.md exactly: `chat.outbox.{origin}.{dest}.{eventType}` (`pkg/subject/subject.go:231`) → `chat.inbox.{dest}.external.{eventType}` (`:290`, used at `handler.go:59`); no legacy `outbox.{siteID}.to.…` form exists. Timestamps are set at the publish site via `time.Now().UTC().UnixMilli()` for `OutboxEvent` (`pkg/outbox/outbox.go:109`) and from the producer's own event clock for the envelope, and outbox-worker forwards `Envelope` verbatim so neither is rewritten. `DedupID` is required (`:92-94`) and rides both hops as `Nats-Msg-Id`. Request-ID propagates via `logctx.ConsumeContext` → `natsutil.NewMsg` (`main.go:105`, `pkg/natsutil/request_id.go:68-74`). Every forwarded type has an inbox-worker handler (`inbox-worker/handler.go:225-272`). No `chat.user.` handlers ⇒ `docs/client-api.md` not implicated. `jsretry.Settle` used throughout; no bare `Nak()`.

### Recommendations
- `high` — Fail fast (or expose a red readiness probe) when `federationPeers` is empty while the OUTBOX stream is non-empty, instead of `slog.Warn`; and stop defaulting `ALL_SITE_IDS` to `SITE_ID` in `deploy/docker-compose.yml`.
- `medium` — Set an explicit `Duplicates` window on INBOX/OUTBOX in `pkg/stream` that exceeds `jsretry.DefaultBackoff`'s 10m tail, or document per-type that inbox-worker's idempotency (not the window) is the real guarantee and drop the misleading `handler.go:56` comment.
- `medium` — Switch ordered-lane shutdown from `cc.Stop()` to `cc.Drain()` + `<-cc.Closed()` with a bounded wait, so FIFO forwards finish before `nc.Drain()`.
- `medium` — Add `bot-room-service` to CLAUDE.md's OUTBOX producer list (§Outbox subject naming and §JetStream Streams).
- `low` — Assert `evt.Envelope`'s `type`/`destSiteId` against the parsed subject in `HandleEvent`, treating a mismatch as `errcode.Permanent` — cheap guard against a producer building envelope and subject from different variables.
- `low` — Promote `federationForwardTimeout` to a `FORWARD_TIMEOUT` env knob with `envDefault:"3s"`.
- `low` — Add a table-driven test asserting `ConcurrentEventTypes ∪ OrderedEventTypes` equals the set of types actually passed to `outbox.Publish` across producers, so a new producer type fails a test rather than a runtime publish.
