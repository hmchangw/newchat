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
