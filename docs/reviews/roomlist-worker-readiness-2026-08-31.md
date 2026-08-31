# roomlist-worker — Production Readiness Review

**Service:** `roomlist-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.7 / 5

The strongest service in this audit round, and the only one where no expert found a `critical` and only one found a `high`. It is the worker CLAUDE.md's own architecture notes single out — the one that took the room/subscription writes off `broadcast-worker`'s fan-out path so no MongoDB failure can block delivery — and it honours that contract verifiably: `MaxDeliver=-1`, coalescing with `msgbucket.NewerRow` tie-breaks, bounded batches, back-pressure, a narrow sonic projection, and no read path at all. The tested logic is unusually good: the coalescer, the watermark filters and the settle semantics are each pinned by named regression tests.

What holds it at 3.7 is organisational rather than behavioural. **`main.go` has quietly absorbed five concerns** — the consume loop, the readiness state machine, the self-SIGTERM escalation, the flush-budget validator and the consumer config — four of which already have dedicated `*_test.go` files with **no matching source file**, while `handler.go` contains no handler. And the consumer runs a **third pattern CLAUDE.md does not sanction**: `cons.Messages()` (the high-throughput shape) driven by exactly one goroutine with no semaphore and no `MAX_WORKERS` knob. The reasoning behind that choice is sound and documented; the deviation from binding project law is not.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 1 | 13 | 17 | 11 | **42** |

---

## 2. Go code quality — 4 / 5

Disciplined, idiomatic Go with correct `errcode` tiering, `errors.Is/As` throughout, and slog-only structured logging; deductions are for a handful of literal CLAUDE.md Section-3 violations and one latent nil-error trap.

### Findings
- `low` — Three store methods return a bare `err`, which CLAUDE.md forbids outright ("Never return bare `err`") — `roomlist-worker/store_mongo.go:144`, `:155`, `:166`.
  Mitigated (not cured) by `mongoutil.Collection.BulkWrite` wrapping as `bulk write %s: %w` (`pkg/mongoutil/collection.go:176`) and `flush.write` adding `flush %s: %w` (`roomlist-worker/flush.go:154`), so the chain does carry context — but the rule is stated without exception and the file-header comment at `store_mongo.go:14-17` documents the *reason* for the typed collection, not for skipping the wrap.

- `low` — `consumeState.Check` dereferences a `*error` that could hold a nil error, producing `%!w(<nil>)` instead of a diagnosis — `roomlist-worker/main.go:319-325`.
  `stopped(err)` is currently only reached from `main.go:344` inside an `if err != nil`, so it is latent, not live. The shadowing of `err` for a `*error` (`if err := s.reason.Load(); err != nil`) is what makes the trap easy to reintroduce; storing a non-pointer sentinel or guarding `*err == nil` removes it.

- `low` — `flushOutcome.mongoErrs` puts the driver's raw `BulkWriteException.Error()` text into a log field — `roomlist-worker/flush.go:119`, emitted at `flush.go:204`.
  The rationale at `flush.go:114-119` is sound and the writes here carry only `roomId`/`u.account`/timestamps, never message content, so no body can leak today. It is a standing hazard only if this batch ever grows a content-bearing field; worth a one-line comment pinning that invariant.

- `low` — Audit-coverage gap, not a service defect: per `GLOBAL_PREP.md`, gosec (0 findings) and the 18 repo-owned semgrep rules (0 findings, 2/2 fixture tests pass) are clean, but `govulncheck` and the semgrep registry packs could not run — `vuln.go.dev` and `semgrep.dev` are 403-blocked by session egress. Dependency-CVE and third-party-pattern coverage for this service is therefore unverified.

- `nitpick` — `NewMongoStore` is exported but returns the unexported `*mongoStore`, inside `package main` where nothing external can consume either — `roomlist-worker/store_mongo.go:23`.
  Contradicts "export only what other packages consume". It matches sibling precedent (`broadcast-worker/store_mongo.go:53`, `upload-service/store_mongo.go:19`), so this is fleet-wide drift rather than a local lapse; `media-service`/`search-service` use the correct `newMongoStore`.

- `nitpick` — Stale suppression: `// #nosec G601` plus `// nosemgrep: gosec.G601-1` guard a loop-variable-alias that cannot occur, and the justification says so ("go.mod requires go 1.25; since 1.22 each iteration has its own loop variable") — `roomlist-worker/store_mongo.go:68-69`.
  A suppression whose own comment explains the rule is inapplicable is dead weight that trains readers to skim `#nosec` lines.

- `nitpick` — 73 top-level test functions against only 10 `t.Run` subtests across the package (`batch_test.go`, `flush_test.go`, `consumeloop_test.go`), where CLAUDE.md Section 4 prefers table-driven form for input/output variations of one behaviour. `batch_test.go:38-183` is eleven near-identical one-assertion functions over `batch.add` that read as one natural table; `handler_test.go:14` and `config_test.go:29` already do it correctly.

### Recommendations
- `low` — Wrap the three `BulkWrite` returns in `store_mongo.go` with this function's own context (`fmt.Errorf("bulk update room last message: %w", err)`), or add an explicit header comment stating the double-wrap is deliberately avoided as redundant. Either satisfies the rule; silence does not.
- `low` — Make `consumeState.reason` hold a non-nil value by construction (store a sentinel when `err == nil`) and rename the loaded variable off `err` so the `*error`/`error` distinction is visible at the call site — `main.go:319-325`.
- `low` — Add a one-line invariant comment beside `flushOutcome.mongoErrs` (`flush.go:119`) recording that the batch carries no message content, so the "never log full message bodies" rule stays checkable if fields are added.
- `nitpick` — Rename `NewMongoStore` → `newMongoStore` (`store_mongo.go:23`); it is a mechanical, zero-risk change within `package main`, and worth raising fleet-wide since four services already have it right.
- `nitpick` — Delete the `#nosec G601` / `nosemgrep` pair at `store_mongo.go:68-69` and confirm `make sast` still reports clean for the file.
- `nitpick` — Collapse `batch_test.go:38-183` into one table-driven `TestBatch_Add` with the existing function names as case names; it preserves every assertion and removes the largest block of structural duplication in the package.

---

## 3. Architecture — 4 / 5

Boundaries, DI and the store interface are exemplary and the MaxDeliver=-1/ownership contract is verifiably honored; the deductions are file-organization and consumer-pattern deviations from CLAUDE.md's per-service law.

### Findings
- `medium` — Message-handling logic lives in `main.go`, not `handler.go`. `consumeLoop` (sonic unmarshal, `logctx.ConsumeContext`, settle-on-malformed, flusher hand-off), `consumeState`, `validateFlushBudget` and `buildConsumerConfig` are all in a 400-line `main.go` — `roomlist-worker/main.go:334-384`, `:242-274`, `:388-400`. CLAUDE.md reserves `main.go` for config parsing, wiring, startup and shutdown; `handler.go` (`roomlist-worker/handler.go`) holds only the pure `deriveIntents` mapper. The tell is `consumeloop_test.go` — a 396-line test file with no production counterpart of that name.
- `medium` — Third, unsanctioned JetStream consumer pattern. `cons.Messages()` is used (`main.go:154`) but with a single consume goroutine and no `chan struct{}` semaphore and no `MAX_WORKERS` knob (`main.go:170-173`, `wg.Add(1)` for exactly one goroutine). CLAUDE.md sanctions `cons.Messages()` + semaphore sized by `MAX_WORKERS` (high-throughput) or `cons.Consume()` (sequential). The choice is reasoned at `main.go:327-331` (no per-message I/O), but the result is a sequential consumer wearing the pull-iterator's clothes, with no operator-tunable concurrency knob if the derive path ever grows an I/O call.
- `low` — `store.go` omits the `//go:generate mockgen` directive and the service has no `mock_store_test.go`; hand-written stubs in `flush_test.go` are used instead. Deliberate and documented at `roomlist-worker/store.go:8-11`, but CLAUDE.md's per-service layout names both artifacts explicitly, and hand stubs drift silently when `Store` gains a method.
- `low` — Bootstrap derives the stream's `Subjects` from the consumer filter rather than the stream config. `main.go:128` passes `wiring.CanonicalWildcard` and `bootstrap.go:216-219` writes it as `Subjects`, bypassing `wiring.CanonicalStream.Subjects` (`pkg/stream/pipeline.go:59-62`). Identical today; if `MESSAGES-CANONICAL` ever gains a second subject, a dev `CreateOrUpdateStream` here silently narrows the stream instead of matching `pkg/stream`.
- `low` — `DeliverPolicy` is overridden to `New` inside the service (`main.go:398`), contradicting the project-wide invariant `pkg/stream.DurableConsumerDefaults` documents and sets (`pkg/stream/consumer.go:38-47`, `DeliverAllPolicy`). The reasoning is sound, but the override belongs alongside `stream.WithUnlimitedRedelivery` as a `pkg/stream` option so the invariant stays enforceable; as written, a durable deleted during an incident skips its backlog, and nothing outside this file says so.
- `nitpick` — Service-specific env vars are unprefixed: `FLUSH_INTERVAL`, `FLUSH_TIMEOUT`, `HEALTH_ADDR` (`main.go:50,55,56`), while CLAUDE.md asks for a service-name prefix; `deploy/user/docker-compose.yml:20` maps `ROOMLIST_FLUSH_INTERVAL` onto the unprefixed name. Consistent with `broadcast-worker/main.go:121`, so this is fleet-wide drift, not a local defect.
- `nitpick` — CLAUDE.md says `bootstrapStreams` "no-ops when `Enabled=false`"; this service (`bootstrap.go:224`) and every sibling checked (`broadcast-worker/bootstrap.go:38`, `message-worker/bootstrap.go:61`, `notification-worker/bootstrap.go:44`) verify the stream and fail fast instead. The code is better than the rule; the rule is stale.

**Boundary verified (no finding).** Ownership is genuinely exclusive: `broadcast-worker/store_mongo.go:158` and `preview_writer.go:65` confirm broadcast-worker touches only `preview*`; no other non-test writer of `lastMsgAt`/`lastUserMsgAt`/`lastMentionAllAt`/`hasMention` exists outside `room-service` (indexes/projections) and read-side `user-service`. `MaxDeliver=-1` is set correctly and once, via `stream.WithUnlimitedRedelivery` **before** the backoff schedule is derived (`main.go:392`) — the exact ordering `pkg/stream/consumer.go:162-176` warns about. Shutdown uses `shutdown.WaitOn` with the documented order plus a correctly-placed flusher drain between `wg.Wait()` and `nc.Drain()` (`main.go:189-221`). Store interface is consumer-defined with three methods (`store.go:16-30`), constructor DI throughout, `NewMongoStore` returns a struct, and `streamManager`/`messageIterator` are minimal local interfaces for testability. Config is a typed `caarlos0/env` struct with `mongoutil.PoolConfig` and `stream.ConsumerSettings` mounted as named prefixed fields — no re-declared shared knobs, no `os.Getenv`.

### Recommendations
- `medium` — Move `consumeLoop`, `consumeState`, `requestSelfShutdown` and `buildConsumerConfig` out of `main.go` into `handler.go` (or a `consumeloop.go` matching the existing test file), leaving `main.go` as config + wiring + shutdown.
- `medium` — Either add a `MaxWorkers` knob and the standard semaphore, or switch to `cons.Consume()` and state the sequential choice in `deploy/README.md`; do not leave a third pattern undocumented at the fleet level.
- `low` — Add the `//go:generate mockgen` directive to `store.go` and either generate `mock_store_test.go` or amend CLAUDE.md to sanction hand stubs for order-asserting flush tests.
- `low` — Pass `wiring.CanonicalStream.Subjects` into `bootstrapStreams` instead of the consumer wildcard.
- `low` — Promote the `DeliverNew` choice into `pkg/stream` as a named option (mirroring `WithUnlimitedRedelivery`) so the delivery-policy invariant remains owned by one package.

---

## 4. Test coverage — 2 / 5

Statement coverage is 65.9%, below the CLAUDE.md §4 80% floor (capping this dimension at 2), but the shortfall is almost entirely unwired `main()` — the tested logic is unusually high-quality, with the coalescer, watermark filters and MaxDeliver=-1 settle semantics all pinned by named regression tests.

### Findings
- `high` — Coverage is **65.9% (279 stmts)**, under the 80% floor mandated by CLAUDE.md §4 — `roomlist-worker/main.go:65`
  95 uncovered statements; **83 of them are inside `main()`** (lines 65–222), which is pure wiring. Excluding `main()`, the service is at **93.9%** — this is not vanity coverage, but the number as reported does not merge.
- `medium` — `requestSelfShutdown` is 0% covered — `roomlist-worker/main.go:265-274`
  This is the mechanism that turns a dead consume loop into a pod restart (the only actor that can rebuild the iterator). Every test substitutes a recorder for `consumeState.onUnexpectedStop` (`consumeloop_test.go:227`, `:246`), so the real SIGTERM raise and its two `slog.Error` fallbacks never execute. It is testable: install `signal.Notify` on a buffered channel in-test and assert delivery.
- `medium` — A **transient** failure in the third write stage (`subscription mentions`) is untested; that early return is uncovered — `roomlist-worker/flush.go:176-178`
  `failWith["mentions"]` is only ever set to `permanentWriteErr(9)` (`flush_test.go:119`); transient failures are only injected on `"rooms"` (`:106`, `:131`). If that `if err != nil { return }` were deleted, `permanentErrs` would be empty and the batch would return `flushOutcome{}` → **Ack**, silently dropping mention badges on a Mongo blip, and the suite would stay green. Stages 1 and 2 have the guard tested; stage 3 does not.
- `medium` — No integration test exercises the JetStream side of the MaxDeliver=-1 contract — `roomlist-worker/integration_test.go:64`
  `flushOne` drives the real flusher against real Mongo, but always the happy path; redelivery, Nak-with-backoff, and the poison-batch Ack-drop (`classifyFlushErr`) are proven only against `stubStore`. Nothing tests that a real `mongo.BulkWriteException` from a live server carries the codes `permanentMongoCodes` (`flush.go:216-227`) expects — the allow-list is asserted only against hand-built exceptions (`flush_test.go:71-78`), so a driver-version change in `BulkWriteException.ErrorCodes()` would flip poison batches to infinite retry undetected.
- `low` — `flushNow`'s zero-timeout fallback to `flushloop.DefaultFinalTimeout` is uncovered — `roomlist-worker/flush.go:78-80`
  Every `newFlusher(store, 0, 0)` test has budget 0, so the early-drain path never fires; the bound that keeps an early drain from running unbounded is never exercised.
- `low` — No `mock_store_test.go`; `Store` has no `//go:generate mockgen` directive, contrary to CLAUDE.md §1/§4 — `roomlist-worker/store.go:7-10`
  Hand-written stubs are used instead. The deviation is documented in-code and defensible (order and context-cancellation assertions), but it is a stated-rule deviation, and `stubStore` (`flush_test.go:21`) has an unsynchronised `order`/`rooms` write path (`:44-47`) that `-race` only tolerates because flushes are sequential.
- `nitpick` — `pretouchJSON` (`roomlist-worker/pretouch.go:16`) and `NewMongoStore` (`store_mongo.go:23`) are 0% in the unit profile; `NewMongoStore` and the `BulkAdvanceLastSeen`/`BulkSetMentions` loop bodies (`store_mongo.go:149-153`, `:160-164`) *are* covered by the integration suite, which the profile excludes.

## Positive signal (not findings)
Table-driven with descriptive subtest names throughout (`main_test.go:40`, `flush_test.go:289`, `handler_test.go:14`); no `t.Parallel`, no package-level mutable state, `captureLogs` restores `slog.Default` via `t.Cleanup` (`consumeloop_test.go:305`); no real DB/NATS in unit tests (`fakeIterator`, `fakeJetstreamMsg`); integration file carries `//go:build integration`, `TestMain(m){testutil.RunTests(m)}`, and `testutil.MongoDB` with no inline containers. The same-millisecond `NewerRow` tie is pinned in both directions at unit *and* integration level (`batch_test.go:58`, `integration_test.go:161`), and the mixed permanent/transient join invariant has a named guard (`flush_test.go:158`).

### Recommendations
- `high` — Close the gap with targeted tests, not bulk: the three below plus a small `main()` extraction (move the post-config wiring into a `run(cfg) error`) would clear 80% without diluting the suite.
- `medium` — Add `TestFlusher_TransientMentionsFailureNaks`: `failWith{"mentions": errors.New("connection refused")}`, assert the held messages are Nak'd, not Ack'd.
- `medium` — Add `TestRequestSelfShutdown_RaisesSIGTERM`: register `signal.Notify` on a buffered channel, call `requestSelfShutdown()`, assert receipt within a bounded select.
- `medium` — Add an integration test that drives a real `BulkWriteException` (e.g. a `DocumentValidationFailure` via a JSON-schema-validated `rooms` collection, or a duplicate `_id`) through `classifyFlushErr`, asserting it classifies permanent — pinning the allow-list against the real driver.
- `low` — Add `TestFlusher_FlushNowWithoutTimeoutUsesTheDefaultBound`: construct with `flushTimeout: 0`, call `flushNow`, assert the store saw a deadline.
- `low` — Either add the `//go:generate mockgen` directive and a generated `mock_store_test.go`, or note the stub exception in `store.go` as an explicit, reviewed departure; also guard `stubStore.order`/`rooms` with the existing mutex.

---

## 5. Maintainability — 4 / 5

Genuinely well-factored — small pure functions, a real per-concern split (`flush.go`/`batch.go`/`projection.go`/`store_mongo.go`), and comments that explain WHY — but `main.go` has quietly absorbed two whole concerns that already have their own test files, and comment volume has outgrown the code it annotates.

### Findings
- `medium` — `main()` is 157 lines of straight-line wiring (config validation → obs → Mongo → NATS → bootstrap → consumer → flusher goroutine → iterator → signal → consume goroutine → health → 8 shutdown stages) — `roomlist-worker/main.go:65-222`
  Adding one dependency means reading all 157 lines to find the right insertion point and the matching shutdown stage; the flusher's start (`:146-150`) and its teardown (`:207-215`) are 60 lines apart with no compiler link between them.

- `medium` — `main.go` also holds the consume loop, the readiness state machine, the self-SIGTERM escalation, the flush-budget validator and the consumer config — five concerns, four of which already have dedicated test files with no matching source file — `roomlist-worker/main.go:226-400` vs `consumeloop_test.go`, `config_test.go`, `main_test.go`
  The tests were split per concern; the source was not. `consumeloop_test.go` (396 lines) tests code that lives in `main.go`, so the obvious "where does the loop live" lookup fails.

- `medium` — `handler.go` contains no handler: it is the pure `writeIntents`/`deriveIntents` derivation. The actual per-message handler (unmarshal → settle-on-malformed → add → inline drain) is an anonymous closure inside `consumeLoop` — `roomlist-worker/handler.go:14-97`, `roomlist-worker/main.go:364-382`
  CLAUDE.md §1 defines `handler.go` as "request/message handling logic". A reader following the convention finds the wrong file, and the real handler is only reachable through the loop.

- `medium` — Comment lines are 42% of `flush.go` (127/299) and 33% of `main.go` (130/400), with six blocks of 12–19 consecutive comment lines — `flush.go:34-51` (18-line `newFlusher` doc), `flush.go:122-140`, `main.go:276-294`
  The content is correct and load-bearing, but at this density the invariants are prose-only: `flush.go:208-216` documents a cross-file invariant ("`out.err` is never a mixture") whose enforcement is "three frames away" in `write`, and only a named test guards it.

- `low` — `deploy/bot/Dockerfile` and `deploy/user/Dockerfile` are byte-identical (`diff` exits 0); the two compose files differ only in name/`MODE`/dockerfile path — `roomlist-worker/deploy/bot/`, `roomlist-worker/deploy/user/`
  Two copies of the same build recipe drift silently; only the compose `MODE` var actually varies.

- `low` — Untraceable references: `#382`, `#396`, `#467`, and "the design spec's 'Consistency trade-off' section" with no path — `handler.go:23`, `handler.go:36`, `handler.go:38`, `store.go:28`, `store_mongo.go:121`
  The spec does exist (`docs/superpowers/specs/2026-08-13-unread-worker-extraction-design.md`) but is never named, so the reasoning behind the read-floor trade-off is unreachable from the code.

- `low` — `writeIntents` is passed by value through two layers, each needing a `//nolint:gocritic // hugeParam` suppression — `flush.go:65-66`, `batch.go:75-76`
  A duplicated lint waiver is the signal the struct has outgrown by-value; `*writeIntents` removes both.

- `nitpick` — `heldMsg` stores a `context.Context` as a struct field — `batch.go:37-40`. Justified by the comment (settle-time request id), but it is the one place the batch abstraction leaks request scope into buffer state.

- `nitpick` — Two independent fake-message types in tests (`fakeMsg`, `fakeJetstreamMsg`) — `batch_test.go:17`, `consumeloop_test.go:23`; and `Store`/`NewMongoStore` are exported from `package main`, which nothing can import — `store.go:16`, `store_mongo.go:23`.

### Recommendations
- `medium` — Extract `consumeloop.go` (`messageIterator`, `consumeLoop`, and the per-message closure lifted to a named `handleMessage(ctx, msg, f)`) and `consumestate.go` (`consumeState`, `requestSelfShutdown`, `Check`) from `main.go`. Existing test files already carry those names; this is a pure move.
- `medium` — Move `deriveIntents`/`writeIntents` to `intents.go` and let `handler.go` hold the extracted `handleMessage`, restoring the CLAUDE.md file-role mapping.
- `medium` — Group the flusher's start/stop into one `startFlusher(...) (f *flusher, stop func(context.Context) error)` returning its own shutdown stage, and move config validation into `config.go` (`cfg.validate()`). Target `main()` under ~70 lines.
- `low` — Compress the essay comments to ≤2 lines each (repo's own `remove_comments` norm), and convert the two cross-file invariants — the no-mixed-join rule (`flush.go:208-216`) and the NewerRow/preview agreement (`batch.go:81-87`) — into named assertions or a shared helper so they fail loudly rather than by review.
- `low` — Collapse `deploy/bot/` and `deploy/user/` to one `deploy/Dockerfile` plus two compose files that differ only in `MODE`; parameterize `IMAGE_NAME` in a single pipeline.
- `low` — Replace `#NNN` and "the design spec" with the concrete doc path; pass `*writeIntents` and delete both `hugeParam` waivers.

---

## 6. Integration — 4 / 5

Integration surface is small and unusually disciplined — the central cross-service tie-break claim holds end to end, subjects come only from `pkg/subject` via `stream.Resolve`, and the service publishes nothing — but the one test guarding its `pkg/model` contract has a hole on the highest-consequence field, and its shared-collection index dependency is unverified.

**Verified clean**
- **NewerRow agreement (central claim): PASS.** Both coalescers order by `msgbucket.NewerRow` — `roomlist-worker/batch.go:87` (room pointer) and `:97` (user position), `broadcast-worker/preview_writer.go:114,120` (preview key + body). The Mongo-side guard is the same comparator in BSON (`roomlist-worker/store_mongo.go:42-49`: `$not/$gte` OR same-instant `lastMsgId $lt`), pinned by `store_mongo_test.go:44-46` and `integration_test.go:152-179`. Both sides compare at millisecond precision (`pkg/msgbucket/order.go:27`), matching BSON date granularity.
- **Disjoint halves: PASS.** `roomlist-worker` is the sole writer of `rooms.lastMsgAt/lastMsgId/lastUserMsgAt/lastMentionAllAt` (`store_mongo.go:110-137`); `broadcast-worker/store_mongo.go:156-158` touches only `preview*` under its own `previewAsOf` watermark; `message-worker/store_mongo.go:138-152` writes only `thread_rooms`.
- **No publish sites**, so the event-`Timestamp` rule is vacuous here — zero `Publish`/`QueueSubscribe`/`natsrouter` registrations, zero raw `fmt.Sprintf` subject construction. No OUTBOX/INBOX participation, no `chat.user.…` handler, hence no `docs/client-api.md` obligation (confirmed: no mention in the doc or its derived views).
- **Stream/consumer wiring** is `stream.Resolve` → `subject.MsgCanonicalWildcard` (`main.go:127-135`); `bootstrap.go:242-244` sets only `Name + Subjects`; `buildConsumerConfig` derives `BackOff` via `stream.DurableConsumerDefaults` (`main.go:394`) and never hardcodes it.
- **Projection tags** match `model.MessageEvent` (`pkg/model/event.go:29-35`) and `model.Message` (`pkg/model/message.go:10-42`) field for field.

### Findings
- `medium` — the projection drift-guard asserts 8 of the 9 fields it decodes, omitting `Type` — `roomlist-worker/projection_test.go:57-68`; the fixture never sets it either (`:24-40`).
  `Type` is the only input to `SystemMsg` (`handler.go:64`), which selects the `lastUserMsgAt` freeze. A rename of `model.Message`'s `json:"type"` would silently decode empty, classify every system message as a user message, and promote a system timestamp into `lastUserMsgAt` — which `roomPointerUpdate`'s sticky `$ifNull` then keeps forever (`store_mongo.go:129-136`). This is the one drift with an unrecoverable outcome and it is the one field left unpinned.
- `medium` — the service issues `(roomId, u.account)` filters on the shared `subscriptions` collection twice per flush at full message rate (`store_mongo.go:151`, `:55-61`) but never verifies the index — `mongoutil.WarnMissingIndexes` is absent from the whole service.
  `inbox-worker/main.go:560-562` does exactly this warn for the same-shape query at far lower volume, and the index is owned by `room-service/store_mongo.go:125`. A dropped index degrades this worker into a collscan per bulk model, which under `MaxDeliver=-1` becomes a flush-timeout retry loop rather than a visible error.
- `low` — `bootstrapStreams(ctx, js, streamName, subjectFilter, enabled)` (`bootstrap.go:240`) deviates from CLAUDE.md's mandated `bootstrapStreams(ctx, js, siteID, enabled)`, and the disabled branch verifies the stream (`:250-252`) rather than no-opping as CLAUDE.md specifies. The fail-fast is defensible; the contract drift is not — CLAUDE.md wins. `broadcast-worker/main.go:330` carries the identical drift, so it is a two-service convention, not a local slip.
- `low` — two writers of `subscriptions.lastSeenAt` with different monotonicity: `$max` here (`store_mongo.go:152`) vs `$set` in `room-service/store_mongo.go:1085`. A `messageRead` landing after a sender advance can regress the value this service's comment (`handler.go:34-41`) calls read-floor input for `MinSubscriptionLastSeenByRoomID`.
- `nitpick` — `pkg/subject/subject.go:666-669` documents `MsgCanonicalMessageWildcard` as excluding a `.teams.batch` subject on this stream, but that subject is `chat.teams.msg.canonical.…` on `MESSAGES-TEAMS` (`:353`). Binding `.>` here is therefore safe; the comment implies a hazard that does not exist and could push a future consumer to the wrong builder.

### Recommendations
- `medium` — set `Type: model.MessageTypeRoomRenamed` (or similar) in `canonicalPayload` and add `assert.Equal(t, full.Message.Type, got.Message.Type)` to `TestEventProjection_MatchesFullDecode`; assert `SystemMsg` in `TestEventProjection_DerivesTheSameIntents`.
- `medium` — add an `ensureIndexes` on `mongoStore` calling `mongoutil.WarnMissingIndexes(ctx, subCol, "roomId_1_u.account_1")` at startup, mirroring `inbox-worker`.
- `low` — align `bootstrapStreams` with the CLAUDE.md signature, or amend CLAUDE.md §6 to sanction the `(streamName, subjectFilter)` form plus the verify-when-disabled branch, since two services now depend on it.
- `low` — document in `deploy/README.md` which of `$max` and `$set` is authoritative for `lastSeenAt`, so the read-floor contract is explicit rather than emergent.
- `nitpick` — correct the `MsgCanonicalMessageWildcard` doc comment to name the real Teams subject prefix.

---

## 7. Performance — 4 / 5

Genuinely strong performance engineering — coalescing, bounded batches, back-pressure, a narrow sonic projection and zero read-path — with a handful of real but bounded hot-path and sizing gaps.

### Findings
- `medium` — `mention.Parse` runs a full RE2 scan over every created message's content, including the large majority containing no `@` at all — `roomlist-worker/handler.go:59` (and `:84` on edits).
  The pattern is `(^|\s)@(…)` (`pkg/mention/mention.go:15`); the leading alternation defeats Go's literal-prefix optimization, so the whole (up to 20KB) body is walked, and `FindAllStringSubmatch` allocates a `[][]string` of 4 submatches per hit. This is the single consume goroutine — the worker's entire throughput ceiling.

- `medium` — the mention budget is checked *after* the merge, so one message can overshoot it, and `BulkSetMentions` never chunks — `roomlist-worker/flush.go:66-69`, `roomlist-worker/batch.go:114-119`, `roomlist-worker/store_mongo.go:158-166`.
  `newFlusher`'s own comment says `mention.Parse` caps neither token count nor input; a 20KB body yields ~5k accounts, all added under one `f.add` before the `>= mentionBudget` test can fire. The map can therefore reach ~9k (budget 4000 at defaults, `main.go:145`) and `BulkSetMentions` emits one `UpdateOne` model per entry in a single un-chunked `BulkWrite` under `FLUSH_TIMEOUT` (10s) with majority write concern — exactly the "cannot complete inside flushTimeout" livelock the budget was written to prevent.

- `low` — the service has no `EnsureIndexes`; every `BulkAdvanceLastSeen`/`BulkSetMentions` model filters on `roomId` + `u.account` and depends on an index another service owns — `roomlist-worker/store_mongo.go:151`, `:162`, created only in `room-service/store_mongo.go:124-125`.
  CLAUDE.md §6 (MongoDB) says indexes are created in the store constructor or an `EnsureIndexes` at startup. Deployed against a DB where room-service has not yet run, each flush becomes up to `MaxAckPending` COLLSCANs on `subscriptions`.

- `low` — a deterministically panicking message has no drop path under `MaxDeliver=-1` — `roomlist-worker/main.go:364`.
  `jobguard.Guard` recovers but does not settle, so the message is neither held nor acked and re-occupies an ack-pending slot every `AckWait` forever. `jobguard.Run` is the Ack-on-panic variant (`pkg/jobguard/jobguard.go:47-50`). Note the nuance that makes this only `low`: a panic *after* `f.add` would double-settle under `Run`, so the fix is a guard scoped to the derive step, not a blanket swap. Malformed payloads are handled correctly by contrast (`main.go:368-373`).

- `low` — the room stage emits two write models per room into one unordered bulk, both targeting the same document — `roomlist-worker/store_mongo.go:72` and `:95`.
  Justified (the `$max` dimensions must escape the pointer's regression filter, and the comment says so), but it doubles the op count and adds same-document write-lock contention on the hottest collection. Worth measuring whether the `$max` fields can ride the pipeline branch of `roomPointerUpdate`.

- `nitpick` — `writeIntents` is copied by value three times per message (return from `deriveIntents`, `flusher.add`, `batch.add`) — `roomlist-worker/handler.go:51`, `flush.go:66`, `batch.go:76`. Both `//nolint:gocritic // hugeParam` directives acknowledge it; a `*writeIntents` would remove ~100B×3 of copying per message with no readability cost.

- `nitpick` — ~4 context allocations per message on the hot path (`logctx.ConsumeContext` → `StampRequestID` + `Admit`, then `obs.ContextWithIdentity`) — `roomlist-worker/main.go:365`, `:375`, and each is retained in `held` until flush.

**Verified clean (no finding):** no Mongo reads at all, so no N+1 and the projection rule is N/A (`handler.go:49-50`); no `$lookup`; `BulkWrite` is unordered with an empty-input guard (`pkg/mongoutil/collection.go:169-179`); sonic confined to `Unmarshal` of a narrow `eventProjection` with startup pretouch — no byte-identity exposure (`projection.go`, `pretouch.go:12`); jsretry discipline correct throughout — `Settle`/`SettleQuiet` only, never a bare `Nak()` or `NakWithDelay(0)` (`main.go:371`, `flush.go:218`); `cc.BackOff` derived, never hardcoded (`main.go:392`); no `time.Sleep` anywhere; both long-lived goroutines have explicit termination paths (`main.go:148` + `:207-215`; `main.go:173` + `:194-204`); `newBatch` reuse sizing is clamped by `flushloop.ReuseCap` (`batch.go:58-68`); `validateFlushBudget` correctly charges `2×timeout+interval` against `EffectiveAckWait` (`main.go:78`, `:295-313`).

### Recommendations
- `medium` — Add `if strings.IndexByte(content,'@') < 0 { return ParseResult{} }` at the top of `pkg/mention.Parse`; it benefits `broadcast-worker` and `notification-worker` identically.
- `medium` — Chunk `BulkSetMentions` models (e.g. 1000/call) in `store_mongo.go:158-166`, or have `batch.add` report the mention count so `flusher.add` can drain mid-message; today one message can defeat the budget.
- `low` — Add an `EnsureIndexes` to `mongoStore` asserting `(roomId, u.account)` and `_id`, or document the room-service ownership dependency inline at `store_mongo.go:151`.
- `low` — Scope panic recovery to `deriveIntents` and settle the message permanently on panic, so `MaxDeliver=-1` cannot pin an ack-pending slot indefinitely.
- `nitpick` — Take `*writeIntents` through `flusher.add`/`batch.add` and drop the two `hugeParam` nolints.
- `nitpick` — Emit a metric for batch sizes (`rooms`/`lastSeen`/`mentions`/`held`) and flush duration; the budget and `FLUSH_TIMEOUT` are both tuned against numbers nothing currently exports.
