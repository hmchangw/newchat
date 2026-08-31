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
