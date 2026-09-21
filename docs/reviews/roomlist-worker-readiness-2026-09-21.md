# roomlist-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `377040a` (base `main`)  
**Overall score:** 3.7 / 5 (baseline 2026-09-01: 3.7, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

roomlist-worker is one of the best-engineered workers in the fleet: the disjoint-halves contract with broadcast-worker's preview writer holds exactly as CLAUDE.md describes, `msgbucket.NewerRow` ordering is applied on both the coalescer and the BSON filter, settlement is exclusively `jsretry.Settle`, shutdown ordering is explicit and load-bearing, and every business-logic function sits at 80–100% coverage. The 65.9% figure is mechanical — `main()` holds 30% of the statements — and the remaining findings are ops-facing rather than correctness bugs. Three reviewers independently flagged that the only bound on the mention map is derived as `4×CONSUMER_MAX_ACK_PENDING` with no validation, so a legal `0` or `-1` (JetStream "unlimited") silently disables the early drain the worker's memory model depends on. The operator README contradicts the binary: it omits `FLUSH_TIMEOUT` and the `2×FLUSH_TIMEOUT + FLUSH_INTERVAL < ACK_WAIT` fail-fast rule, and says the health endpoint checks only NATS when `/readyz` now also fails on a dead consume loop. Subscription writes filter on `(roomId, u.account)` with no `WarnMissingIndexes` unlike every sibling on the shared collection; the 10-minute `DefaultBackoff` tail pins the whole ack-pending budget for 5–10 minutes after a MongoDB outage ends, which `MaxDeliver=-1` makes unnecessary; `DeliverNewPolicy` on a durable means a deleted-and-recreated consumer silently loses every pointer and badge in the gap; the edit path can badge a message's own sender; and `docs/client-api.md` still names broadcast-worker as the `hasMention` writer. The store is stubbed by hand with a reasoned but rule-deviating no-mockgen comment.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 4 |
| Integration | 4 |
| Performance | 4 |

**Findings by severity:** 0 critical, 1 high, 9 medium, 21 low, 11 nitpick (42 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Derived mention budget silently disables itself on a legal `CONSUMER_MAX_ACK_PENDING` — `roomlist-worker/main.go:145` — `newFlusher(store, 4*cfg.Consumer.MaxAckPending, …)`; `stream.ConsumerSettings` (`pkg/stream/consumer.go:22`) has no `Validate`, so `-1` (NATS "unlimited", the plausible operator override the README's "outage buffer" wording invites) or `0` yields a budget ≤ 0 and `flush.go:193` (`f.mentionBudget > 0 && …`) turns the early drain off. That drain is the only bound on the `mentions` map (`batch.go:42-46`, `flush.go:160-168`), and `validateFlushBudget` (`main.go:295`) checks every other knob but not this one — the exact "never accumulate, never livelock" invariant the worker is built around goes unenforced by config.
- [low] Store methods return bare `err` — `roomlist-worker/store_mongo.go:229-230`, `:240-241`, `:251-252` — `_, err := m.roomCol.BulkWrite(…); return err` in all three methods. CLAUDE.md §3 "never return bare err"; sibling stores (`broadcast-worker`, `message-worker`, `notification-worker`) have no such site. Impact is nil in practice: `mongoutil.Collection.BulkWrite` wraps with the collection name (`pkg/mongoutil/collection.go:176`) and `flush.go:277` adds the stage name, so the chain reads `flush subscription mentions: bulk write subscriptions: …` — but the store's own method name never appears, and the file-header comment (`store_mongo.go:100-103`) documents relying on the callee's wrap instead of following the rule.
- [low] Hand-written store stubs instead of mockgen — `roomlist-worker/store.go:64-67` — "No mockgen directive: the tests use hand-written stubs … because they assert call order and context cancellation, which gomock expectations do not express". CLAUDE.md §1/§4 mandate `//go:generate mockgen` + `mock_store_test.go`; gomock's `gomock.InOrder` and `DoAndReturn` (which receives `ctx`) express both concerns, so the stated rationale does not hold. Three stub types (`stubStore`, `panicStore`, `blockingStore` in `flush_test.go:21,250,422`) now duplicate what one generated mock would provide.
- [low] Operator README contradicts the code it documents — `roomlist-worker/deploy/README.md:57` says "The health endpoint deliberately checks only NATS", but `main.go:178-181` registers `consume.Check()` alongside it; the Tuning table (`README.md:47-51`) omits `FLUSH_TIMEOUT` entirely and states `CONSUMER_ACK_WAIT` "must exceed FLUSH_INTERVAL plus write latency", whereas `main.go:78`/`:308` fail startup unless `2×FLUSH_TIMEOUT + FLUSH_INTERVAL < ACK_WAIT`. An operator tuning from the README hits a fail-fast the README never mentions.
- [nitpick] Batch-level failure log inherits one message's identity — `roomlist-worker/flush.go:199-206` — `flushNow` derives the drain context from a single message's `handlerCtx` (request id, trace, `obs.ContextWithIdentity` user/room baggage), and `settle` logs the whole batch's failure with it (`flush.go:329`). A poison-batch ERROR will be attributed to whichever message tipped the budget, not to the batch; the periodic path carries no request id at all.
- [nitpick] `time.Sleep` pacing a feeder goroutine in a test — `roomlist-worker/flush_test.go:402` — synchronisation is via `require.Eventually`, so this is pacing rather than sync, but it is the pattern CLAUDE.md §3 bans and the loop would work as a tight `select` without it.

### Recommendations

- [medium] Extend `validateFlushBudget` (or add a sibling) to reject `Consumer.MaxAckPending <= 0` and derive the budget from the validated value — `main.go:78,145` — turns a silent loss of the mention-map bound into the same fail-fast the other flush knobs already get; add table cases `{0, -1}` to `TestValidateFlushBudget` (`main_test.go:250`).
- [low] Wrap each store return: `return fmt.Errorf("bulk update room last message: %w", err)` etc. — `store_mongo.go:229,240,251` — restores CLAUDE.md §3 compliance and keeps `errors.As(mongo.BulkWriteException)` in `classifyFlushErr` working (`%w` preserves it; `TestClassifyFlushErr` "wrapped bulk exception" already proves this).
- [low] Replace the three hand stubs with a mockgen `MockStore` (`//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main`) using `gomock.InOrder` for stage ordering and `DoAndReturn` for the deadline/blocking cases — `store.go:64`, `flush_test.go:21-67,250-256,422-446` — aligns with the repo convention and the "mocks not stale" CI check.
- [low] Bring `deploy/README.md` in line with `main.go`: document `FLUSH_TIMEOUT`, state the `2×FLUSH_TIMEOUT + FLUSH_INTERVAL < CONSUMER_ACK_WAIT` rule and the consume-loop readiness check — `README.md:47-58`.
- [nitpick] Have `flushNow` build its drain context from the flusher's own base context (trace-less, like the periodic path) and log the triggering request id as an attribute instead — `flush.go:199-206`, `:329`.

## 3. Architecture — score 4

### Evidence

- [medium] The only bound on the batch maps is derived from `MaxAckPending` and silently vanishes when that knob is set to 0 or -1 — `roomlist-worker/main.go:145` — `newFlusher(store, 4*cfg.Consumer.MaxAckPending, …)` makes the mention budget 0/-4, and `flusher.add` (`flush.go:70`) treats any non-positive budget as "no early drain". `batch.go:42-46` documents that `held`/`rooms`/`lastSeen` are bounded by MaxAckPending and only `mentions` needs the budget; with `CONSUMER_MAX_ACK_PENDING=-1` (JetStream unlimited) or `0` (server default) both bounds evaporate. `cfg.Pool.Validate()` runs at `main.go:87` but nothing validates `cfg.Consumer`, and `pkg/stream.ConsumerSettings` has no `Validate` (`pkg/stream/consumer.go`).
- [low] `DeliverNewPolicy` overrides the `DurableConsumerDefaults` DeliverAll invariant — `roomlist-worker/main.go:398` — justified for first deploy, but a deleted-and-recreated durable (ops mistake, consumer-config migration) silently skips every message in the gap: badges, `lastMsgAt` and sender `lastSeenAt` for those messages are lost with no replay path, the exact loss class `deploy/README.md` warns about for deploy ordering. Neither the README nor the code comment names this second trigger.
- [low] No `//go:generate mockgen` directive or `mock_store_test.go` — `roomlist-worker/store.go:8-11` — CLAUDE.md's per-service layout and testing rules prescribe mockgen; the hand-written `stubStore`/`blockingStore`/`panicStore` in `flush_test.go:21,250,422` are a documented, reasoned exception, but it is still a repo-convention deviation a reviewer should sign off explicitly.
- [low] `deploy/README.md` contradicts the readiness contract — `roomlist-worker/deploy/README.md` ("The health endpoint deliberately checks only NATS") vs `main.go:178-181`, which registers `consume.Check()` so `/readyz` fails once the loop dies. Ops-facing docs drift on the probe semantics.
- [nitpick] Room-pointer write stamps `updatedAt` with the message's `createdAt` rather than write time — `roomlist-worker/store_mongo.go:114` — a rename in `room-service` (`room-service/store_mongo.go:1922`, `updatedAt: now`) landing between message creation and the flush is regressed. No reader sorts or versions on `rooms.updatedAt` today (grep), so inert.
- [nitpick] Consumer pattern is a third shape beyond the two CLAUDE.md documents: `cons.Messages()` drained by one goroutine with no semaphore — `roomlist-worker/main.go:154,334` — justified in the comment (no per-message I/O), not mixed within the consumer.
- [nitpick] `bootstrapStreams` receives `wiring.CanonicalWildcard` as the stream subject instead of `wiring.CanonicalStream.Subjects` — `roomlist-worker/main.go:128` — identical today (`pkg/stream/pipeline.go:53,62`), but it is a second source of truth for the schema `pkg/stream` owns. Same shape as `broadcast-worker/bootstrap.go:28`.

### Recommendations

- [medium] Validate `cfg.Consumer.MaxAckPending > 0` at startup (fail fast like `validateFlushBudget`), or clamp the mention budget to a positive floor independent of MaxAckPending — `roomlist-worker/main.go:145` — restores the documented memory bound under every operator-settable value; ideally add `ConsumerSettings.Validate()` in `pkg/stream` so every worker gets it.
- [low] Document the durable-recreation loss in `deploy/README.md` (and the `main.go:395` comment) and state the operator remedy (never delete the durable; use `CreateOrUpdateConsumer` in place) — `roomlist-worker/main.go:398` — makes the one unrecoverable data-loss path visible to ops.
- [low] Either add the `//go:generate mockgen` directive (mocks unused but convention-compliant) or record the exception in CLAUDE.md's sanctioned-exceptions list — `roomlist-worker/store.go:8` — keeps `make generate` uniform and removes reviewer ambiguity.
- [low] Fix the README health paragraph to describe both probes (NATS + consume-loop readiness, and why MongoDB is deliberately excluded) — `roomlist-worker/deploy/README.md`.
- [nitpick] Pass `wiring.CanonicalStream.Subjects` (or the whole `stream.Config`) into `bootstrapStreams` — `roomlist-worker/main.go:128` — single source of truth for the stream schema.
- [nitpick] Drop `updatedAt` from the pointer `$set` or write `$$NOW`/write-time — `roomlist-worker/store_mongo.go:114` — prevents a future reader from inheriting a regressing timestamp.
