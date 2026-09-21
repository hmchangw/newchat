# outbox-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `139f825` (base `main`)  
**Overall score:** 3.0 / 5 (baseline 2026-09-01: 3.2, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

outbox-worker is a small, cleanly written NATS-to-NATS relay (429 non-test lines, one responsibility, every checklist item in the rulebook honoured) whose score is held down by three advertised guarantees that the reviewers verified do not hold. The per-peer isolation stops at the consumer: every concurrent lane funnels into one shared `MaxWorkers` semaphore, so a down peer's parked forwards (up to 1000, each costing 0.5–3s per attempt) can occupy the whole pool and stall healthy peers for the first minutes of an outage. The FIFO lane's "a rename cannot overtake the add it renames" contract is origin-only: inbox-worker routes `room_renamed` to its concurrent pool, so the ordering this service pays `MaxAckPending=1` for is discarded at the destination and a new member is stranded on the old name. And the two-lane shutdown handshake covers only the concurrent lane: ordered-lane callbacks are outside the WaitGroup and `cc.Stop()` discards rather than drains, so `natsutil.Drain` can run under a live forward. Beyond those, a dead pump leaves the process healthy with one peer's lane silently stopped, destinations absent from `ALL_SITE_IDS` strand in the stream with no detection, forward idempotency relies on a 2-minute server-default dedup window nothing sets against a 10-minute retry tail, the service emits no consumer metrics, and three docs (`nats-subject-naming.md`, `architecture.md`, `nats-traffic-estimation.md`) still describe a sourcing-based topology that no longer exists. Coverage is 37.8% because `main()` holds every closure and the drain-pool message path is untested; the integration suite drives the concurrent lane through `Consume` rather than the production `Messages()`+`drainPool` path, so the pull-iterator mechanism is exercised nowhere.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 4 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 4 high, 19 medium, 17 low, 3 nitpick (44 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [high] Package coverage is 37.8% (54/143 stmts) against the CLAUDE.md 80% floor; every uncovered statement is a closure inside `main` — `outbox-worker/main.go:90-96` (publish closure), `:103-108` (process disposition), `:167-191` (shutdown steps) — plus the dispatch half of `drainPool` at `main.go:217-225` (50%): `drainpool_test.go:18-21`'s `stubIter` never yields a message, so the semaphore acquire/release and per-message `wg.Add/Done` path is never executed. Everything outside `main` is 100%. Context: 34 of 35 services in the tree are below 80% (repo-wide gap), so this is a shared convention failure, not a local regression.
- [medium] Ordered-lane shutdown is not covered by the drain handshake — `outbox-worker/main.go:172-174` calls `cc.Stop()` (o11y `ConsumeContext.Stop` "halts immediately and discards buffered messages") and ordered callbacks are not counted in `wg`, so the `wg.Wait` step at `main.go:178-186` only waits for concurrent-lane work; a FIFO callback still awaiting a PubAck (up to 3s, `handler.go:34`) can be live when `natsutil.Drain` runs (`main.go:187`). The service's own `integration_test.go:57-72` documents this exact race and uses `Drain()`+`Closed()` there. Impact is bounded (MaxAckPending=1, redelivery is idempotent via DedupID), so it costs a redelivery after AckWait, not data.
- [low] Unmarshal failure discards its cause — `outbox-worker/handler.go:47-48` returns `errcode.Permanent(errcode.BadRequest("unmarshal outbox event"))` without `errcode.WithCause(err)`. `Error.Error()` returns only `Message` (`pkg/errcode/error.go:14`), so the "permanent message failure — dropping" warn in `pkg/jsretry/jsretry.go:129` logs nothing about *why* the body was rejected. `hr-sync-worker/handler.go:26` shows the repo convention with `WithCause`.
- [low] Empty-`Envelope` skip branch has no test — `handler.go:53` guards `len(evt.Envelope) == 0 || evt.DedupID == ""`, but `handler_test.go:83-95` only exercises the empty-DedupID half; the six `HandleEvent` scenarios are also separate functions where CLAUDE.md prefers a table.
- [low] Timing-based assertions — `integration_test.go:335` uses `time.Sleep(300ms)` then asserts `NumAckPending == 1` (`:340`), which requires the consumer to have delivered the head within 300ms and can flake on a slow CI runner; `drainpool_test.go:39-43` uses a 50ms `time.After` negative assertion that passes vacuously if the pump never starts.
- [nitpick] `integration_test.go:31-44` boots an in-process `nats-server` rather than `testutil.NATS(t)`; 9 other packages use the same embedded pattern, so it is an accepted repo deviation, but the `//go:build integration` tag then gates a test that needs no container.
- [nitpick] `Handler`, `NewHandler`, `PublishFunc` are exported inside `package main` (`handler.go:20-28`) against the "keep handler implementations unexported" rule; 16 of 24 services do the same.

### Recommendations

- [high] Lift the three `main` closures into named, testable functions: `newJetStreamPublish(js) PublishFunc`, `newProcess(handler) func(ctx, msg)`, and a `shutdownSteps(...)` builder — `main.go:90-108, 167-191` — and add a `drainPool` test whose iterator yields N messages against a cap-1 semaphore to prove bounded concurrency and `sem` release. This alone lifts the package to ~80%+ without touching behaviour.
- [medium] Track ordered-lane callbacks in the drain handshake: wrap `process` for `ocons.Consume` with `wg.Add(1)/defer wg.Done()`, and replace `cc.Stop()` with `cc.Drain()` + wait on `cc.Closed()` inside the first shutdown step — `main.go:149, 172-174` — so `wg.Wait` and `Drain` see every in-flight forward, matching the comment's intent at `main.go:198-202`.
- [low] Attach the decode error: `errcode.BadRequest("unmarshal outbox event", errcode.WithCause(err))` — `handler.go:47-48` — so the poison-drop warn names the cause.
- [low] Convert the `HandleEvent` tests into one table (`handler_test.go:30-113`) and add the empty-`Envelope`/non-empty-`DedupID` case for `handler.go:53`.
- [low] Replace the `time.Sleep(300ms)` probe at `integration_test.go:335` with `require.Eventually` on `NumAckPending == 1`, then assert `AckFloor.Consumer == 0` — keeps the "head parked" claim without the fixed delay.
