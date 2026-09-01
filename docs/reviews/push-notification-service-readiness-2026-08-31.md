# push-notification-service — Production Readiness Review

**Service:** `push-notification-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.7 / 5

The lowest-scoring service in the fleet on architecture, and the reason is not a defect in what it does — it is that **the production binary does not do the thing it is named for**. The only `Dispatcher` implementation is `LogDispatcher`, which logs and returns nil, so every push event is acked and dropped. There is no APNs or FCM call anywhere in the service. Whether that is a deliberate not-yet-wired stage or an oversight, it is what ships today, and nothing in the code or the deploy files says which.

Around that gap, three operational guards its eight sibling workers all have are missing: **no `jobguard` panic recovery** (an unrecovered panic kills the process and crash-loops on redelivery), **no failure signal from the consume loop** (any `iter.Next()` error returns silently while the health probe — which reports only NATS connectivity — stays green on a pod consuming nothing), and **a shutdown race** where the consume-loop goroutine is not counted in the WaitGroup, so `wg.Wait()` can observe zero in-flight and let `nc.Drain()` proceed while a handler is about to ack on a drained connection. The peer that documents this exact window is `notification-worker`, its own upstream.

Coverage is 26.9%, and there is no integration test at all.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 3 / 5 |
| 2 | Architecture | 2 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 4 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 6 | 9 | 9 | 2 | **27** |

---

## 2. Go code quality — 3 / 5

Idiomatic, well-wrapped errors and clean structured `slog` with no secret/body leakage, but errors are silently discarded at two ack sites and no log line carries a request ID.

### Findings
- `medium` — every log line in the service omits `request_id`; the handler never derives a correlation ID from the message, unlike every peer worker (`logctx.ConsumeContext` at `notification-worker/main.go:368`, `natsutil.RequestIDFromContext` throughout `broadcast-worker/handler.go`). Violates CLAUDE.md §3 "Request Logging & Tracing" — `push-notification-service/handler.go:31,38,43,49,58`
- `medium` — `_ = msg.Ack()` discards the ack error with no comment, twice, while line 48 checks the same call — CLAUDE.md §3 "never ignore errors silently — comment if intentionally discarded" — `push-notification-service/handler.go:33`, `:40`
- `low` — `HandleJetStreamMsg` hand-rolls the exact ack/permanent/nak decision tree `jsretry.Settle` already owns (`pkg/jsretry/jsretry.go:86-141`), duplicating the `errcode.IsPermanent` branch — `push-notification-service/handler.go:28-52`
- `low` — SAST audit-coverage gap, environmental not service: gosec and repo-owned semgrep are clean repo-wide; `govulncheck` and the semgrep registry packs could not run (blocked egress) — per `GLOBAL_PREP.md:6-9`
- `nitpick` — stray blank line inside the config struct — `push-notification-service/main.go:30`

### Recommendations
- `medium` — call `logctx.ConsumeContext(msgCtx, msg.Headers(), msg.Subject(), msg.Data())` in the consume loop and add `request_id` to all five log sites.
- `medium` — replace the two bare `_ = msg.Ack()` with a checked ack (or one justifying comment each).
- `low` — collapse `HandleJetStreamMsg`'s branch to `jsretry.Settle(ctx, msg, …, err)`, keeping only the unmarshal ack-drop.

---

---

## 3. Architecture — 2 / 5

The shape is right (constructor DI, consumer-owned `Dispatcher` interface, `pkg/stream` wiring, `pkg/shutdown`), but the production binary ships a log-only dispatcher that acks and discards every push, and the consume loop has neither panic recovery nor a failure signal.

### Findings
- `high` — production wiring injects `LogDispatcher{}`, the only implementation; it logs and returns nil, so every push event is acked and dropped and no APNs/FCM call exists anywhere in the service — `push-notification-service/main.go:61`, `handler.go:54-63`
- `high` — no panic guard: the per-message goroutine calls the handler directly, outside any recovery. Every other JetStream worker wraps it in `jobguard.Run` (`broadcast-worker`, `message-worker`, `notification-worker`, `inbox-worker`, `outbox-worker`, `roomlist-worker`, `hr-sync-worker`, `search-sync-worker`) — an unrecovered panic kills the process and crash-loops on redelivery — `push-notification-service/main.go:84-87`
- `high` — the consume loop returns silently on any `iter.Next()` error with no log, metric, or health impact; the health probe only reports NATS connection status, so the pod stays "healthy" while consuming nothing. `notification-worker/main.go:346` records `LoopFailed` here — `push-notification-service/main.go:78-81`, `:91-93`
- `medium` — no `Bootstrap bootstrapConfig` / `bootstrapStreams` helper, so the service cannot stand up against a fresh NATS and never verifies its input stream exists; `notification-worker/bootstrap.go:33-40` creates `PUSH-NOTIFICATION-{siteID}` as its *output*. Mitigated (not excused) by `CreateOrUpdateConsumer` failing fast on a missing stream — `push-notification-service/main.go:21-31`, `:65`
- `low` — no consumer/domain metrics at all despite `obs.Init`; every comparable worker instruments via `pkg/natsmetrics` — `push-notification-service/main.go:47,65-89`

### Recommendations
- `high` — implement a real APNs/FCM dispatcher behind the existing `Dispatcher` interface, or make the log-only mode an explicit, alarmed config flag so ops cannot ship it unknowingly.
- `high` — wrap the handler call in `jobguard.Run`.
- `high` — log + `natsmetrics` on loop exit and fail the health probe once the loop is dead.
- `medium` — add `bootstrap.go` with the standard `bootstrapStreams(ctx, js, siteID, enabled)` no-op-when-disabled helper, verifying the push stream when disabled.

---

---

## 4. Test coverage — 1 / 5

Coverage is **26.9%** (78 statements) — below the 60% critical threshold, with `run()` entirely untested and no integration test.

### Findings
- `critical` — 26.9% vs the CLAUDE.md §4 80% floor; `main` 0.0% and `run` 0.0% carry the loss, `HandleJetStreamMsg` 93.3%, `buildConsumerConfig` 100% — `covfunc.txt:2481-2486`
- `high` — no `integration_test.go` and no `TestMain`; nothing exercises the real consumer against `testutil.NATS(t)`, so the durable name, `FilterSubject` binding to `PushInputWildcard`, and the real NAK-redelivery path are unverified. CLAUDE.md §1 lists `integration_test.go` in the per-service layout; `notification-worker/integration_test.go` is the peer precedent — `push-notification-service/`
- `high` — the nak delay is never asserted: the fake's `NakWithDelay(_ time.Duration)` discards the duration, so the repo's central invariant (never a zero/bare nak) is untested here — `push-notification-service/handler_test.go:53`, `:92`
- `medium` — uncovered handler branch is the failed-ack path (`handler.go:48-51`); the fake's `Ack()` always returns nil — `push-notification-service/handler_test.go:50`
- `medium` — four near-identical single-scenario tests instead of the CLAUDE.md-preferred table-driven form, and names break the `Test<Type>_<Method>_<Scenario>` rule (`TestDispatchSuccessAcks`, `TestMalformedJSONAcks`) — `push-notification-service/handler_test.go:65,83,95,106`
- `low` — `buildConsumerConfig` is asserted only against `DurableConsumerDefaults`; nothing pins `MaxDeliver` against the chosen backoff window, unlike `broadcast-worker/consumer_config_test.go:31` — `push-notification-service/consumer_config_test.go:14-30`

### Recommendations
- `critical` — table-drive `TestHandler_HandleJetStreamMsg` over {success, transient, permanent, malformed, ack-failure}, asserting the recorded nak delay is `> 0`.
- `high` — add `integration_test.go` (`//go:build integration`, `TestMain` → `testutil.RunTests(m)`, `testutil.NATS(t)`) covering consumer creation, subject-filter binding, and redelivery after a transient dispatch failure.
- `medium` — extract the loop from `run()` into a testable `consume(ctx, iter, h)` so the 0% block becomes reachable.

---

---

## 5. Maintainability — 4 / 5

At 63 + 125 lines with trivial complexity, single responsibilities and WHY-style comments, it is easy to change; the only real debt is a hand-rolled copy of shared retry logic.

### Findings
- `medium` — the settle decision tree is duplicated from `pkg/jsretry`; a future change to the permanent/transient policy must be made in two places — `push-notification-service/handler.go:28-52` vs `pkg/jsretry/jsretry.go:86-141`
- `low` — `LogDispatcher` (a deployment stub) lives in `handler.go` beside the domain logic; a real dispatcher will want its own file, and the stub then becomes test-only — `push-notification-service/handler.go:54-63`
- `low` — the consume loop is inline in `run()`, mixing wiring with the hot path and blocking any unit test of the loop — `push-notification-service/main.go:74-89`
- `nitpick` — comments are appropriately WHY-oriented (`main.go:118-119`, `consumer_config_test.go:12-13`); no dead code or WHAT-restating comments found.

### Recommendations
- `medium` — delete the duplicated branch in favour of `jsretry.Settle`.
- `low` — move `LogDispatcher` to `dispatcher_log.go` and add the real dispatcher beside it.
- `low` — extract `consume(ctx, iter, h, maxWorkers)`; it pays for itself the moment jobguard/logctx/metrics are added.

---
