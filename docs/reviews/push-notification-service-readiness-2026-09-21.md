# push-notification-service — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `a7241fa` (base `main`)  
**Overall score:** 2.7 / 5 (baseline 2026-09-01: 2.7, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

push-notification-service is the last hop of the push pipeline and it does not deliver anything. The only dispatcher wired into production is a log stub: it writes a line, returns nil, and the message is acked and removed from the stream. There is no APNs or FCM client anywhere in the repository, no credential or endpoint configuration, and no switch to select a real sink, so every push event the notification pipeline produces is silently consumed. Its retry semantics also directly contradict its own written contract: the ops document says this service must ack on receipt before any provider call, must never NAK, and must run at `MaxDeliver=1`, because a duplicate push is user-visible spam — the code instead acks after dispatch, NAKs transient errors, and inherits `MaxDeliver=6`. That same document tells operators to provision a five-minute duplicate window on a stream whose producer refuses to start under one hour. The consume loop is missing three guards every sibling worker has: it is not registered in the WaitGroup, so a message can slip past the drain and be acked on a drained connection; it has no panic guard, so a real dispatcher's first crash becomes a crash loop; and it stamps no request ID, so none of its log lines correlate with the notification-worker trace that produced them. Worst of all, when the loop dies it returns with no log, no metric and no exit, while the health check probes only the NATS connection — the pod stays Ready while the stream backs up. Coverage is 26.9% because `run` holds the wiring, the retry choice and all three missing guards, and nothing tests it.

| Dimension | Score |
|---|---|
| Code quality | 3 |
| Architecture | 3 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 3 |

**Findings by severity:** 1 critical, 11 high, 23 medium, 14 low, 4 nitpick (53 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 3

### Evidence

- [high] No request-ID stamped at the JetStream entry point — `push-notification-service/main.go:78-87` — the consume loop hands `mCtx` straight to the handler; it never calls `logctx.ConsumeContext`, which nine sibling consumers use (`roomlist-worker/main.go:365`, `message-worker/main.go:392`, …). `pkg/logctx/consume.go:24-27` names this service explicitly as one that "stamps nothing at all". Result: every line in `handler.go` and the `request_id` field `pkg/jsretry/jsretry.go:116` emits are correlation-less, and the inbound X-Debug rung / payload capture are dropped. CLAUDE.md §"Request Logging & Tracing" makes extraction-at-entry-point a MUST.
- [high] Consume-loop error silently swallowed — `push-notification-service/main.go:78-81` — `if err != nil { return }` ends the only consuming goroutine with no log, no metric and no process exit. The health check is `natsutil.HealthCheck(nc)` alone (`main.go:92`), which probes the connection, so the pod stays Ready while pushes stop. `roomlist-worker/main.go:350-356` distinguishes shutdown from failure and logs ERROR. CLAUDE.md §3 "Never ignore errors silently — comment if intentionally discarded".
- [high] The production dispatcher is a stub, hardcoded with no config seam — `push-notification-service/main.go:61` wires `LogDispatcher{}` (`handler.go:54-62`), which logs and returns nil. A repo-wide grep for apns/fcm/firebase matches only this service's own comments — no real dispatcher exists, and `deploy/user/docker-compose.yml` has no knob to select one. The deployed service therefore acks every `PushNotificationEvent` without delivering it.
- [medium] Per-message goroutine has no panic guard — `push-notification-service/main.go:84-87` — every other JetStream worker in the repo wraps the handler in `pkg/jobguard` (broadcast-worker, message-worker, roomlist-worker, outbox-worker, inbox-worker, …). Here a panic inside `Dispatch` or on a malformed-but-parseable event kills the process; the message stays unacked and redelivers into a crash loop.
- [low] Ack errors discarded without comment on both drop paths — `push-notification-service/handler.go:33` and `handler.go:40` use `_ = msg.Ack()`, while `handler.go:48` logs the identical failure. Inconsistent within one function; CLAUDE.md asks for a comment on an intentional discard.
- [low] Startup failures leak the observability SDK — `push-notification-service/main.go:47-72` — `obsShutdown` is only registered inside `shutdown.Wait` (`main.go:113`), so any of the four early `return fmt.Errorf(...)` paths exits without flushing telemetry; likewise the iterator/goroutine started at `main.go:69-89` is never stopped if `health.ServeWithPprof` fails at `main.go:91`.
- [low] Exported symbols nothing can consume — `push-notification-service/handler.go:16` (`Dispatcher`) and `handler.go:55` (`LogDispatcher`) are exported inside `package main`. CLAUDE.md §3: "Export only what other packages consume".
- [nitpick] Startup log omits the pipeline — `push-notification-service/main.go:98` logs only `site`, yet one binary is deployed twice (`deploy/user` MODE=user, `deploy/bot` MODE=bot); `roomlist-worker/main.go:187` logs `mode`.
- [nitpick] `buildConsumerConfig` signature diverges from the three sibling dual-mode workers — `push-notification-service/main.go:120` takes `stream.Pipeline` and calls `ConsumerName` internally, while `roomlist-worker/main.go:134` / `broadcast-worker/main.go:338` pass the resolved durable name.

### Recommendations

- [high] Replace the raw per-message ctx with `ctx, _ := logctx.ConsumeContext(mCtx, msg.Headers(), msg.Subject(), msg.Data())` before `h.HandleJetStreamMsg` — `main.go:86` — restores request-ID correlation across notification-worker → push, plus the debug rung; zero behaviour change otherwise.
- [high] Log and classify the iterator exit — `main.go:78-81` — mirror `roomlist-worker/main.go:350-356` (Info on `jetstream.ErrMsgIteratorClosed` during shutdown, Error otherwise) and add a "consumer alive" signal to the health endpoint so a dead loop is not Ready.
- [high] Make the dispatcher selectable by config and implement the real one (Resty client with an explicit timeout per CLAUDE.md §HTTP) — `main.go:61` — or, if log-only is the intended state, say so in a comment at `handler.go:55` and gate it behind an explicit `PUSH_DISPATCHER=log` so a silent no-op cannot be mistaken for delivery in prod.
- [medium] Wrap the handler call in `jobguard` — `main.go:84-87` — matches all nine sibling workers and converts a poison-event panic from a crash loop into an Ack-drop.
- [low] Register `obsShutdown` (and iterator stop) via `defer` immediately after each resource is created — `main.go:47-96` — so startup failures still flush traces/logs.
- [low] Unexport `Dispatcher`/`LogDispatcher` and add a one-line comment on the two `_ = msg.Ack()` discards — `handler.go:16,33,40,55`.

## 3. Architecture — score 3

### Evidence

- [high] The only delivery sink is a log stub, wired unconditionally — `push-notification-service/main.go:61`, `push-notification-service/handler.go:54-63`. `newHandler(LogDispatcher{})` is the sole construction; `LogDispatcher.Dispatch` logs and returns nil, so every batch is Acked and removed from PUSH-NOTIFICATION with nothing sent. There is no APNs/FCM client, no credential/endpoint config in `config` (`main.go:21-31`), and no env switch between stub and real sink. The `Dispatcher` seam itself (consumer-defined interface, constructor DI) is right; the production implementation simply does not exist.
- [high] Consume loop runs without `jobguard` — `push-notification-service/main.go:84-87`. Every other JetStream worker in the repo (`notification-worker/main.go:363`, `broadcast-worker/main.go:556`, `message-worker`, `roomlist-worker`, `inbox-worker`, `outbox-worker`, `search-sync-worker`) wraps the per-message call in `jobguard.Run`. Here a panic in `Dispatch` (nil `evt.Data.Sender`, a real HTTP client bug) kills the process with the message un-acked, so JetStream redelivers it after restart — the crash-loop `pkg/jobguard/jobguard.go:1-18` exists to prevent.
- [high] Retry budget is ~12 min against an upstream contract sized for 1 h — `push-notification-service/main.go:120-124` vs `notification-worker/main.go:446`. `buildConsumerConfig` calls `stream.DurableConsumerDefaults(s)` raw, leaving `MaxDeliver=6` (`pkg/stream/consumer.go:141-145`), while the handler settles with `jsretry.DefaultBackoff` (`handler.go:45`), whose documented span across 6 deliveries is 12.6 min nominal and ~6 min at minimum jitter. notification-worker opts into `stream.WithOutageRetryBudget(s, jsretry.DefaultBackoff)` and its bootstrap enforces `Duplicates = stream.OutageRetryWindow` = 1 h on PUSH-NOTIFICATION (`notification-worker/bootstrap.go:43-46`, `pkg/stream/consumer.go:151`). An APNs/FCM outage longer than ~12 min therefore exhausts MaxDeliver and silently drops pushes at the last hop, inside a window the producer deliberately sized to survive.
- [medium] Consume-loop death is silent and invisible to `/healthz` — `push-notification-service/main.go:78-81`. A terminal `iter.Next()` error returns from the pump goroutine with no log, no metric, and no process exit; the health server only checks the NATS connection (`main.go:91-93`), so the pod stays Ready while delivering nothing. `pkg/natsmetrics/loop.go:48-52` (`consumer.LoopFailed`) is the shared handling this loop bypasses.
- [medium] The bounded-pull loop is hand-rolled instead of reusing `natsmetrics.Consume` — `push-notification-service/main.go:74-89` vs `pkg/natsmetrics/loop.go:39-64`. Consequence: no consumer metrics at all (no in-flight gauge, no delivery-attempt/terminal-drop counters, no loop-up gauge), where `broadcast-worker/main.go:442` and `notification-worker/main.go:353` have them. The last hop of the push pipeline is the one with the least telemetry.
- [medium] No `bootstrap.go` / `BOOTSTRAP_STREAMS` config, unlike every other stream-attached service — `main.go:21-31` (12 services ship `bootstrap.go`). CLAUDE.md requires the opt-in helper on services that consume a stream. Stream presence is only checked implicitly by `CreateOrUpdateConsumer` (`main.go:65`) failing; the duplicate-window invariant is verified solely by the producer, so a dev stack started without notification-worker cannot bring this service up.
- [medium] `Dispatcher` is all-or-nothing per batch, with no idempotency contract — `handler.go:16-18`, `handler.go:36-46`. `evt.ID` (`{messageID}-b{N}`, `pkg/model/push.go:5`) dedups *stream entries*, not *device sends*. A partial batch failure (60 of 100 recipients delivered, then an error) NAKs the whole message and re-dispatches all 100, so users get duplicate notifications. The interface exposes no per-recipient result or dedup key for a real sink to use.
- [low] The handler re-implements `jsretry.Settle` — `handler.go:36-47` duplicates the permanent→Ack / transient→Nak branch of `pkg/jsretry/jsretry.go:120-141`, including the log. `jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)` is the one-liner.

### Recommendations

- [high] Implement (or explicitly gate) the real sink — `main.go:61`. Add a `PUSH_SINK` mode plus APNs/FCM config, keep `LogDispatcher` as the dev implementation, and fail startup if a production deploy resolves to the stub. Until then this service is a sink-hole for every push.
- [high] Wrap the per-message call in `jobguard.Run(msg, func(){ h.HandleJetStreamMsg(mCtx, msg) })` — `main.go:86`. One line; removes the poison-message crash-loop class.
- [high] Size the consumer with `stream.WithOutageRetryBudget(s, jsretry.DefaultBackoff)` — `main.go:121` — so the last hop rides out the same 1 h window the producer's `Duplicates` window and `MaxDeliver` already assume. Raise `MaxAckPending` alongside it, since parked messages hold slots for the whole window.
- [medium] Replace the hand-rolled loop with `natsmetrics.Start(...)` — `main.go:74-89` — to inherit `LoopFailed`, the in-flight/terminal-drop counters and the loop-up gauge for free, and add a loop-liveness check to `health.ServeWithPprof` so a dead consumer fails readiness.
- [medium] Add `bootstrap.go` with the standard `bootstrapConfig{Enabled}` no-op helper and a non-bootstrap path that verifies PUSH stream presence with a clear error — `main.go:65` — matching the 12 sibling services.
- [medium] Give `Dispatch` a per-recipient result (delivered/permanently-failed/retry) so a redelivery only re-sends the unresolved accounts — `handler.go:16-18`. Otherwise every NAK duplicates notifications on users' devices once a real sink lands.
- [low] Collapse `handler.go:36-47` onto `jsretry.Settle` and drop the manual `errcode.IsPermanent` branch.
