# notification-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `2155c60` (base `main`)  
**Overall score:** 3.2 / 5 (baseline 2026-09-01: 3.2, Δ -0.0)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

notification-worker's fan-out core is well engineered — error tiering, sonic on the hot path, projected reads, `jsretry.Settle` everywhere, and consumer-defined interfaces that make gates easy to swap — but four independent reviewers converged on the same shutdown bug and the same bootstrap defect. The ROOMS invalidation consumer goroutine is tracked by no WaitGroup, and shutdown calls `invalIter.Stop()` then immediately `close(invalCh)`, so a message already returned by `Next()` sends on a closed channel and panics, skipping the NATS drain and every database disconnect. The dev bootstrap hands `bootstrapStreams` the `.created` leaf instead of the `pkg/stream` schema, so with `BOOTSTRAP_STREAMS=true` a notification-worker restart narrows the shared MESSAGES-CANONICAL stream and history-service's edit/delete/pin publishes fail until another service re-widens it (production is unaffected). Beyond those: `PRESENCE_RPC_ENABLED=true` calls a snapshot RPC that no service serves, so DND suppression can never engage and the contracts doc still says "flip once live"; that same ops doc tells operators to provision a 5-minute duplicate window when the service refuses to start under 1 hour; the second consumer is hand-rolled inline in a 359-line `main()` bypassing `DurableConsumerDefaults`; per-message dependency timeouts sum well past `AckWait` with no overall deadline; `MONGO_URI` is defaulted while `WithDegradedStart` removes the fail-fast backstop; and the room-meta L2 tier is built with a nil breaker, unlike both sibling hot-path services. Coverage is 59.5% only because `main` holds 30% of the statements — everything else sits at 84.8% — but the consume loop, invalidation loop and shutdown sequence have no test at any tier, and the history parent fetcher is a verbatim copy of broadcast-worker's.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 1 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 1 critical, 3 high, 18 medium, 20 low, 10 nitpick (52 total).  
**Highest-risk dimension:** Test coverage (1).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [medium] Shutdown race: untracked invalidation reader can send on a closed channel and panic — `notification-worker/main.go:305-326` vs `main.go:417-421` — The reader goroutine is in no WaitGroup. Step 3 calls `invalIter.Stop()`, step 4 immediately `close(invalCh)`; a message already returned by `Next()` (line 307) that reaches `invalCh <- evt.RoomID` (line 319) after the close panics — `select`/`default` does not protect a send on a closed channel. Shutdown-only and narrow, but the panic skips the remaining steps (NATS drain, Mongo/Valkey disconnect, obs flush).
- [medium] Per-message logs are not trace-correlated: 37 non-`Context` slog calls vs 2 `*Context` — `handler.go:247,332,387,497`, `presence.go:76,83,87,95`, `usersettings.go:107` — The o11y logger installed by `pkg/obs/obs.go:228-230` correlates trace/span ids only through the ctx passed to `Handle`; `slog.Warn(...)` passes `context.Background()`, so every in-pipeline warning is orphaned from its span. Siblings use `*Context` almost everywhere (broadcast-worker 32/39, message-worker 24/26). `presence.go:76-95` also carry no `request_id` at all.
- [medium] Connection string and secret defaulted, and `WithDegradedStart` removes the fail-fast backstop — `main.go:41` (`MONGO_URI` → localhost), `main.go:44` (`MONGO_PASSWORD` → ""), `main.go:120` — CLAUDE.md: never default connection strings/secrets. With `mongoutil.WithDegradedStart()` (`pkg/mongoutil/mongo.go:39-41`, "return a usable client on an unreachable MongoDB") a pod missing `MONGO_URI` starts "healthy" against localhost and NAKs every cold-room message for the outage budget instead of exiting. Repo-wide pattern (`broadcast-worker/main.go:53,56`), but this service's degraded start makes it bite.
- [low] Log-AND-return on batch emit failure — `handler.go:332-335`, then `main.go:376` → `pkg/jsretry/jsretry.go:137-138` — The per-batch `slog.Error` fires, then `jsretry.Settle` logs "message failed — retrying" with the aggregate whose text already embeds each `emit push batch N: …` (`handler.go:335,350`). One failure, two ERROR lines.
- [low] Ack errors silently discarded without the required comment — `main.go:314`, `main.go:324` — The same file logs ack failures on the migration path (`main.go:369-371`) and `jsretry.settle` logs them; these two are the odd ones out.
- [low] Bare `err` returned from the publisher adapter — `emit.go:88-92` — `jsPublisher.PublishMsg` records the metric and returns `err` unwrapped. The caller wraps at `emit.go:62`, so nothing is lost on the wire, but the per-function rule is broken.
- [nitpick] `errcode.Parse` used without the `Code.Valid()` guard the sibling clients apply — `presence.go:86` vs `badge_client.go:56`, `parent_fetcher.go:82` — Safe today (a `PresenceSnapshotReply` has no top-level `error` key), but three RPC clients should share one envelope check.
- [nitpick] Redundant loop-variable copy — `presence.go:70` (`ch := ch`) — go.mod is Go 1.25; `handler.go:383` correctly omits it.
- [nitpick] Two startup failures log the identical message "invalid config" — `main.go:94`, `main.go:98` — pool vs breaker validation are indistinguishable.
- [nitpick] `nosemgrep` suppression lacks the `-- <reason>` suffix the rule asks for — `badge_client.go:45` vs `.semgrep/jsnak.yml:65-66` — the justification sits on the lines above instead.
- [nitpick] Unit tests boot an in-process `nats-server` — `parent_fetcher_test.go:25-38`, reused by `badge_client_test.go` — grey against "never connect to real NATS in unit tests"; embedded (no Docker) and used repo-wide (`pkg/natsutil`, `pkg/natsrouter`, broadcast-worker), so noted, not scored.

### Recommendations

- [medium] Track the invalidation reader in a WaitGroup and wait for it between `invalIter.Stop()` and `close(invalCh)` — `main.go:305,417-421` — removes the send-on-closed-channel panic; or let the reader own `close(invalCh)` when `Next()` errors.
- [medium] Switch every in-pipeline log to `slog.WarnContext/ErrorContext(ctx, …)` and add `request_id` to the four presence lines — `handler.go:247,332,387,497`, `presence.go:76,83,87,95`, `usersettings.go:107` — restores the trace correlation the o11y handler gives for free; matches broadcast-worker/message-worker.
- [medium] Mark `MONGO_URI` `required` (drop the `""` default on `MONGO_PASSWORD`), or refuse `WithDegradedStart` when `MONGO_URI` is still the default — `main.go:41,44,120` — restores fail-fast on the one misconfiguration this service otherwise runs through silently.
- [low] Delete the per-batch `slog.Error` at `handler.go:332` (or downgrade to Debug); the aggregate returned at `handler.go:350` already reaches `jsretry.Settle`'s single log line.
- [low] Add `// ack error intentionally ignored: …` or log the failure at `main.go:314,324`, matching `main.go:369-371`.
- [low] Wrap the adapter's return at `emit.go:90-91` or move the metric call into `mobileEmitter.Emit` so the adapter is a pure pass-through by design.
- [nitpick] Extract one `parseRemoteEnvelope(data)` helper shared by `presence.go`, `badge_client.go`, `parent_fetcher.go` so all three check `Code.Valid()` identically.
