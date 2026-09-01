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
