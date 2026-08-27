# Production readiness: `roomlist-worker`

| | |
|---|---|
| **Service** | `roomlist-worker` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/pr-188-dry-refactor-7v5g7s` |
| **Head** | `17c8c2b` |
| **Overall score** | **3.7 / 5** |

## TL;DR

`roomlist-worker` is a small, cohesive, unusually well-reasoned service whose production
logic is better than the repo average — error classification, replay-safe writes, flush-budget
validation against `EffectiveAckWait`, back-pressure that demonstrably engages, and a hot path
measured at ~3.9 µs and ~11 allocations per message with roughly 500× CPU headroom at the
target rate. Five of six dimensions score 4/5. The score is dragged down by test coverage
(63.4%, below the repo's mandatory 80% floor) and by a small number of genuine production
risks that cluster around one theme: **this service is designed to survive a MongoDB outage by
holding messages un-acked under `MaxDeliver=-1`, and it has no instrumentation to tell anyone
when it is doing so.** The single strongest finding is measured, not theoretical: the `mentions`
map amplifies ~2,600× per maximum-size message, so a slow-flush window can hold ~2.6 M entries
and build a bulk write that cannot finish inside `FLUSH_TIMEOUT` — a livelock, invisible,
with `/readyz` still green.

## Dimension scores

| Dimension | Score | One-line verdict |
|---|---|---|
| Go code quality | 4 / 5 | Strong idioms and slog discipline; an ERROR log on every clean shutdown and defaulted connection strings |
| Architecture | 4 / 5 | Conventions honoured and deviations justified; the only canonical consumer with no instrumentation |
| Test coverage | **2 / 5** | Excellent test *design*, but 63.4% measured — below the 80% floor, so the floor rule applies |
| Maintainability | 4 / 5 | Dense comments that mostly earn their length; `main.go` has outgrown its file |
| Integration | 4 / 5 | Subjects, streams and comparators correctly shared; two real cross-service field disagreements |
| Performance | 4 / 5 | Measured-fast and correctly batched; one unbounded map defeats the design's only bound |

Overall = mean of six = **3.7 / 5**.

## Findings by severity

| Severity | Count |
|---|---|
| critical | 0 |
| high | 5 |
| medium | 15 |
| low | 14 |
| nitpick | 4 |

The five `high` findings are:

1. `mentions` map amplification is unbounded and can livelock the consumer — *performance*
2. No service or consumer instrumentation, under `MaxDeliver=-1` — *architecture, performance*
3. Unit coverage 63.4%, below the repo minimum of 80% — *test coverage*
4. A cross-site sender's `lastSeenAt` advance is never federated home — *integration*
5. The Ack-vs-Nak decision rests on an invariant recorded nowhere near its use — *maintainability*

## How this report was produced

Six independent expert passes, each reading `CLAUDE.md` and the full service before judging,
then cross-checked against source by the synthesizer. Where two experts disagreed on a line
number or scope, the claim was re-verified against the file and the corrected form recorded
here — three findings were amended that way, and one was downgraded after it turned out to be
a fleet-wide pattern rather than a defect this service introduced.

**Verification status of the SAST gate** is recorded in the Code quality chapter and is
partial: `gosec` and the repo's own semgrep rules ran clean, but `govulncheck` and the semgrep
registry rulesets could not run in this environment. CI remains authoritative for those two.

---

# 2. Code quality — 4 / 5

Error classification, wrapping, `slog` discipline, batch/flush concurrency and shutdown
ordering are all better than repo average and heavily reasoned in comments. Deductions are for
a misleading ERROR-on-clean-shutdown log, defaulted connection strings, three bare `err`
returns, and the mock/coverage deviations.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `medium` | `main.go:340` | `consumeLoop` logs `slog.Error("consume loop stopped; no further room-list state will be written")` unconditionally, so every graceful `iter.Stop()` on a normal pod termination emits an alarming ERROR line. `state.stopping` is used two lines earlier to suppress the self-SIGTERM but is not used to pick the log level. |
| `medium` | `main.go:32,35` | `NATS_URL` and `MONGO_URI` carry `envDefault:"nats://localhost:4222"` / `"mongodb://localhost:27017"` rather than `,required`, against CLAUDE.md §3 ("never default secrets or connection strings"). The dominant repo convention is `,required` — room-service, message-worker, message-gatekeeper, media-service, push-notification-service. |
| `medium` | `main.go:351` | The loop uses `jobguard.Guard` (recover-and-continue) rather than `jobguard.Run` (Ack-drop), so a deterministic panic in `deriveIntents`/`mention.Parse` leaves the message un-acked under `MaxDeliver=-1` and it redelivers forever, while `consumeState.Check` still reports ready because the loop is alive. The comment above it claims the crash-loop is avoided — only the *process* crash is, not the infinite redelivery. |
| `low` | `store_mongo.go:98,109,120` | All three `Bulk*` methods end `_, err := …BulkWrite(…); return err` — a bare `err` return, which CLAUDE.md §3 forbids outright. The chain only reads well because `mongoutil` and `flush.go` happen to wrap on both sides. |
| `low` | `main.go:291` | `validateFlushBudget` bounds `2×FLUSH_TIMEOUT + FLUSH_INTERVAL` against `EffectiveAckWait` but never against the 25 s shutdown budget, even though shutdown charges up to one in-flight periodic flush plus `flushloop.DefaultFinalTimeout` before `Drain`/`Disconnect`. Safe at defaults (≤15 s of 25 s), silently unsafe once an operator raises `FLUSH_TIMEOUT`. |
| `low` | `store.go:8` | No `//go:generate mockgen` directive and no `mock_store_test.go`, deviating from the layout every sibling service follows. The stated justification — that gomock cannot express call order and context cancellation — is factually wrong: `gomock.InOrder` plus `DoAndReturn` express both. Hand-written stubs remain a defensible *outcome*; the waiver rests on a bad *reason*. |
| `low` | package-wide | Unit coverage 63.4%, under the 80% floor. See the Test coverage chapter for why the shape of that number matters. |
| `nitpick` | `main.go:317` | `Check()` does `fmt.Errorf("consume loop stopped: %w", *err)` on a `*error`; a stored nil would render `%!w(<nil>)`. Fails closed, harmless today. |
| `nitpick` | `store_mongo.go:23` | `NewMongoStore` is exported and returns the unexported `*mongoStore`, beside unexported `newFlusher`/`newBatch` in a `package main` nothing can import. |
| `nitpick` | `main.go:222` | `messageIterator` is a single-method interface without the `-er` suffix CLAUDE.md §3 mandates. |

## SAST gate — partial, and the gaps are environmental

CLAUDE.md §5 makes SAST a blocking CI gate. Status as run for this audit:

| Tool | Result | Detail |
|---|---|---|
| `gosec` | **PASS** | `-severity medium -confidence medium -tests=true`, whole module, zero findings; nothing in `roomlist-worker/`. |
| `semgrep` — repo rules | **PASS** | `.semgrep/` (errcode, jsnak, exec, room-subject, msgraph-secrets): 9 Go rules over 9 files, **0 findings**. These are the CLAUDE.md-specific rules — no bare `Nak()`/`NakWithDelay(0)`, no direct `ConsumerConfig.BackOff` assignment, no inline `errcode.Reason`, no `errcode.WithCause(errcode.X(...))`, no multi-`%w`. |
| `semgrep` — registry | **COULD NOT RUN** | `p/golang` and `p/security-audit` need `semgrep.dev`, which the agent proxy denies (`403 Forbidden` on CONNECT). |
| `govulncheck` | **COULD NOT RUN** | `vuln.go.dev:443` denied by gateway policy (`403` on CONNECT, confirmed in the proxy's own relay-failure log). Dependency-CVE reachability is **unverified, not clean**. |

`make lint` and `make test SERVICE=roomlist-worker` both pass. **Do not read this chapter as a
clean SAST result** — two of the three scanners did not execute, and CI is authoritative for
them.

## Recommendations

1. `medium` — `main.go:339-342`: branch the stop log on `state.stopping.Load()`. `slog.Info("consume loop stopped (shutdown)")` when intended; keep `slog.Error` for the unexpected path only, so deploys stop firing error-rate alerts.
2. `medium` — `main.go:32,35`: change to `env:"NATS_URL,required"` and `env:"MONGO_URI,required"`, moving the localhost values into `deploy/docker-compose.yml` where the other required-var services keep them.
3. `medium` — `main.go:351`: either switch to `jobguard.Run(msg, …)` so a panicking message is Ack-dropped, or amend the comment to state plainly that a deterministic panic redelivers indefinitely under `MaxDeliver=-1`.
4. `low` — `store_mongo.go:98,109,120`: wrap each return, e.g. `return fmt.Errorf("bulk update room last message: %w", err)`. `errors.As(mongo.BulkWriteException)` in `classifyFlushErr` is unaffected by the extra layer.
5. `low` — `main.go:291`: give `validateFlushBudget` a fourth argument (the 25 s shutdown budget) and reject `FLUSH_TIMEOUT + flushloop.DefaultFinalTimeout >= budget − consumeDrainAllowance`, so raising `FLUSH_TIMEOUT` fails at startup rather than at SIGKILL.
6. `low` — `store.go:8`: correct the justification comment to the real reason (a three-method interface where a stub is cheaper than a generated mock), since the current one is checkably false.

---

# 3. Architecture — 4 / 5

Unusually rigorous for its size: flush-budget validation against `EffectiveAckWait`, replay-safe
monotonic writes, majority write concern, self-SIGTERM on a dead loop. It loses a point for
being the only MESSAGES-CANONICAL consumer with no consumer instrumentation while running
`MaxDeliver=-1` — precisely the combination in which a stalled consumer is invisible.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | `main.go:134-181` | No `natsmetrics.Consumer` wiring (`Track`/`LoopStarted`/`LoopFailed`), unlike every sibling canonical consumer (`broadcast-worker/main.go:420`, `message-worker/main.go:228`). With `MaxDeliver=-1`, a batch that Naks forever exhausts `MaxAckPending=1000` and the worker silently stops writing room-list state — while `/readyz`, which detects a *dead* loop but not a *stalled* one, keeps answering 200. |
| `medium` | `main.go:371-377` | **Verified against source.** `cc.MaxDeliver = -1` is applied *after* `stream.DurableConsumerDefaults(s)` has already used the configured `CONSUMER_MAX_DELIVER` to clamp the derived `BackOff` length (`pkg/stream/consumer.go:82-87`). The clamp is explicitly meant to be skipped for an unlimited cap ("An unlimited MaxDeliver skips the clamp below"), but it has already fired. Net effect: `CONSUMER_MAX_DELIVER` has **no effect on the delivery cap** yet silently shortens server-side redelivery spacing. Nothing at the override says so. |
| `medium` | `main.go:378-381` | `DeliverNewPolicy` overrides the repo default, and the stated reason ("replaying would re-apply historical writes for no benefit") is weaker than it reads: `store.go:13-15` asserts every write is replay-safe and each filter is monotonic-guarded, so `DeliverAll` would be a bursty no-op. Meanwhile `DeliverNew` makes a deleted-and-recreated durable (ops action, snapshot restore, site rebuild) a permanent silent gap in badges and room pointers — the exact loss `MaxDeliver=-1` and `writeconcern.Majority` exist to prevent. |
| `medium` | `store_mongo.go:42-50` | The same-millisecond tie-break rule now exists in **three** hand-maintained encodings — `msgbucket.NewerRow`, `batch.go:93`, and this BSON `$or` — bound only by a comment. A change to `NewerRow`'s tie-break desyncs the BSON copy, and the affected rooms permanently stop serving previews because `previewForMsgId == lastMsgId` never holds again. |
| `medium` | `handler.go:60-66` | Mention derivation is duplicated across services on the same stream: this service parses content for *local* badges, `broadcast-worker/handler.go:426` derives *remote* badges from resolved `participants`, and no shared contract test pins the two sets equal. A divergence badges a mentionee on one site and not the other. |
| `low` | `store.go:8-11` | The no-mockgen justification is factually wrong (see Code quality). Hand stubs are a defensible outcome resting on a bad reason. |
| `low` | `flush_test.go:44-46` | `stubStore` writes `order`/`rooms`/`lastSeen`/`mentions` outside the mutex it takes for `sawDeadline` — a latent `-race` failure the moment a test drives `Run`'s ticker alongside a direct `Flush`. |
| `low` | `flush.go:129-133` | The "lastSeenAt precedes mentions so a self-mention doesn't badge the sender" invariant holds only when the lastSeen stage *succeeds*. On a permanent failure, `write` deliberately continues to mentions with the sender's `lastSeenAt` un-advanced — badging the sender against their own message. |

## Explicitly not a finding

**The single-goroutine `cons.Messages()` shape** (`main.go:323-326`) is a third pattern beside
CLAUDE.md's two, and the justification holds. All I/O is batched into the flusher, so the
per-message path is a sonic unmarshal plus a regex parse; workers would only contend on `f.mu`.
It is the sequential shape in everything but the `cons.Consume()` call, and the
`messageIterator` seam buys real testability (`consumeloop_test.go`). Bootstrap, deploy layout,
`pkg/subject` usage (zero raw `fmt.Sprintf` on subjects), shutdown ordering, and all three
JetStream backoff server rules from CLAUDE.md are clean.

## Recommendations

1. `high` — Wire `natsmetrics.Consumer`/`Track` in `main.go` as `message-worker/main.go:228-290` does, and add an ack-pending-saturation signal to `consumeState.Check()` so a *stalled* consumer fails readiness, not only a dead one.
2. `medium` — Set `s.MaxDeliver = -1` **before** calling `DurableConsumerDefaults(s)` at `main.go:370`, so the backoff schedule is derived against the real unlimited cap. Extend `main_test.go` to assert the schedule length is independent of `CONSUMER_MAX_DELIVER`.
3. `medium` — At `main.go:381`, either revert to `DeliverAll` (writes are already replay-safe by contract) or add an explicit `OptStartTime`/one-shot cutover knob so durable recreation cannot silently skip a backlog. Document the recreation runbook in the comment either way.
4. `medium` — Move `roomLastMsgFilter` (`store_mongo.go:42`) into `pkg/msgbucket` as `NewerRowFilter(roomID, msgID, at) bson.M`, next to `NewerRow`, so the Go and BSON encodings of the tie-break cannot drift.
5. `medium` — Add a shared table-driven test in `pkg/mention` that both `roomlist-worker/handler_test.go` and `broadcast-worker` bind to, pinning local-badge accounts against federated-mention accounts for identical content.
6. `low` — Put every field write in `flush_test.go`'s `stubStore` under `s.mu`.
7. `low` — Consider `jsretry.LowLatencyBackoff` at `flush.go:165`: unread badges and room ordering are user-visible, and `broadcast-worker` already settles that way on the same stream.
