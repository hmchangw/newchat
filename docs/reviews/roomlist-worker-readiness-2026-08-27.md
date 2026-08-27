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

---

# 4. Test coverage — 2 / 5

The test *design* is exemplary — rigorous table-driven error-path coverage, comments that
explain the failure mode each case pins, real-MongoDB integration depth. But measured coverage
is **63.4%**, below the repo's mandatory 80% floor, so CLAUDE.md §4's floor rule applies and
caps this dimension at 2.

## Measurements

```
go test -race ./roomlist-worker/...
ok  github.com/hmchangw/chat/roomlist-worker  1.609s        PASS

go tool cover -func=/tmp/cov.out
total:  (statements)  63.4%
```

`make generate SERVICE=roomlist-worker` was a **no-op** with an empty `git status` — consistent
with `store.go`'s documented no-mockgen decision. **No mocks are stale**, and nothing needed
reverting. The tree was verified clean at exit.

Five lowest-covered functions:

| Function | File | Coverage |
|---|---|---|
| `main` | `main.go:66` | 0.0% |
| `requestSelfShutdown` | `main.go:261` | 0.0% |
| `pretouchJSON` | `pretouch.go:16` | 0.0% |
| `NewMongoStore` | `store_mongo.go:23` | 0.0% |
| `BulkAdvanceLastSeen` / `BulkSetMentions` | `store_mongo.go:101/112` | 80.0% |

## The shape of the number matters more than the number

`main()` alone is **83 of 257 statements (32%)**, entirely untested. Excluding it, the package
sits at **93.7%** (163/174) — only **11 non-`main` statements** are uncovered anywhere.

So this is not the usual failure mode of a high percentage concealing untested error branches.
It is the inverse: the error branches are unusually well covered and one giant wiring function
drags the total under the floor. For calibration, sibling workers measure 67.3%
(broadcast-worker), 58.2% (notification-worker), 51.3% (message-worker), 37.3% (outbox-worker) —
**the floor breach is fleet-wide, not a roomlist-worker regression.** That context does not
waive the gate; it does change what fixing it means.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | package | Coverage below repo minimum 80%, currently **63.4%**. |
| `medium` | `main.go:66` | `main()` is 83 uncovered statements containing load-bearing wiring no unit test can reach: that `validateFlushBudget` is fed `cfg.Consumer.EffectiveAckWait()` rather than the configured `AckWait`, that its error exits non-zero, that `cfg.Pool.Validate()` is called, and that `requestSelfShutdown` is installed as `onUnexpectedStop`. The helpers are 100% covered; their call sites are not. |
| `medium` | `integration_test.go:19` | The integration suite exercises real MongoDB only. There is no `testutil.NATS(t)` test, so `bootstrapStreams`' verify/create paths, `buildConsumerConfig`'s acceptance by a live server, and `consumeLoop` against a real JetStream iterator are never validated end to end. This is exactly where the `MaxDeliver`/`BackOff` clamp interaction (Architecture, `medium`) would have been caught. |
| `medium` | `flush.go:132` | The only uncovered non-`main` branch of real logic: a **transient** failure in the third stage (`subscription mentions`). Every transient test fails at stage 1 or 2, so the `return flushOutcome{err: err}` after the last stage never executes. |
| `low` | `main_test.go:35` | **Verified; the line reference has been corrected from the expert's `:22`.** `TestBuildConsumerConfig_BotModePrefixesDurable` never calls `buildConsumerConfig`. It asserts only `stream.PipelineBot.ConsumerName(...)` and `stream.PipelineUser.ConsumerName(...)` — `pkg/stream` behaviour that would pass identically with every line of `roomlist-worker` deleted. |
| `low` | `flush.go:67-76` | `flushOutcome.stageCodes` / `mongoErrs` — the fields whose own comments call them the load-bearing poison-vs-retry diagnostic — are executed but never asserted. No test file mentions `stageCodes`, `mongoErrs`, or `flushOutcome`, so the `"<stage>=<codes>"` pairing could break silently. |
| `low` | `consumeloop_test.go:214` | `TestConsumeLoop_ReadyWhileConsuming` duplicates the first assertion of `TestConsumeLoop_ExitFailsReadiness` (`:198`) and only probes a zero-value `consumeState`. |
| `info` | `store_mongo.go:103,114` | `BulkAdvanceLastSeen`/`BulkSetMentions` sit at 80% because unit tests pass only empty maps; the loop bodies are covered by integration tests, which the untagged profile cannot see. |

## What is genuinely good here

Unit tests touch no real MongoDB or NATS — hand-written `stubStore`/`fakeMsg`/`fakeIterator`/
`fakeStreamManager` only. `integration_test.go` has the required
`func TestMain(m *testing.M) { testutil.RunTests(m) }` and uses `testutil.MongoDB(t, prefix)`
for per-test isolation. Every helper lives in a `_test.go` file. No shared mutable state, no
order dependence. `TestClassifyFlushErr`, `TestValidateFlushBudget`, `TestDeriveIntents` and
`TestBootstrapStreams` are properly table-driven with descriptive `t.Run` names, and cover the
mixed-code allow-list case, the WriteConcern/RetryableWriteError overrides, non-positive
duration validation, and panic recovery in both the consume loop and the flush loop.

`TestValidateFlushBudget` deserves specific mention as the opposite of the hollow test above:
13 cases, each carrying a comment naming the failure mode it pins, including the exact
arithmetic an earlier version waved through — *"20.25 s looks safe against a 30 s AckWait, but
a stalled flush plus the wait for the next tick plus this message's own flush is 40.25 s."*

## Recommendations

1. `high` — Extract `main()` into `run(ctx context.Context, cfg config) error` and test the wiring: that `validateFlushBudget` receives `EffectiveAckWait()` rather than `cfg.Consumer.AckWait`, and that a budget or `Pool.Validate` error returns non-zero without connecting to anything. **This single change moves the package over 80%.**
2. `medium` — Add a NATS integration test (`testutil.NATS(t)`) that calls `bootstrapStreams` with `Enabled=true`, creates the consumer from `buildConsumerConfig`, and asserts the server accepts it and reports `MaxDeliver=-1` and `DeliverNew`.
3. `medium` — Cover `flush.go:132`: a `stubStore` failing only `"mentions"` with a plain transient error, asserting all three stages ran and the message was Nak'd.
4. `medium` — Assert `flushOutcome.stageCodes` and `mongoErrs` directly for a two-permanent-stage batch, pinning the `"subscription mentions=[11000]"` shape that makes a dropped poison batch diagnosable.
5. `low` — Either rename `TestBuildConsumerConfig_BotModePrefixesDurable` to reflect that it tests `stream.Pipeline.ConsumerName`, or make it call `buildConsumerConfig` with a bot-prefixed durable and assert `cc.Durable`.
6. `low` — Add a unit test pinning `cc.BackOff` from `buildConsumerConfig` (length, and first entry equal to `AckWait`), guarding the MaxDeliver-override/BackOff-clamp interaction CLAUDE.md flags as a hard consumer-create error.
7. `low` — Cover `requestSelfShutdown` by injecting the signal function, and drop the redundant `TestConsumeLoop_ReadyWhileConsuming`.

---

# 5. Maintainability — 4 / 5

Small, cohesive, unusually well-tested, and the dense commenting is overwhelmingly load-bearing
WHY rather than restatement. It loses a point for a `main.go` that has visibly outgrown its
file, a dead config knob, an already-stale deploy README, and one Ack/Nak correctness invariant
that lives entirely in `write`'s control flow.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | `flush.go:139` → `pkg/jsretry/jsretry.go:113` | The drop-vs-retry decision runs `errcode.IsPermanent` — an `errors.As` **any-match** — over an `errors.Join`. A mixed join would silently Ack-drop a transient failure. The only guard is `write`'s early return at `:127`/`:131`/`:133`, and neither the `SettleQuiet` call at `:165` nor `IsPermanent`'s doc ("err's chain") mentions joins. Currently correct and pinned by `flush_test.go:158,:176` — but nothing tells the next editor that adding a stage which *records* rather than *returns* a transient error breaks it, and the failure mode is silent: an Ack looks identical to success from outside. |
| `medium` | `main.go` (whole) | Four separable units in one 383-line file — config + `validateFlushBudget`; wiring/shutdown; `consumeState`/readiness; `consumeLoop` + `buildConsumerConfig`. `config_test.go` and `consumeloop_test.go` already exist with no matching production file, which is the codebase pointing at its own seams. |
| `medium` | `batch.go:97-99` | `lastMentionAllAt` merges only inside the `in.LastMsgID != ""` branch, so any future intent carrying an `@all` timestamp without a room pointer — for instance an edit that *adds* `@all` — is dropped with no error. No test covers that combination. |
| `medium` | `main.go:351` vs `:344-350` | `jobguard.Guard` turns a deterministic panic into an un-acked message under `MaxDeliver=-1` (unbounded redelivery) while `consumeState.Check` still reports ready. The comment describes the crash-loop it prevents, not the silent stall it leaves. (Same defect as Code quality `medium`; recorded once in the action list.) |
| `medium` | `main.go:377` | `cc.MaxDeliver = -1` applied after the `BackOff` clamp has already used `CONSUMER_MAX_DELIVER`; nothing at the override says the env var silently degrades to a schedule-length knob. (Same defect as Architecture `medium`.) |
| `low` | `main.go:57` | `MetricsAddr` / `METRICS_ADDR` is declared, defaulted to `:9090`, and never read — the real listener comes from `OTEL_EXPORTER_PROMETHEUS_PORT` (default `2112`, `pkg/obs/obs.go:88`). **Downgraded from `medium` after verification:** the identical dead knob exists in `message-gatekeeper`, `broadcast-worker`, `message-worker`, `history-service` and `room-worker`. This is inherited fleet-wide vestigial config, not something this service introduced — fix it fleet-wide or not at all. |
| `low` | `deploy/README.md:57` | *"The health endpoint deliberately checks only NATS"* contradicts `main.go:174-177`, which registers `consume.Check()` alongside it. **Verified.** Already stale on a newly extracted service. |
| `low` | `deploy/user/Dockerfile` vs `deploy/bot/Dockerfile` | Byte-identical; every change must be made twice and nothing enforces it. |
| `low` | `deploy/README.md` Tuning table | Omits `FLUSH_TIMEOUT`, yet `validateFlushBudget` refuses startup when `2×FLUSH_TIMEOUT + FLUSH_INTERVAL >= CONSUMER_ACK_WAIT`. Following the table to tune `CONSUMER_ACK_WAIT` down hard-fails the pod. |
| `low` | `flush.go:117-119` vs `:61-65` | The stage-name/codes rationale is written out twice; two copies of one rationale drift. |
| `low` | `flush.go:126-134` | A permanent-then-transient sequence discards the recorded `stageCodes`/`mongoErrs`, so the poison document's codes are never logged on that pass. |
| `nitpick` | `main.go:358` | `DefaultBackoff` passed alongside `errcode.Permanent(...)`; the permanent branch never reads it. |

## Comment verdict

31–37% comment lines in `main.go` and `flush.go`. The long blocks earn their length:
`validateFlushBudget`'s 19 lines explain a `2×` factor no reader could derive,
`classifyFlushErr`'s explains deny-by-default convergence, `projection.go`'s explains why a
narrow struct exists at all. Genuine WHAT-noise is rare (`store_mongo.go:95-96`, `batch.go:118`).
The real cost is **redundancy**: the readiness rationale is restated at `main.go:226-237`,
`:311-313` and `:335-338`.

## Adding a fourth write stage — the newcomer walkthrough

Concretely, adding a room-level unread count:

1. **`handler.go`** — add fields to `writeIntents` plus a presence marker, populate in `deriveIntents`. *Trap:* pick a marker that is not a sub-field of `LastMsgID` — see the `lastMentionAllAt` finding.
2. **`batch.go`** — new map on `batch`, merge in `add`, size in `newBatch`. *Traps:* forgetting `newBatch` gives a nil map → panic → recovered by jobguard → message never acks → infinite redelivery **with a green readiness probe**; and a map keyed by anything not bounded by `held` breaks the documented `MaxAckPending` bound.
3. **`store.go` + `store_mongo.go`** — new `Bulk*` method, models, and a `$not/$gte`-style filter if it must be replay-safe.
4. **`flush.go:126-134`** — insert the stage. **Two non-obvious invariants:** it must go *after* `lastSeen` if it reads sender state, and it must use the `stage(...)` + early-return shape or the `errors.Join`/`IsPermanent` safety at `:139` collapses.
5. **`flush.go:151-157`** — add the count to the failure log attrs, or it is silently omitted.
6. **`flush_test.go`** — extend `stubStore` with the method, a `failWith` key and an `order` entry, or the package stops compiling. Then `batch_test.go`, `handler_test.go`, `integration_test.go`.
7. **`deploy/README.md`** and both compose files if a knob is added.

Realistically 6–8 files. The two that bite are step 4's ordering and error-shape invariants,
both discoverable only by reading `write`'s 20-line doc comment.

## Recommendations

1. `high` — At `flush.go:165`, state the invariant in one line ("`out.err` is either a single transient error or a join of only permanents — see `write`") and add a note to `pkg/errcode.IsPermanent` that it matches **any** branch of an `errors.Join`.
2. `medium` — Split `main.go` into `consumeloop.go` (loop + `consumeState` + `requestSelfShutdown`) and `config.go` (struct + `validateFlushBudget` + `buildConsumerConfig`), matching the test files that already exist. This also sets up the `run()` extraction the coverage chapter needs.
3. `medium` — Add a `batch_test.go` case for `LastMentionAllAt` with an empty `LastMsgID`, and decide explicitly whether it should be dropped.
4. `medium` — Add a one-line comment at `cc.MaxDeliver = -1` noting what it does to `CONSUMER_MAX_DELIVER`.
5. `low` — Fix the deploy README's health-check sentence and add `FLUSH_TIMEOUT` (with the budget rule) to the Tuning table.
6. `low` — Collapse the duplicated Dockerfile to one shared file referenced by both compose files; de-duplicate the three readiness comments down to one.
7. `low` — Leave `MetricsAddr` alone unless doing a fleet-wide sweep; removing it here alone makes this service inconsistent with five siblings for no gain.

---

# 6. Integration — 4 / 5

Subject, stream and consumer wiring, the typed-projection contract and the tie-break comparator
are all correctly shared with peers and pinned by tests. Two genuine cross-service field
disagreements and one missing fleet convention keep it off 5.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | `store_mongo.go:101-110` | `BulkAdvanceLastSeen` filters `(roomId, u.account)` in the **room's** site only, but a cross-site member's authoritative subscription lives at their **home** site (created by `inbox-worker/main.go:586`, and the reason `room-service/handler.go:1436` federates `subscription_read` at all). Nothing federates the send-implied advance — `broadcast-worker/handler.go:426 federateMentions` relays the *badge* across sites but not the sender's `lastSeenAt`. Same-site senders write one document so the gap is invisible; **for a cross-site sender, their own message leaves the room `HasUnread=true` at home** (`user-service/service/subscriptions.go:256`) and their own `@all` self-badges `HasGroupMention` (`:257`) — because `lastMentionAllAt` arrives by RPC from the room's site while `lastSeenAt` is read from the un-advanced home copy. |
| `medium` | `store_mongo.go:71-75` | Writes `updatedAt: u.at` — the *message*'s `createdAt` — into a field every other writer treats as a document-modification clock set to `time.Now()` (`room-worker/store_mongo.go:766`, `room-service/store_mongo.go:1833`), and which `inbox-worker/main.go:148` uses as a strict monotonic replication guard (`updatedAt: {$lt: room.UpdatedAt}`). Guarded only against `lastMsgAt`, so a message flushing just after a rename regresses `updatedAt` below the rename's timestamp. Latent rather than live only because the migration `room_sync` path is currently its sole reader. |
| `medium` | `store_mongo.go:23-28` | `NewMongoStore` does not call `mongoutil.WarnMissingIndexes(ctx, subCol, "roomId_1_u.account_1")`, which every other service issuing this exact filter does (`inbox-worker/main.go:560-562`, `user-service/mongorepo/subscriptions.go:74`, `bot-room-service/store_mongo.go:36`) — and this service runs the shape at the highest volume in the fleet, twice per flush. |
| `low` | `projection.go:25-44` | A *renamed* or retyped `model.Message` field breaks `projection_test.go:52` at compile/assert time, but an **added** field with pointer semantics (a future soft-delete or suppression flag) is silently ignored and the room pointer keeps advancing. No `reflect.NumField` drift guard exists, unlike the risk the comment claims to manage. |
| `low` | `broadcast-worker/handler.go:241-251` | Badge sources diverge under failure: roomlist-worker badges from raw `mention.Parse(content).Accounts`, broadcast-worker federates from `resolved.Participants`, and a `FindUsersByAccounts` error is logged-not-returned — so local mentionees get badged while remote ones silently do not. |
| `info` | `main.go:371-377` | The `MaxDeliver`/`BackOff` ordering (see Architecture). Harmless for delivery semantics — the server normalizes `-1` first — but the schedule length is clamped to a cap that no longer applies. |

## Contract matrix

| Contract | Other participants | Agrees? |
|---|---|---|
| `MESSAGES-CANONICAL-{siteID}` / `BOT-…` via `stream.Resolve` | message-gatekeeper, history-service (`.updated`), broadcast / notification / search-sync | **Yes** — no raw `fmt.Sprintf` subjects anywhere |
| Durable `roomlist-worker` / `bot-roomlist-worker` | distinct streams per mode | **Yes** |
| Stream bootstrap (`bootstrap.go:31-40`) | message-gatekeeper / bot-message-handler own it | **Yes** — `Name+Subjects` only, verifies when disabled |
| `model.MessageEvent` → `eventProjection` | both user and bot producers use the same struct | **Yes** — parity-tested |
| sonic decode | CLAUDE.md `Reactions` struct-keyed-map caveat | **Yes** — projection has no such type; `pretouch.go` warms exactly what is decoded |
| `msgbucket.NewerRow` tie-break | `broadcast-worker/preview_writer.go:114,120` | **Yes** — same comparator; `store_mongo.go:42-49` is the same rule in BSON at ms granularity |
| `rooms.lastMsgAt/lastMsgId/lastMentionAllAt` | broadcast-worker (preview fields only), user-service / history-service (read) | **Yes** — disjoint halves, separate watermarks |
| `subscriptions.hasMention` | `inbox-worker/main.go:571-578`, room-service (clear) | **Yes** — identical `$not/$gte` guard (#467) |
| `subscriptions.lastSeenAt` | room-service `$set` on read; roomlist `$max` | **No** — see `high` finding |
| `rooms.updatedAt` | room-worker, room-service, inbox-worker guard | **No** — see `medium` finding |
| Client API | none registered — no `chat.user.` subject, no Gin routes | **N/A** — `docs/client-api.md` correctly unaffected; the `Timestamp` publish rule also does not bind, as the service publishes nothing |
| Deploy | matches broadcast / notification dual-mode layout; `golang:1.25.13-alpine` → `alpine:3.21`, repo-root context, `BOOTSTRAP_STREAMS=true` in compose | **Yes** |

## Recommendations

1. `high` — Federate the sender's `lastSeenAt` advance to their home site, or have roomlist-worker emit an OUTBOX `subscription_read`-shaped event for senders whose resolved `SiteID != siteID`. Reuse `inbox-worker`'s existing `UpdateSubscriptionRead` `$lt` guard; add the type to exactly one `pkg/outbox` filter set per CLAUDE.md.
2. `medium` — Stop writing `updatedAt` from `msg.CreatedAt` (`store_mongo.go:73`). Either drop it from `roomLastMsgModels` or write `$currentDate`, so the field keeps one meaning across roomlist-worker, room-worker and inbox-worker's guard.
3. `medium` — Add `mongoutil.WarnMissingIndexes(ctx, subCol, "roomId_1_u.account_1")` to `NewMongoStore`, matching inbox-worker's usage; take `ctx` in the constructor as those services do.
4. `low` — Add a `reflect.TypeOf(model.Message{}).NumField()` assertion to `projection_test.go`, so a new canonical field forces a conscious decision about the narrow projection.
5. `low` — Have `broadcast-worker.handleCreated` NAK rather than warn when `FindUsersByAccounts` errors on a message with parsed mentions, so cross-site badges cannot be lost while local ones land.
6. `info` — Record the `lastSeenAt` / `updatedAt` ownership rules in `deploy/README.md` alongside the existing preview/pointer section; it is currently the best cross-service contract doc in the repo and is the right home for both.

---

# 7. Performance — 4 / 5

A genuinely well-engineered hot path — ~4 µs and ~8–17 allocations per message, correctly
batched index-supported bulk writes, and back-pressure that actually engages. Held back by one
unbounded amplification path and zero service-level metrics. Figures below were measured with a
scratch benchmark module outside the repo; no repo file was touched.

## Findings

| Severity | Location | Defect |
|---|---|---|
| `high` | `batch.go:43,110-115` | **`mentions` amplifies ~2,600× per message, defeating the only bound the design has.** `MaxAckPending=1000` caps *messages*, but a 20 KB message yields **2,644 unique accounts** (measured), so a slow-flush window can hold 1000 × 2644 ≈ **2.6 M `subKey` entries (~250 MB** of map plus strings), then build a 2.6 M-operation `BulkWrite` that cannot finish inside `FLUSH_TIMEOUT` → Nak → rebuild → **livelock under `MaxDeliver=-1`**. The code documents `mentions` as the one map without a `MaxAckPending` bound; this quantifies what that costs. |
| `medium` | `pkg/mention/mention.go:15,38` | The regex is compiled once at package level (correct), but `FindAllStringSubmatch` runs at ~20 MB/s and allocates 4 objects per match: measured **1.04 ms / 20 KB** benign, **3.44 ms / 20 KB adversarial (1.34 MB, 8,239 allocs)**. This one call is ~60% of per-message cost at typical size and collapses the single consume goroutine's ceiling from ~240 k msg/s to **~290 msg/s** on maximum-size content. |
| `medium` | `store_mongo.go:101-120`, `main.go:114` | Both subscription bulks rely on the `subscriptions (roomId, u.account)` index, created only by `room-service/store_mongo.go:126`. This service neither creates it nor calls `mongoutil.WarnMissingIndexes`, so against a database where room-service has not run, **every bulk operation COLLSCANs, silently.** (Same defect as Integration `medium`.) |
| `low` | `flush.go:52` | `newBatch` runs **inside** `f.mu`: measured **23 µs / 56 KB** at 125 entries and **793 µs / 1.8 MB** at the 4096 clamp, versus 130 ns for `add`. Allocate before `Lock`, swap under it. |
| `low` | `main.go:193` | The `go func(){ wg.Wait(); close(done) }()` drain helper leaks if the drain times out. Benign — the process is exiting — but it is the one goroutine in the file with no termination path. |
| `low` | `store_mongo.go:87` | An `@all` room emits two `UpdateOne`s, so the rooms bulk is up to `2×len(rooms)`. |
| `info` | `handler.go:44`, `flush.go:35` | The two `//nolint:gocritic // hugeParam` suppressions guard a non-issue: `writeIntents` is 168 B, and by-value measures **130.1 ns/op** against by-pointer **141.1 ns/op** — by-value is *faster*. Keep the signature; the suppressions are noise, not debt. |
| `info` | `main.go:304` | `2×timeout + interval < ackWait` is sound and slightly conservative. A 10 s flush always leaves a tick buffered in the 1-slot ticker channel, so the true worst case is `max(2×timeout, timeout+interval)`; charging both is correct padding. `EffectiveAckWait` is the right input. Not covered: `flushloop.DefaultFinalTimeout` (5 s), which sits outside this budget but inside the 25 s shutdown allowance. |

## Throughput model

At ~500 msg/s with typical (~150 B) content, **per message**:

| Step | Time | Bytes | Allocs |
|---|---|---|---|
| sonic unmarshal | 978 ns | 415 B | 3 |
| `mention.Parse` (one mention) | ~2.3 µs | 386 B | 4 |
| `logctx.ConsumeContext` + `obs.ContextWithIdentity` | 404 ns | 128 B | 4 |
| `batch.add` | 130 ns | ~51 B amortized | 0 |
| **Total** | **≈3.9 µs** | **~980 B** | **~11** |

That is **0.2% of one core** and ~0.5 MB/s of garbage.

**Per 250 ms flush**: 125 held messages → ≤125 room operations (≤250 with `@all`), ≤125
lastSeen, ~25 mention ≈ **275 indexed single-document updates across 3 unordered bulks,
~1,100 writes/s** to MongoDB. Every filter is an exact index seek — `_id` for rooms, the unique
`roomId_1_u.account_1` prefix for both subscription stages, with `lastSeenAt` as a residual on
one fetched document. No N+1, no scan.

**The first bottleneck is MongoDB write throughput, not this process** — CPU headroom is ~500×.

Back-pressure traces correctly: a MongoDB outage fails a flush in ~2 s
(`MONGO_SERVER_SELECTION_TIMEOUT`), the batch Naks with 1 s–10 m jittered backoff, and the
un-acked count pins at `MaxAckPending`, blocking `iter.Next()` — bounded at ~1000 messages
(≤20 MB of retained payloads). **Nothing buffers unboundedly except `mentions`.** The second
bottleneck, only under attack, is the regex at ~290 msg/s.

## Recommendations

1. `high` — Cap `mentions` per batch (for example, force an early flush above `4 × MaxAckPending`) or cap `MentionAccounts` per message in `deriveIntents`; log the truncation either way. Without this, the one documented unbounded map is also the one that wedges the consumer.
2. `high` — Add three metrics: (a) flush-duration histogram plus outcome counter labelled `stage` / `success|transient|permanent`; (b) batch-size histogram over `held`/`rooms`/`lastSeen`/`mentions` — the only pre-OOM warning for finding #1 and the input for sizing `MaxAckPending`; (c) end-to-end age at settle (`now − msg.CreatedAt`) plus `num_ack_pending`, which is what tells an operator back-pressure has engaged and how stale room lists are.
3. `medium` — Replace `mentionRe.FindAllStringSubmatch` with a hand-rolled byte scanner; the grammar is trivial. Measured ceiling today is 20 MB/s with 4 allocs/match; a scanner is allocation-free and ~10–20× faster, which also largely neutralises the CPU half of finding #1.
4. `medium` — Call `mongoutil.WarnMissingIndexes(ctx, subCol, "roomId_1_u.account_1")` at startup, matching `room-service/store_mongo.go:129`.
5. `low` — Build the replacement batch outside `f.mu` (23–793 µs measured inside the critical section).
6. `low` — Tie `maxReuseCap` to `MaxAckPending` rather than the literal 4096; for `rooms`/`lastSeen`/`held` the constant is unreachable dead code, and only `mentions` ever meets it.
7. `info` — `sync.Mutex` is the right primitive and contention is effectively zero: one producer at ~500 uncontended acquisitions/s (~20 ns each) against one flusher at 4/s. Do not reach for sharding or atomics here.
