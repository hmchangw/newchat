# Loadgen Verify — Correctness Automation Test

**Status:** Draft
**Owner:** hmchang
**Date:** 2026-08-08

## 1. Goal

Add a `loadgen verify` subcommand that asserts **messaging correctness
under concurrency**, as opposed to the capacity and latency questions the
existing `loadgen daily` scenario answers.

The output answers: *"Under a realistic concurrent daily-IM workload, did
every tracked message reach exactly the right recipients exactly once, and
did it persist?"*

`daily` answers *how much* the system can take. `verify` answers *whether
what it took came out correct.*

## 2. Scope

**In scope (single-site only):**

- Delivery completeness — every tracked message reaches every expected
  room member
- Exactly-once delivery — no recipient receives the same `messageID` twice
- Persistence readback — each tracked message is retrievable through
  `history-service` with the correct `roomID`, `senderID`, and
  `threadParentMessageID`
- Probe sampling to bound accounting cost while keeping the system under
  full realistic load
- PASS / FAIL / INCONCLUSIVE verdict with actionable violation detail

**Out of scope:**

- **Per-room ordering.** Deliberately cut: the most expensive invariant to
  track and the most flake-prone. Revisit only if reordering bugs are
  observed in the wild.
- **Content-byte integrity.** Metadata assertions only. Comparing bodies
  would drag per-room key handling into the test under the default
  `ENCRYPTION_ENABLED=true` for little added signal.
- **CI gating.** Operator-invoked, on demand or pre-release. This preserves
  the existing repo-wide stance that loadgen is not a CI gate.
- **Cross-site federation (OUTBOX/INBOX) correctness.** Single-site only.
- **Capacity or SLO measurement.** `verify` never reports a capacity
  number; latency thresholds do not produce a FAIL.

## 3. Correctness Definition (what "incorrect" means)

A run FAILs if any probe message shows any of:

| Violation | Meaning | Likely origin |
|---|---|---|
| `missing_recipient` | An expected recipient never received the probe | broadcast fan-out |
| `total_loss` | The probe reached **zero** expected recipients | gatekeeper reject, or canonical-publish failure |
| `duplicate_delivery` | One recipient received the same `messageID` twice on the same lane | JetStream redelivery / worker idempotency |
| `persistence_miss` | Probe not retrievable via `history-service` after bounded retries | `message-worker` write loss |
| `persistence_mismatch` | Retrieved, but `roomID` / `senderID` / `threadParentMessageID` differs | wrong-partition or field-mapping write bug |

`total_loss` is counted separately from `missing_recipient` on purpose.
Zero recipients points upstream of broadcast; partial delivery points at
fan-out. Same symptom, different investigation.

## 4. Run Lifecycle

```
preflight → BuildFixtures → activate users → warmup → steady
          → quiesce → drain → readback → finalize → verdict
```

Two phases do not exist in `daily` and are the reason `verify` needs its
own runner rather than a flag on the ramp:

- **quiesce** — cancel all action emitters, stop publishing entirely, keep
  every subscription live. Nothing new enters the system.
- **drain** — wait `--drain` (default `30s`) with receivers still
  listening so in-flight probes land, *then* finalize.

Without a drain window, every probe in flight at the cutoff reads as a
lost message. `daily` cannot host this: `Collector.Reset()` at each
warmup→hold boundary deliberately discards mid-flight state
(`collector.go:293`), which is correct for a per-step latency ramp and
fatal for a delivery-completeness assertion.

The steady window drives the **unmodified** Markov/Poisson emitter, so the
system sees the same realistic mixed workload `daily` produces. Probes ride
the ordinary `sendMessage` path — they are normal messages that happen to
be tracked, never a special class the backend could treat differently.

**Exit codes:** `0` PASS, `1` FAIL, `2` INCONCLUSIVE. Distinct so the
command can be scripted without parsing stdout.

### 4.1 CLI flags

| Flag | Default | Notes |
|---|---|---|
| `--preset` | `daily-heavy` | Same presets as `daily` |
| `--users` | `preset.Users` | Activation count. Full activation maximises room eligibility (§6) |
| `--warmup` | `30s` | Pre-measurement settle |
| `--steady` | `120s` | Probe-generating window |
| `--drain` | `30s` | Post-quiesce wait for in-flight probes |
| `--probe-rate` | `0.01` | Fraction of sends tracked (§5) |
| `--min-probes` | `50` | Below this, verdict is INCONCLUSIVE |
| `--large-room-threshold` | `500` | Must match the gatekeeper's setting; preflight enforces (§6.1) |
| `--seed` | `42` | Drives fixtures and probe selection; same seed ⇒ same probe set |
| `--json` | `""` | Full violation detail output path |

## 5. Probe Sampling

Full per-recipient accounting is O(messages × room members). A single send
into a 2000-member room creates 2000 expectation tuples, and the workload
runs hundreds of sends per second — unbounded in practice.

Instead, a deterministic fraction of sends are tagged as **probes** and get
full per-recipient accounting. Everything else is ordinary load.

- Probe selection happens at publish time in `sendMessage`, derived from
  the run seed and a per-user counter. Never `rand` on the hot path, never
  wall-clock. Same seed ⇒ same probe set.
- `--probe-rate` (default `0.01`) tunes the fraction.
- Tracked state stays bounded and predictable: at ~200 sends/sec and a 1%
  rate, ~2 probes/sec times average room size.

The system under test still runs at full concurrency; only the *accounting*
is sampled.

## 6. Expected-Recipient Set

For each probe, the expected set is a three-way intersection:

```
room members (from Fixtures.Subscriptions)
  ∩ activated users
  ∩ direct-pool members
```

`Fixtures` is a pure function of `(preset, seed, siteID)` (`preset.go:127`),
so membership is derivable in-process, deterministically, with no Mongo
query at verdict time.

The second and third intersections are what make the verdict honest:

- A user who never activated has no NATS subscription and provably cannot
  receive. Counting them would manufacture failures.
- A user in the **multiplex** pool can lose a broadcast to a full per-user
  inbox channel — a harness artifact, already counted as
  `Collector.RecordMultiplexDrop`. Restricting probes to direct-pool
  members (one `nats.Conn` per user, no shared channel) removes that path
  entirely.

**Room eligibility.** A room is probe-eligible only if all its members are
in the direct pool. Because fixture membership spans all `preset.Users`,
running at `N < preset.Users` makes many rooms ineligible. Recommended
operation is full activation (`N = preset.Users`). Either way the report
prints eligible-room coverage so thin coverage is visible, never silent.

### 6.1 Large-room exclusion (mandatory)

The gatekeeper rejects non-thread sends from member-role users into rooms
above `LARGE_ROOM_THRESHOLD` (default 500). Daily fixtures place ~3 such
rooms on every user, so ~5% of sends are rejected **by design** — a
documented known limitation of the daily scenario.

That rejection happens *after* a successful publish, so an affected probe
is indistinguishable from a lost message. At full activation those rooms
become probe-eligible, so we would manufacture guaranteed failures.

Two mitigations, both required:

1. Probe eligibility excludes rooms at or above the configured threshold.
2. Preflight verifies the configured threshold matches what the gatekeeper
   is actually running, and refuses to start on mismatch.

Failing fast in seconds beats a ten-minute run reporting a phantom bug.

## 7. Receiver Attribution

Today `directPool.onBroadcast` (`daily_pool.go:98`) discards which user
received an event — it calls `RecordBroadcast(evt.LastMsgID)` and
`RecordBroadcast` deletes the map entry on **first** delivery
(`collector.go:159`). So "1 of 500 members got it" is today
indistinguishable from "500 of 500 got it", and duplicates are invisible.

`directPool.Add` already holds `u` in closure scope (`daily_pool.go:59`),
so attribution is a small edit: pass `u.ID` through so deliveries record as
`(userID, messageID, lane)`.

**`Collector` is not modified.** It is a latency structure and its
first-delivery-wins delete is exactly wrong for fan-out accounting. Leaving
it untouched keeps the `daily`, `max-rps`, `soak`, and `members` scenarios
unaffected.

### 7.1 Dual-lane de-duplication

Every room is subscribed on **both** the global and local lanes
(`for _, global := range []bool{true, false}`, `daily_pool.go:58`) so runs
stay `ROOM_SUBJECT_MODE`-agnostic. If both lanes are ever simultaneously
live, one broadcast arrives twice and a naive exactly-once check fires
falsely.

Deliveries therefore dedupe per `(userID, messageID, lane)`, and a
duplicate is a violation only **within** a single lane.

### 7.2 Sender self-delivery

`broadcast-worker` **does** echo a message back to its sender. The sender is
therefore always a member of its own probe's expected set, and a missing
self-delivery is a `missing_recipient` violation like any other.

No calibration step is needed. `ProbeTracker` tests assert the sender is
present in the expected set explicitly, so a future change to echo
behaviour surfaces as a test failure rather than silent under-counting.

## 8. Persistence Readback

Runs after drain. O(probes), not O(probes × members) — cheap regardless of
room size.

**Read path: `history-service` RPC, not direct CQL.** Readback reuses
`subject.MsgGet` (already used by the `scrollHistory` action,
`daily_actions.go:110`) and inspects the response rather than discarding
it. Rationale:

- Exercises the real client read path.
- Avoids coupling the test to the Cassandra bucketing scheme. Direct CQL
  would need `pkg/msgbucket` math, and would silently target the wrong
  partition if `MESSAGE_BUCKET_HOURS` ever differed between the test and
  the services — a documented silent-data-loss trap, and not one worth
  building into a correctness test.

**Asserted per probe:** present in the room's history, with matching
`roomID`, `senderID`, `threadParentMessageID`.

**Storage lag is not storage loss.** `message-worker` consumes
MESSAGES-CANONICAL asynchronously, so a probe missing on first read may
simply be late. Missing probes retry with bounded backoff (a few attempts
over ~15s) before being declared lost. Queries batch per room — one
`MsgGet` covering a room's probe window, not one call per probe.

A readback query that errors or times out yields INCONCLUSIVE for the
affected probes, never FAIL: an unreachable `history-service` tells us
nothing about whether the write happened.

## 9. Verdict

INCONCLUSIVE overrides, matching the precedence `daily` already uses.

**FAIL** if any probe shows any violation from §3 surviving retries.

**INCONCLUSIVE** if any of:

- Any multiplex drop was recorded during the run
- A probe's expected set shrank between publish and finalize — i.e. a
  tracked recipient's connection dropped mid-run, so its non-delivery
  cannot be attributed to the system under test
- Readback errored or timed out
- Fewer than `--min-probes` (default `50`) eligible probes were sampled —
  nothing meaningful was measured, and a PASS would be a silent lie
- `ctx` cancelled mid-run
- Loadgen GC pause p99 above the existing self-metric threshold — the load
  box was saturated, so the measurement is not trustworthy

**PASS** otherwise.

Latency never produces a FAIL. `verify` is not an SLO tool.

## 10. Output

Console summary, styled after `daily_report.go`:

```
probes:      412 sent / 409 eligible / 3 skipped (large-room)
delivery:    407 complete / 2 partial / 0 total-loss
duplicates:  0
persistence: 409 confirmed / 0 missing / 0 mismatch
coverage:    88 of 100 rooms eligible

VIOLATIONS (showing 2 of 2)
  missing_recipient  msg=7Hq3... room=r-000042  missing: u-000317, u-000904
  missing_recipient  msg=9Kp1... room=r-000042  missing: u-000317

VERDICT: FAIL
```

Violations print `msgID`, `roomID`, and missing recipient IDs — enough to
grep service logs directly. Console caps at ~10; `--json` carries full
detail. Never prints message content.

## 11. Implementation Layout

Following the existing flat per-scenario convention in `tools/loadgen/`:

| File | Contents |
|---|---|
| `verify.go` | `runVerify`, config parsing, lifecycle (warmup/steady/quiesce/drain) |
| `verify_probe.go` | `ProbeTracker` — expected sets, delivery recording, dedupe, finalize |
| `verify_readback.go` | `history-service` readback with bounded retry |
| `verify_verdict.go` | PASS/FAIL/INCONCLUSIVE evaluation |
| `verify_report.go` | Console + JSON rendering |
| `verify_*_test.go` | Unit tests per the above |

Modified: `main.go` (dispatch a `verify` case), `daily_pool.go` (thread
`u.ID` into broadcast attribution), `deploy/Makefile` (`run-verify`).

No new dependencies — `nats.go`, `testify`, and `go.uber.org/mock` are
already in `go.mod`.

## 12. Testing

TDD throughout per CLAUDE.md: tests first, confirmed red, then
implementation. Coverage floor 80%, targeting 90%+ on tracker and verdict.

Unit-testable with no infrastructure — which is most of it:

- `ProbeTracker` — table-driven: expected-set intersection, delivery
  recording, per-lane dedupe, duplicate detection, partial-vs-total-loss
  classification, finalize
- Probe selection determinism — same seed ⇒ same probe set
- `verify_verdict` — table-driven over every trigger and the override
  precedence
- Readback — `requestFn` is already an injectable func type
  (`daily_actions.go:26`), so retry/backoff, late-arrival-then-found, and
  error→INCONCLUSIVE all test against a stub

The **negative** cases carry the weight: a test that withholds a delivery
and asserts FAIL is what proves the harness detects the bug it exists to
catch.

**End-to-end.** `daily_integration_test.go` is skipped with a clear
rationale — a `testutil` NATS container alone cannot run the pipeline, so
every request/reply action times out. That applies identically here. End-to-end
validation therefore lives in the docker-compose harness as
`make -C tools/loadgen/deploy run-verify`. No vacuously-skipped integration
test will be added.

## 13. Error Handling

- Wrap with `fmt.Errorf("…: %w", err)`; never bare `err`
- `log/slog` structured fields only
- Violation output logs `msgID`, `roomID`, user IDs — **never** message
  content, which keeps it clear of the no-logging-bodies rule and sidesteps
  encryption entirely
- `ctx` cancellation drains what it can and reports INCONCLUSIVE rather
  than passing a partial run
- Preflight failures exit non-zero immediately, matching the existing
  daily preflight style

## 14. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Drain too short ⇒ false failures | Generous 30s default; on-demand runs have no CI clock to fight |
| Large-room gatekeeper rejects ⇒ phantom bugs | Excluded from eligibility + preflight threshold check (§6.1) |
| Multiplex drops ⇒ phantom bugs | Probes restricted to direct pool; any drop ⇒ INCONCLUSIVE |
| Dual-lane subscribe ⇒ phantom duplicates | Dedupe per `(user, msg, lane)` (§7.1) |
| Thin room coverage at low N | Coverage printed in report; full activation recommended |
| Sender self-delivery assumption wrong | Calibrated empirically before encoding (§7.2) |

## 15. Success Criteria

1. `loadgen verify` runs to completion against the docker-compose stack and
   reports PASS on a healthy system.
2. Withholding a delivery in a unit test produces FAIL with the correct
   violation class and actionable IDs.
3. A harness-side drop produces INCONCLUSIVE, never FAIL.
4. Two runs with the same seed select the same probe set.
5. Unit coverage ≥ 80% overall, ≥ 90% on `ProbeTracker` and the verdict.
6. No behaviour change to `daily`, `max-rps`, `soak`, or `members` — the
   `Collector` is untouched.

## 16. References

- `docs/superpowers/specs/2026-05-27-daily-im-load-scenario-design.md` —
  the capacity scenario this reuses
- `tools/loadgen/README.md` §Daily-IM scenario — operator guide, known
  limitations
- `tools/loadgen/collector.go`, `daily_pool.go`, `daily_verdict.go` —
  the seams this builds on
