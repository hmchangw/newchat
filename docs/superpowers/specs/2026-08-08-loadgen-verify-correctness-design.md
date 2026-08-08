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
| `--users` | `preset.Users` | Total activation count. Background load; probe recipients are chosen separately (§6) |
| `--probe-rooms` | `50` | Number of probe rooms; their members are forced into the direct pool (§6) |
| `--warmup` | `30s` | Pre-measurement settle |
| `--steady` | `120s` | Probe-generating window |
| `--drain` | `30s` | Post-quiesce wait for in-flight probes |
| `--probe-rate` | `0.01` | Fraction of sends into probe rooms that are tracked (§5) |
| `--min-probes` | `50` | Below this, verdict is INCONCLUSIVE |
| `--large-room-threshold` | `500` | Must match the gatekeeper's setting; preflight enforces (§6.1) |
| `--lane` | `both` | `global` \| `local` \| `both`. Halves subscription count when set explicitly (§6.3) |
| `--direct-only` | `false` | Disable multiplex; every user gets a dedicated conn. Preflight errors if `--users` exceeds the direct budget (§6.3) |
| `--seed` | `42` | Drives fixtures, probe-room choice, and probe selection; same seed ⇒ same probe set |
| `--json` | `""` | Full violation detail output path |

## 5. Probe Sampling

Full per-recipient accounting is O(messages × room members). A single send
into a 2000-member room creates 2000 expectation tuples, and the workload
runs hundreds of sends per second — unbounded in practice.

Instead, a deterministic fraction of sends **into probe rooms** (§6) are
tagged as **probes** and get full per-recipient accounting. Everything
else — including all sends into non-probe rooms — is ordinary load.

- Probe selection happens at publish time in `sendMessage`, derived from
  the run seed and a per-user counter. Never `rand` on the hot path, never
  wall-clock. Same seed ⇒ same probe set.
- `--probe-rate` (default `0.01`) tunes the fraction.
- Tracked state stays bounded and predictable: at ~200 sends/sec and a 1%
  rate, ~2 probes/sec times average room size.

The system under test still runs at full concurrency; only the *accounting*
is sampled.

## 6. Probe Rooms and the Expected-Recipient Set

Probe rooms are chosen **first**, and the direct pool is built to cover
them — rather than activating N users and discovering which rooms happen to
be fully covered.

### 6.0 Probe-room-first activation

1. Select `--probe-rooms` rooms from the eligible bands (small and medium;
   see §6.1 for why large rooms are excluded), deterministically from
   `--seed`.
2. Force the **union of their members** into the direct pool. This set is
   complete by construction.
3. Activate remaining users up to `--users` normally — multiplex is fine,
   they are background load and never probe recipients.
4. Probes only ever target the selected probe rooms.

The expected set for a probe is then simply **all members of its room**,
with the sender included (§7.2). No intersection with activation state is
needed, because step 2 guarantees every member is live on a dedicated
connection.

`Fixtures` is a pure function of `(preset, seed, siteID)` (`preset.go:127`),
so membership is derivable in-process, deterministically, with no Mongo
query at verdict time.

**Why not the obvious alternative.** Activating N users and treating
fully-covered rooms as eligible collapses coverage at any `N <
preset.Users`. Activation is a strict prefix (`env.users[from:to]`,
`daily.go:411`) and direct-pool assignment is also prefix-ordered, while
room membership scatters across the whole user index. The chance a
size-`k` room is fully covered is ≈ `(N/preset.Users)^k`:

| Band | k | N=2 000 | N=5 000 | N=10 000 |
|---|---|---|---|---|
| DM | 2 | 4% | 25% | 100% |
| small | ~10 | ~0% | 0.1% | 100% |
| medium | ~100 | ~0 | ~0 | 100% |

Medium rooms are never eligible below full activation. That forces
`N = preset.Users` — 10 000 dedicated connections — which is the load the
multiplex pool exists to avoid, and pushes the run into the GC-pause
INCONCLUSIVE branch. Probe-room-first bounds the direct pool by probe-room
membership instead: ~30 small (~20 members) plus ~20 medium (~100) is
~2 600 connections worst case, less with overlap.

**Why not expect only the direct-pool subset of each room.** That would
restore coverage cheaply, but tracked recipients would be systematically
the low-index users, since activation and direct assignment are both index
prefixes. A truncation-style fan-out bug — "only the first K members
receive it" — would leave exactly the untracked tail broken. That is a
correlated blind spot on the most plausible fan-out bug class, so it is
rejected.

Coverage (probe rooms, their total membership, and direct-pool size) is
printed in the report so thin coverage is visible, never silent.

### 6.1 Large-room exclusion (mandatory)

The gatekeeper rejects non-thread sends from member-role users into rooms
above `LARGE_ROOM_THRESHOLD` (default 500). Daily fixtures place ~3 such
rooms on every user, so ~5% of sends are rejected **by design** — a
documented known limitation of the daily scenario.

That rejection happens *after* a successful publish, so an affected probe
is indistinguishable from a lost message — we would manufacture guaranteed
failures.

Two mitigations, both required:

1. Probe-room selection (§6.0 step 1) excludes rooms at or above the
   configured threshold.
2. Preflight verifies the configured threshold matches what the gatekeeper
   is actually running, and refuses to start on mismatch.

Failing fast in seconds beats a ten-minute run reporting a phantom bug.

### 6.2 Multiplex users are never probe recipients

A user in the multiplex pool can lose a broadcast to a full per-user inbox
channel (128-deep, non-blocking send — `daily_pool.go:173`, `:249`), already
counted as `Collector.RecordMultiplexDrop`. Worse, `multiplexPool.route`
calls `RecordBroadcast` **once per routed message, before the inbox sends
and regardless of whether every inbox dropped it** (`daily_pool.go:245`).
The multiplex path therefore conflates "arrived at the pool" with
"delivered to users" — correct for a latency histogram, fatal for a
completeness assertion.

Attribution also moves in-process there: with refcounted shared
subscriptions the broker delivers once and loadgen's own `dispatch` map
decides who received it, so a completeness check would be partly testing
loadgen's bookkeeping rather than the system under test.

§6.0 step 2 removes both problems by construction. Any multiplex drop
recorded during a run still forces INCONCLUSIVE (§9) as a backstop.

### 6.3 Resource budget

Each direct user opens one connection and `2 × rooms + 1` subscriptions —
the ×2 because every room is subscribed on both lanes (`daily_pool.go:58`).

| Preset | Rooms/user | Subs/user | Subs @ 2 000 | @ 10 000 |
|---|---|---|---|---|
| daily-light | ~32 | 65 | 130 k | 650 k |
| daily-heavy | ~56 | 113 | 226 k | 1.13 M |
| daily-power | ~83 | 167 | 334 k | 1.67 M |

**The estimates below are unmeasured** — derived from the code and stock
nats.go behaviour, not from a benchmark. Establishing real numbers is a
task in the implementation plan.

Limits in the order they bite:

1. **File descriptors** — one per conn. A default `ulimit -n` of 1024 caps
   you near 1 000 users; raise to 65536.
2. **Client memory** — nats.go allocates ~32 KB read + ~32 KB write buffers
   per `Conn` plus 2–3 goroutines. Call it ~100 KB/user, so ~1 GB at
   10 000 users before any message processing.
3. **NATS server sublist** — 1.13 M subscriptions at daily-heavy is
   server-side memory plus matching work on every publish.
4. **Loadgen GC pressure** — trips the existing INCONCLUSIVE branch.

Rough tiers, to be confirmed rather than trusted: ~2 000 comfortable on a
dev box; ~5 000 needs raised limits and a few GB; ~10 000 needs a dedicated
load box. The `--max-direct-users=20000` default is a safety ceiling, not a
demonstrated capability.

**`--lane`** halves every subscription count above when set to `global` or
`local`. The dual-lane default exists to stay `ROOM_SUBJECT_MODE`-agnostic;
an operator who knows their stack's mode should set it explicitly. Doing so
also removes the phantom-duplicate concern in §7.1.

**`--direct-only`** disables multiplex so every user gets a dedicated conn,
trading background-load scale for uniform fidelity. Note that with
multiplex disabled, `daily` silently skips any user past
`--max-direct-users`; `verify` instead **fails preflight**, because a
silently-absent recipient corrupts a completeness verdict.

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
duplicate is a violation only **within** a single lane. Setting `--lane`
explicitly (§6.3) removes the ambiguity entirely and halves subscription
cost.

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
probe rooms: 50 (32 small, 18 medium) / 2417 members / direct pool 2417
background:  7583 users on multiplex
probes:      412 tracked
delivery:    410 complete / 2 partial / 0 total-loss
duplicates:  0
persistence: 412 confirmed / 0 missing / 0 mismatch

VIOLATIONS (showing 2 of 2)
  missing_recipient  msg=7Hq3... room=r-000042  missing: u-000317, u-000904
  missing_recipient  msg=9Kp1... room=r-000042  missing: u-000317

VERDICT: FAIL
```

The `direct pool` figure equalling probe-room membership is the invariant
from §6.0 step 2 — if they ever diverge, the run is misconfigured and
preflight should have caught it.

Violations print `msgID`, `roomID`, and missing recipient IDs — enough to
grep service logs directly. Console caps at ~10; `--json` carries full
detail. Never prints message content.

## 11. Implementation Layout

Following the existing flat per-scenario convention in `tools/loadgen/`:

| File | Contents |
|---|---|
| `verify.go` | `runVerify`, config parsing, lifecycle (warmup/steady/quiesce/drain) |
| `verify_rooms.go` | Probe-room selection and direct-pool member union (§6.0) |
| `verify_probe.go` | `ProbeTracker` — expected sets, delivery recording, dedupe, finalize |
| `verify_readback.go` | `history-service` readback with bounded retry |
| `verify_verdict.go` | PASS/FAIL/INCONCLUSIVE evaluation |
| `verify_report.go` | Console + JSON rendering |
| `verify_*_test.go` | Unit tests per the above |

Modified: `main.go` (dispatch a `verify` case), `daily_pool.go` (thread
`u.ID` and lane into broadcast attribution; honour `--lane`), `daily.go`
(seed the direct pool with a designated member set before the prefix walk
in `activateUsers`), `deploy/Makefile` (`run-verify`).

The `activateUsers` change is the only edit touching a path `daily` also
uses. It is additive — an empty designated set reproduces today's
behaviour exactly — and covered by a regression test asserting `daily`'s
activation order is unchanged when no set is supplied.

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
| Large-room gatekeeper rejects ⇒ phantom bugs | Excluded from probe-room selection + preflight threshold check (§6.1) |
| Multiplex drops ⇒ phantom bugs | Probe-room members forced into direct pool (§6.0); any drop ⇒ INCONCLUSIVE |
| Dual-lane subscribe ⇒ phantom duplicates | Dedupe per `(user, msg, lane)`; `--lane` to disambiguate (§7.1) |
| Coverage collapse at partial activation | Probe-room-first activation decouples coverage from N (§6.0) |
| Truncation-style fan-out bug hidden by index-correlated sampling | Full room membership tracked, never a subset (§6.0) |
| Direct-pool resource limits unknown | Budget estimates flagged unmeasured; benchmarking is a plan task (§6.3) |

## 15. Success Criteria

1. `loadgen verify` runs to completion against the docker-compose stack and
   reports PASS on a healthy system.
2. Withholding a delivery in a unit test produces FAIL with the correct
   violation class and actionable IDs.
3. A harness-side drop produces INCONCLUSIVE, never FAIL.
4. Two runs with the same seed select the same probe rooms and probe set.
5. Every probe-room member is in the direct pool — asserted at preflight,
   not merely assumed.
6. Unit coverage ≥ 80% overall, ≥ 90% on `ProbeTracker` and the verdict.
7. A measured direct-pool resource profile replaces the estimates in §6.3.
8. No behaviour change to `daily`, `max-rps`, `soak`, or `members` — the
   `Collector` is untouched and `activateUsers` is additive-only.

## 16. References

- `docs/superpowers/specs/2026-05-27-daily-im-load-scenario-design.md` —
  the capacity scenario this reuses
- `tools/loadgen/README.md` §Daily-IM scenario — operator guide, known
  limitations
- `tools/loadgen/collector.go`, `daily_pool.go`, `daily_verdict.go` —
  the seams this builds on
