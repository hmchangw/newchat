# Loadgen Verify — Correctness Automation Test

**Status:** Draft
**Owner:** hmchang
**Date:** 2026-08-08

## 1. Goal

Add a `loadgen verify` subcommand that asserts **messaging correctness
under concurrency**, as opposed to the capacity and latency questions the
existing `loadgen daily` scenario answers.

The output answers: *"Under a realistic concurrent daily-IM workload, did
every tracked message reach exactly the right recipients exactly once, did
it persist, and did membership changes actually take effect?"*

`daily` answers *how much* the system can take. `verify` answers *whether
what it took came out correct.*

## 2. Scope

**In scope (single-site only):**

- Delivery completeness — every tracked message reaches every expected
  room member
- No leakage — and *only* those members, on the per-user lane (§7.3)
- Exactly-once delivery — no recipient receives the same `messageID` twice
- Persistence readback — each tracked message is retrievable through
  `history-service` with the correct `roomID`, `senderID`, and
  `threadParentMessageID`
- Membership changes — an add or remove is applied to subscription state
  and takes effect on the authorization path (§9)
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
| `persistence_mismatch` | Retrieved, but `senderID` / `threadParentMessageID` differs | field-mapping write bug (see §8 — wrong-room is not detectable here) |
| `unexpected_recipient` | A tracked user received a probe for a room they are not a member of, on the per-user lane | DM/user-lane mis-addressing |
| `membership_not_applied` | After add/remove + settle, `subscription.list` does not reflect the change | room-service / room-worker / Mongo write |
| `membership_add_ineffective` | An added member's send into the room is still rejected after settle | membership visible to `subscription.list` but not to the gatekeeper |
| `membership_remove_ineffective` | A removed member's send into the room is still accepted after settle | stale membership on the authorization path |

`total_loss` is counted separately from `missing_recipient` on purpose.
Zero recipients points upstream of broadcast; partial delivery points at
fan-out. Same symptom, different investigation.

`unexpected_recipient` is the most severe class — a message reaching a
non-member is a privacy incident, not a delivery defect. See §7.3 for why
it is meaningful only on the per-user lane.

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
| `--reserve-users` | `200` | Direct-connected floaters, initially in no probe room, used as membership-change targets (§6.0 step 3) |
| `--member-churn` | `0.2` | Membership changes per probe room per minute. `0` disables §9 entirely |
| `--settle` | `5s` | Post-change quiet window per room; probes suspended, then E recomputed (§9.2) |
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

1. Select `--probe-rooms` rooms from the eligible bands — **DM, small, and
   medium** — deterministically from `--seed`. Large rooms are excluded
   (§6.1).

   DM rooms are mandatory in the mix, not optional: they are the only band
   that broadcasts on the per-user lane, and §7.3 establishes that lane as
   the only place the leakage check (`O ⊆ E`) is meaningful. A probe-room
   set without DMs would leave `unexpected_recipient` permanently
   unexercised. Selection therefore takes a fixed proportion from each
   band (default ⅓ DM, ⅓ small, ⅓ medium) rather than sampling the bands
   uniformly at random, so no seed can produce a DM-free set.
2. Force the **union of their members** into the direct pool. This set is
   complete by construction.
3. Add `--reserve-users` **floaters** to the direct pool — users in no
   probe room initially, held as targets for membership changes (§9).
   Without this, a user added mid-run would have no dedicated connection
   and their deliveries would be unobservable.
4. Activate remaining users up to `--users` normally — multiplex is fine,
   they are background load and never probe recipients.
5. Probes only ever target the selected probe rooms.

The expected set for a probe is **all members of its room at the probe's
membership epoch** (§9.2), with the sender included (§7.2). No intersection
with activation state is needed, because steps 2 and 3 guarantee every
current and future member is live on a dedicated connection.

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
2. `--large-room-threshold` must be set by the operator to match the
   gatekeeper's actual `LARGE_ROOM_THRESHOLD`. **This is trusted, not
   verified** — `verify` never queries the gatekeeper to check the flag
   against its real running config. Get it wrong and preflight stays
   silent; the run instead produces phantom `total_loss` /
   `missing_recipient` violations on probes in rooms that sit in the gap
   between the configured and actual threshold. See
   `tools/loadgen/README.md` § Verify scenario → Prerequisites.

Failing fast in seconds beats a ten-minute run reporting a phantom bug —
mitigation 1 does that; mitigation 2 does not, and is only as good as the
operator's manual bookkeeping.

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

§6.0 step 2 removes both problems by construction.

**Correction (post-review).** An earlier version of this section claimed a
multiplex drop is indistinguishable from a delivery bug and therefore had
to force INCONCLUSIVE (§10) as a backstop. That was wrong, and it made PASS
unreachable in the default configuration: the multiplex pool's per-user
inbox channels are write-only by design — nothing in the tree ever receives
from them (`daily_pool.go`'s send loop is their only reference), so filling
and dropping is their normal steady state, and `daily` has always done it
without caring. Under `daily-heavy` roughly 7000 users sit on multiplex, so
drops are certain within seconds and every run would report INCONCLUSIVE
unless `--direct-only` was passed.

The loss is in fact perfectly distinguishable, because it lands on a
population probes never touch. `preflightVerify` refuses to start unless
every probe-room member is in the **direct** pool, and the `MissingUsers`
assertion checks actual membership rather than a count — so a multiplex
user is never an expected recipient of a probe, and a multiplex drop cannot
affect probe accounting at all. The count is reported as load context in
the run summary; it does not gate the verdict.

### 6.3 Resource budget

Each direct user opens one connection and `2 × rooms + 1` subscriptions —
the ×2 because every room is subscribed on both lanes (`daily_pool.go:58`).

| Preset | Rooms/user | Subs/user | Subs @ 2 000 | @ 10 000 |
|---|---|---|---|---|
| daily-light | ~32 | 65 | 130 k | 650 k |
| daily-heavy | ~56 | 113 | 226 k | 1.13 M |
| daily-power | ~83 | 167 | 334 k | 1.67 M |

**The estimates below are unmeasured** — derived from the code and stock
nats.go behaviour, not from a benchmark. Establishing real numbers was a
task in the implementation plan (Task 11); it did not happen. Docker was
unavailable in the environment that ran Task 11, so the compose stack could
never be brought up to measure against — see
`.superpowers/sdd/2026-08-08-loadgen-verify-correctness/task-11-report.md`.
These estimates **remain unmeasured** and are the next thing an operator
with Docker access should establish before trusting them at scale.

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

The table above and the halving both describe the **direct pool only**.
`--lane` does not touch the multiplex background pool, which always
subscribes both lanes regardless of the flag (same code path as `daily`,
deliberately not forked — see the design note at `daily_pool.go`). So
`--lane` halves the direct-pool subscription budget tabulated above, not
the run's total subscription count; the multiplex pool's contribution is
unchanged either way.

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

### 7.3 Lane asymmetry — where non-delivery is checkable

Check 1 asserts `E ⊆ O`. The leakage check asserts `O ⊆ E`. The second is
only meaningful on one of the two lanes, and conflating them would produce
guaranteed false failures.

**Room lane (channel rooms) — topic broadcast.** `broadcast-worker`
publishes **once** to `subject.RoomEvent(roomID)`; NATS fans it out to
whoever subscribed. Membership is not consulted at broadcast time — it is
enforced when a client subscribes. Two consequences:

- A non-member who stays subscribed still receives, and loadgen dials with
  `backend.creds` (full `chat.>` permissions), so it can *always* stay
  subscribed. A room-lane leakage check would fail on every run and would
  be testing NATS ACLs, not the chat system.
- Symmetrically, "an added member now receives" is near-vacuous on this
  lane: *loadgen* issues the subscribe, so delivery follows from NATS, not
  from anything the system did.

**Per-user lane — addressed delivery.** DM and BotDM broadcasts go to
`subject.UserRoomEvent(account)` (`daily_pool.go:70`), one subject per
recipient. Here the system chooses *who* to address, so mis-addressing is a
real, system-controlled defect and `unexpected_recipient` is meaningful.

**Therefore:** `O ⊆ E` is asserted on the per-user lane only. On the room
lane, membership correctness is verified through the authorization path
instead (§9.1), which is what the system actually controls.

This is why DM rooms are a mandatory share of the probe-room mix (§6.0
step 1) — without them the leakage check has no lane to run on.

## 8. Persistence Readback

Runs after drain. O(probes), not O(probes × members) — cheap regardless of
room size.

**Read path: `history-service` RPC, not direct CQL.** Readback uses
`subject.MsgGetIDs` → `GetMessagesByIDs` (`history-service/internal/service/messages.go:445`),
the batch-by-ID RPC. Rationale:

- Exercises the real client read path.
- Avoids coupling the test to the Cassandra bucketing scheme. Direct CQL
  would need `pkg/msgbucket` math, and would silently target the wrong
  partition if `MESSAGE_BUCKET_HOURS` ever differed between the test and
  the services — a documented silent-data-loss trap, and not one worth
  building into a correctness test.

**Wire contract** (verified against the handler, not assumed):

| | |
|---|---|
| Subject | `subject.MsgGetIDs(account, roomID, siteID)` |
| Request | `{"messageIds": [...]}`, **max 100 per request** (`maxGetByIDsBatchSize`); empty list is a `BadRequest` |
| Response | `{"messages": [...]}` |
| Fields read | `messageId`, `threadParentId`, and `sender.id` (a nested `Participant`) |

A room with more than 100 probes is chunked across multiple requests.

**Asserted per probe:** present in the room's history, with matching
`senderID` and `threadParentMessageID`.

**Two server-side filters drop rows silently — both degrade a mismatch
into a miss.** `GetMessagesByIDs` fetches by ID and then filters the
result set twice before replying (`messages.go:467-477`):

1. **Room filter** — rows whose `RoomID` differs from the subject's room
   are dropped. A message persisted under the wrong room is therefore
   absent rather than present-and-wrong.
2. **`accessSince` filter** — rows with `CreatedAt` before the caller's
   access window are dropped. A probe read back under an account whose
   `historySharedSince` has moved forward (a rejoin after removal — which
   membership churn does produce, §9) is likewise absent.

Both surface as `persistence_miss`, never `persistence_mismatch`. The
`roomID` comparison is consequently omitted from the readback rather than
carried as unreachable code. This is a real reduction in diagnostic
precision: the violation still fires, but names the wrong cause.

**Caller constraint:** readback MUST run as the probe's own sender
account. Querying as an unrelated account narrows or empties the access
window and manufactures phantom misses.

**An error reply is not an empty result.** NATS handlers in this repo
answer errors with an ordinary message body on the reply subject
(`pkg/errcode/errnats/reply.go:41-45`), which `requestFn` returns with a
nil Go error. Decoded naively into the response envelope, that yields zero
messages and reports every probe in the batch as lost. Readback therefore
runs `errcode.Parse` on each reply before decoding and returns the parsed
error, so it exits via the INCONCLUSIVE path. This matters because our own
workload provokes it: `GetMessagesByIDs` calls `getAccessSince` first, and
a sender removed by membership churn gets `Forbidden` for that room.

**Storage lag is not storage loss.** `message-worker` consumes
MESSAGES-CANONICAL asynchronously, so a probe missing on first read may
simply be late. Missing probes retry with bounded backoff (a few attempts
over ~15s) before being declared lost. Queries batch per room — one
`MsgGet` covering a room's probe window, not one call per probe.

A readback query that errors or times out yields INCONCLUSIVE for the
affected probes, never FAIL: an unreachable `history-service` tells us
nothing about whether the write happened.

## 9. Membership Correctness

Membership churn runs throughout the steady window at `--member-churn`
changes per probe room per minute, drawing targets from the reserve pool
(§6.0 step 3). Each change is an add or a remove via the existing
`memberAdd` action path and its removal counterpart.

Set `--member-churn=0` to disable this section entirely and run
delivery-only.

### 9.1 What is asserted

For a change **C** (add or remove of user **T** in probe room **R**),
evaluated after the settle window:

| Check | Assertion | Violation |
|---|---|---|
| Change applied | `subscription.list` for T reflects the change | `membership_not_applied` |
| Add effective | T's send into R is **accepted** by the gatekeeper | `membership_add_ineffective` |
| Remove effective | T's send into R is **rejected** by the gatekeeper | `membership_remove_ineffective` |
| Post-add delivery | probes after settle include T in E, and reach T | `missing_recipient` (§3) |

The add/remove-effective checks are phrased against **authorization**, not
fan-out, for the reason given in §7.3: on the room lane the system does not
decide who receives, it decides who may *send*. The gatekeeper's
`user X is not subscribed to room Y` rejection is the observable that
actually exercises room-service → room-worker → Mongo → gatekeeper end to
end.

A removed member's send being accepted is the severe direction here: it
means stale membership on the write path.

### 9.2 Membership epochs and the settle window

E is no longer static. A message published microseconds before an add
creates a genuine race — whether T receives it depends on whether
`room-worker` committed before `broadcast-worker` read the member list.
**Both outcomes are legitimate**, so the race must be avoided rather than
adjudicated.

Each probe room therefore carries a **membership epoch**:

1. A change bumps the room's epoch and opens a `--settle` quiet window.
2. During settle, **no probes are sent into that room**. Ordinary
   (untracked) load continues, so the system stays under churn.
3. After settle, E is recomputed and probing resumes at the new epoch.

Messages in flight across a change are simply never probed. Probes carry
their epoch, so a late delivery is matched against the E in force when it
was published, not the current one.

### 9.3 Two oracles, deliberately

Membership is tracked from **two independent sources**:

- **Loadgen's model** — fixtures plus the changes loadgen itself issued.
  This is the oracle for what membership *should* be.
- **`subscription.list`** — what the system *thinks* membership is.

Divergence between them is `membership_not_applied`. Delivery is then
judged against loadgen's model, never against `subscription.list`: if the
membership write were lost, both the system's state and its self-report
would agree, and judging against it would mask exactly the bug the check
exists to find.

### 9.4 Ordering caveat

Per-room message ordering is out of scope (§2), but membership changes have
an ordering hazard of their own — a remove overtaking its add leaves the
wrong final state. `verify` does not track membership event ordering; it
asserts the **final state after settle**, which catches the observable
consequence without the cost of full ordering tracking.

Cross-site membership ordering (the per-destination FIFO OUTBOX lanes) is
out of scope with the rest of federation.

## 10. Verdict

INCONCLUSIVE overrides, matching the precedence `daily` already uses.

**FAIL** if any probe shows a delivery, leakage, or persistence violation
from §3 surviving retries, **or** any membership change shows a
`membership_*` violation after its settle window (§9.1).

**INCONCLUSIVE** if any of:

- A tracked recipient's connection dropped mid-run, so its non-delivery
  cannot be attributed to the system under test. This is distinct from an
  epoch change: a membership change legitimately alters the expected set
  (§9.2) and is never INCONCLUSIVE on its own
- Readback errored or timed out
- A `subscription.list` query backing the membership oracle errored or
  timed out — same reasoning as readback: an unreachable service tells us
  nothing about whether the write happened
- Fewer than `--min-probes` (default `50`) probes were tracked — probes
  suppressed inside a settle window (§9.2) do not count toward this floor,
  so aggressive `--member-churn` with a long `--settle` can starve the run
  into INCONCLUSIVE rather than silently thinning coverage
- `ctx` cancelled mid-run
- Loadgen GC pause p99 above the existing self-metric threshold — the load
  box was saturated, so the measurement is not trustworthy

A multiplex drop is deliberately **not** on this list — see the correction
in §6.2. Probe recipients are guaranteed direct-pool by preflight, so a
drop on the background pool is irrelevant to probe accounting; the count is
reported as load context only.

**PASS** otherwise.

Latency never produces a FAIL. `verify` is not an SLO tool.

## 11. Output

Console summary, styled after `daily_report.go`:

```
probe rooms: 50 (32 small, 18 medium) / 2417 members / direct pool 2617
background:  7383 users on multiplex / 200 reserve floaters
probes:      412 tracked / 18 suppressed (settle window)
delivery:    410 complete / 2 partial / 0 total-loss
leakage:     0 unexpected recipients (user lane)
duplicates:  0
persistence: 412 confirmed / 0 missing / 0 mismatch
membership:  24 changes (14 add, 10 remove) / 24 applied / 24 effective

VIOLATIONS (showing 3 of 3)
  missing_recipient  msg=7Hq3... room=r-000042  missing: u-000317, u-000904
  missing_recipient  msg=9Kp1... room=r-000042  missing: u-000317
  membership_remove_ineffective  room=r-000108 target=u-009412 epoch=3
      send still accepted 5s after remove

VERDICT: FAIL
```

The `direct pool` figure equals probe-room membership plus reserve
floaters — the invariant from §6.0 steps 2–3. If it ever diverges, the run
is misconfigured and preflight should have caught it.

Violations print `msgID`, `roomID`, and missing recipient IDs — enough to
grep service logs directly. Console caps at ~10; `--json` carries full
detail. Never prints message content.

## 12. Implementation Layout

Following the existing flat per-scenario convention in `tools/loadgen/`:

| File | Contents |
|---|---|
| `verify.go` | `runVerify`, config parsing, lifecycle (warmup/steady/quiesce/drain) |
| `verify_rooms.go` | Probe-room selection and direct-pool member union (§6.0) |
| `verify_probe.go` | `ProbeTracker` — epoch-scoped expected sets, delivery recording, dedupe, leakage, finalize |
| `verify_membership.go` | Churn driver, epoch/settle bookkeeping, dual-oracle comparison (§9) |
| `verify_readback.go` | `history-service` readback with bounded retry |
| `verify_verdict.go` | PASS/FAIL/INCONCLUSIVE evaluation |
| `verify_report.go` | Console + JSON rendering |
| `verify_*_test.go` | Unit tests per the above |

Modified:

- `main.go` — dispatch a `verify` case
- `daily_pool.go` — thread `u.ID` and lane into broadcast attribution;
  honour `--lane`; add `directPool.SubscribeRoom(userID, roomID)` for
  dynamic subscription when a reserve floater is added to a room mid-run
  (which is what a real client does on joining)
- `daily.go` — seed the direct pool with a designated member set before
  the prefix walk in `activateUsers`
- `deploy/Makefile` — `run-verify`

The `activateUsers` change is the only edit touching a path `daily` also
uses. It is additive — an empty designated set reproduces today's
behaviour exactly — and covered by a regression test asserting `daily`'s
activation order is unchanged when no set is supplied. `SubscribeRoom` is
a new method, so it cannot affect existing callers.

No new dependencies — `nats.go`, `testify`, and `go.uber.org/mock` are
already in `go.mod`.

## 13. Testing

TDD throughout per CLAUDE.md: tests first, confirmed red, then
implementation. Coverage floor 80%, targeting 90%+ on tracker and verdict.

Unit-testable with no infrastructure — which is most of it:

- `ProbeTracker` — table-driven: expected-set construction, delivery
  recording, per-lane dedupe, duplicate detection, partial-vs-total-loss
  classification, leakage (`O ⊆ E`) on the user lane only, finalize
- Probe selection determinism — same seed ⇒ same probe rooms and probe set
- Membership epochs — a probe published at epoch N is judged against
  epoch N's expected set even when it lands after epoch N+1 begins; probes
  inside a settle window are suppressed, not failed
- Dual-oracle divergence — loadgen's model and a stubbed
  `subscription.list` disagree ⇒ `membership_not_applied`
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

## 14. Error Handling

- Wrap with `fmt.Errorf("…: %w", err)`; never bare `err`
- `log/slog` structured fields only
- Violation output logs `msgID`, `roomID`, user IDs — **never** message
  content, which keeps it clear of the no-logging-bodies rule and sidesteps
  encryption entirely
- `ctx` cancellation drains what it can and reports INCONCLUSIVE rather
  than passing a partial run
- Preflight failures exit non-zero immediately, matching the existing
  daily preflight style

## 15. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Drain too short ⇒ false failures | Generous 30s default; on-demand runs have no CI clock to fight |
| Large-room gatekeeper rejects ⇒ phantom bugs | Excluded from probe-room selection + preflight threshold check (§6.1) |
| Multiplex drops ⇒ phantom bugs | Probe-room members forced into direct pool (§6.0), so a background drop cannot reach probe accounting; count reported as load context, not a verdict gate (§6.2) |
| Re-adding a churn target ⇒ phantom `duplicate_delivery` | `SubscribeRoom` is idempotent per user-room pair — a remove leaves the subscription in place, so a re-add must not open a second one (§6.0 step 3) |
| Dual-lane subscribe ⇒ phantom duplicates | Dedupe per `(user, msg, lane)`; `--lane` to disambiguate (§7.1) |
| Coverage collapse at partial activation | Probe-room-first activation decouples coverage from N (§6.0) |
| Truncation-style fan-out bug hidden by index-correlated sampling | Full room membership tracked, never a subset (§6.0) |
| Direct-pool resource limits unknown | Budget estimates flagged unmeasured; benchmarking is a plan task (§6.3) |
| Room-lane leakage check would always fire (loadgen holds full-permission creds) | `O ⊆ E` asserted on the per-user lane only; room-lane membership verified via authorization instead (§7.3) |
| Membership change races a concurrent send ⇒ ambiguous expectation | Epoch + settle window; probes suppressed during settle, never adjudicated (§9.2) |
| Membership write lost ⇒ system and its self-report agree | Dual oracle; delivery judged against loadgen's model, not `subscription.list` (§9.3) |
| Added member unobservable (no dedicated conn) | Reserve floaters pre-connected; `SubscribeRoom` on add (§6.0 step 3) |

## 16. Success Criteria

**Outstanding — not met.** Criteria 1 and 7 require running the assembled
`loadgen verify` command against a live docker-compose stack. Docker was
unavailable in the environment that implemented this plan through Task 11,
so neither has been attempted; both remain open work for an operator with
Docker access. See
`.superpowers/sdd/2026-08-08-loadgen-verify-correctness/task-11-report.md`.

1. **OUTSTANDING.** `loadgen verify` runs to completion against the
   docker-compose stack and reports PASS on a healthy system.
2. Withholding a delivery in a unit test produces FAIL with the correct
   violation class and actionable IDs.
3. A harness-side drop produces INCONCLUSIVE, never FAIL.
4. Two runs with the same seed select the same probe rooms and probe set.
5. Every probe-room member is in the direct pool — asserted at preflight,
   not merely assumed.
6. Unit coverage ≥ 80% overall, ≥ 90% on `ProbeTracker` and the verdict.
7. **OUTSTANDING.** A measured direct-pool resource profile replaces the
   estimates in §6.3.
8. A membership change is detected as `membership_not_applied` when the
   `subscription.list` stub withholds it, and as
   `membership_remove_ineffective` when a removed member's send is still
   accepted.
9. A probe published before a change and delivered after it is judged
   against its own epoch, not the current one.
10. `--member-churn=0` reproduces the delivery-only behaviour exactly, so
    membership can be switched off when triaging.
11. No behaviour change to `daily`, `max-rps`, `soak`, or `members` — the
    `Collector` is untouched and `activateUsers` is additive-only.

## 17. References

- `docs/superpowers/specs/2026-05-27-daily-im-load-scenario-design.md` —
  the capacity scenario this reuses
- `tools/loadgen/README.md` §Daily-IM scenario — operator guide, known
  limitations
- `tools/loadgen/collector.go`, `daily_pool.go`, `daily_verdict.go` —
  the seams this builds on
