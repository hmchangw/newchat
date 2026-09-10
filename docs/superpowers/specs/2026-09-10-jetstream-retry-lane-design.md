# JetStream Retry Lane — Tiered Redelivery, Dead-Letter, and Triage

## 1. Problem

A worker that Naks a message with delay **keeps its ack-pending slot for the whole
backoff**. With the repo defaults — `MaxDeliver=6`, `MaxAckPending=1000`
(`pkg/stream/consumer.go:18-20`), `jsretry.DefaultBackoff` `{1s, 5s, 30s, 2m, 10m}` — a
message that fails every attempt occupies a slot for `1+5+30+120+600 = 756s` (~567s after
`jsretry`'s equal jitter).

Little's Law at a sustained transient-failure rate λ:

| λ (failures/s) | Steady-state slots needed | Time to exhaust 1000 |
|---|---|---|
| 1.3 | 1000 | — (the ceiling) |
| 5 | 3,780 | **200s** |
| 20 | 15,120 | 50s |

**The stall precedes the loss by an order of magnitude.** At 5 failures/s the consumer
stops delivering *anything* — healthy messages included — about 3.3 minutes into an
incident, while the first `MaxDeliver` drop is still 9 minutes away. The current maximum
sustainable failure rate is **~1.3 msg/s per consumer**.

Two consequences follow, and they are the two problems this design solves together:

1. **Q1 — the stall.** Parked retries consume a budget shared with healthy traffic.
2. **Q2 — the loss.** At `MaxDeliver` the message is abandoned:
   `tools/observability/METRICS.md:101` calls this "Not healed — abandoned … nothing
   auto-routes them (no DLQ)". Only a counter records it.

A dead-letter queue alone answers Q2 and leaves Q1 untouched — the first message would not
reach the DLQ until 12.6 minutes in, by which point the lane has been dead for nine of
them. Conversely, lengthening in-place retries (more attempts, longer backoff) makes Q1
strictly worse: every added second is occupancy multiplied by the failure rate.

## 2. Decision: relocate the wait, don't extend or abandon it

Keep the retry budget exactly as it is (12.6 minutes, five spaced attempts) but **move the
long waits off the hot lane**:

- fast rungs stay in place — `DefaultBackoff[:3]` = `{1s, 5s, 30s}` = **36s occupancy**
- slow rungs move to a retry lane — `DefaultBackoff[3:]` = `{2m, 10m}` = 12 minutes
- terminal failures land in a dead-letter stream instead of vanishing

Hot-lane occupancy drops 756s → 36s, so the sustainable failure rate rises from ~1.3/s to
**~27/s** (`1000/36`). The total patience per message is unchanged, which makes the change
easy to reason about and to roll back.

## 3. Design

### 3.1 `pkg/retrylane` — a wrapper, not a wider `pkg/jsretry`

`jsretry` is deliberately dependency-free: it decides Ack vs Nak and nothing else.
Escalation must *publish*, and putting a NATS handle into `jsretry` would impose it on all
12 importing services, including the three that must never escalate.

`pkg/retrylane` sits on top and mirrors `pkg/outbox.Publish`'s shape — the publish is an
injected function, not a connection:

```go
type Lane struct {
    Consumer string
    SiteID   string
    Publish  func(ctx context.Context, subj string, data []byte, msgID string) error
}

func (l *Lane) Settle(ctx context.Context, msg Msg, backoff []time.Duration, err error)
```

Below the threshold it delegates to `jsretry.Settle`; at the threshold it escalates. The 14
non-participating call sites are untouched, and CLAUDE.md's testing rule ("inject the
publish function as a field so tests can capture data without a real NATS connection") is
satisfied by construction.

`retrylane.Msg` widens `jsretry.Msg` (`Metadata/Ack/NakWithDelay`) with `Data()`,
`Subject()` and `Headers()`. `jsretry.Msg` itself does not change.

### 3.2 The four outcomes

| Condition | Outcome |
|---|---|
| `err == nil` | Ack |
| `errcode.IsPermanent(err)` | Ack-drop + WARN (unchanged — **never** escalates) |
| `NumDelivered <= FastSteps` | `jsretry` Nak with the fast schedule |
| `NumDelivered > FastSteps` | **Escalate**: publish to RETRY, then Ack the original |

Server-side `MaxDeliver=6` remains the backstop for the paths that never reach `Settle` —
pod crash, OOM, a handler hung past `AckWait`. Client-side escalation covers
handler-visible failures. The two levers stay on disjoint failure modes, extending the
existing `BackOff` vs `jsretry` split documented in CLAUDE.md §6.

### 3.3 Lane topology

`RETRY-{siteID}`, one stream per site, subject
`chat.retry.{siteID}.{consumer}.{tier}`, where `{tier}` is `slow` for an escalation from the
hot lane and `replay` for an operator-triggered replay from the DLQ (§3.8). One tier value
per entry path, so a replay is distinguishable from an organic escalation in both metrics
and logs; both are drained by the same per-service consumer, which filters
`chat.retry.{siteID}.{consumer}.>`.

The `{consumer}` token is load-bearing. `chat.msg.canonical.{siteID}.created` is consumed by
`message-worker`, `broadcast-worker`, `notification-worker` **and** `search-sync-worker`;
replaying to the original subject would re-run all four when one failed. JetStream consumers
filter by subject, not by header, so single-consumer targeting requires a per-consumer
subject.

Therefore **each participating service binds a second consumer** to `RETRY-{siteID}`,
filtered to its own name, running the same handler with the slow schedule and escalating to
the DLQ at the end. This is the design's main cost: a second consume loop per participating
service, not a shared drain service.

### 3.4 The escalation envelope

The republished message carries **byte-identical `Data()`**. `message-worker` writes those
bytes to Cassandra, and the hot-path workers marshal via sonic, whose output is explicitly
not byte-identical to stdlib (CLAUDE.md §6); re-marshalling would risk changing bytes that
dedup keys and wire-compat tests pin.

Metadata rides in headers, following the `X-Request-ID` / `X-Debug` / `Nats-Encoding`
convention:

| Header | Purpose |
|---|---|
| `X-Retry-Origin-Stream`, `X-Retry-Origin-Seq`, `X-Retry-Origin-Subject` | Provenance |
| `X-Retry-Consumer` | Routes back to exactly one consumer |
| `X-Retry-Attempt` | Cumulative across lanes; the loop guard |
| `X-Retry-First-Failed-At` | Unix ms — real time-to-dead-letter |
| `X-Retry-Reason` | errcode category / terminal reason **only** — never the error string |

`X-Retry-Reason` carries a category because `errcode.Error.cause` is never serialized by
design and a raw cause can carry a body or token. `X-Request-ID` and the W3C traceparent are
carried through unchanged (`natsutil.HeaderCarrier`), so a retry twelve minutes later lands
in the same trace lineage as the original send.

### 3.5 Dedup

`Nats-Msg-Id = {origStream}:{origSeq}:{consumer}` — deterministic, mirroring
`natsutil.CanonicalDedupID`. Escalation is publish-then-Ack, hence at-least-once: a crash in
that window re-runs the handler and re-escalates, and the deterministic ID makes the second
publish a dedup no-op. This is the same pattern as the OUTBOX publish-before-settle at
`docs/design/2026-07-05-membership-federation-durability.md:33`.

**This guarantee holds only inside the stream's duplicate window** — see §6.

### 3.6 Where the lane is forbidden

Escalation reorders by construction: message A escalates while later-published B proceeds.
The lane is therefore opt-in, and excluded from:

| Consumer | Reason |
|---|---|
| `outbox-worker` ordered lanes | `MaxAckPending=1` FIFO is the mechanism — a `room_renamed` must not overtake the `member_added` that creates the subscription it renames |
| `hr-sync-worker` | `MaxAckPending=1` so a quit cannot overtake the upsert (`main.go:113`) |
| `search-sync-worker` | Relies on single-consumer FIFO for `.created` before `.edited` (`docs/superpowers/specs/2026-05-14-message-edit-delete-canonical-events-design.md:316`) |

These keep their current semantics unchanged. First adopters are the concurrent hot-path
workers where order is already not guaranteed: `message-worker`, `broadcast-worker`,
`notification-worker`.

### 3.7 The content rule

**Message content lives only where a machine needs it to keep processing, never where a
human goes to look.**

| Hop | Carries the body? | Why |
|---|---|---|
| Source stream | yes | It is the message |
| `RETRY-{siteID}` | **yes** | The retry consumer re-runs the handler on those bytes |
| `DLQ-{siteID}` | **no** | Nothing re-processes it; it exists to be read by people |
| Mongo triage record | **no** | Same, plus it is long-lived and queryable |

RETRY is machine-only, short-lived, and no more exposed than the source stream it copies
from. The DLQ is human-facing and long-lived, so the body stops at the RETRY→DLQ boundary:
the dead-letter record is metadata, identifiers and failure context only.

This replaces encrypting the stored payload. Not storing content is a stronger guarantee
than storing it encrypted, and it removes a whole subsystem — no DEK handling, no decryption
path, no key rotation concern for `dlq-worker`.

The cost is that replay can no longer read the body from the DLQ; it fetches from the source
stream by sequence (§3.8), which bounds the replayability window. That trade is taken
deliberately.

### 3.8 Dead-letter: stream first, Mongo second

`DLQ-{siteID}` is its own stream, separate from RETRY. RETRY is fully consumed; a DLQ is
deliberately not, and mixing them would make the DLQ indistinguishable from the "sitting in
the stream unconsumed until retention" bug that `pkg/outbox.go:60-63` exists to prevent.
They also want opposite retention: RETRY short, DLQ long.

A new `dlq-worker` drains `DLQ-{siteID}` into MongoDB. **The ordering is load-bearing, not
incidental:** the most likely cause of dead-lettering is a datastore outage — quite possibly
Mongo itself. A terminal step writing directly to Mongo would fail in exactly the scenario
the DLQ exists for. The stream is the write-ahead buffer that survives Mongo being down; the
worker drains once it returns.

Two rules follow:

- **`dlq-worker` never escalates.** `MaxDeliver=-1`, in-place retry forever, same posture as
  `outbox-worker` — otherwise the dead-letter handler needs its own dead-letter handler.
- **Mongo is the queryable, mutable index.** Triage state (untriaged / triaged / bug-filed /
  replayed / ignored) is mutable and a stream is immutable append-only; you cannot mark a
  message triaged in JetStream. Mongo also answers "everything for `consumer=X`,
  `reason=Y`, last 24h", which a stream cannot.

### 3.9 The dead-letter record

`errcode.Permanent` never reaches the DLQ — `jsretry` Ack-drops it immediately. So every
DLQ entry was **classified transient by its handler and still failed after ~12.6 minutes**:
either a dependency outage that outlived the budget, or a permanent error misclassified as
transient. The second is itself a bug worth surfacing. In normal operation the DLQ is
empty, which is what makes depth a legitimate paging condition.

Per §3.7 the record carries no message content — it is a **pointer plus failure context**:

| Field | Source |
|---|---|
| `originStream`, `originSeq`, `originSubject` | `X-Retry-Origin-*` — the pointer used for replay |
| `consumer` | Which handler failed |
| `reason` | errcode category / terminal reason — never an error string |
| `attempts`, `firstFailedAt`, `deadLetteredAt` | Failure shape over time |
| `requestID`, `traceID` | The original trace; the primary troubleshooting handle |
| `siteID` | Routing / filtering |
| `status` | `untriaged` → `triaged` / `bug-filed` / `replayed` / `ignored` |

**The record has no free-form caller-supplied field.** Every field is typed and enumerable.
An open `map[string]string` or `notes` column would, given enough time, end up holding the
message text somebody wanted for debugging — which is exactly what §3.7 forbids. If a
service needs a domain identifier surfaced (room ID, message ID), it is added here as a
typed field with review, not passed through a generic bag.

### 3.10 Triage, replay and expiry

1. **Alert** — see §5.
2. **Triage** — the trace is the artifact, not the payload. `requestID`/`traceID` lead to
   the original request; `reason` plus `attempts` distinguish "dependency was down" from
   "this specific message cannot be processed".
3. **Replay** — `tools/dlqreplay`: list, inspect, replay by filter, replay one. Because the
   record holds no body, replay **fetches the original from the source stream by
   `{originStream, originSeq}`** and republishes it to
   `chat.retry.{siteID}.{consumer}.replay` with `X-Retry-Attempt` reset — not to the origin
   subject, for the same single-consumer reason as §3.3. Rate-limited, or replaying an
   outage's backlog re-creates the herd that caused it.
4. **Expire** — explicit `MaxAge` (~30d, IaC-owned) on the Mongo record. Anything unreplayed
   at expiry is accepted loss, stated plainly.

**Replayability window.** Replay works only while the original is still in the source
stream, so it is `min(source stream retention, DLQ record retention)` — in practice the
source stream governs. The DLQ record therefore outlives its own replayability: a 30-day-old
entry is still a valid bug record but may no longer be replayable. The `dlqreplay` tool must
report "original no longer in stream" as a distinct outcome rather than a generic failure,
and the admin view should show replayability as a derived state. Fetching by sequence is new
ground for this repo — there is no existing `GetMsg`/direct-get usage — so it may require
`AllowDirect` on the source streams (§6).

**No auto-drain.** Automatically replaying on dependency recovery re-creates the thundering
herd, and for genuinely poisoned messages it is an infinite loop with extra steps. Replay
stays operator-triggered.

### 3.11 Read access for QA and developers

**The access path is `admin-service`, not a Mongo shell.** It already has `requireAdmin`, a
permissions system and an audit endpoint (`routes.go:28-31`), with `admin-frontend` in
front. DLQ list/detail endpoints there inherit authn, authz and audit.

Because the record holds no content, the QA-facing view needs no data-protection controls
beyond ordinary admin authz — that is the point of §3.7. Content access is not absent, it is
**relocated to a privileged, audited action**: `dlqreplay --inspect` reads the body from the
source stream for an operator who has stream access, rather than every DLQ reader seeing a
stored copy. Ambient exposure becomes a deliberate one.

This supersedes the earlier proposal to store the payload encrypted via `pkg/atrest`. The
context for why it mattered: `atrest` encrypts the Cassandra `enc_payload` column, but the
JetStream payload on MESSAGES-CANONICAL is plaintext JSON — `message-worker` encrypts on the
way *into* Cassandra — so a naive DLQ copy would hold message bodies in the clear, against
CLAUDE.md's rule never to log full message bodies.

## 4. Configuration

Per the `caarlos0/env` convention, opt-in by default like `BOOTSTRAP_STREAMS`:

| Env | Default | Meaning |
|---|---|---|
| `RETRY_LANE_ENABLED` | `false` | Opt in per service |
| `RETRY_LANE_FAST_STEPS` | `3` | Split index into `jsretry.DefaultBackoff` |
| `RETRY_LANE_MAX_ATTEMPTS` | `3` | Retry-lane attempts before DLQ; the loop guard |
| `RETRY_CONSUMER_MAX_ACK_PENDING` | `4000` | Sized for 5/s escalation × 720s slow-lane occupancy |
| `RETRY_CONSUMER_MAX_WORKERS` | `10` | Deliberately small — doubles as the recovery herd damper |

The surface exposes a **split index, not a schedule**. A raw `[]time.Duration` env knob
would reproduce the bug documented at
`docs/superpowers/specs/2026-08-21-jetstream-exponential-backoff-design.md:§4.1` (an empty
env value falls back to `envDefault` under `caarlos0/env` v11.4.0, so there is no
off-switch).

**Rollback semantics:** `RETRY_LANE_ENABLED=false` stops new escalations but the retry
consumer **keeps draining until empty**. Without that asymmetry, flipping the flag strands
whatever is parked on RETRY.

**Degradation:** when the retry lane's own ack-pending budget saturates, retries stall and
the hot path continues. That is the designed behaviour, not a fault.

## 5. Observability

`pkg/natsmetrics` additions:

- **`OutcomeEscalated`** alongside `ack`/`nak`/`term`. Without it the hot lane's
  Ack-after-republish is indistinguishable from a successful process, making escalation
  invisible in exactly the dashboards built to catch it.
- **`TerminalDeadLettered`** as a new `TerminalReason`.

`Message.finish()` auto-marks `TerminalMaxDeliver` on final delivery (`metrics.go:488`) and
`MarkTerminal` is once-only, so the escalation path must mark **before** that fires or every
escalation is mislabeled `max_deliver`.

Three alerts, with distinct meanings:

| Signal | Means | Fires at |
|---|---|---|
| Escalation rate | An incident is in progress | **~36s** |
| Untriaged count in Mongo | Real dead letters awaiting a human | ~13 min |
| DLQ **stream** depth / oldest age | The drain worker is broken, or Mongo is down | drain-dependent |

The first is the incident alert; the other two are aftermath tooling. Note the second and
third are not interchangeable: once `dlq-worker` drains and Acks, stream depth sits at ~0
permanently, so a naive "DLQ length" alert would never fire.

## 6. Preconditions (ops/IaC-owned)

`pkg/stream.Config` carries only `Name` and `Subjects` (`stream.go:9-12`) — retention,
replication and dedup are ops-owned, as with OUTBOX's `R3 + file` at
`docs/design/2026-07-05-membership-federation-durability.md:39`. These cannot be enforced
from this repo:

1. **`RETRY-{siteID}` `Duplicates` ≥ the escalation window.** §3.5's exactly-once escalation
   silently degrades to at-least-once if this is set too short.
2. **`DLQ-{siteID}` `MaxAge` ≥ notice + triage time** (~30 days).
3. **Both `R3 + file`**, for the same reason OUTBOX is.
4. **Source-stream retention governs replayability.** Because the DLQ holds no body (§3.7),
   replay reads the original by sequence, so `min(source retention, DLQ MaxAge)` is the real
   replay window — and in practice the source stream is the shorter of the two. If
   MESSAGES-CANONICAL retention is materially shorter than the triage turnaround, replay is
   unavailable for exactly the entries a human got to late. **Confirm the current retention
   of every participating source stream before phase 4**, and size DLQ `MaxAge` knowing the
   record outlives its replayability.
5. **`AllowDirect` on participating source streams**, if direct get is chosen for the
   sequence fetch. No `GetMsg`/direct-get exists in the repo today, so this is unproven
   ground rather than an established pattern.

## 7. Rollout

| Phase | Content | Gate |
|---|---|---|
| 0 | Ack-pending premise test (§8) | **Stop and revisit if it fails** |
| 1 | `pkg/retrylane`, stream configs, metrics — ships dark | `RETRY_LANE_ENABLED=false` |
| 2 | `notification-worker` only — lowest blast radius | Watch escalation rate |
| 3 | `message-worker`, `broadcast-worker` | |
| 4 | DLQ stream, `dlq-worker`, Mongo, admin endpoints — **separate PR** | |

Phase 4 is collectively larger than the retry lane itself. The retry lane stops the outage;
the DLQ pipeline is aftermath tooling. Sequencing it second keeps the stall fix from being
blocked behind a QA console.

## 8. Test plan (TDD, Red first)

1. **The gate.** Real NATS via `testutil.NATS(t)`, consumer at `MaxAckPending=1`,
   Nak-with-delay one message, assert a second is **not** delivered while the first is
   parked. This proves the premise the entire design rests on — currently sourced from
   `docs/design/2026-07-05-...:38` and `METRICS.md:56`, not from nats-server source or a
   live consumer. If it fails, the tiering loses its justification and the design is
   revisited before any implementation.
2. **`pkg/retrylane` unit tests**, table-driven, injected publish func, no NATS: escalates at
   threshold; delegates below it; `errcode.Permanent` never escalates; deterministic
   `Nats-Msg-Id`; headers populated; **body byte-identical**; loop guard at max attempts;
   disabled = pure `jsretry` passthrough.
3. **End-to-end escalation** on real NATS: fail N times, assert the message appears on the
   RETRY subject with correct headers and the original is acked.
4. **Metrics:** `OutcomeEscalated` emitted; no mislabeling as `max_deliver`.
5. **Per-service wiring** for each phase-2/3 adopter.
6. **The content rule is a test, not a convention** (phase 4). Assert that a dead-letter
   record and the DLQ stream message carry **no bytes from the original payload** — including
   the negative case where the source payload contains a known sentinel string that must not
   appear anywhere in the DLQ record or its serialized form. A rule enforced only by review
   erodes the first time somebody wants the body for debugging.
7. **Replay against an expired original** returns the distinct "no longer in stream" outcome
   rather than a generic error (§3.10).

Coverage floor 80%, 90%+ for `pkg/retrylane` per CLAUDE.md.

## 9. Documentation changes in the same PR

- `tools/observability/METRICS.md:101` — the "Not healed — abandoned … no DLQ" row becomes
  false for participating consumers and would actively mislead an on-call engineer.
- CLAUDE.md §6 "JetStream Redelivery Backoff" — documents two levers; there are now three.
- No `docs/client-api.md` change: nothing client-facing moves.

## 10. Non-goals

- No change to the FIFO lanes in §3.6.
- No auto-drain of the DLQ (§3.8).
- No exactly-once claim — at-least-once plus idempotent handlers, as everywhere else.
- No consumer-level backpressure (pausing the pull loop on correlated dependency failure).
  It is the cheapest treatment for the *cause* of a 5/s failure rate and complements this
  design, but it is a separate change with its own health-plumbing surface.
