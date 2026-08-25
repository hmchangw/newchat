# Surviving a 1-Hour Cassandra Outage Without Message Loss

**Status:** Implemented on `claude/cassandra-downtime-data-loss-yc8al7`. This document
describes the **shipped** design. It replaced a `MESSAGES-PARKED` parking-lane design
mid-implementation, and §4 records the non-goal that change had to retract; the superseded
plan is summarised in `docs/superpowers/plans/2026-08-13-cassandra-outage-message-durability.md`.
Integration tests are written but have **never been executed** — see §5.1.
**Date:** 2026-08-13
**Related:** `docs/design/2026-07-05-membership-federation-durability.md` (PR #410, the
OUTBOX relay whose "delay-not-drop within retention" guarantee this mirrors),
`docs/superpowers/specs/2026-07-17-message-worker-thread-subscription-outbox-durability-design.md`,
`pkg/jsretry`, `pkg/stream`.

## 1. Problem

`message-worker` is the sole persister of message history to Cassandra
(`message-worker/handler.go:74`). When Cassandra is unavailable, its writes fail,
the handler returns a wrapped error, and `jsretry.Settle` NAKs on
`jsretry.DefaultBackoff` (1s, 5s, 30s, 2m — `pkg/jsretry/jsretry.go:43`, last entry
reused).

The consumer takes its redelivery cap from `stream.DurableConsumerDefaults`, which
defaults to **`MaxDeliver=5`** (`pkg/stream/consumer.go:17`). No deployment in this
repo overrides it. Five deliveries against that backoff is a retry window of
**~2m36s**, after which JetStream terminates the message.

**The loss is silent and produces divergence, not just absence.** Nothing consumes
`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES`, so a terminated message leaves only an
ERROR log line that stops appearing. Meanwhile `broadcast-worker` has already
delivered the message to online clients and `search-sync-worker` has already indexed
it in Elasticsearch — neither touches Cassandra. The message is on screen and
findable in search, but absent from history permanently.

Restarting the pod during the outage accelerates the loss rather than deferring it:
`cassutil.Connect` fails at startup and the process exits (`message-worker/main.go`),
so the pod crashloops while each `AckWait=30s` expiry burns another delivery.

**Goal:** a Cassandra outage of ~1 hour delays message persistence; it never destroys
a message.

### 1.1 Poison is not currently distinguishable from infra failure

`jsretry.Settle` Ack-drops an error only when it carries the `errcode.Permanent`
marker. Across the entire repo, **every one of the ~20 `errcode.Permanent` call sites
is a payload-decode failure** (`message-worker/handler.go:75`,
`inbox-worker/handler.go:383`, `outbox-worker/handler.go:45`, …). No Cassandra or
Mongo error is classified anywhere.

So "poison" today means "unparseable bytes", and a genuinely unwritable message is
indistinguishable from an infra failure — both NAK and both ride the redelivery cap to
termination. Any design that keeps a finite cap as a poison backstop is relying on a
distinction the code does not make. §3.3 makes the distinction properly, by asking
Cassandra for the error code instead of inferring it from a redelivery count.

### 1.2 What is already handled

- `message-gatekeeper` degrades the quoted-parent path during a transient history
  outage rather than rejecting the send (`message-gatekeeper/handler.go:349`).
- `history-service` edit/delete/reaction/pin writes go to Cassandra first and fail
  cleanly back to the caller (`history-service/internal/service/messages.go:485`) —
  a rejected operation, not silent loss.

## 2. Product decision

**Divergence is preferred over downtime.** Sends keep succeeding and messages keep
delivering live during the outage; history backfills on recovery. The alternative —
having `message-gatekeeper` reject sends while history is unavailable — would prevent
divergence outright at the cost of an hour of chat downtime, and was rejected.

Because divergence is accepted, clients are told about it rather than being shown a
gap as truth (§3.2).

## 3. Design

### 3.1 Remove the loss boundary

Pin **`MaxDeliver = -1`** for message-worker's default-mode consumer, so JetStream never terminates a
message, and move the give-up decision into application code where it can be made
against the reason Cassandra rejected the write (§3.3) rather than against a delivery
count that knows nothing about it.

A finite consumer-level cap was considered and rejected. Any fixed value is a cliff:
a cap sized for ~72 minutes silently kills the in-flight set at minute 73, and — per
§1.1 — it cannot serve as a poison backstop anyway, because poison and infra failure
are the same error to this code. The true durability ceiling is stream retention, and
a second, tighter, less observable limit underneath it buys nothing.

**`MaxDeliver = -1` is set in code, not left to an env var.** `pkg/stream/consumer.go`
still defaults `MaxDeliver` to 5, and the give-up logic in §3.3 is written assuming
JetStream will never terminate anything. A deploy that merely forgot
`CONSUMER_MAX_DELIVER` would therefore run the new policy on a terminating consumer and
silently restore the original loss — behind a marker that now tells clients history is
complete. `buildConsumerConfig` pins `-1` on the default-mode path and
`validateConsumerConfig` refuses to start if the resolved value is `>= 0`. Two sibling
services already establish this pattern (`outbox-worker/main.go`,
`hr-sync-worker/main.go`).

**Teams mode keeps a finite cap.** It has no give-up path of its own and uses plain
`jsretry.Settle`, so an unbounded NAK there would be a regression; the shared env var is
folded to a finite value on that path.

**`MaxAckPending` stays at 1000.** The stall it produces is load-bearing, not a defect
— see §3.8.

**Ops prerequisites** (stream limits are ops/IaC-owned; `pkg/stream/stream.go` sets
only `Name` + `Subjects`):

- `MESSAGES-CANONICAL-{siteID}` `MaxAge` must comfortably exceed the target outage.
- `MaxBytes` must hold the backlog: at 2.92M publishes/day ≈ 34/s at ~1KB
  (`docs/nats-traffic-estimation.md:235`), one hour is **~122 MB**.

The guarantee is **delay-not-drop within `MaxAge`** — the same conditional guarantee
PR #410 shipped with, not a weaker one.

### 3.2 Degraded-history marker

Cassandra returning 40 rows is indistinguishable from "there are 40 messages";
`history-service` cannot infer that a write is still sitting in a NATS retry queue.
Something must tell it.

**The marker does not select the retry policy.** An earlier revision made retry-forever
conditional on it; that coupling was removed, because a failing message's own
contribution to the marker destroyed the evidence needed to judge that message (§3.3
decides from the error class instead). The marker's consumers are all read-only: the
`incompleteSince` field below, the quoted-parent retry (§3.5), and thread-badge
suppression (§3.6).

**Granularity: site-wide.** `message-worker` sets a "history degraded since T" marker
when its Cassandra writes begin failing, and clears it once the backlog drains.

**Only an infra-class failure sets it**, and this is load-bearing precisely *because* the
marker is site-wide. A request-class verdict (§3.3) is the classifier saying "this one row
is unwritable" — per-message by construction — and a clean `messages_by_id` miss on a
thread parent is an ordering race between concurrent workers, not evidence about
Cassandra. Either one, allowed to mark, turns a single message into a site-wide "history
is incomplete" for the whole drain grace: `incompleteSince` on every room, every thread
badge suppressed. The cost of this rule is that a site-wide fault presenting as request
class (a migration returning `Invalid` for every write) sets no marker; that is the
population-signal follow-up, covered today by the drop-rate metric, its alert and
`HISTORY_DROP_ENABLED`. Marking per-message was never a stand-in for it — it fired on one
poison row just as readily.
Per-room high-water-mark tracking was rejected as bookkeeping on the hot write path
disproportionate to a rare event; over-flagging a quiet room costs a user nothing worse
than a "still catching up" hint, and the coarse marker cannot produce false negatives.

**Storage: a single MongoDB document**, keyed by `siteID`, in a dedicated collection
(e.g. `history_degradation`), holding `degradedSince` (UTC millis) and `updatedAt`.
Both services already hold a Mongo client (`message-worker/main.go`,
`history-service/internal/mongorepo`), so this adds **no new infrastructure
dependency** to either. NATS KV was rejected because the repo uses it nowhere today;
Valkey was rejected because `message-worker` does not currently connect to it.
`history-service` caches the read for a few seconds — the marker changes at most twice
per incident, and a history read is not worth a Mongo round-trip per call.

**Set/clear transitions:**

- **Set** on the first Cassandra write failure after a healthy period — written once on
  transition, not per failed message. `degradedSince` is the timestamp of that first
  failure, which is what `incompleteSince` reports.
- **Clear** when a write succeeds **and** the consumer's backlog has settled — both
  `NumPending` and `NumAckPending` (`cons.Info()`). Clearing on first success alone would
  be wrong: the drain is exactly the window where §3.5 needs the flag held, since parents
  are still being replayed. The backlog check is what distinguishes "Cassandra answered
  one write" from "history has caught up".
- **Bounded by `drainTailGrace`.** A strict `NumAckPending == 0` gate deadlocks: a
  permanently-NAK'd message holds the count above zero forever (nats-server keeps a
  delayed NAK in the consumer's pending set), so the marker would pin and never clear. The
  grace elapsing is the normal way a recovery ends, not an anomaly.
- **Only a write failure restarts that grace.** The clock starts when the undelivered
  backlog first reads empty and runs from there; arriving traffic does not reset it. It
  used to, and that made the bound useless on any site under load — `NumPending` is
  non-zero constantly, so a 20-minute grace needed ~240 uninterrupted 5s samples it would
  never get, and the marker pinned indefinitely after recovery (taking `incompleteSince`
  and thread-badge suppression with it, and leaving §3.5's quote retry with no exit).
  Queue depth is not the health signal — writes failing is, so `OnWriteFailure` is what
  restarts the clock. Clearing is still gated on `NumPending == 0`, so a live backlog
  holds the marker regardless of how long the grace has run. Its length is **derived
  from `jsretry.DefaultBackoff`'s tail** (two tail waits, floored at 5m), not written down:
  a literal silently falls under the interval it must outlast the next time that schedule is
  retuned — which is exactly what #344 did to the original hard-coded 5m.

A marker stuck set costs lag, not loss — §3.7's ack-floor-age alert is what catches it.
Because the marker no longer gates retries, a stuck marker can no longer keep messages
in the retry loop; it only prolongs `incompleteSince`, quote retries, and badge
suppression.

**Wire change — additive only.** One optional field on each of the three history read
responses in `history-service/internal/models/message.go` — `LoadHistoryResponse`
(:30), `LoadNextMessagesResponse` (:42), `LoadSurroundingMessagesResponse` (:60):

```go
IncompleteSince *int64 `json:"incompleteSince,omitempty"` // UTC millis; rows at/after this may not be persisted yet
```

Pointer + `omitempty` means the field is absent entirely on the happy path. No
existing field changes type or meaning; no new RPC; no version negotiation. **A client
that does not implement the marker sees exactly today's behavior**, so the backend
ships once and each frontend — including the cross-team one outside this monorepo —
adopts on its own schedule.

This follows vocabulary the client API already has: read receipts return
`code: unavailable` / `reason: read_receipts_unavailable` when history is unreachable
(`docs/client-api.md:2396`), and the quote path already ships a
`"Content temporarily unavailable"` placeholder during a transient history outage
(`docs/client-api.md:6212`).

**Docs (required by CLAUDE.md, same PR):** `docs/client-api.md` plus both derived
views, `docs/client-api/request-reply.md` and `docs/client-api/events.md`.

### 3.3 Ask Cassandra why the write failed

Giving up is decided per failure by the CQL error code, in
`message-worker/cqlclass.go`:

| Class | Codes | Policy on a failed write |
|---|---|---|
| **Request** | `Invalid` (0x2200), `SyntaxError` (0x2000) | NAK until the retry window elapses, then **drop** (Ack). |
| **Infra** | everything else, including every unrecognised error | NAK indefinitely. |

Request class is deliberately tiny. These two are deterministic for a given message
against the static prepared statements this service issues — an oversized mutation, a
null clustering key, a code bug in a statement — so no amount of retrying changes the
outcome, and an unbounded NAK loop would hold an ack-pending slot forever.

**`Unauthorized` (0x2100) and `ConfigError` (0x2300) are infra class, and that is the
single most important line in the classifier.** A rotated credential and a keyspace
missing at a new site look permanent per message but fail *every* write; dropping on
them would destroy the site's whole feed for the duration of a misconfiguration —
strictly worse than the bug this design removes. `Unprepared` (0x2500) is transient
because the driver re-prepares. The driver-level sentinels (`ErrNoConnections`,
`ErrConnectionClosed`, `ErrTimeoutNoResponse`) and context deadlines carry no CQL code
at all and fall through the default. **Anything unrecognised is infra class**: unknown
errors retry, they never drop.

Classification uses `errors.As` against gocql's exported `RequestError` interface — the
store wraps every driver error with `%w`, so the chain is traversable, and no code ever
matches on an error string.

### 3.4 The retry window

A request-class failure is retried for `INVALID_RETRY_WINDOW` (default **1h**) and then
dropped. The threshold is a **duration, not a delivery count**, because the question it
answers is "will this error resolve on its own?" — schema drift resolves when the
migration finishes, which is a time, while a malformed value never resolves. A delivery
count is the same deadline in disguise (a cap of 5 deliveries silently meant ≈2m36s)
and would move if anyone retuned the backoff schedule.

Elapsed retry time is derived from `NumDelivered` and the backoff schedule via
`jsretry.ElapsedFor`, which sums the delays `Settle` has already applied — with
`DefaultBackoff` that is `2m36s + (N-5) × 10m`, so a 1h window is first crossed on the
**11th delivery** (logged at startup so the mapping is visible, not implied).

That number is schedule-dependent and has already moved once: it was the 34th delivery
while `DefaultBackoff` ended in a 2m tail, and became the 11th when the shared schedule
gained a 10m tail. Nothing in the service needed changing, which is the point of
expressing the threshold as a duration — but it does mean the count is a derived fact to
read off the startup log, never a constant to hard-code.

`ElapsedFor` sums **un-jittered** schedule entries while the delays actually served are
jittered into `[half, full]` of each entry. The budget is therefore an upper bound on
real elapsed time: a 1h window is crossed after roughly 45m of wall clock on average.
Deliberate — the deadline stays reproducible instead of depending on the random draws a
particular message got.

It is deliberately **not** the message's age on the stream (`meta.Timestamp`): after an
outage the replayed backlog is already hours old, so an age-since-publish measure would
drop a request-class failure on its *first* delivery, with zero retries — precisely
backwards in the situation this design exists for. Only time actually spent retrying
counts. A message whose metadata cannot be read has no delivery count and is therefore
retried, never dropped.

**The degraded marker is not consulted here.** An earlier revision selected the retry
policy by site health and had to be reverted: a history failure re-degrades the site, so
a message's own failure destroyed the evidence needed to condemn it. The error class
answers the same question — is this failure specific to this message? — directly and per
failure. The marker keeps its other three consumers (§3.2, §3.5, §3.6).

**Two brakes, because `Invalid` is mostly a site-wide fault in practice.** `Invalid`
(0x2200) is request class because it is deterministic for one message against a static
statement — but Cassandra also returns `InvalidRequest` for **`unconfigured table`**,
**`Undefined column name`**, and a **failed re-prepare after a column is dropped or
retyped**. Those hit *every* write, identically to the `ConfigError` the classifier
refuses to drop on. The oversized-mutation / null-clustering-key framing describes the
per-message case, not the common one. So the classifier protects the feed from
`Unauthorized`/`ConfigError` while `Invalid` leaves an adjacent door open, and both
brakes below exist to bound what can go through it:

- **`HISTORY_DROP_ENABLED`** (default `true`) turns every drop back into a NAK, logged
  at WARN. The *attended* brake: it needs a human to notice the class-labelled metric
  and flip it inside the retry window.
- **`MAX_DROPS_PER_MINUTE`** (default `10`, validated `>= 1`) caps drops with an
  in-process fixed-window counter; over the cap, the message NAKs instead of dropping.
  The *unattended* brake — at 02:00 on a weekend nobody flips a switch. A rate cap was
  chosen over a "what fraction of failures are request class?" test because it needs no
  threshold tuning and bounds loss deterministically regardless of *why* a wave is
  happening: a genuine burst of hundreds of distinct bad rows is bounded too, where a
  ratio test would wave it through. Sizing: genuine poison is a trickle, so 10/min is
  generous for real bad rows while capping a wave at well under 1% of a ~34/s feed.
  Per-pod and in-process by design (no shared state, no round trip on the failure
  path), so **N pods allow N × the cap in aggregate**. The window is fixed rather than
  a token bucket, so an interval straddling a boundary can pass up to `2×max − 1` drops
  (a token bucket of equal rate would not re-burst like that). The aggregate bound is
  unaffected — windows are disjoint and independently capped, so total drops over a
  duration `D` stay ≤ `max × ceil(D/window)` per pod, which is what the figures below
  rest on.

Refusing a drop is not refusing it forever: the message returns on the backoff schedule
and may drop in a later window, so destruction is *spread out* rather than blocked —
the cap buys reaction time without creating an unbounded retry loop. Both suppression
paths increment `message_worker_history_drop_suppressed_total{reason="rate_limited"|"disabled"}`,
so an operator can tell whether it is safe to release the kill switch, and so the brake
itself is not invisible.

**Accepted risk, stated plainly:** a request-class error that affects *all* messages —
schema drift during a rolling migration that overruns the window — still destroys
messages once `INVALID_RETRY_WINDOW` elapses, but the loss is now **bounded**:
`MAX_DROPS_PER_MINUTE` per pod per minute, not the whole feed. A one-hour migration
overrun on a 3-pod deployment costs at most ~1,800 messages instead of ~120,000, while
`message_worker_history_dropped_total` and the class-labelled failure counter make the
wave visible from the first failure. What remains unbounded is *duration*: nothing
stops the drip if nobody responds, so the dropped-message counter warrants an alert on
any non-zero value. A dropped message is unrecoverable — there is no triage queue to
replay it from.

### 3.5 Preserve quotes across the recovery drain

`reprojectUnverifiedQuote` (`message-worker/handler.go:171-183`) drops a quote
permanently when the authoritative parent reads as not-found.

The exposure is the **recovery drain**, not the outage itself. While Cassandra is
down, `GetQuotedParentSnapshot` returns an *error*, so the message NAKs and nothing is
dropped. The drop occurs once Cassandra is answering correctly but the parent is still
in the replay backlog. The affected population is narrow — `QuotedParentUnverified` is
set only when the gatekeeper's fetch failed at send time, so it is replies sent during
the outage that quoted a parent also sent during the outage — but the defect is
permanent when it hits.

```go
if !found {
    if h.historyDegraded() {
        // Parent is very likely still in the replay backlog — retry rather than
        // persist a permanent defect. Bounded by the marker clearing.
        return fmt.Errorf("quoted parent %s not yet persisted during degraded window", q.MessageID)
    }
    // ... existing drop path, unchanged
}
```

This reuses the §3.2 marker as its gate, so it costs one dependency on the `Handler`
and one branch.

**No new failure mode:** the marker clears when the backlog drains, so a reply whose
parent genuinely never persists stops retrying, takes the existing drop-the-quote path
on its next redelivery, and persists without its quote. The message is never lost. The
change strictly improves on current behavior.

### 3.6 Replay hygiene

Draining replays the whole handler, not only the failed Cassandra write: mention
resolution, user lookup, thread-room creation, `AdvanceThreadSubscriptionLastSeen`,
thread-mention marking, and `publishThreadReplyEvent`. The Cassandra writes are keyed
by message ID and are effectively idempotent; the **side-effect publishes are not**.

Suppress `publishThreadReplyEvent` during drain so recovery does not fire an hour of
stale thread-reply badges at clients in a burst, following the existing `isMigration`
suppression pattern in `processMessage`.

### 3.7 Observability

Retry-with-no-cap converts loud loss into quiet lag unless the lag is visible. There is
currently no advisory consumer, no oldest-unacked-age metric, and no backlog alert
anywhere in the repo.

Add, with alerts:

- Consumer backlog depth and oldest-unacked age (catches a stuck marker, which under
  §3.4 is now one of the two things that keep a retry loop running: the other is an
  infra-class failure, which is meant to).
- Dropped-message count by CQL code (`message_worker_history_dropped_total`) — any
  non-zero value is data destruction and warrants an alert.
- Suppressed drops by reason (`message_worker_history_drop_suppressed_total{reason}`) —
  `rate_limited` means the unattended brake is the only thing holding the feed together
  and needs a human now; `disabled` means the kill switch is still engaged.
- History write failures by error class (`message_worker_history_write_failures_total{class}`)
  — a rising `class="request"` rate is a migration-induced wave, visible before the
  retry window elapses and anything is destroyed.
- Degraded-marker state and duration.

**This section is what keeps the change from trading a visible failure for an invisible
one; it is not optional polish.** It ships before the behavioral changes (§6).

### 3.8 Why the retry load is safe for NATS

Redelivery rate is bounded by `MaxAckPending ÷ backoff tail` = **1000 ÷ 120s ≈ 8.3
redeliveries/sec**, flat, regardless of incoming traffic and regardless of outage
length. Against a stream already doing ~34 publishes/s and 169 delivery-ops/s
(`docs/nats-traffic-estimation.md:235`), that is roughly 5% overhead. Each NAK is a
consumer-state update (a RAFT proposal on a replicated consumer), which at 8/s is
noise.

**The rate does not grow with the backlog.** Once 1000 messages are pending the
consumer stops pulling; the remaining ~121,000 messages arriving during the hour sit in
the stream untouched — never delivered, never NAK'd, `NumDelivered=0` — costing only
disk. The population that can ever be affected by a give-up decision is therefore
capped at ~1000 regardless of outage length, and the backlog behind them is delivered
for the *first* time after recovery.

Two consequences to carry into implementation:

- **Do not raise `MaxAckPending` to avoid the stall.** Redelivery rate scales with it;
  at 100k pending it would be ~833/s plus a 100k-entry pending map in replicated
  consumer state.
- **Expect ~2 minutes of dead time after recovery** while the in-flight batch waits on
  its NAK timers. Cosmetic, but it will appear in metrics as a lag plateau after
  Cassandra looks healthy.

Unbounded retry costs **zero in steady state** — it is inert until something fails.

## 4. Non-goals

- **Edits, deletes, reactions, pins.** Synchronous request/reply with no stream to
  buffer into; they remain unavailable for the duration of the outage and fail cleanly
  to the caller. Making them durable would mean inverting those paths to event-driven,
  changing their read-your-writes semantics.
- **Anti-entropy / reconciliation** beyond stream retention. Same boundary PR #410 and
  the 2026-07-17 thread-subscription spec ship with.
- **Reconstructing a dropped message.** A drop is final: the log line names what was
  lost, and there is no replay lane to recover it from. That is the cost of dropping
  the parking lane, and it is why request class is only two codes and why §3.4 caps the
  drop rate.
- **Consistency during the window.** Live delivery and Elasticsearch will hold messages
  that Cassandra does not, until the drain completes. This is the accepted cost of
  choosing divergence over downtime (§2); §3.2 makes it visible rather than removing it.

## 5. Testing

Per CLAUDE.md, TDD (Red-Green-Refactor) for every item below.

**Unit (`message-worker/handler_test.go`, table-driven, existing `mock_store_test.go`):**
- Classifier (§3.3): each request-class code → request; every infra code → infra, with
  `Unauthorized` and `ConfigError` called out; wrapped errors classify through
  `errors.As`; an unknown error and a nil error → infra.
- Retry policy (§3.4): success → Ack; decode failure → still `errcode.Permanent`
  Ack-drop, unchanged; non-history failure at high `NumDelivered` → NAK, never dropped;
  **infra-class failure at any `NumDelivered` → NAK, never dropped** (the regression
  guard: this is what stops a future change from making outages destroy messages
  again); request-class inside the window → NAK; request-class past the window → Acked
  with `onDropped` recorded; request-class past the window with
  `HISTORY_DROP_ENABLED=false` → NAK.
- `jsretry.ElapsedFor`: matches the delays `Settle` actually applies, and saturates
  rather than wraps on an absurd delivery count.
- Marker (§3.2): set once on the first write failure, not per message; **not** cleared
  on a successful write while `NumPending > 0`; cleared on success once `NumPending`
  is ~0.
- Quotes (§3.5): degraded + not-found → transient error (NAK); not degraded +
  not-found → existing drop-quote-and-persist; found → authoritative snapshot applied
  in both states; store error → transient regardless of flag.
- Replay hygiene (§3.6): `publishThreadReplyEvent` suppressed during drain, published
  normally otherwise.

**Unit (`history-service/internal/service`):** `incompleteSince` present and correct
when the marker is set; **absent from the marshalled JSON** when clear (guards the
additive-only wire contract old clients depend on).

**Integration (`//go:build integration`, containers from `pkg/testutil`):** with
Cassandra rejecting writes, messages are NAK'd and retained rather than terminated, and
none are dropped no matter how many redeliveries pile up (the failure is infra class);
after Cassandra recovers, the full backlog persists with no gaps and quotes intact.

**Coverage:** 80% floor, 90%+ target for handler and store paths.

### 5.1 Not yet executed — CI must close this

Every integration test on this branch is **written and compiling but never run.** The
environment it was built in has no persistent Docker daemon, and image pulls are denied
by proxy policy, so no `testutil` container can start. Specifically unverified:

- The end-to-end guard above — the only test covering the outage → recovery → drain →
  clear sequence against a real Cassandra and Mongo.
- All of `pkg/histdegrade`'s store behavior. `Set`'s `$setOnInsert` first-writer-wins
  semantics, the `_id` projection round-trip, and `Get`'s `(nil, nil)` on a missing
  document are covered **only** there. A `$setOnInsert`/`$set` inversion would make
  `incompleteSince` ratchet forward on every failure and no unit test would notice.
- Full `make sast`: `gosec` runs clean, but `govulncheck` is network-blocked and
  `semgrep` is not installed.

A staging soak is also worth running, since no unit test can substitute for it: a real
consumer at `MaxDeliver=-1`, confirming `NakWithDelay(2m) > AckWait(30s)` behaves as
assumed, that the `MaxAckPending` stall engages instead of unbounded redelivery, and that
the marker clears at the end of a drain rather than sticking.

## 6. Implementation order

1. **§3.7 observability.** First, so the drain and the retry loop are measurable before
   anything about their behavior changes.
2. **§3.2 degraded marker** (storage, set/clear transitions) — the dependency for
   everything below. Ships with `incompleteSince` and the `docs/client-api.md` +
   derived-view updates.
3. **§3.3 error classification** — must exist before retries become unbounded, so that
   the only thing that can ever give up is a failure specific to one message.
4. **§3.4 retry window** + pinning `MaxDeliver = -1` in code and ops confirmation of
   `MaxAge`/`MaxBytes`. This is the step that actually stops the loss.
5. **§3.5 quote preservation.**
6. **§3.6 replay hygiene.**

Steps 3 and 4 must not be separated: `MaxDeliver=-1` without a give-up decision means
nothing ever gives up, and a give-up decision without `MaxDeliver=-1` still lets
JetStream terminate messages behind its back.
