# P2 Instrumentation Spec — J1 (SLO-1a / 1b / 2)

> What to build so the flagship journey becomes measurable, written against the
> code rather than against the roadmap line. Constraint: **the fewest instruments
> that make the three SLOs computable, and no regression on the send hot path.**
>
> Verified against the code on 2026-08-27. #337 is the baseline — its recorder
> discipline (precomputed attribute sets, nil-collapse when disabled, the semgrep
> cardinality gate, the `pkg/obs` registry guard) applies to everything here.

| | |
|---|---|
| **Status** | Draft — for review |
| **Net change** | **3 instruments to add, 1 already exists, 1 to drop.** Only one of the three has a measurable hot-path cost, and it is bounded and plumbing-shaped, not per-recipient |
| **Not in scope** | Anything that needs a JetStream advisory consumer — see §6 |

---

## 0. What P2 actually needs, after reading the code

The roadmap line (`common/sli-slo.md` §8 P2) reads as five pieces of work. Two of
them are already done and one should be dropped.

| Roadmap piece | Reality | Action |
|---|---|---|
| gatekeeper `messages_canonical_published_total{broadcast_path}` | Absent | **Add** — §1A |
| message-worker persisted numerator | **Already exists** as `message_worker_persistence_total{message_kind,result}` (`message-worker/nats_metrics.go:46`, recorded at `handler.go:190,193,204,207`) | **Nothing** — §1B |
| broadcast-worker `broadcast_channel_enqueue_total` | #337's `broadcast_worker_recipient_deliveries_total` is close but counts per publish target | **Add**, one counter — §1C |
| broadcast-worker `broadcast_channel_enqueue_age_seconds` | Absent, and the handler cannot see the message | **Add** + plumbing — §1D |
| `broadcast_channel_enqueue_age_invalid_total{reason}` | Absent | **Add** — free — §1E |
| gatekeeper build→publish diagnostic | Explicitly unscored; #337's `rpc.server.call.duration` already answers the question | **Drop** — §1F |

---

## 1. The instruments

### A. `messages_canonical_published_total{broadcast_path}` — message-gatekeeper · **add**

The shared denominator for all three SLOs.

**Site.** `message-gatekeeper/handler.go`, immediately after the canonical
`PublishMsg` returns without error (~`:404`). Success path only — a rejected
send is not a published canonical message.

**Why the gatekeeper and not broadcast-worker.** This is the whole point of the
instrument. The denominator has to sit **upstream** of the workers it measures,
so that a dead message-worker or broadcast-worker makes the ratio *drop* rather
than making the traffic *disappear*. A self-emitted denominator turns an outage
into missing data — `sli-slo.md` §0.1 names this trap, and §5's search SLO is
already living in it.

**Label derivation is free — no new I/O.** Everything needed is already in hand
at that line:

```go
switch {
case req.ThreadParentMessageID != "" && !req.TShow: // mirrors shouldUseThreadFanOut
    path = "thread"
case sub.RoomType == model.RoomTypeChannel:
    path = "room_subject"
default:
    path = "dm"
}
```

- `sub` comes from `GetSubscription` (`handler.go:376`), which the handler
  already calls and which is cache-absorbed. `model.Subscription` carries
  `RoomType` (`pkg/model/subscription.go:33`).
- `req.ThreadParentMessageID` and `req.TShow` are on the request
  (`pkg/model/message.go:64,71`).

**Do not reach for `GetRoomMeta` to build this label.** The handler deliberately
*skips* that fetch on the thread-reply and owner/admin-bypass paths
(`handler.go:394-396`, "Both bypasses skip the Room fetch entirely"). Adding a
Mongo read to label a counter would put I/O on the send path to measure the send
path. If a future label needs something `sub` does not carry, drop the label.

**The classification must mirror broadcast-worker's dispatch, not the room type.**
`shouldUseThreadFanOut` is `ThreadParentMessageID != "" && !TShow`
(`broadcast-worker/handler.go:220`) and is checked **first**, before the room-type
switch. A channel thread reply with `TShow=false` is a channel room that routes
to per-account thread fan-out, so a `room_type="channel"` denominator would count
a message that never reaches `publishChannelEvent` — depressing SLO-1b and SLO-2
with messages they do not own.

**Cost.** One `Add` with a precomputed option, on the success path only. Three
label values, so three series. No allocation, no I/O, no branch that was not
already evaluated (`isThreadReply` is computed at `:394` today).

---

### B. SLO-1a numerator — message-worker · **already exists, do nothing**

`message_worker_persistence_total{message_kind, result}` already records
`success`/`error` at every persist site, for both the thread and non-thread
paths. Paired with A:

```promql
sum by (site) (rate(message_worker_persistence_total{result="success"}[28d]))
/
sum by (site) (rate(messages_canonical_published_total[28d]))
```

Denominator is the **all-paths** total (persistence covers every message),
unlike 1b/2 which take the `room_subject` slice only.

**Known approximation, already declared.** The counter records one outcome per
*attempt*, so a JetStream redelivery that eventually succeeds counts twice. This
is exactly the "approximate (lag-enforced)" status `sli-slo.md` §0.1 assigns to
every v1 async ratio; the primary enforcement signal remains consumer lag. Making
it exact needs the outcome ledger (P7), not a new counter here.

**The only action is documentation:** `sli-slo.md` §8 should stop listing this as
outstanding work.

---

### C. `broadcast_channel_enqueue_total{result}` — broadcast-worker · **add**

The SLO-1b numerator. `result` is `ok` | `failed`.

**Why #337's counter cannot be used directly**, even though it is very close.
`broadcast_worker_recipient_deliveries_total{room_kind="channel", …}` already
isolates the right messages — `publishChannelEvent` stamps `roomChannel`
(`handler.go:1060`) while `publishToThreadAccounts` stamps `roomThread`
(`:1254`), so thread replies are correctly excluded without any label change.

The problem is the **unit**. `publishRoomEvent` loops over
`subject.RoomEventTargets(...)` (`handler.go:1063`), which returns **one or two**
subjects — a room inside its cross-site flip grace window dual-publishes to the
global and local namespaces (`pkg/subject/subject.go:454`). The delivery counter
increments once per target, so for those rooms the numerator exceeds the
denominator and the ratio goes above 1. An SLI needs **one outcome per logical
message**; loadgen's own recipient observer already makes exactly this
distinction ("treats one global and one local copy of the same logical room
event as one delivery").

**Site.** `publishChannelEvent`, once, after the target loop returns, carrying
the aggregate result. One `Add` per canonical channel message — not per
recipient, not per target. Two series.

**What it does and does not mean.** `nc.PublishMsg` on Core NATS is
fire-and-forget: `ok` means the message entered the local client's send buffer,
not that a server confirmed it. The metric name says `enqueue` for that reason.
A full reconnect buffer *does* return an error synchronously, so that drop is
counted as `failed`; an accepted-then-lost publish during a disconnect is not
countable here at all and is covered by the connection-risk counters
(`chat_nats_client_connected` / `_connection_events_total`, which #337 wired).

---

### D. `broadcast_channel_enqueue_age_seconds` — broadcast-worker · **add, and this is the one with a real cost**

The SLO-2 numerator: age ≤ 1 s is good, later is bad, no upper sanity cap.

**Origin.** `msg.Metadata().Timestamp` — the JetStream metadata store timestamp,
set by the stream-server leader when MESSAGES-CANONICAL persisted the message.
It is deliberately **not** `evt.Timestamp`, which gatekeeper stamps before its
own quote/parent/user lookups and would fold gatekeeper processing time into the
broadcast SLO.

**The 1 s bound is exactly measurable — check this before writing the code.**
Use `o11y.DefaultLatencyBuckets()`, the same set #337 pinned for the RPC
families: `{.005 .01 .025 .05 .1 .25 .5 1 2.5 5 10}`. `1` is a real boundary, so
the numerator is an exact `le="1"` bucket read rather than an interpolation —
the problem that forced SLO-5 from 300 ms to 250 ms does not arise here.

**The cost, stated plainly.** `Handler.HandleMessage(ctx, data []byte)` receives
only bytes (`handler.go:156`); the `jetstream.Msg` stays in the consume loop. And
`msg.Metadata()` is not free — it parses the `$JS.ACK.…` reply subject and
allocates a metadata struct. The repo already treats it as a hot-path cost: the
flow-log block in `broadcast-worker/main.go:436` is gated *specifically* so that
`msg.Metadata()` is skipped when the log level is off, with a comment saying so.

**Plumbing — parse once, in the loop.**

1. In `broadcastProcessor`, call `msg.Metadata()` once and keep the timestamp.
2. Pass it to the handler as an **explicit parameter**, not through `context`. A
   ctx value costs a map allocation on write and an interface assertion on read,
   and this value is required rather than optional — a parameter says that and is
   cheaper.
3. The existing flow-log branch then **reuses** the parsed value instead of
   parsing again. On flow-log-enabled runs this is strictly cheaper than today.

**Three things not to do:**

- **Do not sample the histogram.** SLO-2 is an event ratio over *all* valid
  events; a sampled numerator cannot be divided by a full denominator.
- **Do not derive the age from `evt.Timestamp`** to avoid the parse. That is a
  different, earlier, unbounded origin — the SLO would measure something else.
- **Do not pre-optimize the parse.** Measure it at your target rate first, with
  the same benchmark discipline #337 used. If `Metadata()` shows up, the fallback
  is to pull just the timestamp token out of the reply subject without building
  the struct — but only on evidence.

**Cross-process clock contract.** The age subtracts a stream-server clock from
broadcast-worker's clock. It only means something with NTP/chrony on every node
and skew monitoring alerting above ~100 ms (10% of the bound).

---

### E. `broadcast_channel_enqueue_age_invalid_total{reason}` — broadcast-worker · **add, effectively free**

`reason` is `missing_metadata` | `negative_age`. Two series.

It only increments when the *measurement* is broken — a missing or zero metadata
timestamp, or an age below zero (worker clock behind the stream-server clock).
Zero cost on the normal path.

It is not optional bookkeeping: `sli-slo.md` §2 makes these samples **fail-closed**
— kept in the denominator, excluded from the good numerator — and any nonzero
rate fails a load-test hard gate, because SLO-2 cannot be certified while the
measurement itself is broken. Without this counter that state is invisible.

---

### F. gatekeeper build→publish diagnostic · **drop**

The roadmap asks for a same-process monotonic duration from message-build start
to canonical publish. Reasons not to build it now:

- It is **explicitly unscored** — a diagnostic that explains a bad SLO-2, not an
  SLI.
- #337 already put `rpc.server.call.duration` on the gatekeeper's handler, which
  answers "how long did the gatekeeper take" for the same request.
- It would be a second histogram on the busiest RPC path in the system, to
  explain a number that is already visible.

Revisit only if SLO-2 starts missing *and* the RPC histogram does not explain
why. That is the honest trigger for adding a diagnostic: a question you actually
have and cannot answer.

---

## 2. Cost summary

Per canonical message, with #337's recorder discipline applied (nil-collapse when
`O11Y_ENABLED=false`: 3.27 ns / 0 allocs; warm option lookup ~9.6 ns / 0 allocs).

| Instrument | Fires | Added work per message | New series |
|---|---|---|---|
| A `messages_canonical_published_total` | once per accepted send | 1 counter add, label already computed | 3 |
| B `message_worker_persistence_total` | — | **none, exists** | 0 |
| C `broadcast_channel_enqueue_total` | once per canonical **channel** message | 1 counter add | 2 |
| D `broadcast_channel_enqueue_age_seconds` | once per canonical **channel** message | 1 histogram record **+ one `msg.Metadata()` parse per consumed message** | 1 histogram (11 buckets) |
| E `_age_invalid_total` | only on a broken reading | ~0 | 2 |

**Everything except D's metadata parse is noise** next to what those paths already
do — the gatekeeper's cache-absorbed Mongo reads, broadcast-worker's user lookup
and room-meta read. D's parse is the one line item worth benchmarking, and the
plumbing that enables it makes the existing flow-log path cheaper.

None of these is per-recipient. That is the property that keeps them safe in a
5 000-member room.

---

## 3. What not to add

- **Per-recipient channel enqueue counters.** A channel broadcast is one
  room-subject publish; NATS fans out downstream. #337's comment already makes
  this point, and per-recipient counting on the channel path would be both wrong
  and expensive.
- **Any message, room, account, user or subject identifier as a label.** The
  `.semgrep/metrics.yml` taint rule from #337 blocks it, including through a
  local variable. Exact identifiers belong in the loadgen WAL, never in
  Prometheus.
- **Attribute construction on a recording path.** Same gate. Use the precomputed
  option map, or `optTable` if the label space is wide.
- **A redelivery counter.** #337 deleted `chat.nats.consumer.redeliveries`
  deliberately and the reasoning holds. Do not reintroduce it as a substitute for
  §6.
- **The build→publish diagnostic** (§1F).
- **Sampling on D.**

Every instrument added here must appear in `docs/specs/o11y/nats-metrics-contract.md`
next to *what reads it*, or the `pkg/obs` registry test fails. For these four the
reader is named: the SLO-1a/1b/2 recording rules, and the load-test hard gate.

---

## 4. The resulting queries

```promql
# SLO-1a — persisted / published (all paths)
sum by (site) (rate(message_worker_persistence_total{result="success"}[28d]))
/ sum by (site) (rate(messages_canonical_published_total[28d]))

# SLO-1b — channel enqueue accepted / published on the room-subject path
sum by (site) (rate(broadcast_channel_enqueue_total{result="ok"}[28d]))
/ sum by (site) (rate(messages_canonical_published_total{broadcast_path="room_subject"}[28d]))

# SLO-2 — enqueued within 1 s / published on the room-subject path
sum by (site) (rate(broadcast_channel_enqueue_age_seconds_bucket{le="1"}[28d]))
/ sum by (site) (rate(messages_canonical_published_total{broadcast_path="room_subject"}[28d]))
```

`by (site)` is required for the same reason #337 spelled out for SLO-4/5: a bare
`sum()` lets one healthy site hide another site's total failure.

Note the SLO-2 numerator and denominator come from **different processes**, so
tail-end in-flight messages land after the denominator has been counted. That is
what the load-test settle window exists for; over a 28-day production window it
rounds away.

---

## 5. Order of work

1. **A** — gatekeeper counter. Standalone, unblocks nothing else but is the
   denominator for all three. Smallest diff.
2. **C + E** — broadcast-worker counters. No plumbing needed.
3. **D** — the metadata plumbing and the histogram. Benchmark before and after.
4. Update `sli-slo.md` §8: P1 ✅ (#337), P2 rows for B removed, remaining P2
   narrowed to A/C/D/E.

After 1–3, SLO-1a/1b/2 are computable and the calibration window (Track 1.2) can
start with all of J1 in it.

---

## 6. X9 without advisories — the premise changed

Capturing `$JS.EVENT.ADVISORY.>` needs a stream provisioned on the NATS cluster,
which is the platform team's territory and not available to us. That constraint
matters less than it looks, for three reasons.

**The severity was overstated, and the source doc is stale.**
`failure/nats-jetstream.md` §3 says message-gatekeeper does an "immediate `Nak()`
against `MaxDeliver=5`", so "a short fault can burn the whole delivery budget in
seconds". The code does not do that: `message-gatekeeper/handler.go:212` calls
`jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, …)`, which is
`1s / 5s / 30s / 2m / 10m` (`pkg/jsretry/jsretry.go:51`), and `MaxDeliver`
defaults to **6**, not 5 (`pkg/stream/consumer.go:18`). The client-side budget is
therefore about **12.6 minutes**, and the server-side `BackOff` for an un-acked
message is `{30s, 1m, 2m, 4m, 8m}`. A two-second dependency blip cannot exhaust
either. X9 drops from "highest-severity silent loss path" to "confirm the budget
holds", and the two stale numbers in the failure doc should be corrected.

**What still needs watching, and how to see it without advisories.** An outage
*longer than the budget* does still end in terminal drops. `msg.Metadata()`
carries `NumDelivered`, and §1D's plumbing parses that metadata anyway — so a
bounded counter of "settled at delivery attempt ≥ N" costs nothing extra and
shows how close messages are getting to the cap. It does not prove a drop, and it
must not be sold as proving one; it is a leading indicator, and it is entirely in
our own code. Treat it as **optional**: add it only if the budget check above
shows real headroom pressure, so it does not become another instrument nobody
reads.

**For a load test specifically, the ledger is already the ground truth.**
loadgen's `missing_after_deadline` means admission recorded `good` and the effect
never appeared — that *is* the drop, observed end to end, and it is the strongest
loss claim the evidence model defines. Advisories would add **attribution**
(which consumer dropped it), not detection. So a campaign can reach a verdict
without them; what it must not do is claim advisory-grade completeness. The
failure program already says exactly this: a campaign whose advisory capture was
absent or a plain subscription "may report advisory counts, but must not claim
advisory completeness".

---

## 7. Sibling documents

- [`common/sli-slo.md`](common/sli-slo.md) §2, §8 — the SLO definitions and the roadmap line this spec implements
- [`execution-priority-plan.md`](execution-priority-plan.md) — Track 1.1
- [`extreme-scenarios.md`](extreme-scenarios.md) X9 — the delivery-budget scenario
- `docs/specs/o11y/nats-metrics-contract.md` — where every instrument must be registered
