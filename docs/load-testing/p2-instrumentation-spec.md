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
| **Net change** | **2 instruments to add, 1 label to widen, 2 already exist, 1 to drop.** Only one addition has a measurable hot-path cost, and it is bounded and plumbing-shaped, not per-recipient. **SLO-1a is computable today with no code change at all** — see §1A |
| **Not in scope** | Anything that needs a JetStream advisory consumer — see §6 |

---

## 0. What P2 actually needs, after reading the code

The roadmap line (`common/sli-slo.md` §8 P2) reads as five pieces of work. Two of
them are already done and one should be dropped.

| Roadmap piece | Reality | Action |
|---|---|---|
| gatekeeper `messages_canonical_published_total{broadcast_path}` | **The counter already exists** as `message_gatekeeper_messages_total{result="accepted"}` (`message-gatekeeper/nats_metrics.go:56`, recorded at `handler.go:218` after the canonical publish). Only the `broadcast_path` slice is missing | **Widen the label** (or add a small dedicated counter) — §1A |
| message-worker persisted numerator | **Already exists** as `message_worker_persistence_total{message_kind,result}` (`message-worker/nats_metrics.go:46`, recorded at `handler.go:190,193,204,207`) | **Nothing** — §1B |
| broadcast-worker `broadcast_channel_enqueue_total` | #337's `broadcast_worker_recipient_deliveries_total` is close but counts per publish target | **Add**, one counter — §1C |
| broadcast-worker `broadcast_channel_enqueue_age_seconds` | Absent, and the handler cannot see the message | **Add** + plumbing — §1D |
| `broadcast_channel_enqueue_age_invalid_total{reason}` | Absent | **Add** — free — §1E |
| gatekeeper build→publish diagnostic | Explicitly unscored; #337's `rpc.server.call.duration` already answers the question | **Drop** — §1F |

---

## 1. The instruments

### A. The J1 denominator — message-gatekeeper · **already exists; it needs one more label**

**The counter is already there.** `message_gatekeeper_messages_total{result, reason}`
records `resultAccepted` at `message-gatekeeper/handler.go:218` — after the
canonical `PublishMsg` succeeded and the client reply was sent. That is exactly
"a canonical message was accepted", which is the J1 denominator.

**Consequence: SLO-1a is computable today, with no code change.** See §1B and §4.

**What is missing is only the fan-out slice.** SLO-1b and SLO-2 count the
`room_subject` path only, and the counter has no `broadcast_path` label.

**Label derivation is free — no new I/O.** Everything needed is in hand at that
line:

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

- `sub` comes from `GetSubscription` (`handler.go:376`), already called and
  cache-absorbed; `model.Subscription` carries `RoomType`
  (`pkg/model/subscription.go:33`).
- `req.ThreadParentMessageID` and `req.TShow` are on the request
  (`pkg/model/message.go:64,71`).

**Do not reach for `GetRoomMeta` to build this label.** The handler deliberately
*skips* that fetch on the thread-reply and owner/admin-bypass paths
(`handler.go:394-396`, "Both bypasses skip the Room fetch entirely"). Adding a
Mongo read to label a counter would put I/O on the send path in order to measure
the send path.

**The classification must mirror broadcast-worker's dispatch, not the room type.**
`shouldUseThreadFanOut` is `ThreadParentMessageID != "" && !TShow`
(`broadcast-worker/handler.go:220`) and is checked **first**, before the room-type
switch. A channel thread reply with `TShow=false` is a channel room that routes to
per-account thread fan-out, so a `room_type="channel"` denominator would count a
message that never reaches `publishChannelEvent` — depressing SLO-1b and SLO-2
with messages they do not own.

**Two ways to carry the label; pick one deliberately.**

| | Widen the existing counter | Add `messages_canonical_published_total{broadcast_path}` |
|---|---|---|
| New instruments | 0 | 1 |
| Series | `accepted` currently pairs only with `reason="none"`, so adding 3 paths turns 1 series into 3 — total goes 32 → 34 | 32 unchanged + 3 |
| Semantics | One counter carries a business outcome *and* a routing dimension | Business outcomes stay one counter; the SLO denominator is a single-purpose series |
| Registry entry | Amend the existing one | One new entry naming its reader |

Recommendation: **widen the existing counter** unless a reviewer objects to mixing
the two concerns. It adds no instrument, the cardinality delta is two series, and
the SLO queries filter on `result="accepted"` anyway. Build the label only for the
`accepted` key — the other results have no fan-out route and must not gain one.

**Cost either way.** No extra work at all: the counter is already incremented on
this line, and the label value comes from data already in registers. The only
change is which precomputed option is looked up.

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
outstanding work — and §1 should move SLO-1a from 🔧 to ✅ (approximate), because
paired with §1A's existing denominator the ratio is computable on `main` today.

**One filter is mandatory.** message-worker also persists `system` and
`teams_migration` messages, which reach MESSAGES-CANONICAL from history-service
mutations and room-worker system events rather than through the gatekeeper. Without
`message_kind=~"user|thread_reply"` the numerator counts messages the denominator
never saw.

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
| A J1 denominator | already incremented per accepted send | **none** — one extra label value on an existing add | +2 |
| B `message_worker_persistence_total` | — | **none, exists** | 0 |
| — SLO-1a as a whole | — | **none — computable today** | 0 |
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
# SLO-1a — persisted / published.  WORKS TODAY, no code change.
sum by (site) (rate(message_worker_persistence_total{
      message_kind=~"user|thread_reply", result="success"}[28d]))
/ sum by (site) (rate(message_gatekeeper_messages_total{result="accepted"}[28d]))

# SLO-1b — channel enqueue accepted / published on the room-subject path
sum by (site) (rate(broadcast_channel_enqueue_total{result="ok"}[28d]))
/ sum by (site) (rate(message_gatekeeper_messages_total{
      result="accepted", broadcast_path="room_subject"}[28d]))

# SLO-2 — enqueued within 1 s / published on the room-subject path
sum by (site) (rate(broadcast_channel_enqueue_age_seconds_bucket{le="1"}[28d]))
/ sum by (site) (rate(message_gatekeeper_messages_total{
      result="accepted", broadcast_path="room_subject"}[28d]))
```

`by (site)` is required for the same reason #337 spelled out for SLO-4/5: a bare
`sum()` lets one healthy site hide another site's total failure.

Note the SLO-2 numerator and denominator come from **different processes**, so
tail-end in-flight messages land after the denominator has been counted. That is
what the load-test settle window exists for; over a 28-day production window it
rounds away.

---

## 5. Order of work

0. **Nothing** — write the SLO-1a recording rule against the two counters that
   already exist (§4) and start its calibration window immediately. This is a
   Grafana/rules change, not a code change, and it puts a third of J1 into
   observation weeks before the rest.
1. **A** — widen the gatekeeper label. Smallest diff; unblocks 1b and 2's
   denominator.
2. **C + E** — broadcast-worker counters. No plumbing needed.
3. **D** — the metadata plumbing and the histogram. Benchmark before and after.
4. Update `sli-slo.md`: §1 marks SLO-1a ✅ (approximate), §8's P1 row is ✅ (#337),
   the P2 row loses B and narrows to A/C/D/E.

Step 0 matters for scheduling: the calibration window (Track 1.2) does not have to
wait for steps 1–3 to begin.

---

## 5b. The actual diff, file by file

Verified call sites, so the change can be estimated rather than guessed.

### A — the `broadcast_path` label · `message-gatekeeper`

| | |
|---|---|
| Files | `handler.go`, `nats_metrics.go` |
| Size | ~30 lines |
| Hot-path cost | none — the counter is already incremented on this line |

**The label must be computed inside `processMessage`, not at the `Record` call.**
`resultAccepted` is recorded at `handler.go:218`, in `HandleJetStreamMsg`. Two of
the three inputs (`req.ThreadParentMessageID`, `req.TShow`) are in scope there,
but the two that decide the answer are not:

- `sub.RoomType` is fetched inside `processMessage` (`handler.go:376`).
- **`TShow` must be the normalized value.** `processMessage` computes
  `tshow := req.TShow && req.ThreadParentMessageID != ""` (`handler.go:455`) and
  it is that value which lands on the message and which
  `shouldUseThreadFanOut` reads. Using the raw `req.TShow` would misclassify a
  `tshow=true` send that carries no thread parent.

So: compute the path in `processMessage` next to the built `msg`, and return it —
`processMessage(...) ([]byte, broadcastPath, error)`. There is exactly one call
site (`handler.go:176`), so the signature change is contained. `Record` then takes
the path and applies it **only to the `resultAccepted` key**; the other three
results have no fan-out route and must not gain the label.

*(Alternative: record the accepted outcome inside `processMessage` right after the
publish succeeds. That is arguably more correct — "accepted" would then mean
published rather than published-and-replied — but it moves the call site, and
`sendReply` returns no error today, so nothing observable changes. Not worth the
churn.)*

### C — `broadcast_channel_enqueue_total{result}` · `broadcast-worker`

| | |
|---|---|
| Files | `handler.go`, `nats_metrics.go` |
| Size | ~25 lines |
| Hot-path cost | one counter add per canonical channel message |

`publishChannelEvent` (`handler.go:1034`) currently ends with
`return h.publishRoomEvent(...)`. Capture that error, record once, return it. That
is the whole change.

**The denominator match is exact, and this is worth checking rather than
assuming.** `publishChannelEvent` has **one caller** — `handleCreated`
(`handler.go:285`). Mutations (edit, delete, pin, react) reach the room subject
through `publishMutation` → `publishRoomEvent`, bypassing `publishChannelEvent`
entirely. So a counter placed here counts exactly "created channel messages on the
room-subject path", which is precisely SLO-1b's denominator. No filtering, no
event-type label needed.

### D + E — the age histogram · `broadcast-worker`

| | |
|---|---|
| Files | `main.go`, `handler.go`, `nats_metrics.go` |
| Size | ~60 lines plus a benchmark |
| Hot-path cost | one `msg.Metadata()` parse per consumed message |

1. `broadcastProcessor` (`main.go:427`) parses `msg.Metadata()` once and keeps
   `meta.Timestamp`. The existing flow-log branch (`main.go:436`) then **reuses**
   it instead of parsing again.
2. **Carry it in the context value that already exists.** `HandleMessage` already
   does two `context.WithValue` calls per message
   (`obs.ContextWithIdentity`, `withBroadcastMetricLabels`), so a third would be
   out of step — instead add a `streamTS time.Time` field to the existing
   `broadcastMetricLabels` struct. It is already a ctx value, so a bigger struct
   costs no additional allocation. The three re-stamping call sites
   (`handler.go:665, 887, 1060`) must preserve it rather than zeroing it — that is
   the one thing a test should pin.
3. `publishChannelEvent` records `now − streamTS` into the histogram, or
   increments `_age_invalid_total{missing_metadata|negative_age}` when the reading
   is broken.

Threading it as an explicit parameter would touch `HandleMessage`,
`handleCreated` and `publishChannelEvent` signatures; the ctx-field approach
touches one struct and three call sites. Prefer the latter **only because the ctx
value already exists on this path** — do not add a new one.

---

## 5c. Which PR

**Not #337.** That PR is 67 files, +4 435 / −912, 61 commits, 74 comments, and has
been in review for a week with `mergeable_state: blocked`. Adding a new
instrument family to it resets that review for everyone, and P2 is a different
question from "cut the metrics down to what is read".

**A new PR, based on #337's branch.** Two reasons to stack rather than branch from
`main`:

- #337 **moves the metrics contract** from
  `docs/load-testing/failure/nats-metrics-contract.md` to
  `docs/specs/o11y/nats-metrics-contract.md`. A P2 branch off `main` would
  document its four instruments in the old path and conflict on merge.
- #337 adds the guards P2 has to satisfy anyway — the `pkg/obs` registry test,
  the `.semgrep/metrics.yml` cardinality and attribute-construction rules, and the
  nil-collapse recorder pattern. Stacking means they run against P2 from the first
  commit instead of after the rebase.

If #337 merges first, rebase onto `main` and nothing else changes.

**Split it in two, by cost and by risk:**

| PR | Contents | Why separate |
|---|---|---|
| **P2a** | A + C + E | Zero hot-path cost, ~55 lines, no plumbing. **Delivers SLO-1b outright.** Reviewable in one sitting |
| **P2b** | D | Needs the metadata plumbing, a ctx-struct change, and a before/after benchmark. **Delivers SLO-2.** Its review is about the cost, not the metric |

Shipping P2a alone is a real milestone: J1 goes from one enforceable SLO to two.

---

## 5d. What happens if P2 is never built

Be precise about this — the cost is **not** "we cannot see problems". Several
signals survive.

**What you keep without P2:**

- `broadcast_worker_recipient_deliveries_total{room_kind="channel", result="failed"}`
  — publish errors on the channel lane are already counted.
- Consumer lag on broadcast-worker's MESSAGES-CANONICAL durable — which
  `sli-slo.md` §0.1 names the **primary** enforcement signal for every async SLO,
  precisely because the v1 ratios are approximate.
- `chat.nats.client.connected` / `_connection_events_total` — the connection-risk
  backstop for the core-NATS hop.
- loadgen's one-sided bound in a test window (`slo-measurement-map.md` §7).

**What you lose, specifically:**

1. **No error budget for J1's delivery half.** Burn-rate alerting (`sli-slo.md`
   §7) is defined on event ratios. An error counter with no denominator cannot
   produce one, so SLO-1b and SLO-2 can never be alerted on the way every other
   SLO is. They stay 🔧 permanently, and J1 ships with one of its three SLOs
   enforceable.
2. **SLO-2's failure mode has no proportional signal.** Consumer lag reports
   **queue depth, not age**. `sli-slo.md` §6 already makes this exact argument for
   federation: growth alone is insufficient, because a lane can park a small,
   constant backlog forever and look stable. The same trap applies here — a
   broadcast-worker that is *steadily* a minute behind shows a flat, non-zero
   pending and no alert, while every user sees late messages. Age is the signal
   that distinguishes "slow" from "backed up", and nothing else produces it.
3. **A delivery incident is not attributable from metrics.** When messages stop
   arriving, the question is "did broadcast-worker fail to enqueue, or did the
   delivery lane drop them?" Without the enqueue ratio both look identical from
   the dashboards, and the investigation falls back to logs.
4. **Calibration for 1b/2 is pushed out by a full window.** §0.2 asks for 4–6
   weeks of observation before targets are set. That clock cannot start until the
   counters exist, so deferring P2 by N weeks delays *enforceable* J1 SLOs by
   N + 6.
5. **Every load-test verdict on 1b/2 stays one-sided.** A pass is conclusive; a
   miss is unattributable. Good enough for the first achievability run
   (`slo-measurement-map.md` §7), not good enough for a release gate.

**The honest summary:** without P2 you can still tell that something is wrong on
the J1 delivery path. You cannot say *how often*, cannot set a budget, cannot
page on the burn rate, and cannot separate a slow worker from a backed-up one.

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

- [`slo-measurement-map.md`](slo-measurement-map.md) — every journey's path, hop by hop, and which instrument sits where
- [`common/sli-slo.md`](common/sli-slo.md) §2, §8 — the SLO definitions and the roadmap line this spec implements
- [`execution-priority-plan.md`](execution-priority-plan.md) — Track 1.1
- [`extreme-scenarios.md`](extreme-scenarios.md) X9 — the delivery-budget scenario
- `docs/specs/o11y/nats-metrics-contract.md` — where every instrument must be registered
