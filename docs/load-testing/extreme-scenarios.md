# Extreme-Scenario Catalog (code-derived)

> Worst-case load shapes read out of the service code, not out of the workload
> model. The workload model (`common/workload-model.md`) describes *typical*
> production traffic; this document describes what the code does when traffic
> is **not** typical — the shapes that make one user action cost O(N) or O(N²)
> in a dependency.
>
> Every row names the code that produces the amplification, the arithmetic, a
> judgement on whether the behaviour is acceptable, and the test that would
> settle it. Verified against the code on 2026-08-27.

| | |
|---|---|
| **Status** | Draft — for review |
| **Why it exists** | The soak and the first failure round both ran a *realistic* mix. Neither pushed a pathological shape, so no evidence exists for any row below |
| **How to read a row** | `Amplification` is per single user action. `Verdict` is an engineering judgement, not a measurement — the test column is what turns it into one |

---

## 0. The amplification budget, per message

Baseline cost of one message in a channel room of **N** members, from the code:

| Stage | Cost | Code |
|---|---|---|
| message-gatekeeper | 1–3 Mongo reads (all cache-absorbed) + 1 history RPC **iff** quoting or thread reply | `message-gatekeeper/handler.go:376,397,419,432` |
| message-worker (top-level) | 1 unlogged batch over 2 tables | `message-worker/store_cassandra.go` |
| message-worker (thread reply) | batch over 2–3 tables + **full thread-partition scan** + 2 parent UPDATEs | `store_cassandra.go:346` → `pkg/threadcount` |
| broadcast-worker (channel) | **1** room-subject publish (NATS fans out) | `handler.go:1034` |
| broadcast-worker (DM / channel-thread) | **per-recipient** publish loop | `handler.go:1146,1252` |
| broadcast-worker (mention) | 1 Mongo `UpdateMany` over the mentioned set | `handler.go:272` → `store_mongo.go:197` |
| broadcast-worker (room preview) | coalesced — 1 BulkWrite per flush interval, **not** per message | `coalescer.go` |
| notification-worker | 1 `GetMembers` (Valkey→Mongo) + **⌈N/512⌉** presence RPCs + ⌈survivors/100⌉ push publishes | `handler.go:125,244,286`; `presence.go:49` |

The channel broadcast is genuinely O(1) — that part of the design holds. **The
O(N) terms all live in notification-worker, in the mention write, and in the
thread reply path.** Every scenario below is an instance of one of those three.

---

## 1. Ranked scenarios

Rank = blast radius × how plausible the shape is in production × still-unmeasured.

### X1 — Unbounded thread partition, fully re-scanned on every reply · **Cassandra** · 🔴 highest

**What the code does.** `pkg/threadcount.CountAndLatest` issues
`SELECT deleted, created_at FROM thread_messages_by_thread WHERE thread_room_id = ?`
with **no `LIMIT`**, pages at 5000 rows, and walks the entire partition. Its own
comment says so: *"Scans the full partition under scanTimeout, so cost grows with
thread length"*, with a 15 s `context.WithTimeout` described as a *"backstop for
an unbounded partition"* (`pkg/threadcount/count.go:16,33,36`).

It is called on **every reply add** (`message-worker/store_cassandra.go:346`),
**every reply delete** (`history-service/internal/cassrepo/write.go:379`), and
the bot path (`bot-message-worker/store_cassandra.go:250`).

`thread_messages_by_thread` is partitioned by `thread_room_id` **alone** — no
bucket, no cap (CLAUDE.md §Cassandra; soak plan §1 lists "No bucket cap → wide
partition" as the design risk). No service enforces a maximum thread length;
I7's "max 500" is a workload *assumption*, not an enforced limit.

**Amplification.** Reply number K costs a K-row scan. A thread of N replies costs
**Σ K = N²/2 rows read** over its lifetime. Soft-deleted rows are walked too
(`deleted` is a live cell, not a tombstone), so a heavily-edited thread scans more
than its live count.

| Thread length | Rows scanned per new reply | Pages (5000/page) |
|---|---:|---:|
| 50 (I7 p99) | 50 | 1 |
| 500 (I7 max) | 500 | 1 |
| 5 000 | 5 000 | 1 |
| 50 000 | 50 000 | 10 |
| 200 000 | 200 000 | 40 → likely hits the 15 s ceiling |

**Verdict: not acceptable as written.** The 15 s backstop converts an unbounded
partition into a *timeout* rather than into back-pressure, and the timeout fires
inside a JetStream handler with `AckWait=30s`. The path runs in `message-worker`
(`message-worker/store_cassandra.go`), whose `MaxDeliver` is **17**, not the repo
default 6 — `main.go:360` wraps its settings in `stream.WithOutageRetryBudget`.
So a long thread does not degrade, it **terminally drops replies** after
seventeen **deliveries** — the first attempt plus sixteen redeliveries — spanning
roughly two hours, while the stream looks healthy. The longer budget makes the drop slower to arrive, not less total: every
one of those attempts re-runs the same full-partition scan and times out again.

**Documentation is wrong about this.** `soak/cassandra-soak-plan.md` §1 states the
count is *"a bounded client-side scan … tallies live rows to `Cap`(99)"*. There is
no `Cap` in `pkg/threadcount`. A test designed from that sentence under-tests the
real cost by the full thread length.

**Test.** Seed threads of 100 / 1k / 5k / 20k / 100k replies at 0% and 10%
soft-delete density; ramp reply-send against each. Measure reply-add p95/p99,
`cassandra.query.attempts`, coordinator read latency, tombstones-scanned, and the
rate of 15 s scan timeouts → `MaxDeliver` exhaustion (via max-delivery advisories).
Report the thread length at which p99 crosses the SLO bound and the length at
which replies start dropping. Isolated keyspace.

---

### X2 — Large-room notification fan-out · **NATS + presence-service + Mongo/Valkey** · 🔴

**What the code does.** Per canonical message, notification-worker calls
`GetMembers(roomID)` (`handler.go:125`), then `Presence.Snapshot(accounts)`
(`handler.go:244`), which chunks the account list into batches of **512** and
issues one NATS request/reply **per chunk** (`presence.go:49,62,69`). Survivors
are then emitted in batches of **100** (`handler.go:27,286`).

**Amplification** per message in an N-member room:
`⌈N/512⌉` synchronous presence RPCs + `⌈survivors/100⌉` JetStream publishes +
1 member-list read.

| N | Presence RPCs / msg | At 10 msg/s in that room |
|---|---:|---:|
| 500 | 1 | 10 RPC/s |
| 2 000 | 4 | 40 RPC/s |
| 5 000 | 10 | 100 RPC/s |
| 20 000 (all-hands) | 40 | 400 RPC/s |

**The Valkey miss path is the sharp edge.** The member list has *no in-process
tier* — deliberately, to bound per-pod memory (`members.go:24-26`). Every cache
miss is a Mongo query returning N subscription documents, collapsed by
singleflight *per pod*. So a Valkey outage, or a membership change that
invalidates a hot large room, turns every subsequent message in that room into an
N-document Mongo read until the entry repopulates.

**Verdict: plausible and probably the first thing to break.** `max-room-size`
already gates on notification backlog, so the *backlog* signal is covered; what is
**not** measured is the load this puts on presence-service and on Mongo during a
Valkey miss.

**Test.** (a) Re-run `max-room-size` while scraping presence-service RPS and p99 —
the existing verdict does not look at it. (b) Repeat with Valkey stopped (the
resilience variant `environments §6` calls out and nothing measures): expect the
member read to fall through to Mongo on every message. Report the room size at
which presence-service or Mongo, rather than notification backlog, becomes the
limit.

---

### X3 — `@all` in a large room · **MongoDB** · 🟠

**What the code does.** `SetSubscriptionMentions` is a single `UpdateMany` over
`{roomId, u.account ∈ accounts, lastSeenAt not ≥ msgCreatedAt}`
(`broadcast-worker/store_mongo.go:197-205`), called on create (`handler.go:272`)
and again on **every edit** of a mentioning message (`handler.go:399`).

**Amplification.** For `@all`, the resolved account set is the whole room: one
message = up to N subscription-document writes. An edit repeats it. This is the
`end-to-end-plan.md` T1-5 item, still marked unasserted.

| Room size | Sub docs written per `@all` | 1 `@all`/s |
|---|---:|---:|
| 100 | 100 | 100 writes/s |
| 2 000 | 2 000 | 2 000 writes/s |
| 5 000 | 5 000 | 5 000 writes/s |

**Verdict: acceptable shape, unvalidated magnitude.** `UpdateMany` is the right
primitive and the `lastSeenAt` predicate trims it; the open question is whether
the index supports the filter at that width and what it does to the oplog and
WiredTiger cache. The edit path doubling the cost is worth confirming is intended.

**Test.** Mention-storm: `@all` at 0.2 / 1 / 5 per second into rooms of 500 /
2 000 / 5 000, with an edit-follow-up share matching I4. Measure Mongo write p99,
`db.client.operation.duration`, oplog throughput, and whether the filter uses the
`(roomId, u.account)` index. Mongo may be driven to capacity (environments §3).

---

### X4 — Key-rotation storm on member removal · **room-service + Mongo** · 🟠

**What the code does.** Removing a member rotates the room key
(`room-worker/handler.go:419,629` → `roomkeystore.CommitRotation`). Every
remaining member's cached key is now the previous version, so the next message
they receive is encrypted under a version they must fetch — an N-way `key.get`
burst against room-service.

The retention window is `ROOM_KEY_RETIRED_TTL` (default **20 m**,
`room-service/main.go:60`), which must be ≥ 2× broadcast-worker's
`ROOM_KEY_CACHE_TTL` (default **10 m**, `broadcast-worker/main.go:71`);
broadcast-worker fails fast at startup if not (`main.go:301`).

**Amplification.** One membership removal in an N-member room = up to N `key.get`
RPCs, arriving within one message-delivery window. Removing a departing employee
from 30 rooms during business hours multiplies that by 30.

**Verdict: the mechanism is sound, the burst is unmeasured.** The
retired-key TTL invariant is enforced in code, which is good; what is unknown is
room-service's `key.get` ceiling and whether a rotation in a 5 000-member room
during peak send causes decrypt failures at the tail.

**Test.** Under sustained send in a large room, remove one member; measure the
`key.get` RPS spike, room-service p99, and any client-visible decrypt failure.
Ramp room size. Then repeat with `ROOM_KEY_RETIRED_TTL` set deliberately short to
confirm the failure mode is the documented one (permanent `key.get` failure for
messages already on the wire), not something worse.

---

### X5 — Morning login / reconnect storm · **history-service + Cassandra** · 🟠

**What the code does.** `subscription.list` enriches with the last message **by
default** — `withLastMsg := req.IncludeLastMessage == nil || *req.IncludeLastMessage`
(`user-service/service/subscriptions.go:87`). Enrichment fans out to
`rooms.get` in chunks of `ROOM_BATCH_CHUNK` (**100**, hard-capped by
history-service) with at most `MAX_SITE_FANOUT` (**8**) concurrent RPCs *per
request* (`user-service/config/config.go:75,83`; `subscriptions.go:535`). Each
resolved preview is a Cassandra read.

**Amplification.** Per user opening the app with M subscribed rooms:
`⌈M/100⌉` history RPCs, each resolving up to 100 previews. The concurrency bound
is **per request** — nothing bounds the aggregate when U users do it at once.

| Users reconnecting | M (daily-power ≈ 83) | history RPCs in the burst | Preview reads |
|---:|---:|---:|---:|
| 1 000 | 83 | 1 000 | 83 000 |
| 5 000 | 83 | 5 000 | 415 000 |
| 5 000 | 500 (power/bot-heavy) | 25 000 | 2 500 000 |

**Verdict: the most likely real production incident on this list.** It is a
*normal* daily event (09:00 login peak, or a gateway blip reconnecting everyone
at once), it is unbounded in aggregate, and **no loadgen mode drives it** —
`daily` ramps N with a warm-up, which is the opposite of a cold-start burst, and
`presence-storm` reconnects presence only, not the initial-data legs.

**Test.** New scenario: cold-start storm. U users each issue
`subscription.list` + the resulting `rooms.get` fan-out inside a short window;
ramp U through 500 / 1k / 2k / 5k. Measure history-service p99, Cassandra read
latency, NATS request/reply timeouts, and the `includeLastMessage=false` A/B
(the soak already exposes `SOAK_SUBSCRIPTION_LIST_INCLUDE_LAST_MESSAGE` to
isolate exactly this fan-out). This is also the only realistic way to exercise
SLO-3's declared journey (auth → connect → initial data) end to end.

---

### X6 — Bucket window vs TWCS window, and hot-room partition size · **Cassandra** · 🟠

**Two separate problems, same knob.**

**(a) The deployed compaction window may not match the code.**
`MESSAGE_BUCKET_HOURS` defaults to **360** in every service
(`message-worker/main.go:46`, `history-service/internal/config/config.go:53`,
`bot-message-worker/main.go:37`). The init DDL matches at
`compaction_window_size: '360'`
(`docker-local/cassandra/init/10-table-messages_by_room.cql:41`) — but the
migration that introduced TWCS pins **72**
(`docker-local/cassandra/migrations/2026-05-twcs-message-tables.cql:40`), and
`soak/cassandra-soak-plan.md` §D still documents 72 as the live value with line
references that no longer resolve. A cluster built from the migration and never
re-`ALTER`ed runs a 72 h compaction window under 360 h buckets: **one partition
spans five compaction windows**, so a single-bucket read touches five SSTable
groups instead of one.

**(b) The bucket is a fixed *time* window, so partition size scales with room
rate — with no row cap.** 360 h is 15 days.

| Room send rate | Rows in one 15-day partition |
|---|---:|
| 100 msgs/day (I6 hot) | 1 500 |
| 1 msg/s | 1.3 M |
| 10 msg/s (busy announcement channel) | 13 M |

The Cassandra guideline in the soak plan's own appendix is ~100 MB / ~100 k rows.
A room busier than roughly **7 messages/minute sustained** exceeds that within one
bucket, and nothing in the write path notices.

**Verdict: (a) is a preflight check that should have blocked the soak; (b) is a
real design limit that the realistic soak could not surface**, because the soak
spreads 100 msg/s across a whole room set by Zipf — no single room gets near it.

**Test.** (a) Query the deployed `compaction_window_size` on staging and prod and
compare to `MESSAGE_BUCKET_HOURS` — a config assertion, not a load test; add it to
the pre-run gate. (b) Hot-partition ramp: concentrate 1 / 5 / 10 / 50 msg/s into a
**single** room (loadgen already supports this: `max-room-size --rooms-per-size=1`)
and hold long enough to fill a bucket; measure partition size, SSTables-per-read,
read p99 on `LoadHistory`, and per-node skew. Isolated keyspace (F5).

---

### X7 — Reaction MAP width · **Cassandra** · 🟡

**What the code does.** Each reaction is `UPDATE … SET reactions[?] = ?` across
**three** tables, each removal a `DELETE reactions[?]`
(`history-service/internal/cassrepo/reactions.go:18-24`). Reaction is a *toggle*
at the API layer, so an ambiguous retry reverses intent.

**Amplification.** A message with R distinct reaction keys carries an R-entry MAP
that is read in full whenever the message is read. Each removal leaves a
collection tombstone retained for `gc_grace_seconds` (default 10 days). I8 puts
the max at 500 per message — and I8's meaning is still one of the flagged
unconfirmed inputs.

**Verdict: bounded and low-frequency, but the tombstone accumulation is real.**
The soak plan already scopes this as F4; it has never run.

**Test.** Soak plan F4, isolated keyspace: build messages with 30 / 500 / 5 000
reaction keys, cycle add/remove at a realistic per-user cadence, measure row
bytes, collection tombstones scanned per read, and the read-latency curve against
MAP width.

---

### X8 — Cross-site membership burst through the ordered FIFO lane · **NATS federation** · 🟡

**What the code does.** `member_added` / `member_removed` / `room_renamed` ride a
per-destination consumer with `MaxAckPending = 1`
(`outbox-worker/main.go:275`; `pkg/outbox.OrderedEventTypes`), so the server
releases event N+1 only after N is acked. Throughput per peer is `1 / RTT`.

**Amplification.** Adding M members to a room with a remote peer serializes M
forwards. SLO-9 is *forwarded within 30 s*.

| Peer RTT | Max forwards/s | Members forwardable inside SLO-9's 30 s |
|---:|---:|---:|
| 5 ms | ~200 | ~6 000 |
| 20 ms | ~50 | ~1 500 |
| 100 ms | ~10 | ~300 |

**Verdict: correct by design, but the SLO may be arithmetically unreachable for a
bulk membership change on a distant peer.** The isolation property (a down peer
parks only its own lane) is the right trade; the throughput consequence has never
been measured because loadgen is single-site.

**Test.** Needs the two-site topology that SLO-9 needs anyway (D4 in the priority
plan). Bulk-add 100 / 1 000 / 5 000 members to a room with remote members;
measure per-event forward latency distribution against the 30 s bound at the real
inter-site RTT.

---

### X9 — Delivery-budget exhaustion under a long outage · **NATS** · 🟢 largely mitigated

**Corrected after reading the code.** An earlier revision of
`failure/nats-jetstream.md` §3 said message-gatekeeper does an "immediate `Nak()`
against `MaxDeliver=5`", so "a short fault can burn the whole delivery budget in
seconds" — **that document is fixed in this branch**, and the code never did
that. `message-gatekeeper/handler.go:212` calls
`jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, …)` — `1s / 5s / 30s / 2m / 10m`
(`pkg/jsretry/jsretry.go:51`) — and `MaxDeliver` defaults to **6**, not 5
(`pkg/stream/consumer.go:20`). The client-side budget is therefore about
**12.6 minutes**; the server-side `BackOff` for a message that goes un-acked is
`{30s, 1m, 2m, 4m, 8m}`. A two-second dependency blip cannot exhaust either.

`pkg/jsretry`'s package comment exists precisely because a bare `Nak()` would do
what the failure doc describes, and CLAUDE.md forbids it repo-wide.

**What is left.** An outage *longer than the budget* still ends in terminal
drops, and those are still invisible from consumer pending alone.

**Verdict: verify the budget, do not hunt the drop.** The residual question is
arithmetic — does the configured budget exceed the longest outage you intend to
survive — not instrumentation.

**Test — and it has to be split per consumer, because they have different budgets
and different oracles.** An earlier revision of this section prescribed one
outage "longer than 12.6 minutes" and one expected result. That does not work:
12.6 minutes is *the gatekeeper's* budget, and if the gatekeeper exhausts, the
message was never admitted — so nothing downstream can report it missing *after*
a successful admission. Meanwhile the two consumers that are downstream need
roughly two hours to exhaust anything.

| Inject at | Budget to exceed | Oracle |
|---|---|---|
| **message-gatekeeper** (`MaxDeliver=6`) | ~12.6 min client-side | `chat_nats_terminal_failures_total{reason="max_deliver"}` on the gatekeeper, plus an **admission failure** — the send never becomes a canonical message. Do **not** expect a downstream `missing_after_deadline` for an admission that never happened |
| **message-worker** (`MaxDeliver=17`) | ~2 h — read the live value first | Absence in Cassandra for messages that *were* admitted, plus the terminal counter on message-worker |
| **broadcast-worker** (`MaxDeliver=18`) | ~2 h — read the live value first | Recipient absence for messages that *were* admitted, plus the terminal counter on broadcast-worker |

The two-hour budgets make this an expensive test. Run the gatekeeper leg first:
it is twelve minutes and it exercises the same mechanism.

**Three signals, three different jobs — do not treat any one as "the only way".**

| Signal | Covers | Blind to |
|---|---|---|
| **`chat_nats_terminal_failures_total{reason="max_deliver"}`** (`pkg/natsmetrics`, on `main` today) | The handler returned an error on its **final** delivery — the ordinary exhaustion path | Anything where the handler never completes: pod crash, OOM, hang, an `AckWait` expiry |
| **loadgen's ledger** (`missing_after_deadline`) | End-to-end absence: the message never arrived, whatever killed it | Which consumer dropped it, and anything outside the run |
| **Max-delivery advisories** | Completeness and **attribution** — including un-instrumented consumers and the un-acked paths the app counter cannot see | Nothing, but the stream is the platform team's to provision and is not available to us |



### X10 — Sparse/aged room history walk · **Cassandra** · 🟡

**What the code does.** The bucket walker widens its wave adaptively over sparse
runs (`adaptiveWaveWidth`, `history-service/internal/cassrepo/walker.go:286`) and
stops at a `maxBuckets` budget (`:243`). With 360 h buckets, a two-year-old room
with little traffic spans ~48 buckets.

**Amplification.** A cold room's first page may exhaust the `maxBuckets` budget
before filling `pageSize`, returning a short page and forcing extra client round
trips — each one re-walking.

**Verdict: mitigated by design (the adaptive wave is exactly the fix), but never
measured against aged data.** The soak explicitly cannot see this: *"no historical
backfill in v1 … reads only hit run-generated data"* (soak plan blind spot 11), so
every read in the soak walked one shallow, dense bucket.

**Test.** Seed rooms with a **1-year sparse span** (100 messages over 12 months —
the existing `history-large` preset is 30 days and dense) and ramp `LoadHistory`
with `--before-mode=scrollback`. Read the `bucket-walk depth` block loadgen
already reports. Compare short-page rate and p99 against the dense preset.

---

## 2. What this changes about the test plan

1. **X1 and X5 are new top-priority items** — neither is covered by any existing
   loadgen mode, and both are O(N) paths reachable by ordinary user behaviour.
2. **X2, X4, X6b are extensions of modes that already exist** (`max-room-size`
   with `--rooms-per-size=1`, plus extra scrape targets) — cheap to add.
3. **X6a is a config assertion, not a load test.** It belongs in the pre-run gate
   and should have blocked the soak that already ran; the soak's compaction and
   SSTables-per-read evidence is only interpretable once the deployed window is
   known.
4. **X8 stays blocked** on the two-site topology; **X9 is largely mitigated** —
   the delivery budget is minutes, not milliseconds, and the failure doc's
   numbers are stale.
5. **Three documentation defects surfaced** and are listed in the priority plan's
   corrections section: the `threadcount` Cap(99) claim, the soak plan's 72 h
   `MESSAGE_BUCKET_HOURS` references, and the stale TWCS migration file.

## 3. Sibling documents

- [`execution-priority-plan.md`](execution-priority-plan.md) — where these land in the schedule
- [`common/sli-slo.md`](common/sli-slo.md) — acceptance criteria
- [`soak/cassandra-soak-plan.md`](soak/cassandra-soak-plan.md) §5 — F1–F6 pathological experiments (X1, X6b, X7 refine F2, F5, F4)
- [`failure/overview.md`](failure/overview.md) — fault campaigns (X9)
