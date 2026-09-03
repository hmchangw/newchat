# Availability & Journey-Performance Audit

**Repo:** `hmchangw/newchat` · **Date:** 2026-09-01 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 9 independent expert agents on two axes the per-service audit structurally could not see — 4 availability lenses (retry amplification, head-of-line blocking, backpressure, blast radius) and 5 journey lenses (send, app-open, history, federation, capacity). Every agent was instructed to **discard** any finding a single-service read would have produced.
**Companion:** `fleet-readiness-2026-08-31.md` (35 services, 210 experts, 1,441 findings, 0 `critical` on performance).

---

## The question that prompted this

> *"Frequent retries will block NATS and bring down the whole system."*

**The outcome is confirmed. The mechanism is not — and the difference is the whole point.**

Retries never saturate NATS. A delayed NAK is one small control message; the redelivery is a message the broker was already storing; no service raises subscription pending limits; and channel fan-out is **one publish per room, not per member** (`broadcast-worker/handler.go:1072-1083`), which removes the obvious broker-saturation path. The broker keeps running.

What saturates is **software admission budget**, in two tiers:

1. **`MaxAckPending` = 1000 per durable consumer.** A NAK-with-delay keeps the message *in the consumer's pending set for the whole delay* — the premise the repo already relies on elsewhere (`outbox-worker/main.go:117-122`). Retries therefore consume the budget rather than the broker.
2. **`MAX_WORKERS` = 100 worker slots per pod**, and below that a Valkey pool of `5 × GOMAXPROCS` — **~10 connections on a 2-CPU pod.**

So "the system goes down" is right, and it goes down at a layer that is far more fixable than the broker. Three of the four availability experts reached this independently.

---

## The single worst case

**`MESSAGES` has exactly one consumer, so `message-gatekeeper`'s 1000-slot budget *is* the site's entire message-send capacity** — verified: nothing else in the repo binds `stream.Messages`.

It runs plain `DurableConsumerDefaults` (`main.go:272-275`) — `MaxDeliver=6`, **no outage budget** — and two bare-error paths NAK: the subscription read with the Mongo breaker open, and a canonical publish failure. Its infra path NAKs and `return`s **without replying to the client** (`handler.go:228-234`).

The sequence, at ~100 failing sends/sec:

| t | state |
|---|---|
| 0 s | Mongo breaker opens; sends begin NAKing with delay |
| ~10 s | 1000-slot budget full. **The server delivers nothing further on this consumer.** |
| 10 s+ | Clients keep publishing to MESSAGES *successfully* and simply never receive a reply. No error is surfaced. |
| 6.3 min | Each parked send is dropped permanently. The sender is never told. |

Recovery is automatic **only if the dependency returns inside 6.3 minutes**, and is then floored by the 10-minute repeating backoff tail — so a pipeline can stay dead for up to 10 minutes *after* the dependency is healthy.

---

## The amplifying loop

`history-service` defaults to `REQUEST_TIMEOUT=10s`. All three of its hot-path callers request with a hardcoded **2s** — `broadcast-worker/parent_fetcher.go:19`, `message-gatekeeper/fetcher_history.go:19`, `room-service/reader_history.go:18`. *(All four values verified.)*

NATS request/reply carries no cancellation to the responder. So:

- At 2 s the caller abandons; **history-service keeps its admission slot and a Mongo/Cassandra connection for 8 more seconds doing work nobody will read.**
- Effective capacity collapses from `256/2s` to `256/10s ≈ 25 req/s`. Everything above sheds as `service busy`.
- `broadcast-worker` classifies that shed as *non-terminal* and NAKs on `LowLatencyBackoff`, with `WithOutageRetryBudget` sizing `MaxDeliver` to ride out a **one-hour** window — **~18 deliveries, the first five inside 78 seconds.**

**Offered load rises as capacity falls.** That is an outage, not a brownout, and it does not self-limit.

It is made worse by a two-hop correlated failure that the intended escape hatch cannot catch: when the gatekeeper soft-fails parent resolution it publishes the canonical event *without* the parent fields — which is exactly the condition that makes `broadcast-worker` and `notification-worker` each issue *their own* 2 s fetch against the same dead service. `errcode.Terminal` returns false for `Unavailable`, so the `parentResolveExhausted` Ack-drop added for this purpose never fires.

---

## Verdict by axis

| Axis | Verdict |
|---|---|
| **Availability** | **Serious.** The synchronous fabric is built to **queue, not shed**: the admission cap sits *above* the resource it protects in every guarded service but one. Four `critical` and one site-wide silent-drop path. |
| **Journey performance** | **Bounded but uneven.** The warm steady-state path is genuinely fast (~10 ms p50 send, one publish per room regardless of size). Every serious finding is a *cliff* — thread replies, cold rooms, one slow peer, a cold cache — not steady-state slowness. |
| **Capacity** | **A hard per-site ceiling of ~3,300–4,000 msg/s** that **adding pods does not raise**, and every hot-path worker stops gaining from replicas at about **5 pods**. |

## What was verified by hand

Every claim reproduced in this report was re-read in the source before inclusion. Specifically confirmed: the `inbox-worker` blocking send and its single drainer; `history-service`'s 10s guard against three 2s callers; `message-gatekeeper`'s plain consumer defaults and reply-less NAK; `MESSAGES` having one consumer; `Duplicates` never set in `pkg/stream`; `InboxDedupID` omitting the event type; `threadcount` scanning with no `LIMIT`; the double LWT re-stamp; `SerialConsistency` unset repo-wide; `previewWriter.Flush` swapping before writing; `previewWarmer.Submit` dropping silently; `roomlist-worker`'s 250 ms flush interval and un-acked hold; and `valkeyutil` setting no `PoolSize`.

---

## 2. What multiple independent lenses found

Nine agents worked from separate briefs with no shared state. Where several converged on the same code, the finding is unusually well-established — and the convergence itself says something about which defects are structural rather than local.

| Finding | Found by | Severity range |
|---|---|---|
| `inbox-worker`'s blocking membership send stalls the whole INBOX | **6 of 9** (head-of-line, backpressure, blast-radius, retry, federation, capacity) | `critical` → `medium` |
| `outbox-worker`'s shared worker pool defeats its own per-peer isolation | **5 of 9** | `high` |
| `history-service`'s 10 s budget vs its callers' 2 s | **3 of 9** | `critical` |
| Federation dropped at `MaxDeliver=6` on both ends of a "retry forever" path | **2 of 9** | `high` |
| `MAX_CONCURRENCY` (256) exceeds `MONGO_MAX_POOL_SIZE` (150) | **3 of 9** | `high` → `low` |

### 2.1 `inbox-worker` — the most-found defect in the audit

```go
// inbox-worker/main.go:963-975  (verified by hand)
go func() {
    defer close(membershipCh)
    for {
        msgCtx, msg, err := iter.Next()
        if err != nil { return }
        m := laneMsg{ctx: msgCtx, msg: msg}
        if isMembershipSubject(msg.Subject(), cfg.SiteID) {
            membershipCh <- m        // ← BLOCKING, from the pump goroutine
            continue
        }
        sem <- struct{}{}            // concurrent lane
        ...
```

`membershipCh` has capacity `MAX_WORKERS` (100) and is drained by **exactly one** goroutine (`:953-960`). Once it fills, the send blocks, `iter.Next()` stops, and **nothing** is read from INBOX — read receipts, mute/favourite toggles, role updates, thread subscriptions and mention badges, from **every** peer.

The comment directly above (`:929-934`) is the tell. It reasons carefully and correctly about throughput — *"membership traffic is a tiny fraction of the lane, so serializing it costs negligible throughput while the read-receipt path keeps its full MaxWorkers concurrency"* — and never considers that a **blocking** send couples the serial lane to the concurrent one. The serialisation is right; the coupling is the bug.

**It then degrades into loss.** This consumer takes plain `DurableConsumerDefaults` (`:1042`): `MaxDeliver=6`, `{30s,1m,2m,4m,8m}`. The ~200 prefetched messages age past `AckWait` while the pump is parked, redeliver, and after ~6.3 minutes guaranteed (jittered; 15.5 min nominal) are **silently dropped**.

**The architectural irony:** `outbox-worker` spends real complexity — per-peer consumers, `MaxAckPending=1`, `MaxDeliver=-1`, a long explanatory comment — isolating federation lanes at the origin so a down peer cannot affect a healthy one. The destination re-merges every peer's membership traffic into one serial lane and then drops on overflow. **The guarantee is paid for at one end and discarded at the other.**

### 2.2 `outbox-worker` — isolation that stops one layer short

Per-peer *consumers* are real and correct: each peer gets its own ack-pending budget, so a down peer's parked forwards fill only its own (`main.go:135-148`). But every concurrent lane dispatches into **one shared `sem` of `MAX_WORKERS`=100** (`:114`, `:142`), and each forward to an unreachable peer holds a slot for the full 3 s `federationForwardTimeout`.

With a 1000-message ack budget on the down peer's lane, that is `1000 × 3s / 100 slots` ≈ **30 seconds of complete pool occupancy per redelivery wave**, during which healthy peers' forwards block at `sem <- struct{}{}`.

The comment at `:116-124` claims a down peer "stalls only its own lane." **True of `MaxAckPending`; false of the pool that does the actual forwarding.** Two experts flagged the comment itself as the problem — it is precise about the half that holds and silent about the half that does not.

### 2.3 The pattern behind the convergence

All three convergent findings share a shape: **a correct isolation or ordering decision, undone one layer down by a shared resource nobody re-checked.**

- Per-peer consumers → one shared worker pool.
- A deliberately serial lane → a blocking send that couples it to the concurrent one.
- An admission cap sized to protect a pool → set larger than the pool.

That is not a code-quality problem — the per-service audit scored D1 at 3.9/5 and it was right to. It is a **systems-composition** problem, and it is invisible to any lens that reads one service at a time.

---

## 3. Availability: the system is built to queue, not to shed

Queuing is how a slow dependency becomes an outage. Shedding is how it stays a slow dependency. This system mostly queues.

### 3.1 The admission cap sits above the resource it protects

`MAX_CONCURRENCY` defaults to **256** (`pkg/natsrouter/guard.go:21`); `MONGO_MAX_POOL_SIZE` defaults to **150** (`pkg/mongoutil/poolconfig.go:25`).

`history-service` is the **only** service that noticed, raising its pool to 384 with a comment saying why (`deploy/docker-compose.yml:28-32`). `room-service`, `search-service`, `media-service` and `room-worker` inherit 256-over-150; `bot-room-service` and `bot-message-handler` explicitly set 200-over-150.

The 50–106 handlers above the pool size are **admitted and then block in the driver's connection checkout** for up to the full `REQUEST_TIMEOUT`. The guard's own doc comment says it exists to "keep a burst from holding pooled connections" — and at these defaults it admits straight past them. **Admission control that admits past the bottleneck is queuing wearing a shedding costume.**

### 3.2 Timeout budgets that cannot be met

| Service | Inner budget | Outer budget | Effect |
|---|---|---|---|
| `botplatform-service` | 15 s room-mgmt / DM-ensure (`bot_forwarder.go:75`) | 10 s `REQUEST_TIMEOUT` (`main.go:124`) | The 15 s **never fires**; and `BOT_IDEMPOTENCY_ROOM_MGMT_TTL` is sized against a number the code cannot reach |
| `translation-service` | up to 70 s (token 5 s + translate 30 s + refresh 5 s + translate 30 s) | **none** — `HandlerTimeout` never installed | Work continues long after the caller left |
| `auth-service` | 10 s OIDC client | 10 s `WriteTimeout`, no handler deadline | net/http closes the connection at the exact moment the handler is still waiting; the client retries into a service with **no cap at all** |
| `atrest` DEK fetch | 20 s, tuned for `ACK_WAIT=30s` workers | 10 s in `history-service` | Every first reader of a cold encrypted room burns its full budget during a Vault stall |
| `client-update-service` | 30 s `ReadTimeout` bounding a 2 GiB body | `admin-service` budgets 10 min | Any upload over ~570 Mbps-equivalent is severed mid-body |

`pkg/natsrouter` provides `DefaultGuarded` **specifically** so the cap and the timeout cannot be applied separately. `translation-service` calls `natsrouter.Default` with `WithMaxConcurrency` and never installs the timeout — the exact split the helper exists to prevent.

### 3.3 Where a shed is silent

`natsrouter.replyBusy` drops fire-and-forget messages silently (`pkg/natsrouter/router.go:148-152`). `user-presence-service` **declines the concurrency cap entirely** because of this — on Hello/Ping/Bye a silent drop would strand users online — and applies the timeout half anyway (`main.go:161-171`). That is the correct answer to the silent-drop question, reasoned explicitly in code, and it is worth naming as the model the rest of the fleet should follow when a shed would be a correctness bug rather than a latency one.

### 3.4 Unbounded buffering before the limit

Three services read or spool the entire request body **before** any size check: `upload-service` (`handler.go:161`, `:237` — spooled to ephemeral disk, then `fh.Size` checked), `botplatform-service` (`middleware_idempotency.go:60` — `io.ReadAll` before the handler's own cap at `bot_handlers.go:193`), and `tcard-service` (`handler.go:148`). `admin-service/client_update.go:316` and `client-update-service/routes.go:27` both cap *before* parsing and document why — so the correct pattern is in the repo, applied inconsistently.

### 3.5 Verified sound

Worth recording, because these are the things that keep the failure modes above from being worse:

- **No bare `Nak()` or `NakWithDelay(0)` exists in any service.** Every settle path goes through `pkg/jsretry`, whose `minNakDelay` floor prevents a degenerate schedule from producing the instant-redelivery storm CLAUDE.md forbids. **The most-cited failure mode in the rulebook is genuinely closed.**
- `WithOutageRetryBudget` is handed the same schedule the service actually settles with in both users — so the 20×-short-window bug its own comment describes does not exist today.
- `ginutil.MaxConcurrency` sheds correctly: non-blocking acquire, **429 not 503** (so mesh outlier detection does not eject the host mid-burst), `Retry-After`, and a shed counter rather than a per-rejection log line.
- `user-service` is the reference implementation, and the only service that gets the whole nesting right: cap **before** auth so a shed request pays nothing, `ginutil.Timeout`, `LimitListener` above the cap, and `config.Load` **enforcing** `WriteTimeout > HandlerTimeout` and `MaxConns > MaxConcurrency` rather than documenting them.
- `roomlist-worker` validates its flush budget against `EffectiveAckWait()` — the value the server actually enforces after `BackOff[0]` overwrites `AckWait` — not the configured field.
- `search-sync-worker` and `roomlist-worker` are the two services that treat `MaxAckPending` as a capacity budget and **fail startup on an incoherent config**. That reasoning is correct and belongs in more services.

---

## 4. Availability: blast radius, and whether anyone would know

### 4.1 Health checks that report green while the service does nothing

`pkg/natsutil.HealthCheck` reports healthy on `CONNECTED` **or** `RECONNECTING` — i.e. it is green in exactly the failure that kills a consume loop.

Every JetStream worker **except `roomlist-worker`** returns from its `iter.Next()` loop on error and then does nothing: no readiness flip, no exit, and in two cases (`push-notification-service`, `bot-message-worker`) not even a log line. The pod stays Ready and consumes nothing.

`roomlist-worker` is the sole service that closes this, with `consumeState` + `consume.Check()` + a **self-SIGTERM** so the final flush drains (`main.go:171-181`, `:262-278`). The fix is written, correct, and not propagated.

Three more that report healthy while broken:

- **`outbox-worker` with an empty `ALL_SITE_IDS`** builds zero lanes, warns once, and runs as a no-op **while producers keep filling OUTBOX** — and its own compose default collapses to exactly that. The sole OUTBOX owner, silently doing nothing.
- **`auth-service`'s `/readyz` registers zero checks**, so it is a constant 200 identical to `/healthz`. `docs/health-probes.md` describes it as reporting NATS connectivity — which this service does not hold. A pod that cannot validate a single token reports Ready.
- **`media-service` and `upload-service` expose `/healthz` only** — a constant `{"status":"ok"}` — so a dead MinIO still receives traffic.

### 4.2 The federation path is entirely unmonitored

Neither `outbox-worker` nor `inbox-worker` imports `pkg/natsmetrics`. No service anywhere reads `ConsumerInfo`/`NumPending` — a repo-wide grep returns nothing. **A site can stop federating completely and every probe and dashboard stays green** until a user reports that a remote member never appeared.

### 4.3 Panic containment

`message-gatekeeper` — the single validation hop every user message crosses — passes a bare closure to `natsmetrics.Start`, which spawns a goroutine per message **with no `recover()`**. Every other MESSAGES-CANONICAL consumer wraps in `jobguard`. A panic in `HandleJetStreamMsg` kills the process with the message un-acked; JetStream redelivers on `{30s,1m,2m,4m,8m}`, so **one malformed payload crash-loops the pod ~6 times over ~15 minutes, during which no message on the site is accepted.** `bot-message-worker` and `push-notification-service` share the gap with smaller radius.

### 4.4 Fail-open where it matters

**Valkey — an explicitly fail-open cache tier — is a fatal startup dependency in all four hot-path services.** `message-gatekeeper`, `broadcast-worker`, `notification-worker` and `room-worker` all `os.Exit(1)` on a failed PING, while `valkeyutil.ConnectOptional` exists for precisely this and is used by `message-worker` with a written justification: *"exiting here would crash-loop the pod over a fail-open cache tier."*

The fault is **latent**. A running pod survives a Valkey outage fine — the tiers fall through to Mongo. But a Valkey outage plus **any** rolling deploy puts ingestion, fan-out, notifications and membership into CrashLoopBackOff simultaneously.

**The push gate is a compound fail-open.** `notification-worker`'s settings and presence snapshots both discard errors into an empty map, and `shouldPush` reads the zero value as "not muted, not DND, push". Worse than uniform: settings chunks run **sequentially under one shared 2 s timeout**, and the first failure returns what it has — so in a large room the *earlier* chunks stay enforced and the later ones fail open. **Mute enforcement becomes a function of account sort order.**

### 4.5 One config change that silently becomes data loss

`WithOutageRetryBudget` returns its settings **unchanged** when `s.MaxDeliver != DefaultMaxDeliver`:

```go
// pkg/stream/consumer.go:154-156
if s.MaxDeliver != DefaultMaxDeliver { return s }
```

So setting `CONSUMER_MAX_DELIVER=10` on `message-worker` — a plausible "give it more retries" change — **bypasses the guard and yields fewer deliveries than the ~17 the hour budget computes.** Blast radius crosses services: `broadcast-worker` has already delivered the message live to every connected client, so the drop leaves a message users saw and that no longer exists in Cassandra. Unrecoverable — the row was never written.

### 4.6 Two cases that cannot self-recover

Everything else here recovers automatically once the fault clears (floored by the 10-minute backoff tail). Two do not:

1. **`hr-sync-worker`** combines `MaxDeliver=-1` with `MaxAckPending=1` and classifies only decode errors as permanent. A Mongo write failure retries forever with one in-flight message and **nothing behind it moving** — the only consumer in the repo that requires operator intervention.
2. **`outbox-worker`'s ordered lane** wedges permanently on any deterministically-unforwardable event (over `max_payload`, a peer whose INBOX was never provisioned, a decommissioned site still in `ALL_SITE_IDS`). `HandleEvent` wraps every publish failure in a plain `fmt.Errorf`, so **nothing is ever classified permanent**, and `MaxAckPending=1` means the head blocks every membership event to that peer, forever, with no dead-letter and no metric.

---

## 5. Journey: send a message → delivered

**Steady-state p50 is genuinely good: ~10 ms end-to-end.** Every finding below is a *cliff*, not baseline slowness.

Assumptions, stated so the arithmetic is checkable: NATS core RTT ~0.3 ms, JetStream PubAck ~2 ms, Valkey ~1 ms, Mongo indexed point read ~2 ms, Cassandra `LocalQuorum` ~4 ms, Cassandra LWT ~20 ms, one warm history RPC ~8 ms.

| Hop | Round trips (serial unless noted) | p50 | Worst |
|---|---|---|---|
| client → MESSAGES | 1 JS publish + PubAck | 2 ms | — |
| **gatekeeper** | 0 warm; up to **6 serial** | 6–10 ms | **6.15 s** |
| gatekeeper → sender reply | 1 core publish | 0.3 ms | 0.3 ms |
| broadcast-worker (delivery) | 0 warm; 4–5 cold | 3–6 ms | ~2 s+ |
| message-worker (plain create) | 1 Cassandra batch | 5 ms | 10 s |
| roomlist-worker | coalesced, off the path | — | — |
| notification-worker | **6 serial + N/100 serial** | 10–20 ms | ~11 s+ |

### 5.1 The sender's reply path — the highest-value target

The sender is blocked awaiting `chat.user.{account}.response.{requestId}`. On the reply path today:

- `resolveQuoteSnapshot` (2 s) completes **before** `resolveThreadParent` starts.
- `resolveThreadParent` is itself fetch (2 s) + `waitFor(150 ms)` + refetch (2 s).
- Neither `HandleJetStreamMsg` nor `processMessage` imposes an overall deadline, and `natsmetrics.Consume` supplies none.

The two fetches are only coupled when the IDs are equal, so **a thread reply that also quotes something else runs two full serial RPCs that could run concurrently — 6.15 s worst case, with the sender blocked on all of it.** Running them in an `errgroup` under a single ~2.5 s deadline cuts the worst case to ~2.2 s.

### 5.2 The thread-reply cliff in persistence

Every thread reply costs `message-worker` **~15 serial round trips**, including:

- **Two Cassandra LWTs** (`IF EXISTS`, Paxos, ~4 extra round trips each) re-stamping `thread_room_id` on *every* subsequent reply, though the stamp is immutable after the first (`store_cassandra.go:464-484` — verified).
- **A full-partition scan with no `LIMIT` and no `COUNT`** on every reply (`pkg/threadcount/count.go:41-44` — verified). Its own comment explains why (`deleted` tombstones must be walked past, not counted), which makes it deliberate — and O(N) per reply, O(N²) per thread. A 20k-reply thread decodes 20k rows to add one message.

And **`SerialConsistency` is never set anywhere in the repo** (verified — zero occurrences), so those LWTs take the server default `SERIAL` rather than `LOCAL_SERIAL`. Nor is the client pinned to a DC: `cassutil` sets `Consistency = LocalQuorum` but its host policy is `TokenAwareHostPolicy(RoundRobinHostPolicy())`, not `DCAwareRoundRobinPolicy` (`pkg/cassutil/cass.go:73`, `:77`). So **the code takes no local-DC guarantee at all** — under any multi-DC Cassandra topology this is cross-DC Paxos on every thread reply. Whether a given site is multi-DC is a deployment fact this audit could not observe; what is verifiable is that nothing in the repo prevents it.

### 5.3 The `@all` cliff

`mentioned := mentionsAll || …` makes `EligibleForPush` return true for **every** member regardless of `isLargeRoom`, so a 10,000-member room yields 10,000 candidates. Then:

- `mongoUserSettings.Snapshot` chunks at 512 **sequentially** under a single 2 s budget — 20 chunks, and its own comment concedes the tail "never gets read and fails open" (see §4.4).
- 100 push batches are emitted **one at a time, each awaiting a PubAck**.

### 5.4 Work done four times

The gatekeeper does **not** put resolved mentions on the canonical event, so `mention.Parse` runs independently in `broadcast-worker`, `message-worker`, `roomlist-worker` and `notification-worker`, with three separate account→user resolutions against three separate L1 caches. The gatekeeper already does exactly this shape of lookup for the sender's display name, so the pattern is established. And `mention.Parse` is **uncapped** — a 20 KB body can carry ~1,600 distinct `@tokens`; `notification-worker` caps at 50 and `roomlist-worker` has a mention budget, but `broadcast-worker` and `message-worker` bound neither.

### 5.5 Verified sound

- **Channel fan-out is O(1) publishes, not O(members)** — one marshal, one publish to `chat.room.{roomID}.event`, NATS does the fan-out. **Room size does not enter the delivery budget at all.** This is the single most important thing the design gets right, and it is why the message path has no room-size cliff.
- The gatekeeper carries thread-parent resolution on the canonical event **precisely so** downstream consumers skip their own RPC — on the healthy path that removes two history RPCs per thread reply. (§1's correlated loop is the failure of this optimisation, not its absence.)
- `roomlist-worker` is genuinely off the delivery path: it holds messages un-acked and coalesces to a 250 ms flush, so no MongoDB write can NAK a delivered message. CLAUDE.md's account of that split is accurate.
- `natsrouter` sheds rather than queues, so a saturated `history-service` returns `unavailable` fast and the gatekeeper degrades to its placeholder in microseconds rather than burning 2 s.
- `message-gatekeeper` treats a failed quoted-parent fetch as soft-fail degradation rather than a NAK — the correct posture, and the contrast that makes `broadcast-worker`'s NAK on the same failure stand out.

---

## 6. Journey: app open → sidebar

**This is the journey every user pays on every cold start, and it is the weakest of the five.**

### 6.1 The client gives up before the server is allowed to finish

The client's `requestSync` default timeout is **5 s** with no override in `fetchSidebarBuckets`. `user-service`'s budget is **15 s**, and its enrichment alone budgets **10 s serially**: `enrichWithRoomInfoAndLastMsg` runs `enrichCrossSite` **then** `enrichLastMessage`, never overlapped, each bounded by a 5 s NATS RPC timeout, each waiting on the slowest site.

**One unreachable peer costs 5 s in phase 1 and another 5 s in phase 2.** The client has already abandoned; `user-service` keeps burning the full 15 s of Mongo and RPC work per abandoned request; and `fetchAllPages` treats the failure as terminal for that bucket. **Every user holding ≥1 subscription on a degraded peer loses their whole sidebar bucket** — including the rooms on the healthy local site.

The two phases are independent for the local site. Running them concurrently, and `lookupApps`/`lookupHRInfo` alongside, removes ~2 of the 4 serial phases at no correctness cost.

### 6.2 Paginated in its reply, not in its work

`AggregateSubscriptions` issues an **unpaginated** `Find` over all matching subscriptions before any offset/limit is applied, then resolves sort keys for all of them. The client drains buckets by offset, so a 500-room user's `rooms` bucket costs 3 serial round trips × 500-doc scan. With three overlapping buckets fired in parallel, **one app-open reads roughly 4–5× the user's subscription set.** Scaling is O(rooms²/limit) in documents.

### 6.3 The badge path's failure mode is a permanent cache miss

`unreadRooms` builds `roomIDsBySite[site]` with **every** remote room id and calls `GetRoomsMeta` once — **no chunking**, unlike `enrichCrossSite` which does chunk. At ~200 B per `RoomInfo`, a 600-room peer reply is ~120 KB against a 128 KB ceiling. Overflow marks the site failed, sets `degraded`, and `CountSubscriptionsFor` then **skips `Reseed`** — so the badge cache can never warm for exactly the accounts that most need it, and every `subscription.count` recomputes the full aggregate plus the doomed RPC.

Server-side, `badge.count.batch` is **O(accounts) sequential** on a cold cache: each miss is one Mongo query plus up to P−1 cross-site RPCs, **one account at a time**, inside a 10 s handler called with a 5 s client timeout.

---

## 7. Journey: open a room → history

**The read path is well-bounded and the enrichment has no N+1** — this is the strongest journey in the audit. One cross-component loop is dangerous.

### 7.1 The bucket walk, quantified

`messages_by_room` is `(room_id, bucket)` with `MESSAGE_BUCKET_HOURS=360` — **15-day partitions**. The walk is contiguous by design (it will not trust `rooms.lastMsgAt` as a ceiling, correctly — see §7.4), bounded by `max(room.createdAt, now−730d)` and `MESSAGE_READ_MAX_BUCKETS=122`.

| Case | Partitions read | Sequential waves |
|---|---|---|
| Recent page, busy room | **1** | **1** |
| Room silent 1 year | 25 | **4** |
| Room ≥2 y old, empty in window | **50** | **7** |
| Thread open | **1 partition** | **3 round trips, no walk** |
| Restricted reader (unclamped floor) | up to **122** | **~16** |

So a single cold room is bounded: ~50 partitions, ~35–140 ms of Cassandra RTT. **Two things break the bound:**

1. **The restricted-access branch bypasses `walkBounds` entirely.** Config validation sizes `MESSAGE_READ_MAX_BUCKETS` off the history floor only, so a member who joined a since-silent room >2 y ago walks past the floor the validation assumes — and an exhausted budget returns an **empty page with `hasNext=false`**: silent history loss, for exactly the readers the validation exists to protect.
2. **`MESSAGE_BUCKET_HOURS` is one global constant with a hard tension and no escape.** 15-day partitions in a 10 msg/min room are ~216k rows — past Cassandra's practical partition size. Shrinking it multiplies walk length, and the validator then *forces* `MESSAGE_READ_MAX_BUCKETS` up to match: at `W=24h` a quiet-room read legitimately touches **733 partitions / ~92 waves**. It is also welded to `compaction_window_size` in the DDL, so it cannot be retuned without a schema change.

### 7.2 The preview loop — the dangerous one

`openStoredPreview` serves a stored preview **only while `previewForMsgId == lastMsgId`** — and those two fields are written by **two different consumers**. When they disagree the room falls to `walkForPreview`: a `pageSize=1` walk, ~9 waves / ~48 partitions.

`rooms.get` takes **100 rooms** with 16-way concurrency and fanout 8. So a chat-list load of cold rooms is **up to 5,000 partition reads and 128 concurrent Cassandra queries per in-flight RPC, ~63 sequential round trips, against a 5 s caller deadline.**

Two mechanisms create and sustain the cold rooms (both verified by hand):

- **`previewWriter.Flush` swaps the pending map out *before* the bulk write** (`broadcast-worker/preview_writer.go:148-161`), so a failed flush **discards the batch entirely** — no retry, no re-buffer. Above 5,000 buffered rooms it also sheds bodies.
- **`previewWarmer.Submit` drops silently when its queue is full** (`history-service/internal/service/warmback.go:110-125`) — which is precisely when a burst of cold rooms is arriving. Its comment calls a dropped job "self-correcting — the next read re-walks the room and submits again," which is **circular: the re-walk is the expensive thing.**

**This contradicts CLAUDE.md.** Line 278 states the `previewForMsgId`/`lastMsgId` disagreement "only costs history-service's lazy walk, which then warms the room back." Both halves understate it: the cost is up to 5,000 partition reads per RPC, and the warm-back is not guaranteed. This is the **second** instance in this audit of a CLAUDE.md risk assessment that justified leaving a trade-off in place while materially understating it — the first was the `bot-message-worker` write-timestamp note found in the fleet audit.

### 7.3 Search is the weakest single hop

`track_total_hits: true` with an **unbounded `from`** on a cross-cluster index pattern. `normalizePagination` caps `size` at 100 and never caps `offset`. Exact total-hit counting has no 10k cap, so every query costs a full count **across every shard of every remote cluster**; deep `from` makes each shard in each cluster materialise `from+size` hits; past `index.max_result_window` ES 400s and the user sees `internal`. There is also no ES-side `timeout`/`terminate_after`, so a cancelled search **keeps running remotely** — compounding on the cluster already slow.

### 7.4 Verified sound

- **Per page of messages, enrichment costs zero extra round trips.** Sender, mentions, quoted parent, reactions, attachments, `tcount`, `pinnedAt` are denormalised columns on the row. At-rest decryption is one DEK per room, 2Q-cached, singleflight-coalesced, with a Valkey L2 — per-row cost is an AES-GCM open, not a fetch.
- **Thread reads are exactly what the schema promises**: one partition, one query, a real page-state cursor.
- The refusal to use `rooms.lastMsgAt` as a walk ceiling is correct and well-argued — it is written by a separate consumer with `MaxDeliver=-1` and lags without bound; using it would silently hide just-written rows.
- The startup check on `MESSAGE_READ_MAX_BUCKETS` correctly accounts for the partial partitions at each end and **refuses to start rather than lose history**. It is the most careful capacity check in the repo.
- Empty pages never claim `hasNext`, which keeps a budget-exhausted walk from wedging the client's pagination.

---

## 8. Journey: cross-site federation

**The SLA the code actually implements:**

| | Healthy | Degraded |
|---|---|---|
| **Message crossing sites** | Same-site latency + one WAN hop. **No durability** — the payload is core NATS, not JetStream; a gateway blip loses it silently and recovery is the client's own history fetch | — |
| **Membership change** | One WAN RTT plus two JetStream store-and-consume hops; tens of ms over same-site. Per-peer ceiling on the ordered lane is **~12 events/s at 80 ms RTT** | Peer down → events park; **recovery = outage length + 5–10 min**, because after ~2.6 min every parked forward is on `DefaultBackoff`'s 10-minute repeating tail. A 30-second blip and a 6-hour outage cost the same recovery tail |
| **Either end failing** | — | **Dropped after ~6.3 min, silently.** Neither `room-worker` (ROOMS) nor `inbox-worker` (INBOX) applies an outage budget; both keep `MaxDeliver=6` |

That last row is the structural one: **OUTBOX runs `MaxDeliver=-1` explicitly so a peer down for an hour never drops a federated event — and both ends of that path drop at 6 deliveries.** The unlimited-redelivery guarantee covers only the middle hop.

### 8.1 Two silent-data-loss paths

**The Teams dedup collision** (verified). `natsutil.InboxDedupID` composes the `Nats-Msg-Id` as `requestID + ":" + destSiteID` — **no event type**. A single Teams batch calls `federateJoinedAtRefresh`, then `federateTeamsMembership(added)`, then `federateTeamsMembership(removed)` — same peer, same request context, **identical msg-ID**. JetStream drops the 2nd and 3rd. **Departed Teams members are never removed at their home site.** The `payloadSeed` that would disambiguate is a fallback used only when the request ID is empty, and `room-worker/main.go:415-425` guarantees one always exists. The three internal-lane publishes collide identically, so `search-sync` loses two of three too.

**The rename/add race.** `isMembershipSubject` matches only `member_added`/`member_removed` — **`room_renamed` is not on the sequential lane.** It is dispatched into the wide fan-out pool while `member_added` queues on the single-worker channel. `UpdateSubscriptionNamesForRoom` is an `UpdateMany` on `roomId` that matches **zero** documents before the subscription exists, and `handleMemberAdded` writes `Name` from the event's captured `RoomName` while never setting `nameUpdatedAt`. A rename that wins the race leaves the new remote member **permanently on the old room name** — the exact failure `outbox.OrderedEventTypes` exists to prevent, reintroduced one hop later.

### 8.2 Dedup windows are wrong in both directions

**`Duplicates` is never set on any stream** (verified — zero occurrences in `pkg/stream`), so the JetStream default of **2 minutes** applies, while every retry tail is **10 minutes**. Note where the fix has to land: `BOOTSTRAP_STREAMS` defaults to `false` and CLAUDE.md forbids services creating streams in production, so editing `pkg/stream` alone moves the window in local dev only — the live window is set wherever ops/IaC provisions the stream.

- *Inside* the window, colliding IDs silently drop distinct events (§8.1).
- *Outside* it, a redelivery on the 3rd/4th rung duplicates at the destination — and one duplicate canonical event costs a **second full pass through all five MESSAGES-CANONICAL consumers**: extra Cassandra write, extra full room fan-out to every connected client, extra roomlist write, extra push batch, extra ES index.

Today's handlers are `$max`/`$setOnInsert`/`$lt`-guarded, so duplicates are benign — but that is timing luck, not design.

### 8.3 Five event types bypass OUTBOX entirely

`user_status`, `settings`, `chatlist`, `permissions` and `account_updated` publish straight to the remote INBOX from inside the request path, `O(peers)` per request, failures logged and swallowed, and `admin-service` passes an **empty msg-ID** (no dedup at all). None is in `outbox.ConcurrentEventTypes`/`OrderedEventTypes`, so they *cannot* be routed through the retried lane, and no reconciliation exists.

---

## 9. The capacity model

### 9.1 The binding constraint: ~3,300–4,000 msg/s per site, unliftable by adding pods

`roomlist-worker` holds messages **un-acked** until the flush carrying their intents lands. Steady-state in-flight = `rate × (FLUSH_INTERVAL + flush duration)`, bounded by `MaxAckPending`:

```
1000 / (0.25s + ~0.05s)  ≈  3,300 msg/s
```

**`MaxAckPending` is enforced server-side per durable consumer, shared by every pod bound to it** — so two pods each running their own 250 ms ticker still divide one 1000-message budget. Its own compose file states the coupling in a comment (*"CONSUMER_MAX_ACK_PENDING must exceed FLUSH_INTERVAL x peak message rate"* — verified) but **nothing derives the value.**

This leads the next hot-path constraint by roughly two orders of magnitude: `message-gatekeeper` holds a message for one PubAck (~1 ms), giving it a comparable-budget ceiling near 10⁶/s.

### 9.2 Horizontal scaling stops at ~5 pods

All six `MAX_WORKERS` workers use `PullMaxMessages(2 × MaxWorkers)` = **200** at the default. Buffered-but-unprocessed messages are *delivered*, so they consume `MaxAckPending`:

```
1000 / 200  =  5 pods before prefetch alone exhausts the budget
```

The 6th pod's workers idle while its buffer starves. Worker-side the same arithmetic gives 10 pods. **No service couples `MAX_WORKERS` to `CONSUMER_MAX_ACK_PENDING`, and no validation warns** — except `search-sync-worker`, which checks exactly this shape and only **warns**, and whose shipped code defaults **fail their own check** (`(2+1) × 500 = 1500 > 1000`; the local compose repairs it by pinning 1500).

### 9.3 The per-pod ceiling nobody set

`valkeyutil.dialCluster` builds `redis.ClusterOptions` with **only `Addrs` and `Password`** (verified — no `PoolSize`). go-redis v9 defaults *cluster* pools to `5 × GOMAXPROCS` — half the standalone default — and Go 1.25 makes `GOMAXPROCS` container-aware. **On a 2-CPU pod that is 10 connections against 100 workers: a 10× oversubscription**, with a 4 s pool timeout behind it.

Whenever a cache tier is warm and Mongo is untouched — i.e. the normal case — **this is the real binding constraint per pod**, and it is set by a library default nobody chose.

Compounding it: **`roomsubcache` has no L1 and is read once per message.** Its own comment says so. Every notifiable message pulls the room's whole member blob from **one cluster slot** in both `notification-worker` and `broadcast-worker`. A 1000-member room at 10 msg/s draws ~2.8 MB/s off that single slot, and each multi-hundred-KB `GET` occupies one of those ~10 connections for its full duration. Cluster sharding gives no relief — it is one key.

### 9.4 Storage ceilings

- `messages_by_room` exceeds the 100 MB partition guideline at roughly **6,700 messages/day sustained in one room** — reachable for a busy 1,000-member channel.
- `thread_messages_by_thread` is partitioned by `thread_room_id` **alone, with no bucket**, on default STCS compaction, and no cap on thread reply count exists anywhere in the repo. **One unbounded partition per thread**, and §5.2's per-reply full scan walks it.

### 9.5 Verified sound

- Channel fan-out is O(1) publishes in both room size and peer count; `@all` is a room-level pointer, not N per-member intents.
- `room-worker` buckets accounts by destination and emits **one** federation event per site carrying the account list — O(sites), not O(sites × accounts).
- `notification-worker` narrows settings and presence lookups to *surviving candidates* rather than all members, and `LARGE_ROOM_THRESHOLD=500` restricts pushes in big rooms to mentioned users.
- `roomlist-worker`'s mention budget is **derived** (`4 × MaxAckPending`) rather than a free-standing knob.
- `broadcast-worker`'s preview writer sheds bodies rather than blocking — the correct trade, and its 20,000 rooms/s shed threshold sits far above the site ceiling in §9.1.

---

## 10. Prioritized action list

Ordered by blast radius ÷ effort. Items 1–4 are the availability chain that answers the original question; 5–8 are the journey cliffs; 9–11 are ceilings you will hit as you grow.

| # | Sev | Action | File | Why here |
|---|---|---|---|---|
| 1 | `critical` | **Make `inbox-worker`'s membership enqueue non-blocking** — `select` on the send vs. a NAK-and-continue, or shard the lane by `hash(roomID)` (ordering is only required per room+account) | `inbox-worker/main.go:971` | **Found by 6 of 9 lenses.** A blocking send from the pump goroutine stops *all* cross-site state for the site, then silently drops after ~6.3 min. One `select` statement. |
| 2 | `critical` | **Reconcile `history-service`'s 10 s budget with its callers' 2 s**, and stop NAKing on a downstream that is *shedding* — route `Unavailable`/`TooManyRequests` onto a backpressure schedule, or soft-fail | `history-service/internal/config/config.go:82`; `broadcast-worker/handler.go:1398-1410`, `main.go:539` | **This is the outage you asked about.** The 5× asymmetry converts a brownout into a capacity collapse; the 18-delivery retry budget then aims the load at the service that is already failing. Fix both halves or neither helps. |
| 3 | `critical` | **Give `message-gatekeeper` a reply-on-exhaustion path and panic containment** — reply `Unavailable` when `IsFinalDeliveryFromContext` is true, and wrap the handler in `jobguard.Run` | `message-gatekeeper/handler.go:234`, `main.go:229-233` | MESSAGES has **one consumer**, so its 1000-slot budget is the site's entire send capacity, and today a dropped send is **completely silent to the sender**. The missing `jobguard` means one malformed payload stops all sends for ~15 min. |
| 4 | `critical` | **Include the event type in the dedup ID**, and set an explicit `Duplicates` window ≥15 min **in production stream provisioning**, mirrored by the local bootstrap | `pkg/natsutil/request_id.go:149`; ops/IaC stream definitions, mirrored in `pkg/stream/stream.go` | Silent, permanent data loss today: **departed Teams members are never removed at their home site.** The window fix closes the duplicate-amplification half. |
| 5 | `high` | **Switch the four hot-path services to `valkeyutil.ConnectOptional`** | `message-gatekeeper/main.go:137`, `broadcast-worker/main.go:193`, `notification-worker/main.go:148`, `room-worker/main.go:182` | A Valkey outage is harmless until **any** pod restarts, then ingestion, fan-out, notifications and membership CrashLoopBackOff together. `message-worker` already does this, with the justification written out. Four one-line changes. |
| 6 | `high` | **Give `outbox-worker` a per-peer worker budget; classify deterministic publish rejections as `Permanent`; add `natsmetrics` to both federation workers** | `outbox-worker/main.go:114`, `handler.go:63` | **Found by 5 of 9.** The shared pool defeats the isolation the design paid for; the missing `Permanent` classification lets one unforwardable event wedge a peer's membership lane *forever*; and the whole federation path is currently **unmonitored**. |
| 7 | `high` | **Overlap the two enrichment phases in `subscription.list`, and align the client timeout with the server budget** | `user-service/service/subscriptions.go:240-244`; `chat-frontend/.../asyncJob.ts:207` | One slow peer blanks the sidebar **including local rooms**, and the client abandons at 5 s work the server spends 15 s on. The phases are independent for the local site. |
| 8 | `high` | **Bound `rooms.get`'s lazy work per request; stop discarding the preview batch on a failed flush; back-pressure warm-back instead of dropping it** | `history-service/internal/service/rooms.go:81-117`; `broadcast-worker/preview_writer.go:148-161`; `warmback.go:118` | 5,000 partition reads behind a 5 s deadline is an outage amplifier, not a degradation mode — and the two mechanisms that create and sustain the cold rooms are both silent. |
| 9 | `high` | **Extract `roomlist-worker`'s `consumeState` into `pkg/` and wire it into every worker's health check** | `roomlist-worker/main.go:262-303` | Every other worker reports Ready while consuming nothing. The correct implementation, including the self-SIGTERM so the final flush drains, is already written — it just was not propagated. |
| 10 | `high` | **Set `PoolSize` explicitly in `valkeyutil`, sized from the caller's `MAX_WORKERS`; add an L1 in front of `roomsubcache`** | `pkg/valkeyutil/valkey.go:111`; `pkg/roomsubcache/lookup.go:123` | ~10 Valkey connections against 100 workers is the real per-pod ceiling on the normal warm path, and it is a library default nobody chose. The `roomsubcache` single-slot hot key rides on the same connections. |
| 11 | `high` | **Derive the ack budget from the target rate, and add a startup check coupling `PullMaxMessages × replicas` to `MaxAckPending`** — in `pkg/stream`, so all six workers inherit it. Make `search-sync-worker`'s existing check **fail**, not warn | `roomlist-worker/main.go:50`; `pkg/stream/consumer.go` | The ~3,300 msg/s site ceiling and the ~5-pod scaling wall are both invisible from any single service, and the second one means **adding pods silently stops helping**. `search-sync-worker`'s shipped defaults already fail their own check. |
| 12 | `medium` | **Fix the LWT re-stamp and the per-reply partition scan; set `LOCAL_SERIAL`** | `message-worker/store_cassandra.go:464-484`; `pkg/threadcount/count.go:41`; `pkg/cassutil/cass.go:73` | O(N²) per thread, two Paxos rounds per reply, and — because `SerialConsistency` is unset repo-wide — **cross-DC Paxos on every thread reply.** |

### Sequencing

**This week:** 1, 3, 5 — three small, local changes that remove a site-wide stall, a silent-drop path, and a latent multi-service crash-loop.
**Next:** 2, 4, 6 — the amplification loop and the two federation data-loss paths. Item 2 needs both halves in one change.
**Then:** 7–12.

**Before any load test:** items 9 and 6's metrics half. Running a load test against a fleet where a dead consumer reports Ready and the federation path emits nothing will produce numbers you cannot interpret.

---

## 11. What this audit cannot tell you

Stated plainly so none of the above is over-read:

1. **Nothing here was measured.** Every latency and every ceiling is **derived** from configured defaults and round-trip counts in the source. The arithmetic and its assumptions are written out so you can check them, but a p50 estimate is not a p50. The `docs/load-testing/` SLOs exist — items 9–11 are the instrumentation you need before those numbers mean anything.
2. **Stream retention and limits are absent from the repo by design** (CLAUDE.md assigns them to ops/IaC). So the one mechanism that *could* genuinely exhaust the broker — unbounded stream growth behind a parked consumer — **is unverifiable from this code and must be checked against your IaC.** That is the one place where your original "NATS goes down" framing may still be literally right, and this audit cannot settle it.
3. **The retry hypothesis was tested, not assumed.** The finding that retries saturate consumer admission budget rather than the broker is a *correction* to the stated premise, reached independently and confirmed against the code. If your production incident showed broker-level symptoms, point 2 is where to look next.
4. **Nine agents, cross-checked but not exhaustive.** Convergence (§2) raises confidence on five findings; a single-lens finding is one careful read, not a proof. Every claim reproduced in this report was re-verified by hand against the source — but claims in the per-lens reports that did not make it here were not.
