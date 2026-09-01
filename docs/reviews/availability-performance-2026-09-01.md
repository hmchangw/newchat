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
