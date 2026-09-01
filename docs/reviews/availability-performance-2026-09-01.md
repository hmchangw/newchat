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
