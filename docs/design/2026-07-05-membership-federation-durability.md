# Membership Federation Durability — Per-Destination Serialized Outbox

**Status:** Core design **shipped in PR #410** (OUTBOX relay + per-destination ordered/FIFO lanes for membership and `room_renamed`). The reconciliation backstop (§3.4) is the remaining follow-up.
**Date:** 2026-07-06

## 1. Problem

Cross-site federation of **membership** events (`member_added` / `member_removed`) could be **permanently lost** when a destination site was unreachable for longer than the consumer's bounded retry window, and could be **misapplied** (resurrection race) if delivered out of order.

Before #410:
- `room-worker` consumed the **ROOMS** stream, did the local membership work (subscriptions, room-key rotation/fan-out), **and** forwarded `member_added`/`member_removed` cross-site via a direct `InboxExternal` publish on a consumer with the default `MaxDeliver=5`. On an outage longer than ~5 redeliveries (~minutes) the event was **dropped** — the local mutation already committed, so local and remote silently diverged with no repair.
- The destination (`inbox-worker`) applies `member_added` as an **insert-if-absent** (`$setOnInsert`) and `member_removed` as a **hard delete** (`DeleteMany`), with **no version/watermark guard**. It compensates with a single serialized apply lane, but arrival order is whatever the transport delivered — so out-of-order delivery across an outage could resurrect a removed member.

**Goal:** **no membership change is ever lost** when a remote site is down for an hour (or a day), and none is misapplied — with the least machinery.

## 2. Decision: serialize per-destination (don't guard)

Membership is **low-volume and batched** (a bulk add of N accounts is *one* `member_added` event with an `Accounts[]` array, not N events). So a serial lane's throughput ceiling (`~1/RTT`, tens–hundreds/sec per destination) is far above the real rate. That removes the only reason not to serialize.

Serializing membership **per destination** delivers everything we need with far less to build than the alternative (a per-`(room,account)` version guard + tombstones + a read-path rewrite):

| Property | How serialize-per-dest provides it |
|---|---|
| **Durable (delay-not-drop)** | `MaxDeliver=-1` + adequate stream retention |
| **In-order → no resurrection** | FIFO per destination (`MaxAckPending=1`); **no version, no tombstone, no read-path change** |
| **Per-peer isolation** | one lane per destination — a down/slow peer's lane pauses; others flow |
| **No circuit breaker needed** | one in-flight message per destination ⇒ no concurrent pile-up to shed; the head message *is* the pause-and-probe. (`MaxAckPending=1` is the declarative form of "stop consuming while the peer is down": the server withholds message N+1 until N acks, and the head retry is the recovery probe.) |

The guard approach (needed only if membership ever wants the *concurrent* shared path) is retained as **Appendix A**.

## 3. Design (as shipped in #410, except §3.4)

### 3.1 Producer — `room-worker`
- All five membership publish sites (add-members, remove individual/org, create-room, sync-DM) call `federateMember`, which wraps the pre-marshaled `InboxEvent` envelope in an `OutboxEvent` and publishes it to the **local** OUTBOX stream on the destination-scoped subject `chat.outbox.{origin}.{dest}.{eventType}` — one event per destination site. Envelope bytes and dedup IDs are unchanged from the previous direct publish.
- **The OUTBOX publish happens before the ROOMS message is settled** — a publish failure returns an error, so jsretry Naks and the ROOMS message redelivers; local work re-runs idempotently and re-enqueues. The stable `Nats-Msg-Id` (= the event's `DedupID`) makes the re-enqueue a dedup no-op.
- `room-worker` itself stays a concurrent consumer with **no per-room producer ordering**. Two conflicting ops for the same `(room, account)` processed simultaneously can be *enqueued* out of causal order within a milliseconds-wide window — the same race class, with the same window, that the local Mongo writes already accept. Deliberately unmitigated (minimize, not eliminate); see §5.

### 3.2 Transport — OUTBOX stream + per-destination FIFO consumers (`outbox-worker`)
- **OUTBOX stream:** `R3 + file` storage, `MaxAge` ≥ the longest tolerated destination outage (membership is low-volume, so retaining a week is cheap). *(IaC precondition — see #410 ops notes.)*
- **One durable ordered consumer per remote peer** from `ALL_SITE_IDS` (`outbox-worker-ordered-{dest}`), `FilterSubjects = chat.outbox.{origin}.{dest}.{member_added|member_removed|room_renamed}` (`outbox.OrderedEventTypes`), `MaxAckPending=1` (strict FIFO), `MaxDeliver=-1` (never drop), drained by a sequential `Consume()` callback settled via `pkg/jsretry` (`DefaultBackoff`, capped at 2m).
- On a failed forward (remote down): the head message Naks-with-backoff and **never advances or drops** — the lane is paused on it, holding exactly one in-flight probe per backoff interval toward the down peer. On recovery the retried head succeeds and the lane drains **in order**. No health-check subsystem: the head message is the probe.
- FIFO is enforced **server-side** (a property of the durable consumer, not the client), so outbox-worker replicas add availability, not reordering. A stalled replica can produce a duplicate forward after `AckWait`, never a reorder — the 3s forward timeout is far below the 30s `AckWait`, and the destination's `Nats-Msg-Id` dedup absorbs the duplicate.
- The order-insensitive subscription-state event types ride a separate **per-destination concurrent consumer** (`outbox-worker-concurrent-{dest}`, default `MaxAckPending`, `MaxDeliver=-1`), one per peer for the same reason membership is per-peer: a NAK'd-with-delay message holds its ack-pending slot for the whole backoff, so with retry-forever a single down peer's parked events would fill a *shared* consumer's finite budget and stall first-delivery to every healthy peer. Per-peer, a down peer stalls only its own concurrent lane. The two filter sets partition the stream; a new OUTBOX event type MUST be added to exactly one of them or it is silently never forwarded.
- **Poison policy (current state):** jsretry distinguishes *transient* (remote unreachable → retry forever) from *permanent* (malformed → `errcode.Permanent` → Ack-drop with a warning log). There is **no dead-letter stream yet** — a dead-letter + alert would upgrade the drop to an operator-visible event (§7); reconciliation (§3.4) repairs the membership state regardless.

### 3.3 Destination — `inbox-worker` (unchanged)
- Applies `member_added`/`member_removed` in arrival order. Because the transport is per-destination FIFO, arrival order = enqueue order, so the existing insert/delete semantics are safe **without** a version guard. The existing single membership apply lane stays as defense-in-depth.

### 3.4 Backstop — reconciliation (anti-entropy) — **not yet built**
- Periodically (e.g., every 10–15 min) and **on peer-recovery**, for each room with remote members, fetch the authoritative roster from the room's home site and reconcile local subscriptions (add missing, remove extra).
- Because it syncs *current state* rather than replaying deltas, it is order-independent and repairs **anything** the event path misses — beyond-retention, ack-dropped poison, or lost-to-a-bug. This is what makes the no-loss guarantee unconditional (see §4, vectors 3 & 5).
- Likely reuses an existing membership-query surface (`member.list` / `RoomsInfoBatch`); confirm (§7).

## 4. The no-loss guarantee (remote down)

"No message lost when the remote is down" holds because every loss vector is closed. A remote outage itself never threatens the data — while the peer is down, events sit safely in the **local** OUTBOX. The vectors and their mitigations:

| # | Loss vector | Mitigation | Status |
|---|---|---|---|
| 1 | **Remote unreachable** (the target case) | Producer already committed to the local OUTBOX; the forward Naks with `MaxDeliver=-1` → retained, delivered in order on recovery. | shipped (#410) |
| 2 | **Local OUTBOX node loss** | OUTBOX provisioned `R3 + file` — survives a node failure. | IaC precondition |
| 3 | **Outage longer than retention** | `MaxAge` sized ≥ max tolerated outage; **and** reconciliation (§3.4) re-derives anything expired. | sizing: IaC; reconciliation: follow-up |
| 4 | **Producer crash before durable enqueue** | OUTBOX-publish-before-ROOMS-settle + idempotent redelivery + `Nats-Msg-Id` dedup ⇒ the event is re-enqueued exactly once. | shipped (#410) |
| 5 | **Permanent/poison event** | Today: Ack-drop + warning log (producer never emits malformed events, so this is defensive). Dead-letter + alert (§7) and reconciliation (§3.4) close it fully. | partial |
| 6 | **Destination write durability** | `inbox-worker` writes with majority-acknowledged concern (control-plane truth). | existing |
| 7 | **Peer missing from `ALL_SITE_IDS`** | No lane exists → events sit in the stream unconsumed. outbox-worker warns at startup when the list has no remote peers; alert on OUTBOX message age (§7). | operational |

The **end-to-end invariant**: the destination's membership state converges to the room home site's source of truth, and no membership change is silently discarded. The event path gives fast, ordered, durable propagation within retention; reconciliation guarantees convergence beyond it. Neither alone is sufficient for an unbounded guarantee — together they are.

## 5. Ordering (minimize, not eliminate)

The shipped design preserves **enqueue order** for each destination end-to-end:
1. **Transport**: per-destination FIFO (`MaxAckPending=1`) — an outage cannot scramble redelivery order; without it, events parked across a 1-hour outage would replay in arbitrary backoff-timer order, inflating the reorder window from milliseconds to the whole outage. All order-sensitive types (`outbox.OrderedEventTypes`: membership **and `room_renamed`**) share one lane per destination, so a `room_renamed` cannot overtake the `member_added` that creates the subscription it renames (which would strand a new cross-site member on the old name — a `room_renamed` applies update-only at the destination and is lost if the sub doesn't exist yet).
2. **Destination**: in-order apply ⇒ a `remove` is never overtaken by a stale `add` *that was enqueued before it*.

What remains is the **producer-side race**: room-worker's concurrent workers can enqueue conflicting ops for the same room out of causal order within a milliseconds-wide processing overlap — e.g. `member.add` and `room.rename` are separate ROOMS messages, so the rename (less work) can finish and enqueue before the add. Putting `room_renamed` on the ordered lane (rather than leaving it a direct INBOX publish) removes the *transport-latency* skew that made this reliably reachable, but not the producer-side reorder itself. This is the same race class the local path already accepts everywhere (validate-then-enqueue in room-service, no per-room ordering in room-worker). The elimination is per-room hashed producer lanes (cross-room parallelism, per-room causal order) — deliberately not built now (§7) — with reconciliation (§3.4) as the unconditional backstop.

## 6. Scaling ladder

The per-destination serial ceiling is ~`1/(inter-site RTT + persist)` per peer (roughly 15–60 events/s at typical RTTs). Batching keeps real demand far below it; the trigger to climb is **sustained consumer lag on an ordered lane while the peer is healthy**.

1. **Per-destination lanes** — *shipped (#410)*. Scales with peer count; a down peer stalls only its own lane.
2. **Hashed per-room lanes** — add a hash bucket to the subject, N FIFO consumers per destination: FIFO per room, N-way parallel across rooms.
3. **Guard-based ordering** (Appendix A) — full concurrency; the only rung that touches the read path.

## 7. Open questions / follow-ups

1. **Reconciliation** (§3.4): does a suitable authoritative membership-list RPC already exist (`member.list`, `RoomsInfoBatch`), or is a new server-to-server endpoint needed? Cadence + on-recovery trigger.
2. **Dead-letter + alert** for permanent (malformed) membership events, replacing the current Ack-drop-with-log.
3. **Monitoring**: per-lane consumer lag (doubles as the per-peer health signal), OUTBOX oldest-message age (catches both a long outage and a peer missing from `ALL_SITE_IDS`).
4. **Retention sizing**: pick `MaxAge` for OUTBOX and confirm `R3 + file` in IaC (also for ROOMS/INBOX/MESSAGES_CANONICAL).
5. **Peer-list lifecycle**: adding a federated site requires updating `ALL_SITE_IDS` on outbox-worker and restarting it; consider deriving producer and consumer peer sets from the same source to prevent drift.

## 8. Non-goals

- No change to the six subscription-state events (order-insensitive, `$lt`-guarded, concurrent — also moved onto OUTBOX in #410 for the same durability, via `room-service`).
- No exactly-once claim (at-least-once + idempotent apply = effectively-once at the destination).
- No version guard for membership in the primary design — see Appendix A only if volume ever demands the concurrent path.
- No producer-side per-room ordering (§5) — accepted residual race, matching local semantics.

---

## Appendix A — Guard alternative (only if membership needs the *concurrent* shared path)

If membership federation ever exceeds the per-destination serial ceiling (unlikely given batching), make member events order-*insensitive* so they can ride the concurrent OUTBOX consumer like the six subscription-state events, instead of serializing.

- Add `memberVersion int64` + `removed bool` to the subscription doc. Never hard-delete; write a **tombstone**.
- **member_added(V)** and **member_removed(V)** become the same guarded upsert (guard `memberVersion $lt V` in the filter, `upsert:true`, swallow `DuplicateKeyError`) — the pattern `inbox-worker.UpsertRoom` already uses. `member_removed` sets `removed:true`; `member_added` sets `removed:false`.
- **Cost:** every read that lists a user's rooms / a room's members must exclude tombstones (`removed:{$ne:true}`); tombstones need reaping (TTL ≥ stream `MaxAge`).
- **Hard part:** `memberVersion` must be monotonic **and causal** per `(room, account)` — a per-room atomic counter assigned at a per-room-serialized producer point (timestamps are unsafe: intra-site concurrency + clock skew).
- This is strictly more machinery than §3 and only buys the single uniform concurrent path. Prefer §3 for membership; reach for this only on measured need. A **snapshot** variant (replicate the per-destination roster subset at a per-room version, max-wins) sidesteps tombstones and causal versioning and converges with reconciliation — consider it before the delta+tombstone guard.
