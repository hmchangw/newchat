# End-to-end results — channel create + first message (2026-08-22)

Real stack: `room-service`, `room-worker`, `message-gatekeeper`, `broadcast-worker`,
`message-worker`, `history-service` against NATS 2.11 / MongoDB 8.2 / Cassandra 5 /
Valkey 8.1. Driver: `go run ./tools/roomrace/e2e`.

The room should contain three messages: `room_created` (sys), `members_added` (sys),
and alice's `first message`. All three are confirmed present in Cassandra.

## Summary

| run | alice live | alice history | bob live | bob history |
|---|---|---|---|---|
| baseline (today) | **1 of 3** | **2 of 3** | 3 of 3 | **2 of 3** |
| `-early-sub` | 3 of 3 | **2 of 3** | 3 of 3 | **2 of 3** |
| `-hint` | 1 of 3 | 3 of 3 | 3 of 3 | 3 of 3 |
| `-early-sub -hint` | 3 of 3 | 3 of 3 | 3 of 3 | 3 of 3 |
| `-retry-history` (55 tries over 3.5 s) | 1 of 3 | **2 of 3** | 3 of 3 | **2 of 3** |
| `-retry-history`, `HISTORY_ROOM_CACHE_SIZE=0` | 1 of 3 | 3 of 3 (5 tries) | 3 of 3 | 3 of 3 (1 try) |

## Baseline timeline

```
   13ms  alice  rpc            room.create sync reply accepted
   23ms  alice  user-subject   subscription.update added
   23ms  bob    user-subject   subscription.update added
   24ms  bob    room-subject   subscribed
   29ms  bob    room-subject   new_message [members_added]
   29ms  bob    room-subject   new_message [room_created]
  529ms  alice  rpc            create-room async result ok      <-- 500ms after the sys messages
  534ms  alice  rpc            msg.send reply ok
  534ms  alice  room-subject   subscribed                        <-- too late for the sys messages
  539ms  bob    room-subject   new_message first message
  539ms  alice  room-subject   new_message first message
  543ms  alice  history        msg.history returned 2 (tries 1)  <-- her own message missing
  548ms  bob    history        msg.history returned 2 (tries 1)
```

## Findings

**1. The creator is told about her own room ~500 ms after everyone else.**
`room-worker.finishCreateRoom` publishes `subscription.update` to every member,
*then* the system messages, *then* the deferred `publishAsyncJobResult`. A client
that waits for the async result before subscribing has already missed the system
messages. Subscribing on the client's own `subscription.update` (`-early-sub`)
catches them: subscribed at 24 ms, system messages at 27 ms.

**2. `msg.history` cannot see a just-sent message, and retrying does not help.**
Both users' history returns 2 of 3 even though all three rows are in Cassandra.
`history-service.walkBounds` ceilings the scan at the room document's `lastMsgAt`,
and `LoadHistory` caps `before` at that value — so anything newer than the room
doc is invisible. Two stacked delays:

* `broadcast-worker` advances `room.lastMsgAt` on a batched flush
  (`LAST_MSG_FLUSH_INTERVAL`, default **250 ms**);
* `history-service` then caches the resolved room times
  (`HISTORY_ROOM_CACHE_TTL`, default **10 s**), pinning the stale ceiling.

Controlled isolation: with the cache on, **55 retries over 3.5 s still returned 2**.
With `HISTORY_ROOM_CACHE_SIZE=0`, the same retry loop returned 3 after **5 tries
(~200 ms)** — the residual 200 ms is the `lastMsgAt` flush. A probe of the same
room after the TTL expired returned 3.

**3. The documented `meta.lastMsgAt` hint fixes the read.**
`msg.history` accepts `meta.lastMsgAt` (documented in `docs/client-api.md` under
Common request fields). It is the scan ceiling, so supplying `now` returns all
three on the first try, with the cache still enabled — and skips the Mongo lookup.

**4. The two fixes are independent and both are needed.** `-early-sub` fixes the
live path for the creator; `-hint` fixes the read path for everyone.

---

# Residual race and the fix (2026-08-23)

The client-side algorithm is *subscribe → flush → hinted read → merge*. The question this
round answers: the client cannot subscribe instantly, so can a message be missed by **both**
paths — delivered before the subscription was live, and not yet readable when history was read?

`-client-delay` models the time a client spends between receiving `subscription.update` and
having a live subscription; the client then reads history immediately, as the recommended
algorithm says. 12 rooms per delay, `-early-sub -hint`.

## Without the grace window

| client delay | missing from live | missing from read | **missing from both** |
|---|---|---|---|
| 0 ms | 0% | 58–64% | **0%** |
| 2 ms | 0–6% | 33–36% | **0%** |
| 5 ms | 53% | 33% | **0%** |
| 8 ms | 56% | 33% | **0%** |
| 12 ms | 67% | 33% | **0%** |
| 20 ms | 67% | 33% | **0%** |
| 40 ms | 67% | 33% | **0%** |

The live path degrades exactly as expected — past ~5 ms both system messages are gone — and
the read covers it. Across 84 rooms nothing was lost. But that is a property of an idle
machine: `send → readable in history` measured **avg 51 ms, max 74 ms**, so the hole exists,
it just never opened here.

## Forcing the hole: a lagging `message-worker`

`message-worker` and `broadcast-worker` are independent consumers of MESSAGES-CANONICAL, so a
backlog on the writer does not stop the fan-out. SIGSTOP on `message-worker`, `-client-delay 12ms`:

| | missing from live | missing from read | **missing from both** |
|---|---|---|---|
| alice | 67% | 100% | **67%** |
| bob | 67% | 100% | **67%** |

Both system messages lost outright — missed live because the client was 12 ms late, missed in
history because they were not written yet. **Subscribe-then-read is not sufficient.** And the
client cannot even detect it: there is no per-room sequence number, so a client has no way to
know two messages are missing from the middle of what it holds.

## With the grace window (`JOIN_GRACE=30s`)

`broadcast-worker` also publishes a channel's events to the user subject of every member who
joined within the window — a subject the client has held since login, so there is no
subscription to race.

| client delay | missing from live | **missing from both** |
|---|---|---|
| 0 ms | **0%** | **0%** |
| 5 ms | **0%** | **0%** |
| 12 ms | **0%** | **0%** |
| 30 ms | **0%** | **0%** |
| 100 ms | **0%** | **0%** |
| 1 s | **0%** | **0%** |

And the case that defeated the client-side fix:

| `message-worker` STALLED, delay 12 ms | missing from live | missing from read | **missing from both** |
|---|---|---|---|
| alice | **0%** | 100% | **0%** |
| bob | **0%** | 100% | **0%** |

The read still returns nothing — nothing has been persisted — and nothing is lost, because
delivery no longer depends on either the room subject or the read.

## Conclusion

The live path can only be made race-free by delivering on a subject the client subscribed to
**before the room existed**. Everything else — subscribing faster, flushing, reading history —
narrows the window without closing it, because the client's subscribe causally follows the
event that tells it the room exists.
