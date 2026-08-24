# New-room → first-message race: analysis, measurements, recommendation

**Status:** analysis + reproduction (no production code changed)
**Date:** 2026-08-22
**Repro:** `./tools/roomrace/run.sh` (real NATS in Docker, production subjects from `pkg/subject`)
**Scope:** `room-worker`, `broadcast-worker`, `message-gatekeeper`, client event contract

---

## 1. Answer up front

**Can the current architecture solve these two problems? Yes — both. Neither needs a new RPC.**

| | Where the message is lost | Fixable with today's architecture? |
|---|---|---|
| **Problem 1 — DM** | NATS delivers it to client B; **B's client discards it** because the room isn't in local state yet | **Yes, client-only.** No backend change. |
| **Problem 2 — channel** | Two independent losses: NATS drops the live event (nobody is subscribed to `chat.room.{id}.event` yet), **and** `msg.history` cannot see the just-sent message | **Not fully.** The client fix (subscribe → flush → hinted read → merge) narrows it but loses everything when the write path lags. Closing it needs the server-side join grace window. |

The measured, decisive fact:

**The client cannot close Problem 2 on its own.** Subscribe → flush → hinted read → merge
narrows the window but does not close it: with `message-worker` stalled and a 12 ms client
delay, **67% of messages were lost from both paths** — missed live because the client was late,
missed in history because they were not written yet. The client cannot even detect this (no
per-room sequence number). The fix that measures 0% at every client delay, and 0% even with the
write path stalled, is the **join grace window** (§4, Fix B): deliver on the per-user subject
the client has held since login, which no join can race. It is now implemented behind
`JOIN_GRACE` (default off). See `tools/roomrace/RESULTS-e2e.md`.

**A second, separate defect surfaced once the full stack was run** (see "What the end-to-end run added" below): `msg.history`
ceilings its Cassandra scan at the room document's `lastMsgAt`, which is advanced
asynchronously. A message sent moments ago is therefore invisible to a plain history
load — and **retrying does not help**, because history-service caches the stale ceiling
for `HISTORY_ROOM_CACHE_TTL` (10 s by default). Measured: 55 retries over 3.5 s still
returned 2 of 3 messages. This is why "just backfill" is not on its own a fix.

> **The race window is dominated by the client, not by NATS.**
> With an instant client, the channel window is under 1 ms. With a realistic 30 ms
> render-then-subscribe, it is 30 ms — a 30–1000× larger target. Making the client
> subscribe faster narrows the window; only a backfill or a per-user delivery path closes it.

---

## 2. Reproduction

`tools/roomrace` drives a real NATS server using the production subject builders, so the
topology under test is the one the services publish on:

| Actor | Modelled as |
|---|---|
| `room-worker` | publish `subscription.update{added}` → `chat.user.{B}.event.subscription.update` |
| `broadcast-worker` | DM → `chat.user.{B}.event.room` (per member); channel → `chat.room.{id}.event` |
| `message-worker` | persists the message before fan-out (so a backfill can find it) |
| `history-service` | request/reply stub on the real `…msg.history` subject |
| desktop client B | login-time subs to its own user subjects; per-room sub opened only when it handles `subscription.update`, after a render delay |

The DM/channel split is not an assumption — it is what `broadcast-worker` does
(`publishDMEvents` vs `publishChannelEvent`) and what its own tests already assert
(`subject.UserRoomEvent("alice")` vs `subject.RoomEvent("room-1", true)` in
`broadcast-worker/handler_test.go`). Those tests were re-run and pass.

### 2.1 Realistic client (30 ms to render + subscribe)

Cell = % of first messages user B never saw. Columns = delay between `subscription.update`
and the first message. 40 iterations per cell.

**Single NATS server**

| scenario | +0ms | +10ms | +20ms | +25ms | +28ms | +30ms | +32ms | +35ms |
|---|---|---|---|---|---|---|---|---|
| dm / client drops unknown room | 100% | 100% | 100% | 100% | 98% | 0% | 0% | 0% |
| dm / client buffers unknown room | 0% | 0% | 0% | 0% | 0% | 0% | 0% | 0% |
| channel / subscribe on update | 100% | 100% | 100% | 100% | 100% | 70% | 0% | 0% |
| channel / subscribe + flush | 100% | 100% | 100% | 100% | 100% | 60% | 0% | 0% |
| channel / subscribe + flush + backfill | 0% | 0% | 0% | 0% | 0% | 0% | 0% | 0% |
| channel / server join grace window | 0% | 0% | 0% | 0% | 0% | 0% | 0% | 0% |
| channel / grace window, client still drops | 100% | 100% | 100% | 100% | 92% | 12% | 0% | 0% |

**3-node cluster (B on node a, services on node b)** — same shape, wider:
`channel / subscribe on update` stays at 100% through +28ms and is still 98% at +30ms.

### 2.2 Instant client (render delay 0) — isolating the transport

| scenario | +0ms | +1ms | +2ms | +3ms | +5ms |
|---|---|---|---|---|---|
| channel / subscribe on update — single server | 78% | 0% | 0% | 0% | 0% |
| channel / subscribe on update — 3-node cluster | 98% | 0% | 0% | 0% | 0% |
| dm (any client behaviour) — both topologies | 0% | 0% | 0% | 0% | 0% |

Subscription interest window (`Subscribe()` returns → a publisher actually reaches it),
measured with ~0.5 ms probe granularity, so read these as upper bounds:

| topology | median | p95 | max | `Flush()` RTT |
|---|---|---|---|---|
| single server | 40–67 µs | ~1.5 ms | 1.6 ms | ~290 µs |
| 3-node cluster (publisher on another node) | ~1.44 ms | ~1.8 ms | 5.0 ms | ~250 µs |

Gateways (their real supercluster) were **not** measured; route propagation across a
gateway is slower than an intra-cluster route, so treat the cluster column as a floor.

---

## 3. Problem 1 — DM

### What actually happens

```
room-worker  ──► chat.user.B.event.subscription.update      (B is subscribed from login)
             ──► async job result ──► A ──► msg.send ──► gatekeeper ──► CANONICAL
broadcast-worker ──► chat.user.B.event.room                 (B is subscribed from login)
```

`broadcast-worker.publishDMEvents` fans a DM out **per member on the user subject**. B holds
that subject from login and never opens a per-room subscription for a DM — the reference
frontend's own test asserts this (`expect(subjects).not.toContain('chat.room.d1.event')`).

So the `new_message` event **is delivered to B's connection**. Your own description says as
much — *"new_message event is already arrived"*. What follows is a client decision: the
handler runs, finds no room in local state, and discards the event.

The harness isolates exactly this. Same transport, same timing, two client behaviours:

* `dm / client drops unknown room` → **100% lost** whenever the message beats the render.
* `dm / client buffers unknown room` → **0% lost, at every delay, on both topologies.**

### Fix — client only, no backend change

Make the `new_message` handler **upsert the room from the event payload** instead of
requiring it to pre-exist. `model.RoomEvent` already carries everything a sidebar row and a
first bubble need: `roomId`, `roomName`, `roomType`, `siteId`, `userCount`, `lastMsgAt`,
`lastMsgId`, and the full `message`. When `subscription.update` lands (before or after), it
reconciles the row with the authoritative subscription record.

Upsert beats "buffer and replay" because it also survives the case where
`subscription.update` is **lost outright** — `room-worker.publishSubscriptionUpdate` is a
best-effort core publish that logs and continues, so a B that is mid-reconnect never sees it.

One caveat: cross-site. `subscription.update` is published on the origin site and routed to
B's home site over the supercluster (`inbox-worker.handleMemberAdded` deliberately does not
re-publish it). Both events then cross a gateway on different subjects with no ordering
guarantee between them, so the arrival order can genuinely invert. The upsert fix handles
that too; a buffer-and-replay fix keyed on "subscription first" does not.

---

## 4. Problem 2 — channel

Your description matches the code exactly, and the harness confirms it is real — and worse
than intuition suggests.

`broadcast-worker.publishChannelEvent` publishes **once** to `chat.room.{id}.event`. B only
subscribes to that subject inside its `subscription.update` handler. Everything published
before that subscription is live is gone: core NATS is at-most-once with no replay.

Three things the measurements settle:

1. **It is not a narrow window.** With a 30 ms render-then-subscribe, every message sent
   within ~30 ms of the subscription event is lost — 100%, not "occasionally".
2. **`Flush()` does not fix it.** 100% → 100% at short delays; it only helps in the
   coin-flip millisecond at the boundary (70% → 60%). `Flush()` confirms your *own* server
   processed the SUB; it says nothing about the publisher's server. On the cluster it
   barely moves (98% → 95%).
3. **Even an instantaneous client loses.** Render delay 0, message at +0 ms: 78% lost on one
   server, 98% on a cluster. There is no client-side "subscribe faster" that reaches 0%.
   NATS exposes no cluster-wide "interest is live" signal — [nats-server#1142](https://github.com/nats-io/nats-server/issues/1142)
   is still open on exactly this.

### What the end-to-end run added (`tools/roomrace/e2e`)

Running the real services changed the picture in two ways. Full numbers in
`tools/roomrace/RESULTS-e2e.md`.

**Correction — the 500 ms creator gap was a harness artifact, not production.**
Earlier runs showed the creator's async job result arriving ~500 ms after the system messages.
That was caused by the minimal test stack omitting `inbox-worker`: `finishCreateRoom`
JetStream-publishes to `chat.inbox.{siteID}.internal.member_added`, the INBOX stream did not
exist, and the publish blocked until its PubAck timed out (`nats: no response from stream`),
delaying the deferred `publishAsyncJobResult`. With `inbox-worker` running the async result
lands at **23–25 ms**. In production INBOX exists, so the real gap is tens of milliseconds.

This makes the client's job **harder**, not easier: the whole first exchange — subscription
events, both system messages, and the first user message — now happens inside ~30 ms, so a
client that takes even 12 ms to subscribe misses most or all of it live (measured: the invited
member missed 97–100% of live events at a 12–30 ms delay). The history read carries the load,
which makes its freshness the whole ballgame.

**The original ordering point still stands.**
`room-worker.finishCreateRoom` publishes `subscription.update` to every member,
*then* the `room_created` / `members_added` system messages, *then* the deferred
`publishAsyncJobResult`. Measured: members get `subscription.update` at 23 ms, the
system messages are broadcast at 29 ms, and the creator's async job result lands at
**529 ms**. A creator who waits for that result before subscribing has already missed
both system messages — which is exactly the reported symptom. Subscribing on the
client's *own* `subscription.update` instead (24 ms) catches them.

**`msg.history` cannot see a just-sent message, and retrying does not help.**
This is the more important finding, and it invalidates "backfill alone" as a fix.

`history-service.walkBounds` sets the scan ceiling to the room document's `lastMsgAt`,
and `LoadHistory` caps `before` at that value. Anything newer than the room document
is therefore invisible. Two delays stack:

* `broadcast-worker` advances `room.lastMsgAt` on a **batched flush**
  (`LAST_MSG_FLUSH_INTERVAL`, default 250 ms);
* `history-service` then **caches** the resolved room times
  (`HISTORY_ROOM_CACHE_TTL`, default 10 s), pinning the stale ceiling.

Measured, with all three messages confirmed present in Cassandra:

| run | alice history | bob history |
|---|---|---|
| baseline | 2 of 3 | 2 of 3 |
| retry until it appears (55 tries / 3.5 s) | **2 of 3** | **2 of 3** |
| same retry, `HISTORY_ROOM_CACHE_SIZE=0` | 3 of 3 (5 tries, ~200 ms) | 3 of 3 (1 try) |
| `meta.lastMsgAt` hint, cache enabled | **3 of 3, first try** | **3 of 3, first try** |

The last row is the fix and it needs no backend change: `msg.history` already accepts
`meta.lastMsgAt` (documented in `docs/client-api.md`, Common request fields). The doc
presents it as a way to skip a MongoDB lookup; it is *also* the scan ceiling, so a
client that supplies a fresh value sees its own just-sent message. Values up to one
hour ahead pass sanitisation, so `Date.now()` is accepted.

**Server-side follow-up worth doing anyway.** Ceiling-at-`lastMsgAt` is an optimisation
to avoid scanning empty buckets, but for a *newest-page* request (no `before` supplied)
it costs nothing to ceiling at `max(lastMsgAt, now)` instead — with
`MESSAGE_BUCKET_HOURS=360` that is almost always the same partition, just a wider
clustering-range upper bound. That removes the sharp edge for every client, including
ones that never send the hint.

### Fix A — client backfill **with a freshness hint** (works today, zero backend change)

On `subscription.update{added}` for a channel: **subscribe first, then read the durable log,
then merge by message ID.**

```
open chat.room.{id}.event  →  Flush()  →  msg.history{limit:N, meta:{lastMsgAt: now}}  →  merge, dedupe by messageId
```

`msg.history` already exists —
`chat.user.{account}.request.room.{roomID}.{siteID}.msg.history`, documented in
`docs/client-api.md`. Harness result: **0% lost at every delay on both topologies** (215 and
241 messages recovered that the live path had dropped).

The ordering is the whole point, and it is Zulip's documented guarantee applied per room:
register the listener before reading state, so anything that arrives during the read is
still delivered. Backfill-then-subscribe re-opens the window.

**The `meta` hint is not optional — see the end-to-end section above.** Without it this recovers nothing for up to 10 s.

**One caveat the NATS-level harness was optimistic about.** It persists before fanning out. In
production `message-worker` and `broadcast-worker` are **separate durable consumers of
MESSAGES-CANONICAL** and run concurrently, so a backfill issued milliseconds after the
fan-out can read Cassandra *before* the write lands. Backfill therefore needs a companion:
gap detection on `lastMsgId` (already on every `RoomEvent` and on `SubscriptionRoom`) — if
the id doesn't chain onto what the client holds, re-fetch. Without it, a backfill can come
back one message short and stay short.

### Fix B — server join grace window (implemented behind `JOIN_GRACE`; the only race-free path)

Have `broadcast-worker` **also** publish a channel's room event to
`chat.user.{X}.event.room` for members whose `joinedAt` is inside a grace window (say 30 s).
Harness result: **0% lost at every delay on both topologies** — and delivered *live*, with
no backfill round trip and no empty-room flicker.

Cost is the obvious objection, and it is answerable. The channel path today does exactly one
publish and never lists subscriptions — that is the point of the room subject. Listing
subscriptions per message would be a real regression for large rooms. Avoid it by gating on
room metadata:

```go
if meta.LastJoinAt != nil && now.Before(meta.LastJoinAt.Add(joinGrace)) {
    // only now pay for the subscription lookup
}
```

`roommetacache.Meta` already carries `CrossSiteAt` for a structurally identical grace window
(`pkg/subject.RoomEventTargets` dual-publishes during the local/global flip so
not-yet-resubscribed clients keep receiving). Adding `LastJoinAt` follows that precedent
exactly. Steady state — no recent joins — costs one nil check and nothing else. Only rooms
with a join in the last N seconds pay for the lookup, and `broadcast-worker.Store` already
exposes `ListSubscriptions`.

**Fix B requires Fix (Problem 1) first.** The grace-window copy arrives on the per-user
subject, so a client that still discards unknown rooms discards it too. The harness makes
this explicit: `channel / grace window, client still drops` = **100% lost**. Server-side
delivery cannot rescue a client that throws the event away.

Fix B also produces duplicates by construction (both the room subject and the user subject
carry the message) — 142 and 300 in the runs above. **Dedupe by message ID is mandatory.**

---

## 5. On the combined create-room + send-message RPC

The idea is directionally right, and the measurements show what it is really doing.

Its value is not "one RPC instead of two". It is that the server gets to put the first
message on **a subject the recipient already holds** instead of one they have to go
subscribe to. That is the same mechanism as Fix B — the grace window is simply that idea
generalised from *the first message* to *every message in the vulnerable window*.

Which exposes the limitation: the combined RPC covers message #1. Message #2, sent 40 ms
later, is back to 100% loss (row 3 of the table). Same for every add-member-then-post in an
existing channel, which the combined RPC does not touch at all.

There is also a construction problem. For the combined event to carry the message, the
server must build the payload itself rather than let it flow through MESSAGES-CANONICAL. But
the message has to reach that pipeline anyway — `message-worker` (Cassandra), `notification-worker`,
`search-sync-worker` all consume it — and `broadcast-worker` will then emit its own
`new_message`. So you end up with two events regardless, and Problem 2 is untouched.

**Recommendation:** build it if you want the round-trip latency win on "start a
conversation", and build it as a *thin orchestrator* — call the existing synchronous
`chat.server.request.room.{site}.create.dm` (`room-worker.serverCreateDM`, already used by
`user-service/roomclient`), then publish the message into MESSAGES exactly as `msg.send`
does. Zero duplicated validation. But do not schedule it as the fix for either problem.

For reference, neither Slack nor Zulip built a compound endpoint: both made DM creation
*implicit on send* ([Slack `chat.postMessage`](https://api.slack.com/methods/chat.postMessage),
[Zulip `POST /messages type=direct`](https://zulip.com/api/send-message)).

---

## 6. Recommended plan

| # | Change | Side | Fixes | Evidence |
|---|---|---|---|---|
| 1 | `new_message` upserts the room from the event payload instead of dropping it | client | **Problem 1, completely** | `dm / client buffers unknown room`: 0% at every delay |
| 1b | Subscribe to the room subject on the client's **own** `subscription.update`, not on the create job result or on room-open | client | the creator missing her own room's system messages | E2E: the whole first exchange completes inside ~30 ms, so this is the only moment early enough to catch any of it |
| 2 | On channel join: subscribe → flush → `msg.history` **with `meta.lastMsgAt`** → merge by message id | client | **Problem 2**, recovered | `channel / … + backfill`: 0% at every delay; E2E: 3 of 3 on the first try |
| 3 | Gap detection on `lastMsgId` + reconcile on reconnect | client | the `message-worker`/`broadcast-worker` write race, and every event lost to a disconnect | see §4 caveat |
| 4 | Join grace window: dual-publish to the per-user subject for members joined < N s ago (`JOIN_GRACE`, default off) | server | **Problem 2**, delivered live — and the only fix that survives a lagging `message-worker` | 0% live loss at client delays 0 ms–1 s; 0% lost with `message-worker` stalled |
| 4b | `msg.history` newest-page ceiling at `max(lastMsgAt, now)` | server | removes the stale-ceiling edge for every client, hint or not | E2E: cache disabled → 3 of 3 |
| 5 | *(optional)* combined DM create+send as a thin orchestrator | server | latency/UX only | §5 |

Steps 1–3 need no backend change at all, and they are still worth doing — they cover DMs,
reconnects, and everything outside the join window. But step 4 is the one that makes Problem 2
go away rather than shrink, and it remains strictly dependent on step 1: a client that discards
a room event for an unknown room discards the grace copy too (measured: 100% still lost).

**The window costs nothing per message.** A channel message must stay one publish to the room
subject — that is the whole reason the room subject exists — so the fan-out does **not** query
the roster. `room-worker` publishes a join notice to `chat.server.joingrace.{siteID}` (core
NATS, *not* queue-subscribed: every `broadcast-worker` replica needs it), each replica holds
the joiners in memory for the window, and the message path does one map lookup that misses in
the steady state. No Mongo read, no Valkey read, no extra round trip.

Two alternatives were rejected. A roster query per message is what the room subject exists to
avoid. Gating that query on a `LastJoinAt` in `roommetacache.Meta` looks cheaper but is
unreliable: the L1 meta cache is per-replica with its own TTL (`ROOM_META_CACHE_TTL`, 2 m) and
`bustRoomMeta` clears only L2, so a member added to an *active* channel — where the entry is
warm — would not be seen until the window had already passed.

Do **not** start with step 4 alone: measured, it fixes nothing.

## 7. Non-recommendations

* **Per-user JetStream consumers for client delivery.** Solves everything including offline,
  but a consumer per connected user does not scale the way core-NATS fan-out does, and it
  replaces a deliberate design.
* **A short-TTL JetStream replay buffer for room events** (Discord's `RESUME` model). More
  general than the grace window, but it puts a JetStream write on the message hot path.
* **Routing all channel events per-user.** Deletes Problem 2 outright, but moves fan-out
  cost from NATS into `broadcast-worker` for every message in every room — the opposite
  direction from the current `chat.local.room.>` traffic-reduction work.
* **"Just subscribe faster / add a Flush."** Measured: 100% → 100%.

## 8. Things to get right

* **Dedupe by message ID everywhere.** Steps 2 and 4 both produce duplicates by design.
* **Never assume `subscription.update` precedes `new_message`.** Different services,
  different subjects, no ordering guarantee — and cross-site they cross a gateway
  independently.
* **Grace window keys off the local subscription's `joinedAt`**, not origin wall-clock, or
  federated members get the wrong window.
* **Docs.** A new `Meta` field is internal, but any client-facing subject or `pkg/model`
  change requires `docs/client-api.md` plus both derived views in the same PR (CLAUDE.md §5).
* **Pre-existing doc drift**, unrelated but worth fixing: `docs/client-api.md` §"ID formats"
  describes the DM room ID as a concat of *accounts*; `idgen.BuildDMRoomID` concatenates
  `user.ID` values.

## References

* [nats-server#1142 — no signal for cluster-wide subscription registration](https://github.com/nats-io/nats-server/issues/1142)
* [NATS — core NATS is at-most-once](https://docs.nats.io/nats-concepts/core-nats/pubsub)
* [Zulip — register the event queue before fetching state](https://zulip.readthedocs.io/en/stable/subsystems/events-system.html)
* [Zulip — send a direct message (implicit conversation)](https://zulip.com/api/send-message)
* [Slack — `chat.postMessage` opens the DM if needed](https://api.slack.com/methods/chat.postMessage)
* [Matrix — de-duplicate by event ID](https://spec.matrix.org/latest/client-server-api/)
* [Discord — gateway sequence numbers and `RESUME`](https://docs.discord.com/developers/events/gateway)
