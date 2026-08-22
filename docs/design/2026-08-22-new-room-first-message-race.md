# New-room → first-message race: analysis and recommendation

**Status:** analysis / recommendation (no code changed)
**Date:** 2026-08-22
**Scope:** `room-service`, `room-worker`, `message-gatekeeper`, `broadcast-worker`, client event contract

---

## 1. TL;DR

The reported symptom ("B gets the subscription event but misses `new_message`") is not one race —
it is **three independent races** stacked on the same transition, and they have different fixes:

| # | Race | Who loses | Applies to |
|---|------|-----------|-----------|
| **R1** | A sends before `room-worker` materialized the subscription → `message-gatekeeper` rejects with *not subscribed* | the **sender** | DM + channel |
| **R2** | B has not yet issued `SUB chat.room.{id}.event` (and NATS has not yet propagated that interest) when `broadcast-worker` publishes | the **recipient** | **channel only** |
| **R3** | B receives `new_message` on a subject it *is* subscribed to, but its local store has no room yet, so the client drops/misfiles the event | the **recipient** | DM + channel |

A combined "create room + send message" RPC — the proposed idea — **fully closes R1**, and closes
R3 *if and only if* the combined event carries the message payload on a subject the recipient is
already subscribed to. **It does not close R2 at all**, and R2 is the only one of the three that is
structurally unfixable on the client side alone.

**Recommendation (layered, in priority order):**

1. **Client contract fix (today, no server change):** the client must wait for the *phase-2* async
   job result before sending, and must tolerate a `new_message` for a room it does not know yet.
2. **Server, small:** on the `subscription.update{action:"added"}` event, populate the already-existing
   `subscription.room.lastMsgId` / `previewMessage` fields so the recipient can render and detect a gap.
3. **Server, targeted — the actual R2 fix:** a **join grace window** in `broadcast-worker` — additionally
   publish a channel's room event to `chat.user.{account}.event.room` for members whose `joinedAt` is
   within N seconds. This mirrors the `roomLocalityGrace` pattern already in `pkg/subject`.
4. **Client, structural:** subscribe-then-backfill on every room entry, dedupe by message ID
   (the Zulip guarantee, adapted). This also fixes the strictly larger bug class: events lost while
   the client was disconnected.
5. **Optional, and only for DMs:** the proposed combined RPC — but built as a *thin orchestrator over
   the existing synchronous DM-create RPC*, not as a second implementation of create + send.

---

## 2. What the code does today

### 2.1 Create

`chat.user.{A}.request.room.{siteID}.room.create` → `room-service` `Handler.createRoom`
(`room-service/handler.go`):

1. `classifyAndValidate` → `RoomTypeDM` (empty name, one counterpart).
2. `handleCreateRoomDMOrBotDM` → `idgen.BuildDMRoomID(requester.ID, other.ID)` (deterministic),
   `FindDMSubscription` dedup (returns `status:"exists"` when the DM already exists).
3. `publishCreateRoom` → `EnsureDEK`, then JetStream publish to `chat.room.canonical.{site}.create`.
4. Replies **`{status:"accepted", roomId, roomType}`** — this is only an *acknowledgement of handoff*.
   No room document and no subscription exists yet at this point.

`room-worker` `processCreateRoom` then does the real work asynchronously:
room insert → `BulkCreateSubscriptions` → `finishCreateRoom` → `publishSubscriptionAdded`
(core publish, per member, to `chat.user.{X}.event.subscription.update`) → federation →
**`defer publishAsyncJobResult`** to `chat.user.{A}.response.{requestID}` (phase 2).

So the ordering guarantee that *does* exist server-side is:
`subscriptions written` → `subscription.update` published to B → `async job result` published to A.

### 2.2 Send

`chat.user.{A}.room.{roomID}.{siteID}.msg.send` → MESSAGES stream → `message-gatekeeper.processMessage`,
which validates and then:

```go
sub, err := h.store.GetSubscription(ctx, account, roomID)
if errors.Is(err, errNotSubscribed) { return nil, err }   // → rejected
```

→ MESSAGES-CANONICAL → `broadcast-worker`.

### 2.3 Fan-out (the decisive detail)

`broadcast-worker/handler.go` has **two different delivery shapes**:

| Room type | Function | Subject |
|---|---|---|
| DM / botDM | `publishDMEvents` | **`chat.user.{account}.event.room`** — one publish per member |
| channel | `publishChannelEvent` → `publishRoomEvent` | **`chat.room.{roomID}.event`** (or `chat.local.room.…`) |

And the reference client (`chat-frontend/src/context/RoomEventsContext/useRoomSubscriptions.js`):

* subscribes **once at login** to `chat.user.{me}.event.room`, `…event.subscription.update`,
  `…event.room.metadata.update`, `…event.room.key`;
* subscribes **lazily, per room** to `chat.room.{id}.event` — and only for channels:
  `if (sub.roomType === 'channel') openChannelSub(sub.roomId, room.crossSite)`, driven by the
  `subscription.update{added}` event. Its own test asserts
  `expect(subjects).not.toContain('chat.room.d1.event')` for a DM.

**Consequence:** for a *DM*, B's client is already listening on the delivery subject before the room
even exists. There is no transport-level race for DMs. Which means the reported DM symptom is
**R1 and/or R3, not R2** — unless the desktop client (unlike the reference frontend) subscribes
per-room for DMs too. This is worth confirming before building anything.

---

## 3. The three races in detail

### R1 — sender-side: "room not ready"

A gets `{status:"accepted"}` and sends immediately. `room-worker` has not yet written the
subscription, so `GetSubscription` returns `errNotSubscribed` and the message is **rejected outright**
(it never reaches MESSAGES-CANONICAL — it is not merely delayed).

Window: one JetStream hop + `GetUser` + room insert + `BulkCreateSubscriptions` + reconcile — typically
tens of ms, but unbounded under load or Mongo pressure.

The contract already provides the fix: `createRoom` is a **two-phase async job**
(`requestWithAsyncResult` in `chat-frontend/src/api/createRoom/index.ts`). A client that sends on the
*sync* reply instead of the *async job result* has this bug by construction.

### R2 — transport-side: "no interest yet" (channels only)

B learns about the room, then calls `nc.subscribe("chat.room.{id}.event")`. That subscription must
reach B's NATS server and be gossiped to every other server (and across gateways, for cross-site).
Core NATS is **at-most-once with no replay**: anything published into that window is gone from the
live path permanently.

NATS gives you **no signal for this**. From
[nats-server#1142](https://github.com/nats-io/nats-server/issues/1142): *"there is no guarantee that
in the moment when the 'server' is publishing the message 'data.xyz', the subscription (RS+) from
'nats-server-A' has been propagated to 'nats-server-B'"* — and the issue is still open, with no
"subscription is now live cluster-wide" primitive. `flush()` only confirms *your own* server processed
the SUB; it says nothing about the rest of the cluster or the gateways.

This is why **no amount of server-side atomicity fixes R2**. Even a perfect single combined
operation that emits the subscription event and the message event in one breath still publishes the
message before B has finished subscribing, because B's subscribe *causally follows* the event it is
reacting to.

### R3 — application-state-side: "unknown room"

`subscription.update` (from `room-worker`) and the room event (from `broadcast-worker`) are published
by **different processes on different subjects**. There is no ordering guarantee between them —
neither NATS nor the code provides one. B can legitimately receive `new_message` for a room it has
never heard of.

The reference frontend survives this: `MESSAGE_RECEIVED` uses
`state.roomState[roomId] ?? emptyRoomState()` and records a preview regardless. A client that
requires the room to exist first will silently drop the event.

### The common root cause

> The system delivers live events over an **at-most-once transport whose subscription set is derived
> from state the client learns over that same at-most-once transport.**

That circularity guarantees a window between *"you are a member"* and *"you are listening"*. Only two
things remove it: (a) don't require a per-room subscription, or (b) reconcile against the durable log
after subscribing. Everything else just narrows the window.

Worth noting the same reasoning covers a strictly larger bug: `publishSubscriptionUpdate` and
`publishDMEvents` are **best-effort core publishes** (both log-and-continue on error). If B is
mid-reconnect, the events are lost with no race involved at all. Whatever you build should fix that
too, or you will be back here.

---

## 4. How comparable systems solve this

| System | Mechanism | Takeaway |
|---|---|---|
| **Zulip** | `POST /register` creates the event queue **before** the initial state is fetched; events that arrive during the fetch are replayed onto the fetched state via `apply_events`. Documented goal: the client sees *either* old state + event, *or* new state + no event — never a mix. ([events-system](https://zulip.readthedocs.io/en/stable/subsystems/events-system.html)) | **Subscribe first, fetch second, merge.** Solve it once on the server side of the contract, not in N clients. |
| **Zulip (DMs)** | There is no "create DM" call at all: `POST /messages` with `type:"direct"`, `to:[user_ids]` — the conversation is implicit. ([send-message](https://zulip.com/api/send-message)) | If DM identity is deterministic, "create" is a redundant step you can delete. |
| **Slack** | `conversations.open` exists, but `chat.postMessage` with a user ID as `channel` **opens the DM if it isn't open already**. ([conversations.open](https://docs.slack.dev/reference/methods/conversations.open/), [chat.postMessage](https://api.slack.com/methods/chat.postMessage)) | Same conclusion, from the biggest product in the category. |
| **Matrix** | `/sync` with a `since` token; clients are told to **de-duplicate by event ID** because ordering across APIs can produce duplicates. ([spec](https://spec.matrix.org/latest/client-server-api/)) | Idempotent merge by ID is the price of admission for any backfill design. |
| **Discord** | Every gateway event carries a sequence `s`; `RESUME` replays from the last `s`. Replay is **bounded** — overflow → `INVALID_SESSION` → full re-fetch. ([gateway](https://docs.discord.com/developers/events/gateway)) | Gap detection + bounded replay + full-resync fallback. |
| **General chat architecture** | The live pub/sub path is best-effort; the durable per-conversation log is the source of truth; clients re-read on reconnect. | The message is already durable in Cassandra here. The fix belongs in *reconciliation*, not in making the live event perfect. |

Not one of these systems solves this with a compound "create + send" endpoint. Slack and Zulip
converge on **implicit creation on send**; everyone converges on **subscribe-then-backfill with
idempotent merge**.

---

## 5. Solution catalogue

### S1 — Client waits for the phase-2 async job result before sending

* **Fixes:** R1 only.
* **Pros:** zero server change; it is already the documented contract and what the reference client does.
* **Cons:** adds the full create latency to the first send; nothing else improves.
* **Verdict:** mandatory baseline, not a solution on its own.

### S2 — Combined `create room + send message` RPC (the proposal)

Server creates the room, writes the message, emits one combined fan-out.

* **Fixes:** R1 completely (the room provably exists before the message is accepted).
  R3 **only if** the combined event carries the message and lands on a subject B already holds.
  R2: **not at all**.
* **Pros:**
  * One round trip; best possible latency for "start a conversation" — the single most latency-visible
    action in the product.
  * Creates a **server-side ordering point**: subscription-added can be published *after* the message
    exists, so the event can carry `room.lastMsgId` / `previewMessage` — B renders the DM with its
    first message from a single event.
  * Matches Slack/Zulip product semantics.
  * Message IDs are client-supplied 20-char base62, so a retry is naturally idempotent.
* **Cons:**
  * **Only fixes the first message.** A second message 50 ms later hits the identical R2/R3 window in a
    channel. The race is not about *the first* message; it is about *any* message that arrives before
    the recipient is listening.
  * If it re-implements create and send, it duplicates: validation, attachments/quote/thread
    resolution, mention extraction, large-room gating, encryption, DEK provisioning, sys-messages,
    federation. Two code paths that must not drift — exactly the failure mode CLAUDE.md's
    "define interfaces in the consumer" discipline exists to avoid.
  * If instead it publishes into MESSAGES, the send is async again and the "single atomic fan-out" is
    gone (which is fine — see the recommendation).
  * Combinatorial API growth: create-channel+message, add-member+message, …
  * A synchronous RPC now covers Vault (DEK), Mongo writes, fan-out and federation — the reply timeout
    budget has to cover the slowest of those, and partial failure ("room created, message rejected")
    needs an explicit reply shape.

### S3 — Fat `subscription.update{added}` event (embed the first message)

`SubscriptionRoom` **already has** `LastMsgID`, `LastMsgAt` and `PreviewMessage`; they are simply not
populated at create time (there is no message yet). Combined with S2's ordering point they can be.

* **Fixes:** R3 for the first message.
* **Pros:** tiny delta; no new subjects; no new RPC; helps every client immediately.
* **Cons:** covers only what is known at publish time. Not a general fix.

### S4 — Client: subscribe-then-backfill, dedupe by message ID (the Zulip pattern)

On `subscription.update{added}` — and on every room entry: (1) open the room subscription first,
(2) buffer incoming events, (3) `loadHistory` since `joinedAt`/`historySharedSince`, (4) merge and
dedupe by message ID.

* **Fixes:** R2 and R3, generally — plus everything lost to disconnects.
* **Pros:** the industry-standard answer; no API surface change; self-healing; the durable message is
  already in Cassandra and `history-service` already exposes it.
* **Cons:** frontend work in every client; one extra history RPC per new room; must be genuinely
  idempotent (Matrix's warning). Does not by itself close the sub-millisecond interest-propagation
  gap — the backfill must run *after* the subscribe, which is exactly why the ordering matters.

### S5 — Gap detection from `lastMsgId`

`RoomEvent` already carries `LastMsgID`/`LastMsgAt` (`buildRoomEvent`). Keep a per-room
`lastKnownMsgId`; when an event's `lastMsgId` doesn't chain, backfill. Re-reconcile on reconnect.

* **Fixes:** all three, after the fact (self-healing rather than preventive).
* **Pros:** cheap; makes every loss cause recoverable, not just this one; Discord's model.
* **Cons:** eventually-consistent — the user may see the message a beat late; needs a per-room cursor.

### S6 — Durable per-user delivery (JetStream consumer per client)

* **Fixes:** everything, including offline delivery.
* **Cons:** a consumer per connected user does not scale the way core-NATS fan-out does; ack/state
  management; replaces the deliberate core-NATS FE design. Long-term option, not this fix.

### S7 — Route channel events per-user (or: a **join grace window**)

Full version: deliver channel events on `chat.user.{X}.event.room` like DMs, so no per-room
subscription ever exists. Fixes R2 by deleting it — but moves fan-out cost from NATS into
`broadcast-worker` (N publishes per message), against the direction of the current
`chat.local.room.>` traffic-reduction work.

**Narrow version — recommended:** publish the channel room event **additionally** to
`chat.user.{X}.event.room` for members whose `joinedAt` is within a grace window (e.g. 30 s).

* **Fixes:** R2, precisely, for exactly the members who can be affected by it.
* **Pros:** steady-state fan-out cost unchanged; there is already a grace-window precedent in
  `pkg/subject` (`roomLocalityGrace` dual-publishes during the local/global flip so
  not-yet-resubscribed clients keep receiving) — this is the same idea applied to the join transition.
  Clients dedupe by message ID, which S4/S5 require anyway.
* **Cons:** duplicate delivery during the window (harmless once dedupe exists); needs `joinedAt` in the
  fan-out path (`ListSubscriptions` already returns subscriptions; room-meta cache may need the field).

### S8 — Implicit DM creation on send (Zulip/Slack semantics)

Client sends to the deterministic DM room ID; `message-gatekeeper`, on `errNotSubscribed` for a
well-formed DM room ID that includes the sender, calls the **existing synchronous**
`chat.server.request.room.{site}.create.dm` RPC (`room-worker.serverCreateDM`, already implemented and
already used by `user-service/roomclient`), then proceeds.

* **Fixes:** R1; deletes the "create" step from the client flow entirely.
* **Pros:** no new client API; reuses one create implementation and the whole existing send pipeline;
  matches Slack/Zulip.
* **Cons:** the client must compute the DM room ID, which is `sortedConcat(userA.ID, userB.ID)` — that
  leaks an ID-construction rule into clients (and `docs/client-api.md` currently describes it as a
  concat of *accounts*, which does not match `idgen.BuildDMRoomID`; that doc drift needs fixing either
  way). Also puts a synchronous room-create in the gatekeeper hot path — needs a guard so it can only
  fire for DM-shaped IDs.

---

## 6. Verdict on the proposed combined RPC

**It is a good idea for the sender's problem and a decent idea for the product, but it is not the fix
for the recipient's problem.** Specifically:

* ✅ It removes R1 by construction — the strongest argument for it.
* ✅ It creates the ordering point that lets `subscription.update{added}` carry the first message (S3),
  which closes R3 for the common case.
* ❌ It does nothing for R2. For a channel, B still has to subscribe to `chat.room.{id}.event` *after*
  processing the combined event, and NATS provides no way to know when that subscription is live
  ([nats-server#1142](https://github.com/nats-io/nats-server/issues/1142)).
* ❌ It fixes exactly one message. The same race recurs on message #2, and on *every* add-member +
  immediate-post in a channel.

If you build it, build it as a **thin orchestrator, not a second implementation**:

1. Call the existing synchronous `RoomCreateDMSync` (`room-worker.serverCreateDM`) — it already does
   deterministic ID, idempotent insert, dup-key reconcile, DEK provisioning, subscription pair
   creation, `subscription.update` fan-out, and cross-site inbox.
2. Then publish the client's message to MESSAGES exactly as `msg.send` would, so it flows through
   `message-gatekeeper` → MESSAGES-CANONICAL → `broadcast-worker` unchanged.
3. Reply with `{roomId, subscription, messageAccepted}` — and define the partial-failure shape
   explicitly (room created + message rejected must not read as total failure).

That gets one round trip and zero duplicated logic. It does **not** get a single combined fan-out
event — and it shouldn't try to: the message must go through the canonical pipeline so that
`message-worker` (Cassandra), `notification-worker`, and `search-sync-worker` all see it.

---

## 7. Recommended plan

**Phase 0 — confirm which race you actually have (do this first).**
For a DM, `broadcast-worker` delivers on `chat.user.{B}.event.room`, which B holds from login. So
either the desktop client subscribes per-room for DMs (unlike the reference frontend), or it is
dropping a `new_message` for an unknown room (R3), or it is sending on the sync `accepted` reply (R1).
These have different fixes; picking one blind risks building the wrong thing. Add a client-side counter
for "room event received for unknown room" and a `message-gatekeeper` metric for `errNotSubscribed`
rejections within N seconds of a room create.

**Phase 1 — cheap, no new API.**
* Client: send only after the **phase-2 async job result** (fixes R1).
* Client: accept `new_message` for an unknown room — synthesize the room and refetch the subscription
  (fixes R3, matching the reference frontend's reducer).
* Client: on `subscription.update{added}` for a channel, **open the room subscription before** applying
  state, then backfill (S4 ordering).

**Phase 2 — the actual R2 fix.**
* `broadcast-worker`: join grace window (S7 narrow) — dual-publish channel events to
  `chat.user.{X}.event.room` for members with `joinedAt` inside the window.
* Client: dedupe by message ID (required by the dual publish; also required by Phase 3).

**Phase 3 — make it self-healing.**
* Client: gap detection on `lastMsgId` (S5) + full reconcile on reconnect. This retires the whole bug
  class, including best-effort publishes lost while disconnected.

**Phase 4 — optional product/latency work.**
* The combined DM RPC as a thin orchestrator (S2 + S3), or the implicit-create-on-send variant (S8).
  Do this for the UX win, once Phases 1–3 mean it is not load-bearing for correctness.

**Explicitly not recommended now:** S6 (per-user JetStream consumers) and full S7 (all channel events
per-user). Both are large, and Phases 1–3 make them unnecessary.

---

## 8. Things to get right

* **Idempotency.** Message IDs are client-supplied 20-char base62 — dedupe on that everywhere. Matrix
  makes this explicit for exactly this reason.
* **Ordering assumptions.** Do not assume `subscription.update` precedes the room event. Different
  publishers, different subjects, no guarantee. Write the client so either order works.
* **Federation.** A cross-site B receives the same events via INBOX. The grace window must key off the
  *subscription's* `joinedAt` at the delivering site, not wall-clock at the origin.
* **Docs.** Any change to a client-facing subject or to a `pkg/model` client-facing struct requires
  updating `docs/client-api.md` **and** both derived views in the same PR (CLAUDE.md §5). Note the
  existing drift: §"ID formats" describes the DM room ID as a concat of *accounts*, but
  `idgen.BuildDMRoomID` concatenates `user.ID` values.
* **TDD.** All of the above is new behaviour — tests first (CLAUDE.md §4).

---

## 9. Open questions

1. Does the desktop client subscribe per-room for **DMs**, or only for channels like the reference
   frontend? This determines whether the reported bug is R2 or R3.
2. Does the desktop client send the first message on the **sync** `accepted` reply or on the **async
   job result**? This determines whether R1 is in play.
3. Is "create channel + immediately post" (not just DM) a real flow in the product? If yes, R2 is
   in scope and Phase 2 is required, not optional.

---

## References

* [NATS — cannot say when a subscription is registered across the entire cluster (nats-server#1142)](https://github.com/nats-io/nats-server/issues/1142)
* [NATS — Publish-Subscribe (core NATS is at-most-once)](https://docs.nats.io/nats-concepts/core-nats/pubsub)
* [NATS — Super-cluster with Gateways](https://docs.nats.io/running-a-nats-service/configuration/gateways)
* [Zulip — Real-time push and events (queue-before-fetch atomicity, `apply_events`)](https://zulip.readthedocs.io/en/stable/subsystems/events-system.html)
* [Zulip — Send a message (`type:"direct"`, implicit conversation)](https://zulip.com/api/send-message)
* [Slack — `conversations.open`](https://docs.slack.dev/reference/methods/conversations.open/)
* [Slack — `chat.postMessage` (opens the DM if not already open)](https://api.slack.com/methods/chat.postMessage)
* [Matrix — Client-Server API `/sync`, de-duplicate by event ID](https://spec.matrix.org/latest/client-server-api/)
* [Discord — Gateway sequence numbers and `RESUME`](https://docs.discord.com/developers/events/gateway)
