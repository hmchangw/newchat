# Enriched "added" subscription.update — Design

**Date:** 2026-08-01
**Status:** Approved

## Problem

When the frontend first loads it calls `subscription.list`, whose rows carry a nested
`room` object (`model.SubscriptionRoom`) with everything needed to render a sidebar
entry: canonical name, `crossSite`, `userCount`, `appCount`, `lastMsgAt`, `lastMsgId`,
`lastMentionAllAt`, `minUserLastSeenAt`, and the E2E room key
(`privateKey`/`keyVersion`). After that, the FE maintains its local store from
real-time events.

The `subscription.update` event with `action: "added"` — what a newly added member
receives — carries only the bare persisted `Subscription` plus a flat `roomName`
string. `Subscription.Room` is a read-time-only field populated exclusively by
user-service's list enrichment, and none of room-worker's publish sites set it. The
new member's FE therefore cannot render the room like a list row and must correlate a
separate `room.key` event to get the key.

## Decisions (from brainstorming)

1. **Scope:** ALL `"added"` publish paths get the enriched room object — member.add,
   create-room, DM-sync, self-DM — so the FE handles one uniform event shape.
2. **Room key:** `privateKey`/`keyVersion` are embedded in the event's `room` object
   (full parity with `subscription.list`). Consequently the separate `room.key`
   fan-out for initial key delivery on add/create is **removed** — it is redundant.
   The rotation fan-out (new key to survivors on member removal) and the
   `room.key.get` recovery RPC remain.
3. **`previewMessage`:** stays out. It is not on the room doc (subscription.list
   resolves it via a history-service RPC at read time); including it would add a
   synchronous cross-service call to room-worker's hot path, and a member added
   without shared history arguably shouldn't see the prior last message anyway.
   The FE fills it in from the next message event or `subscription.list`.

## Design

### 1. Event contract

No struct changes. `SubscriptionUpdateEvent` already embeds `Subscription`, whose
`Room *SubscriptionRoom` field (`json:"room,omitempty"`, `bson:"-"`) is simply never
set at publish time today. On every `"added"` publish room-worker now populates it:

| Field | Source |
|-------|--------|
| `siteId` | `room.SiteID` |
| `name` | `room.Name` (canonical room name; the sub's own display `name` is untouched) |
| `crossSite` | `room.CrossSite` (tri-state pointer passes through; FE `?? true` fail-safe) |
| `userCount` / `appCount` | `room.UserCount` / `room.AppCount` |
| `lastMsgAt` / `lastMsgId` | `room.LastMsgAt` / `room.LastMsgID` |
| `lastMentionAllAt` | `room.LastMentionAllAt` |
| `minUserLastSeenAt` | `room.MinUserLastSeenAt` |
| `privateKey` / `keyVersion` | key pair fetched from the room key store; omitted when the room is keyless |
| `previewMessage` | always omitted (see Decisions) |

The flat `RoomName` event field stays for back-compat.

Key encoding is already consistent on the wire: `SubscriptionRoom.PrivateKey` is
std-base64 of the 32-byte secret (matching user-service's `buildLocalRoom`), and the
legacy `room.key` event's `[]byte` key marshals to the identical base64 string.

### 2. room-worker changes

One new helper alongside `newSub` / `resolveSubUpdateRoomName`:

```go
// subscriptionRoomFor builds the read-model room view for an "added"
// subscription.update; nil pair (DM/botDM/self-DM) omits the key fields.
func subscriptionRoomFor(room *model.Room, pair *roomkeystore.VersionedKeyPair) *model.SubscriptionRoom
```

Wired at the four `"added"` publish sites:

- **member.add** (`handleAddMembers`): move the existing `keyStore.Get` from after
  the subscription.update loop to before it, and pass the pair into the event build.
  A `Get` failure still fails the handler → JetStream redelivery, so key-delivery
  reliability is unchanged. `needIRM` accounts (already-subscribed) get no event,
  same as today.
- **create-room** (`finishCreateRoom`): `pair` is already a parameter — pass it
  through to the event build.
- **DM-sync + self-DM** (`publishSubscriptionUpdates`): extend the signature to
  accept `room *model.Room`; both call sites have it in scope. Pair is nil — these
  rooms are keyless by design.

### 3. room.key fan-out removal

Removed (redundant with the embedded key):

- member.add initial-key fan-out to newly subscribed users.
- create-room initial-key fan-out to all initial members (including its NAK-on-
  failure path — the key now rides the subscription.update publish).

Kept:

- rotation fan-out to survivors on member removal (`fanOutRoomKeyToSurvivors`) —
  that delivers a NEW key to EXISTING members, which no `"added"` event covers.
- the `room.key` event type, `fanOutKey`/`buildAndFanOutRoomKey` plumbing used by
  rotation, and the `room.key.get` RPC (FE recovery path).

### 4. Cross-site

Zero changes to inbox-worker / outbox-worker. The enriched event is built at the
room's home site — the authority for all these fields — and already routes to the
member's home site via the user-scoped subject over the NATS supercluster.
inbox-worker's `member_added` handler keeps creating the bare persisted sub;
`Subscription.Room` is `bson:"-"` and is never persisted anywhere.

### 5. Frontend (chat-frontend)

On `action: "added"`, merge `event.subscription` — including its `room` object —
into local storage exactly like a `subscription.list` row (the `crossSite ?? true`
fail-safe already exists). Keep handling `room.key` events (rotation). If a key is
absent when needed (rollout skew), fall back to the `room.key.get` RPC.

### 6. Docs

Same PR updates:

- `docs/client-api.md`: the subscription.update `"added"` event now carries
  `subscription.room` (reference the existing SubscriptionRoom schema table, note
  `previewMessage` is omitted on events); `room.key` documented as
  rotation/recovery-only (no longer sent on add/create).
- `docs/client-api/events.md`: same changes in the derived events view.

### 7. Error handling & reliability

- `keyStore.Get` failure on member.add fails the handler before any publish →
  JetStream redelivers, as today.
- The subscription.update publish itself remains best-effort (log-and-continue),
  as today; a missed event is recovered by the next `subscription.list`, which
  already delivers the key.
- The event travels the same user-scoped, JWT-authorized subject the `room.key`
  event uses today — no new exposure. Event payloads (which now contain the key)
  must never be logged; existing publish sites log only account/roomId.

### 8. Testing (TDD)

Red-first in `room-worker/handler_test.go`, table-driven per path:

- member.add: event carries the full `room` object incl. key; no `room.key`
  publish; `keyStore.Get` failure fails the handler before any subscription.update
  publish.
- create-room: each initial member's event carries the room object incl. key; no
  `room.key` publish.
- DM-sync / self-DM / botDM: room object present, key fields absent (nil pair).
- rotation fan-out on member removal: unchanged (regression guard).

Integration tests updated where they assert on the removed `room.key` publishes or
on event payloads.

### 9. Rollout note (accepted)

Old FE builds only extract keys from `room.key` events; once the backend stops
sending those on add/create, such builds won't store keys for new rooms until the
next `subscription.list`. Deploy the updated FE with or before the backend to avoid
the skew entirely.
