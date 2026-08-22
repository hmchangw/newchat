# Thread-scoped event subject for open thread panels

**Status:** approved
**Date:** 2026-08-22

## Problem

A user who opens a thread panel but has never replied is not a thread follower, so
`broadcast-worker` never delivers new thread replies to them. The panel silently goes
stale until the user refetches.

`broadcast-worker/handler.go:1143` (`channelThreadFanOut`) builds the channel recipient
set as *reply sender ∪ parent author ∪ `thread_rooms.replyAccounts` ∪ history-gated
@-mentions*, then publishes once per recipient on `chat.user.{account}.event.room`. A
viewer is in none of those sets.

The root cause is that one set is doing two jobs: **notification interest** (who wants to
be told later) and **delivery interest** (who is looking right now). Followership is the
right answer to the first question and the wrong answer to the second.

## Solution

Publish thread events a second time onto a **thread-scoped subject** derived from the room
subject. A client subscribes to it while the thread panel is open and unsubscribes when it
closes, so NATS's own subscription table becomes the viewer registry.

```
chat.room.{roomID}.thread.{parentMessageID}.event         # cross-site rooms
chat.local.room.{roomID}.thread.{parentMessageID}.event   # same-site rooms
```

The per-follower fan-out is unchanged. Followers who also have the panel open receive the
event twice and the client dedupes.

### Why this shape

- **No new state.** NATS tracks interest and reaps it on disconnect — no TTL, no heartbeat,
  no sweeper, no leak-on-crash, no cross-site replication.
- **No new hot-path I/O.** `handleThreadCreated` already holds `roommetacache.Meta`, which
  already carries `CrossSite`/`CrossSiteAt`. Routing reuses `roomRouteGlobals`, so a
  same-site room stays on the leaf-filtered local namespace and never touches a gateway.
- **No auth change.** Client JWTs already grant `sub` on `chat.room.>` and
  `chat.local.room.>` (`auth-service/handler.go:313`).
- **Zero cost when unwatched.** A publish with no interest is dropped at the server.
- **Same envelope.** Identical payload and event types (`new_thread_message`,
  `message_edited`, `message_deleted`), same room-key encryption, so existing client
  handlers work unchanged and create/edit/delete are covered uniformly.

## Scope

Channel rooms only. DM and botDM thread replies already fan out to every member
(`handleThreadCreated`, `RoomTypeDM` branch), so no viewer gap exists there.

## Design

### `pkg/subject`

```go
func RoomThreadEvent(roomID, parentMessageID string, global bool) string
func RoomThreadEventTargets(roomID, parentMessageID string, crossSite *bool,
    crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []string
```

`RoomThreadEvent` appends `.thread.{parentMessageID}.event` to `roomBase(roomID, global)`.
`RoomThreadEventTargets` mirrors `RoomEventTargets` exactly — same `roomRouteGlobals` call,
so local/global routing and the post-flip dual-publish grace window behave identically.

Six-token subjects cannot collide with the four-token `chat.room.*.event` pattern used by
existing wildcard subscribers.

### `broadcast-worker`

New config field on `config`:

| Env | Type | Default | Purpose |
|---|---|---|---|
| `THREAD_VIEW_SUBJECT_ENABLED` | bool | `true` | Kill switch for the thread-subject publish |

New handler field `threadViewSubject bool`, wired through `NewHandler`.

A single helper mirrors an already-published payload onto the thread subject:

```go
func (h *Handler) publishThreadViewEvent(ctx context.Context, roomID, parentMsgID string,
    crossSite *bool, crossSiteAt *time.Time, payload []byte)
```

Called from the `RoomTypeChannel` branch of `handleThreadCreated`, `handleThreadUpdated`,
and `handleThreadDeleted`, immediately after the payload is marshalled and **before** the
per-follower fan-out — so a per-follower failure that NAKs the message still leaves the
viewer served.

All three handlers currently short-circuit when the follower set is empty
(`handleThreadCreated:263`, `handleThreadUpdated:381`, `handleThreadDeleted:433`). That is
exactly the case a lone viewer hits, so the short-circuits are removed and the handlers fall
through to marshal and publish. `publishToThreadAccounts` already no-ops on an empty set.

**Best-effort by design.** The helper returns nothing and never fails the handler. The
per-follower fan-out has already committed; returning an error would NAK and redeliver,
duplicating that fan-out. A failure is logged and counted. Viewers reconcile on panel open.

No-ops when the flag is off or when `parentMsgID` is empty (an unroutable subject token).

### Metrics

One new instrument, failures only:

```
broadcast_worker_thread_view_publish_failures_total{event_type}
```

No success or volume counter. Publish attempts still flow through the existing
`broadcastMetricPublisher` wrapper, so no instrumentation is removed.

### `chat-frontend`

- `subjects.ts` — `roomThreadEvent(roomId, parentMessageId, crossSite)`, mirroring
  `roomEvent`'s fail-safe (only an explicit `false` routes local).
- `subscribeToThreadEvents/` — new API module in the shape of `subscribeToRoomEvents/`.
- Thread panel — subscribe on open, unsubscribe on close, mirroring `openChannelSub`.

**Ordering matters.** Subscribe *first*, then fetch thread messages, then merge. Fetching
first leaves a gap in which a reply arrives before the subscription exists and is lost.

**Dedup.** Already handled: `threadEventsReducer` drops a `THREAD_REPLY_RECEIVED` whose id is
already present (`reducer.js:78`), and `REPLY_EDITED`/`REPLY_DELETED` are idempotent. A
follower with the panel open needs no new client logic.

## Error handling

| Failure | Behavior |
|---|---|
| Thread-subject publish fails | Log, increment failure counter, continue. Handler returns nil. |
| Per-follower fan-out fails | Unchanged — error returned, JetStream redelivers. |
| Flag off | No thread-subject publish; per-follower path unchanged. |
| Empty `parentMessageID` | Skipped — an unroutable subject token. |
| Empty follower set | Thread-subject publish still happens; per-follower fan-out no-ops. |
| Client reconnects | NATS drops stale interest; the panel resubscribes and refetches. |

## Testing

**`pkg/subject`** — table-driven over local/global/dual-publish and the grace window;
assert the six-token shape and that it does not match `chat.room.*.event`.

**`broadcast-worker` unit** — thread created, edited, and deleted each publish to the thread
targets; flag off publishes nothing; empty parent ID skipped; publish failure increments the
counter and still returns nil; DM rooms publish nothing on the thread subject.

**`broadcast-worker` integration** — real NATS via `testutil.NATS(t)`: subscribe to the
thread subject as a non-follower, feed a canonical thread-reply event, assert the event
arrives with the expected type and payload.

**`chat-frontend` vitest** — subject builder including the `crossSite` fail-safe;
subscription opens on `openThread` and closes on `closeThread`; a duplicate reply arriving on
both lanes lands once.

## Documentation

`docs/client-api.md` gains the thread-scoped subject and the events that ride it, and the
Send / Edit / Delete "Triggered events" bullets are updated. `docs/client-api/events.md` is
updated to match, per the same-PR rule in CLAUDE.md §5.

## Out of scope

- Removing or changing the per-follower fan-out.
- Any change to followership, `thread_rooms.replyAccounts`, thread subscriptions, thread
  unread badges, or push notification targeting.
- A server-side viewer registry, presence integration, or "who is viewing" surface.
