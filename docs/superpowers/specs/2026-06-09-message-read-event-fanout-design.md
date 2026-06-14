# MessageReadEvent fan-out in the `messageRead` RPC

**Date:** 2026-06-09
**Status:** Approved — ready for implementation plan
**Service:** `room-service`

## Problem

When a user marks a room read via the `messageRead` RPC, the handler recomputes
the room's strict read floor (`rooms.minUserLastSeenAt` — the `MIN(lastSeenAt)`
across all subscriptions). That recomputed floor is the signal other clients
need to advance read-receipt / unread-marker UI, but today it is written to
Mongo silently — no live event is emitted, so peers only learn of the new floor
on their next refetch.

This adds a typed `message_read` fan-out event published whenever the floor
actually moves.

## Goals

- Publish a `MessageReadEvent` from `messageRead` **only when the recomputed
  floor changes** (the existing `!sameFloor(...)` branch).
- Channel rooms fan out once to the room's shared event subject; DM rooms fan
  out per-subscriber.
- Fan-out is best-effort: a publish failure never fails the RPC (the floor write
  is the source of truth).

## Non-Goals

- `RoomTypeBotDM` rooms — their floor always resolves to `nil`
  (`MinSubscriptionLastSeenByRoomID` counts the bot subscription, which never
  reads), and the task scopes DM only. Not handled.
- No encryption branch — the event carries no message content.
- No cross-site outbox for this event — it is a same-site live UI hint. The
  existing `subscription_read` outbox is unchanged.

## Design

### 1. Model — `pkg/model/event.go`

Add a new room-event discriminator to the existing `RoomEventType` const block:

```go
RoomEventMessageRead RoomEventType = "message_read"
```

Add a flat event struct, following the `EditRoomEvent` / `DeleteRoomEvent`
convention (no zero-valued `RoomEvent` base fields shipped to clients):

```go
// MessageReadEvent is the live event published when a room's read floor
// (minUserLastSeenAt) advances after a member marks the room read. Channel
// rooms receive it once on the room event subject; DM rooms receive it per
// member on their user event subject. MinUserLastSeenAt is nil when any member
// is still fully unread (the floor cannot be established), in which case the
// field is omitted.
type MessageReadEvent struct {
	Type              RoomEventType `json:"type" bson:"type"`
	RoomID            string        `json:"roomId" bson:"roomId"`
	MinUserLastSeenAt *time.Time    `json:"minUserLastSeenAt,omitempty" bson:"minUserLastSeenAt,omitempty"`
	Timestamp         int64         `json:"timestamp" bson:"timestamp"`
}
```

**No `SiteID` field:** unlike the rest of the live room-event family, this event
omits `siteId`. Clients key rooms by `roomId` and already cache each room's
origin `siteId` (from `rooms.list` / `subscription.update`); a `message_read`
event can only ever update a room the client is already subscribed to and
rendering — it is never first-contact — so `siteId` is redundant for every
consumer of this event.

**Field-type rationale:**

- `MinUserLastSeenAt` is `*time.Time` (RFC3339), matching the live fan-out event
  family (`RoomEvent.LastMsgAt`, `EditRoomEvent.EditedAt`, `DeleteRoomEvent.DeletedAt`
  are all `time.Time`) rather than the `*int64`-millis form used by the
  `RoomInfo` batch-RPC DTO. It is a **pointer** because the read floor is
  genuinely nullable — `MinSubscriptionLastSeenByRoomID` returns `*time.Time` and
  yields `nil` when any member has never read. This matches the source
  `room.MinUserLastSeenAt` field exactly (zero-conversion pass-through) and
  preserves the null floor instead of serializing a misleading zero time.
- `Timestamp` is `int64` UnixMilli, set at the publish site via
  `time.Now().UTC().UnixMilli()` — the event-level timestamp required of every
  NATS event struct (CLAUDE.md §6).

Covered by `pkg/model/model_test.go`'s generic `roundTrip` marshal/unmarshal
helper.

### 2. Handler — `room-service/handler.go`

In `messageRead`, the tail already recomputes the floor and writes it only when
changed:

```go
if !sameFloor(minTime, room.MinUserLastSeenAt) {
	if err := h.store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime); err != nil {
		return nil, fmt.Errorf("update room minUserLastSeenAt: %w", err)
	}
	// NEW: fan out the read-floor advance, dispatched by room type.
	switch room.Type {
	case model.RoomTypeChannel:
		h.publishChannelEvent(ctx, roomID, minTime)
	case model.RoomTypeDM:
		h.publishDMEvents(ctx, roomID, minTime)
	}
}
return &model.StatusReply{Status: "accepted"}, nil
```

- The event carries the **new** floor (`minTime`, the value just written), not
  the prior `room.MinUserLastSeenAt`.
- No event fires on the early-return paths (`room.LastMsgAt == nil`, reader
  already caught up) or when `sameFloor` holds — those paths return before this
  block, preserving the existing hot-path no-op behaviour.
- Both fan-out methods are best-effort and return nothing; a publish failure is
  logged and swallowed, never propagated, so the RPC still returns `accepted`
  (mirrors the existing `publishSubscriptionUpdate` best-effort pattern).

### 3. Fan-out methods — `room-service/handler.go`

Names per the task; signatures shaped to this RPC's context.

```go
// buildMessageReadEvent constructs the wire payload for a read-floor advance.
func (h *Handler) buildMessageReadEvent(roomID string, floor *time.Time) model.MessageReadEvent {
	return model.MessageReadEvent{
		Type:              model.RoomEventMessageRead,
		RoomID:            roomID,
		MinUserLastSeenAt: floor,
		Timestamp:         time.Now().UTC().UnixMilli(),
	}
}

// publishChannelEvent fans a read-floor advance out once to the channel's shared
// room event subject. Best-effort: a marshal/publish failure is logged, not
// returned. Used for RoomTypeChannel.
func (h *Handler) publishChannelEvent(ctx context.Context, roomID string, floor *time.Time) {
	evt := h.buildMessageReadEvent(roomID, floor)
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal message_read channel event failed", "error", err, "roomId", roomID)
		return
	}
	if err := h.publishCore(ctx, subject.RoomEvent(roomID), payload); err != nil {
		slog.Error("publish message_read channel event failed", "error", err, "roomId", roomID)
	}
}

// publishDMEvents fans a read-floor advance out to each DM member on their
// per-user event subject. Mirrors broadcast-worker's publishDMEvents: it lists
// the room's subscriptions and publishes once per subscriber. Best-effort per
// account. Used for RoomTypeDM.
func (h *Handler) publishDMEvents(ctx context.Context, roomID string, floor *time.Time) {
	subs, err := h.store.ListSubscriptionsByRoom(ctx, roomID)
	if err != nil {
		slog.Error("list subscriptions for message_read DM fan-out failed", "error", err, "roomId", roomID)
		return
	}
	evt := h.buildMessageReadEvent(roomID, floor)
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal message_read DM event failed", "error", err, "roomId", roomID)
		return
	}
	for i := range subs {
		account := subs[i].User.Account
		if err := h.publishCore(ctx, subject.UserRoomEvent(account), payload); err != nil {
			slog.Error("publish message_read DM event failed", "error", err, "roomId", roomID, "account", account)
		}
	}
}
```

- Channel → `subject.RoomEvent(roomID)` (one publish).
- DM → `subject.UserRoomEvent(account)` per subscription via
  `h.store.ListSubscriptionsByRoom(ctx, roomID)`.
- Transport is `h.publishCore` (core NATS) — the live-hint transport already used
  for `subscription.update` fan-out in this handler.

### Data flow

```
client → messageRead RPC (chat.user.{account}.room.{roomID}...read)
  → UpdateSubscriptionRead
  → [GetUserSiteID ∥ GetRoom]
  → cross-site subscription_read outbox (unchanged)
  → recompute floor (MinSubscriptionLastSeenByRoomID)
  → if floor changed:
       UpdateRoomMinUserLastSeenAt
       ├─ Channel: publishCore → chat.room.{roomID}.event           (MessageReadEvent)
       └─ DM:      publishCore → chat.user.{account}.event.room  ×N  (MessageReadEvent)
  → reply {status: accepted}
```

## Testing (TDD, Red → Green → Refactor)

### Model — `pkg/model/model_test.go`
- Add `MessageReadEvent` to the `roundTrip` coverage (non-nil and nil
  `MinUserLastSeenAt`).

### Handler — `room-service/handler_test.go`
New table-driven cases on `messageRead`, capturing publishes through the
injected `publishCore` and a mocked store:

| Case | Expectation |
|------|-------------|
| Channel room, floor moves | one `publishCore` on `subject.RoomEvent(roomID)`; payload has `type=message_read`, `roomId`, `minUserLastSeenAt`, `timestamp` |
| DM room, floor moves | one `publishCore` per subscription on `subject.UserRoomEvent(account)`; correct payload |
| Floor unchanged (`sameFloor`) | no `publishCore` |
| `room.LastMsgAt == nil` | no `publishCore` (early return) |
| Reader already past `LastMsgAt` | no `publishCore` (early return) |
| New floor `nil` (member fully unread) | event published with `minUserLastSeenAt` omitted |
| `publishCore` returns error | RPC still returns `accepted` (best-effort) |
| `ListSubscriptionsByRoom` errors (DM) | logged, RPC still returns `accepted` |

Coverage target: ≥ 80% (handler is core business logic — aim 90%+), covering the
new branches and error paths.

## Docs — `docs/client-api.md`

`messageRead` is a client-facing `chat.user.…` RPC, so the client API doc is
updated in the same PR: document the new `message_read` room event — its schema
(field table + JSON example), the `MinUserLastSeenAt` nullable semantics, and
which channel it arrives on (channel rooms: room event channel; DM rooms:
per-user event channel).

## Risks / Notes

- **Floor-nil transitions are intentional events.** When the floor moves from a
  concrete time to `nil` (e.g. a never-read member's subscription is the new
  minimum), `sameFloor` is false and an event fires with `minUserLastSeenAt`
  omitted — a valid "floor reset" signal, not a bug.
- **No new store methods.** `ListSubscriptionsByRoom` already exists on
  `RoomStore`; no mock regeneration needed beyond what's already generated.
- **Method-name overlap with broadcast-worker is fine** — different package,
  different `Handler` type; no collision.
