# `message.read` RPC — Design

**Status:** Approved for planning
**Date:** 2026-05-04
**Service:** `room-service` (with cross-site sync via `inbox-worker`)

## 1. Goal

Add an RPC that lets a client mark a room as read for a given user. The handler must:

1. Validate the user has access to the room (subscription exists).
2. Recompute the user's per-subscription `Alert` flag.
3. Persist the new `LastSeenAt` and `Alert` on the local `Subscription`.
4. If the user's home site differs from the handler's site, publish a federated outbox event so the user's home-site `inbox-worker` can update its local copy.
5. Recompute `Room.MinUserLastSeenAt` from the room's subscriptions when the read receipt advances the user past the previous `LastMsgAt`; otherwise short-circuit.

## 2. Wire contract

### 2.1 NATS subject

| Concrete | Wildcard |
| --- | --- |
| `chat.user.{account}.request.room.{roomID}.{siteID}.message.read` | `chat.user.*.request.room.*.{siteID}.message.read` |

The handler runs in `room-service` and is queue-subscribed under the existing `room-service` queue group. `{siteID}` in the subject is the room's home site (consistent with sibling RPCs such as `member.role-update` / `member.list`).

New builders in `pkg/subject/subject.go`:

```go
func MessageRead(account, roomID, siteID string) string
func MessageReadWildcard(siteID string) string
```

The existing `subject.ParseUserRoomSubject` already extracts `account` and `roomID` from this subject shape; no new parser is required.

### 2.2 Request body

In `pkg/model/subscription.go`:

```go
type MessageReadRequest struct {
    RoomID string `json:"roomId"`
}
```

The handler validates that `req.RoomID == roomID` parsed from the subject (mismatch is a hard error, mirroring `handleRemoveMember` / `handleUpdateRole`). Empty `req.RoomID` is treated as "trust the subject".

### 2.3 Response body

```json
{"status": "accepted"}
```

The status string matches the convention used by sibling membership-mutating RPCs (`member.add`, `member.remove`, `member.role-update`). The work is performed synchronously by the handler; there is no downstream worker chain.

## 3. Handler logic

`Handler.handleMessageRead(ctx, subj, data) ([]byte, error)` in `room-service/handler.go`. NATS wrapper `natsMessageRead(m otelnats.Msg)` follows the established pattern (`wrappedCtx`, `natsutil.ReplyError(m.Msg, sanitizeError(err))` on failure, `m.Msg.Respond(resp)` on success). Registration line in `Handler.RegisterCRUD`:

```go
if _, err := nc.QueueSubscribe(subject.MessageReadWildcard(h.siteID), queue, h.natsMessageRead); err != nil {
    return fmt.Errorf("subscribe message read: %w", err)
}
```

Flow:

1. **Parse subject** via `subject.ParseUserRoomSubject(subj)` → `account`, `roomID`. `!ok` → `errors.New("invalid message-read subject")`.
2. **Unmarshal** request body into `MessageReadRequest`. If `req.RoomID != "" && req.RoomID != roomID` → "room ID mismatch" error.
3. **Load subscription:** `sub, err := h.store.GetSubscription(ctx, account, roomID)`.
   - `errors.Is(err, model.ErrSubscriptionNotFound)` → return `errNotRoomMember` (existing sentinel).
   - other error → wrap with `fmt.Errorf("get subscription: %w", err)`.
4. **Compute `newAlert`:** `newAlert := sub.Alert && len(sub.ThreadUnread) > 0`.
5. **Compute `originalLastSeen`:** `originalLastSeen := sub.JoinedAt; if sub.LastSeenAt != nil { originalLastSeen = *sub.LastSeenAt }`. (`Subscription.LastSeenAt` is `*time.Time`; `nil` means "never read".)
6. **Compute `now`:** `now := time.Now().UTC()`.
7. **Persist subscription update** via `store.UpdateSubscriptionRead(ctx, roomID, account, now, newAlert)`. Errors wrap.
8. **Cross-site outbox publish** (always, regardless of whether the room recompute will run):
   - `userSiteID, err := store.GetUserSiteID(ctx, account)`. Wrap on error.
   - If `userSiteID == ""` (user not found locally): `slog.Warn` and skip the publish — the local subscription has already been updated; the outbox is only for cache-syncing other sites.
   - If `userSiteID != "" && userSiteID != h.siteID`:
     - Build `model.SubscriptionReadEvent{Account: account, RoomID: roomID, LastSeenAt: now.UnixMilli(), Alert: newAlert, Timestamp: now.UnixMilli()}`.
     - Marshal as the `Payload` of `model.OutboxEvent{Type: model.OutboxSubscriptionRead, SiteID: h.siteID, DestSiteID: userSiteID, Payload, Timestamp: now.UnixMilli()}`.
     - Publish to `subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionRead)` via `h.publishToStream`. Errors wrap and abort the rest of the handler.
9. **Load room** via `store.GetRoom(ctx, roomID)`. Wrap errors.
10. **Decide on room recompute:**
    - If `room.LastMsgAt == nil` → return `{"status":"accepted"}`. (Defensive: if the room has never had a message, there is no `MinUserLastSeenAt` to update.)
    - If `originalLastSeen.After(*room.LastMsgAt)` → return `{"status":"accepted"}`. (The user was already up-to-date before this read; the floor cannot have moved.)
11. **Recompute room min-last-seen:**
    - `minTime, err := store.MinSubscriptionLastSeenByRoomID(ctx, roomID)`.
    - `store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime)` (`nil` clears the field via `$unset`).
12. **Return** `{"status":"accepted"}`.

### 3.1 Step ordering rationale

- The cross-site outbox is published **before** the early-return check so the user's home site receives every read receipt, even ones that don't advance the room floor. (The room `MinUserLastSeenAt` is per-room cluster-scoped state; the user's subscription state is per-user state — they have different freshness needs.)
- The local subscription update is performed **before** the outbox to avoid any window where the local site has acked the read but failed to durably record it. If outbox publish fails, the local state is correct; the federation gap is what gets reported.
- The room recompute runs strictly **after** the per-user write so the aggregation sees the new `LastSeenAt` value.

### 3.2 Error sanitization

The wrapper invokes `natsutil.ReplyError(m.Msg, sanitizeError(err))` so internal error chains never reach the client. Sentinels (`errNotRoomMember`) pass through untouched per the existing `sanitizeError` allow-list pattern.

## 4. Model changes

### 4.1 New types in `pkg/model/subscription.go`

```go
type MessageReadRequest struct {
    RoomID string `json:"roomId"`
}
```

### 4.2 New types and constants in `pkg/model/event.go`

```go
const OutboxSubscriptionRead OutboxEventType = "subscription_read"

type SubscriptionReadEvent struct {
    Account    string `json:"account"`
    RoomID     string `json:"roomId"`
    LastSeenAt int64  `json:"lastSeenAt"` // UnixMilli, UTC
    Alert      bool   `json:"alert"`
    Timestamp  int64  `json:"timestamp"`
}
```

`int64` UnixMilli is used on the wire for cross-site safety (matches `MemberAddEvent.JoinedAt`, `HistorySharedSince`). The local Mongo store still writes `time.Time` (BSON `DateTime`).

### 4.3 Round-trip test

Add a `roundTrip` case for `SubscriptionReadEvent` in `pkg/model/model_test.go`.

## 5. Store changes

### 5.1 `RoomStore` (room-service) — three new methods

```go
// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
// keyed by (roomID, account). Returns model.ErrSubscriptionNotFound (wrapped)
// when no subscription matches.
UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error

// GetUserSiteID returns the home site of a user looked up by account.
// Returns ("", nil) when the user is not found locally; callers treat that
// as "skip cross-site outbox".
GetUserSiteID(ctx context.Context, account string) (string, error)

// MinSubscriptionLastSeenByRoomID returns the minimum effective lastSeenAt
// across all subscriptions for roomID. Subscriptions whose lastSeenAt is the
// zero value contribute their joinedAt instead. Returns nil when there are
// no subscriptions for the room.
MinSubscriptionLastSeenByRoomID(ctx context.Context, roomID string) (*time.Time, error)

// UpdateRoomMinUserLastSeenAt writes rooms.minUserLastSeenAt for roomID.
// A nil value clears the field via $unset; a non-nil value writes via $set.
UpdateRoomMinUserLastSeenAt(ctx context.Context, roomID string, t *time.Time) error
```

### 5.2 Mongo implementations (`room-service/store_mongo.go`)

- **`UpdateSubscriptionRead`** — `s.subscriptions.UpdateOne(ctx, bson.M{"roomId": roomID, "u.account": account}, bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}})`. If `result.MatchedCount == 0`, return `fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)`.
- **`GetUserSiteID`** — `s.users.FindOne(ctx, bson.M{"account": account}, options.FindOne().SetProjection(bson.M{"siteId": 1, "_id": 0}))`. On `mongo.ErrNoDocuments` return `"", nil`. Otherwise decode into `struct{ SiteID string \`bson:"siteId"\` }`.
- **`MinSubscriptionLastSeenByRoomID`** — aggregation pipeline. The Go `time.Time{}` zero value (`0001-01-01T00:00:00Z`) is what `lastSeenAt` decodes to when the field is missing or unwritten on the BSON side, so the `$cond` falls back to `joinedAt` whenever `lastSeenAt` is null OR equal to that zero date:
  ```go
  zeroTime := time.Time{}
  pipeline := mongo.Pipeline{
      {{Key: "$match", Value: bson.M{"roomId": roomID}}},
      {{Key: "$group", Value: bson.M{
          "_id": nil,
          "min": bson.M{"$min": bson.M{"$cond": bson.A{
              bson.M{"$or": bson.A{
                  bson.M{"$eq": bson.A{"$lastSeenAt", nil}},
                  bson.M{"$lte": bson.A{"$lastSeenAt", zeroTime}},
              }},
              "$joinedAt",
              "$lastSeenAt",
          }}},
      }}},
  }
  ```
  Empty cursor → `nil, nil`. Decode into `struct{ Min time.Time \`bson:"min"\` }` and return `&result.Min`.
- **`UpdateRoomMinUserLastSeenAt`** — filter `bson.M{"_id": roomID}`. If `t == nil`, `bson.M{"$unset": bson.M{"minUserLastSeenAt": ""}}`; else `bson.M{"$set": bson.M{"minUserLastSeenAt": *t}}`.

### 5.3 `InboxStore` (inbox-worker) — one new method

```go
// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
// keyed by (roomID, account). Idempotent and order-safe: the write only
// applies when the stored lastSeenAt is missing or strictly earlier than
// the supplied value. Older or duplicate events are silent no-ops.
// Missing-subscription is also a silent no-op (replays may arrive before
// the local member_added has been processed).
UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error
```

Mongo implementation:

```go
filter := bson.M{
    "roomId":    roomID,
    "u.account": account,
    "$or": bson.A{
        bson.M{"lastSeenAt": bson.M{"$exists": false}},
        bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
    },
}
update := bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}}
_, err := s.subscriptions.UpdateOne(ctx, filter, update)
return err  // Mongo errors only — MatchedCount==0 is a no-op.
```

The two fields are written together so they never drift: `lastSeenAt` and `alert` always come from the same read event.

### 5.4 Indexes

Existing indexes cover all new queries:

- `subscriptions(roomId, u.account)` unique → covers `UpdateSubscriptionRead` filter and the inbox-worker variant's `$lt` predicate.
- `subscriptions(roomId, u.account)` is also a usable prefix index for `MinSubscriptionLastSeenByRoomID`'s `$match {roomId}` stage.
- `users(account)` → covers `GetUserSiteID`.

No new indexes needed.

### 5.5 Mocks

`make generate SERVICE=room-service` and `make generate SERVICE=inbox-worker` regenerate `mock_store_test.go` after the interfaces gain new methods.

## 6. Inbox-worker integration

`inbox-worker/handler.go`:

- Extend the `HandleEvent` switch with `case model.OutboxSubscriptionRead: return h.handleSubscriptionRead(ctx, &evt)`.
- `handleSubscriptionRead`:

```go
func (h *Handler) handleSubscriptionRead(ctx context.Context, evt *model.OutboxEvent) error {
    var e model.SubscriptionReadEvent
    if err := json.Unmarshal(evt.Payload, &e); err != nil {
        return fmt.Errorf("unmarshal subscription_read payload: %w", err)
    }
    lastSeenAt := time.UnixMilli(e.LastSeenAt).UTC()
    if err := h.store.UpdateSubscriptionRead(ctx, e.RoomID, e.Account, lastSeenAt, e.Alert); err != nil {
        return fmt.Errorf("update subscription read for %q in room %q: %w", e.Account, e.RoomID, err)
    }
    return nil
}
```

No `SubscriptionUpdateEvent` is published from the inbox handler — this is symmetric with the existing `member_added` / `member_removed` / `role_updated` arms.

## 7. Testing

### 7.1 TDD

Per `CLAUDE.md`, all new code follows Red → Green → Refactor. Write the handler / store tests first, confirm they fail, then implement.

### 7.2 Room-service unit tests (`room-service/handler_test.go`)

Mock `RoomStore`; capture `publishToStream`. Cases for `handleMessageRead`:

1. Subject mismatch (body `RoomID` ≠ subject `roomID`) → error, no store calls.
2. Invalid subject → error, no store calls.
3. Not a member (`GetSubscription` → `ErrSubscriptionNotFound`) → `errNotRoomMember`.
4. Happy path, local user, alert clears (`Alert=true, ThreadUnread=[]`).
5. Happy path, alert stays true (`Alert=true, ThreadUnread=["t1"]`).
6. `LastSeenAt.IsZero()` → fallback to `JoinedAt`; `JoinedAt > Room.LastMsgAt` triggers early-return after subscription update.
7. Early-return: `originalLastSeen > Room.LastMsgAt` → no `MinSubscriptionLastSeenByRoomID` / `UpdateRoomMinUserLastSeenAt` calls.
8. Early-return: `Room.LastMsgAt == nil` → no recompute.
9. Cross-site user (`GetUserSiteID == "site-b"`, handler at `"site-a"`) → outbox publish; assert subject `outbox.site-a.to.site-b.subscription_read`; decode payload and assert all fields.
10. Cross-site outbox publish failure → handler returns wrapped error, room recompute not attempted.
11. `GetUserSiteID` returns `("", nil)` → no publish, no error.
12. `GetUserSiteID` returns error → wrapped error, no publish, no recompute.
13. `MinSubscriptionLastSeenByRoomID` returns `nil` → `UpdateRoomMinUserLastSeenAt(nil)` (clears).
14. Store error at each of: `UpdateSubscriptionRead`, `GetRoom`, `MinSubscriptionLastSeenByRoomID`, `UpdateRoomMinUserLastSeenAt` → propagated (one subtest each).

### 7.3 Inbox-worker unit tests (`inbox-worker/handler_test.go`)

1. Happy path: outbox event with `subscription_read` payload → store called with correctly converted `time.UnixMilli(...).UTC()` and `Alert`.
2. Malformed inner JSON → wrapped error.
3. Store error → wrapped error.
4. Existing "unknown type → skip" test stays green (no regression).

### 7.4 Integration tests (`testcontainers-go`, `//go:build integration`)

`room-service/integration_test.go`:

- `UpdateSubscriptionRead` writes both fields; missing sub returns `ErrSubscriptionNotFound`.
- `GetUserSiteID` returns `siteID` on hit, `("", nil)` on miss.
- `MinSubscriptionLastSeenByRoomID`: returns the minimum across subs; subs with zero `LastSeenAt` contribute `JoinedAt`; empty room → `nil`.
- `UpdateRoomMinUserLastSeenAt`: `$set` on non-nil, `$unset` on nil.

`inbox-worker/integration_test.go`:

- Happy path: newer `lastSeenAt` applied.
- Out-of-order: seed sub with `LastSeenAt = T2`; apply event with `LastSeenAt = T1 < T2`; verify doc unchanged.
- Equal timestamp: idempotent replay → no change.
- Missing subscription: no error, no write.

### 7.5 Coverage

Project minimum is 80%; handlers and stores target ≥90% per `CLAUDE.md`.

## 8. Out of scope

- No system message ("X read up to here") — read receipts are silent.
- No notification clearing fan-out beyond the `Alert` recompute. Pruning of `ThreadUnread` itself is a separate concern handled elsewhere.
- No batch `message.read` (one room per request).
- No write back from inbox-worker to other sites — the user's home site is a cache, not authoritative.
- No `$max`-style guard on the room-service local write — `time.Now()` is monotonically increasing within a process; the guard exists only on the inbox-worker side where federation can deliver out-of-order.

## 9. Migration / rollout

- New subject — no existing client conflicts.
- New event type `subscription_read` — older `inbox-worker` deployments will hit the existing `default` arm and `slog.Warn("unknown event type, skipping")`. No crash, no poison-pill. Roll out `inbox-worker` before `room-service` to avoid the warning window.
- No schema migration on existing collections.
