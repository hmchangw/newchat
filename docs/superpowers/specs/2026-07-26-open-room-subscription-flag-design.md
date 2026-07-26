# Open-Room Subscription Flag + `open` RPC in room-service

## Summary

A new per-user, per-room boolean `Subscription.open` plus a client-facing NATS
request/reply RPC, `open`, that sets `open = true` for the requester on a single
room. `subscription.list` excludes subscriptions whose `open` is explicitly
`false`, so an "opened" room shows in the sidebar and a "closed" room is hidden
from the list.

Architecturally mirrors `favorite.toggle` (`docs/.../2026-06-01-favorite-toggle-rpc-design.md`):
same subject shape, same store `FindOneAndUpdate` pattern, same cross-site
replication path through OUTBOX → INBOX. Two deliberate differences:

1. **Set, not toggle.** Every call sets `open = true`. No race handling — per
   the requirement, "just make it true every time." Applying `true` twice, or
   out of order, always converges to `true`, so no `openUpdatedAt` high-water
   guard is needed (unlike `favorite`/`mute`).
2. **Default true, always present.** New subscriptions are born `open = true`.
   The model field is a plain `bool` that is always written — there is no
   legacy/absent-field handling (per the agreed scope: assume every
   subscription document carries `open`).

## Subscription model

`pkg/model/subscription.go` gains one field on `Subscription`:

```go
Open bool `json:"open" bson:"open"`
```

Both tags always present (no `omitempty`) — `false` is serialized explicitly so
clients can distinguish "closed" from "unknown".

### Creation default (`room-worker`)

`room-worker`'s single subscription constructor `newSub` sets `Open: true`, so
**every** creation path is born open: DM, botDM, self-DM, channel-create members,
and add-members. This is required: with a plain `bool` and no `omitempty`, a
newly created subscription would otherwise persist `open: false` (the Go zero
value) and be immediately excluded from `subscription.list`.

`buildSelfDMSub` and the other builders need no change — they all route through
`newSub`.

## `subscription.list` filter

`user-service/mongorepo/subscriptions.go`, `AggregateSubscriptions` match stage:

```go
match["open"] = bson.M{"$ne": false} // exclude only rooms explicitly closed
```

`$ne: false` includes `open: true` (and, defensively, any doc missing the
field) and excludes only an explicit `open: false`. This mirrors the
requirement: "exclude all subscriptions that open field exist and value is
false."

Scope: applied to the `subscription.list` RPC (`AggregateSubscriptions`) only.
The count/active/getChannels/getDM/getByRoomID paths are unchanged. For
consistency, `subscriptionProjection` gains `"open": 1` so the field surfaces on
the getChannels path, but no filter is added there.

## Subject

- Concrete: `chat.user.{account}.request.room.{roomID}.{siteID}.open`
- Wildcard: `chat.user.*.request.room.*.{siteID}.open`
- Pattern (natsrouter): `chat.user.{account}.request.room.{roomID}.{siteID}.open`

`{siteID}` is the room's origin site — the site that owns the room and runs the
`room-service` handling this RPC. Subject parsing reuses
`subject.ParseUserRoomSubject`. Queue group / router: same as the other
room-scoped RPCs.

`pkg/subject/subject.go` gains three builders mirroring the `FavoriteToggle`
trio:

```go
func OpenRoom(account, roomID, siteID string) string {
    return fmt.Sprintf("chat.user.%s.request.room.%s.%s.open", account, roomID, siteID)
}
func OpenRoomWildcard(siteID string) string {
    return fmt.Sprintf("chat.user.*.request.room.*.%s.open", siteID)
}
func OpenRoomPattern(siteID string) string {
    return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.open", siteID)
}
```

## Wire Format

`pkg/model/event.go`:

```go
// OpenRoomResponse is the sync reply for the open RPC. Open is always true.
type OpenRoomResponse struct {
    Status string `json:"status"` // always "ok" on success
    Open   bool   `json:"open"`   // always true
}

// SubscriptionOpenedEvent is the InboxEvent.Payload for type "subscription_opened".
type SubscriptionOpenedEvent struct {
    Account   string `json:"account"   bson:"account"`
    RoomID    string `json:"roomId"    bson:"roomId"`
    Open      bool   `json:"open"      bson:"open"`
    Timestamp int64  `json:"timestamp" bson:"timestamp"` // UnixMilli UTC
}
```

Inbox event-type constant (`pkg/model/event.go`):

```go
InboxSubscriptionOpened InboxEventType = "subscription_opened"
```

Request body: empty. The subject already carries `account` and `roomID`; any
body content is ignored. Registered with `natsrouter.RegisterNoBody`, like
`favoriteToggle`.

The `SubscriptionUpdateEvent.Action` enum gains `"opened"` alongside the
existing `"added"`, `"removed"`, `"role_updated"`, `"mute_toggled"`,
`"favorite_toggled"`.

## Handler Flow

`room-service/handler.go`:

1. `Register` adds
   `natsrouter.RegisterNoBody(r, subject.OpenRoomPattern(h.siteID), h.openRoom)`.
2. `openRoom(c *natsrouter.Context) (*model.OpenRoomResponse, error)`:
   1. `account := c.Param("account")`, `roomID := c.Param("roomID")`.
   2. Tracing: set `room.id` and `site.id` span attributes.
   3. `now := time.Now().UTC()`.
   4. `sub, err := h.store.OpenSubscription(ctx, roomID, account)`.
      - `errors.Is(err, model.ErrSubscriptionNotFound)` → `errNotRoomMember`
        (this atomic no-match **is** the "does the user have this subscription"
        check — no separate `GetSubscription`, same as `favorite.toggle`).
      - Other errors → `fmt.Errorf("open subscription: %w", err)`.
   5. `h.publishSubscriptionUpdate(ctx, account, "opened", sub, "", now)` —
      best-effort core publish; a failure is logged, not returned.
   6. `userSiteID, err := h.store.GetUserSiteID(ctx, account)`.
      Error → `fmt.Errorf("get user siteId: %w", err)`.
   7. If `userSiteID != "" && userSiteID != h.siteID`:
      - Build `SubscriptionOpenedEvent{Account, RoomID, Open: sub.Open, Timestamp: now.UnixMilli()}`.
      - `json.Marshal` → error wrapped `"marshal opened payload: %w"`.
      - `h.federateOne(ctx, roomID, userSiteID, model.InboxSubscriptionOpened, payloadData, roomID+":"+account, now.UnixMilli())`.
        Failure → `fmt.Errorf("federate opened: %w", err)` (fatal to the client;
        federation retries on next action).
   8. Reply `OpenRoomResponse{Status: "ok", Open: sub.Open}`.

Idempotency: this is a **set** to `true`. Every successful call yields `true`.
No dedup, no debounce required — repeated calls are harmless.

## Errors

Reuses the existing room-service sentinel — no new error variables:

- `errNotRoomMember` (already in `room-service/helper.go`) — returned when the
  requester has no subscription in the room.

## Store

### Interface (`room-service/store.go`)

```go
// OpenSubscription atomically sets open=true for (roomID, account) via a single
// FindOneAndUpdate and returns the post-update subscription, or
// model.ErrSubscriptionNotFound (wrapped) when no match.
OpenSubscription(ctx context.Context, roomID, account string) (*model.Subscription, error)
```

`GetUserSiteID` already exists — reused as-is.

### Mongo implementation (`room-service/store_mongo.go`)

Reuses the existing `findOneAndUpdateSub` helper (same one `favorite`/`mute`
use), with a plain `$set` (no timestamp — no high-water guard):

```go
func (s *MongoStore) OpenSubscription(ctx context.Context, roomID, account string) (*model.Subscription, error) {
    return s.findOneAndUpdateSub(ctx, roomID, account, "open subscription", bson.M{
        "open": true,
    })
}
```

`findOneAndUpdateSub` already maps `mongo.ErrNoDocuments` →
`model.ErrSubscriptionNotFound` and returns the `options.After` post-image.

### Indexes

No new indexes. The existing `(roomId, "u.account")` compound index covers the
lookup.

## Cross-Site Federation

Mirrors `subscription_favorite_toggled`. The room's site is the write site; the
user's home site is the destination.

### Outbox partition (`pkg/outbox/outbox.go`)

Add `model.InboxSubscriptionOpened` to `ConcurrentEventTypes` (order-insensitive:
the destination apply is an idempotent set-true, so parallel/out-of-order
forwarding is safe). This registration is mandatory — `outbox.Publish` rejects
any event type in neither partition.

### Inbox handler (`inbox-worker`)

`inbox-worker/handler.go`:

1. Dispatch switch gains
   `case "subscription_opened": return h.handleSubscriptionOpened(ctx, &evt)`.
2. `handleSubscriptionOpened`:
   1. Unmarshal `evt.Payload` into `SubscriptionOpenedEvent`.
   2. `h.store.UpdateSubscriptionOpen(ctx, e.RoomID, e.Account, e.Open)`.

`InboxStore` interface (`inbox-worker/handler.go`) gains:

```go
UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error
```

Mongo implementation (`inbox-worker/main.go`) — plain `$set`, **no**
`openUpdatedAt` guard (no race handling required):

```go
func (s *mongoInboxStore) UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error {
    res, err := s.subCol.UpdateOne(ctx,
        bson.M{"roomId": roomID, "u.account": account},
        bson.M{"$set": bson.M{"open": open}},
    )
    if err != nil {
        return fmt.Errorf("update subscription open for %q in room %q: %w", account, roomID, err)
    }
    if res.MatchedCount == 0 {
        return s.naksIfSubscriptionMissing(ctx, account, roomID)
    }
    return nil
}
```

Missing-subscription handling reuses the existing `naksIfSubscriptionMissing`
path (same as `UpdateSubscriptionFavorite`): a federation race where the user
left the room between the origin write and the mirror is a silent no-op, not a
poison-pill.

### Stream ownership

No `bootstrap.go` change. `OUTBOX_{site}` and `INBOX_{site}` are owned by
ops/IaC (CLAUDE.md §6). `inbox-worker` already owns local INBOX creation in dev.

## Client API Doc

Per CLAUDE.md §5, the same PR updates `docs/client-api.md` and its derived views
(`docs/client-api/request-reply.md`, `docs/client-api/events.md`):

- New "Open Room" section under user-scoped RPCs, sibling to "Toggle Favorite":
  subject, empty request body, `OpenRoomResponse`, error cases (`errNotRoomMember`),
  triggered `subscription.update` event with `action: "opened"`, and the
  cross-site outbox-mirror behaviour.
- The `subscription.update` event section's `action` enum gains `"opened"`.
- The `Subscription` field table gains `open` (bool).

## Testing (TDD)

### Subject builders (`pkg/subject/subject_test.go`)

- `TestOpenRoom` — concrete-subject builder.
- `TestOpenRoomWildcard` — wildcard form.
- `TestOpenRoomPattern` — natsrouter pattern form.
- Round-trip with `ParseUserRoomSubject` on the concrete subject.

### Model round-trip (`pkg/model/model_test.go`)

- Extend the `Subscription` round-trip case to populate `Open: true`.
- Assert `"open"` is present in the JSON even when `false` (parallel to `muted`).
- `TestOpenRoomResponseJSON` — round-trip + raw-map assertion on `{status, open}`.
- `TestSubscriptionOpenedEventJSON` — round-trip.
- `TestInboxSubscriptionOpenedConst` — exact string `"subscription_opened"`.
- `TestOpenRoomInOutboxConcurrentSet` (in `pkg/outbox` test) — asserts the new
  type is in `ConcurrentEventTypes` and accepted by `Publish`.

### Handler unit tests (`room-service/handler_test.go`)

Parallel to the favorite suite; each builds a `Handler` with a mocked
`RoomStore` and captured `publishCore` / `publishToStream` closures.

| Test | Setup | Asserts |
|------|-------|---------|
| `TestHandler_OpenRoom_Success` | store returns sub with `Open: true`, `GetUserSiteID` returns same site | Reply `{ok, true}`. One core publish on `subscription.update` with `Action: "opened"`. No stream publish. |
| `TestHandler_OpenRoom_CrossSitePublishesOutbox` | `GetUserSiteID` returns remote site | Stream publish carries `OutboxEvent` with inner `SubscriptionOpenedEvent{Open:true, nonzero Timestamp}`, `Type=InboxSubscriptionOpened`. |
| `TestHandler_OpenRoom_NotRoomMember` | store returns `ErrSubscriptionNotFound` | `errors.Is(err, errNotRoomMember)`; no publishes. |
| `TestHandler_OpenRoom_StoreError` | store returns `fmt.Errorf("db down")` | Error contains `"open subscription"`; no publishes. |
| `TestHandler_OpenRoom_GetUserSiteIDError` | open ok, `GetUserSiteID` fails | Error contains `"get user siteId"`; no stream publish. |
| `TestHandler_OpenRoom_CrossSiteOutboxPublishFailure` | cross-site, `publishToStream` errors | Error contains `"federate opened"`. |
| `TestHandler_OpenRoom_CorePublishFailureIsNonFatal` | same-site, `publishCore` errors | `require.NoError`; still replies `{ok, true}`. |

Mocks: `make generate SERVICE=room-service` regenerates `mock_store_test.go`
with `OpenSubscription`.

### room-worker (`room-worker/handler_test.go`)

- `TestNewSub_OpenTrue` (or extend an existing `newSub`/create test) — asserts
  a subscription built by `newSub` has `Open == true`, so every create path is
  born open.

### user-service (`user-service/mongorepo/subscriptions_test.go` / integration)

- `TestAggregateSubscriptions_ExcludesClosed` — a doc with `open: false` is
  excluded from the list; `open: true` and (defensively) a doc missing the field
  are included.

### room-service integration (`room-service/integration_test.go`)

`TestMongoStore_OpenSubscription`:
1. Insert a subscription with `open: false`.
2. `OpenSubscription` → returned `Open == true`; `GetSubscription` reads back `true`.
3. Call again → still `true` (idempotent).
4. `OpenSubscription("missing", account)` → `errors.Is(err, model.ErrSubscriptionNotFound)`, nil sub.

### inbox-worker unit tests (`inbox-worker/handler_test.go`)

Extend `stubInboxStore` with `UpdateSubscriptionOpen`.

- `TestHandler_SubscriptionOpened` — happy path, store's sub has `Open == true`
  after `HandleEvent`.
- `TestHandler_SubscriptionOpened_MissingSubscriptionNoOp` — empty store, unknown
  account, `HandleEvent` returns `nil`.
- `TestHandler_SubscriptionOpened_MalformedPayload` — `Payload: []byte("not-json")`,
  `HandleEvent` returns error (redeliver path).

### inbox-worker integration (`inbox-worker/integration_test.go`)

- `TestMongoInboxStore_UpdateSubscriptionOpen` — sets `open` on an existing sub;
  missing sub is a silent no-op.

### Coverage

New handler/store methods hit every branch above. Expected ≥90% on new code;
project floor 80% applies to each package.

## Out of Scope

- **"Close room" RPC.** Nothing in this change sets `open = false`; a close/hide
  endpoint is separate future work. Today every subscription stays `open` unless
  some external writer sets it false, which the list filter then honors.
- **Frontend changes.** The frontend reads `open` off `Subscription` separately.
- **`openUpdatedAt` / ordering guard.** Explicitly omitted — set-true is
  idempotent and order-insensitive.
- **Notification / broadcast / gatekeeper behaviour.** `open` is a
  list-visibility hint only; no delivery, routing, or validation changes.
- **Applying the filter to count/getChannels/getDM/getByRoomID.** Only
  `subscription.list` filters on `open`.

## Risks

- **Create-default omission.** If `newSub` did not set `Open: true`, every new
  subscription would persist `open: false` and vanish from lists. The
  `TestNewSub_OpenTrue` test guards this invariant.
- **Cross-site mirror lag.** Between the room-site write and the home-site
  mirror, a home-site `subscription.list` returns the pre-open value. Acceptable
  — the requester's session already got the `subscription.update` event
  synchronously; other devices reconcile on next refetch. Same as `favorite`.
- **Federation race silent no-op.** If the user leaves the room between the
  origin write and the inbox mirror, the mirror silently no-ops (via
  `naksIfSubscriptionMissing`). Same pattern as `UpdateSubscriptionFavorite`.
