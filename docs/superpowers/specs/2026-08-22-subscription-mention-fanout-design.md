# Cross-site fan-out for room mention badges

## Problem

`broadcast-worker` flags `hasMention=true` on the mentioned accounts'
subscriptions, but only in the *origin* site's Mongo:

- `handleCreated` — `broadcast-worker/handler.go:216`
- `badgeNewlyMentionedAccounts` (message edits) — `broadcast-worker/handler.go:348`

A federated user reads their subscription list from their **home** site, so a
mention raised at another site never reaches them. `room-worker` already solves
this shape for `member_added` (`room-worker/handler.go:1281-1313`): group the
affected accounts by home site, then publish one OUTBOX event per destination
for `outbox-worker` to forward into that site's INBOX.

This design gives room mentions the same lane.

## Scope

In: the two room-level `SetSubscriptionMentions` call sites (new messages and
edits).

Out:

- **Thread replies.** `handleThreadCreated` deliberately skips room-level
  badges (a `TShow=false` reply has no visible message to explain a badge —
  `handler.go:287`), and `message-worker` already federates the thread
  subscription's own mention via `InboxThreadSubscriptionUpserted`.
- **`@all`.** It rides `UpdateRoomLastMessage`, a room-document concern with a
  different replication story.
- **Client fan-out.** The remote user's client already receives the message,
  `mentions` included, over the global room subject.

## Contract

`pkg/model/event.go`:

```go
InboxSubscriptionMention InboxEventType = "subscription_mention"

type SubscriptionMentionEvent struct {
    RoomID           string   `json:"roomId"           bson:"roomId"`
    Accounts         []string `json:"accounts"         bson:"accounts"`
    MessageCreatedAt int64    `json:"messageCreatedAt" bson:"messageCreatedAt"`
    Timestamp        int64    `json:"timestamp"        bson:"timestamp"`
}
```

`Accounts` carries only the accounts homed at the destination site — sending
the full list would ship mention identities to sites with no business knowing
them, exactly as `room-worker`'s per-site filtering avoids.

`MessageCreatedAt` is the message's `createdAt` (or `editedAt` on the edit
path) as unix-millis, feeding the destination's read guard. Mongo stores BSON
datetimes at millisecond precision, so the wire type loses nothing.

### Lane: concurrent

`InboxSubscriptionMention` joins `outbox.ConcurrentEventTypes`. The destination
write is `$set hasMention:true` under the same "has not already read past
`MessageCreatedAt`" guard the origin applies, which is idempotent and commutes
with `subscription_read` replays. It needs no FIFO ordering against
`member_added`/`member_removed`: a mention arriving before its `member_added`
matches no subscription and is a silent no-op, and the next mention re-badges.

## Producer — `broadcast-worker`

`main.go` gains a JetStream publish closure (`js.PublishMsg` +
`jetstream.WithMsgID`), copied from `message-worker/main.go:155`, injected via a
new `withOutboxFederation(siteID, publish)` handler option. An option rather
than a positional parameter because `NewHandler` has ~80 test call sites; a nil
`federate` disables the fan-out, mirroring `inbox-worker`'s nil-`badge`
precedent.

`broadcast-worker` does **not** bootstrap `OUTBOX`. `outbox-worker` owns that
stream, and `message-worker` — the existing precedent for a worker publishing
onto it — does not bootstrap it either.

### New messages

After the client fan-out, group `resolved.Participants` by `SiteID`, skipping
blank and local entries, and publish one `outbox.Publish` per remote site.
Site IDs already ride the existing `FindUsersByAccounts` result
(`pkg/mention/mention.go:115`), so the hot path takes no extra round-trip.

Dedup ID: `mention:{roomID}:{msgID}:{destSiteID}` — stable across
MESSAGES-CANONICAL redeliveries, distinct per destination.

### Edits

`badgeNewlyMentionedAccounts` only parses; it has no site information. It gains
one `FindUsersByAccounts` call, made only when the edit actually contains
mentions.

Dedup ID: `mention-edit:{roomID}:{msgID}:{editedAtMillis}:{destSiteID}`.
`editedAt` is in the seed so a second edit adding a new mention is not
swallowed by stream-level dedup.

### Failure handling

Publish failures are logged and swallowed; the canonical message is not NAKed.
This keeps the pre-fan-out latency path clear and avoids a redelivery
re-broadcasting the message to clients, at the cost of making the badge
best-effort: a NATS blip at enqueue time drops that remote badge rather than
deferring it. Accepted trade-off, chosen deliberately.

A mentionee the user lookup never resolved cannot be routed to a home site. It
is warned (accounts only) and skipped, matching `message-worker/handler.go:688`.

## Consumer — `inbox-worker`

`InboxStore` gains:

```go
SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error
```

implemented on `mongoInboxStore` with the guard filter copied verbatim from
`broadcast-worker/store_mongo.go:131` — `lastSeenAt: {$not: {$gte: ...}}`, not
`$lt`, so a never-read subscription (missing/null `lastSeenAt`) still matches
(#467).

`HandleEvent` gains a `model.InboxSubscriptionMention` case calling a new
`handleSubscriptionMention`. Mocks regenerate via `make generate`.

No badge-cache invalidation: `pkg/badgecache` holds a set of unread **room
IDs**, and the message itself already made the room unread.
`handleThreadUnreadAdded` sets the same precedent.

## Testing

Red-Green-Refactor throughout.

| Package | Cases |
|---|---|
| `pkg/model` | round-trip `SubscriptionMentionEvent` |
| `pkg/outbox` | new type is in the concurrent set and the partition stays disjoint |
| `broadcast-worker` | all-local mentions publish nothing; mixed sites produce one event per remote site with only that site's accounts; unresolved mentionee warns and is skipped; `@all`-only publishes nothing; publish error is swallowed and the message still fans out; nil `federate` is a no-op; edit path federates under its own dedup ID |
| `inbox-worker` | dispatch reaches the handler; malformed payload; store error propagates |
| `inbox-worker` (integration) | badges an unread subscription; skips one that already read past the message; skips a non-subscriber |
