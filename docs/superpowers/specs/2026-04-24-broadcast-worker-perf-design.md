# Broadcast Worker Performance Optimizations — Design

## Purpose

Reduce `broadcast-worker`'s per-message latency by eliminating two redundant
MongoDB round-trips on the hot path:

1. Cache sender-user enrichment lookups so repeated messages from the same
   account don't hit MongoDB every time.
2. Collapse the `GetRoom` + `UpdateRoomOnNewMessage` pair into a single
   `FindOneAndUpdate` round-trip.

Measured motivation: at 500 msg/s, the current per-message path does three
indexed Mongo operations plus one NATS publish. Under load the per-message
work alone saturates the worker pool and E2 broadcast latency climbs into
the 1–10 second range even on small presets. The two changes together remove
one Mongo call unconditionally and a second one for any cache-hit, which is
the common case at steady-state.

## Scope

### In scope

- New `CachedUserStore` in `broadcast-worker/usercache.go` that wraps any
  `userstore.UserStore` with a bounded LRU + TTL cache.
- Replacement of the `GetRoom` + `UpdateRoomOnNewMessage` sequence with a
  new `FetchAndUpdateRoom` store method backed by MongoDB's
  `FindOneAndUpdate`.
- `Store` interface update: add `FetchAndUpdateRoom`, remove
  `UpdateRoomOnNewMessage`, retain `GetRoom` (unused in the handler after
  this change, but kept for future consumers).
- Two new env vars on `broadcast-worker`:
  - `USER_CACHE_SIZE` (default `10000`; `0` disables caching)
  - `USER_CACHE_TTL` (default `5m`)
- Unit tests per the repo's TDD convention.
- Mock regeneration (`make generate SERVICE=broadcast-worker`).
- Integration test verified to still pass against the updated handler.

### Out of scope (this branch)

- `message-gatekeeper` — does not call `FindUsersByAccounts`, no change needed.
- `pkg/userstore` — no API changes. The cache is local to `broadcast-worker`.
- Redis-backed shared cache — deliberately not done. Per-hit latency for
  Redis (~0.5–1 ms local) is in the same order as the MongoDB call we're
  eliminating, so the network-round-trip cost would largely offset the
  benefit. In-process caching gives ~100 ns per hit. Revisit if the fleet
  grows to many replicas or user-record edits become frequent.
- Benchmark harness — lives in `tools/loadgen` on its own branch.
- Changes to `broadcast-worker`'s consumer config, concurrency model, or
  NATS publishing path.

## Architecture

```text
                  broadcast-worker
      ┌─────────────────────────────────────────┐
      │  HandleMessage                           │
      │                                          │
      │  ┌──► FetchAndUpdateRoom  ──► Mongo      │
      │  │                                       │
      │  ├──► mention.Resolve (no DB for uniform)│
      │  │                                       │
      │  ├──► FindUsersByAccounts                │
      │  │      │                                │
      │  │      ▼                                │
      │  │   CachedUserStore ─► hit? ─► return   │
      │  │      │               │                │
      │  │      │               ▼ miss           │
      │  │      │           mongoStore ─► Mongo  │
      │  │      │                                │
      │  │      └──◄ populate cache              │
      │  │                                       │
      │  └──► nc.Publish ─► NATS                 │
      └─────────────────────────────────────────┘
```

Before this change: per message = 3 Mongo round-trips (GetRoom + UpdateRoom
+ FindUsersByAccounts) + 1 NATS publish.

After this change: per message = 1 Mongo round-trip (FetchAndUpdateRoom)
+ 0 or 1 Mongo round-trip (FindUsersByAccounts on cache miss) + 1 NATS
publish. At steady-state with a small sender pool, cache hit rate tends
to 100% after warm-up.

## File layout

```text
broadcast-worker/
├── handler.go              # modified: call FetchAndUpdateRoom instead of GetRoom+Update
├── handler_test.go         # modified: mock expectations updated
├── main.go                 # modified: parse USER_CACHE_SIZE/TTL, wrap userstore
├── store.go                # modified: add FetchAndUpdateRoom, remove UpdateRoomOnNewMessage
├── store_mongo.go          # modified: implement FetchAndUpdateRoom, remove UpdateRoomOnNewMessage
├── usercache.go            # NEW: CachedUserStore implementing userstore.UserStore
├── usercache_test.go       # NEW: six unit tests (hit/miss/partial/TTL/LRU/concurrency)
├── mock_store_test.go      # regenerated
└── integration_test.go     # unchanged; end-to-end path still covered
```

No other services touched.

## Component design

### `CachedUserStore` (`broadcast-worker/usercache.go`)

Implements `userstore.UserStore` so the rest of the code is unaware of the
wrapper. Single-mutex LRU + TTL backed by `container/list` and a map
keyed on account name.

```go
type userCacheEntry struct {
    user     model.User
    inserted time.Time
}

type CachedUserStore struct {
    inner   userstore.UserStore
    ttl     time.Duration
    maxSize int
    mu      sync.Mutex
    lru     *list.List                 // elements store (account, *userCacheEntry)
    index   map[string]*list.Element
}

func NewCachedUserStore(inner userstore.UserStore, maxSize int, ttl time.Duration) *CachedUserStore
func (c *CachedUserStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
```

`FindUsersByAccounts` behavior:

1. For each `account` in the input slice, look up `index[account]`.
2. If present and `now.Sub(entry.inserted) < ttl`, treat as hit: record the
   user in the result slice and `lru.MoveToFront(element)`.
3. If absent or stale, add the account to a `missing` slice.
4. If `len(missing) > 0`, call `inner.FindUsersByAccounts(ctx, missing)`.
   On error, return the partial (cache-hit) results plus the wrapped error.
5. For each returned user, insert into cache; if `lru.Len() > maxSize`, pop
   from the back (`lru.Remove(lru.Back())`) and delete from `index`.
6. Return the union of hits and fresh results (no order guarantee; matches
   existing contract).

Cache entries for accounts that the inner store did not return (i.e.,
missing users) are not inserted — this prevents a misspelled or deleted
account from being cached as a negative result and preventing real data
from being served once the user is created.

### `FetchAndUpdateRoom` (`broadcast-worker/store_mongo.go`)

```go
func (m *mongoStore) FetchAndUpdateRoom(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool) (*model.Room, error) {
    fields := bson.M{
        "lastMsgAt": msgAt,
        "lastMsgId": msgID,
        "updatedAt": msgAt,
    }
    if mentionAll {
        fields["lastMentionAllAt"] = msgAt
    }
    filter := bson.M{"_id": roomID}
    update := bson.M{"$set": fields}
    opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

    var room model.Room
    if err := m.roomCol.FindOneAndUpdate(ctx, filter, update, opts).Decode(&room); err != nil {
        return nil, fmt.Errorf("fetch and update room %s: %w", roomID, err)
    }
    return &room, nil
}
```

`ReturnDocument=After` gives the post-update document so the handler's
`buildRoomEvent` sees the new `lastMsgAt` / `lastMsgId` and the existing
`Name`/`Type`/`SiteID`/`UserCount` in a single load.

### Handler changes (`broadcast-worker/handler.go`)

One two-line replacement in `HandleMessage`:

```go
// before:
room, err := h.store.GetRoom(ctx, msg.RoomID)
if err != nil { return fmt.Errorf("get room %s: %w", msg.RoomID, err) }
// ...
if err := h.store.UpdateRoomOnNewMessage(ctx, room.ID, msg.ID, msg.CreatedAt, resolved.MentionAll); err != nil {
    return fmt.Errorf("update room on new message: %w", err)
}

// after:
room, err := h.store.FetchAndUpdateRoom(ctx, msg.RoomID, msg.ID, msg.CreatedAt, resolved.MentionAll)
if err != nil { return fmt.Errorf("fetch and update room %s: %w", msg.RoomID, err) }
```

No other handler logic changes.

## Wiring (`broadcast-worker/main.go`)

Add to `config`:

```go
UserCacheSize int           `env:"USER_CACHE_SIZE" envDefault:"10000"`
UserCacheTTL  time.Duration `env:"USER_CACHE_TTL"  envDefault:"5m"`
```

Wrap the user store during dependency construction:

```go
var us userstore.UserStore = userstore.NewMongoStore(db.Collection("users"))
if cfg.UserCacheSize > 0 {
    us = NewCachedUserStore(us, cfg.UserCacheSize, cfg.UserCacheTTL)
}
```

Setting `USER_CACHE_SIZE=0` bypasses caching — useful for comparing
cached-vs-uncached runs under the loadgen harness.

## Data flow

Per message (happy path, cache hit):

1. Receive canonical message from JetStream.
2. `FetchAndUpdateRoom` — 1 Mongo round-trip. Returns updated room.
3. `mention.Resolve(content, lookupFn)` — content has no `@`, returns immediately.
4. `userStore.FindUsersByAccounts([sender])` — cache hit, returns immediately.
5. `nc.Publish(subject.RoomEvent(room.ID), payload)` — 1 NATS publish.
6. `msg.Ack()`.

Per message (cache miss on first-time sender):

Same as above but step 4 hits MongoDB through `inner.FindUsersByAccounts`
and populates the cache before returning.

## Error handling

Follows `CLAUDE.md` rules throughout:

- All errors wrapped with `fmt.Errorf("…: %w", err)` including the method
  name or short description of what was being done.
- `FetchAndUpdateRoom` returns `mongo.ErrNoDocuments` wrapped when the room
  is missing. The handler propagates the error up, which causes the
  JetStream message to be Nak'd and redelivered — same behavior as today
  when `GetRoom` misses.
- Cache miss on `inner.FindUsersByAccounts` returns the partial result
  plus a wrapped error. The handler falls through to a warn log ("sender
  lookup failed, falling back to account") exactly as today — the enriched
  fields are missing on that one message.
- Cache operations are lock-protected; all `return`s release via defer.

## Concurrency

- Single `sync.Mutex` around the cache. Cache hit work is microseconds
  (map lookup + LRU `MoveToFront`), so lock contention on 100 concurrent
  goroutines is negligible even at sustained 10 000 msg/s.
- The `inner.FindUsersByAccounts` call happens *outside* the mutex — the
  wrapper releases the lock before issuing the Mongo query, then
  re-acquires it to populate the cache. This prevents a slow Mongo call
  from serializing all other cache operations.
- Race conditions: two goroutines can both miss on the same account and
  each issue a Mongo query. The later one will simply overwrite the first
  result in the cache. Acceptable — the data is identical; the cost is
  one extra Mongo call per concurrent miss, which is rare in practice.

## Testing

### Unit tests: `usercache_test.go`

Standard in-package tests using `stretchr/testify`. A minimal fake
implementing `userstore.UserStore` tracks inner-call arguments so tests
can assert that cache hits do not reach the inner store.

Cases:

- `TestCachedUserStore_HitServedFromCache`: insert user, second call with
  same account doesn't call inner.
- `TestCachedUserStore_MissCallsInner`: first call issues inner lookup,
  returns result.
- `TestCachedUserStore_PartialHit`: two accounts; one cached, one not.
  Inner called with only the missing account.
- `TestCachedUserStore_TTLExpiredReFetches`: inject a clock, advance past
  TTL, verify inner is called again.
- `TestCachedUserStore_LRUEviction`: fill cache to `maxSize`, add one more,
  verify oldest entry evicted (next lookup for it hits inner).
- `TestCachedUserStore_ConcurrentSafe`: many goroutines calling
  `FindUsersByAccounts` with overlapping accounts under `-race` — no race,
  no panic, total inner calls ≤ number of distinct accounts.

Clock injection: pass a `now func() time.Time` field in the struct for
tests; default to `time.Now` in the public constructor.

### Unit tests: `handler_test.go` (modified)

Every existing test case that set up mock expectations for `GetRoom` and
`UpdateRoomOnNewMessage` is updated to a single `FetchAndUpdateRoom`
expectation.

New case: `TestHandler_FetchAndUpdateRoom_Missing` — `FetchAndUpdateRoom`
returns `mongo.ErrNoDocuments`, handler propagates, no fan-out publish
occurs.

### Integration test

`broadcast-worker/integration_test.go` covers the full pipeline against
real MongoDB + NATS testcontainers. It exercises both changes
transparently. No changes required.

### Coverage target

- `usercache.go`: 100% (pure logic, no external dependencies).
- `broadcast-worker` overall: ≥80% per `CLAUDE.md`.

## Dependencies

No new third-party Go dependencies. The cache uses stdlib
`container/list` and `sync`. The store change uses the existing
`go.mongodb.org/mongo-driver/v2` API (`FindOneAndUpdate` with
`options.FindOneAndUpdate()`).

## Deployment notes

- Backward-compatible at the data level: no schema changes, no new
  collections, no new indexes beyond what `seed.go` already creates for
  the loadgen path.
- Backward-compatible at the env-var level: new variables have defaults,
  so existing deployments pick up caching automatically with sensible
  values.
- Rolling deploy safe: the `Store` interface change is internal to
  `broadcast-worker`; no contract with other services changes.

## Future work (deferred)

- Per-service `userstore.NewMongoStore` should create its own index on
  `account` to match `CLAUDE.md`'s "create indexes in the store
  constructor" convention. Currently the loadgen `Seed` creates it; a
  proper fix is in `pkg/userstore`. Touches every consumer and is a
  separate PR.
- Redis-backed shared cache, if/when fleet topology or invalidation
  requirements demand it.
- Room cache: `FetchAndUpdateRoom` still hits Mongo every message. Rooms
  change less frequently than users but require write-through on
  `lastMsgAt` updates, so a naive read cache would be wrong. A proper
  write-through cache is a bigger design.
