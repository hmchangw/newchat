# Badge Precise Read Invalidation + Bounded-Staleness Marker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the badge cache being wiped on every room read, and bound how long it can drift, so `subscription.count` and the push badge stop forcing a MongoDB `$lookup` aggregation per read.

**Architecture:** Two coordinated changes. (1) A room read removes exactly the room read (`ClearRoom`) instead of dropping the whole set (`ClearAll`), decided from the post-update `threadUnread` returned by the read's own Mongo write. (2) The Valkey freshness marker gets its own shorter TTL, stamped only by the Mongo-verifying Seed/Reseed paths and never refreshed by bumps, so it doubles as the maximum staleness of the cached set.

**Tech Stack:** Go 1.25, `go.mongodb.org/mongo-driver/v2`, `redis/go-redis/v9` (Valkey cluster), `go.uber.org/mock`, `stretchr/testify`, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-19-badge-precise-read-invalidation-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it, watch it fail, then implement. Never write implementation first.
- **Always use `make` targets, never raw `go` commands.** `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make lint`, `make generate SERVICE=<name>`.
- **Race detector is always on** — the Makefile handles `-race`.
- **Minimum 80% coverage**; `pkg/badgecache` is shared `pkg/` code and targets 90%+.
- **Error wrapping**: `fmt.Errorf("short description: %w", err)` describing what the current function was doing. Never bare `err`, never `fmt.Errorf("error: %w", err)`.
- **Logging**: `log/slog` only, structured key-value pairs, never interpolated strings. Never log tokens or message bodies.
- **Generated mocks are never edited by hand** — regenerate with `make generate SERVICE=<name>`.
- **Never commit `.env` files. Never merge directly to `master`/`main`.**
- Work on branch `claude/taskbar-unread-count-perf-irdvyi`. Commit after each task.
- **`BADGE_MARKER_TTL` default is `10m`**, and the invariant `markerTTL <= BADGE_CACHE_TTL` must hold (a marker outliving its set reads as a fresh zero).
- Pre-commit hooks run lint and tests; fix failures rather than bypassing.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `pkg/badgecache/badgecache.go` | Valkey unread-room set + freshness marker | Add `Option`/`WithMarkerTTL`, `markerTTL` field; rewrite bump + seed scripts; stamp marker with `markerTTL` |
| `pkg/badgecache/badgecache_test.go` | Unit tests (no Valkey) | Option defaulting and clamping |
| `pkg/badgecache/badgecache_integration_test.go` | Valkey-cluster behavior | Marker TTL independence, bump-miss-on-expired-marker, seed replace semantics |
| `room-service/store.go` | `RoomStore` interface | `UpdateSubscriptionRead` returns post-update `threadUnread` count |
| `room-service/store_mongo.go` | Mongo implementation | `UpdateOne` → `FindOneAndUpdate` with projection + `ReturnDocument(After)` |
| `room-service/handler.go` | `messageRead` badge hook | `ClearAll` → conditional `ClearRoom` |
| `inbox-worker/handler.go` | `InboxStore` interface + `handleSubscriptionRead` | Same shape, plus `applied` gating |
| `inbox-worker/main.go` | Mongo implementation | Same `FindOneAndUpdate` switch |
| `user-service/config/config.go` | Config | `BadgeMarkerTTL` + validation |
| `user-service/main.go` | Wiring | Pass `WithMarkerTTL` |
| `user-service/service/subscriptions.go` | `unreadRooms`, `CountSubscriptions` | Report and honour `degraded` |
| `user-service/service/badge.go` | `BadgeCountBatch` | Honour `degraded` |
| `docs/client-api.md` | Client-facing prose | Caching/staleness note |
| `docs/notification-worker-downstream-contracts.md` | Accuracy model | Staleness bound |

**Task order matters:** Task 1 (badgecache) has no dependencies. Tasks 2 and 3 (room-service, inbox-worker) are independent of each other. Task 4 (user-service config/wiring) depends on Task 1's `WithMarkerTTL`. Task 5 (degraded) is independent. Task 6 is docs.

---

### Task 1: `pkg/badgecache` — marker TTL and script semantics

**Files:**
- Modify: `pkg/badgecache/badgecache.go`
- Test: `pkg/badgecache/badgecache_test.go`, `pkg/badgecache/badgecache_integration_test.go`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `type Option func(*Cache)`
  - `func WithMarkerTTL(d time.Duration) Option`
  - `func New(rdb redis.UniversalClient, ttl time.Duration, maxCount int, opts ...Option) *Cache` — **variadic addition is backward compatible**; the three existing call sites compile unchanged.
  - Behavior change: `BumpBatch` returns a miss for an account whose marker is absent, even when the set exists. `Seed` replaces the set rather than unioning into it.

**Background for the implementer:** the `Cache` holds one Valkey SET per account (`badge:{account}`, the unread room IDs) plus a sibling marker key (`badge:fresh:{account}`). Marker present means "this set was built from MongoDB recently and every edit since was exact." Today the marker shares the set's TTL and is refreshed by every bump, so an active user's set can go indefinitely without being re-checked against Mongo. This task makes the marker a countdown from the last Mongo verification.

- [ ] **Step 1: Write the failing unit tests for the option**

Add to `pkg/badgecache/badgecache_test.go`:

```go
func TestNew_MarkerTTLDefaultsToSetTTL(t *testing.T) {
	c := New(nil, time.Hour, 10)
	assert.Equal(t, time.Hour, c.markerTTL, "absent option ⇒ marker shares the set TTL (today's behavior)")
}

func TestWithMarkerTTL(t *testing.T) {
	tests := []struct {
		name      string
		setTTL    time.Duration
		markerTTL time.Duration
		want      time.Duration
	}{
		{"shorter than set ttl is honored", 24 * time.Hour, 10 * time.Minute, 10 * time.Minute},
		{"equal to set ttl is honored", time.Hour, time.Hour, time.Hour},
		{"longer than set ttl is clamped", time.Hour, 24 * time.Hour, time.Hour},
		{"zero falls back to set ttl", time.Hour, 0, time.Hour},
		{"negative falls back to set ttl", time.Hour, -time.Second, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(nil, tt.setTTL, 10, WithMarkerTTL(tt.markerTTL))
			assert.Equal(t, tt.want, c.markerTTL)
		})
	}
}
```

The clamp matters: a marker outliving its set would make `Count` read marker-present + set-expired as a legitimate "fresh zero" and show an empty badge to a user with unread rooms.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/badgecache`
Expected: FAIL — `c.markerTTL` undefined, `WithMarkerTTL` undefined.

- [ ] **Step 3: Implement the option**

In `pkg/badgecache/badgecache.go`, add the field to `Cache`:

```go
type Cache struct {
	rdb       redis.UniversalClient
	ttl       time.Duration
	markerTTL time.Duration
	maxCount  int
}
```

Replace `New` and add the option type:

```go
// Option customizes a Cache at construction.
type Option func(*Cache)

// WithMarkerTTL gives the freshness marker its own lifetime, shorter than the
// set's. Because only Seed/Reseed (which compute from Mongo) stamp the marker,
// its TTL is the maximum time the set can go unverified — i.e. the badge's
// maximum staleness. Non-positive, or longer than the set TTL, falls back to
// the set TTL: a marker outliving its set would read as a fresh zero.
func WithMarkerTTL(d time.Duration) Option {
	return func(c *Cache) {
		if d <= 0 || d > c.ttl {
			slog.Warn("badgecache marker TTL out of range, using set TTL", "marker_ttl", d, "set_ttl", c.ttl)
			return
		}
		c.markerTTL = d
	}
}

// New builds a Cache. ttl bounds how long a set survives without a refresh
// (missed clears self-heal); maxCount caps returned counts, non-positive
// falls back to DefaultMaxCount so a misconfig can't zero every badge.
// Without WithMarkerTTL the marker shares ttl.
func New(rdb redis.UniversalClient, ttl time.Duration, maxCount int, opts ...Option) *Cache {
	if maxCount <= 0 {
		maxCount = DefaultMaxCount
	}
	c := &Cache{rdb: rdb, ttl: ttl, markerTTL: ttl, maxCount: maxCount}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=pkg/badgecache`
Expected: PASS, including the pre-existing `TestNew_MaxCount`.

- [ ] **Step 5: Write the failing integration tests for script semantics**

Add to `pkg/badgecache/badgecache_integration_test.go`:

```go
// A bump is not a verification: it must add the room without extending the
// marker's countdown, so the staleness bound keeps running.
func TestBadgeCache_BumpDoesNotExtendMarkerTTL(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount, WithMarkerTTL(30*time.Second))
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"roomA"})
	before, err := rdb.PTTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)
	require.Positive(t, before)

	// Wait past the resolution of PTTL so a refresh would be visible as an increase.
	time.Sleep(1100 * time.Millisecond)
	assert.Equal(t, map[string]int{"alice": 2}, c.BumpBatch(ctx, []string{"alice"}, "roomB"))

	after, err := rdb.PTTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)
	assert.Less(t, after, before, "bump must not re-stamp the marker")
}

// Marker and set carry independent lifetimes; the marker is the shorter one.
func TestBadgeCache_MarkerTTLShorterThanSetTTL(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount, WithMarkerTTL(30*time.Second))
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"roomA"})

	setTTL, err := rdb.TTL(ctx, Key("alice")).Result()
	require.NoError(t, err)
	markerTTL, err := rdb.TTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)
	assert.Greater(t, setTTL, markerTTL)
	assert.LessOrEqual(t, markerTTL, 30*time.Second)
}

// An expired marker means the set is unverified: bump must miss so the caller
// recomputes, even though the set is still present.
func TestBadgeCache_BumpMissesWhenMarkerExpiredButSetSurvives(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"roomA"})
	require.NoError(t, rdb.Del(ctx, MarkerKey("alice")).Err()) // simulate marker expiry
	require.Equal(t, int64(1), rdb.Exists(ctx, Key("alice")).Val(), "set still present")

	assert.Empty(t, c.BumpBatch(ctx, []string{"alice"}, "roomB"), "unverified set ⇒ miss")

	n, fresh := c.Count(ctx, "alice")
	assert.False(t, fresh)
	assert.Zero(t, n)
}

// On a miss the caller holds fresh Mongo truth, and any surviving set is
// unverified — so Seed replaces rather than unions.
func TestBadgeCache_SeedReplacesUnverifiedSet(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"stale1", "stale2"})
	require.NoError(t, rdb.Del(ctx, MarkerKey("alice")).Err())

	n, ok := c.Seed(ctx, "alice", []string{"fresh1"}, "trigger1")
	require.True(t, ok)
	assert.Equal(t, 2, n, "fresh1 ∪ trigger1 only — the stale set must not survive")

	members, err := rdb.SMembers(ctx, Key("alice")).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fresh1", "trigger1"}, members)
}

// Marker-present/set-absent (fresh all-read) is still a hit that recreates the set.
func TestBadgeCache_BumpHitsOnMarkerOnlyState(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", nil) // fresh, zero unread: marker only
	require.Equal(t, int64(0), rdb.Exists(ctx, Key("alice")).Val())

	assert.Equal(t, map[string]int{"alice": 1}, c.BumpBatch(ctx, []string{"alice"}, "roomA"))
}
```

- [ ] **Step 6: Run the integration tests to verify they fail**

Run: `make test-integration SERVICE=pkg/badgecache`
Expected: FAIL — the bump still re-stamps the marker, the marker still shares the set TTL, and `Seed` still unions.

- [ ] **Step 7: Rewrite the scripts**

In `pkg/badgecache/badgecache.go`, replace `bumpScript`:

```go
// bumpScript SADDs one room and refreshes the SET's TTL only. The marker's TTL
// is deliberately untouched: a bump is not a verification against Mongo, so it
// must not extend the staleness countdown. Miss (-1, no writes) when the marker
// is absent — a surviving set without a marker is unverified and the caller must
// recompute. KEYS=[set, marker], ARGV=[roomID, setTtlSec]; returns SCARD on hit.
var bumpScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 then
  return -1
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return redis.call('SCARD', KEYS[1])
`)
```

Replace `seedScript`:

```go
// seedScript replaces the set from Mongo-derived IDs and stamps the marker.
// Replace (not union): seed runs on a miss, where any surviving set is
// unverified and must not contaminate a freshly computed answer.
// KEYS=[set, marker], ARGV=[setTtlSec, markerTtlSec, roomIDs...]; returns SCARD.
var seedScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if #ARGV > 2 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 3))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return redis.call('SCARD', KEYS[1])
`)
```

Replace `reseedScript`:

```go
// reseedScript replaces the set wholesale (DEL, then SADD if any IDs) and
// stamps the marker either way — an empty reseed records "fresh, zero unread".
// KEYS=[set, marker], ARGV=[setTtlSec, markerTtlSec, roomIDs...].
var reseedScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if #ARGV > 2 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 3))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return 1
`)
```

- [ ] **Step 8: Update the ARGV builders to carry both TTLs**

`seedArgs` gains the marker TTL as the second element:

```go
// seedArgs builds the seedScript ARGV: set ttl seconds, marker ttl seconds,
// then roomIDs deduplicated with triggerRoomID appended (skipped when empty).
func seedArgs(ttl, markerTTL time.Duration, roomIDs []string, triggerRoomID string) []interface{} {
	seen := make(map[string]struct{}, len(roomIDs)+1)
	argv := make([]interface{}, 0, len(roomIDs)+3)
	argv = append(argv, ttlSeconds(ttl), ttlSeconds(markerTTL))
	add := func(id string) {
		if id == "" {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		argv = append(argv, id)
	}
	for _, id := range roomIDs {
		add(id)
	}
	add(triggerRoomID)
	return argv
}
```

Update `Seed` to pass it:

```go
func (c *Cache) Seed(ctx context.Context, account string, roomIDs []string, triggerRoomID string) (count int, ok bool) {
	argv := seedArgs(c.ttl, c.markerTTL, roomIDs, triggerRoomID)
	n, err := seedScript.Run(ctx, c.rdb, scriptKeys(account), argv...).Int64()
	if err != nil {
		slog.WarnContext(ctx, "badgecache seed failed", "account", account, "error", err)
		return 0, false
	}
	return c.capCount(n), true
}
```

Update `Reseed`'s ARGV the same way:

```go
func (c *Cache) Reseed(ctx context.Context, account string, roomIDs []string) {
	argv := make([]interface{}, 0, len(roomIDs)+2)
	argv = append(argv, ttlSeconds(c.ttl), ttlSeconds(c.markerTTL))
	for _, id := range roomIDs {
		argv = append(argv, id)
	}
	if err := reseedScript.Run(ctx, c.rdb, scriptKeys(account), argv...).Err(); err != nil {
		slog.WarnContext(ctx, "badgecache reseed failed", "account", account, "error", err)
	}
}
```

`BumpBatch`'s ARGV is unchanged (`roomID`, `ttlSeconds(c.ttl)`) — it only touches the set's TTL. Update its doc comment to say the hit condition is marker existence.

Also update `Seed`'s doc comment: it now *replaces* rather than "creates or extends".

- [ ] **Step 9: Run all badgecache tests**

Run: `make test SERVICE=pkg/badgecache && make test-integration SERVICE=pkg/badgecache`
Expected: PASS, including the pre-existing `TestBadgeCache_BumpMissThenSeedThenBump` and the NOSCRIPT retry test.

- [ ] **Step 10: Verify coverage and lint**

Run: `make lint`
Then check coverage is ≥90% for this package using the repo's coverage flow (`go test -coverprofile` is wrapped by the Makefile's test targets; if the target does not emit a profile, run `make test SERVICE=pkg/badgecache` and confirm no new uncovered branches were introduced by inspecting the new `WithMarkerTTL` paths are all exercised by the table test).

- [ ] **Step 11: Commit**

```bash
git add pkg/badgecache/
git commit -m "feat(badgecache): give the freshness marker its own TTL

The marker means 'this set was computed from Mongo recently'. Refreshing it on
every bump let an active user's set go indefinitely without verification, so
bumps no longer stamp it and it carries its own shorter TTL, making it a
countdown from the last Mongo verification.

A surviving set with an expired marker is now a bump miss rather than a hit, and
Seed replaces the set instead of unioning into it, so an unverified leftover
cannot contaminate a freshly computed answer.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: room-service — precise `ClearRoom` on room read

**Files:**
- Modify: `room-service/store.go:129`, `room-service/store_mongo.go:1098`, `room-service/handler.go:1389-1392`
- Regenerate: `room-service/mock_store_test.go`
- Test: `room-service/handler_test.go`, `room-service/integration_test.go`

**Interfaces:**
- Consumes: nothing from Task 1 (independent).
- Produces: `RoomStore.UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) (threadUnread int, err error)` — the returned `int` is `len(threadUnread)` **after** the write.

**Background:** after a room read, `lastSeenAt = now`, so message-unread is settled by construction. The only way the room can still count as unread is an unread followed thread (`Subscription.threadUnread`). The value must come from the **post-update** document, not from the `sub` fetched earlier in the handler: `threadUnread` is written by message-worker while the badge bump comes from notification-worker, two independent MESSAGES-CANONICAL consumers with no ordering between them, so a pre-write snapshot can miss an `$addToSet` that is about to land.

- [ ] **Step 1: Write the failing handler tests**

Add to `room-service/handler_test.go`. The existing `newMessageReadFixture(t)` helper and `fakeBadgeCache` (recording `clearRooms` and `clearAlls`) are already defined in that file — reuse them.

```go
// A fully-read room (no unread followed threads) is removed exactly; the rest of
// the set — and the freshness marker — survive.
func TestHandler_MessageRead_NoThreadUnread_ClearsRoomOnly(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().
		UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(0, nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Equal(t, []clearRoomCall{{account: "alice", roomID: "r1"}}, f.badge.clearRooms)
	assert.Empty(t, f.badge.clearAlls, "a read must never drop the whole set")
}

// The room still has unread followed threads, so it was unread before the read
// and remains unread after it — an exact set needs no edit at all.
func TestHandler_MessageRead_ThreadUnreadRemains_NoBadgeWrite(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().
		UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(2, nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Empty(t, f.badge.clearRooms)
	assert.Empty(t, f.badge.clearAlls)
}

// The decision must follow the POST-UPDATE value, not the pre-write snapshot:
// threadUnread is written by another service on an unordered path, so a stale
// snapshot can be wrong in either direction.
func TestHandler_MessageRead_UsesPostUpdateThreadUnread(t *testing.T) {
	tests := []struct {
		name             string
		snapshot         []string
		postUpdate       int
		wantClearRoom    bool
	}{
		{"snapshot empty, post-update non-empty ⇒ no write", nil, 3, false},
		{"snapshot non-empty, post-update empty ⇒ clear room", []string{"p1"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMessageReadFixture(t)
			joined := time.Now().UTC().Add(-2 * time.Hour)
			lastSeen := joined.Add(time.Hour)

			f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
				User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
				ThreadUnread: tt.snapshot,
			}, nil)
			f.store.EXPECT().
				UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any()).
				Return(tt.postUpdate, nil)
			f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
			f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil)

			_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
			require.NoError(t, err)

			if tt.wantClearRoom {
				assert.Equal(t, []clearRoomCall{{account: "alice", roomID: "r1"}}, f.badge.clearRooms)
			} else {
				assert.Empty(t, f.badge.clearRooms)
			}
			assert.Empty(t, f.badge.clearAlls)
		})
	}
}

// A cross-site reader's home replica is inbox-worker's job — the room site must
// not touch the local set.
func TestHandler_MessageRead_CrossSiteActor_NoLocalBadgeWrite(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().
		UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(0, nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil)

	_, _ = f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))

	assert.Empty(t, f.badge.clearRooms)
	assert.Empty(t, f.badge.clearAlls)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=room-service`
Expected: FAIL — `UpdateSubscriptionRead` returns only `error`, so `.Return(0, nil)` does not compile.

- [ ] **Step 3: Change the store interface**

In `room-service/store.go`, replace the `UpdateSubscriptionRead` declaration (line ~129):

```go
	// UpdateSubscriptionRead sets lastSeenAt on the subscription keyed by
	// (roomID, account), clearing alert and hasMention (reading the room
	// clears both), and returns the number of unread followed threads left on
	// the subscription AFTER the write — the badge hook's exactness depends on
	// the post-update value, not on a pre-write snapshot.
	// Returns model.ErrSubscriptionNotFound when no subscription matches.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) (int, error)
```

- [ ] **Step 4: Change the Mongo implementation**

In `room-service/store_mongo.go`, replace the whole `UpdateSubscriptionRead` function:

```go
// UpdateSubscriptionRead sets lastSeenAt on the subscription keyed by
// (roomID, account), clearing alert and hasMention, and returns the post-update
// unread-thread count. FindOneAndUpdate rather than UpdateOne: the same single
// round trip also yields the post-state the badge hook needs.
// Returns model.ErrSubscriptionNotFound when no subscription matches.
func (s *MongoStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time) (int, error) {
	var out struct {
		ThreadUnread []string `bson:"threadUnread"`
	}
	opts := options.FindOneAndUpdate().
		SetProjection(bson.D{{Key: "threadUnread", Value: 1}}).
		SetReturnDocument(options.After)
	err := s.subscriptions.Raw().FindOneAndUpdate(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": false, "hasMention": false}},
		opts,
	).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	return len(out.ThreadUnread), nil
}
```

Confirm `errors`, `mongo`, and `options` are already imported in this file; add any that are missing.

- [ ] **Step 5: Change the handler hook**

In `room-service/handler.go`, capture the returned count in the errgroup (the `UpdateSubscriptionRead` call at ~line 1359):

```go
	var threadUnread int
	g.Go(func() error {
		n, err := h.store.UpdateSubscriptionRead(gctx, roomID, account, now)
		if err != nil {
			return fmt.Errorf("update subscription read: %w", err)
		}
		threadUnread = n
		return nil
	})
```

`threadUnread` is written by exactly one goroutine and read only after `g.Wait()`, so no synchronisation is needed — the same pattern the existing `userSiteID` and `room` variables use.

Then replace the badge hook (the `ClearAll` at ~line 1391):

```go
	// A read settles message-unread (lastSeenAt = now), so the room stays unread
	// only if a followed thread is unread. None ⇒ exact removal, freshness marker
	// survives. Some ⇒ the room was and remains unread, so the set needs no edit.
	// Home-local only; inbox-worker handles cross-site replicas.
	if userSiteID == h.siteID && h.badge != nil && threadUnread == 0 {
		h.badge.ClearRoom(ctx, account, roomID)
	}
```

- [ ] **Step 6: Regenerate the mock**

Run: `make generate SERVICE=room-service`
This rewrites `room-service/mock_store_test.go`. Never hand-edit it.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS. Pre-existing `messageRead` tests that stub `UpdateSubscriptionRead(...).Return(nil)` must be updated to `.Return(0, nil)` — do that as part of this step; there is no behavior change for them.

- [ ] **Step 8: Add the integration test for the post-update read**

Add to `room-service/integration_test.go`, using the file's existing `setupMongo(t)` helper (`integration_test.go:38`) and `NewMongoStore(db)`:

```go
func TestMongoStore_UpdateSubscriptionRead_ReturnsPostUpdateThreadUnread(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: time.Now().UTC().Add(-time.Hour),
		ThreadUnread: []string{"p1", "p2"},
	})
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	n, err := store.UpdateSubscriptionRead(ctx, "r1", "alice", now)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "reading the room must not drain threadUnread")

	var got model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	require.NotNil(t, got.LastSeenAt)
	assert.WithinDuration(t, now, *got.LastSeenAt, time.Second)
	assert.False(t, got.Alert)

	_, err = store.UpdateSubscriptionRead(ctx, "r1", "nobody", now)
	require.ErrorIs(t, err, model.ErrSubscriptionNotFound)
}
```

- [ ] **Step 9: Run the integration tests**

Run: `make test-integration SERVICE=room-service`
Expected: PASS.

- [ ] **Step 10: Lint and commit**

```bash
make lint
git add room-service/
git commit -m "feat(room-service): remove only the room just read from the badge set

A read set lastSeenAt to now and then dropped the user's entire unread-room set,
so the client's immediate count refetch — and the next message's badge bump —
both rebuilt it with the full \$lookup aggregation.

After a read the room can only still be unread via an unread followed thread, so
UpdateSubscriptionRead now returns the post-update threadUnread count (one
FindOneAndUpdate, same round trip) and the handler removes exactly that room, or
leaves the set untouched when threads remain.

The post-update value matters: threadUnread is written by message-worker while
the badge bump comes from notification-worker, two unordered consumers, so a
pre-write snapshot can miss an \$addToSet that is about to land.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: inbox-worker — precise `ClearRoom` on federated read

**Files:**
- Modify: `inbox-worker/handler.go:47` (interface), `inbox-worker/handler.go:355-369` (`handleSubscriptionRead`), `inbox-worker/main.go:430`
- Regenerate: `inbox-worker/mock_store_test.go`
- Test: `inbox-worker/handler_test.go`, `inbox-worker/integration_test.go`

**Interfaces:**
- Consumes: nothing from Tasks 1–2 (independent).
- Produces: `InboxStore.UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) (applied bool, threadUnread int, err error)` — `applied` is false when the `$lt lastSeenAt` order guard rejected the event (a replay or out-of-order delivery), in which case no write happened.

**Background:** this is the home-replica leaf of a cross-site read. It keeps its `$lt lastSeenAt` order-safety filter. Today it calls `ClearAll` whenever the store call returns no error — including when the guard matched nothing, so a replayed event discards a perfectly good set. Gating on `applied` fixes that alongside the main change.

- [ ] **Step 1: Write the failing handler tests**

**First, replace the existing test that encodes the old behavior.** `TestHandler_HandleEvent_SubscriptionRead_BadgeCache_ClearAll` (`inbox-worker/handler_test.go:1499`) asserts `ClearAll` with the comment *"remaining threads must NOT prevent the drop"* — precisely the behavior being inverted. Delete it and add the three tests below in its place, keeping `..._NilBadgeNoPanic` (`:1519`) as-is.

These use the file's real helpers: `stubInboxStore` (an in-memory store over `s.subscriptions`, mutex-guarded), `fakeBadgeCache` with `getClearRooms()`/`getClearAlls()`, `NewHandler(store)` with `h.badge` assigned after construction, and `subscriptionReadInboxPayload(t, account, roomID)` driving `h.HandleEvent`.

```go
// A fully-read room is removed exactly; the set and its freshness marker survive.
func TestHandler_HandleEvent_SubscriptionRead_BadgeCache_ClearsRoomOnly(t *testing.T) {
	store := &stubInboxStore{}
	store.mu.Lock()
	store.subscriptions = append(store.subscriptions, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
	})
	store.mu.Unlock()
	h := NewHandler(store)
	badge := &fakeBadgeCache{}
	h.badge = badge

	require.NoError(t, h.HandleEvent(context.Background(), subscriptionReadInboxPayload(t, "alice", "r1")))

	assert.Equal(t, []clearRoomCall{{account: "alice", roomID: "r1"}}, badge.getClearRooms())
	assert.Empty(t, badge.getClearAlls(), "a read must never drop the whole set")
}

// Unread followed threads remain, so the room was and stays unread — no edit.
func TestHandler_HandleEvent_SubscriptionRead_BadgeCache_ThreadUnreadRemains(t *testing.T) {
	store := &stubInboxStore{}
	store.mu.Lock()
	store.subscriptions = append(store.subscriptions, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		ThreadUnread: []string{"p1"},
	})
	store.mu.Unlock()
	h := NewHandler(store)
	badge := &fakeBadgeCache{}
	h.badge = badge

	require.NoError(t, h.HandleEvent(context.Background(), subscriptionReadInboxPayload(t, "alice", "r1")))

	assert.Empty(t, badge.getClearRooms())
	assert.Empty(t, badge.getClearAlls())
}

// A replayed / out-of-order event the $lt guard rejects wrote nothing, so it
// must not disturb the set either.
func TestHandler_HandleEvent_SubscriptionRead_BadgeCache_GuardRejected(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour)
	store := &stubInboxStore{}
	store.mu.Lock()
	store.subscriptions = append(store.subscriptions, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		LastSeenAt: &future, // already read past the incoming event
	})
	store.mu.Unlock()
	h := NewHandler(store)
	badge := &fakeBadgeCache{}
	h.badge = badge

	require.NoError(t, h.HandleEvent(context.Background(), subscriptionReadInboxPayload(t, "alice", "r1")))

	assert.Empty(t, badge.getClearRooms())
	assert.Empty(t, badge.getClearAlls())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=inbox-worker`
Expected: FAIL — the stub has no `readApplied`/`readThreadUnread`, and the interface returns only `error`.

- [ ] **Step 3: Change the store interface**

In `inbox-worker/handler.go` (~line 42-47):

```go
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account), guarded so an out-of-order or replayed event
	// cannot regress the read position. Returns applied=false when that guard
	// matched nothing (no write happened), and the number of unread followed
	// threads left on the subscription after the write.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) (bool, int, error)
```

- [ ] **Step 4: Change the Mongo implementation**

In `inbox-worker/main.go`, replace `mongoInboxStore.UpdateSubscriptionRead`:

```go
func (s *mongoInboxStore) UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) (bool, int, error) {
	filter := bson.M{
		"roomId":    roomID,
		"u.account": account,
		"$or": bson.A{
			bson.M{"lastSeenAt": bson.M{"$exists": false}},
			bson.M{"lastSeenAt": bson.M{"$lt": lastSeenAt}},
		},
	}
	update := bson.M{"$set": bson.M{"lastSeenAt": lastSeenAt, "alert": alert}}
	var out struct {
		ThreadUnread []string `bson:"threadUnread"`
	}
	opts := options.FindOneAndUpdate().
		SetProjection(bson.D{{Key: "threadUnread", Value: 1}}).
		SetReturnDocument(options.After)
	err := s.subCol.FindOneAndUpdate(ctx, filter, update, opts).Decode(&out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Either the subscription is missing (NAK to retry) or the order guard
		// rejected a stale event (a no-op, not a failure).
		return false, 0, s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	if err != nil {
		return false, 0, fmt.Errorf("update subscription read for %q in room %q: %w", account, roomID, err)
	}
	return true, len(out.ThreadUnread), nil
}
```

Check whether `s.subCol` is a raw `*mongo.Collection` or a wrapper; if it is a wrapper without `FindOneAndUpdate`, use its `.Raw()` accessor as the other call sites in this file do.

- [ ] **Step 5: Change the handler**

In `inbox-worker/handler.go`, `handleSubscriptionRead`:

```go
	applied, threadUnread, err := h.store.UpdateSubscriptionRead(ctx, e.RoomID, e.Account, lastSeenAt, e.Alert)
	if err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	// The read settles message-unread, so the room stays unread only via an
	// unread followed thread. Skip entirely when the order guard rejected the
	// event — nothing was written, so nothing should be invalidated.
	if applied && h.badge != nil && threadUnread == 0 {
		h.badge.ClearRoom(ctx, e.Account, e.RoomID)
	}
	return nil
```

- [ ] **Step 6: Update the in-memory test stub**

`stubInboxStore.UpdateSubscriptionRead` (`inbox-worker/handler_test.go:353`) is a real in-memory implementation, not a canned-return mock — it must model the new contract or the tests above will not exercise anything:

```go
func (s *stubInboxStore) UpdateSubscriptionRead(_ context.Context, roomID, account string, lastSeenAt time.Time, alert bool) (bool, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subReads = append(s.subReads, subRead{roomID, account, lastSeenAt, alert})
	for i := range s.subscriptions {
		if s.subscriptions[i].RoomID == roomID && s.subscriptions[i].User.Account == account {
			// Order-safe: skip if stored lastSeenAt is not strictly earlier.
			if s.subscriptions[i].LastSeenAt != nil && !s.subscriptions[i].LastSeenAt.Before(lastSeenAt) {
				return false, 0, nil
			}
			ls := lastSeenAt
			s.subscriptions[i].LastSeenAt = &ls
			s.subscriptions[i].Alert = alert
			// A room read does not drain threadUnread — only thread.read does.
			return true, len(s.subscriptions[i].ThreadUnread), nil
		}
	}
	return false, 0, nil // missing-subscription → no-op
}
```

- [ ] **Step 7: Regenerate the mock**

Run: `make generate SERVICE=inbox-worker`

- [ ] **Step 8: Run the tests to verify they pass**

Run: `make test SERVICE=inbox-worker`
Expected: PASS.

- [ ] **Step 9: Update the integration tests**

`inbox-worker/integration_test.go` already has four tests for this method — `..._HappyPath` (:279), `..._OutOfOrderSkipped` (:306), `..._EqualTimestampSkipped` (:333), `..._MissingSubscriptionErrors` (:357). Update each call to the three-value form and assert `applied` matches the test's intent (`true` for happy path, `false` for both skip cases). They build the store as:

```go
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}
```

Then add one new test for the post-state:

```go
func TestInbox_UpdateSubscriptionRead_ReturnsPostUpdateThreadUnread(t *testing.T) {
	ctx := context.Background()
	db := setupMongo(t)
	store := &mongoInboxStore{
		subCol:       db.Collection("subscriptions"),
		roomCol:      db.Collection("rooms"),
		userCol:      db.Collection("users"),
		threadSubCol: db.Collection("thread_subscriptions"),
	}

	_, err := store.subCol.InsertOne(ctx, model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", JoinedAt: time.Now().UTC().Add(-time.Hour),
		ThreadUnread: []string{"p1"},
	})
	require.NoError(t, err)

	applied, n, err := store.UpdateSubscriptionRead(ctx, "r1", "alice", time.Now().UTC().Truncate(time.Millisecond), false)
	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, 1, n, "a room read must not drain threadUnread")
}
```

- [ ] **Step 10: Run the integration tests**

Run: `make test-integration SERVICE=inbox-worker`
Expected: PASS.

- [ ] **Step 11: Lint and commit**

```bash
make lint
git add inbox-worker/
git commit -m "feat(inbox-worker): remove only the room just read from the badge set

Mirrors room-service for the federated home replica: UpdateSubscriptionRead
returns the post-update threadUnread count from the FindOneAndUpdate it already
performs, and the handler removes exactly the room read instead of dropping the
whole set.

Also gates the badge write on the \$lt order guard actually matching — a replayed
or out-of-order event previously discarded a good set despite writing nothing.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: user-service — `BADGE_MARKER_TTL` config and wiring

**Files:**
- Modify: `user-service/config/config.go:62-70` (field), `:109` (validation), `user-service/main.go:173`, `user-service/deploy/docker-compose.yml`
- Test: `user-service/config/config_test.go` (or the existing config test file in that package)

**Interfaces:**
- Consumes: `badgecache.WithMarkerTTL(d time.Duration) Option` from Task 1.
- Produces: `Config.BadgeMarkerTTL time.Duration`.

**Background:** only user-service writes the marker (`Seed`/`Reseed`/`BumpBatch` all live there); room-service and inbox-worker only clear. So this is a one-service config addition.

- [ ] **Step 1: Write the failing config tests**

Add to the config package's existing test file, matching its current table style:

```go
func TestLoad_BadgeMarkerTTLDefault(t *testing.T) {
	t.Setenv("SITE_ID", "site-a") // plus whatever else Load() requires; copy from the neighbouring tests
	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, cfg.BadgeMarkerTTL)
}

func TestLoad_BadgeMarkerTTLValidation(t *testing.T) {
	tests := []struct {
		name      string
		cacheTTL  string
		markerTTL string
		wantErr   string
	}{
		{"zero is rejected", "24h", "0s", "BADGE_MARKER_TTL"},
		{"negative is rejected", "24h", "-1m", "BADGE_MARKER_TTL"},
		{"longer than cache ttl is rejected", "10m", "1h", "BADGE_MARKER_TTL"},
		{"equal to cache ttl is accepted", "10m", "10m", ""},
		{"shorter than cache ttl is accepted", "24h", "10m", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SITE_ID", "site-a") // plus the other required vars
			t.Setenv("BADGE_CACHE_TTL", tt.cacheTTL)
			t.Setenv("BADGE_MARKER_TTL", tt.markerTTL)
			_, err := Load()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `cfg.BadgeMarkerTTL` undefined.

- [ ] **Step 3: Add the config field**

In `user-service/config/config.go`, directly below `BadgeCacheTTL`:

```go
	// BadgeMarkerTTL bounds how long the badge set may go without verification
	// against Mongo: the freshness marker is stamped only by Seed/Reseed (which
	// compute from Mongo) and is never refreshed by bumps, so its lifetime is
	// the maximum badge staleness. Must be <= BADGE_CACHE_TTL — a marker
	// outliving its set would read as a fresh zero and show an empty badge.
	BadgeMarkerTTL time.Duration `env:"BADGE_MARKER_TTL" envDefault:"10m"`
```

- [ ] **Step 4: Add the validation**

In `Load()`, immediately after the existing `BadgeCacheTTL` check (~line 109-111):

```go
	if cfg.BadgeMarkerTTL <= 0 {
		return Config{}, fmt.Errorf("BADGE_MARKER_TTL must be > 0, got %s", cfg.BadgeMarkerTTL)
	}
	if cfg.BadgeMarkerTTL > cfg.BadgeCacheTTL {
		return Config{}, fmt.Errorf("BADGE_MARKER_TTL (%s) must be <= BADGE_CACHE_TTL (%s)", cfg.BadgeMarkerTTL, cfg.BadgeCacheTTL)
	}
```

- [ ] **Step 5: Wire it in main**

In `user-service/main.go` (~line 173):

```go
		badge = badgecache.New(valkeyClient, cfg.BadgeCacheTTL, cfg.BadgeCountCap, badgecache.WithMarkerTTL(cfg.BadgeMarkerTTL))
		slog.Info("badge cache enabled", "ttl", cfg.BadgeCacheTTL, "marker_ttl", cfg.BadgeMarkerTTL, "count_cap", cfg.BadgeCountCap)
```

- [ ] **Step 6: Set it in the local compose file**

In `user-service/deploy/docker-compose.yml`, alongside the other `BADGE_*` vars in the user-service service's `environment` block:

```yaml
      - BADGE_MARKER_TTL=${BADGE_MARKER_TTL:-10m}
```

- [ ] **Step 7: Add the marker-expiry service test**

An expired marker must send `subscription.count` back to Mongo and re-verify the
set. Add to `user-service/service/subscriptions_test.go`, using the existing
`fakeBadgeCache` (its `count` field is a func hook returning `(int, bool)`) and
`newBadgeService` helper from `badge_test.go`:

```go
// Marker expired ⇒ Count reports stale ⇒ recompute from Mongo and Reseed, which
// re-stamps the marker. This is the self-heal that bounds badge drift.
func TestCountSubscriptions_StaleMarker_RecomputesAndReseeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{count: func(string) (int, bool) { return 0, false }}
	svc := newBadgeService(t, subs, badge)
	svc.badgeCacheFirst = true

	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).Return(
		[]model.EnrichedSubscription{localUnreadSub("alice", "r1", "site-a")}, nil)

	unread := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &unread})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
	assert.Equal(t, []string{"alice"}, badge.reseedCalls, "a stale marker must be re-stamped from Mongo truth")
}

// Fresh marker ⇒ served from SCARD, no repo call at all.
func TestCountSubscriptions_FreshMarker_NoRepoCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{count: func(string) (int, bool) { return 4, true }}
	svc := newBadgeService(t, subs, badge)
	svc.badgeCacheFirst = true

	unread := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &unread})
	require.NoError(t, err)
	assert.Equal(t, 4, resp.Count)
	assert.Empty(t, badge.reseedCalls)
}
```

If an equivalent pair already exists in the package (the cache-first gate shipped
with its own tests), extend those rather than duplicating them.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 9: Lint and commit**

```bash
make lint
git add user-service/config/ user-service/main.go user-service/deploy/docker-compose.yml user-service/service/
git commit -m "feat(user-service): add BADGE_MARKER_TTL to bound badge staleness

The freshness marker is stamped only by the Mongo-verifying Seed/Reseed paths
and is no longer refreshed by bumps, so its TTL is the maximum time the cached
set can go unverified. Defaults to 10m and is validated against BADGE_CACHE_TTL,
since a marker outliving its set would read as a fresh zero.

Only user-service writes the marker, so no config change is needed in
room-service or inbox-worker.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: user-service — a degraded compute must not stamp the marker

**Files:**
- Modify: `user-service/service/subscriptions.go:580-663` (`unreadRooms`), `:547-572` (`CountSubscriptions`), `user-service/service/badge.go:19-41` (`BadgeCountBatch`)
- Test: `user-service/service/subscriptions_test.go:832-869`, `user-service/service/badge_test.go`

**Interfaces:**
- Consumes: nothing from Tasks 1–4 (independent, though it is what makes Task 4's bound meaningful).
- Produces: `func (s *UserService) unreadRooms(c *natsrouter.Context, account string) (ids []string, degraded bool, err error)` — `degraded` is true when at least one cross-site `GetRoomsMeta` RPC failed and that site's rooms were dropped from `ids`.

**Background:** `unreadRooms` degrades per-site — a failed cross-site RPC drops that site's rooms and only logs a warning, still returning `(ids, nil)`. Callers then `Reseed`/`Seed` that short list, which stamps the marker and blesses a knowingly-incomplete set as verified. Today the next room read wipes it seconds later; once reads stop wiping the set (Tasks 2–3), one transient blip would consume a whole marker window.

- [ ] **Step 1: Write the failing tests**

Add to `user-service/service/subscriptions_test.go` and `badge_test.go`, reusing the existing `fakeBadgeCache` (which records `reseedCalls`, `seedCalls`, `bumpCalls`) and `mocks.MockSubscriptionRepository`:

```go
// A failing cross-site site still yields a best-effort count, but nothing may be
// written to the cache — a partial set must never be stamped as verified.
func TestCountSubscriptions_Degraded_NoReseed(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)
	svc.rooms = failingRoomClient{} // returns an error from GetRoomsMeta for every site

	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).Return(
		[]model.EnrichedSubscription{crossSiteSub("alice", "r-remote", "site-b")}, nil)

	unread := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &unread})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count, "the unreachable site's rooms drop out")
	assert.Empty(t, badge.reseedCalls, "a degraded compute must not stamp the marker")
}

func TestBadgeCountBatch_Degraded_NoSeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)
	svc.rooms = failingRoomClient{}

	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).Return(
		[]model.EnrichedSubscription{crossSiteSub("alice", "r-remote", "site-b")}, nil)

	resp, err := svc.BadgeCountBatch(ctx("alice", "site-a"),
		model.BadgeCountBatchRequest{RoomID: "r-trigger", Accounts: []string{"alice"}})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Counts["alice"], "still answers from ids ∪ trigger")
	assert.Empty(t, badge.seedCalls, "a degraded compute must not stamp the marker")
}

// The non-degraded path is unchanged: it still writes through to the cache.
func TestCountSubscriptions_NotDegraded_StillReseeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	badge := &fakeBadgeCache{}
	svc := newBadgeService(t, subs, badge)

	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).Return(
		[]model.EnrichedSubscription{localUnreadSub("alice", "r1", "site-a")}, nil)

	unread := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &unread})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
	assert.Equal(t, []string{"alice"}, badge.reseedCalls)
}
```

`failingRoomClient`, `crossSiteSub` and `localUnreadSub` are small helpers — the existing cross-site degradation tests at `subscriptions_test.go:851-869` already build equivalents. Reuse those rather than duplicating; if they are inline, extract them into named helpers in the same `_test.go` file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `unreadRooms` returns two values, and the degraded paths currently do reseed/seed.

- [ ] **Step 3: Change `unreadRooms` to report degradation**

In `user-service/service/subscriptions.go`, change the signature and doc comment:

```go
// unreadRooms returns the account's active room IDs with unread activity, and
// whether the result is degraded — true when at least one cross-site
// GetRoomsMeta RPC failed and that site's rooms were dropped. A degraded result
// is still returned (best-effort, as before) but must not be cached: writing it
// would stamp the freshness marker on a knowingly-incomplete set.
func (s *UserService) unreadRooms(c *natsrouter.Context, account string) ([]string, bool, error) {
```

Record the failure in the per-site goroutine. Because the goroutines run concurrently, use a parallel `[]bool` indexed like the existing `results` slice — each element written by exactly one goroutine — rather than a shared flag:

```go
	results := make([][]string, len(sites))
	failed := make([]bool, len(sites))
```

In the goroutine's error branch, alongside the existing warning:

```go
			if err != nil {
				// Skip this site rather than nuking the whole result.
				slog.WarnContext(c, "unread count degraded for site", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(c), "error", err)
				failed[i] = true
				return
			}
```

A goroutine that returns early because `c.Err() != nil` (client gone) also leaves its site uncounted — set `failed[i] = true` there too, and in the `break` path before the goroutine is launched mark the remaining sites as failed so a cancelled request is never cached as complete.

After `wg.Wait()`:

```go
	degraded := false
	for i := range results {
		ids = append(ids, results[i]...)
		if failed[i] {
			degraded = true
		}
	}
	return ids, degraded, nil
```

Update the early `return ids, nil` paths (the no-cross-site case and the error case) to the three-value form: `return nil, false, fmt.Errorf(...)` and `return ids, false, nil`.

- [ ] **Step 4: Honour `degraded` in `CountSubscriptions`**

```go
	ids, degraded, err := s.unreadRooms(c, account)
	if err != nil {
		return nil, err
	}
	// Best-effort reconciliation from the Mongo source of truth (fail-open) —
	// skipped when degraded, since caching a partial set would stamp the
	// freshness marker on data we already know is incomplete.
	if !degraded {
		s.badge.Reseed(c, account, ids)
	}
	return &models.CountResponse{Count: len(ids)}, nil
```

- [ ] **Step 5: Honour `degraded` in `BadgeCountBatch`**

In `user-service/service/badge.go`:

```go
		ids, degraded, err := s.unreadRooms(c, account)
		if err != nil {
			slog.WarnContext(c, "badge seed degraded", "account", account, "room_id", req.RoomID, "request_id", natsutil.RequestIDFromContext(c), "error", err)
			continue
		}
		// A partial result must not be cached (it would stamp the freshness
		// marker); answer from it directly instead.
		if degraded {
			resp.Counts[account] = cappedUnion(ids, req.RoomID, s.badgeCap)
			continue
		}
		if n, ok := s.badge.Seed(c, account, ids, req.RoomID); ok {
			resp.Counts[account] = n
			continue
		}
		// Cache down entirely: compute without it (ids ∪ trigger, capped).
		resp.Counts[account] = cappedUnion(ids, req.RoomID, s.badgeCap)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS. Update the three pre-existing `unreadRooms` call sites in `subscriptions_test.go:832/851/869` to the three-value form.

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add user-service/service/
git commit -m "fix(user-service): never cache a degraded unread computation

unreadRooms drops an unreachable site's rooms and only logged the failure, so
callers reseeded the short list and stamped the freshness marker — blessing a
knowingly-incomplete set as verified against Mongo.

It now reports degradation, and both callers skip the cache write while still
returning their best-effort count. Previously a room read wiped the bad set
seconds later; with reads no longer wiping it, one transient cross-site blip
would otherwise hold an undercount for a full marker window.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: Documentation

**Files:**
- Modify: `docs/client-api.md:5551`, `docs/notification-worker-downstream-contracts.md`, `docs/superpowers/specs/2026-08-10-badge-cache-invalidation-design.md`

**Interfaces:**
- Consumes: the behavior established in Tasks 1–5.
- Produces: nothing consumed by later tasks.

**Background:** no request/response schema changes, so the derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) are untouched — CLAUDE.md only requires updating those when the canonical schema changes. This is prose accuracy only.

- [ ] **Step 1: Update the `subscription.count` caching note**

Replace the **Caching:** paragraph at `docs/client-api.md:5551`:

```markdown
**Caching:** when the server-side badge cache is enabled, the unread count may be served from the account's maintained unread-room set instead of being recomputed. The set is bumped on every message, and a read removes exactly the room read (a room with unread followed threads stays counted); mute, unmute, thread-read and membership changes invalidate it. The set is re-derived from MongoDB whenever it has gone unverified for longer than the server's badge marker TTL, which is therefore the upper bound on how stale this count can be. Same response schema and staleness bounds as the badge counts carried on push notifications (this count is not capped for display, unlike push counts).
```

- [ ] **Step 2: Update the downstream contracts accuracy model**

In `docs/notification-worker-downstream-contracts.md`, find the badge accuracy section and state: the set is maintained on both edges (bump on arrival, exact removal on read); drift is bounded by the marker TTL rather than the set TTL; the failure direction is undercount-until-recompute; and a degraded cross-site computation is not cached at all. Keep the file's existing prose style and heading structure — read it first and match.

- [ ] **Step 3: Mark the superseded sections**

Add directly under the title of `docs/superpowers/specs/2026-08-10-badge-cache-invalidation-design.md`:

```markdown
> **Partially superseded** by `2026-08-19-badge-precise-read-invalidation-design.md`:
> the read rows of §4 (reads now remove exactly the room read rather than
> dropping the set) and the §7 accuracy model (drift is now bounded by the
> marker TTL). §4's ClearAll choice rested on the recompute being unconditional,
> which stopped being true when `BADGE_COUNT_CACHE_FIRST` was flipped to `true`.
```

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: describe the badge cache's read invalidation and staleness bound

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: Full verification

**Files:** none modified.

- [ ] **Step 1: Run the full unit suite**

Run: `make test`
Expected: PASS across all services.

- [ ] **Step 2: Run the affected integration suites**

Run: `make test-integration SERVICE=pkg/badgecache`, then the same for `room-service`, `inbox-worker`, `user-service`.
Expected: PASS. These need Docker.

- [ ] **Step 3: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: no medium+ findings. This is a blocking CI gate.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/taskbar-unread-count-perf-irdvyi
```

- [ ] **Step 6: Confirm the rollout order is recorded for whoever deploys**

The spec's §8 requires **user-service first**, then room-service and inbox-worker. New room-service with an old user-service would give precise `ClearRoom` while the old bump script still refreshes the marker — drift bounded only by the 24h set TTL, the combination the accuracy model rules out. Make sure this is stated in the PR description when one is opened.

---

## Notes for the implementer

- **Do not reinstate the guard the 2026-08-10 spec deleted.** Thread-read paths (`thread.read`, `thread.read.all`) keep `ClearAll` — their post-state is genuinely ambiguous, since clearing one thread says nothing about message-unread. Only the *room*-read path changes.
- **The `room.LastMsgAt <= lastSeenAt` guard was considered and rejected** (spec §4.1). `lastMsgAt` carries the message's own timestamp, so a message the user just read always compares as older than `now` whether or not broadcast-worker's 250 ms coalescer has flushed it. Adding it would duplicate a decision already made.
- **Race 2 is not closed by this work** (spec §6.1): `threadUnread` is written by message-worker while the badge bump comes from notification-worker, and nothing orders them. Reading post-update narrows the window; only single ownership of both writes would close it, which is out of scope.
