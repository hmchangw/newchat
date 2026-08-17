# Badge Cache Invalidation + Cache-First Count Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Valkey badge set a maintained materialization (ClearAll invalidation on reads/mutes, full-audience bumps) and let `subscription.count` serve from it behind an env gate.

**Architecture:** `pkg/badgecache` gains a same-slot freshness marker (`badge:fresh:{account}`) written by seed/reseed, honored by the bump script, deleted by ClearAll, and read by a new `Count` method. room-service and inbox-worker replace guarded read-path clears with ClearAll and add mute-toggle hooks. notification-worker widens the badge RPC audience from push survivors to everyone whose badge changed. user-service serves `SCARD` on marker hit behind `BADGE_COUNT_CACHE_FIRST` (default false).

**Tech Stack:** Go 1.25, go-redis v9 (Valkey cluster, Lua scripts), NATS, testify + mockgen, testcontainers.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-10-badge-cache-invalidation-design.md`.
- **Single commit:** run each task's test cycle (red → green) but do NOT commit per task; Task 6 makes the one commit and pushes.
- All commands through `make` targets (`make test SERVICE=…`, `make lint`, `make generate`, `make sast`, `make test-integration SERVICE=…`).
- TDD: write/adjust tests first per task, see them fail, then implement.
- `BADGE_COUNT_CACHE_FIRST` defaults **false** (rollout gate — see spec §6.1). `envDefault:"false"`.
- Marker semantics: marker exists ⇒ set is accurate; missing/empty set with live marker ⇒ zero unread. Marker and set share the same TTL and are always refreshed together; every path that degrades the set deletes the marker (`ClearAll`) — `ClearRoom` keeps it (removal is exact).
- `Count` returns the UNCAPPED set size (matches today's Mongo-computed count).

---

### Task 1: pkg/badgecache — marker key, two-key scripts, Count

**Files:**
- Modify: `pkg/badgecache/badgecache.go`
- Test: `pkg/badgecache/badgecache_test.go`, `pkg/badgecache/badgecache_integration_test.go`

**Interfaces:**
- Consumes: existing `Cache` (rdb, ttl, maxCount), `Key(account)`.
- Produces: `MarkerKey(account string) string` = `"badge:fresh:{" + account + "}"`; `func (c *Cache) Count(ctx context.Context, account string) (n int, fresh bool)`; all scripts two-key (`KEYS[1]`=set, `KEYS[2]`=marker); `ClearAll` deletes both keys. `BumpBatch`/`Seed`/`Reseed`/`ClearRoom` signatures unchanged.

- [ ] **Step 1: Write failing integration tests** (append to `badgecache_integration_test.go`; adjust existing tests only where marker behavior changes outcomes):

```go
// TestBadgeCache_Marker_FreshZeroAfterEmptyReseed: an empty reseed records
// "fresh, zero unread" — Count serves 0 without a recompute, and a bump on the
// marker-only state creates the set (count 1) instead of missing.
func TestBadgeCache_Marker_FreshZeroAfterEmptyReseed(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", nil)

	n, fresh := c.Count(ctx, "alice")
	require.True(t, fresh, "empty reseed must leave a fresh marker")
	assert.Equal(t, 0, n)

	counts := c.BumpBatch(ctx, []string{"alice"}, "roomA")
	assert.Equal(t, map[string]int{"alice": 1}, counts, "marker-only state must be a hit that creates the set")

	n, fresh = c.Count(ctx, "alice")
	require.True(t, fresh)
	assert.Equal(t, 1, n)
}

// TestBadgeCache_Count_StaleWithoutMarker: no marker → fresh=false regardless
// of set contents (legacy sets self-migrate via the caller's recompute).
func TestBadgeCache_Count_StaleWithoutMarker(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, fresh := c.Count(ctx, "bob")
	assert.False(t, fresh, "no state at all → stale")

	// Simulate a legacy set written without a marker.
	require.NoError(t, rdb.SAdd(ctx, Key("bob"), "roomA").Err())
	_, fresh = c.Count(ctx, "bob")
	assert.False(t, fresh, "set without marker → stale")
}

// TestBadgeCache_ClearAll_RemovesMarker / ClearRoom keeps it.
func TestBadgeCache_ClearSemantics_Marker(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "carol", []string{"roomA", "roomB"}, "")
	require.True(t, ok)

	c.ClearRoom(ctx, "carol", "roomA")
	n, fresh := c.Count(ctx, "carol")
	require.True(t, fresh, "ClearRoom is an exact removal — marker must survive")
	assert.Equal(t, 1, n)

	c.ClearAll(ctx, "carol")
	_, fresh = c.Count(ctx, "carol")
	assert.False(t, fresh, "ClearAll must delete the marker with the set")
}

// TestBadgeCache_Count_UncappedAboveMaxCount: Count reports the true set size.
func TestBadgeCache_Count_Uncapped(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	rooms := make([]string, 15)
	for i := range rooms {
		rooms[i] = fmt.Sprintf("room%02d", i)
	}
	_, ok := c.Seed(context.Background(), "dave", rooms, "")
	require.True(t, ok)
	n, fresh := c.Count(context.Background(), "dave")
	require.True(t, fresh)
	assert.Equal(t, 15, n, "Count is uncapped — the 10-cap applies only to Bump/Seed returns")
}
```

Also update `TestBadgeCache_ValkeyError_FailsOpen` to assert `Count` fails open:

```go
	_, fresh := c.Count(ctx, "frank")
	assert.False(t, fresh, "Valkey error → stale (caller recomputes)")
```

Marker key shape (unit test, `badgecache_test.go`):

```go
func TestMarkerKey_SameSlotAsKey(t *testing.T) {
	assert.Equal(t, "badge:fresh:{alice}", MarkerKey("alice"))
	// Same {…} hash tag content as Key → same cluster slot (scripts address both).
	assert.Contains(t, Key("alice"), "{alice}")
	assert.Contains(t, MarkerKey("alice"), "{alice}")
}
```

- [ ] **Step 2: Verify red:** `go vet -tags integration ./pkg/badgecache/` compiles? No — `MarkerKey`/`Count` undefined → compile failure is the red.
- [ ] **Step 3: Implement** in `badgecache.go`:

```go
// MarkerKey returns the freshness-marker key for account. Marker exists ⇒ the
// set is an accurate materialization (missing/empty set = zero unread). Same
// {account} hash tag as Key so scripts can address both keys in one slot.
func MarkerKey(account string) string {
	return "badge:fresh:{" + account + "}"
}
```

Scripts (all gain `KEYS[2]` = marker):

```go
var bumpScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 and redis.call('EXISTS', KEYS[2]) == 0 then
  return -1
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return redis.call('SCARD', KEYS[1])
`)

var seedScript = redis.NewScript(`
if #ARGV > 1 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 2))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[1])
return redis.call('SCARD', KEYS[1])
`)

var reseedScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if #ARGV > 1 then
  redis.call('SADD', KEYS[1], unpack(ARGV, 2))
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[2], '1', 'EX', ARGV[1])
return 1
`)
```

Call-site changes: every `bumpScript.EvalSha/Eval` and `seedScript.Run`/`reseedScript.Run` passes `[]string{Key(account), MarkerKey(account)}`. `ClearAll` deletes both (same slot, one DEL): `c.rdb.Del(ctx, Key(account), MarkerKey(account))`. `ClearRoom` unchanged. New method:

```go
// Count returns the account's unread-room count from the cache. fresh=false
// means the marker is absent or Valkey errored — the caller must recompute
// from Mongo (fail-open). fresh=true with n=0 is the legitimate all-read
// state. n is UNCAPPED (the display cap applies only to Bump/Seed returns).
func (c *Cache) Count(ctx context.Context, account string) (n int, fresh bool) {
	var existsCmd, scardCmd *redis.IntCmd
	if _, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		existsCmd = pipe.Exists(ctx, MarkerKey(account))
		scardCmd = pipe.SCard(ctx, Key(account))
		return nil
	}); err != nil {
		slog.WarnContext(ctx, "badgecache count failed", "account", account, "error", err)
		return 0, false
	}
	if existsCmd.Val() == 0 {
		return 0, false
	}
	return int(scardCmd.Val()), true
}
```

Update doc comments: package doc + `ClearAll` (deletes marker) + `Seed`/`Reseed` (write marker).

- [ ] **Step 4: Green:** `make test SERVICE=pkg/badgecache` and `make test-integration SERVICE=pkg/badgecache` → PASS.

---

### Task 2: room-service — ClearAll on reads, mute-toggle hooks

**Files:**
- Modify: `room-service/handler.go` (messageRead ~:1364-1373, thread-read ~:1674-1680, muteToggle ~:2190-2208)
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: room-service's badge interface (already has `ClearRoom(ctx, account, roomID)` + `ClearAll(ctx, account)`), `sub.Muted` post-toggle state, `userSiteID` from `GetUserSiteID`.
- Produces: no signature changes.

- [ ] **Step 1: Update tests first.** Locate the existing messageRead/thread-read badge tests (search `handler_test.go` for the fake badge double and `ClearRoom` assertions on read paths). Rewrite expectations: reads now call `ClearAll(account)` regardless of ThreadUnread state; add mute tests:

```go
// Reading the main room drops the whole badge set — no thread-state guard.
// (Adapt to the file's existing fake-badge + handler harness.)
func TestMessageRead_BadgeClearAll_EvenWithThreadUnread(t *testing.T) { /* sub has ThreadUnread: []string{"p1"}; expect exactly one ClearAll("alice"), zero ClearRoom */ }

func TestThreadRead_BadgeClearAll_EvenWhenThreadsRemain(t *testing.T) { /* $pull leaves ["p2"]; expect ClearAll, zero ClearRoom */ }

func TestMuteToggle_Muted_BadgeClearRoom(t *testing.T)   { /* toggle → Muted=true; expect ClearRoom("alice","r1"), zero ClearAll */ }
func TestMuteToggle_Unmuted_BadgeClearAll(t *testing.T)  { /* toggle → Muted=false; expect ClearAll("alice") */ }
func TestMuteToggle_CrossSiteUser_NoBadgeCall(t *testing.T) { /* userSiteID="site-b"; expect no badge calls (inbox-worker owns the replica) */ }
```

- [ ] **Step 2: Red:** `make test SERVICE=room-service` → new tests FAIL (old guarded behavior).
- [ ] **Step 3: Implement.** messageRead — replace the guarded block:

```go
	// Best-effort badge invalidation: reading the room decreases the account's
	// unread-room set, so drop the whole set (marker included) and let the next
	// count/push recompute from Mongo — no thread-state guard needed. Home-local
	// readers only; a cross-site reader's home replica is invalidated by
	// inbox-worker when the federated subscription_read lands.
	if userSiteID == h.siteID && h.badge != nil {
		h.badge.ClearAll(ctx, account)
	}
```

Thread-read — same replacement (drop the `len(newThreadUnread) == 0` guard, keep the home-local guard). muteToggle — after the `GetUserSiteID` call:

```go
	// Best-effort badge invalidation (home-local actor; inbox-worker handles
	// cross-site replicas via the federated event): muting is an exact removal
	// (set stays fresh); unmuting needs an unread check we don't do inline, so
	// drop the set and let the next count/push recompute.
	if userSiteID == h.siteID && h.badge != nil {
		if sub.Muted {
			h.badge.ClearRoom(ctx, account, roomID)
		} else {
			h.badge.ClearAll(ctx, account)
		}
	}
```

- [ ] **Step 4: Green:** `make test SERVICE=room-service` → PASS.

---

### Task 3: inbox-worker — ClearAll on federated reads, mute hooks, drop SubscriptionHasThreadUnread

**Files:**
- Modify: `inbox-worker/handler.go` (interface ~:43-49, handleSubscriptionRead ~:338-348, handleThreadRead ~:414-426, handleSubscriptionMuteToggled ~:353-362), `inbox-worker/main.go` (mongo impl of SubscriptionHasThreadUnread — delete)
- Test: `inbox-worker/handler_test.go`; regenerate `inbox-worker/mock_store_test.go`

**Interfaces:**
- Consumes: inbox-worker badge double (`ClearRoom`/`ClearAll`), `SubscriptionMuteToggledEvent.Muted`.
- Produces: `InboxStore` interface WITHOUT `SubscriptionHasThreadUnread`.

- [ ] **Step 1: Update tests first.** In `handler_test.go`: delete the `hasThreadUnreadErr` field + stub method `SubscriptionHasThreadUnread` + the two skip-on-error tests (~:1433, ~:1975); rewrite subscription_read / thread_read badge assertions to expect `ClearAll(account)` (no post-state Mongo check); add:

```go
func TestHandleSubscriptionMuteToggled_Muted_BadgeClearRoom(t *testing.T)  { /* evt Muted=true → ClearRoom(account, roomID) */ }
func TestHandleSubscriptionMuteToggled_Unmuted_BadgeClearAll(t *testing.T) { /* evt Muted=false → ClearAll(account) */ }
```

- [ ] **Step 2: Red:** `make test SERVICE=inbox-worker` → compile/behavior failures.
- [ ] **Step 3: Implement.** handleSubscriptionRead / handleThreadRead — replace each post-state block with:

```go
	// Best-effort badge invalidation: a read decreases the unread-room set —
	// drop it wholesale (marker included); the next count/push recomputes.
	if h.badge != nil {
		h.badge.ClearAll(ctx, e.Account)
	}
```

handleSubscriptionMuteToggled — after the store update:

```go
	// Best-effort badge invalidation on the home replica: mute is an exact
	// removal; unmute recomputes (the room re-enters iff unread).
	if h.badge != nil {
		if e.Muted {
			h.badge.ClearRoom(ctx, e.Account, e.RoomID)
		} else {
			h.badge.ClearAll(ctx, e.Account)
		}
	}
```

Delete `SubscriptionHasThreadUnread` from the `InboxStore` interface and its mongo implementation; `make generate SERVICE=inbox-worker`.

- [ ] **Step 4: Green:** `make test SERVICE=inbox-worker` → PASS.

---

### Task 4: notification-worker — badge audience ≠ push survivors

**Files:**
- Modify: `notification-worker/handler.go` (candidate loop ~:150-219, `fetchUnreadCounts` doc)
- Test: `notification-worker/handler_test.go`

**Interfaces:**
- Consumes: existing member pipeline, `h.fetchUnreadCounts(ctx, roomID, accounts, siteByAccount)`.
- Produces: badge RPC audience = all members past the sender/muted/restricted/thread-scope filters; push payload accounts unchanged (survivors only).

- [ ] **Step 1: Update tests first.** Find the handler tests asserting the badge RPC's account list (fake BadgeClient). Add/adjust:

```go
// A hook-vetoed / push-ineligible / online member is still in the badge RPC
// audience (their unread state changed) but absent from the push payload.
func TestHandle_BadgeAudienceWiderThanSurvivors(t *testing.T) { /* member online (presence excludes from push): badge client sees them; Emitter payload accounts do not */ }

// Muted and restricted members are excluded from BOTH.
func TestHandle_BadgeAudienceExcludesMutedRestricted(t *testing.T) { /* ... */ }

// Badge RPC still fires when zero push survivors remain (all online).
func TestHandle_BadgeBumpWithoutPush(t *testing.T) { /* all members online → no Emit, but badge client called once */ }
```

- [ ] **Step 2: Red:** `make test SERVICE=notification-worker` → FAIL.
- [ ] **Step 3: Implement.** Restructure the loop: append to `badgeAccounts` + `siteByAccount` right after the thread-scope check (before hook/eligibility); keep `candidates`/`accounts` for the push filters. After the loop:

```go
	if len(badgeAccounts) == 0 {
		return nil
	}
	snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // push candidates only
	survivors := ... // unchanged filter over candidates via shouldPush
	sort.Strings(survivors)

	// Badge phase covers the full badge audience — bumps must land even for
	// members who won't be pushed (hook-vetoed, ineligible, online), or their
	// cached counts go stale between reads. Reply counts for non-survivors are
	// simply unused.
	unreadCounts := h.fetchUnreadCounts(ctx, msg.RoomID, badgeAccounts, siteByAccount)
	if len(survivors) == 0 {
		return nil
	}
```

(`filterUnreadCounts` already narrows each batch's counts to its accounts.) Update `fetchUnreadCounts`'s doc comment: takes the badge audience, not survivors.

- [ ] **Step 4: Green:** `make test SERVICE=notification-worker` → PASS.

---

### Task 5: user-service — gate + cache-first count + docs

**Files:**
- Modify: `user-service/config/config.go`, `user-service/main.go` (mirror interface + noop), `user-service/service/service.go` (badgeCache interface, UserService field, New), `user-service/service/subscriptions.go` (CountSubscriptions), `docs/notification-worker-downstream-contracts.md`, `docs/client-api.md` (count prose only)
- Test: `user-service/config/config_test.go`, `user-service/service/subscriptions_test.go`, `user-service/service/badge_test.go` (fake)

**Interfaces:**
- Consumes: Task 1's `Count(ctx, account) (int, bool)` on `*badgecache.Cache`.
- Produces: `badgeCache` interface (service.go AND main.go mirror) += `Count(ctx context.Context, account string) (int, bool)`; `noopBadgeCache.Count` returns `(0, false)`; `Config.BadgeCountCacheFirst bool `env:"BADGE_COUNT_CACHE_FIRST" envDefault:"false"``; `UserService.badgeCacheFirst` from cfg.

- [ ] **Step 1: Tests first.**

`config_test.go`:

```go
func TestLoad_BadgeCountCacheFirst(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "BADGE_COUNT_CACHE_FIRST")
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.BadgeCountCacheFirst, "cache-first count must be opt-in (rollout gate)")

	t.Setenv("BADGE_COUNT_CACHE_FIRST", "true")
	cfg, err = Load()
	require.NoError(t, err)
	require.True(t, cfg.BadgeCountCacheFirst)
}
```

`badge_test.go` fake: add `count func(account string) (int, bool)` field, `countCalls []string`, and:

```go
func (f *fakeBadgeCache) Count(_ context.Context, account string) (int, bool) {
	f.countCalls = append(f.countCalls, account)
	if f.count != nil {
		return f.count(account)
	}
	return 0, false
}
```

`subscriptions_test.go`:

```go
// Gate off (default): Count is never consulted; the Mongo path runs.
func TestCountUnread_GateOff_NoCacheRead(t *testing.T) { /* svc from newSvc (gate off); GetActiveSubscriptions expected; assert badge.countCalls empty */ }

// Gate on + fresh: served from the cache — no repo call, no reseed.
func TestCountUnread_CacheFirst_Fresh(t *testing.T) {
	svc, subs, _ := newSvcRaw(t) // adapt to file's helpers
	svc.badgeCacheFirst = true
	badge := svc.badge.(*fakeBadgeCache)
	badge.count = func(string) (int, bool) { return 7, true }
	_ = subs // no GetActiveSubscriptions expectation — a call fails the mock
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 7, resp.Count)
	assert.Empty(t, badge.reseedCalls, "cache hit must not reseed")
}

// Gate on + fresh zero: 0 is a legitimate served answer.
func TestCountUnread_CacheFirst_FreshZero(t *testing.T) { /* count → (0,true); expect Count 0, no repo call */ }

// Gate on + stale: falls back to compute + reseed (today's path).
func TestCountUnread_CacheFirst_Stale_Computes(t *testing.T) { /* count → (0,false); GetActiveSubscriptions expected; reseedCalls == ["alice"] */ }
```

- [ ] **Step 2: Red:** `make test SERVICE=user-service` → compile failures (Count missing on fake/interface).
- [ ] **Step 3: Implement.**

`config.go` (next to BadgeCountCap):

```go
	// BadgeCountCacheFirst serves subscription.count (unread=true) from the
	// Valkey badge set on freshness-marker hit. Rollout gate: flip to true only
	// after ALL badge writers (room-service, inbox-worker, user-service,
	// notification-worker) run the marker-aware pkg/badgecache — an old writer's
	// set-only ClearAll would leave a stale marker reading as "fresh zero".
	BadgeCountCacheFirst bool `env:"BADGE_COUNT_CACHE_FIRST" envDefault:"false"`
```

`service.go`: interface += `Count(ctx context.Context, account string) (int, bool)` (doc: "fresh=false ⇒ recompute"); struct field `badgeCacheFirst bool`; `New` sets `badgeCacheFirst: cfg.BadgeCountCacheFirst`. `main.go`: mirror interface += Count; `func (noopBadgeCache) Count(context.Context, string) (int, bool) { return 0, false }`.

`subscriptions.go` CountSubscriptions, unread path:

```go
	// Cache-first (gated): serve the badge set's size on freshness-marker hit —
	// reads/mutes invalidate it and every message bumps it, so a hit is current.
	// Miss/stale falls through to the Mongo compute, which reseeds (and writes
	// the marker) below.
	if s.badgeCacheFirst {
		if n, fresh := s.badge.Count(c, account); fresh {
			return &models.CountResponse{Count: n}, nil
		}
	}
	ids, err := s.unreadRooms(c, account)
	...
```

Docs: `docs/notification-worker-downstream-contracts.md` — update the badge accuracy-model paragraph (set now maintained on reads/mutes via ClearAll/ClearRoom, bumps cover the full badge audience, count may be served from the set behind `BADGE_COUNT_CACHE_FIRST`, flip order per spec §6.1). `docs/client-api.md` — in the `subscription.count` section prose, note the unread count may be served from the badge cache (same accuracy envelope; no schema change).

- [ ] **Step 4: Green:** `make test SERVICE=user-service` → PASS.

---

### Task 6: Gates, single commit, push

- [ ] **Step 1:** `make generate` (inbox-worker mock changed) — confirm no unexpected diffs beyond inbox-worker.
- [ ] **Step 2:** `make lint` → 0 issues; `make fmt` first if needed.
- [ ] **Step 3:** `make test` (full unit, race) → all pass.
- [ ] **Step 4:** Integration for touched services: `make test-integration SERVICE=pkg/badgecache SERVICE=…` (run each: pkg/badgecache, room-service, inbox-worker, user-service, notification-worker if it has integration tests) → all pass. Ensure Docker daemon is up first.
- [ ] **Step 5:** `make sast` → PASS.
- [ ] **Step 6:** Single commit of ALL changes with trailers, then `git push -u origin claude/threadunread-tracking-analysis-wu3g77`:

```bash
git add -A
git commit -m "feat(badge): ClearAll invalidation, full-audience bumps, cache-first count

pkg/badgecache gains a badge:fresh:{account} marker (same hash slot):
seed/reseed write it (empty reseed = fresh zero), the bump script treats
marker-only state as a hit, ClearAll deletes both keys, ClearRoom keeps
the marker, and Count(ctx, account) serves the uncapped SCARD on marker
hit. room-service and inbox-worker replace the guarded read-path clears
with ClearAll (dropping SubscriptionHasThreadUnread) and invalidate on
mute toggles (mute: exact ClearRoom; unmute: ClearAll → recompute).
notification-worker bumps the full badge audience, not just push
survivors, so online users' sets stay current. user-service serves
subscription.count (unread=true) from the cache behind
BADGE_COUNT_CACHE_FIRST (default false — flip only after all writers run
the marker-aware badgecache).

Spec: docs/superpowers/specs/2026-08-10-badge-cache-invalidation-design.md"
```

---

## Self-Review

- **Spec coverage:** §3 scripts/marker/Count → Task 1; §4 table rows → Tasks 2-3 (message.read, thread read, read.all unchanged, mute both sides, member_removed unchanged); §5 → Task 4; §6 + gate → Task 5; §8 testing → per-task test steps; §6.1 rollout → config comment + docs step.
- **Type consistency:** `Count(ctx context.Context, account string) (int, bool)` identical in Task 1 (impl), Task 5 (interface, noop, fake). `MarkerKey` used only in Task 1. `ClearRoom(ctx, account, roomID)` / `ClearAll(ctx, account)` match existing signatures.
- **Placeholders:** test skeletons in Tasks 2-4 name exact expectations but defer harness details to the file's existing doubles — acceptable since the harnesses exist; no TBDs.
