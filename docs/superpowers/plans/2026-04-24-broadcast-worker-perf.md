# Broadcast Worker Performance Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut `broadcast-worker`'s per-message Mongo round-trips from three to one (in the common cache-hit case) by adding an in-process LRU+TTL sender cache and by merging `GetRoom`+`UpdateRoomOnNewMessage` into a single `FindOneAndUpdate`.

**Architecture:** Add `CachedUserStore` as a `userstore.UserStore` decorator, local to `broadcast-worker`. Add `FetchAndUpdateRoom(ctx, roomID, msgID, msgAt, mentionAll) (*Room, error)` to the `Store` interface backed by `FindOneAndUpdate` with `ReturnDocument=After`; remove `UpdateRoomOnNewMessage`; retain `GetRoom`. Wire two new env vars in `main.go`: `USER_CACHE_SIZE` (default 10000) and `USER_CACHE_TTL` (default 5m).

**Tech Stack:** Go 1.25, `container/list`, `sync.Mutex`, `go.mongodb.org/mongo-driver/v2` (`FindOneAndUpdate`), `go.uber.org/mock` (mockgen), `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-04-24-broadcast-worker-perf-design.md`.

---

## File Structure

### New files

| File | Responsibility |
|---|---|
| `broadcast-worker/usercache.go` | `CachedUserStore` — LRU+TTL decorator implementing `userstore.UserStore`. |
| `broadcast-worker/usercache_test.go` | Unit tests for the cache (hit/miss/partial/TTL/LRU/concurrency). |

### Modified files

| File | Change |
|---|---|
| `broadcast-worker/store.go` | Add `FetchAndUpdateRoom` to `Store` interface; remove `UpdateRoomOnNewMessage`. |
| `broadcast-worker/store_mongo.go` | Implement `FetchAndUpdateRoom` via `FindOneAndUpdate`; remove `UpdateRoomOnNewMessage`. |
| `broadcast-worker/handler.go` | Replace `GetRoom`+`UpdateRoomOnNewMessage` with `FetchAndUpdateRoom`. |
| `broadcast-worker/handler_test.go` | Swap mock expectations to match the new store method. Add a missing-room case. |
| `broadcast-worker/main.go` | Parse `USER_CACHE_SIZE`/`USER_CACHE_TTL`, wrap `userstore` with `CachedUserStore` when size > 0. |
| `broadcast-worker/mock_store_test.go` | Regenerated via `make generate SERVICE=broadcast-worker`. |

### Unchanged

- `broadcast-worker/integration_test.go` — exercises the full path against testcontainers; no changes required, but must continue to pass.
- `pkg/userstore/userstore.go` — no API changes.
- All other services.

---

## Task 1: Scaffold `CachedUserStore` type with constructor (failing unit tests)

**Files:**
- Create: `broadcast-worker/usercache_test.go`
- Create: `broadcast-worker/usercache.go`

- [ ] **Step 1: Write failing tests**

Create `broadcast-worker/usercache_test.go`:

```go
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/userstore"
)

// fakeUserStore is a minimal userstore.UserStore that records calls and
// returns preconfigured users. Tests assert call counts to confirm cache
// hits do not reach the inner store.
type fakeUserStore struct {
	mu        sync.Mutex
	calls     [][]string
	byAccount map[string]model.User
	err       error
}

func newFakeUserStore(users ...model.User) *fakeUserStore {
	f := &fakeUserStore{byAccount: make(map[string]model.User, len(users))}
	for _, u := range users {
		f.byAccount[u.Account] = u
	}
	return f
}

func (f *fakeUserStore) FindUserByID(_ context.Context, _ string) (*model.User, error) {
	return nil, errors.New("unused in these tests")
}

func (f *fakeUserStore) FindUsersByAccounts(_ context.Context, accounts []string) ([]model.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]string(nil), accounts...))
	if f.err != nil {
		return nil, f.err
	}
	out := make([]model.User, 0, len(accounts))
	for _, a := range accounts {
		if u, ok := f.byAccount[a]; ok {
			out = append(out, u)
		}
	}
	return out, nil
}

func (f *fakeUserStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeUserStore) lastCall() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

// sentinel to prove the returned type implements the userstore.UserStore interface.
var _ userstore.UserStore = (*CachedUserStore)(nil)

func TestNewCachedUserStore_ConstructsEmpty(t *testing.T) {
	inner := newFakeUserStore()
	c := NewCachedUserStore(inner, 10, time.Minute)
	require.NotNil(t, c)
	// A fresh cache doesn't call inner until asked.
	assert.Equal(t, 0, inner.callCount())
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/user/chat && go test ./broadcast-worker/ -run TestNewCachedUserStore -v`
Expected: FAIL — `NewCachedUserStore` and `CachedUserStore` undefined.

- [ ] **Step 3: Write the minimal implementation**

Create `broadcast-worker/usercache.go`:

```go
package main

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/userstore"
)

// userCacheEntry is the value stored in each LRU list element.
type userCacheEntry struct {
	account  string
	user     model.User
	inserted time.Time
}

// CachedUserStore wraps a userstore.UserStore with an in-process LRU+TTL
// cache of FindUsersByAccounts results. FindUserByID delegates to the
// inner store unchanged.
type CachedUserStore struct {
	inner   userstore.UserStore
	ttl     time.Duration
	maxSize int

	mu    sync.Mutex
	lru   *list.List // elements hold *userCacheEntry; front = MRU, back = LRU
	index map[string]*list.Element
	now   func() time.Time
}

// NewCachedUserStore returns a cache wrapping inner. maxSize > 0 and ttl > 0
// are required; the main.go wiring guards against zero values.
func NewCachedUserStore(inner userstore.UserStore, maxSize int, ttl time.Duration) *CachedUserStore {
	return &CachedUserStore{
		inner:   inner,
		ttl:     ttl,
		maxSize: maxSize,
		lru:     list.New(),
		index:   make(map[string]*list.Element, maxSize),
		now:     time.Now,
	}
}

// FindUserByID delegates; no caching for single-ID lookups.
func (c *CachedUserStore) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	return c.inner.FindUserByID(ctx, id)
}

// FindUsersByAccounts will be implemented in Task 2.
func (c *CachedUserStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	return c.inner.FindUsersByAccounts(ctx, accounts)
}
```

- [ ] **Step 4: Run to confirm tests pass**

Run: `cd /home/user/chat && go test ./broadcast-worker/ -run TestNewCachedUserStore -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add broadcast-worker/usercache.go broadcast-worker/usercache_test.go
git commit -m "feat(broadcast-worker): scaffold CachedUserStore + fake test helper"
```

---

## Task 2: Implement cache hit + miss in `FindUsersByAccounts`

**Files:**
- Modify: `broadcast-worker/usercache.go`
- Modify: `broadcast-worker/usercache_test.go`

- [ ] **Step 1: Add failing tests**

Append to `broadcast-worker/usercache_test.go` (inside the existing package, add these functions):

```go
func TestCachedUserStore_MissCallsInner(t *testing.T) {
	alice := model.User{ID: "u1", Account: "alice", EngName: "Alice"}
	inner := newFakeUserStore(alice)
	c := NewCachedUserStore(inner, 10, time.Minute)

	users, err := c.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, alice, users[0])
	assert.Equal(t, 1, inner.callCount(), "miss should call inner")
	assert.Equal(t, []string{"alice"}, inner.lastCall())
}

func TestCachedUserStore_HitServedFromCache(t *testing.T) {
	alice := model.User{ID: "u1", Account: "alice", EngName: "Alice"}
	inner := newFakeUserStore(alice)
	c := NewCachedUserStore(inner, 10, time.Minute)

	_, _ = c.FindUsersByAccounts(context.Background(), []string{"alice"}) // prime
	users, err := c.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, alice, users[0])
	assert.Equal(t, 1, inner.callCount(), "hit should not call inner")
}

func TestCachedUserStore_PartialHitCallsInnerWithOnlyMissing(t *testing.T) {
	alice := model.User{ID: "u1", Account: "alice"}
	bob := model.User{ID: "u2", Account: "bob"}
	inner := newFakeUserStore(alice, bob)
	c := NewCachedUserStore(inner, 10, time.Minute)

	_, _ = c.FindUsersByAccounts(context.Background(), []string{"alice"}) // prime alice only

	users, err := c.FindUsersByAccounts(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, 2, inner.callCount(), "partial hit still calls inner for misses")
	assert.Equal(t, []string{"bob"}, inner.lastCall(), "inner called only with missing accounts")
}

func TestCachedUserStore_EmptyInputReturnsNil(t *testing.T) {
	inner := newFakeUserStore()
	c := NewCachedUserStore(inner, 10, time.Minute)

	users, err := c.FindUsersByAccounts(context.Background(), nil)
	require.NoError(t, err)
	assert.Nil(t, users)
	assert.Equal(t, 0, inner.callCount())
}

func TestCachedUserStore_MissingUserNotCached(t *testing.T) {
	inner := newFakeUserStore() // no users registered
	c := NewCachedUserStore(inner, 10, time.Minute)

	// First call: inner returns no users for "ghost".
	users, err := c.FindUsersByAccounts(context.Background(), []string{"ghost"})
	require.NoError(t, err)
	assert.Empty(t, users)

	// Add ghost later to simulate the user being created.
	inner.byAccount["ghost"] = model.User{ID: "u-ghost", Account: "ghost"}

	// Second call: the negative result must NOT be cached — inner must be called again.
	users2, err := c.FindUsersByAccounts(context.Background(), []string{"ghost"})
	require.NoError(t, err)
	require.Len(t, users2, 1)
	assert.Equal(t, 2, inner.callCount(), "missing accounts must not be cached as negatives")
}

func TestCachedUserStore_InnerErrorPropagated(t *testing.T) {
	inner := newFakeUserStore()
	inner.err = errors.New("boom")
	c := NewCachedUserStore(inner, 10, time.Minute)

	_, err := c.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /home/user/chat && go test ./broadcast-worker/ -run TestCachedUserStore -v`
Expected: FAIL — `TestCachedUserStore_PartialHitCallsInnerWithOnlyMissing` expects only `["bob"]` in the last call, but the stub delegates everything and sees `["alice", "bob"]`. Several other tests fail similarly.

- [ ] **Step 3: Replace the `FindUsersByAccounts` method in `broadcast-worker/usercache.go`**

Replace the stub body with the real implementation:

```go
// FindUsersByAccounts returns users for the requested accounts, serving
// cache hits without calling the inner store. Cache misses are forwarded
// in a single batched inner call. Missing users are not cached as
// negatives — an account the inner store didn't return is simply absent
// and will be re-fetched next time.
func (c *CachedUserStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}

	now := c.now()

	c.mu.Lock()
	hits := make([]model.User, 0, len(accounts))
	missing := make([]string, 0, len(accounts))
	for _, account := range accounts {
		elem, ok := c.index[account]
		if !ok {
			missing = append(missing, account)
			continue
		}
		entry := elem.Value.(*userCacheEntry)
		if now.Sub(entry.inserted) >= c.ttl {
			// Stale; treat as miss. Drop entry now so a concurrent writer
			// doesn't collide; the inner result (or its absence) will
			// repopulate below.
			c.lru.Remove(elem)
			delete(c.index, account)
			missing = append(missing, account)
			continue
		}
		c.lru.MoveToFront(elem)
		hits = append(hits, entry.user)
	}
	c.mu.Unlock()

	if len(missing) == 0 {
		return hits, nil
	}

	fresh, err := c.inner.FindUsersByAccounts(ctx, missing)
	if err != nil {
		// Return partial hits plus the error so callers can log and continue.
		return hits, err
	}

	c.mu.Lock()
	for i := range fresh {
		u := fresh[i]
		if existing, ok := c.index[u.Account]; ok {
			// Concurrent race: another goroutine populated the same account.
			// Refresh in place and move to front.
			existing.Value = &userCacheEntry{account: u.Account, user: u, inserted: now}
			c.lru.MoveToFront(existing)
			continue
		}
		entry := &userCacheEntry{account: u.Account, user: u, inserted: now}
		elem := c.lru.PushFront(entry)
		c.index[u.Account] = elem
		if c.lru.Len() > c.maxSize {
			lruElem := c.lru.Back()
			if lruElem != nil {
				lruEntry := lruElem.Value.(*userCacheEntry)
				c.lru.Remove(lruElem)
				delete(c.index, lruEntry.account)
			}
		}
	}
	c.mu.Unlock()

	return append(hits, fresh...), nil
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cd /home/user/chat && go test -race ./broadcast-worker/ -run TestCachedUserStore -v`
Expected: PASS for all six subtests (plus `TestNewCachedUserStore_ConstructsEmpty` from Task 1).

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add broadcast-worker/usercache.go broadcast-worker/usercache_test.go
git commit -m "feat(broadcast-worker): implement CachedUserStore hit/miss/partial"
```

---

## Task 3: TTL expiration + LRU eviction with injected clock

**Files:**
- Modify: `broadcast-worker/usercache_test.go`
- Modify: `broadcast-worker/usercache.go` (if needed — most likely already works via Task 2's implementation; this task adds the tests)

- [ ] **Step 1: Add failing tests**

Append to `broadcast-worker/usercache_test.go`:

```go
func TestCachedUserStore_TTLExpiredReFetches(t *testing.T) {
	alice := model.User{ID: "u1", Account: "alice"}
	inner := newFakeUserStore(alice)
	c := NewCachedUserStore(inner, 10, 1*time.Second)

	// Freeze "now" at a known value.
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	_, err := c.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, 1, inner.callCount())

	// Advance past TTL.
	c.now = func() time.Time { return base.Add(2 * time.Second) }

	_, err = c.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, 2, inner.callCount(), "stale entry should force re-fetch")
}

func TestCachedUserStore_LRUEvictionOnOverflow(t *testing.T) {
	// maxSize=2: inserting 3 distinct accounts must evict the oldest.
	a := model.User{ID: "u1", Account: "alice"}
	b := model.User{ID: "u2", Account: "bob"}
	c := model.User{ID: "u3", Account: "carol"}
	inner := newFakeUserStore(a, b, c)
	store := NewCachedUserStore(inner, 2, time.Minute)

	ctx := context.Background()
	_, _ = store.FindUsersByAccounts(ctx, []string{"alice"})
	_, _ = store.FindUsersByAccounts(ctx, []string{"bob"})
	_, _ = store.FindUsersByAccounts(ctx, []string{"carol"}) // should evict alice
	// alice should now be a miss again.
	_, _ = store.FindUsersByAccounts(ctx, []string{"alice"})

	// Inner calls: alice, bob, carol, alice → 4 total
	assert.Equal(t, 4, inner.callCount(), "alice must be re-fetched after eviction")
}

func TestCachedUserStore_AccessPromotesToMRU(t *testing.T) {
	// maxSize=2: after priming alice + bob, accessing alice makes bob the
	// LRU. Inserting carol should evict bob, not alice.
	a := model.User{ID: "u1", Account: "alice"}
	b := model.User{ID: "u2", Account: "bob"}
	c := model.User{ID: "u3", Account: "carol"}
	inner := newFakeUserStore(a, b, c)
	store := NewCachedUserStore(inner, 2, time.Minute)

	ctx := context.Background()
	_, _ = store.FindUsersByAccounts(ctx, []string{"alice"})
	_, _ = store.FindUsersByAccounts(ctx, []string{"bob"})
	// Touch alice to mark MRU.
	_, _ = store.FindUsersByAccounts(ctx, []string{"alice"})
	// Insert carol: bob should be evicted.
	_, _ = store.FindUsersByAccounts(ctx, []string{"carol"})

	before := inner.callCount()
	// alice is still cached.
	_, _ = store.FindUsersByAccounts(ctx, []string{"alice"})
	assert.Equal(t, before, inner.callCount(), "alice should still be cached")
	// bob is not.
	_, _ = store.FindUsersByAccounts(ctx, []string{"bob"})
	assert.Equal(t, before+1, inner.callCount(), "bob should have been evicted")
}
```

- [ ] **Step 2: Run to verify tests pass**

Run: `cd /home/user/chat && go test -race ./broadcast-worker/ -run TestCachedUserStore -v`
Expected: PASS for all nine subtests. The Task 2 implementation already supports TTL and LRU correctly; these tests simply codify the guarantees.

If any test fails, double-check the Task 2 code — especially the `now.Sub(entry.inserted) >= c.ttl` check and the `c.lru.Remove(lruElem)` eviction path.

- [ ] **Step 3: Commit**

```bash
cd /home/user/chat
git add broadcast-worker/usercache_test.go
git commit -m "test(broadcast-worker): cover CachedUserStore TTL expiry + LRU eviction"
```

---

## Task 4: Concurrency safety under `-race`

**Files:**
- Modify: `broadcast-worker/usercache_test.go`

- [ ] **Step 1: Add a concurrent-access test**

Append to `broadcast-worker/usercache_test.go`:

```go
func TestCachedUserStore_ConcurrentSafe(t *testing.T) {
	// Many goroutines hit overlapping account sets. No race, no panic.
	const (
		goroutines = 32
		iterations = 200
		accounts   = 50
	)
	users := make([]model.User, accounts)
	for i := range users {
		users[i] = model.User{
			ID:      "u-" + strconvItoa(i),
			Account: "acct-" + strconvItoa(i),
		}
	}
	inner := newFakeUserStore(users...)
	store := NewCachedUserStore(inner, 32, time.Minute)

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				idx := (seed*iterations + i) % accounts
				_, err := store.FindUsersByAccounts(ctx, []string{"acct-" + strconvItoa(idx)})
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()
}

// strconvItoa avoids adding a strconv import to the test file purely for
// a test-helper numeric conversion. Kept tiny so it stays self-evident.
func strconvItoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
```

- [ ] **Step 2: Run under `-race`**

Run: `cd /home/user/chat && go test -race ./broadcast-worker/ -run TestCachedUserStore_ConcurrentSafe -v`
Expected: PASS with no race-detector warnings.

If the race detector flags anything, the Task 2 `FindUsersByAccounts` is holding the mutex incorrectly — revisit the lock/unlock boundaries and ensure `c.inner.FindUsersByAccounts` is called outside the mutex.

- [ ] **Step 3: Commit**

```bash
cd /home/user/chat
git add broadcast-worker/usercache_test.go
git commit -m "test(broadcast-worker): stress CachedUserStore under -race"
```

---

## Task 5: Add `FetchAndUpdateRoom` to the `Store` interface

**Files:**
- Modify: `broadcast-worker/store.go`
- Modify: `broadcast-worker/store_mongo.go`
- Modify: `broadcast-worker/mock_store_test.go` (regenerated)

- [ ] **Step 1: Edit `broadcast-worker/store.go`**

Replace the interface with:

```go
package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store
//go:generate mockgen -destination=mock_userstore_test.go -package=main github.com/hmchangw/chat/pkg/userstore UserStore
//go:generate mockgen -destination=mock_keystore_test.go -package=main . RoomKeyProvider

// Store defines data access operations for the broadcast worker.
type Store interface {
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
	ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error)
	FetchAndUpdateRoom(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool) (*model.Room, error)
	SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string) error
}
```

(Net change: `UpdateRoomOnNewMessage` replaced with `FetchAndUpdateRoom`; `GetRoom` retained.)

- [ ] **Step 2: Edit `broadcast-worker/store_mongo.go`**

Replace the `UpdateRoomOnNewMessage` method with `FetchAndUpdateRoom`. The full replacement:

```go
// remove:
// func (m *mongoStore) UpdateRoomOnNewMessage(...) error { ... }

// add (place where UpdateRoomOnNewMessage used to be):
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

You'll need `"go.mongodb.org/mongo-driver/v2/mongo/options"` in the imports if it isn't already there. Check the existing imports and add it to the block if missing.

- [ ] **Step 3: Regenerate the mock**

Run: `cd /home/user/chat && make generate SERVICE=broadcast-worker`
Expected: succeeds silently. `broadcast-worker/mock_store_test.go` now declares a `FetchAndUpdateRoom` method on the mock and no longer declares `UpdateRoomOnNewMessage`.

Verify by grep:
```bash
cd /home/user/chat && grep -c 'FetchAndUpdateRoom\|UpdateRoomOnNewMessage' broadcast-worker/mock_store_test.go
```
Expected: a positive number. If `UpdateRoomOnNewMessage` still appears anywhere in the mock, regeneration didn't take — try `make generate` again, or delete the mock file first then regenerate.

- [ ] **Step 4: Confirm `broadcast-worker/handler.go` no longer compiles (since we haven't updated it yet)**

Run: `cd /home/user/chat && go build ./broadcast-worker/`
Expected: FAIL with messages like "store.UpdateRoomOnNewMessage undefined" — this is the expected state. The next task updates the handler.

- [ ] **Step 5: Commit (intermediate; tree is known-broken until Task 6)**

```bash
cd /home/user/chat
git add broadcast-worker/store.go broadcast-worker/store_mongo.go broadcast-worker/mock_store_test.go
git commit -m "refactor(broadcast-worker): add FetchAndUpdateRoom; drop UpdateRoomOnNewMessage"
```

Note: the tree is temporarily broken between Task 5 and Task 6 because `handler.go` still references the removed method. Task 6 completes the refactor.

---

## Task 6: Update `handler.go` to use `FetchAndUpdateRoom`

**Files:**
- Modify: `broadcast-worker/handler.go`
- Modify: `broadcast-worker/handler_test.go`

- [ ] **Step 1: Update `broadcast-worker/handler.go`**

In `HandleMessage`, find this block:

```go
room, err := h.store.GetRoom(ctx, msg.RoomID)
if err != nil {
    return fmt.Errorf("get room %s: %w", msg.RoomID, err)
}

resolved, err := mention.Resolve(ctx, msg.Content, h.userStore.FindUsersByAccounts)
if err != nil {
    slog.Warn("mention resolve failed", "error", err)
}

if err := h.store.UpdateRoomOnNewMessage(ctx, room.ID, msg.ID, msg.CreatedAt, resolved.MentionAll); err != nil {
    return fmt.Errorf("update room on new message: %w", err)
}
```

Replace with:

```go
resolved, err := mention.Resolve(ctx, msg.Content, h.userStore.FindUsersByAccounts)
if err != nil {
    slog.Warn("mention resolve failed", "error", err)
}

room, err := h.store.FetchAndUpdateRoom(ctx, msg.RoomID, msg.ID, msg.CreatedAt, resolved.MentionAll)
if err != nil {
    return fmt.Errorf("fetch and update room %s: %w", msg.RoomID, err)
}
```

Note the order change: `mention.Resolve` now runs BEFORE the Mongo call because `FetchAndUpdateRoom` needs `resolved.MentionAll` as an argument. This is safe — `mention.Resolve` has no dependency on room state.

- [ ] **Step 2: Confirm it compiles**

Run: `cd /home/user/chat && go build ./broadcast-worker/`
Expected: succeeds.

- [ ] **Step 3: Update `broadcast-worker/handler_test.go`**

Every existing test that set up mock expectations on `GetRoom` + `UpdateRoomOnNewMessage` must be updated. Open the file and find calls matching:

```go
store.EXPECT().GetRoom(...).Return(...)
// ...
store.EXPECT().UpdateRoomOnNewMessage(...).Return(...)
```

Replace each PAIR with a single:

```go
store.EXPECT().FetchAndUpdateRoom(gomock.Any(), <roomID>, <msgID>, gomock.Any(), <mentionAll>).Return(<room>, nil)
```

Use `gomock.Any()` for the `time.Time` argument since tests don't care about exact timestamps (they already do this pattern for other args).

The specific test cases that need to be updated are those that call `HandleMessage`. Read through the file and apply the substitution to each.

- [ ] **Step 4: Add a new test for the missing-room case**

Append to `broadcast-worker/handler_test.go`:

```go
func TestHandler_FetchAndUpdateRoom_Missing(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &stubPublisher{}
	h := NewHandler(store, us, pub)

	evt := model.MessageEvent{
		Message: model.Message{
			ID:          "m-1",
			RoomID:      "ghost-room",
			UserAccount: "alice",
			Content:     "hi",
			CreatedAt:   time.Now().UTC(),
		},
		SiteID: "site-a",
	}
	data, _ := json.Marshal(evt)

	store.EXPECT().
		FetchAndUpdateRoom(gomock.Any(), "ghost-room", "m-1", gomock.Any(), false).
		Return(nil, mongo.ErrNoDocuments)

	err := h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	require.ErrorIs(t, err, mongo.ErrNoDocuments)
	assert.Empty(t, pub.calls, "missing room must not emit a broadcast")
}
```

If `stubPublisher` doesn't already exist, look for the existing pattern in `handler_test.go` — there's almost certainly a recorder or mock publisher that other tests use; reuse it. If the existing one is a mockgen-generated `MockPublisher`, use `NewMockPublisher(ctrl)` and `.EXPECT().Publish(...).Times(0)`.

Imports you may need to add to the test file (check what's already there first):

```go
import (
    "context"
    "encoding/json"
    "testing"
    "time"

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.uber.org/mock/gomock"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/hmchangw/chat/pkg/model"
)
```

- [ ] **Step 5: Run the handler test suite**

Run: `cd /home/user/chat && go test -race ./broadcast-worker/ -run TestHandler -v`
Expected: PASS (including the new missing-room test).

- [ ] **Step 6: Run the full `broadcast-worker` test suite to confirm no regressions**

Run: `cd /home/user/chat && make test SERVICE=broadcast-worker`
Expected: PASS, all tests including `TestCachedUserStore*`.

- [ ] **Step 7: Commit**

```bash
cd /home/user/chat
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "refactor(broadcast-worker): call FetchAndUpdateRoom from handler"
```

---

## Task 7: Wire `CachedUserStore` in `main.go`

**Files:**
- Modify: `broadcast-worker/main.go`

- [ ] **Step 1: Add two new env fields to the `config` struct**

In `broadcast-worker/main.go`, find the `config` struct:

```go
type config struct {
	NatsURL              string        `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile        string        `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID               string        `env:"SITE_ID"                   envDefault:"default"`
	MongoURI             string        `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB              string        `env:"MONGO_DB"                  envDefault:"chat"`
	MaxWorkers           int           `env:"MAX_WORKERS"               envDefault:"100"`
	ValkeyAddr           string        `env:"VALKEY_ADDR,required"`
	ValkeyPassword       string        `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyKeyGracePeriod time.Duration `env:"VALKEY_KEY_GRACE_PERIOD,required"`
}
```

Replace with:

```go
type config struct {
	NatsURL              string        `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile        string        `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID               string        `env:"SITE_ID"                   envDefault:"default"`
	MongoURI             string        `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB              string        `env:"MONGO_DB"                  envDefault:"chat"`
	MaxWorkers           int           `env:"MAX_WORKERS"               envDefault:"100"`
	UserCacheSize        int           `env:"USER_CACHE_SIZE"           envDefault:"10000"`
	UserCacheTTL         time.Duration `env:"USER_CACHE_TTL"            envDefault:"5m"`
	ValkeyAddr           string        `env:"VALKEY_ADDR,required"`
	ValkeyPassword       string        `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyKeyGracePeriod time.Duration `env:"VALKEY_KEY_GRACE_PERIOD,required"`
}
```

- [ ] **Step 2: Wrap the user store**

Find:

```go
us := userstore.NewMongoStore(db.Collection("users"))
```

Replace with:

```go
var us userstore.UserStore = userstore.NewMongoStore(db.Collection("users"))
if cfg.UserCacheSize > 0 && cfg.UserCacheTTL > 0 {
	us = NewCachedUserStore(us, cfg.UserCacheSize, cfg.UserCacheTTL)
	slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)
} else {
	slog.Info("user-cache disabled")
}
```

Note: the existing line `us := userstore.NewMongoStore(...)` uses short-var-declare; the replacement uses `var us userstore.UserStore = ...` so we can reassign it on the next line. This is Go-idiomatic for wrapper decoration.

- [ ] **Step 3: Verify the build compiles**

Run: `cd /home/user/chat && go build ./broadcast-worker/`
Expected: succeeds.

- [ ] **Step 4: Run tests**

Run: `cd /home/user/chat && make test SERVICE=broadcast-worker`
Expected: PASS. `main.go` has no unit tests; if an integration test (under `//go:build integration`) exists, it's not exercised by the default `make test` target.

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add broadcast-worker/main.go
git commit -m "feat(broadcast-worker): wire CachedUserStore via USER_CACHE_SIZE/TTL env"
```

---

## Task 8: Verify integration test and lint

**Files:**
- None modified.

- [ ] **Step 1: Lint**

Run: `cd /home/user/chat && make lint`
Expected: 0 issues. If anything flags, fix the root cause and commit in a follow-up step before proceeding.

Common lint findings to expect and how to address without `//nolint`:

- `rangeValCopy` on `sample` structs: convert `for _, s := range slice` to `for i := range slice { s := &slice[i]; ... }`.
- `hugeParam` on a struct receiver: make the method take a pointer receiver.
- `errcheck` on helper calls that return error: assign with `_ = ...` only if the error is truly uninteresting at the call site; otherwise wrap and return.

- [ ] **Step 2: Run the integration test (requires Docker)**

Run: `cd /home/user/chat && make test-integration SERVICE=broadcast-worker`
Expected: PASS. If Docker isn't available in the current environment, skip this step and note it — the integration test is functionally unchanged by this plan, so its behavior should not have regressed, but running it locally after merging is still advisable.

- [ ] **Step 3: Coverage check**

Run:
```bash
cd /home/user/chat
go test -race -coverprofile=/tmp/bw-cov.out ./broadcast-worker/
go tool cover -func=/tmp/bw-cov.out | tail -20
```

Expected: `usercache.go` at 100%; overall package ≥ 80% (integration-test-only paths like `FetchAndUpdateRoom` and parts of `main.go` are expected to stay at 0% without the integration test — if that drives the overall below 80%, note it as a known follow-up matching the existing loadgen-branch precedent).

- [ ] **Step 4: Clean up stray binaries, push**

```bash
cd /home/user/chat
rm -f broadcast-worker-bin broadcast-worker  # paranoia: go build shouldn't have created anything in repo root, but confirm
git status   # must be clean
git push -u origin claude/broadcast-worker-perf
```

---

## Done when

- `make lint` passes for the whole repo.
- `make test SERVICE=broadcast-worker` passes with `-race`.
- `usercache.go` is at 100% coverage per the project rule.
- `CachedUserStore` implements `userstore.UserStore` and is exercised by at least nine subtests (construction, miss, hit, partial, empty, missing-not-cached, inner-error, TTL, LRU, MRU promotion, concurrent).
- `Store` interface has `FetchAndUpdateRoom`, does not have `UpdateRoomOnNewMessage`, and retains `GetRoom`.
- `handler.go` uses `FetchAndUpdateRoom` and no longer calls `GetRoom` or `UpdateRoomOnNewMessage`.
- `main.go` wires the cache behind `USER_CACHE_SIZE`/`USER_CACHE_TTL` env vars with sensible defaults and a logging line.
- Branch `claude/broadcast-worker-perf` is pushed to origin.

---
