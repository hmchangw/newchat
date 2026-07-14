# Singleflight Leader-Context Cancellation Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop one caller's context cancellation from failing all other callers coalesced onto the same `singleflight` load, across the 6 affected LRU+singleflight caches.

**Architecture:** Convert each affected cache from blocking `singleflight.Group.Do` (whose closure runs on the leader caller's ctx) to `DoChan` + a per-caller `select`, with the shared load running on a detached, time-bounded context (`context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)`). This is the pattern already proven by `media-service/cache.go`, `pkg/atrest/cipher.go`, and `broadcast-worker/keycache.go`.

**Tech Stack:** Go 1.25, `golang.org/x/sync/singleflight`, `github.com/hashicorp/golang-lru/v2/expirable`, `stretchr/testify`, `go.uber.org/mock` (message-gatekeeper only).

**Design spec:** `docs/superpowers/specs/2026-07-06-singleflight-leader-cancel-fix-design.md`

## Global Constraints

- Go version floor: **1.25** (`context.WithoutCancel` needs ≥1.21 — satisfied).
- Detached context is **always** `context.WithTimeout(context.WithoutCancel(ctx), <const>)`; the timeout const is **`10 * time.Second`**, an unexported package-level const.
- **No `time.Sleep` for goroutine synchronization** — use channels. A `time.After` watchdog to detect a hang is allowed.
- Never add errors/negative results to the LRU; **no `sf.Forget`** (singleflight auto-drops the key on return).
- Run tests with the race detector: `make test SERVICE=<path>` (the Makefile adds `-race`). Never run raw `go test`.
- Out of scope: P2 aliasing, the 3 already-safe sites, any shared-helper extraction, client API / `docs/client-api.md`, `make generate` (no store-interface changes).
- Commit trailers on every commit:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
  ```

---

## The standard transformation (reference — every task instantiates this)

**Impl — before:**

```go
v, err, _ := c.sf.Do(key, func() (interface{}, error) {
    if cached, ok := c.lru.Get(key); ok { // in-flight double-check
        return cached, nil
    }
    x, err := c.load(ctx, key) // ctx = leader caller's ctx
    if err != nil {
        return zeroBoxed, err
    }
    c.lru.Add(key, x)
    return x, nil
})
if err != nil { /* metrics + wrap */ }
/* metrics.Miss */ return v.(T), nil
```

**Impl — after:**

```go
resCh := c.sf.DoChan(key, func() (interface{}, error) {
    if cached, ok := c.lru.Get(key); ok {
        return cached, nil
    }
    fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
    defer cancel()
    x, err := c.load(fetchCtx, key)
    if err != nil {
        return zeroBoxed, err
    }
    c.lru.Add(key, x)
    return x, nil
})
select {
case res := <-resCh:
    if res.Err != nil { /* metrics + wrap */ }
    /* metrics.Miss */ return res.Val.(T), nil
case <-ctx.Done():
    /* metrics.Error */ return zero, ctx.Err()
}
```

**Test pattern — a ctx-aware "block-first" fake.** The fake signals `entered` (buffered, size 1), then blocks on `block` **unconditionally**, and only **after** `block` is released does it check `ctx.Err()`. This makes the two regression tests deterministic:

- The leader holds the singleflight key open (blocked on `block`) until the test releases it, so the waiter is guaranteed to coalesce before the shared load returns.
- **Leader-cancel test:** on the current `Do` code the fake runs on the leader's ctx, so after `close(block)` it returns `ctx.Canceled`, poisoning the coalesced waiter → the `require.NoError(waiter)` assertion **fails** (clean red). On the fixed `DoChan` code the fake runs on the detached ctx (never cancelled), so the waiter gets the real value → **green**.
- **Caller-cancel test:** on the fixed code the caller's `select` returns `ctx.Err()` immediately on cancel. On the current blocking `Do` it cannot, so a `time.After(2s)` watchdog reports the hang → red.

---

## Task 1: `pkg/emoji` (pilot)

**Files:**
- Modify: `pkg/emoji/cache.go` (`CustomEmojiExists`, lines 76-101; add a `const`)
- Test: `pkg/emoji/cache_test.go` (extend `countingLookup`; add 2 tests)

**Interfaces:**
- Consumes: `emoji.NewCachedLookup(inner CustomEmojiLookup, size int, ttl time.Duration, ...) (*CachedLookup, error)`; `CustomEmojiExists(ctx, siteID, shortcode) (bool, error)`.
- Produces: unchanged public signatures — behavior fix only.

- [ ] **Step 1: Extend the test fake to be ctx-aware and blockable**

In `pkg/emoji/cache_test.go`, add two fields to `countingLookup` and replace its `CustomEmojiExists`:

```go
type countingLookup struct {
	mu      sync.Mutex
	calls   map[string]int
	results map[string]bool
	err     error
	delay   time.Duration
	block   chan struct{} // when non-nil, blocks (unconditionally) before returning
	entered chan struct{} // when non-nil (buffered), signals once when entered
}

func (l *countingLookup) CustomEmojiExists(ctx context.Context, siteID, shortcode string) (bool, error) {
	key := siteID + "|" + shortcode
	l.mu.Lock()
	l.calls[key]++
	l.mu.Unlock()
	if l.entered != nil {
		select {
		case l.entered <- struct{}{}:
		default:
		}
	}
	if l.block != nil {
		<-l.block
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if l.delay > 0 {
		time.Sleep(l.delay)
	}
	if l.err != nil {
		return false, l.err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.results[key], nil
}
```

- [ ] **Step 2: Write the two failing tests**

Append to `pkg/emoji/cache_test.go` (add `"time"` is already imported):

```go
func TestCachedLookup_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	inner := newCountingLookup()
	inner.results["site-a|tada"] = true
	inner.block = make(chan struct{})
	inner.entered = make(chan struct{}, 1)
	c, err := emoji.NewCachedLookup(inner, 16, time.Minute)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := c.CustomEmojiExists(leaderCtx, "site-a", "tada")
		leaderDone <- e
	}()
	<-inner.entered // leader is inside the shared load, holding the singleflight key

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := c.CustomEmojiExists(context.Background(), "site-a", "tada")
		waiterDone <- e
	}()
	<-waiterReady // waiter goroutine is running and about to coalesce

	cancelLeader()     // leader abandons via its own ctx
	close(inner.block) // release the (detached) shared load

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone, "waiter with a valid ctx must not be poisoned by the leader's cancel")

	// The shared load must have populated the cache: a fresh lookup does not hit inner.
	got, err := c.CustomEmojiExists(context.Background(), "site-a", "tada")
	require.NoError(t, err)
	assert.True(t, got)
	assert.Equal(t, 1, inner.callCount("site-a", "tada"))
}

func TestCachedLookup_CallerCancelReturnsCtxErr(t *testing.T) {
	inner := newCountingLookup()
	inner.results["site-a|tada"] = true
	inner.block = make(chan struct{})
	inner.entered = make(chan struct{}, 1)
	defer close(inner.block)
	c, err := emoji.NewCachedLookup(inner, 16, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := c.CustomEmojiExists(ctx, "site-a", "tada")
		done <- e
	}()
	<-inner.entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s (blocking Do?)")
	}
}
```

- [ ] **Step 3: Run the tests — verify they fail**

Run: `make test SERVICE=pkg/emoji`
Expected: FAIL. `LeaderCancelDoesNotPoisonWaiters` fails at `require.NoError(<-waiterDone)` (waiter got `context.Canceled`); `CallerCancelReturnsCtxErr` fails at the 2s watchdog `t.Fatal`.

- [ ] **Step 4: Apply the fix**

In `pkg/emoji/cache.go`, add the const just after the imports block (before `type Recorder`):

```go
// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second
```

Replace the body of `CustomEmojiExists` (the `sf.Do(...)` block through the final `return`) with:

```go
	resCh := c.sf.DoChan(k.String(), func() (interface{}, error) {
		if cached, ok := c.lru.Get(k); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		exists, err := c.inner.CustomEmojiExists(fetchCtx, siteID, shortcode)
		if err != nil {
			return false, err
		}
		c.lru.Add(k, exists)
		return exists, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			return false, fmt.Errorf("custom emoji lookup %q for site %q: %w", shortcode, siteID, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(bool), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return false, ctx.Err()
	}
```

- [ ] **Step 5: Run tests — verify they pass**

Run: `make test SERVICE=pkg/emoji`
Expected: PASS (all emoji tests, including the two new ones, under `-race`).

- [ ] **Step 6: Commit**

```bash
git add pkg/emoji/cache.go pkg/emoji/cache_test.go
git commit -m "$(cat <<'EOF'
fix(emoji): detach shared emoji-exists load from caller cancellation

Convert CustomEmojiExists from blocking singleflight.Do (running on the
leader caller's ctx) to DoChan + per-caller select, with the shared load
on context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout). A
caller's cancel no longer poisons others coalesced on the same key.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
EOF
)"
```

**⛔ Pilot checkpoint:** stop here for review before replicating to Tasks 2-6.

---

## Task 2: `pkg/roommetacache`

**Files:**
- Modify: `pkg/roommetacache/roommetacache.go` (`Get`, lines 94-119; add a `const`)
- Test: `pkg/roommetacache/roommetacache_test.go` (add 2 tests)

**Interfaces:**
- Consumes: `roommetacache.New(size, ttl, loader Loader, ...) (*Cache, error)`; `Loader = func(ctx, roomID) (Meta, error)`; `(*Cache).Get(ctx, roomID) (Meta, error)`.

- [ ] **Step 1: Write the two failing tests**

Append to `pkg/roommetacache/roommetacache_test.go`:

```go
func TestCache_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	loader := func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
		calls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		if err := ctx.Err(); err != nil {
			return roommetacache.Meta{}, err
		}
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := c.Get(leaderCtx, "r1")
		leaderDone <- e
	}()
	<-entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := c.Get(context.Background(), "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	close(block)

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	got, err := c.Get(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, makeMeta("r1"), got)
	assert.Equal(t, int32(1), calls.Load(), "shared load should have populated the cache")
}

func TestCache_CallerCancelReturnsCtxErr(t *testing.T) {
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	defer close(block)
	loader := func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		if err := ctx.Err(); err != nil {
			return roommetacache.Meta{}, err
		}
		return makeMeta(roomID), nil
	}
	c, err := roommetacache.New(10, time.Minute, loader)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := c.Get(ctx, "r1")
		done <- e
	}()
	<-entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}
```

- [ ] **Step 2: Run the tests — verify they fail**

Run: `make test SERVICE=pkg/roommetacache`
Expected: FAIL (waiter poisoned; caller-cancel watchdog fires).

- [ ] **Step 3: Apply the fix**

Add just after the imports block in `pkg/roommetacache/roommetacache.go`:

```go
// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second
```

Replace the `sf.Do(...)` block in `Get` (lines 100-118) with:

```go
	resCh := c.sf.DoChan(roomID, func() (interface{}, error) {
		if cached, ok := c.lru.Get(roomID); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		loaded, err := c.loader(fetchCtx, roomID)
		if err != nil {
			return Meta{}, err
		}
		c.lru.Add(roomID, loaded)
		return loaded, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			return Meta{}, fmt.Errorf("get room meta for %q: %w", roomID, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(Meta), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return Meta{}, ctx.Err()
	}
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `make test SERVICE=pkg/roommetacache`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/roommetacache/roommetacache.go pkg/roommetacache/roommetacache_test.go
git commit -m "$(cat <<'EOF'
fix(roommetacache): detach shared room-meta load from caller cancellation

Convert Cache.Get from blocking singleflight.Do to DoChan + per-caller
select with the loader on context.WithTimeout(context.WithoutCancel(ctx),
fetchTimeout), so one caller's cancel no longer fails coalesced callers.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
EOF
)"
```

---

## Task 3: `pkg/userstore` (two methods)

**Files:**
- Modify: `pkg/userstore/cache.go` (`FindUserByID` 69-95 and `FindUserByAccount` 97-123; add a `const`)
- Test: `pkg/userstore/cache_test.go` (extend `fakeStore`; add 2 tests targeting `FindUserByAccount`)

**Interfaces:**
- Consumes: `userstore.NewCache(store UserStore, size, ttl, ...) (*Cache, error)`; `FindUserByID(ctx, id) (*model.User, error)`; `FindUserByAccount(ctx, account) (*model.User, error)`; `userstore.ErrUserNotFound`.

- [ ] **Step 1: Extend the test fake with an `entered` signal + ctx check**

`fakeStore` already has `block chan struct{}` on `FindUserByAccount`. Add an `entered` field and make the method ctx-aware, block-first:

```go
type fakeStore struct {
	mu             sync.Mutex
	usersByID      map[string]*model.User
	usersByAccount map[string]*model.User
	byIDCalls      int
	byAccountCalls int
	err            error
	block          chan struct{} // FindUserByAccount blocks (unconditionally) before returning
	entered        chan struct{} // when non-nil (buffered), signals once when entered
}

func (f *fakeStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.block != nil {
		<-f.block
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byAccountCalls++
	if f.err != nil {
		return nil, f.err
	}
	if u, ok := f.usersByAccount[account]; ok {
		return u, nil
	}
	return nil, userstore.ErrUserNotFound
}
```

- [ ] **Step 2: Write the two failing tests**

Append to `pkg/userstore/cache_test.go`:

```go
func TestCache_FindUserByAccount_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	store.block = make(chan struct{})
	store.entered = make(chan struct{}, 1)
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := c.FindUserByAccount(leaderCtx, "alice")
		leaderDone <- e
	}()
	<-store.entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := c.FindUserByAccount(context.Background(), "alice")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	close(store.block)

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	got, err := c.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, store.byAccountCalls, "shared load should have populated the cache")
}

func TestCache_FindUserByAccount_CallerCancelReturnsCtxErr(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	store.block = make(chan struct{})
	store.entered = make(chan struct{}, 1)
	defer close(store.block)
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := c.FindUserByAccount(ctx, "alice")
		done <- e
	}()
	<-store.entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}
```

- [ ] **Step 3: Run the tests — verify they fail**

Run: `make test SERVICE=pkg/userstore`
Expected: FAIL (waiter poisoned; caller-cancel watchdog fires).

- [ ] **Step 4: Apply the fix (both methods)**

Add just after the imports block in `pkg/userstore/cache.go`:

```go
// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second
```

Replace the `sf.Do(...)` block in `FindUserByID` (lines 75-94) with:

```go
	resCh := c.sf.DoChan(id, func() (interface{}, error) {
		if cached, ok := c.byID.Get(id); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		u, err := c.store.FindUserByID(fetchCtx, id)
		if err != nil {
			return nil, err
		}
		c.populate(u)
		return u, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			if errors.Is(res.Err, ErrUserNotFound) {
				return nil, res.Err
			}
			return nil, fmt.Errorf("find cached user %q: %w", id, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(*model.User), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
```

Replace the `sf.Do(...)` block in `FindUserByAccount` (lines 103-122) with:

```go
	resCh := c.sf.DoChan("account:"+account, func() (interface{}, error) {
		if cached, ok := c.byAccount.Get(account); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		u, err := c.store.FindUserByAccount(fetchCtx, account)
		if err != nil {
			return nil, err
		}
		c.populate(u)
		return u, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			if errors.Is(res.Err, ErrUserNotFound) {
				return nil, res.Err
			}
			return nil, fmt.Errorf("find cached user by account %q: %w", account, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(*model.User), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
```

- [ ] **Step 5: Run tests — verify they pass**

Run: `make test SERVICE=pkg/userstore`
Expected: PASS (including existing `TestCache_FindUserByAccount_SingleflightCollapse`, which uses `block` with `context.Background()` and is unaffected by the ctx check).

- [ ] **Step 6: Commit**

```bash
git add pkg/userstore/cache.go pkg/userstore/cache_test.go
git commit -m "$(cat <<'EOF'
fix(userstore): detach shared user loads from caller cancellation

Convert FindUserByID and FindUserByAccount from blocking singleflight.Do
to DoChan + per-caller select, loading on
context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout). One
caller's cancel no longer fails callers coalesced on the same key.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
EOF
)"
```

---

## Task 4: `notification-worker`

**Files:**
- Modify: `notification-worker/members.go` (`GetMembers`, lines 36-64; add a `const`)
- Test: `notification-worker/members_test.go` (extend `fakeLoader`; add 2 tests)

**Interfaces:**
- Consumes: `newCachedMemberLookup(cache roomsubcache.Cache, load memberLoader, ttl) *cachedMemberLookup`; `GetMembers(ctx, roomID) ([]roomsubcache.Member, error)`. Test package is `main` (internal).
- Note: the fast-path `c.cache.Get(ctx, roomID)` before singleflight keeps the **caller's** ctx; only the three ctx uses **inside** the closure switch to the detached `fetchCtx`.

- [ ] **Step 1: Extend `fakeLoader` to be ctx-aware and blockable**

In `notification-worker/members_test.go`, replace `fakeLoader` and its `Load`:

```go
type fakeLoader struct {
	calls   atomic.Int32
	out     []roomsubcache.Member
	err     error
	delay   time.Duration
	block   chan struct{} // when non-nil, blocks (unconditionally) before returning
	entered chan struct{} // when non-nil (buffered), signals once when entered
}

func (f *fakeLoader) Load(ctx context.Context, _ string) ([]roomsubcache.Member, error) {
	f.calls.Add(1)
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.block != nil {
		<-f.block
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.out, f.err
}
```

- [ ] **Step 2: Write the two failing tests**

Append to `notification-worker/members_test.go`:

```go
func TestCachedMemberLookup_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{
		out:     []roomsubcache.Member{{ID: "u1", Account: "alice"}},
		block:   make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := lookup.GetMembers(leaderCtx, "r1")
		leaderDone <- e
	}()
	<-loader.entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := lookup.GetMembers(context.Background(), "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	close(loader.block)

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	got, err := lookup.GetMembers(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, loader.out, got)
	assert.Equal(t, int32(1), loader.calls.Load(), "shared load should have populated the cache")
}

func TestCachedMemberLookup_CallerCancelReturnsCtxErr(t *testing.T) {
	cache := newFakeCache()
	loader := &fakeLoader{
		out:     []roomsubcache.Member{{ID: "u1", Account: "alice"}},
		block:   make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	defer close(loader.block)
	lookup := newCachedMemberLookup(cache, loader.Load, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := lookup.GetMembers(ctx, "r1")
		done <- e
	}()
	<-loader.entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}
```

- [ ] **Step 3: Run the tests — verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL (waiter poisoned; caller-cancel watchdog fires).

- [ ] **Step 4: Apply the fix**

Add just after the imports block in `notification-worker/members.go`:

```go
// memberFetchTimeout bounds the detached shared load so a hung backend cannot
// leak the singleflight goroutine or pin the in-flight key. See the design spec.
const memberFetchTimeout = 10 * time.Second
```

Replace the `sf.Do(...)` block in `GetMembers` (lines 46-63) with:

```go
	resCh := c.sf.DoChan(roomID, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberFetchTimeout)
		defer cancel()
		// Re-check inside the flight in case a sibling caller already populated.
		if got, err := c.cache.Get(fetchCtx, roomID); err == nil {
			return got, nil
		}
		loaded, lerr := c.load(fetchCtx, roomID)
		if lerr != nil {
			return nil, fmt.Errorf("load members for room %s: %w", roomID, lerr)
		}
		if setErr := c.cache.Set(fetchCtx, roomID, loaded, c.ttl); setErr != nil {
			slog.Warn("roomsubcache set failed", "error", setErr, "roomId", roomID)
		}
		return loaded, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			return nil, fmt.Errorf("get members for room %s: %w", roomID, res.Err)
		}
		return res.Val.([]roomsubcache.Member), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
```

Add `"context"` usage is already imported. Confirm `context` is in the import block (it is).

- [ ] **Step 5: Run tests — verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add notification-worker/members.go notification-worker/members_test.go
git commit -m "$(cat <<'EOF'
fix(notification-worker): detach shared member load from caller cancel

Convert cachedMemberLookup.GetMembers from blocking singleflight.Do to
DoChan + per-caller select; the in-flight Valkey/Mongo work now runs on
context.WithTimeout(context.WithoutCancel(ctx), memberFetchTimeout) so one
caller's cancel no longer fails callers coalesced on the same room.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
EOF
)"
```

---

## Task 5: `message-gatekeeper`

**Files:**
- Modify: `message-gatekeeper/subcache.go` (`GetSubscription`, lines 66-107; add a `const`)
- Test: `message-gatekeeper/subcache_test.go` (add 2 tests using gomock `DoAndReturn`)

**Interfaces:**
- Consumes: `newCachedSubStore(inner Store, size, ttl) (*cachedSubStore, error)`; `GetSubscription(ctx, account, roomID) (*model.Subscription, error)`; `NewMockStore(ctrl)`; `errNotSubscribed`. Test package is `main` (internal).

- [ ] **Step 1: Write the two failing tests**

Append to `message-gatekeeper/subcache_test.go`:

```go
func TestCachedSubStore_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	want := &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, Roles: []model.Role{model.RoleMember}}
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").DoAndReturn(
		func(ctx context.Context, _, _ string) (*model.Subscription, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-block
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return want, nil
		}).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := cached.GetSubscription(leaderCtx, "alice", "r1")
		leaderDone <- e
	}()
	<-entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := cached.GetSubscription(context.Background(), "alice", "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	close(block)

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	// Cache populated: a fresh hit does not call inner again (Times(1) enforces this).
	got, err := cached.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, "u1", got.User.ID)
}

func TestCachedSubStore_CallerCancelReturnsCtxErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	block := make(chan struct{})
	defer close(block)
	entered := make(chan struct{}, 1)
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").DoAndReturn(
		func(ctx context.Context, _, _ string) (*model.Subscription, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-block
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &model.Subscription{User: model.SubscriptionUser{Account: "alice"}}, nil
		}).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := cached.GetSubscription(ctx, "alice", "r1")
		done <- e
	}()
	<-entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}
```

- [ ] **Step 2: Run the tests — verify they fail**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL (waiter poisoned; caller-cancel watchdog fires).

- [ ] **Step 3: Apply the fix**

Add just after the imports block in `message-gatekeeper/subcache.go`:

```go
// subFetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const subFetchTimeout = 10 * time.Second
```

Replace the `sf.Do(...)` block in `GetSubscription` (lines 77-106) with:

```go
	resCh := c.sf.DoChan(sfKey, func() (interface{}, error) {
		if cached, ok := c.lru.Get(key); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), subFetchTimeout)
		defer cancel()
		sub, err := c.Store.GetSubscription(fetchCtx, account, roomID)
		if err != nil {
			// Do not cache errNotSubscribed or transient errors — see spec.
			return nil, err
		}
		projected := cachedSubscription{
			ID:      sub.User.ID,
			Account: sub.User.Account,
			Roles:   append([]model.Role(nil), sub.Roles...),
		}
		c.lru.Add(key, projected)
		return projected, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			if errors.Is(res.Err, errNotSubscribed) {
				c.metrics.Miss(ctx)
			} else {
				c.metrics.Error(ctx)
			}
			return nil, fmt.Errorf("get cached subscription: %w", res.Err)
		}
		c.metrics.Miss(ctx)
		return fromCached(res.Val.(cachedSubscription)), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return nil, ctx.Err()
	}
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add message-gatekeeper/subcache.go message-gatekeeper/subcache_test.go
git commit -m "$(cat <<'EOF'
fix(message-gatekeeper): detach shared subscription load from caller cancel

Convert cachedSubStore.GetSubscription from blocking singleflight.Do to
DoChan + per-caller select, loading on
context.WithTimeout(context.WithoutCancel(ctx), subFetchTimeout) so one
caller's cancel no longer fails callers coalesced on the same key.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
EOF
)"
```

---

## Task 6: `history-service` readcache (generic — fixes all 3 caches)

**Files:**
- Modify: `history-service/internal/readcache/readcache.go` (`getOrLoad`, lines 55-82; add a `const`)
- Test: `history-service/internal/readcache/readcache_test.go` (extend `fakeSubSource`; add 2 tests via `SubscriptionCache`)

**Interfaces:**
- Consumes: `NewSubscriptionCache(inner SubscriptionSource, size, ttl) (*SubscriptionCache, error)`; `(*SubscriptionCache).GetHistorySharedSince(ctx, account, roomID) (*time.Time, bool, error)`. Test package is `readcache` (internal). Fixing `getOrLoad` fixes `SubscriptionCache`, `RoomCache.times`, and `RoomCache.minSeen`.

- [ ] **Step 1: Extend `fakeSubSource` with a ctx check**

`fakeSubSource` already has `block`/`started`. Make `GetHistorySharedSince` ctx-aware, block-first:

```go
func (f *fakeSubSource) GetHistorySharedSince(ctx context.Context, _, _ string) (*time.Time, bool, error) {
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.block != nil {
		<-f.block
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	f.calls.Add(1)
	return f.sharedSince, f.subscribed, f.err
}
```

(Note: `started` is unbuffered here — send it before blocking so the test observes entry.)

- [ ] **Step 2: Write the two failing tests**

Append to `history-service/internal/readcache/readcache_test.go`:

```go
func TestSubscriptionCache_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{
		sharedSince: &ts,
		subscribed:  true,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, _, e := c.GetHistorySharedSince(leaderCtx, "alice", "r1")
		leaderDone <- e
	}()
	<-src.started

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, _, e := c.GetHistorySharedSince(context.Background(), "alice", "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	close(src.block)

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	_, sub, err := c.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.True(t, sub)
	assert.Equal(t, int32(1), src.calls.Load(), "shared load should have populated the cache")
}

func TestSubscriptionCache_CallerCancelReturnsCtxErr(t *testing.T) {
	ts := time.Now().UTC()
	src := &fakeSubSource{
		sharedSince: &ts,
		subscribed:  true,
		block:       make(chan struct{}),
		started:     make(chan struct{}),
	}
	defer close(src.block)
	c, err := NewSubscriptionCache(src, 100, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, e := c.GetHistorySharedSince(ctx, "alice", "r1")
		done <- e
	}()
	<-src.started
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}
```

- [ ] **Step 3: Run the tests — verify they fail**

Run: `make test SERVICE=history-service/internal/readcache`
Expected: FAIL (waiter poisoned; caller-cancel watchdog fires).

- [ ] **Step 4: Apply the fix**

Add just after the imports block in `history-service/internal/readcache/readcache.go`:

```go
// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second
```

Replace the `sf.Do(...)` block in `getOrLoad` (lines 61-81) with:

```go
	resCh := c.sf.DoChan(key, func() (any, error) {
		// Re-check under singleflight in case a sibling populated the entry.
		if cached, ok := c.lru.Get(key); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		val, store, err := load(fetchCtx)
		if err != nil {
			return val, err
		}
		if store {
			c.lru.Add(key, val)
		}
		return val, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			var zero V
			return zero, res.Err
		}
		c.metrics.Miss(ctx)
		return res.Val.(V), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		var zero V
		return zero, ctx.Err()
	}
```

- [ ] **Step 5: Run tests — verify they pass**

Run: `make test SERVICE=history-service/internal/readcache`
Expected: PASS (including the existing `TestSubscriptionCache_SingleflightDedupesConcurrentMisses`, which uses `block`/`started` with `context.Background()`).

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/readcache/readcache.go history-service/internal/readcache/readcache_test.go
git commit -m "$(cat <<'EOF'
fix(history-service): detach shared readcache load from caller cancel

Convert the generic ttlCache.getOrLoad from blocking singleflight.Do to
DoChan + per-caller select, loading on
context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout). Fixes
leader-cancel poisoning for all three readcaches (subscription, room
times, min-user-last-seen) in one change.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017kEVyrEBZuozxu7x6TXZ3M
EOF
)"
```

---

## Task 7: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Full unit suite with race detector**

Run: `make test`
Expected: PASS (all services/packages).

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: no findings.

- [ ] **Step 3: SAST (blocking CI gate)**

Run: `make sast`
Expected: no medium+ findings. (The change adds no `InsecureSkipVerify`, unsafe conversions, or new deps.)

- [ ] **Step 4: Push the branch**

```bash
git push -u origin ds-feat/fix_singleflight
```

(Do **not** open a PR unless the user asks. If push fails on a network error, retry with backoff 2s/4s/8s/16s.)

---

## Self-review

**Spec coverage:**
- Spec §2 six affected sites → Tasks 1-6, one per site (userstore's two methods both in Task 3). ✓
- Spec §3 standard fix (`DoChan` + `WithoutCancel` + `WithTimeout` + `select`) → applied verbatim in every task's Step "Apply the fix". ✓
- Spec §4 per-site notes (userstore two methods, notification three ctx uses, history generic `getOrLoad`) → reflected in Tasks 3, 4, 6. ✓
- Spec §5 decisions (`fetchTimeout = 10s` const; no `Forget`; metrics: caller-cancel → `Error`) → const added per task; no `Forget` anywhere; `<-ctx.Done()` branch calls `metrics.Error` where the site has a recorder (emoji, roommetacache, userstore, gatekeeper, history; notification has no recorder). ✓
- Spec §6 testing (two regression tests per site, red→green, no sleep, `-race`) → each task Steps 1-5. ✓
- Spec §8 rollout (pilot emoji, then 5, one PR) → Task 1 checkpoint; Task 7 push. ✓
- Spec §9 verification (`make test`/`lint`/`sast`; no `generate`; no client-api) → Task 7. ✓

**Placeholder scan:** none — every code step shows complete code; every run step shows the exact command + expected result.

**Type/name consistency:** const is `fetchTimeout` in the library packages (emoji, roommetacache, userstore, readcache) and `memberFetchTimeout` / `subFetchTimeout` in the two `package main` services (avoids any collision in those namespaces). Result field access is `res.Val` / `res.Err` (the `singleflight.Result` fields) throughout. Type assertions match each site's cached type: `bool`, `Meta`, `*model.User`, `[]roomsubcache.Member`, `cachedSubscription`, generic `V`.
