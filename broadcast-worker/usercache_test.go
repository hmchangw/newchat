package main

import (
	"context"
	"errors"
	"strconv"
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
	for i := range users {
		f.byAccount[users[i].Account] = users[i]
	}
	return f
}

func (f *fakeUserStore) FindUserByID(_ context.Context, id string) (*model.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if u, ok := f.byAccount[id]; ok {
		return &u, nil
	}
	return nil, errors.New("not found")
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

var _ userstore.UserStore = (*CachedUserStore)(nil)

func TestNewCachedUserStore_ConstructsEmpty(t *testing.T) {
	inner := newFakeUserStore()
	c := NewCachedUserStore(inner, 10, time.Minute)
	require.NotNil(t, c)
	// A fresh cache doesn't call inner until asked.
	assert.Equal(t, 0, inner.callCount())
	assert.Nil(t, inner.lastCall())
}

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
	innerErr := errors.New("boom")
	inner := newFakeUserStore()
	inner.err = innerErr
	c := NewCachedUserStore(inner, 10, time.Minute)

	_, err := c.FindUsersByAccounts(context.Background(), []string{"alice"})
	require.Error(t, err)
	assert.ErrorIs(t, err, innerErr, "inner error should be wrapped, not swallowed")
}

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
			ID:      "u-" + strconv.Itoa(i),
			Account: "acct-" + strconv.Itoa(i),
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
				_, err := store.FindUsersByAccounts(ctx, []string{"acct-" + strconv.Itoa(idx)})
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()
}

func TestCachedUserStore_FindUserByIDDelegates(t *testing.T) {
	// Keyed on account in the fake; for this test reuse the account as the ID.
	alice := model.User{ID: "alice", Account: "alice", EngName: "Alice"}
	inner := newFakeUserStore(alice)
	c := NewCachedUserStore(inner, 10, time.Minute)

	u, err := c.FindUserByID(context.Background(), "alice")
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.Equal(t, "alice", u.Account)

	_, err = c.FindUserByID(context.Background(), "ghost")
	require.Error(t, err, "inner store's not-found error should propagate")
}

func TestCachedUserStore_DedupesDuplicateAccounts(t *testing.T) {
	alice := model.User{ID: "u1", Account: "alice"}
	inner := newFakeUserStore(alice)
	c := NewCachedUserStore(inner, 10, time.Minute)

	// Cold cache: both duplicates would otherwise hit the inner store.
	// After dedup, inner sees alice exactly once and the return has one user.
	users, err := c.FindUsersByAccounts(context.Background(), []string{"alice", "alice"})
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, []string{"alice"}, inner.lastCall())

	// Warm cache: still one user returned regardless of how many dupes are asked.
	users2, err := c.FindUsersByAccounts(context.Background(), []string{"alice", "alice", "alice"})
	require.NoError(t, err)
	assert.Len(t, users2, 1)
	assert.Equal(t, 1, inner.callCount(), "warm-cache dupe lookup must not call inner")
}
