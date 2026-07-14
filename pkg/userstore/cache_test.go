package userstore_test

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

func newFakeStore(users ...model.User) *fakeStore {
	s := &fakeStore{
		usersByID:      make(map[string]*model.User, len(users)),
		usersByAccount: make(map[string]*model.User, len(users)),
	}
	for i := range users {
		u := users[i]
		s.usersByID[u.ID] = &u
		s.usersByAccount[u.Account] = &u
	}
	return s
}

func (f *fakeStore) FindUserByID(_ context.Context, id string) (*model.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byIDCalls++
	if f.err != nil {
		return nil, f.err
	}
	if u, ok := f.usersByID[id]; ok {
		return u, nil
	}
	return nil, userstore.ErrUserNotFound
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

func (f *fakeStore) FindUsersByAccounts(_ context.Context, accounts []string) ([]model.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byAccountCalls++
	if f.err != nil {
		return nil, f.err
	}
	out := make([]model.User, 0, len(accounts))
	for _, a := range accounts {
		if u, ok := f.usersByAccount[a]; ok {
			out = append(out, *u)
		}
	}
	return out, nil
}

func TestNewCache_RejectsInvalidArgs(t *testing.T) {
	_, err := userstore.NewCache(nil, 10, time.Minute)
	require.Error(t, err)

	_, err = userstore.NewCache(newFakeStore(), 0, time.Minute)
	require.Error(t, err)

	_, err = userstore.NewCache(newFakeStore(), 10, 0)
	require.Error(t, err)
}

func TestCache_FindUserByID_MissThenHit(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	cache, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	u, err := cache.FindUserByID(context.Background(), "u1")
	require.NoError(t, err)
	assert.Equal(t, "alice", u.Account)
	assert.Equal(t, 1, store.byIDCalls)

	_, err = cache.FindUserByID(context.Background(), "u1")
	require.NoError(t, err)
	assert.Equal(t, 1, store.byIDCalls, "second lookup must hit cache")
}

func TestCache_FindUserByID_NotFoundIsUnwrapped(t *testing.T) {
	cache, _ := userstore.NewCache(newFakeStore(), 10, time.Minute)
	_, err := cache.FindUserByID(context.Background(), "ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, userstore.ErrUserNotFound)
}

func TestCache_FindUserByID_StoreErrorWrapped(t *testing.T) {
	store := &fakeStore{err: errors.New("mongo down"), usersByID: map[string]*model.User{}, usersByAccount: map[string]*model.User{}}
	cache, _ := userstore.NewCache(store, 10, time.Minute)
	_, err := cache.FindUserByID(context.Background(), "u1")
	require.Error(t, err)
	assert.NotErrorIs(t, err, userstore.ErrUserNotFound)
	assert.Contains(t, err.Error(), "find cached user")
}

func TestCache_FindUsersByAccounts_BatchPartialHit(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore(
		model.User{ID: "u1", Account: "alice"},
		model.User{ID: "u2", Account: "bob"},
		model.User{ID: "u3", Account: "carol"},
	)
	cache, _ := userstore.NewCache(store, 10, time.Minute)

	got, err := cache.FindUsersByAccounts(ctx, []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, 1, store.byAccountCalls)

	got, err = cache.FindUsersByAccounts(ctx, []string{"alice", "carol"})
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, 2, store.byAccountCalls)
}

func TestCache_FindUsersByAccounts_CrossPopulatesByID(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	cache, _ := userstore.NewCache(store, 10, time.Minute)

	_, err := cache.FindUsersByAccounts(ctx, []string{"alice"})
	require.NoError(t, err)

	_, err = cache.FindUserByID(ctx, "u1")
	require.NoError(t, err)
	assert.Equal(t, 0, store.byIDCalls, "FindUserByID must hit cache via cross-populated key")
}

func TestCache_FindUserByID_CrossPopulatesByAccount(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	cache, _ := userstore.NewCache(store, 10, time.Minute)

	_, err := cache.FindUserByID(ctx, "u1")
	require.NoError(t, err)

	got, err := cache.FindUsersByAccounts(ctx, []string{"alice"})
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, 0, store.byAccountCalls, "FindUsersByAccounts must hit cache via cross-populated key")
}

func TestCache_FindUsersByAccounts_DedupesInput(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	cache, _ := userstore.NewCache(store, 10, time.Minute)

	got, err := cache.FindUsersByAccounts(context.Background(), []string{"alice", "alice", "alice"})
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

func TestCache_FindUsersByAccounts_EmptyInput(t *testing.T) {
	cache, _ := userstore.NewCache(newFakeStore(), 10, time.Minute)
	got, err := cache.FindUsersByAccounts(context.Background(), nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCache_FindUsersByAccounts_StoreErrorReturnsPartialHits(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	cache, _ := userstore.NewCache(store, 10, time.Minute)

	_, err := cache.FindUsersByAccounts(ctx, []string{"alice"})
	require.NoError(t, err)

	store.mu.Lock()
	store.err = errors.New("mongo down")
	store.mu.Unlock()
	got, err := cache.FindUsersByAccounts(ctx, []string{"alice", "ghost"})
	require.Error(t, err)
	assert.Len(t, got, 1, "alice hit must be returned alongside the error")
	assert.Equal(t, "alice", got[0].Account)
}

func TestCache_Invalidate(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	cache, _ := userstore.NewCache(store, 10, time.Minute)

	_, err := cache.FindUserByID(ctx, "u1")
	require.NoError(t, err)

	cache.Invalidate("u1", "alice")

	_, err = cache.FindUserByID(ctx, "u1")
	require.NoError(t, err)
	assert.Equal(t, 2, store.byIDCalls, "post-invalidate lookup must re-hit store")

	_, err = cache.FindUsersByAccounts(ctx, []string{"alice"})
	require.NoError(t, err)
	// Note: u1 was repopulated by the FindUserByID above (cross-populates the account prefix),
	// so this call should be a cache hit and not increment byAccountCalls.
	assert.Equal(t, 0, store.byAccountCalls)
}

func TestCache_FindUserByAccount_MissThenHit(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	got1, err := c.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err)
	require.NotNil(t, got1)
	assert.Equal(t, "u1", got1.ID)

	got2, err := c.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err)
	assert.Equal(t, got1, got2, "second call must serve from cache")
	assert.Equal(t, 1, store.byAccountCalls, "second call must not hit the store")
}

func TestCache_FindUserByAccount_NotFoundIsUnwrapped(t *testing.T) {
	store := newFakeStore()
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	got, err := c.FindUserByAccount(context.Background(), "nope")
	assert.Nil(t, got)
	require.Error(t, err)
	assert.ErrorIs(t, err, userstore.ErrUserNotFound, "miss must propagate ErrUserNotFound unwrapped")
}

func TestCache_FindUserByAccount_StoreErrorWrapped(t *testing.T) {
	store := newFakeStore()
	store.err = errors.New("mongo down")
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	got, err := c.FindUserByAccount(context.Background(), "alice")
	assert.Nil(t, got)
	require.Error(t, err)
	assert.False(t, errors.Is(err, userstore.ErrUserNotFound),
		"non-not-found errors must NOT be classified as ErrUserNotFound")
	assert.Contains(t, err.Error(), "mongo down")
}

func TestCache_FindUserByAccount_CrossPopulatesByID(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	_, err = c.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err)

	preByID := store.byIDCalls
	got, err := c.FindUserByID(context.Background(), "u1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, preByID, store.byIDCalls, "FindUserByID must serve from cross-populated entry")
}

func TestCache_FindUserByAccount_SingleflightCollapse(t *testing.T) {
	store := newFakeStore(model.User{ID: "u1", Account: "alice"})
	store.block = make(chan struct{})
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	const N = 20
	var wg sync.WaitGroup
	ready := make(chan struct{}, N)
	results := make([]*model.User, N)
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ready <- struct{}{}
			results[idx], errs[idx] = c.FindUserByAccount(context.Background(), "alice")
		}(i)
	}

	// Drain N readies before releasing the store: each goroutine signals just
	// before its FindUserByAccount call. Goroutines that reach sf.Do while the
	// store is still blocked collapse; any that arrive after the store returns
	// hit the now-populated cache. Either way, byAccountCalls == 1.
	for i := 0; i < N; i++ {
		<-ready
	}
	close(store.block)
	wg.Wait()

	for i := 0; i < N; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		assert.Equal(t, "u1", results[i].ID)
	}
	assert.Equal(t, 1, store.byAccountCalls,
		"singleflight must collapse %d concurrent misses into 1 store call", N)
}

func TestCache_FindUserByAccount_MissDoesNotPoison(t *testing.T) {
	store := newFakeStore() // empty
	c, err := userstore.NewCache(store, 10, time.Minute)
	require.NoError(t, err)

	_, err = c.FindUserByAccount(context.Background(), "alice")
	require.ErrorIs(t, err, userstore.ErrUserNotFound)
	require.Equal(t, 1, store.byAccountCalls)

	alice := model.User{ID: "u1", Account: "alice"}
	store.mu.Lock()
	store.usersByAccount["alice"] = &alice
	store.mu.Unlock()

	got, err := c.FindUserByAccount(context.Background(), "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 2, store.byAccountCalls,
		"miss must not poison the cache; next call must re-hit the store")
}

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
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	close(store.block)
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
