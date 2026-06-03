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
	stats := cache.Stats()
	assert.GreaterOrEqual(t, stats.Hits, uint64(1))
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
