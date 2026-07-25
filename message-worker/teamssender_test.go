package main

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func testSenderCache(t *testing.T) *lru.Cache[string, resolvedSender] {
	t.Helper()
	c, err := lru.New[string, resolvedSender](8)
	require.NoError(t, err)
	return c
}

func TestSenderResolver_AccountHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	existing := &model.User{ID: "uid1", Account: "alice", SiteID: "s1", EngName: "Alice", ChineseName: "愛麗絲"}
	gomock.InOrder(
		store.EXPECT().AccountByTeamsID(gomock.Any(), "graph-1").Return("alice", nil),
		store.EXPECT().FindUserByAccount(gomock.Any(), "alice").Return(existing, nil),
	)
	// account found → no upsert.

	r := newSenderResolver(store, "s1", testSenderCache(t))
	got, err := r.resolve(context.Background(), "graph-1", "愛麗絲")
	require.NoError(t, err)
	assert.Equal(t, "alice", got.Account)
	assert.Equal(t, "uid1", got.UserID)
	assert.Equal(t, "Alice 愛麗絲", got.DisplayName) // engName and chineseName combined
}

func TestSenderResolver_NoUserCreates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	created := &model.User{ID: "new-uid", Account: "bob", SiteID: "s1", ChineseName: "Bob"}
	// Order: account resolved → users miss → upsert (no employeeId) → read back the created row.
	gomock.InOrder(
		store.EXPECT().AccountByTeamsID(gomock.Any(), "graph-2").Return("bob", nil),
		store.EXPECT().FindUserByAccount(gomock.Any(), "bob").Return(nil, nil),
		store.EXPECT().UpsertUserIdentities(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, users []model.IUserWithChange) error {
				require.Len(t, users, 1)
				assert.Equal(t, "bob", users[0].Account)
				assert.Empty(t, users[0].EmployeeID, "migrated users carry no employeeId")
				assert.Equal(t, "Bob", users[0].ChineseName)
				assert.Equal(t, "s1", users[0].SiteID)
				return nil
			}),
		store.EXPECT().FindUserByAccount(gomock.Any(), "bob").Return(created, nil),
	)

	r := newSenderResolver(store, "s1", testSenderCache(t))
	got, err := r.resolve(context.Background(), "graph-2", "Bob")
	require.NoError(t, err)
	assert.Equal(t, "bob", got.Account)
	assert.Equal(t, "new-uid", got.UserID, "created sender carries the minted UserID")
	assert.Equal(t, "Bob", got.DisplayName)
}

func TestSenderResolver_NoTeamsUserErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	store.EXPECT().AccountByTeamsID(gomock.Any(), "graph-x").Return("", nil) // no teams_user record

	r := newSenderResolver(store, "s1", testSenderCache(t))
	_, err := r.resolve(context.Background(), "graph-x", "Nobody")
	require.Error(t, err)
}

func TestSenderResolver_CacheHitSkipsStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	// Exactly one round of lookups for two resolves.
	gomock.InOrder(
		store.EXPECT().AccountByTeamsID(gomock.Any(), "graph-1").Return("al", nil).Times(1),
		store.EXPECT().FindUserByAccount(gomock.Any(), "al").Return(&model.User{Account: "al"}, nil).Times(1),
	)

	r := newSenderResolver(store, "s1", testSenderCache(t))
	for i := 0; i < 2; i++ {
		got, err := r.resolve(context.Background(), "graph-1", "Al")
		require.NoError(t, err)
		assert.Equal(t, "al", got.Account)
	}
}

func TestSenderResolver_EmptyTeamsIDErrors(t *testing.T) {
	r := newSenderResolver(NewMockHRIdentityStore(gomock.NewController(t)), "s1", testSenderCache(t))
	_, err := r.resolve(context.Background(), "", "x")
	require.Error(t, err)
}
