package main

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

func testSenderCache(t *testing.T) *lru.Cache[string, resolvedSender] {
	t.Helper()
	c, err := lru.New[string, resolvedSender](8)
	require.NoError(t, err)
	return c
}

// noPublish fails the test if the create path (which alone should publish) is
// hit unexpectedly.
func noPublish(t *testing.T) func(context.Context, []model.IUserWithChange) error {
	t.Helper()
	return func(context.Context, []model.IUserWithChange) error {
		t.Fatal("publishUsers called unexpectedly")
		return nil
	}
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

	r := newSenderResolver(store, "s1", testSenderCache(t), noPublish(t))
	got, err := r.resolve(context.Background(), "graph-1", "愛麗絲")
	require.NoError(t, err)
	assert.Equal(t, "alice", got.Account)
	assert.Equal(t, "uid1", got.UserID)
	assert.Equal(t, "Alice 愛麗絲", got.DisplayName) // engName and chineseName combined
}

func TestSenderResolver_NoUserPublishesDeterministicIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	wantID := idgen.DeterministicID([]byte("graph-2"))
	// Order: account resolved → users miss → publish (no local write, no read-back).
	gomock.InOrder(
		store.EXPECT().AccountByTeamsID(gomock.Any(), "graph-2").Return("bob", nil),
		store.EXPECT().FindUserByAccount(gomock.Any(), "bob").Return(nil, nil),
	)

	var published []model.IUserWithChange
	publishCalls := 0
	publish := func(_ context.Context, users []model.IUserWithChange) error {
		publishCalls++
		published = users
		return nil
	}

	r := newSenderResolver(store, "s1", testSenderCache(t), publish)
	got, err := r.resolve(context.Background(), "graph-2", "Bob")
	require.NoError(t, err)
	assert.Equal(t, "bob", got.Account)
	assert.Equal(t, wantID, got.UserID, "sender carries the deterministic _id set by the publisher")
	assert.Equal(t, "Bob", got.DisplayName)

	// The publisher owns a deterministic _id (Graph-id hash) so every site's
	// hr-sync-worker converges on the same identity; no employeeId.
	assert.Equal(t, 1, publishCalls, "the new external user must be fanned out")
	require.Len(t, published, 1)
	assert.Equal(t, "bob", published[0].Account)
	assert.Equal(t, wantID, published[0].ID)
	assert.Empty(t, published[0].EmployeeID, "externals aren't employees — no employeeId")
	assert.Equal(t, "Bob", published[0].ChineseName)
	assert.Equal(t, "s1", published[0].SiteID)
	assert.Equal(t, model.IChangeTypeNewHire, published[0].ChangeType)
}

func TestSenderResolver_NoTeamsUserErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	store.EXPECT().AccountByTeamsID(gomock.Any(), "graph-x").Return("", nil) // no teams_user record

	r := newSenderResolver(store, "s1", testSenderCache(t), noPublish(t))
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

	r := newSenderResolver(store, "s1", testSenderCache(t), noPublish(t))
	for i := 0; i < 2; i++ {
		got, err := r.resolve(context.Background(), "graph-1", "Al")
		require.NoError(t, err)
		assert.Equal(t, "al", got.Account)
	}
}

func TestSenderResolver_EmptyTeamsIDErrors(t *testing.T) {
	r := newSenderResolver(NewMockHRIdentityStore(gomock.NewController(t)), "s1", testSenderCache(t), noPublish(t))
	_, err := r.resolve(context.Background(), "", "x")
	require.Error(t, err)
}
