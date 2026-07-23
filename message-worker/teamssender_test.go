package main

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

func testSenderCache(t *testing.T) *lru.Cache[string, resolvedSender] {
	t.Helper()
	c, err := lru.New[string, resolvedSender](8)
	require.NoError(t, err)
	return c
}

func TestSenderResolver_EmployeeIdHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	empID := teamsmigrate.EmployeeIDFromGraphID("graph-1")
	existing := &model.User{ID: "uid1", Account: "alice", SiteID: "s1", EngName: "Alice", ChineseName: "愛麗絲"}
	store.EXPECT().FindUserByEmployeeId(gomock.Any(), empID).Return(existing, nil)
	// employeeId is authoritative: no display-name lookup, no upsert.

	r := newSenderResolver(store, "s1", testSenderCache(t))
	got, err := r.resolve(context.Background(), "graph-1", "愛麗絲")
	require.NoError(t, err)
	assert.Equal(t, "alice", got.Account)
	assert.Equal(t, "uid1", got.UserID)
	assert.Equal(t, "Alice 愛麗絲", got.DisplayName) // engName and chineseName combined
}

func TestSenderResolver_DisplayNameFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	empID := teamsmigrate.EmployeeIDFromGraphID("graph-1")
	store.EXPECT().FindUserByEmployeeId(gomock.Any(), empID).Return(nil, nil) // employeeId miss
	store.EXPECT().FindUserByDisplayName(gomock.Any(), "愛麗絲").
		Return(&model.User{Account: "alice"}, nil)

	r := newSenderResolver(store, "s1", testSenderCache(t))
	got, err := r.resolve(context.Background(), "graph-1", "愛麗絲")
	require.NoError(t, err)
	assert.Equal(t, "alice", got.Account)
}

func TestSenderResolver_NoMatchCreates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	wantEmp := teamsmigrate.EmployeeIDFromGraphID("graph-2")
	created := &model.User{ID: "new-uid", Account: wantEmp, SiteID: "s1", ChineseName: "Bob"}
	// Order: employeeId miss → displayName miss → upsert → read back the created row.
	gomock.InOrder(
		store.EXPECT().FindUserByEmployeeId(gomock.Any(), wantEmp).Return(nil, nil),
		store.EXPECT().FindUserByDisplayName(gomock.Any(), "Bob").Return(nil, nil),
		store.EXPECT().UpsertUserIdentities(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, users []model.IUserWithChange) error {
				require.Len(t, users, 1)
				assert.Equal(t, wantEmp, users[0].EmployeeID)
				assert.Equal(t, wantEmp, users[0].Account) // account = employeeId (no UPN at the message layer)
				assert.Equal(t, "Bob", users[0].ChineseName)
				assert.Equal(t, "s1", users[0].SiteID)
				return nil
			}),
		store.EXPECT().FindUserByEmployeeId(gomock.Any(), wantEmp).Return(created, nil),
	)

	r := newSenderResolver(store, "s1", testSenderCache(t))
	got, err := r.resolve(context.Background(), "graph-2", "Bob")
	require.NoError(t, err)
	assert.Equal(t, wantEmp, got.Account)
	assert.Equal(t, "new-uid", got.UserID, "created sender carries the generated UserID")
	assert.Equal(t, "Bob", got.DisplayName)
}

func TestSenderResolver_EmptyDisplayNameSkipsNameLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	wantEmp := teamsmigrate.EmployeeIDFromGraphID("graph-3")
	created := &model.User{ID: "nu", Account: wantEmp, SiteID: "s1"}
	// No FindUserByDisplayName call when displayName is empty; upsert then read back.
	gomock.InOrder(
		store.EXPECT().FindUserByEmployeeId(gomock.Any(), wantEmp).Return(nil, nil),
		store.EXPECT().UpsertUserIdentities(gomock.Any(), gomock.Any()).Return(nil),
		store.EXPECT().FindUserByEmployeeId(gomock.Any(), wantEmp).Return(created, nil),
	)

	r := newSenderResolver(store, "s1", testSenderCache(t))
	got, err := r.resolve(context.Background(), "graph-3", "")
	require.NoError(t, err)
	assert.Equal(t, wantEmp, got.Account)
	assert.Equal(t, "nu", got.UserID)
}

func TestSenderResolver_CacheHitSkipsStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockHRIdentityStore(ctrl)
	empID := teamsmigrate.EmployeeIDFromGraphID("graph-1")
	// Exactly one round of lookups for two resolves.
	store.EXPECT().FindUserByEmployeeId(gomock.Any(), empID).Return(&model.User{Account: "al"}, nil).Times(1)

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
