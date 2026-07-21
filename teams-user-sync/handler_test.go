package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// fakeLister feeds canned pages to UpdateUsers without a real Graph client.
type fakeLister struct {
	pages [][]msgraph.GraphUser
	err   error // returned after all pages are delivered
}

func (f *fakeLister) ListUsers(_ context.Context, _ int, fn func([]msgraph.GraphUser) error) error {
	for _, p := range f.pages {
		if err := fn(p); err != nil {
			return err
		}
	}
	return f.err
}

func TestSyncer_UpdateUsers_HappyPathTwoPages(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{
		{
			{ID: "u1", UserPrincipalName: "Alice@corp.example"},
			{ID: "u2", UserPrincipalName: "bob@corp.example"},
		},
		{
			{ID: "u3", UserPrincipalName: "carol@corp.example"},
		},
	}}

	// page 1: u1 new, u2 existing
	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2"}).
		Return(map[string]struct{}{"u2": {}}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"alice"}).
		Return(map[string]string{"alice": "site-a"}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "site-a"},
	}).Return(nil)
	// page 2: u3 new
	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u3"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"carol"}).
		Return(map[string]string{"carol": "site-b"}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u3", UPN: "carol@corp.example", Account: "carol", SiteID: "site-b"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 2, Seen: 3, Existing: 1, Upserted: 2}, stats)
}

func TestSyncer_UpdateUsers_AllExistingSkipsLookupAndWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{
		{{ID: "u1", UserPrincipalName: "alice@corp.example"}},
	}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
		Return(map[string]struct{}{"u1": {}}, nil)
	// no HRSiteIDs, no UpsertTeamsUsers

	syncer := NewSyncer(store, lister, 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 1, Existing: 1}, stats)
}

func TestSyncer_UpdateUsers_SkipsMalformedUPN(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "guest#EXT#@other.example"}, // any domain syncs
		{ID: "u2", UserPrincipalName: "no-at-sign"},               // malformed
		{ID: "u3", UserPrincipalName: "Dave@CORP.EXAMPLE"},        // local part lowered
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2", "u3"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"guest#ext#", "dave"}).
		Return(map[string]string{"dave": "site-a"}, nil) // guest has no hr row
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u3", UPN: "Dave@CORP.EXAMPLE", Account: "dave", SiteID: "site-a"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 3, InvalidUPN: 1, HRUnmatched: 1, Upserted: 1}, stats)
}

func TestSyncer_UpdateUsers_HRMissSkippedAndCounted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "alice@corp.example"},
		{ID: "u2", UserPrincipalName: "eve@corp.example"},
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"alice", "eve"}).
		Return(map[string]string{"alice": "site-a"}, nil) // eve unmatched
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "site-a"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 2, HRUnmatched: 1, Upserted: 1}, stats)
}

func TestSyncer_UpdateUsers_AllHRMissSkipsWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "eve@corp.example"},
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRSiteIDs(gomock.Any(), []string{"eve"}).
		Return(map[string]string{}, nil)
	// no UpsertTeamsUsers

	syncer := NewSyncer(store, lister, 500)
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 1, HRUnmatched: 1}, stats)
}

func TestSyncer_UpdateUsers_EmptyPageAndEmptyTenant(t *testing.T) {
	t.Run("empty page makes no store calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{pages: [][]msgraph.GraphUser{{}}}

		syncer := NewSyncer(store, lister, 500)
		stats, err := syncer.UpdateUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, RunStats{Pages: 1}, stats)
	})
	t.Run("no pages at all", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{}

		syncer := NewSyncer(store, lister, 500)
		stats, err := syncer.UpdateUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, RunStats{}, stats)
	})
}

func TestSyncer_UpdateUsers_ErrorPaths(t *testing.T) {
	page := [][]msgraph.GraphUser{{{ID: "u1", UserPrincipalName: "alice@corp.example"}}}

	t.Run("graph error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{err: errors.New("graph down")}

		syncer := NewSyncer(store, lister, 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "graph down")
	})
	t.Run("ExistingIDs error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("read down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "read down")
	})
	t.Run("HRSiteIDs error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(map[string]struct{}{}, nil)
		store.EXPECT().HRSiteIDs(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("hr down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "hr down")
	})
	t.Run("Upsert error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(map[string]struct{}{}, nil)
		store.EXPECT().HRSiteIDs(gomock.Any(), gomock.Any()).
			Return(map[string]string{"alice": "site-a"}, nil)
		store.EXPECT().UpsertTeamsUsers(gomock.Any(), gomock.Any()).
			Return(errors.New("write down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, 500)
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "write down")
	})
}

func TestSplitUPN(t *testing.T) {
	tests := []struct {
		name        string
		upn         string
		wantAccount string
		wantOK      bool
	}{
		{"simple", "alice@corp.example", "alice", true},
		{"uppercase local lowered", "Alice.Smith@corp.example", "alice.smith", true},
		{"guest ext", "guest#EXT#@other.example", "guest#ext#", true},
		{"no at sign", "alice", "", false},
		{"leading at", "@corp.example", "", false},
		{"trailing at accepted", "alice@", "alice", true},
		{"empty", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, ok := splitUPN(tt.upn)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantAccount, account)
		})
	}
}
