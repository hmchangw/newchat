package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
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

// discardLogger keeps Syncer log output out of test noise.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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
	store.EXPECT().HRUsers(gomock.Any(), []string{"alice"}).
		Return(map[string]hrUser{"alice": {LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"}}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "Alice@corp.example", Account: "alice", SiteID: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
	}).Return(nil)
	// page 2: u3 new
	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u3"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRUsers(gomock.Any(), []string{"carol"}).
		Return(map[string]hrUser{"carol": {LocationURL: "https://site-b.mysite.com", EngName: "Carol Jones", Mail: "carol@corp.example"}}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u3", UPN: "carol@corp.example", Account: "carol", SiteID: "https://site-b.mysite.com", EngName: "Carol Jones", Mail: "carol@corp.example"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500, discardLogger())
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
	// no HRUsers, no UpsertTeamsUsers

	syncer := NewSyncer(store, lister, 500, discardLogger())
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
	store.EXPECT().HRUsers(gomock.Any(), []string{"guest#ext#", "dave"}).
		Return(map[string]hrUser{"dave": {LocationURL: "https://site-a.mysite.com", EngName: "Dave Lee", Mail: "dave@corp.example"}}, nil) // guest has no hr row
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "guest#EXT#@other.example", Account: "guest#ext#"},
		{ID: "u3", UPN: "Dave@CORP.EXAMPLE", Account: "dave", SiteID: "https://site-a.mysite.com", EngName: "Dave Lee", Mail: "dave@corp.example"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500, discardLogger())
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 3, InvalidUPN: 1, HRUnmatched: 1, Upserted: 2}, stats)
}

func TestSyncer_UpdateUsers_HRMissUpsertedAndCounted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "alice@corp.example"},
		{ID: "u2", UserPrincipalName: "eve@corp.example"},
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1", "u2"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRUsers(gomock.Any(), []string{"alice", "eve"}).
		Return(map[string]hrUser{"alice": {LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"}}, nil) // eve unmatched
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
		{ID: "u2", UPN: "eve@corp.example", Account: "eve"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500, discardLogger())
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 2, HRUnmatched: 1, Upserted: 2}, stats)
}

func TestSyncer_UpdateUsers_LocationURLVariants(t *testing.T) {
	tests := []struct {
		name string
		hr   hrUser
		want model.TeamsUser
	}{
		{
			// TODO: siteID is the raw locationURL until the real parser lands.
			"non-empty locationURL passes through as siteID",
			hrUser{LocationURL: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
			model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", SiteID: "https://site-a.mysite.com", EngName: "Alice Smith", Mail: "alice@corp.example"},
		},
		{
			"empty locationURL keeps empty siteID",
			hrUser{EngName: "Alice Smith", Mail: "alice@corp.example"},
			model.TeamsUser{ID: "u1", UPN: "alice@corp.example", Account: "alice", EngName: "Alice Smith", Mail: "alice@corp.example"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			lister := &fakeLister{pages: [][]msgraph.GraphUser{{
				{ID: "u1", UserPrincipalName: "alice@corp.example"},
			}}}

			store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
				Return(map[string]struct{}{}, nil)
			store.EXPECT().HRUsers(gomock.Any(), []string{"alice"}).
				Return(map[string]hrUser{"alice": tt.hr}, nil)
			store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{tt.want}).Return(nil)

			syncer := NewSyncer(store, lister, 500, discardLogger())
			stats, err := syncer.UpdateUsers(context.Background())
			require.NoError(t, err)
			assert.Equal(t, RunStats{Pages: 1, Seen: 1, Upserted: 1}, stats)
		})
	}
}

func TestSyncer_UpdateUsers_AllHRMissStillUpserts(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	lister := &fakeLister{pages: [][]msgraph.GraphUser{{
		{ID: "u1", UserPrincipalName: "eve@corp.example"},
	}}}

	store.EXPECT().ExistingIDs(gomock.Any(), []string{"u1"}).
		Return(map[string]struct{}{}, nil)
	store.EXPECT().HRUsers(gomock.Any(), []string{"eve"}).
		Return(map[string]hrUser{}, nil)
	store.EXPECT().UpsertTeamsUsers(gomock.Any(), []model.TeamsUser{
		{ID: "u1", UPN: "eve@corp.example", Account: "eve"},
	}).Return(nil)

	syncer := NewSyncer(store, lister, 500, discardLogger())
	stats, err := syncer.UpdateUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, RunStats{Pages: 1, Seen: 1, HRUnmatched: 1, Upserted: 1}, stats)
}

func TestSyncer_UpdateUsers_EmptyPageAndEmptyTenant(t *testing.T) {
	t.Run("empty page makes no store calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{pages: [][]msgraph.GraphUser{{}}}

		syncer := NewSyncer(store, lister, 500, discardLogger())
		stats, err := syncer.UpdateUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, RunStats{Pages: 1}, stats)
	})
	t.Run("no pages at all", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		lister := &fakeLister{}

		syncer := NewSyncer(store, lister, 500, discardLogger())
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

		syncer := NewSyncer(store, lister, 500, discardLogger())
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "graph down")
	})
	t.Run("ExistingIDs error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("read down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, 500, discardLogger())
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "read down")
	})
	t.Run("HRUsers error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(map[string]struct{}{}, nil)
		store.EXPECT().HRUsers(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("hr down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, 500, discardLogger())
		_, err := syncer.UpdateUsers(context.Background())
		require.ErrorContains(t, err, "hr down")
	})
	t.Run("Upsert error aborts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().ExistingIDs(gomock.Any(), gomock.Any()).
			Return(map[string]struct{}{}, nil)
		store.EXPECT().HRUsers(gomock.Any(), gomock.Any()).
			Return(map[string]hrUser{"alice": {LocationURL: "https://site-a.mysite.com"}}, nil)
		store.EXPECT().UpsertTeamsUsers(gomock.Any(), gomock.Any()).
			Return(errors.New("write down"))

		syncer := NewSyncer(store, &fakeLister{pages: page}, 500, discardLogger())
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

func TestExtractSiteIDFromLocationURL(t *testing.T) {
	// TODO: tighten these expectations once real locationURL parsing lands;
	// for now the locationURL is returned unchanged.
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"url returned unchanged", "https://site-a.mysite.com", "https://site-a.mysite.com"},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, extractSiteIDFromLocationURL(tt.url))
		})
	}
}
