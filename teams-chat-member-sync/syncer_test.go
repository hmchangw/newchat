package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestAccountFromUPN(t *testing.T) {
	tests := []struct {
		name, upn, want string
	}{
		{"simple", "Alice@corp.example", "alice"},
		{"already lower", "bob@x.y", "bob"},
		{"empty", "", ""},
		{"no at", "noatsign", ""},
		{"leading at", "@corp.example", ""},
		{"dotted local", "a.b@corp.example", "a.b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, accountFromUPN(tc.upn))
		})
	}
}

func TestAccountCache_BatchesAndCachesHitsAndMisses(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	// First resolve: u1,u2 uncached -> one batched call. u2 unknown -> miss.
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			assert.ElementsMatch(t, []string{"u1", "u2"}, ids)
			return map[string]string{"u1": "alice"}, nil
		}).Times(1)

	c := newAccountCache(users)
	got, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": ""}, got, "miss cached as empty")

	// Second resolve of the same ids issues NO new query (mock capped at 1).
	got2, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": ""}, got2)
}

func TestAccountCache_OnlyQueriesUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	gomock.InOrder(
		users.EXPECT().AccountsByIDs(gomock.Any(), []string{"u1"}).Return(map[string]string{"u1": "alice"}, nil),
		users.EXPECT().AccountsByIDs(gomock.Any(), []string{"u2"}).Return(map[string]string{"u2": "bob"}, nil),
	)
	c := newAccountCache(users)
	_, err := c.resolve(context.Background(), []string{"u1"})
	require.NoError(t, err)
	// u1 now cached; only u2 is queried.
	got, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u1": "alice", "u2": "bob"}, got)
}

func TestAccountCache_ConcurrentResolveNoRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			out := make(map[string]string, len(ids))
			for _, id := range ids {
				out[id] = "acct-" + id
			}
			return out, nil
		}).AnyTimes()

	c := newAccountCache(users)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.resolve(context.Background(), []string{"u1", "u2", "u3"})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func newTestSyncer(t *testing.T, workers int) (*syncer, *MockTeamsChatStore, *MockTeamsUserStore, *MockmembersFetcher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	chats := NewMockTeamsChatStore(ctrl)
	users := NewMockTeamsUserStore(ctrl)
	graph := NewMockmembersFetcher(ctrl)
	s := newSyncer(chats, users, graph, syncConfig{MaxWorkers: workers, Now: func() time.Time {
		return time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	}})
	return s, chats, users, graph
}

func TestBuildMembers_UPNPresentNoLookup(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	// No AccountsByIDs call expected: every member has a UPN.
	_ = users
	raw := []msgraph.ChatMemberDetail{
		{UserID: "u1", UserPrincipalName: "Alice@corp.example", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{UserID: "u2", UserPrincipalName: "Bob@corp.example"},
	}
	got, err := s.buildMembers(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "u1", Account: "alice", VisibleHistoryStartDateTime: raw[0].VisibleHistoryStartDateTime},
		{ID: "u2", Account: "bob"},
	}, got)
}

func TestBuildMembers_MissingUPNFallsBackToLookup(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	// The two UPN-less members (u2, ghost) are resolved in one batched call.
	// ghost is not in teams_user, so it comes back absent -> account "".
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			assert.ElementsMatch(t, []string{"u2", "ghost"}, ids)
			return map[string]string{"u2": "bob"}, nil
		})
	raw := []msgraph.ChatMemberDetail{
		{UserID: "u1", UserPrincipalName: "alice@corp.example"}, // UPN present -> no lookup
		{UserID: "u2", UserPrincipalName: ""},                   // no UPN -> lookup hit
		{UserID: "ghost", UserPrincipalName: ""},                // no UPN, unknown -> ""
	}
	got, err := s.buildMembers(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "u1", Account: "alice"},
		{ID: "u2", Account: "bob"},
		{ID: "ghost", Account: ""},
	}, got)
}
