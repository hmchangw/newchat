package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestUserRefCache_BatchesAndCachesHitsAndMisses(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	// First resolve: u1,u2 uncached -> one batched call. u2 unknown -> miss.
	users.EXPECT().UsersByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]teamsUserRef, error) {
			assert.ElementsMatch(t, []string{"u1", "u2"}, ids)
			return map[string]teamsUserRef{"u1": {account: "alice", displayName: "Alice Smith"}}, nil
		}).Times(1)

	c := newUserRefCache(users)
	want := map[string]teamsUserRef{"u1": {account: "alice", displayName: "Alice Smith"}, "u2": {}}
	got, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, want, got, "miss cached as the zero ref")

	// Second resolve of the same ids issues NO new query (mock capped at 1).
	got2, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, want, got2)
}

func TestUserRefCache_OnlyQueriesUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	gomock.InOrder(
		users.EXPECT().UsersByIDs(gomock.Any(), []string{"u1"}).
			Return(map[string]teamsUserRef{"u1": {account: "alice", displayName: "Alice Smith"}}, nil),
		users.EXPECT().UsersByIDs(gomock.Any(), []string{"u2"}).
			Return(map[string]teamsUserRef{"u2": {account: "bob", displayName: "Bob Jones"}}, nil),
	)
	c := newUserRefCache(users)
	_, err := c.resolve(context.Background(), []string{"u1"})
	require.NoError(t, err)
	// u1 now cached; only u2 is queried.
	got, err := c.resolve(context.Background(), []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]teamsUserRef{
		"u1": {account: "alice", displayName: "Alice Smith"},
		"u2": {account: "bob", displayName: "Bob Jones"},
	}, got)
}

func TestUserRefCache_ConcurrentResolveNoRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	users.EXPECT().UsersByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]teamsUserRef, error) {
			out := make(map[string]teamsUserRef, len(ids))
			for _, id := range ids {
				out[id] = teamsUserRef{account: "acct-" + id, displayName: "name-" + id}
			}
			return out, nil
		}).AnyTimes()

	c := newUserRefCache(users)
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

func TestBuildMembers_ResolvesAllViaLookup(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	// Every member is resolved from teams_user by userId in one batched call.
	users.EXPECT().UsersByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]teamsUserRef, error) {
			assert.ElementsMatch(t, []string{"u1", "u2"}, ids)
			return map[string]teamsUserRef{
				"u1": {account: "alice", displayName: "Alice Smith"},
				"u2": {account: "bob", displayName: "Bob Jones"},
			}, nil
		})
	raw := []msgraph.ChatMemberDetail{
		{UserID: "u1", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{UserID: "u2"},
	}
	got, err := s.buildMembers(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "u1", Account: "alice", DisplayName: "Alice Smith", VisibleHistoryStartDateTime: raw[0].VisibleHistoryStartDateTime},
		{ID: "u2", Account: "bob", DisplayName: "Bob Jones"},
	}, got)
}

func TestBuildMembers_ResolveFailureIsWrapped(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	sentinel := errors.New("mongo down")
	users.EXPECT().UsersByIDs(gomock.Any(), gomock.Any()).Return(nil, sentinel)

	_, err := s.buildMembers(context.Background(), []msgraph.ChatMemberDetail{{UserID: "u1"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel, "the underlying failure stays matchable")
	assert.Contains(t, err.Error(), "resolve member identity", "buildMembers names what it was doing")
}

func TestBuildMembers_UnknownUserHasEmptyAccountAndDisplayName(t *testing.T) {
	s, _, users, _ := newTestSyncer(t, 1)
	// ghost is not in teams_user, so it comes back absent from the lookup.
	users.EXPECT().UsersByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]teamsUserRef, error) {
			assert.ElementsMatch(t, []string{"u1", "ghost"}, ids, "an unresolvable id is still looked up")
			return map[string]teamsUserRef{"u1": {account: "alice", displayName: "Alice Smith"}}, nil
		})

	got, err := s.buildMembers(context.Background(), []msgraph.ChatMemberDetail{{UserID: "u1"}, {UserID: "ghost"}})
	require.NoError(t, err)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "u1", Account: "alice", DisplayName: "Alice Smith"},
		{ID: "ghost", Account: "", DisplayName: ""},
	}, got)
}
