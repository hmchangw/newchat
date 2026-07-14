package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

var (
	wtDefaultFrom = time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	wtNow         = time.Date(2026, 7, 14, 10, 30, 0, 0, time.UTC)
	wtTo          = time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC) // startOfDayUTC(wtNow)
)

func fixedNow() time.Time { return wtNow }

func newTestSyncer(t *testing.T, workers int) (*syncer, *MockTeamsUserStore, *MockTeamsChatStore, *MockchatsFetcher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{MaxWorkers: workers, DefaultFrom: wtDefaultFrom, Now: fixedNow})
	return s, users, chats, graph
}

func graphChat(id string, memberIDs ...string) msgraph.Chat {
	ms := make([]msgraph.ChatMember, 0, len(memberIDs))
	for _, m := range memberIDs {
		ms = append(ms, msgraph.ChatMember{UserID: m})
	}
	return msgraph.Chat{
		ID: id, ChatType: "group", Topic: "t-" + id,
		CreatedDateTime:     time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             ms,
	}
}

func TestRun_HappyPath_AdvancesWatermarkAndUsesDefaultFrom(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 2)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"}, // no From -> DefaultFrom
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:g1", "u1")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1), wtNow).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

func TestRun_ExistingFromIsUsed(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", from, wtTo).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	_ = chats // no chats -> no upsert call

	require.NoError(t, s.run(context.Background()))
}

func TestRun_SkipsUserWithEmptyWindow(t *testing.T) {
	s, users, _, _ := newTestSyncer(t, 1)
	from := wtTo // watermark already at startOfDay(now): nothing to fetch
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice", From: &from},
	}, nil)
	// No ListUserChats, no UpsertChats, no SetFrom expected.

	require.NoError(t, s.run(context.Background()))
}

func TestRun_SharedChatUpsertedOnce(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 4)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob"},
	}, nil)
	shared := graphChat("19:shared", "u1", "u2")
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return([]msgraph.Chat{shared}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtDefaultFrom, wtTo).Return([]msgraph.Chat{shared}, nil)

	var mu sync.Mutex
	var upserted []string
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any(), wtNow).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat, _ time.Time) error {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range batch {
				upserted = append(upserted, c.ID)
			}
			return nil
		}).MaxTimes(2) // the loser's batch may be empty and skip the call entirely
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
	assert.Equal(t, []string{"19:shared"}, upserted, "a chat shared by two users is upserted exactly once")
}

func TestRun_GraphFailureHoldsWatermarkAndFailsRun(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return(nil, fmt.Errorf("graph returned status 429"))
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:g2", "u2")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1), wtNow).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtTo).Return(nil)
	// No SetFrom for u1: its watermark must hold so next run retries the window.

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 users failed")
}

func TestRun_UpsertFailureHoldsWatermark(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:g1", "u1")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any(), wtNow).Return(fmt.Errorf("mongo down"))
	// No SetFrom expected.

	require.Error(t, s.run(context.Background()))
}

func TestRun_SetFromFailureFailsUser(t *testing.T) {
	s, users, _, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return(nil, nil)
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(fmt.Errorf("mongo down"))

	require.Error(t, s.run(context.Background()))
}

func TestRun_EmptySiteIDVote_SkipsChatAndFailsUser(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	// The only member is unknown to the user cache, so voteSiteID returns "".
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:unknown", "unknown-user")}, nil)
	// No UpsertChats, no SetFrom expected: the batch is empty and the run fails.
	_ = chats

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 1 users failed")
}

func TestRun_EmptySiteIDVote_MixedWithGoodChat_UpsertsOnlyGoodAndFailsUser(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{
			graphChat("19:good", "u1"),
			graphChat("19:unknown", "unknown-user"),
		}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any(), wtNow).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat, _ time.Time) error {
			require.Len(t, batch, 1)
			assert.Equal(t, "19:good", batch[0].ID)
			return nil
		})
	// No SetFrom expected: the skipped chat fails the user despite the good upsert.

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 1 users failed")
}

func TestRun_ListUsersFailure(t *testing.T) {
	s, users, _, _ := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return(nil, fmt.Errorf("mongo down"))
	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load teams users")
}
