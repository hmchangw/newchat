package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1)).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat) error {
			assert.True(t, batch[0].UpdatedAt.Equal(wtNow), "chats carry the build-time UpdatedAt stamp")
			return nil
		})
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

func TestRun_SharedChatUpsertedByEachMember(t *testing.T) {
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
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat) error {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range batch {
				upserted = append(upserted, c.ID)
			}
			return nil
		}).Times(2) // each member persists the chat; the idempotent upsert makes redundancy safe
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
	assert.ElementsMatch(t, []string{"19:shared", "19:shared"}, upserted,
		"a chat shared by two users is persisted by each member, not claimed by one")
}

// TestRun_SharedChat_MemberFailureStillPersistsViaOther is the regression guard
// for the cross-worker-claim data loss: when one member's sync fails, a shared
// chat must still be persisted through another member that succeeds, rather than
// being skipped as "already claimed" by the failing worker.
func TestRun_SharedChat_MemberFailureStillPersistsViaOther(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1) // one worker => u1 processed before u2
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
		{ID: "u2", SiteID: "site-b", Account: "bob"},
	}, nil)
	shared := graphChat("19:shared", "u1", "u2")
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).Return([]msgraph.Chat{shared}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u2", wtDefaultFrom, wtTo).Return([]msgraph.Chat{shared}, nil)

	var mu sync.Mutex
	var upserted []string
	call := 0
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat) error {
			mu.Lock()
			defer mu.Unlock()
			call++
			if call == 1 { // u1's upsert fails
				return fmt.Errorf("mongo down")
			}
			for _, c := range batch { // u2 still persists the shared chat
				upserted = append(upserted, c.ID)
			}
			return nil
		}).Times(2)
	// u1 fails and holds its watermark; u2 succeeds and advances.
	users.EXPECT().SetFrom(gomock.Any(), "u2", wtTo).Return(nil)

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 users failed")
	assert.Equal(t, []string{"19:shared"}, upserted,
		"a failing member must not strand a shared chat claimed by no surviving writer")
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
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1)).Return(nil)
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
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any()).Return(fmt.Errorf("mongo down"))
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

func TestRun_EmptySiteIDVote_SkipsChatAndAdvancesWatermark(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	// The only member is unknown to the user cache, so voteSiteID returns "".
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:unknown", "unknown-user")}, nil)
	// The chat is skipped (empty batch, no UpsertChats), but the user still
	// succeeds and its watermark advances.
	_ = chats
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

func TestRun_EmptySiteIDVote_MixedWithGoodChat_UpsertsOnlyGood(t *testing.T) {
	s, users, chats, graph := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{
			graphChat("19:good", "u1"),
			graphChat("19:unknown", "unknown-user"),
		}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat) error {
			require.Len(t, batch, 1)
			assert.Equal(t, "19:good", batch[0].ID)
			return nil
		})
	// The good chat is upserted, the skipped chat is dropped, and the user
	// succeeds with its watermark advanced.
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

func TestRun_EmptySiteIDVote_ConfiguredDefaultIsUsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{
		MaxWorkers: 1, DefaultFrom: wtDefaultFrom, Now: fixedNow, DefaultSiteID: "site-default",
	})

	users.EXPECT().ListUsers(gomock.Any()).Return([]model.TeamsUser{
		{ID: "u1", SiteID: "site-a", Account: "alice"},
	}, nil)
	// The only member is unknown, but a default siteID is configured: the chat
	// is upserted with the default instead of being skipped, and the user succeeds.
	graph.EXPECT().ListUserChats(gomock.Any(), "u1", wtDefaultFrom, wtTo).
		Return([]msgraph.Chat{graphChat("19:unknown", "unknown-user")}, nil)
	chats.EXPECT().UpsertChats(gomock.Any(), gomock.Len(1)).DoAndReturn(
		func(_ context.Context, batch []model.TeamsChat) error {
			assert.Equal(t, "site-default", batch[0].SiteID)
			return nil
		})
	users.EXPECT().SetFrom(gomock.Any(), "u1", wtTo).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

// cancelTestUsers builds n teams_user docs with no watermark, so every one of
// them is eligible for dispatch.
func cancelTestUsers(n int) []model.TeamsUser {
	out := make([]model.TeamsUser, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, model.TeamsUser{ID: fmt.Sprintf("u%d", i), SiteID: "site-a"})
	}
	return out
}

// captureLogs redirects the default slog logger into a buffer for the duration
// of the test, restoring the previous logger on cleanup.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestRun_CancellationStopsDispatch is the regression guard for the SIGTERM
// error flood: when the run's context is cancelled (pod deletion, CronJob
// activeDeadlineSeconds), the dispatcher must stop feeding users instead of
// pushing every remaining one through a Graph call that fails instantly with
// `get chats: Get "<graph url>": context canceled`.
func TestRun_CancellationStopsDispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{
		MaxWorkers: 1, DefaultFrom: wtDefaultFrom, Now: fixedNow, DefaultSiteID: "site-default",
	})
	_ = chats

	const total = 50
	users.EXPECT().ListUsers(gomock.Any()).Return(cancelTestUsers(total), nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var graphCalls atomic.Int64
	graph.EXPECT().ListUserChats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(reqCtx context.Context, userID string, _, _ time.Time) ([]msgraph.Chat, error) {
			if graphCalls.Add(1) == 1 {
				cancel() // SIGTERM lands while the first user is in flight
			}
			// Mirrors what msgraph returns once the request context is cancelled:
			// net/http surfaces context.Cause verbatim inside a *url.Error.
			return nil, fmt.Errorf("get chats: Get %q: %w",
				"https://graph.microsoft.com/v1.0/users/"+userID+"/chats", reqCtx.Err())
		}).AnyTimes()

	err := s.run(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"a cancelled run must report cancellation, not an opaque N-of-M-users-failed")
	assert.LessOrEqual(t, graphCalls.Load(), int64(2),
		"dispatch must stop once the context is cancelled, not run every remaining user into a doomed Graph call")
}

// TestRun_CancellationDoesNotLogPerUserFailures pins the operator-facing
// symptom: one terminate signal must not produce one ERROR line per remaining
// user, each carrying an opaque Graph URL.
func TestRun_CancellationDoesNotLogPerUserFailures(t *testing.T) {
	logs := captureLogs(t)

	ctrl := gomock.NewController(t)
	users := NewMockTeamsUserStore(ctrl)
	chats := NewMockTeamsChatStore(ctrl)
	graph := NewMockchatsFetcher(ctrl)
	s := newSyncer(users, chats, graph, syncConfig{
		MaxWorkers: 1, DefaultFrom: wtDefaultFrom, Now: fixedNow, DefaultSiteID: "site-default",
	})
	_ = chats

	users.EXPECT().ListUsers(gomock.Any()).Return(cancelTestUsers(50), nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var graphCalls atomic.Int64
	graph.EXPECT().ListUserChats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(reqCtx context.Context, userID string, _, _ time.Time) ([]msgraph.Chat, error) {
			if graphCalls.Add(1) == 1 {
				cancel()
			}
			return nil, fmt.Errorf("get chats: Get %q: %w",
				"https://graph.microsoft.com/v1.0/users/"+userID+"/chats", reqCtx.Err())
		}).AnyTimes()

	require.Error(t, s.run(ctx))

	assert.NotContains(t, logs.String(), "user failed",
		"cancellation is not a per-user failure; it must not be logged as one")
	assert.NotContains(t, logs.String(), "graph.microsoft.com",
		"a cancelled run must not spray Graph URLs into the log")
}

func TestRun_ListUsersFailure(t *testing.T) {
	s, users, _, _ := newTestSyncer(t, 1)
	users.EXPECT().ListUsers(gomock.Any()).Return(nil, fmt.Errorf("mongo down"))
	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load teams users")
}
