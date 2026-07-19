package service

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func ptrTime(ms int64) *time.Time { t := time.UnixMilli(ms).UTC(); return &t }

// newThreadUnreadService builds a UserService with only the deps GetThreadUnreadSummary
// needs; other deps are nil (unused by this handler).
func newThreadUnreadService(t *testing.T, ts *mocks.MockThreadSubscriptionRepository, rc *mocks.MockRoomClient) *UserService {
	t.Helper()
	return &UserService{threadSubs: ts, rooms: rc, siteID: "site-a"}
}

func TestGetThreadUnreadSummary_AggregatesAcrossSites(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "trA", SiteID: "site-a", RoomType: model.RoomTypeChannel, LastSeenAt: ptrTime(100)},     // local: lastMsg 200 > 100 → unread
		{ThreadRoomID: "trB", SiteID: "site-b", RoomType: model.RoomTypeDM, LastSeenAt: nil, HasMention: true}, // remote: never seen + mention + DM
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"trA"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "trA", Found: true, LastMsgAt: 200}}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-b", []string{"trB"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "trB", Found: true, LastMsgAt: 300}}, nil)

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.NoError(t, err)
	assert.True(t, resp.Unread)
	assert.True(t, resp.UnreadDirectMessage) // trB is DM + unread
	assert.True(t, resp.UnreadMention)       // trB hasMention
	require.NotNil(t, resp.LastMessageAt)
	assert.Equal(t, int64(300), *resp.LastMessageAt)
	assert.Empty(t, resp.UnavailableSites)
}

func TestGetThreadUnreadSummary_SiteFailureDegrades(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "trB", SiteID: "site-b", RoomType: model.RoomTypeChannel, LastSeenAt: nil},
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-b", []string{"trB"}).
		Return(nil, errors.New("boom"))

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.NoError(t, err)
	assert.False(t, resp.Unread)
	assert.Equal(t, []string{"site-b"}, resp.UnavailableSites)
}

func TestGetThreadUnreadSummary_MultiSiteMixedSuccessAndFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "trA", SiteID: "site-a", RoomType: model.RoomTypeChannel, LastSeenAt: ptrTime(100)},
		{ThreadRoomID: "trC", SiteID: "site-c", RoomType: model.RoomTypeChannel, LastSeenAt: nil},
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"trA"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "trA", Found: true, LastMsgAt: 200}}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-c", []string{"trC"}).
		Return(nil, errors.New("site-c unreachable"))

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.NoError(t, err)
	assert.True(t, resp.Unread)
	require.NotNil(t, resp.LastMessageAt)
	assert.Equal(t, int64(200), *resp.LastMessageAt)
	assert.Equal(t, []string{"site-c"}, resp.UnavailableSites)
}

// A single site with more than threadInfoBatchChunk threads is split into
// multiple GetThreadRoomInfoBatch calls whose results are merged into one badge.
func TestGetThreadUnreadSummary_MultiChunkSameSite(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	const total = threadInfoBatchChunk + 2 // spills into a second chunk
	rows := make([]model.ThreadUnreadRow, 0, total)
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("tr-%04d", i)
		ids = append(ids, id)
		// First thread is an unread DM (proves chunk 1 is processed); the thread that
		// lands in chunk 2 is an unread channel with the newest activity (proves chunk
		// 2 is processed and merged). All others are already-read, non-contributing.
		row := model.ThreadUnreadRow{ThreadRoomID: id, SiteID: "site-a", RoomType: model.RoomTypeChannel, LastSeenAt: ptrTime(1_000_000)}
		switch i {
		case 0:
			row.RoomType, row.LastSeenAt = model.RoomTypeDM, ptrTime(100)
		case threadInfoBatchChunk:
			row.LastSeenAt = nil
		}
		rows = append(rows, row)
	}
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(rows, nil)

	// Chunk boundaries mirror the handler's grouping order (ids appended in row order).
	chunk1, chunk2 := ids[:threadInfoBatchChunk], ids[threadInfoBatchChunk:]
	infos1 := make([]model.ThreadRoomInfo, len(chunk1))
	for i, id := range chunk1 {
		infos1[i] = model.ThreadRoomInfo{ThreadRoomID: id, Found: true, LastMsgAt: 1} // already read
	}
	infos1[0].LastMsgAt = 200 // tr-0000: 200 > 100 → unread DM
	infos2 := []model.ThreadRoomInfo{
		{ThreadRoomID: chunk2[0], Found: true, LastMsgAt: 999}, // never seen → unread, newest
		{ThreadRoomID: chunk2[1], Found: true, LastMsgAt: 1},
	}
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", chunk1).Return(infos1, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", chunk2).Return(infos2, nil)

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.NoError(t, err)
	assert.True(t, resp.Unread)
	assert.True(t, resp.UnreadDirectMessage) // tr-0000 (chunk 1) is an unread DM
	require.NotNil(t, resp.LastMessageAt)
	assert.Equal(t, int64(999), *resp.LastMessageAt) // newest activity lives in chunk 2
	assert.Empty(t, resp.UnavailableSites)
}

func TestGetThreadUnreadSummary_NoSubs(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, nil)
	// rc: no calls expected.

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.NoError(t, err)
	assert.False(t, resp.Unread)
	assert.Nil(t, resp.LastMessageAt)
	assert.Empty(t, resp.UnavailableSites)
}

func TestGetThreadUnreadSummary_NotFoundSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "gone", SiteID: "site-a", RoomType: model.RoomTypeChannel, LastSeenAt: nil},
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"gone"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "gone", Found: false}}, nil)

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.NoError(t, err)
	assert.False(t, resp.Unread)
	assert.Nil(t, resp.LastMessageAt)
}

func TestGetThreadUnreadSummary_ListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, errors.New("db down"))
	svc := newThreadUnreadService(t, ts, mocks.NewMockRoomClient(ctrl))
	_, err := svc.GetThreadUnreadSummary(ctx("alice", "site-a"), model.ThreadUnreadSummaryRequest{})
	require.Error(t, err)
}

func TestChunkStrings(t *testing.T) {
	tests := []struct {
		name     string
		ids      []string
		size     int
		expected [][]string
	}{
		{
			name:     "empty input",
			ids:      nil,
			size:     2,
			expected: [][]string{nil},
		},
		{
			name:     "fewer than size",
			ids:      []string{"a"},
			size:     2,
			expected: [][]string{{"a"}},
		},
		{
			name:     "exact multiple",
			ids:      []string{"a", "b", "c", "d"},
			size:     2,
			expected: [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:     "size+1 remainder",
			ids:      []string{"a", "b", "c", "d", "e"},
			size:     2,
			expected: [][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		},
		{
			name:     "non-positive size falls back to a single chunk",
			ids:      []string{"a", "b"},
			size:     0,
			expected: [][]string{{"a", "b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := chunkStrings(tt.ids, tt.size)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClearAllThreadUnread_MultiSite(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", SiteID: "site-a"},
		{ThreadRoomID: "tr2", SiteID: "site-b"},
		{ThreadRoomID: "tr3", SiteID: "site-b"},
	}, nil)
	rc.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-a", "alice").Return(1, nil)
	rc.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-b", "alice").Return(2, nil)

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.ClearAllThreadUnread(ctx("alice", "site-a"), model.ThreadReadAllRequest{})
	require.NoError(t, err)
	assert.Equal(t, 3, resp.ClearedThreads)
	assert.Empty(t, resp.UnavailableSites)
}

func TestClearAllThreadUnread_NoThreads(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, nil)
	// No ClearAllThreadUnread calls expected.

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.ClearAllThreadUnread(ctx("alice", "site-a"), model.ThreadReadAllRequest{})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.ClearedThreads)
	assert.Empty(t, resp.UnavailableSites)
}

func TestClearAllThreadUnread_SiteDegrades(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", SiteID: "site-a"},
		{ThreadRoomID: "tr2", SiteID: "site-b"},
	}, nil)
	rc.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-a", "alice").Return(1, nil)
	rc.EXPECT().ClearAllThreadUnread(gomock.Any(), "site-b", "alice").Return(0, fmt.Errorf("site down"))

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.ClearAllThreadUnread(ctx("alice", "site-a"), model.ThreadReadAllRequest{})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.ClearedThreads)
	assert.Equal(t, []string{"site-b"}, resp.UnavailableSites)
}

func TestClearAllThreadUnread_ListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, fmt.Errorf("mongo down"))

	svc := newThreadUnreadService(t, ts, rc)
	_, err := svc.ClearAllThreadUnread(ctx("alice", "site-a"), model.ThreadReadAllRequest{})
	require.Error(t, err)
}
