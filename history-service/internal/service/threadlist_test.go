package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
)

// decodeThreadMsg decodes an opaque ThreadListItem body back into a message for
// assertion; the RPC carries these bodies as pre-marshaled json.RawMessage.
func decodeThreadMsg(t *testing.T, raw json.RawMessage) models.Message {
	t.Helper()
	var m models.Message
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func newThreadListService(t *testing.T) (
	*service.HistoryService,
	*mocks.MockMessageRepository,
	*mocks.MockSubscriptionRepository,
	*mocks.MockRoomRepository,
	*mocks.MockThreadSubscriptionRepository,
) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	cfg := &config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)
	return svc, msgs, subs, rooms, threadSubs
}

func TestHistoryService_ListThreadSubscriptions_Success(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", RoomName: "general", RoomType: pkgmodel.RoomTypeChannel, ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour), LastSeenAt: ptrTime(base.Add(2 * time.Hour)), HasMention: true},
		{ThreadRoomID: "tr-2", RoomID: "r1", SiteID: "site-a", RoomName: "general", RoomType: pkgmodel.RoomTypeChannel, ParentMessageID: "p2", LastMsgID: "m2", LastMsgAt: base.Add(3 * time.Hour), LastSeenAt: nil},
	}
	// Single keyset fetch; hasMore rides through from the repository.
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, true, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent 1", TCount: intPtr(4)},
		{MessageID: "m1", RoomID: "r1", Msg: "last 1"},
		{MessageID: "p2", RoomID: "r1", Msg: "parent 2"},
		{MessageID: "m2", RoomID: "r1", Msg: "last 2"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 2})
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)
	assert.True(t, resp.HasMore)

	first := resp.Items[0]
	assert.Equal(t, "tr-1", first.ThreadRoomID)
	assert.Equal(t, "general", first.RoomName)
	assert.Equal(t, pkgmodel.RoomTypeChannel, first.RoomType)
	assert.Equal(t, "site-a", first.SiteID)
	assert.True(t, first.HasMention)
	assert.Equal(t, base.Add(5*time.Hour).UnixMilli(), first.LastMsgAt)
	require.NotNil(t, first.LastSeenAt)
	assert.Equal(t, base.Add(2*time.Hour).UnixMilli(), *first.LastSeenAt)
	require.NotNil(t, first.ParentMessage)
	parent := decodeThreadMsg(t, first.ParentMessage)
	assert.Equal(t, "p1", parent.MessageID)
	require.NotNil(t, parent.TCount)
	assert.Equal(t, 4, *parent.TCount) // reply count rides on the parent
	require.NotNil(t, first.LastMessage)
	assert.Equal(t, "m1", decodeThreadMsg(t, first.LastMessage).MessageID)
	assert.True(t, first.Unread) // lastMsgAt 5h > lastSeenAt 2h

	second := resp.Items[1]
	assert.Nil(t, second.LastSeenAt)
	assert.True(t, second.Unread) // never-seen ⇒ unread
}

// A parent carrying reactions still builds and its body rides through as the
// grouped-by-emoji wire form (guards the user-service forward path).
func TestHistoryService_ListThreadSubscriptions_ParentWithReactions(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent", Reactions: models.Reactions{
			{Emoji: "👍", UserAccount: "bob"}: {Account: "bob", EngName: "Bob Chen", ReactedAt: base},
		}},
		{MessageID: "m1", RoomID: "r1", Msg: "last"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	// The body is carried opaque; a reactions-bearing body cannot be decoded back
	// into a Message (that is exactly the parse this change avoids), so assert on
	// the raw wire JSON only — messageId plus the grouped-by-emoji reactions form.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(resp.Items[0].ParentMessage, &raw))
	assert.Equal(t, "p1", raw["messageId"])
	reactions, ok := raw["reactions"].(map[string]any)
	require.True(t, ok, "reactions must be present in the built body")
	assert.Contains(t, reactions, "👍")
}

func TestHistoryService_ListThreadSubscriptions_Empty(t *testing.T) {
	svc, _, _, _, threadSubs := newThreadListService(t)
	// No rows ⇒ no message/room lookups at all.
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice"})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
	assert.NotNil(t, resp.Items) // never nil — JSON [] not null
	assert.False(t, resp.HasMore)
}

func TestHistoryService_ListThreadSubscriptions_MissingAccount(t *testing.T) {
	svc, _, _, _, _ := newThreadListService(t)
	_, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Limit: 10})
	require.Error(t, err)
	ec := errcode.Classify(context.Background(), err)
	require.NotNil(t, ec)
	assert.Equal(t, errcode.CodeBadRequest, ec.Code)
}

// The handler does no per-room access lookup: room membership is enforced inside
// the aggregation pipeline (the subscriptions $lookup — see mongorepo), so the
// handler simply returns whatever rows the repository yields. In particular it
// must not call GetHistorySharedSince — the subs mock has no expectations, so any
// such call fails the test.
func TestHistoryService_ListThreadSubscriptions_ReturnsAllRowsNoAccessCheck(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
		{ThreadRoomID: "tr-2", RoomID: "r2", SiteID: "site-a", ParentMessageID: "p2", LastMsgID: "m2", LastMsgAt: base.Add(3 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1"}, {MessageID: "m1", RoomID: "r1"},
		{MessageID: "p2", RoomID: "r2"}, {MessageID: "m2", RoomID: "r2"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)
	assert.Equal(t, "tr-1", resp.Items[0].ThreadRoomID)
	assert.Equal(t, "tr-2", resp.Items[1].ThreadRoomID)
}

// A thread whose parent is old (or whose parent was deleted and is absent from
// hydration) is still returned — there is no access window to filter against.
// A missing parent simply yields a nil ParentMessage.
// A thread whose parent or last message can't be hydrated (hard-deleted, or not
// yet replicated) is dropped rather than surfaced as a half-empty item.
func TestHistoryService_ListThreadSubscriptions_DropsUnhydratableThreads(t *testing.T) {
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		returned []models.Message // what Cassandra hydration yields
	}{
		{
			name:     "parent missing",
			returned: []models.Message{{MessageID: "m1", RoomID: "r1", CreatedAt: base.Add(5 * time.Hour)}},
		},
		{
			name:     "last message missing",
			returned: []models.Message{{MessageID: "p1", RoomID: "r1", CreatedAt: base.Add(5 * time.Hour)}},
		},
		{
			name:     "both missing",
			returned: []models.Message{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, msgs, _, _, threadSubs := newThreadListService(t)
			rows := []mongorepo.ThreadSubRow{
				{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
			}
			threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
			msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(tt.returned, nil)

			resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
			require.NoError(t, err)
			assert.Empty(t, resp.Items)
		})
	}
}

// A fully-hydratable thread is kept even when its room doc is missing — only the
// message bodies gate inclusion, not the room name/type enrichment.
func TestHistoryService_ListThreadSubscriptions_KeepsHydratableThread(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1"}, {MessageID: "m1", RoomID: "r1"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "tr-1", resp.Items[0].ThreadRoomID)
	require.NotNil(t, resp.Items[0].ParentMessage)
	require.NotNil(t, resp.Items[0].LastMessage)
}

func TestHistoryService_ListThreadSubscriptions_MissingRoomMetaDegrades(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	// Row with no room name/type — the pipeline's rooms $lookup found no doc.
	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(1 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1"}, {MessageID: "m1", RoomID: "r1"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice"})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Empty(t, resp.Items[0].RoomName)
	assert.Empty(t, string(resp.Items[0].RoomType))
}

func TestHistoryService_ListThreadSubscriptions_RepoError(t *testing.T) {
	svc, _, _, _, threadSubs := newThreadListService(t)
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false, errors.New("mongo down"))

	_, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice"})
	require.Error(t, err)
	ec := errcode.Classify(context.Background(), err)
	require.NotNil(t, ec)
	assert.Equal(t, errcode.CodeInternal, ec.Code)
}

func TestHistoryService_ListThreadSubscriptions_HydrationError(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(1 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(nil, errors.New("cassandra timeout"))

	_, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice"})
	require.Error(t, err)
	ec := errcode.Classify(context.Background(), err)
	require.NotNil(t, ec)
	assert.Equal(t, errcode.CodeInternal, ec.Code)
}

func TestHistoryService_ListThreadSubscriptions_LimitClamp(t *testing.T) {
	svc, _, _, _, threadSubs := newThreadListService(t)
	var gotLimit int
	threadSubs.EXPECT().
		ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ *time.Time, _ string, limit int) ([]mongorepo.ThreadSubRow, bool, error) {
			gotLimit = limit
			return nil, false, nil
		})

	_, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 9999})
	require.NoError(t, err)
	assert.Equal(t, 100, gotLimit) // clamped to maxPageSize
}

func TestHistoryService_ListThreadSubscriptions_CursorConverted(t *testing.T) {
	svc, _, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 5, 0, 0, 0, time.UTC)
	var gotTs *time.Time
	var gotID string
	threadSubs.EXPECT().
		ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ts *time.Time, id string, _ int) ([]mongorepo.ThreadSubRow, bool, error) {
			gotTs, gotID = ts, id
			return nil, false, nil
		})

	ms := base.UnixMilli()
	_, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{
		Account: "alice", CursorLastMsgAt: &ms, CursorThreadRoomID: "tr-9",
	})
	require.NoError(t, err)
	require.NotNil(t, gotTs)
	assert.Equal(t, base.UnixMilli(), gotTs.UnixMilli())
	assert.Equal(t, "tr-9", gotID)
}
