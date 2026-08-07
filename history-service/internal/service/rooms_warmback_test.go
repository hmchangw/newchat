package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	readcache "github.com/hmchangw/chat/history-service/internal/readcache"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
)

// newWarmBackTestService builds a service with bare mocks and installs NO default
// SetPreviewMessage stub — unlike newRoomsService, so each test below gets full,
// unshadowed control over that mock's EXPECT()s (gomock matches the first
// registered expectation for a method, so a shared default would always win over
// a specific one registered afterward inside the test body).
func newWarmBackTestService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
	return svc, msgs, rooms
}

// newWarmBackTestServiceWithPreviewCache is newWarmBackTestService plus a real
// installed PreviewCache, for the cache-hit-skips-warm-back scenario.
func newWarmBackTestServiceWithPreviewCache(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
	pc, err := readcache.NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, service.WithPreviewCache(pc))
	return svc, msgs, rooms
}

// A resolvePreview walk that finds a preview warm-backs it onto the room doc,
// asOf == the resolved preview's own CreatedAt millis. The read itself still
// succeeds and returns the resolved preview regardless of the warm-back outcome.
func TestHistoryService_RoomsGet_WarmsBackResolvedPreview(t *testing.T) {
	svc, msgs, rooms := newWarmBackTestService(t)

	msgCreatedAt := roomLastMsgAt
	stubRoomTimes(rooms, []string{"room-1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m1", RoomID: "room-1", Msg: "hi", CreatedAt: msgCreatedAt, Sender: models.Participant{Account: "alice"}},
		}, false), nil)
	rooms.EXPECT().
		SetPreviewMessage(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), msgCreatedAt.UTC().UnixMilli()).
		DoAndReturn(func(_ context.Context, _ string, pvw models.PreviewMessage, forMsgID string, _ int64) error {
			require.Equal(t, "m1", pvw.MessageID)
			// Newest observed == the preview itself when the newest message is eligible.
			require.Equal(t, "m1", forMsgID)
			return nil
		}).
		Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "room-1")
	require.Equal(t, "m1", resp.Rooms["room-1"].MessageID)
}

// A warm-back failure is best-effort: the read must still succeed and still
// return the resolved preview.
func TestHistoryService_RoomsGet_WarmBackFailureDoesNotFailRead(t *testing.T) {
	svc, msgs, rooms := newWarmBackTestService(t)

	stubRoomTimes(rooms, []string{"room-1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m1", RoomID: "room-1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}},
		}, false), nil)
	rooms.EXPECT().
		SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("mongo down")).
		Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "room-1")
	require.Equal(t, "m1", resp.Rooms["room-1"].MessageID)
}

// A preview served from a warm cache hit must not re-run the walk-and-warm-back
// loader: no SetPreviewMessage call on the second (cache-hit) resolve.
func TestHistoryService_RoomsGet_CacheHitSkipsWarmBack(t *testing.T) {
	svc, msgs, rooms := newWarmBackTestServiceWithPreviewCache(t)

	stubRoomTimesN(rooms, []string{"room-1"}, roomLastMsgAt, roomCreatedAt, 2)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m1", RoomID: "room-1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}},
		}, false), nil).Times(1)
	// Exactly one warm-back call: the priming (cache-miss) resolve. A second call
	// here would mean the cache hit re-ran the loader — exhausting this Times(1)
	// expectation fails the test via gomock's "unexpected call" fatal.
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	resp1, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Equal(t, "m1", resp1.Rooms["room-1"].MessageID)

	resp2, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Equal(t, "m1", resp2.Rooms["room-1"].MessageID)
}

// The freshness key is the newest message the walk OBSERVED, which is NOT the
// preview's own id whenever the newest message is ineligible and gets skipped —
// the ordinary case for a room whose last activity was a system message. Keying
// on the preview's id instead would leave the memo permanently "stale" and make
// every read re-walk.
func TestHistoryService_RoomsGet_WarmBackKeysOnNewestObservedMessage(t *testing.T) {
	svc, msgs, rooms := newWarmBackTestService(t)

	stubRoomTimes(rooms, []string{"room-1"}, roomLastMsgAt, roomCreatedAt)
	// Newest row is a deleted message (skipped); the eligible preview is older.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "newest", RoomID: "room-1", Deleted: true, CreatedAt: roomLastMsgAt},
			{MessageID: "older", RoomID: "room-1", Msg: "hi", CreatedAt: roomLastMsgAt.Add(-time.Minute), Sender: models.Participant{Account: "alice"}},
		}, false), nil)
	rooms.EXPECT().
		SetPreviewMessage(gomock.Any(), "room-1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, pvw models.PreviewMessage, forMsgID string, _ int64) error {
			require.Equal(t, "older", pvw.MessageID, "preview is the newest ELIGIBLE message")
			require.Equal(t, "newest", forMsgID, "freshness key is the newest OBSERVED message")
			return nil
		}).
		Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Equal(t, "older", resp.Rooms["room-1"].MessageID)
}

// A stored preview that the repo reports as current short-circuits the walk
// entirely: no Cassandra read, no re-write. This is the whole point of the memo.
func TestHistoryService_RoomsGet_FreshStoredPreviewSkipsWalk(t *testing.T) {
	svc, msgs, rooms := newWarmBackTestService(t)

	stored := models.PreviewMessage{
		MessageID: "m1",
		Content:   "from the doc",
		CreatedAt: roomLastMsgAt,
	}
	rooms.EXPECT().
		GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{
			"room-1": {LastMsgAt: roomLastMsgAt, CreatedAt: roomCreatedAt, Preview: &stored},
		}, nil).
		Times(1)
	// Strict: any Cassandra walk or preview write fails the test.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Equal(t, "from the doc", resp.Rooms["room-1"].Content)
}
