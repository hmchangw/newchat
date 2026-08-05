package service_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
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
		SetPreviewMessage(gomock.Any(), "room-1", gomock.Any(), msgCreatedAt.UTC().UnixMilli()).
		DoAndReturn(func(_ interface{}, _ string, pvw models.PreviewMessage, _ int64) error {
			require.Equal(t, "m1", pvw.MessageID)
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
		SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
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
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "room-1", gomock.Any(), gomock.Any()).Return(nil).Times(1)

	resp1, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Equal(t, "m1", resp1.Rooms["room-1"].MessageID)

	resp2, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"room-1"}})
	require.NoError(t, err)
	require.Equal(t, "m1", resp2.Rooms["room-1"].MessageID)
}
