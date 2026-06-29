package service_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// newRoomsService builds a service with bare mocks (no permissive room-time
// default) so each rooms.get test states its exact per-room expectations.
func newRoomsService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockRoomRepository) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	customEmojis := mocks.NewMockCustomEmojiStore(ctrl)
	cfg := &config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, customEmojis, cfg)
	return svc, msgs, subs, rooms
}

func roomsCtx() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "alice"})
}

var roomLastMsgAt = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
var roomCreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Mirror the production caps (house pattern — see maxGetByIDsBatchSize in messages_test).
const (
	roomsGetMaxBatch     = 100
	roomsGetPreviewRunes = 256
)

func TestHistoryService_RoomsGet_FullAccess(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)

	// Full access (nil historySharedSince) ⇒ the unbounded GetMessagesBefore walk.
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hello", CreatedAt: roomLastMsgAt, Sender: models.Participant{ID: "u1", Account: "alice"}}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	lm := resp.Rooms["r1"]
	assert.Equal(t, "m1", lm.MessageID)
	assert.Equal(t, "hello", lm.Content)
	assert.Equal(t, "alice", lm.Sender.Account)
	assert.Equal(t, roomLastMsgAt.UnixMilli(), lm.CreatedAt)
	assert.False(t, lm.Deleted)
}

func TestHistoryService_RoomsGet_WindowedAccess(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)
	since := roomCreatedAt.Add(24 * time.Hour)

	// Windowed access (non-nil historySharedSince) ⇒ GetMessagesBetweenDesc, floored at the join window.
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(&since, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", since, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m2", resp.Rooms["r1"].MessageID)
}

func TestHistoryService_RoomsGet_EmptyRoomOmitted(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)

	// Empty room: GetRoomTimes yields zero lastMsgAt (normalised to createdAt), the
	// walk returns no rows, so the room is omitted from the response.
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(time.Time{}, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	assert.NotNil(t, resp.Rooms)
}

func TestHistoryService_RoomsGet_NotSubscribedDegrades(t *testing.T) {
	svc, _, subs, rooms := newRoomsService(t)

	// Not subscribed ⇒ getAccessSince Forbidden ⇒ that room degrades to no entry.
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, false, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil).AnyTimes()

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
}

func TestHistoryService_RoomsGet_PerRoomDegradeKeepsSiblings(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)

	// r1 history read errors → omitted; r2 succeeds → returned.
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), errors.New("cassandra timeout"))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r2").Return(nil, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r2").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r2", Msg: "ok", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	require.Contains(t, resp.Rooms, "r2")
}

func TestHistoryService_RoomsGet_DeletedLastReturnedAsIs(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m9", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.True(t, resp.Rooms["r1"].Deleted)
}

func TestHistoryService_RoomsGet_DedupsRoomIDs(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)

	// A duplicate roomId resolves once (Times(1) on each per-room read).
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, true, nil).Times(1)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil).Times(1)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "x", CreatedAt: roomLastMsgAt}}, false), nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r1"}})
	require.NoError(t, err)
	assert.Len(t, resp.Rooms, 1)
}

func TestHistoryService_RoomsGet_ContentPreviewTrimmed(t *testing.T) {
	svc, msgs, subs, rooms := newRoomsService(t)
	long := strings.Repeat("世", 1000) // 1000 runes, well over the preview cap

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "alice", "r1").Return(nil, true, nil)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: long, CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.LessOrEqual(t, len([]rune(resp.Rooms["r1"].Content)), roomsGetPreviewRunes)
	assert.NotEmpty(t, resp.Rooms["r1"].Content)
}

func TestHistoryService_RoomsGet_EmptyRoomIDs(t *testing.T) {
	svc, _, _, _ := newRoomsService(t)
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: nil})
	assertBadRequestErr(t, err, "roomIds must not be empty")
}

func TestHistoryService_RoomsGet_TooManyRoomIDs(t *testing.T) {
	svc, _, _, _ := newRoomsService(t)
	ids := make([]string, roomsGetMaxBatch+1)
	for i := range ids {
		ids[i] = "r" + string(rune('a'+i%26))
	}
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: ids})
	assertBadRequestErr(t, err, "too many roomIds")
}
