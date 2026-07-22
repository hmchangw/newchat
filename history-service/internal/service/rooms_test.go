package service_test

import (
	"encoding/json"
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
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// newRoomsService builds a service with bare mocks. rooms.get is server-to-server
// now — no access (subscription) check — so tests only set room-time + message reads.
func newRoomsService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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

// newRoomsServiceWithApps also exposes the AppStore mock, needed by the preview
// enrichment tests (bot sender → app name).
func newRoomsServiceWithApps(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository, *mocks.MockAppStore) {
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
	return svc, msgs, rooms, apps
}

func roomsCtx() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "alice"})
}

var roomLastMsgAt = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
var roomCreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Mirror the production caps (house pattern — see maxGetByIDsBatchSize in messages_test).
const roomsGetMaxBatch = 100

func TestHistoryService_RoomsGet_LatestMessage(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

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
}

func TestHistoryService_RoomsGet_EmptyRoomOmitted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(time.Time{}, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	assert.NotNil(t, resp.Rooms)
}

func TestHistoryService_RoomsGet_PerRoomDegradeKeepsSiblings(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	// r1 history read errors → omitted; r2 succeeds → returned.
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), errors.New("cassandra timeout"))

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r2").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r2", Msg: "ok", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	require.Contains(t, resp.Rooms, "r2")
}

// Latest message deleted → walk back within the page and return the first survivor.
func TestHistoryService_RoomsGet_SkipsDeletedTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m3", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt},
			{MessageID: "m2", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-2 * time.Minute), Sender: models.Participant{Account: "alice"}},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, "alive", resp.Rooms["r1"].Content)
}

// Every message in the page is deleted (and the page is the whole room) → no entry.
func TestHistoryService_RoomsGet_AllDeletedOmitted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	// A short all-deleted page (below the walk page size) means no older messages
	// remain, so a single read is enough to conclude "no last message".
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
}

func TestHistoryService_RoomsGet_DedupsRoomIDs(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	// A duplicate roomId resolves once (Times(1) on each per-room read).
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil).Times(1)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "x", CreatedAt: roomLastMsgAt}}, false), nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r1"}})
	require.NoError(t, err)
	assert.Len(t, resp.Rooms, 1)
}

// Content is returned in full — the client truncates for display.
func TestHistoryService_RoomsGet_FullContent(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	long := strings.Repeat("世", 1000)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: long, CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, long, resp.Rooms["r1"].Content)
}

// Latest message is a system message → walk back to the first non-system, non-quoted survivor.
func TestHistoryService_RoomsGet_SkipsSystemTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Type: "call_ended", CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

// Latest message quotes another message → walk back to the first non-quoted survivor.
func TestHistoryService_RoomsGet_SkipsQuotedTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Msg: "re: x", QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m0"}, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

// Mixed tail: system + quoted + deleted before a real message → returns the real one.
func TestHistoryService_RoomsGet_MixedTailSkipsAllIneligible(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m4", RoomID: "r1", Type: "call_started", CreatedAt: roomLastMsgAt},
			{MessageID: "m3", RoomID: "r1", QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m0"}, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
			{MessageID: "m2", RoomID: "r1", Deleted: true, CreatedAt: roomLastMsgAt.Add(-2 * time.Minute)},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-3 * time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

// A normal message (no type, no quote, not deleted) is returned as-is.
func TestHistoryService_RoomsGet_NormalMessageUnaffected(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

func TestHistoryService_RoomsGet_EmptyRoomIDs(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: nil})
	assertBadRequestErr(t, err, "roomIds must not be empty")
}

func TestHistoryService_RoomsGet_TooManyRoomIDs(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	ids := make([]string, roomsGetMaxBatch+1)
	for i := range ids {
		ids[i] = "r" + string(rune('a'+i%26))
	}
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: ids})
	assertBadRequestErr(t, err, "too many roomIds")
}

// Preview enrichment: attachments, mentions (wire Participants), and visibleTo are
// carried; a non-bot sender's chineseName comes from the Cassandra company_name and no
// app lookup happens.
func TestHistoryService_RoomsGet_EnrichesPreview(t *testing.T) {
	svc, msgs, rooms, apps := newRoomsServiceWithApps(t)
	apps.EXPECT().AppNameByAccount(gomock.Any(), gomock.Any()).Times(0) // no bot → no lookup

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{
			MessageID:   "m1",
			RoomID:      "r1",
			Msg:         "hi",
			CreatedAt:   roomLastMsgAt,
			Sender:      models.Participant{ID: "u1", Account: "alice", EngName: "Alice", CompanyName: "愛麗絲"},
			Attachments: cassandra.EncodeAttachments([]cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}}),
			Mentions:    []models.Participant{{ID: "u2", Account: "bob", CompanyName: "小明"}},
			VisibleTo:   "u1",
		}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	pm := resp.Rooms["r1"]
	assert.Equal(t, "愛麗絲", pm.Sender.ChineseName)       // company_name → chineseName
	assert.Equal(t, "Alice 愛麗絲", pm.Sender.DisplayName) // composed, not a bot
	assert.Equal(t, "u1", pm.Sender.UserID)
	require.Len(t, pm.Attachments, 1)
	assert.Equal(t, "a.png", pm.Attachments[0].Title)
	require.Len(t, pm.Mentions, 1)
	assert.Equal(t, "bob", pm.Mentions[0].Account)
	assert.Equal(t, "小明", pm.Mentions[0].ChineseName)
	assert.Equal(t, "u1", pm.VisibleTo)
}

// A bot sender's displayName is its app name (mirrors the reaction actor path).
func TestHistoryService_RoomsGet_BotSenderAppName(t *testing.T) {
	svc, msgs, rooms, apps := newRoomsServiceWithApps(t)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "acme.bot").Return("Acme Assistant", nil)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{
			MessageID: "m1", RoomID: "r1", Msg: "beep", CreatedAt: roomLastMsgAt,
			Sender: models.Participant{ID: "b1", Account: "acme.bot", EngName: "acme"},
		}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "Acme Assistant", resp.Rooms["r1"].Sender.DisplayName)
}

// Empty attachments/mentions serialize away (omitempty) — no [] noise in the preview.
func TestHistoryService_RoomsGet_EmptyCollectionsOmitted(t *testing.T) {
	svc, msgs, rooms, _ := newRoomsServiceWithApps(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	pm := resp.Rooms["r1"]
	assert.Nil(t, pm.Attachments)
	assert.Nil(t, pm.Mentions)

	data, err := json.Marshal(pm)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "attachments")
	assert.NotContains(t, string(data), "mentions")
	assert.NotContains(t, string(data), "visibleTo")
}
