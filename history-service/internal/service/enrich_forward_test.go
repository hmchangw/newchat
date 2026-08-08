package service_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/model"
)

func fwdMsg(id, srcRoomID string, at time.Time) models.Message {
	return models.Message{
		MessageID: id, RoomID: "r1", CreatedAt: at,
		ForwardedMessage: &models.ForwardedMessage{
			MessageID: "src-" + id, RoomID: srcRoomID, Msg: "src body",
		},
	}
}

func TestLoadHistory_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{
		fwdMsg("m1", "src-ch", joinTime.Add(4*time.Minute)),
		fwdMsg("m2", "src-dm", joinTime.Add(3*time.Minute)),
		fwdMsg("m3", "src-botdm", joinTime.Add(2*time.Minute)),
		{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)}, // no forward
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	// One batched lookup over the DISTINCT source-room IDs.
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), gomock.InAnyOrder([]string{"src-ch", "src-dm", "src-botdm"})).
		Return(map[string]mongorepo.RoomNameType{
			"src-ch":    {Name: "prj-alpha", Type: model.RoomTypeChannel},
			"src-dm":    {Name: "bob", Type: model.RoomTypeDM},
			"src-botdm": {Name: "MyApp", Type: model.RoomTypeBotDM},
		}, nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 4)

	chRoom := resp.Messages[0].ForwardedMessage.Room
	require.NotNil(t, chRoom)
	assert.Equal(t, "src-ch", chRoom.ID)
	assert.Equal(t, "prj-alpha", chRoom.Name)
	assert.Equal(t, model.RoomTypeChannel, chRoom.Type)

	// dm source: id + type ONLY — the counterpart name must not leak.
	dmRoom := resp.Messages[1].ForwardedMessage.Room
	require.NotNil(t, dmRoom)
	assert.Equal(t, "src-dm", dmRoom.ID)
	assert.Equal(t, model.RoomTypeDM, dmRoom.Type)
	assert.Empty(t, dmRoom.Name)
	assert.Nil(t, dmRoom.HRInfo)
	assert.Nil(t, dmRoom.AppInfo)

	// botDM source: id + type ONLY — the app name must not leak.
	botDMRoom := resp.Messages[2].ForwardedMessage.Room
	require.NotNil(t, botDMRoom)
	assert.Equal(t, "src-botdm", botDMRoom.ID)
	assert.Equal(t, model.RoomTypeBotDM, botDMRoom.Type)
	assert.Empty(t, botDMRoom.Name)
	assert.Nil(t, botDMRoom.HRInfo)
	assert.Nil(t, botDMRoom.AppInfo)

	assert.Nil(t, resp.Messages[3].ForwardedMessage)
}

func TestLoadHistory_ForwardEnrichment_DedupesRoomIDs(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{
		fwdMsg("m1", "src-ch", joinTime.Add(2*time.Minute)),
		fwdMsg("m2", "src-ch", joinTime.Add(time.Minute)),
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	// Exactly ONE lookup with the single distinct ID (gomock's default
	// cardinality makes a second call fail).
	expectSrcChLookup(rooms)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Equal(t, "prj-alpha", resp.Messages[0].ForwardedMessage.Room.Name)
	assert.Equal(t, "prj-alpha", resp.Messages[1].ForwardedMessage.Room.Name)
}

func TestLoadHistory_ForwardEnrichment_BestEffortOnError(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	rooms.EXPECT().GetRoomsNameType(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("mongo down"))

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err) // read never fails on enrichment
	assert.Nil(t, resp.Messages[0].ForwardedMessage.Room)
}

func TestLoadHistory_ForwardEnrichment_RoomMissing(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{fwdMsg("m1", "src-gone", joinTime.Add(time.Minute))}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	rooms.EXPECT().GetRoomsNameType(gomock.Any(), []string{"src-gone"}).
		Return(map[string]mongorepo.RoomNameType{}, nil) // room deleted

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Nil(t, resp.Messages[0].ForwardedMessage.Room) // omitted, client falls back to roomId
}

func TestLoadHistory_NoForwards_NoLookup(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	// NO GetRoomsNameType expectation: zero forwards must cost zero lookups
	// (gomock fails the test on an unexpected call).

	_, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
}

func expectSrcChLookup(rooms *mocks.MockRoomRepository) {
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), []string{"src-ch"}).
		Return(map[string]mongorepo.RoomNameType{"src-ch": {Name: "prj-alpha", Type: model.RoomTypeChannel}}, nil)
}

func TestLoadNextMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))}, false), nil)
	expectSrcChLookup(rooms)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.Messages[0].ForwardedMessage.Room.Name)
}

func TestGetMessageByID_EnrichesForwardedRoom(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	m := fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(&m, nil)
	expectSrcChLookup(rooms)

	resp, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.NoError(t, err)
	require.NotNil(t, resp.ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.ForwardedMessage.Room.Name)
}

func TestGetMessagesByIDs_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1"}).
		Return([]models.Message{fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))}, nil)
	expectSrcChLookup(rooms)

	resp, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1"}})
	require.NoError(t, err)
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
}

func TestListPinnedMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	pinned := fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))
	pinned.PinnedAt = ptrTime(joinTime.Add(2 * time.Minute))
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).
		Return(makePage([]models.Message{pinned}, false), nil)
	expectSrcChLookup(rooms)

	resp, err := svc.ListPinnedMessages(c, models.ListPinnedMessagesRequest{Limit: 10})
	require.NoError(t, err)
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
}

func TestLoadSurroundingMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	central := fwdMsg("m-central", "src-ch", joinTime.Add(10*time.Minute))
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-central").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).
		Return(makePage(nil, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)
	expectSrcChLookup(rooms)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "m-central", Limit: 5})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1) // the spliced central message
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.Messages[0].ForwardedMessage.Room.Name)
}

func TestGetThreadMessages_EnrichesForwardedParent(t *testing.T) {
	svc, msgs, subs, rooms, _, threadRooms, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	threadRooms.EXPECT().GetMinThreadUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	parent := fwdMsg("m-parent", "src-ch", joinTime.Add(5*time.Minute))
	parent.ThreadRoomID = "tr-1"
	parent.TCount = intPtr(1)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(&parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	replies := []models.Message{{
		MessageID: "reply-1", RoomID: "r1", ThreadRoomID: "tr-1",
		ThreadParentID: "m-parent", CreatedAt: parent.CreatedAt.Add(time.Minute),
	}}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(replies, false), nil)
	expectSrcChLookup(rooms) // exactly ONE lookup shared by replies + parent

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	require.NotNil(t, resp.ParentMessage.ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.ParentMessage.ForwardedMessage.Room.Name)
}

func TestGetThreadParentMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, threadRooms, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	// makeThreadPage's rows reference parent IDs "p1"/"p2" (threads_test.go).
	p1 := fwdMsg("p1", "src-ch", joinTime.Add(time.Minute))
	p2 := models.Message{MessageID: "p2", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{p1, p2}, nil)
	expectSrcChLookup(rooms)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterAll, Limit: 20})
	require.NoError(t, err)
	require.Len(t, resp.ParentMessages, 2)
	require.NotNil(t, resp.ParentMessages[0].ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.ParentMessages[0].ForwardedMessage.Room.Name)
	assert.Nil(t, resp.ParentMessages[1].ForwardedMessage)
}
