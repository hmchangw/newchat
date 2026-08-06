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
		fwdMsg("m1", "src-ch", joinTime.Add(3*time.Minute)),
		fwdMsg("m2", "src-dm", joinTime.Add(2*time.Minute)),
		{MessageID: "m3", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)}, // no forward
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	// One batched lookup over the DISTINCT source-room IDs.
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), gomock.InAnyOrder([]string{"src-ch", "src-dm"})).
		Return(map[string]mongorepo.RoomNameType{
			"src-ch": {Name: "prj-alpha", Type: model.RoomTypeChannel},
			"src-dm": {Name: "bob", Type: model.RoomTypeDM},
		}, nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 3)

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

	assert.Nil(t, resp.Messages[2].ForwardedMessage)
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

	// Exactly ONE lookup with the single distinct ID.
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), []string{"src-ch"}).
		Return(map[string]mongorepo.RoomNameType{"src-ch": {Name: "prj-alpha", Type: model.RoomTypeChannel}}, nil).
		Times(1)

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
