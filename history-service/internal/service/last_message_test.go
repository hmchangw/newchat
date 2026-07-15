package service_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// newLastMsgService builds a service with NO permissive room-times pre-stub so
// tests can assert exact GetRoomTimes behaviour (including the not-found path).
func newLastMsgService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
	return service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg), msgs, rooms
}

func TestHistoryService_GetLastRoomMessage_Success(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	// Anchor times relative to real now so the historyFloor clamp never kicks in
	// and the repo bounds can be asserted exactly.
	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	msgCreatedAt := lastMsgAt
	editedAt := lastMsgAt.Add(-time.Minute)
	row := &models.Message{
		MessageID:   "m1",
		RoomID:      "r1",
		Sender:      models.Participant{ID: "u1", Account: "alice", EngName: "Alice Smith"},
		Msg:         "hello world",
		CreatedAt:   msgCreatedAt,
		EditedAt:    &editedAt,
		Attachments: cassandra.EncodeAttachments([]cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}, {ID: "f2", Title: "b.pdf", Type: "file"}}),
	}
	// before = lastMsgAt+1ms (so the newest row survives the strict < bound);
	// floor = createdAt (inside the 90-day history floor).
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", lastMsgAt.Add(time.Millisecond), createdAt).
		Return(row, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.LastMessage)

	preview := resp.LastMessage
	assert.Equal(t, "m1", preview.MessageID)
	assert.Empty(t, preview.Type, "ordinary user messages carry no type")
	assert.Equal(t, "alice", preview.SenderAccount)
	assert.Equal(t, "Alice Smith", preview.SenderName)
	assert.Equal(t, "hello world", preview.Msg)
	assert.Equal(t, msgCreatedAt, preview.CreatedAt)
	require.NotNil(t, preview.EditedAt)
	assert.Equal(t, editedAt, *preview.EditedAt)
	assert.Equal(t, 2, preview.AttachmentCount)
}

func TestHistoryService_GetLastRoomMessage_SenderNameFallbacks(t *testing.T) {
	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)

	tests := []struct {
		name     string
		sender   models.Participant
		wantName string
	}{
		{name: "eng name wins", sender: models.Participant{Account: "alice", EngName: "Alice", AppName: "Bot"}, wantName: "Alice"},
		{name: "bot falls back to app name", sender: models.Participant{Account: "bot-1", AppName: "Reminder Bot", IsBot: true}, wantName: "Reminder Bot"},
		{name: "account is the last resort", sender: models.Participant{Account: "bob"}, wantName: "bob"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, msgs, rooms := newLastMsgService(t)
			rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)
			msgs.EXPECT().
				GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
				Return(&models.Message{MessageID: "m1", RoomID: "r1", Sender: tc.sender, CreatedAt: lastMsgAt}, nil)

			resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
			require.NoError(t, err)
			require.NotNil(t, resp.LastMessage)
			assert.Equal(t, tc.wantName, resp.LastMessage.SenderName)
			assert.Equal(t, tc.sender.Account, resp.LastMessage.SenderAccount)
		})
	}
}

func TestHistoryService_GetLastRoomMessage_NoQualifyingMessage(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(nil, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Nil(t, resp.LastMessage, "all-deleted / all-system room yields a nil preview, not an error")
}

func TestHistoryService_GetLastRoomMessage_RepoError(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("cassandra timeout"))

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "loading last room message")
}

func TestHistoryService_GetLastRoomMessage_EmptyRoomID(t *testing.T) {
	svc, _, _ := newLastMsgService(t)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{})
	assert.Nil(t, resp)
	assertBadRequestErr(t, err, "roomId is required")
}

// A room unknown to Mongo answers "no last message" rather than an error: the
// RPC is server-to-server and callers treat a missing room like an empty one.
func TestHistoryService_GetLastRoomMessage_RoomNotFound(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), "r-missing").
		Return(time.Time{}, time.Time{}, fmt.Errorf("get room times for r-missing: %w", mongo.ErrNoDocuments))
	// Critically no GetLastRoomMessage expectation — the Cassandra walk must be skipped.

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r-missing"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Nil(t, resp.LastMessage)

	_ = msgs // absence of expectations asserts the repo is never touched
}
