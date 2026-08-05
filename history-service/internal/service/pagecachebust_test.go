package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
)

// bustSpy records the rooms whose page cache was invalidated.
type bustSpy struct{ rooms []string }

func (s *bustSpy) Bump(_ context.Context, roomID string) { s.rooms = append(s.rooms, roomID) }

// newBustService builds a HistoryService with a spy PageCacheBuster and
// permissive room/publish stubs, exposing the mocks the mutation paths touch.
func newBustService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockEventPublisher, *bustSpy) {
	t.Helper()
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).AnyTimes()
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), gomock.Any()).Return(0, nil).AnyTimes()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// Edit/delete refresh the room preview via a backward walk; don't assert on it here.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil).AnyTimes()

	spy := &bustSpy{}
	cfg := &config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, service.WithPageCacheBuster(spy))
	return svc, msgs, subs, pub, spy
}

func editableMessage() *models.Message {
	return &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "original content",
	}
}

func TestBust_EditMessage_Success_Bumps(t *testing.T) {
	svc, msgs, subs, _, spy := newBustService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := editableMessage()
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated", gomock.Any()).Return(nil)

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "msg-1", NewMsg: "updated"})
	require.NoError(t, err)
	assert.Equal(t, []string{"r1"}, spy.rooms, "a successful edit must invalidate the room's page cache")
}

func TestBust_EditMessage_WriteFails_NoBump(t *testing.T) {
	svc, msgs, subs, _, spy := newBustService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := editableMessage()
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated", gomock.Any()).Return(errors.New("boom"))

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "msg-1", NewMsg: "updated"})
	require.Error(t, err)
	assert.Empty(t, spy.rooms, "a failed write must not invalidate the cache")
}

func TestBust_DeleteMessage_Applied_Bumps(t *testing.T) {
	svc, msgs, subs, _, spy := newBustService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := editableMessage()
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		Return(time.Now().UTC(), true, nil, nil, nil)

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"r1"}, spy.rooms, "an applied delete must invalidate the room's page cache")
}

func TestBust_DeleteMessage_NotApplied_NoBump(t *testing.T) {
	svc, msgs, subs, _, spy := newBustService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := editableMessage()
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	// applied=false: a concurrent delete won the CAS — nothing changed here.
	msgs.EXPECT().SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		Return(time.Now().UTC(), false, nil, nil, nil)

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
	assert.Empty(t, spy.rooms, "a lost delete race must not invalidate the cache")
}
