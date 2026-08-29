package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// Regression: a room whose Mongo doc has no lastMsgAt (zero from GetRoomTimes)
// but holds messages in Cassandra must load history. Before the fix, the
// resolver collapsed the zero to createdAt, LoadHistory capped before at
// createdAt+1ms, and the walk scanned only pre-creation time — always empty.
func TestLoadHistory_MissingLastMsgAt_ReturnsMessages(t *testing.T) {
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
	s := closeOnCleanupIn(t, New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg))

	createdAt := time.Now().UTC().Add(-120 * 24 * time.Hour)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(time.Time{}, createdAt, nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	recent := models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: time.Now().UTC().Add(-time.Hour)}
	// The before bound must NOT be capped at createdAt+1ms — it must stay ≈ now.
	msgs.EXPECT().
		GetMessagesBefore(gomock.Any(), "r1",
			gomock.Cond(func(x any) bool {
				before, ok := x.(time.Time)
				return ok && before.After(createdAt.Add(24*time.Hour))
			}),
			gomock.Any(), gomock.Any()).
		Return(cassrepo.Page[models.Message]{Data: []models.Message{recent}}, nil)

	c := natsrouter.NewContext(map[string]string{"account": "u1", "roomID": "r1"})
	resp, err := s.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1, "room with messages but no lastMsgAt must return them")
	assert.Equal(t, "m1", resp.Messages[0].MessageID)
}
