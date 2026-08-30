package service

import (
	"context"
	"errors"
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

// On the degraded path the cached createdAt reaches the walk as its floor,
// while `before` stays at now — the frozen lastMsgAt caps nothing. Both halves
// matter: the floor is what keeps the walk from scanning to the global history
// floor, and the uncapped ceiling is what keeps messages written during the
// outage reachable.
func TestLoadHistory_Degraded_FloorsAtCachedCreatedAtAndKeepsTheCeilingAtNow(t *testing.T) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	cfg := &config.Config{MessageHistoryFloorDays: 730, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}

	start := time.Now().UTC()
	cachedCreated := start.Add(-200 * 24 * time.Hour)

	tier := newFakeRoomTimesTier()
	tier.fallback["r1"] = cachedCreated
	s := New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg,
		WithRoomTimesCache(tier))

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").
		Return(time.Time{}, time.Time{}, errors.New("mongo down"))
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	var gotBefore, gotFloor time.Time
	msgs.EXPECT().
		GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, before, floor time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			gotBefore, gotFloor = before, floor
			return cassrepo.Page[models.Message]{}, nil
		})

	c := natsrouter.NewContext(map[string]string{"account": "u1", "roomID": "r1"})
	_, err := s.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)

	assert.Equal(t, cachedCreated, gotFloor, "the cached createdAt is the walk floor")
	assert.False(t, gotBefore.Before(start),
		"the ceiling stays at now, so messages written during the outage are still reachable")
}

// rooms.lastMsgAt is written by roomlist-worker on a coalescing flush, on a
// consumer separate from the message-worker that writes the Cassandra row — so
// the pointer trails the data by at least one flush interval, and without bound
// whenever that worker is backlogged or down. Capping the read's ceiling at it
// therefore hides messages that demonstrably exist: post, see it broadcast,
// reload history, and it is gone.
//
// The ceiling has to come from the clock, which no write can outrun.
func TestLoadHistory_CeilingDoesNotTrustLastMsgAt(t *testing.T) {
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
	s := New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)

	now := time.Now().UTC()
	createdAt := now.Add(-30 * 24 * time.Hour)
	// What Mongo knows, one flush interval behind the write that already landed.
	staleLastMsgAt := now.Add(-2 * time.Second)
	justWritten := models.Message{MessageID: "m-new", RoomID: "r1", CreatedAt: now.Add(-100 * time.Millisecond)}

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(staleLastMsgAt, createdAt, nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	var gotBefore time.Time
	msgs.EXPECT().
		GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, before, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			gotBefore = before
			if justWritten.CreatedAt.After(before) {
				return cassrepo.Page[models.Message]{}, nil // excluded by the ceiling
			}
			return cassrepo.Page[models.Message]{Data: []models.Message{justWritten}}, nil
		})

	c := natsrouter.NewContext(map[string]string{"account": "u1", "roomID": "r1"})
	resp, err := s.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)

	assert.True(t, gotBefore.After(justWritten.CreatedAt),
		"ceiling %s must not sit below a row already in Cassandra at %s", gotBefore, justWritten.CreatedAt)
	require.Len(t, resp.Messages, 1, "a message that exists must not vanish because the Mongo pointer lags")
	assert.Equal(t, "m-new", resp.Messages[0].MessageID)
}

// A DESC walk starts at its upper bound's bucket. The lastMsgAt cap removed in
// 59cb66f was incidentally clamping a bogus client `before` as well, so without
// a replacement a far-future value starts the walk thousands of buckets above
// any row and burns the whole MESSAGE_READ_MAX_BUCKETS budget on empties.
// Nothing can be created after now+skew, so clamping there cannot hide a row.
func TestLoadHistory_ClampsAFutureBeforeToTheServerClock(t *testing.T) {
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
	s := New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)

	now := time.Now().UTC()
	farFutureMs := now.AddDate(1000, 0, 0).UnixMilli()
	farFuture := &farFutureMs

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(now.Add(-time.Hour), now.Add(-30*24*time.Hour), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	var gotBefore time.Time
	msgs.EXPECT().
		GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, before, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			gotBefore = before
			return cassrepo.Page[models.Message]{}, nil
		})

	c := natsrouter.NewContext(map[string]string{"account": "u1", "roomID": "r1"})
	_, err := s.LoadHistory(c, models.LoadHistoryRequest{Before: farFuture})
	require.NoError(t, err)

	assert.False(t, gotBefore.After(now.Add(clockSkewTolerance+time.Minute)),
		"a year-3026 before must be clamped to the clock, got %s", gotBefore)
}
