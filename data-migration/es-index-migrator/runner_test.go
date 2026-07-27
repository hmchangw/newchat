package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestRunWithWorkerPool_RunsEveryItem(t *testing.T) {
	var seen []int
	var mu sync.Mutex
	err := runWithWorkerPool(context.Background(), 2, []int{1, 2, 3}, func(_ context.Context, item int) error {
		mu.Lock()
		seen = append(seen, item)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{1, 2, 3}, seen)
}

func TestRunWithWorkerPool_PropagatesFirstError(t *testing.T) {
	boom := errors.New("boom")
	err := runWithWorkerPool(context.Background(), 2, []int{1, 2, 3}, func(_ context.Context, item int) error {
		if item == 2 {
			return boom
		}
		return nil
	})
	require.ErrorIs(t, err, boom)
}

func TestRunMessages_FlushesEvenWhenAWorkerFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	messages := NewMockMessageSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()
	cfg.WorkerConcurrency = 1 // deterministic ordering for this test

	subs.EXPECT().RoomIDs(gomock.Any(), cfg.SiteID).Return([]string{"room-ok", "room-fail"}, nil)
	messages.EXPECT().StreamMessages(gomock.Any(), cfg.SiteID, "room-ok", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _, _ time.Time, fn func(cassandra.Message) error) error {
			return fn(cassandra.Message{MessageID: "m1", RoomID: "room-ok", CreatedAt: time.Now()})
		})
	messages.EXPECT().StreamMessages(gomock.Any(), cfg.SiteID, "room-fail", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("cassandra timeout"))
	// The one action buffered from room-ok must still reach Bulk even though room-fail errors.
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).Return([]searchengine.BulkResult{{Status: 200}}, nil)

	f := newFlusher(store, 500)
	err := runMessages(context.Background(), subs, messages, f, cfg)

	require.Error(t, err, "a room read error must still fail the run overall")
	assert.Equal(t, 0, f.FailedCount(), "the room-ok action that did reach ES succeeded and must not count as failed")
}

func TestRunSpotlight_OneActionPerSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()

	subs.EXPECT().Subscriptions(gomock.Any(), cfg.SiteID).Return([]model.Subscription{
		{ID: "s1", RoomID: "room1", SiteID: cfg.SiteID, User: model.SubscriptionUser{Account: "alice"}, JoinedAt: time.Now()},
		{ID: "s2", RoomID: "room2", SiteID: cfg.SiteID, User: model.SubscriptionUser{Account: "bob"}, JoinedAt: time.Now()},
	}, nil)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(2)).Return([]searchengine.BulkResult{{Status: 200}, {Status: 200}}, nil)

	f := newFlusher(store, 500)
	err := runSpotlight(context.Background(), subs, f, cfg)

	require.NoError(t, err)
	assert.Equal(t, 0, f.FailedCount())
}

func TestRunUserRoom_SkipsBotSubscriptionsWithoutError(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()

	subs.EXPECT().Subscriptions(gomock.Any(), cfg.SiteID).Return([]model.Subscription{
		{ID: "s1", RoomID: "room1", User: model.SubscriptionUser{Account: "helper.bot", IsBot: true}, JoinedAt: time.Now()},
		{ID: "s2", RoomID: "room1", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: time.Now()},
	}, nil)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).Return([]searchengine.BulkResult{{Status: 200}}, nil)

	f := newFlusher(store, 500)
	err := runUserRoom(context.Background(), subs, f, cfg)

	require.NoError(t, err)
}

func TestRunSpotlight_EmptySubscriptionsIsANoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()

	subs.EXPECT().Subscriptions(gomock.Any(), cfg.SiteID).Return(nil, nil)
	// no EXPECT().Bulk(...) — nothing to flush

	f := newFlusher(store, 500)
	err := runSpotlight(context.Background(), subs, f, cfg)

	require.NoError(t, err)
}
