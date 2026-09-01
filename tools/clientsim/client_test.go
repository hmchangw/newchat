package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestSimClient_RunLifecycle_SubscribesWalksAndDelivers(t *testing.T) {
	fc := newFakeConn(
		subListPage{Subscriptions: []subRow{{RoomID: "r1", RoomType: "channel"}}, HasMore: true},
		subListPage{Subscriptions: []subRow{{RoomID: "d1", RoomType: "dm"}}, HasMore: false},
	)
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	startClient(t, s)

	require.Eventually(t, func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		_, ok := fc.chanSubs[subject.RoomEvent("r1", true)]
		return ok
	}, 3*time.Second, 5*time.Millisecond, "walk must open the channel room sub")
	assert.GreaterOrEqual(t, fc.reqCount.Load(), int64(2), "walk must paginate")
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)

	// User lane delivery via the callback sub.
	evt, err := json.Marshal(deliveredEnvelope{Timestamp: time.Now().Add(-5 * time.Millisecond).UnixMilli()})
	require.NoError(t, err)
	fc.deliverCB(t, subject.UserRoomEvent("user-lc"), evt)
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Delivered.WithLabelValues("user")), 0.001)

	// Channel delivery via the shared pump channel.
	fc.mu.Lock()
	roomCh := fc.chanSubs[subject.RoomEvent("r1", true)]
	fc.mu.Unlock()
	roomCh <- &nats.Msg{Data: evt}
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.Delivered.WithLabelValues("channel")) >= 1
	}, 3*time.Second, 5*time.Millisecond, "pump must drain the shared room channel")

	s.close()
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)
	assert.Equal(t, int64(1), fc.closes.Load())
	s.close() // idempotent
	assert.Equal(t, int64(1), fc.closes.Load())
}

func TestSimClient_RunFailsCleanlyWhenWalkFails(t *testing.T) {
	fc := newFakeConn() // no pages: the walk RPC errors
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.run(ctx)
	require.Error(t, err, "run must fail when the walk cannot decode a page")
	assert.Equal(t, int64(1), fc.closes.Load(), "a failed startup must not leave a zombie conn")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)
}
