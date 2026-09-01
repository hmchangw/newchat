//go:build integration

package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// fixedMinter satisfies minter without an auth-service: the test broker
// runs no auth, so the JWT only has to be decodable by the client's own
// cache bookkeeping.
type fixedMinter struct{ jwt string }

func (f fixedMinter) Mint(context.Context, string, string) (string, error) { return f.jwt, nil }

func TestSimClient_EndToEnd_WSSubscribeWalkAndCount(t *testing.T) {
	info := testutil.NATSWebSocket(t)
	// Per-test account on a process-shared broker: derive from t.Name so a
	// sibling test can never cross-talk on the same subjects.
	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	account := "user-" + name[:min(len(name), 20)]
	const site = "site-a"

	// Backend stub on the TCP side: answer subscription.list with a channel
	// room on page one and a DM on page two to exercise pagination.
	backend, err := nats.Connect(info.TCPURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := backend.Drain(); err != nil {
			backend.Close()
		}
	})

	pages := []subListPage{
		{Subscriptions: []subRow{{RoomID: "room-1", RoomType: "channel"}}, HasMore: true},
		{Subscriptions: []subRow{{RoomID: "dm-1", RoomType: "dm"}}, HasMore: false},
	}
	var calls atomic.Int64
	_, err = backend.Subscribe(subject.UserSubscriptionList(account, site), func(msg *nats.Msg) {
		i := int(calls.Add(1)) - 1
		page := pages[min(i, len(pages)-1)]
		data, mErr := json.Marshal(page)
		if mErr != nil {
			return
		}
		_ = msg.Respond(data)
	})
	require.NoError(t, err)
	require.NoError(t, backend.Flush())

	cfg := config{
		NATSWSURL: info.WSURL, AuthURL: "http://unused", PoolFile: "unused",
		SiteID: site, JWTMode: jwtModeProactive,
		SubPendingMsgs: 512, SubPendingBytes: 1 << 20,
		ReconnectBufBytes: 1 << 16, PingInterval: 2 * time.Minute,
	}
	m := newMetrics()
	sc, err := newSimClient(account, &cfg, fixedMinter{jwt: mintTestJWT(t, time.Now().Add(time.Hour))}, m)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sc.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Log("simClient run did not exit within 10s")
		}
		sc.close()
	})

	evt := model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "room-1",
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: time.Now().UTC().Add(-20 * time.Millisecond).UnixMilli(),
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	// Channel fan-out reaches the ws client once the walk has opened the sub.
	require.Eventually(t, func() bool {
		_ = backend.Publish(subject.RoomEvent("room-1", true), payload)
		return promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")) >= 1
	}, 15*time.Second, 100*time.Millisecond, "channel fan-out must reach the ws client")

	// User lane delivery.
	require.Eventually(t, func() bool {
		_ = backend.Publish(subject.UserRoomEvent(account), payload)
		return promtestutil.ToFloat64(m.Delivered.WithLabelValues("user")) >= 1
	}, 15*time.Second, 100*time.Millisecond, "user-lane delivery must reach the ws client")

	// Live update: added channel room-2 on the LOCAL namespace starts receiving.
	upd := []byte(`{"action":"added","subscription":{"roomId":"room-2","roomType":"channel","room":{"crossSite":false}},"timestamp":1}`)
	require.NoError(t, backend.Publish(subject.SubscriptionUpdate(account), upd))
	evt2 := evt
	evt2.RoomID = "room-2"
	payload2, err := json.Marshal(evt2)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_ = backend.Publish(subject.RoomEvent("room-2", false), payload2)
		return promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")) >= 2
	}, 15*time.Second, 100*time.Millisecond, "live-added local-namespace room must start receiving")

	assert.GreaterOrEqual(t, calls.Load(), int64(2), "bootstrap walk must paginate")
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.DecodeFailures), 0.001)
	// Eventually re-publishes until the counter moves, so exact counts are
	// unknowable — the invariant is that new_message deliveries observed
	// the broadcast latency at least once.
	assert.Greater(t, func() uint64 {
		fams, gErr := m.Registry.Gather()
		require.NoError(t, gErr)
		for _, fam := range fams {
			if fam.GetName() == "clientsim_broadcast_to_client_latency_seconds" {
				return fam.GetMetric()[0].GetHistogram().GetSampleCount()
			}
		}
		return 0
	}(), uint64(0), "new_message deliveries must observe the broadcast latency")
}
