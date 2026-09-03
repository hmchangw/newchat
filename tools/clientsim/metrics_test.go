package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/subject"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scrape(t *testing.T, m *metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	// #nosec G107 -- srv.URL is this test's own httptest server address, untainted
	// nosemgrep: gosec.G107-1 -- srv.URL is this test's own httptest server address, untainted
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() == name {
			require.NotEmpty(t, fam.GetMetric())
			return fam.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("histogram %s not found in registry", name)
	return 0
}

func TestNewMetrics_RegistersAllSeries(t *testing.T) {
	m := newMetrics()
	m.ConnsActive.Set(3)
	m.ConnsConnecting.Set(1)
	m.Delivered.WithLabelValues("channel").Add(2)
	m.JWTRefreshes.WithLabelValues("proactive").Inc()
	m.Disconnects.WithLabelValues("auth_expired").Inc()
	m.AuthDuration.Observe(0.01)
	m.ConnectDuration.Observe(0.02)
	m.BroadcastLatency.Observe(0.05)
	m.CanonicalLatency.Observe(0.07)
	m.Reconnects.Inc()
	m.DecodeFailures.Inc()
	m.InvalidTimestamp.Inc()
	m.SlowConsumer.Inc()
	m.AuthFailures.Inc()
	m.Errors.WithLabelValues("connect").Inc()
	m.RunInfo.WithLabelValues("proactive", "0", "1").Set(1)

	names := []string{
		"clientsim_conns_active",
		"clientsim_conns_connecting",
		"clientsim_auth_duration_seconds",
		"clientsim_connect_duration_seconds",
		"clientsim_disconnects_total",
		"clientsim_reconnects_total",
		"clientsim_jwt_refreshes_total",
		"clientsim_reconnect_attempt",
		"clientsim_msgs_delivered_total",
		"clientsim_broadcast_to_client_latency_seconds",
		"clientsim_canonical_to_client_latency_seconds",
		"clientsim_decode_failures_total",
		"clientsim_invalid_timestamp_total",
		"clientsim_slow_consumer_events_total",
		"clientsim_auth_failures_total",
		"clientsim_errors_total",
		"clientsim_run_info",
	}
	// Delivered pre-resolves all three lane children (user | channel |
	// member), so its family carries three series; every other name carries one.
	wantSeries := len(names) + 2
	got, err := promtestutil.GatherAndCount(m.Registry, names...)
	require.NoError(t, err)
	assert.Equal(t, wantSeries, got)

	assert.InDelta(t, 3, promtestutil.ToFloat64(m.ConnsActive), 0.001)
	assert.InDelta(t, 2, promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
	assert.InDelta(t, 2, promtestutil.ToFloat64(m.delivered("channel")), 0.001,
		"pre-resolved child must be the same series as the vec child")
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.delivered("member")), 0.001,
		"the member lane is registered up-front so it scrapes as 0, not absent")
	assert.Equal(t, uint64(1), histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
}

func TestMetrics_HandlerServesRegistry(t *testing.T) {
	m := newMetrics()
	m.DecodeFailures.Inc()
	body := scrape(t, m)
	assert.True(t, strings.Contains(body, "clientsim_decode_failures_total 1"),
		"scrape body must carry the counter, got:\n%.500s", body)
	assert.Contains(t, body, "go_goroutines", "Go collector must be registered")
}

// The room lane is bounded in MESSAGES, not bytes — nats.go rejects
// SetPendingLimits on a channel subscription, and giving it a real byte limit
// would mean a dispatcher goroutine per room per client. What was missing was
// not the bound but the visibility: how deep the queue actually runs is what
// tells an operator whether the pump is keeping up and how much is sitting in
// memory. The pump samples it on every dequeue.
func TestPump_RecordsRoomQueueDepth(t *testing.T) {
	fc := newFakeConn(emptySubListPage())
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.conn = fc

	const queued = 4
	for i := 0; i < queued; i++ {
		s.roomCh <- &nats.Msg{Subject: subject.RoomEvent("r1", true), Data: []byte(`{"type":"other"}`)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.pump(ctx) }()
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(s.m.Delivered.WithLabelValues("channel")) == queued
	}, 3*time.Second, 5*time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, uint64(queued), histogramCount(t, s.m.Registry, "clientsim_room_queue_depth"),
		"every dequeue samples the depth behind it")
}

// The trough is meant to describe the measurement window. Seeding it on the
// first decrement means a churn cycle during the ramp — when the fleet has not
// come up yet — permanently pins clientsim_conns_ready_min near zero, however
// healthy the rest of the run is. Tracking starts once the floor is reached.
func TestMetrics_TroughIgnoresChurnBeforeTheFleetIsUp(t *testing.T) {
	m := newMetrics()
	m.armTroughAt(10)

	for i := 0; i < 4; i++ { // ramping
		m.readyInc()
	}
	m.readyDec() // a churn cycle at 4/10 — not the window's trough
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsReadyMin), 0.001,
		"no trough is recorded before the fleet reaches its floor")

	for i := 0; i < 7; i++ {
		m.readyInc() // reaches 10
	}
	m.readyDec() // now at 9, inside the window
	assert.InDelta(t, 9, promtestutil.ToFloat64(m.ConnsReadyMin), 0.001)
}

// With the gate disabled (floor 0) the trough tracks from the first client, so
// a run without a floor still reports one.
func TestMetrics_TroughWithNoFloorTracksImmediately(t *testing.T) {
	m := newMetrics()
	m.armTroughAt(0)
	m.readyInc()
	m.readyInc()
	m.readyDec()
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsReadyMin), 0.001)
}
