package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
