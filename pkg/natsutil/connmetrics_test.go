package natsutil

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestConnMetrics(t *testing.T) (*connMetrics, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return newConnMetrics(mp.Meter("test")), reader
}

func connPoints(t *testing.T, reader sdkmetric.Reader, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, name)
				return sum.DataPoints
			}
		}
	}
	return nil
}

func eventCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, dp := range connPoints(t, reader, "chat.nats.client.connection.events") {
		for _, kv := range dp.Attributes.ToSlice() {
			if string(kv.Key) == "event" {
				out[kv.Value.AsString()] = dp.Value
			}
		}
	}
	return out
}

// The gauge is what separates "the broker went away" from "the loop died while
// the broker was fine" during a complete-outage campaign.
func TestConnMetrics_GaugeTracksOutage(t *testing.T) {
	m, reader := newTestConnMetrics(t)
	conn := m.Connection()

	conn.Connected(context.Background())
	points := connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)

	conn.Disconnected(context.Background(), errors.New("broker unreachable"))
	points = connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(0), points[0].Value)

	conn.Reconnected(context.Background())
	points = connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)

	conn.Closed(context.Background())
	points = connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(0), points[0].Value)
}

// The gauge must stay a 0/1 state even if the client library repeats a
// callback. The events counter mirrors loadgen_nats_connection_events_total and
// stays a raw transition count, so it is not deduplicated.
func TestConnMetrics_GaugeIsIdempotentButEventsAreRaw(t *testing.T) {
	m, reader := newTestConnMetrics(t)
	conn := m.Connection()

	conn.Connected(context.Background())
	conn.Connected(context.Background())
	conn.Disconnected(context.Background(), nil)
	conn.Disconnected(context.Background(), nil)

	points := connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(0), points[0].Value)
	assert.Equal(t, map[string]int64{"connected": 2, "disconnected": 2}, eventCounts(t, reader))
}

func TestConnMetrics_ConnectionsTrackIndependently(t *testing.T) {
	m, reader := newTestConnMetrics(t)
	first := m.Connection()
	second := m.Connection()

	first.Connected(context.Background())
	second.Connected(context.Background())
	points := connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(2), points[0].Value)

	first.Disconnected(context.Background(), errors.New("first broker unavailable"))
	points = connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)

	second.Closed(context.Background())
	points = connPoints(t, reader, "chat.nats.client.connected")
	require.Len(t, points, 1)
	assert.Equal(t, int64(0), points[0].Value)
}

func TestConnMetrics_EventLabelsAreBounded(t *testing.T) {
	m, reader := newTestConnMetrics(t)
	conn := m.Connection()

	conn.Connected(context.Background())
	conn.Disconnected(context.Background(), errors.New("some dynamic error text"))
	conn.Reconnected(context.Background())
	conn.Closed(context.Background())
	// Every async error collapses to one bounded value. A reconnect-buffer
	// overflow never arrives here — nats.go returns it synchronously from
	// Publish — so it must not have an event value of its own to imply it does.
	conn.AsyncError(context.Background(), nats.ErrReconnectBufExceeded)
	conn.AsyncError(context.Background(), errors.New("unclassified async failure"))

	assert.Equal(t, map[string]int64{
		"connected":    1,
		"disconnected": 1,
		"reconnected":  1,
		"closed":       1,
		"async_error":  2,
	}, eventCounts(t, reader))
}

func TestConnMetrics_NilReceiverIsSafe(t *testing.T) {
	var m *connMetricState
	m.Connected(context.Background())
	m.Disconnected(context.Background(), nil)
	m.Reconnected(context.Background())
	m.Closed(context.Background())
	m.AsyncError(context.Background(), nats.ErrSlowConsumer)
}
