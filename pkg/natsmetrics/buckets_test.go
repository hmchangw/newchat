package natsmetrics

import (
	"context"
	"testing"

	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The OTel SDK default histogram boundaries top out at 10000 and start at 0/5,
// which puts every sub-second NATS duration in the first bucket. Both latency
// histograms must instead carry the SDK's shared latency boundaries so p95/p99
// are comparable with the http.server.* families.
func TestLatencyHistogramsUseSharedLatencyBuckets(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{ServiceName: "svc", Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
	tracked := c.Track(context.Background(), &fakeMsg{}, EventCreated, 5)
	require.NoError(t, tracked.Ack())
	m.Publisher("svc", "s1").Request(context.Background(), OperationPresenceLookup, 0, nil)

	rm := collect(t, reader)
	want := o11y.DefaultLatencyBuckets()
	for _, name := range []string{"chat.nats.consumer.processing.duration", "chat.nats.request.duration"} {
		points := histogramPoints(t, rm, name)
		require.Len(t, points, 1, name)
		assert.Equal(t, want, points[0].Bounds, name)
	}
}

func TestNewFromProviderUsesPackageScope(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := NewFromProvider(mp)
	m.Publisher("svc", "s1").Attempt(context.Background(), DestinationCanonical, OperationCanonicalPublish, nil)

	rm := collect(t, reader)
	var scopes []string
	for _, scope := range rm.ScopeMetrics {
		scopes = append(scopes, scope.Scope.Name)
	}
	assert.Contains(t, scopes, ScopeName)
}
