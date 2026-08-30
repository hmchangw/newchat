package natsmetrics

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/flywindy/o11y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The OTel SDK default histogram boundaries top out at 10000 and start at 0/5,
// which puts every sub-second NATS duration in the first bucket. The consumer
// latency histogram must instead carry the SDK's shared latency boundaries so
// p95/p99 are comparable with the http.server.* families.
//
// The two RPC histograms use the same shared set, deviating from the RPC
// convention's own boundary table so that all three families interpolate at the
// same points. See TestAllLatencyHistogramsShareOneBoundarySet.
func TestLatencyHistogramsUseSharedLatencyBuckets(t *testing.T) {
	m, reader := newTestMetrics(t)
	c := m.Consumer(ConsumerConfig{Site: "s1", Stream: "STREAM_s1", Consumer: "durable"})
	tracked := c.Track(context.Background(), &fakeMsg{}, EventCreated, 5)
	require.NoError(t, tracked.Ack())

	rm := collect(t, reader)
	points := histogramPoints(t, rm, "chat.nats.consumer.processing.duration")
	require.Len(t, points, 1)
	assert.Equal(t, o11y.DefaultLatencyBuckets(), points[0].Bounds)
}

func TestNewFromProviderUsesPackageScope(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := NewFromProvider(mp)
	m.Publisher("s1").Failure(context.Background(), DestinationCanonical, OperationCanonicalPublish, nats.ErrTimeout)

	rm := collect(t, reader)
	var scopes []string
	for _, scope := range rm.ScopeMetrics {
		scopes = append(scopes, scope.Scope.Name)
	}
	assert.Contains(t, scopes, ScopeName)
}
