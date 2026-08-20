package natsmetrics

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A disabled Metrics must collapse to the zero-value recorders, not to no-op
// instruments. o11y builds no-op providers when O11Y_ENABLED is false, and a
// no-op instrument still costs a map lookup and two allocations per call — the
// `metrics == nil` short-circuit is what makes "off" actually free, so it has
// to be reachable when the operator turns metrics off.
func TestNewFromProviderIfEnabled_DisabledCollapsesToZeroValue(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	m := NewFromProviderIfEnabled(mp, false)

	publisher := m.Publisher("site-a")
	assert.Nil(t, publisher.metrics, "a disabled Publisher must take the zero-value path")

	consumer := m.Consumer(ConsumerConfig{Site: "site-a", Stream: "ROOMS-site-a", Consumer: "room-worker"})
	require.NotNil(t, consumer, "a disabled Consumer must still be usable, just inert")

	ctx := context.Background()
	publisher.Failure(ctx, DestinationCanonical, OperationCanonicalPublish, nil)
	publisher.Request(ctx, OperationHistoryRead, time.Millisecond, nil)
	publisher.HandledRequest(ctx, OperationMemberRead, time.Millisecond, RequestSuccess)
	consumer.LoopStarted(ctx)
	consumer.LoopStopped(ctx)
	consumer.Terminal(ctx, EventUnknown, TerminalInternal)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Empty(t, rm.ScopeMetrics, "a disabled Metrics must not register or record any instrument")
}

// The enabled path is unchanged: instruments are live and record.
func TestNewFromProviderIfEnabled_EnabledRecords(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	m := NewFromProviderIfEnabled(mp, true)
	m.Publisher("site-a").Failure(context.Background(), DestinationCanonical, OperationCanonicalPublish, nats.ErrTimeout)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.NotEmpty(t, rm.ScopeMetrics)
	assert.NotEmpty(t, metricPoints[int64](t, rm, "chat.nats.publish.failures"))
}

// A disabled Consumer must survive the whole message lifecycle, because the
// worker loops call Track/Finish unconditionally.
func TestDisabledConsumer_TracksWithoutRecording(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	consumer := NewFromProviderIfEnabled(mp, false).
		Consumer(ConsumerConfig{Site: "site-a", Stream: "ROOMS-site-a", Consumer: "room-worker"})

	ctx := context.Background()
	tracked := consumer.Track(ctx, &fakeMsg{}, EventMemberAdd, 5)
	require.NotNil(t, tracked, "Track must return a usable Message even when disabled")
	require.NoError(t, tracked.Ack())
	tracked.Finish(ctx)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Empty(t, rm.ScopeMetrics, "a disabled Consumer must not record dispositions")
}
