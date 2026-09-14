package natsmetrics

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

// A successful publish records nothing. The broker already counts JetStream
// acceptances as jetstream_stream_total_messages, and a Core NATS success only
// means the message entered the client's write buffer — neither is worth a
// series, and both were being paid for on every publish including inside the
// broadcast and room-key fan-out loops.
func TestPublisher_Failure_SuccessRecordsNothing(t *testing.T) {
	m, reader := newTestMetrics(t)
	p := m.Publisher("site-a")

	p.Failure(context.Background(), DestinationCanonical, OperationCanonicalPublish, nil)

	rm := collect(t, reader)
	assert.Empty(t, metricPoints[int64](t, rm, "chat.nats.publish.failures"),
		"a successful publish must not create a series")
}

// Failures keep their bounded cause. The broker never sees a publish that
// failed, so this is the only place the failure exists.
func TestPublisher_Failure_RecordsBoundedCause(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{name: "timeout", err: nats.ErrTimeout, wantReason: "timeout"},
		{name: "no responders", err: nats.ErrNoResponders, wantReason: "no_responders"},
		{name: "disconnected", err: nats.ErrConnectionClosed, wantReason: "disconnected"},
		{name: "reconnect buffer overflow", err: nats.ErrReconnectBufExceeded, wantReason: "buffer_full"},
		{name: "unmapped error", err: errors.New("dynamic error text"), wantReason: "other_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMetrics(t)
			p := m.Publisher("site-a")

			p.Failure(context.Background(), DestinationCanonical, OperationCanonicalPublish, tt.err)

			rm := collect(t, reader)
			points := metricPoints[int64](t, rm, "chat.nats.publish.failures")
			require.Len(t, points, 1)
			assert.Equal(t, int64(1), points[0].Value)
			attrs := attrs(points[0])
			assert.Equal(t, tt.wantReason, attrs["outcome"])
			assert.Equal(t, "canonical", attrs["destination_kind"])
			assert.NotContains(t, attrs, "error", "the raw error text must never become a label")
		})
	}
}

// The instrument no longer carries a success value at all, so the closed enum
// cannot mint one and a query for the family means "something failed".
func TestPublishOutcomes_CarryNoSuccessValue(t *testing.T) {
	for _, outcome := range allPublishOutcomes {
		assert.NotEqual(t, PublishOutcome("success"), outcome,
			"publish outcomes are failure causes; success is not recorded")
	}
}

// A disabled publisher stays inert on the failure path too.
func TestPublisher_Failure_DisabledRecordsNothing(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	p := NewFromProviderIfEnabled(mp, false).Publisher("site-a")

	p.Failure(context.Background(), DestinationCanonical, OperationCanonicalPublish, nats.ErrTimeout)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.Empty(t, rm.ScopeMetrics)
}
