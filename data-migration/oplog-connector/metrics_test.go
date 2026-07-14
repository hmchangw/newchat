package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewMetrics(t *testing.T) {
	m, err := newMetrics("messages")
	require.NoError(t, err)
	require.NotNil(t, m)
	// Recording must not panic with a real (no-op exporter) meter.
	m.onPublished(context.Background(), "rocketchat_message", 42)
	m.onPublishError(context.Background(), "rocketchat_message")
	m.onSkipped(context.Background(), "rocketchat_message")
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics // the unit-test case: watcher.metrics is nil
	require.NotPanics(t, func() {
		m.onPublished(context.Background(), "c", 1)
		m.onPublishError(context.Background(), "c")
		m.onSkipped(context.Background(), "c")
	})
}

func TestMetrics_RecordingsCarryRoleAndCollection(t *testing.T) {
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	m, err := newMetrics("collections")
	require.NoError(t, err)
	ctx := context.Background()
	m.onPublished(ctx, "rocketchat_room", 7)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "oplog_events_published_total" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			attrs := sum.DataPoints[0].Attributes
			role, _ := attrs.Value(attribute.Key("role"))
			coll, _ := attrs.Value(attribute.Key("collection"))
			assert.Equal(t, "collections", role.AsString())
			assert.Equal(t, "rocketchat_room", coll.AsString())
			found = true
		}
	}
	assert.True(t, found, "published counter recorded with role+collection")
}
