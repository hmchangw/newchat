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
	m, err := newMetrics()
	require.NoError(t, err)
	require.NotNil(t, m)
	// Recording must not panic with a real (no-op exporter) meter.
	m.onProcessed(context.Background(), "insert", "rocketchat_room")
	m.onNak(context.Background(), "update", "rocketchat_room")
	m.onTerm(context.Background(), "insert", "rocketchat_room")
	m.onSkipped(context.Background(), "other_collection")
	m.onExhausted(context.Background(), "update", "rocketchat_room")
	m.onUserSeed(context.Background(), "insert")
	m.onResolveMiss(context.Background(), "user")
	m.onWrite(context.Background(), "company_room_members", "upsert")
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics // the unit-test case: handler's metrics is nil
	require.NotPanics(t, func() {
		m.onProcessed(context.Background(), "insert", "rocketchat_room")
		m.onNak(context.Background(), "update", "rocketchat_room")
		m.onTerm(context.Background(), "insert", "rocketchat_room")
		m.onSkipped(context.Background(), "other_collection")
		m.onExhausted(context.Background(), "update", "rocketchat_room")
		m.onUserSeed(context.Background(), "present")
		m.onResolveMiss(context.Background(), "thread_room")
		m.onWrite(context.Background(), "company_room_members", "delete_noop")
	})
}

// TestMetrics_DispositionCountersCarryCollection verifies the disposition counters are labelled
// by both op and collection, so ops can see which source collection is stuck/poisoning.
func TestMetrics_DispositionCountersCarryCollection(t *testing.T) {
	// Restore the global meter provider so this test doesn't leak its manual reader into siblings.
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	m, err := newMetrics()
	require.NoError(t, err)

	ctx := context.Background()
	m.onProcessed(ctx, "insert", "rocketchat_subscription")
	m.onNak(ctx, "update", "rocketchat_subscription")
	m.onTerm(ctx, "delete", "rocketchat_message")
	m.onExhausted(ctx, "update", "company_thread_subscriptions")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	want := map[string]map[string]string{
		"oplog_collections_transformer_events_processed_total": {"op": "insert", "collection": "rocketchat_subscription"},
		"oplog_collections_transformer_naks_total":             {"op": "update", "collection": "rocketchat_subscription"},
		"oplog_collections_transformer_terms_total":            {"op": "delete", "collection": "rocketchat_message"},
		"oplog_collections_transformer_exhausted_total":        {"op": "update", "collection": "company_thread_subscriptions"},
	}

	found := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			wantAttrs, ok := want[md.Name]
			if !ok {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s should be an int64 sum", md.Name)
			require.Len(t, sum.DataPoints, 1)
			attrs := sum.DataPoints[0].Attributes
			for k, v := range wantAttrs {
				got, present := attrs.Value(attribute.Key(k))
				require.True(t, present, "%s missing attribute %q", md.Name, k)
				assert.Equal(t, v, got.AsString(), "%s attribute %q", md.Name, k)
			}
			found[md.Name] = true
		}
	}
	assert.Len(t, found, len(want), "all disposition counters recorded")
}
