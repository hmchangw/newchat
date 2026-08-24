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
	m.onProcessed(context.Background(), "insert", "rooms")
	m.onNak(context.Background(), "update", "rooms")
	m.onTerm(context.Background(), "insert", "users")
	m.onSkipped(context.Background(), "other_collection")
	m.onExhausted(context.Background(), "update", "rooms")
	m.onWrite(context.Background(), "rooms", "upsert")
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics
	require.NotPanics(t, func() {
		m.onProcessed(context.Background(), "insert", "rooms")
		m.onNak(context.Background(), "update", "rooms")
		m.onTerm(context.Background(), "insert", "users")
		m.onSkipped(context.Background(), "other_collection")
		m.onExhausted(context.Background(), "update", "rooms")
		m.onWrite(context.Background(), "rooms", "delete")
	})
}

func TestMetrics_WriteCounterCarriesCollectionAndAction(t *testing.T) {
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	m, err := newMetrics()
	require.NoError(t, err)
	ctx := context.Background()
	m.onWrite(ctx, "subscriptions", "delete")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "dr_oplog_worker_writes_total" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			attrs := sum.DataPoints[0].Attributes
			coll, _ := attrs.Value(attribute.Key("collection"))
			action, _ := attrs.Value(attribute.Key("action"))
			assert.Equal(t, "subscriptions", coll.AsString())
			assert.Equal(t, "delete", action.AsString())
			found = true
		}
	}
	assert.True(t, found, "writes counter recorded")
}
