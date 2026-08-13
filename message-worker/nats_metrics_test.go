package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPersistenceMetrics_BoundedLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newPersistenceMetrics(mp.Meter("test"))
	m.Record(context.Background(), "user", "success")
	m.Record(context.Background(), "dynamic-id", "raw error")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "message_worker_persistence_total" {
				continue
			}
			for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["message_kind"]+"/"+values["result"]] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"user/success": 1, "unknown/error": 1}, got)
}
