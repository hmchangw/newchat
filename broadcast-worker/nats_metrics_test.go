package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestBroadcastMetrics_BoundedLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newBroadcastMetrics(mp.Meter("test"))
	m.Fanout(context.Background(), "dm", "created", 3)
	m.Delivery(context.Background(), "dynamic-room", "raw-event", errors.New("secret"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var fanoutCount uint64
	var deliveries int64
	var deliveryLabels map[string]string
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch metric.Name {
			case "broadcast_worker_fanout_recipients":
				fanoutCount = metric.Data.(metricdata.Histogram[int64]).DataPoints[0].Count
			case "broadcast_worker_recipient_deliveries_total":
				point := metric.Data.(metricdata.Sum[int64]).DataPoints[0]
				deliveries = point.Value
				deliveryLabels = map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					deliveryLabels[string(attr.Key)] = attr.Value.AsString()
				}
			}
		}
	}
	assert.Equal(t, uint64(1), fanoutCount)
	assert.Equal(t, int64(1), deliveries)
	assert.Equal(t, map[string]string{"room_kind": "unknown", "event_type": "unknown", "result": "failed"}, deliveryLabels)
}
