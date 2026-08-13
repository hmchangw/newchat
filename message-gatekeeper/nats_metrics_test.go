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

func TestCanonicalPublishError_PreservesClassificationAndCause(t *testing.T) {
	cause := errors.New("stream unavailable")
	err := &canonicalPublishError{cause: cause}

	assert.ErrorIs(t, err, errCanonicalPublish)
	assert.ErrorIs(t, err, cause)
}

func TestGatekeeperMetrics_BoundedResults(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newGatekeeperMetrics(mp.Meter("test"))
	m.Record(context.Background(), "accepted", "none")
	m.Record(context.Background(), "dynamic", "secret error text")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "message_gatekeeper_messages_total" {
				continue
			}
			sum := metric.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["result"]+"/"+values["reason"]] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"accepted/none": 1, "failed/unknown": 1}, got)
}
