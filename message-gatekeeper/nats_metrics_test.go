package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestCanonicalPublishError_PreservesClassificationAndCause(t *testing.T) {
	cause := errors.New("stream unavailable")
	err := fmt.Errorf("publish to MESSAGES-CANONICAL: %w", errors.Join(errCanonicalPublish, cause))

	assert.ErrorIs(t, err, errCanonicalPublish)
	assert.ErrorIs(t, err, cause)
}

func gatekeeperCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "message_gatekeeper_messages_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["result"]+"/"+values["reason"]] = point.Value
			}
		}
	}
	return got
}

func TestGatekeeperMetrics_BoundedResults(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newGatekeeperMetrics(mp.Meter("test"))

	m.Record(context.Background(), resultAccepted, reasonNone)
	// A value outside the enum can only arrive via a conversion; it must still
	// collapse instead of minting an unbounded series.
	m.Record(context.Background(), gatekeeperResult("dynamic"), gatekeeperReasonCode("secret error text"))

	assert.Equal(t, map[string]int64{"accepted/none": 1, "failed/unknown": 1}, gatekeeperCounts(t, reader))
}
