package valkeyutil

import (
	"context"
	"testing"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// swapBreakerStateGauge points the breaker STATE GAUGE at an in-memory reader and
// restores the original on cleanup. Deliberately narrow: breakerTransitions is
// left on the global meter, so do not expect counter assertions to work here.
func swapBreakerStateGauge(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	m := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("valkey")

	gauge, err := m.Int64Gauge("valkey_breaker_state")
	require.NoError(t, err)
	oldState := breakerState
	breakerState = gauge
	t.Cleanup(func() { breakerState = oldState })

	return reader
}

// gaugeValue returns the gauge value for one breaker label, and whether it exists.
func gaugeValue(t *testing.T, reader *sdkmetric.ManualReader, name, breaker string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != name {
				continue
			}
			g, ok := mt.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "%s must be an int64 gauge", name)
			for _, dp := range g.DataPoints {
				if v, found := dp.Attributes.Value(attribute.Key("breaker")); found && v.AsString() == breaker {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func TestStateValue(t *testing.T) {
	tests := []struct {
		name  string
		state gobreaker.State
		want  int64
	}{
		{"closed", gobreaker.StateClosed, 0},
		{"half-open", gobreaker.StateHalfOpen, 1},
		{"open", gobreaker.StateOpen, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, stateValue(tt.state))
		})
	}
}

func TestRecordBreakerTransition_NoPanicWithoutMeterProvider(t *testing.T) {
	// Instruments fall back to no-ops when no MeterProvider is installed;
	// recording must stay safe on the hot path regardless.
	assert.NotPanics(t, func() {
		recordBreakerTransition("test", gobreaker.StateClosed, gobreaker.StateOpen)
	})
}

func TestRecordBreakerState_NoPanicWithoutMeterProvider(t *testing.T) {
	assert.NotPanics(t, func() {
		recordBreakerState("test", gobreaker.StateClosed)
	})
}

// NewBreakerClient must seed the gauge, or a process whose breaker never trips
// exports no valkey_breaker_state series at all — indistinguishable from a dead
// process on a dashboard, when it is in fact the healthy steady state.
func TestNewBreakerClient_SeedsStateGauge(t *testing.T) {
	reader := swapBreakerStateGauge(t)
	NewBreakerClient(&stubClient{}, "seed-test")

	v, ok := gaugeValue(t, reader, "valkey_breaker_state", "seed-test")
	assert.True(t, ok, "a freshly constructed breaker must export its closed state")
	assert.Equal(t, int64(0), v)
}
