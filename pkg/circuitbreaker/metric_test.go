package circuitbreaker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestGauge(t *testing.T) (StateGauge, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	g, err := NewStateGauge(mp.Meter("test"))
	require.NoError(t, err)
	return g, reader
}

// gaugeValue returns the value of the breaker-state datapoint carrying the given
// breaker name, or -1 when no such datapoint exists.
func gaugeValue(t *testing.T, reader sdkmetric.Reader, breakerName string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	want := attribute.NewSet(attribute.String("breaker", breakerName))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != StateMetricName {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "metric %s is not Gauge[int64]", m.Name)
			for _, dp := range g.DataPoints {
				if dp.Attributes.Equals(&want) {
					return dp.Value
				}
			}
		}
	}
	return -1
}

// Several breakers in one service share the gauge instrument, so without a
// distinguishing attribute their datapoints overwrite each other and the metric
// describes whichever breaker moved last. Each must get its own series.
func TestStateGauge_TracksBreakersIndependently(t *testing.T) {
	ctx := context.Background()
	g, reader := newTestGauge(t)

	subBreaker := New(1, time.Minute, g.Track(ctx, "subscription"))
	metaBreaker := New(1, time.Minute, g.Track(ctx, "roommeta"))

	// Only the subscription breaker fails.
	require.Error(t, subBreaker.Do(func() error { return errBoom }))
	require.Equal(t, StateOpen, subBreaker.State())

	assert.Equal(t, int64(StateOpen), gaugeValue(t, reader, "subscription"))
	assert.Equal(t, int64(-1), gaugeValue(t, reader, "roommeta"),
		"a breaker that never transitioned must not inherit another's series")

	// The meta breaker's own transition lands on its own series and leaves the
	// subscription series alone.
	require.Error(t, metaBreaker.Do(func() error { return errBoom }))
	assert.Equal(t, int64(StateOpen), gaugeValue(t, reader, "roommeta"))
	assert.Equal(t, int64(StateOpen), gaugeValue(t, reader, "subscription"))
}

// Recovery must be visible, not just the trip — an operator watching the gauge
// needs to see it return to closed.
func TestStateGauge_RecordsRecovery(t *testing.T) {
	ctx := context.Background()
	g, reader := newTestGauge(t)

	b := New(1, testCooldown, g.Track(ctx, "subscription"))
	require.Error(t, b.Do(func() error { return errBoom }))
	require.Equal(t, int64(StateOpen), gaugeValue(t, reader, "subscription"))

	waitPastCooldown()
	require.NoError(t, b.Do(func() error { return nil })) // half-open probe succeeds
	assert.Equal(t, int64(StateClosed), gaugeValue(t, reader, "subscription"))
}

// Services wire breakers through the package-level Tracked, which is backed by
// a gauge that init() always populates — with a noop instrument if the global
// meter provider rejects registration — so recording is safe with no meter
// provider installed at all.
func TestTracked_SafeWithNoMeterProvider(t *testing.T) {
	b := New(1, time.Minute, Tracked(context.Background(), "subscription"))
	assert.ErrorIs(t, b.Do(func() error { return errBoom }), errBoom)
	assert.Equal(t, StateOpen, b.State())
}
