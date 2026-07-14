package cachemetrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// counterValue returns the value of the named Int64 sum counter whose
// datapoint carries exactly the given attributes, or -1 if not found.
func counterValue(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	want := attribute.NewSet(attrs...)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s is not Sum[int64]", name)
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					return dp.Value
				}
			}
		}
	}
	return -1
}

func collect(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func newTestMetrics(t *testing.T) (*Metrics, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := New(mp.Meter("test"))
	require.NoError(t, err)
	return m, reader
}

func TestRecorder_Hit(t *testing.T) {
	ctx := context.Background()
	m, reader := newTestMetrics(t)
	rec := m.For("roomsub", "l2")

	rec.Hit(ctx)
	rec.Hit(ctx)

	rm := collect(t, reader)
	got := counterValue(t, rm, "cache_hits_total",
		attribute.String("cache", "roomsub"), attribute.String("tier", "l2"))
	assert.Equal(t, int64(2), got)
}

func TestRecorder_Miss(t *testing.T) {
	ctx := context.Background()
	m, reader := newTestMetrics(t)
	rec := m.For("roommeta", "l2")

	rec.Miss(ctx)

	rm := collect(t, reader)
	got := counterValue(t, rm, "cache_misses_total",
		attribute.String("cache", "roommeta"), attribute.String("tier", "l2"))
	assert.Equal(t, int64(1), got)
}

func TestRecorder_Error(t *testing.T) {
	ctx := context.Background()
	m, reader := newTestMetrics(t)
	rec := m.For("roomsub", "l2")

	rec.Error(ctx)

	rm := collect(t, reader)
	got := counterValue(t, rm, "cache_errors_total",
		attribute.String("cache", "roomsub"), attribute.String("tier", "l2"))
	assert.Equal(t, int64(1), got)
}

// Each (cache, tier) pair is tracked as its own datapoint so a single
// dashboard query can break hit rate down by label.
func TestRecorder_SeparatesByCacheAndTier(t *testing.T) {
	ctx := context.Background()
	m, reader := newTestMetrics(t)

	m.For("roomsub", "l2").Hit(ctx)
	m.For("roommeta", "l2").Hit(ctx)

	rm := collect(t, reader)
	assert.Equal(t, int64(1), counterValue(t, rm, "cache_hits_total",
		attribute.String("cache", "roomsub"), attribute.String("tier", "l2")))
	assert.Equal(t, int64(1), counterValue(t, rm, "cache_hits_total",
		attribute.String("cache", "roommeta"), attribute.String("tier", "l2")))
}

// The package-default recorder is safe to use when no MeterProvider has been
// installed (instruments degrade to no-ops); recording must never panic.
func TestPackageDefault_For_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		rec := For("roomsub", "l2")
		rec.Hit(context.Background())
		rec.Miss(context.Background())
		rec.Error(context.Background())
	})
}
