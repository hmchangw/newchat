package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/loopguard"
)

func TestNewMetrics(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)
	// With the global no-op meter these must not panic.
	m.segments(1, 10)
	m.events(sourceEvents, outcomeArchived, 2)
	m.writeFailure("bucket")
	m.blobs("archived", 5)
	m.redelivered(1)
	m.lag(sourceEvents, time.Second)
	m.blobLag(time.Second)
}

func TestMetrics_NilAndZeroAreSafe(t *testing.T) {
	record := func(m *metrics) {
		m.segments(1, 10)
		m.events(sourceMembers, outcomeArchived, 1)
		m.writeFailure("index")
		m.blobs("archived", 1)
		m.redelivered(1)
		m.lag(sourceMembers, time.Second)
		m.blobLag(time.Second)
	}
	assert.NotPanics(t, func() { record(nil) })
	assert.NotPanics(t, func() { record(&metrics{}) })
}

// failMeter fails creating the one instrument named failName.
type failMeter struct {
	noop.Meter
	failName string
}

var errInstrument = errors.New("instrument refused")

func (f failMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == f.failName {
		return nil, errInstrument
	}
	return f.Meter.Int64Counter(name, opts...)
}

func (f failMeter) Int64Histogram(name string, opts ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	if name == f.failName {
		return nil, errInstrument
	}
	return f.Meter.Int64Histogram(name, opts...)
}

func (f failMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == f.failName {
		return nil, errInstrument
	}
	return f.Meter.Float64Histogram(name, opts...)
}

func TestNewMetricsFrom_InstrumentErrors(t *testing.T) {
	for _, name := range []string{
		"archive_segments_total", "archive_segment_bytes", "archive_events_total", "archive_write_failures_total",
		"archive_blobs_total", "archive_blob_bytes_total", "archive_redeliveries_total",
		"archive_lag_seconds", "archive_blob_lag_seconds",
	} {
		t.Run(name, func(t *testing.T) {
			m, err := newMetricsFrom(failMeter{failName: name})
			require.Error(t, err)
			assert.Nil(t, m)
			assert.ErrorIs(t, err, errInstrument)
			assert.Contains(t, err.Error(), name)
		})
	}
}

func TestMetrics_RecordsInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m, err := newMetricsFrom(provider.Meter("archive-worker"))
	require.NoError(t, err)

	m.segments(2, 100)
	m.events(sourceEvents, outcomeArchived, 3)
	m.events(sourceMembers, outcomeArchived, 2)
	m.events(sourceMembers, outcomePoison, 1)
	m.events(sourceEvents, outcomeConflict, 1)
	m.events("bogus", "bogus", 7)
	m.writeFailure("bucket")
	m.blobs("archived", 50)
	m.blobs("skipped_size", 0)
	m.blobs("exists", 0)
	m.redelivered(4)
	m.lag(sourceEvents, 1500*time.Millisecond)
	m.lag(sourceMembers, 2*time.Second)
	m.blobLag(3 * time.Second)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	sums := map[string]int64{}
	byAttr := map[string]int64{}
	var histCount uint64
	lagSums := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			switch d := mt.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					sums[mt.Name] += dp.Value
					key := mt.Name
					if v, ok := dp.Attributes.Value(attribute.Key("source")); ok {
						key += "/" + v.AsString()
					}
					if v, ok := dp.Attributes.Value(attribute.Key("outcome")); ok {
						key += "/" + v.AsString()
					}
					if v, ok := dp.Attributes.Value(attribute.Key("store")); ok {
						key += "/" + v.AsString()
					}
					byAttr[key] = dp.Value
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					histCount += dp.Count
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					key := mt.Name
					if v, ok := dp.Attributes.Value(attribute.Key("source")); ok {
						key += "/" + v.AsString()
					}
					lagSums[key] += dp.Sum
				}
			}
		}
	}
	assert.Equal(t, int64(2), sums["archive_segments_total"])
	assert.Equal(t, uint64(1), histCount)
	assert.Equal(t, int64(3), byAttr["archive_events_total/events/archived"])
	assert.Equal(t, int64(2), byAttr["archive_events_total/members/archived"])
	assert.Equal(t, int64(1), byAttr["archive_events_total/members/poison"])
	assert.Equal(t, int64(1), byAttr["archive_events_total/events/conflict"])
	assert.Equal(t, int64(7), byAttr["archive_events_total/unknown/unknown"], "an unlisted label pair is counted, never dropped or minted")
	assert.Equal(t, int64(1), byAttr["archive_blobs_total/exists"])
	assert.InDelta(t, 1.5, lagSums["archive_lag_seconds/events"], 1e-9)
	assert.InDelta(t, 2.0, lagSums["archive_lag_seconds/members"], 1e-9)
	assert.InDelta(t, 3.0, lagSums["archive_blob_lag_seconds"], 1e-9)
	assert.Equal(t, int64(1), byAttr["archive_write_failures_total/bucket"])
	assert.Equal(t, int64(0), byAttr["archive_write_failures_total/index"])
	assert.Equal(t, int64(1), byAttr["archive_blobs_total/archived"])
	assert.Equal(t, int64(1), byAttr["archive_blobs_total/skipped_size"])
	assert.Equal(t, int64(50), sums["archive_blob_bytes_total"])
	assert.Equal(t, int64(4), sums["archive_redeliveries_total"])
}

// redeliveryCount reads archive_redeliveries_total from a manual reader.
func redeliveryCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if d, ok := mt.Data.(metricdata.Sum[int64]); ok && mt.Name == "archive_redeliveries_total" {
				for _, dp := range d.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func TestNoteRedelivery(t *testing.T) {
	tests := []struct {
		name      string
		delivered uint64
		want      int64
	}{
		{"first delivery is not counted", 1, 0},
		{"second delivery is counted", 2, 1},
		{"later delivery is counted once", 9, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			m, err := newMetricsFrom(provider.Meter("archive-worker"))
			require.NoError(t, err)

			noteRedelivery(m, &fakeMsg{seq: 1, stream: "S", delivered: tt.delivered})
			assert.Equal(t, tt.want, redeliveryCount(t, reader))
		})
	}
	t.Run("nil metrics is a no-op", func(t *testing.T) {
		noteRedelivery(nil, &fakeMsg{seq: 1, delivered: 5})
	})
}

func TestLane_Handle_CountsRedelivery(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m, err := newMetricsFrom(provider.Meter("archive-worker"))
	require.NoError(t, err)

	cfg := laneCfg()
	cfg.metrics = m
	l := newLane(cfg, nil, buildEventItem, testCipher(t), newBatcher(10, 1<<20, time.Hour),
		newFlusher(&fakeObjects{}, &fakeIndex{}, cfgFast(), nil), loopguard.New("t", func() {}))
	msg := eventMsg(t, 1, loadEvents(t)["created"])
	msg.delivered = 3
	l.handle(context.Background(), context.Background(), msg)
	assert.Equal(t, int64(1), redeliveryCount(t, reader))
}

func TestBlobLane_Handle_CountsRedelivery(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m, err := newMetricsFrom(provider.Meter("archive-worker"))
	require.NoError(t, err)

	l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, workers: 1, now: time.Now},
		nil, &fakeSource{}, &fakeObjects{}, &recordingIndex{}, testCipher(t), loopguard.New("b", func() {}), m)
	msg := eventMsg(t, 1, loadEvents(t)["deleted"]) // no attachments: acked after the redelivery check
	msg.delivered = 2
	l.handle(context.Background(), msg)
	assert.Equal(t, int64(1), redeliveryCount(t, reader))
}

// testMetrics returns metrics backed by a manual reader.
func testMetrics(t *testing.T) (*metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m, err := newMetricsFrom(provider.Meter("archive-worker"))
	require.NoError(t, err)
	return m, reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

// eventCount reads archive_events_total{source,outcome}.
func eventCount(t *testing.T, reader *sdkmetric.ManualReader, source, outcome string) int64 {
	t.Helper()
	var total int64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, mt := range sm.Metrics {
			d, ok := mt.Data.(metricdata.Sum[int64])
			if !ok || mt.Name != "archive_events_total" {
				continue
			}
			for _, dp := range d.DataPoints {
				s, _ := dp.Attributes.Value("source")
				o, _ := dp.Attributes.Value("outcome")
				if s.AsString() == source && o.AsString() == outcome {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// blobCount reads archive_blobs_total{outcome}.
func blobCount(t *testing.T, reader *sdkmetric.ManualReader, outcome string) int64 {
	t.Helper()
	var total int64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, mt := range sm.Metrics {
			d, ok := mt.Data.(metricdata.Sum[int64])
			if !ok || mt.Name != "archive_blobs_total" {
				continue
			}
			for _, dp := range d.DataPoints {
				if o, _ := dp.Attributes.Value("outcome"); o.AsString() == outcome {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// lagSum reads the sum of a lag histogram, filtered by source when given.
func lagSum(t *testing.T, reader *sdkmetric.ManualReader, name, source string) float64 {
	t.Helper()
	var total float64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, mt := range sm.Metrics {
			d, ok := mt.Data.(metricdata.Histogram[float64])
			if !ok || mt.Name != name {
				continue
			}
			for _, dp := range d.DataPoints {
				if s, _ := dp.Attributes.Value("source"); source == "" || s.AsString() == source {
					total += dp.Sum
				}
			}
		}
	}
	return total
}
