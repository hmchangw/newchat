package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metrics records archive-worker's OpenTelemetry instruments. A nil *metrics
// (or a zero value) is a valid no-op so tests can build lanes without one.
type metrics struct {
	segmentsTotal  metric.Int64Counter
	segmentBytes   metric.Int64Histogram
	eventsTotal    metric.Int64Counter
	writeFailures  metric.Int64Counter
	blobsTotal     metric.Int64Counter
	blobBytesTotal metric.Int64Counter
	redeliveries   metric.Int64Counter
}

// newMetrics resolves instruments from the global MeterProvider, which
// obs.Init installs; before that (or with metrics off) they are no-ops.
func newMetrics() (*metrics, error) {
	return newMetricsFrom(otel.Meter("archive-worker"))
}

func newMetricsFrom(meter metric.Meter) (*metrics, error) {
	var m metrics
	var err error
	if m.segmentsTotal, err = meter.Int64Counter("archive_segments_total", metric.WithDescription("Segments written to the archive bucket.")); err != nil {
		return nil, fmt.Errorf("metric archive_segments_total: %w", err)
	}
	if m.segmentBytes, err = meter.Int64Histogram("archive_segment_bytes", metric.WithUnit("By"), metric.WithDescription("Encrypted size of each segment written.")); err != nil {
		return nil, fmt.Errorf("metric archive_segment_bytes: %w", err)
	}
	if m.eventsTotal, err = meter.Int64Counter("archive_events_total", metric.WithDescription("Events settled, by outcome.")); err != nil {
		return nil, fmt.Errorf("metric archive_events_total: %w", err)
	}
	if m.writeFailures, err = meter.Int64Counter("archive_write_failures_total", metric.WithDescription("Failed archive writes after retries, by store.")); err != nil {
		return nil, fmt.Errorf("metric archive_write_failures_total: %w", err)
	}
	if m.blobsTotal, err = meter.Int64Counter("archive_blobs_total", metric.WithDescription("Attachments processed, by outcome.")); err != nil {
		return nil, fmt.Errorf("metric archive_blobs_total: %w", err)
	}
	if m.blobBytesTotal, err = meter.Int64Counter("archive_blob_bytes_total", metric.WithUnit("By"), metric.WithDescription("Bytes of attachments archived.")); err != nil {
		return nil, fmt.Errorf("metric archive_blob_bytes_total: %w", err)
	}
	if m.redeliveries, err = meter.Int64Counter("archive_redeliveries_total", metric.WithDescription("Messages delivered more than once.")); err != nil {
		return nil, fmt.Errorf("metric archive_redeliveries_total: %w", err)
	}
	return &m, nil
}

func (m *metrics) segments(n, bytes int) {
	if m == nil || m.segmentsTotal == nil {
		return
	}
	m.segmentsTotal.Add(context.Background(), int64(n))
	m.segmentBytes.Record(context.Background(), int64(bytes))
}

func (m *metrics) events(outcome string, n int) {
	if m == nil || m.eventsTotal == nil {
		return
	}
	m.eventsTotal.Add(context.Background(), int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
}

func (m *metrics) writeFailure(store string) {
	if m == nil || m.writeFailures == nil {
		return
	}
	m.writeFailures.Add(context.Background(), 1, metric.WithAttributes(attribute.String("store", store)))
}

func (m *metrics) blobs(outcome string, bytes int64) {
	if m == nil || m.blobsTotal == nil {
		return
	}
	m.blobsTotal.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	if bytes > 0 {
		m.blobBytesTotal.Add(context.Background(), bytes)
	}
}

func (m *metrics) redelivered(n int) {
	if m == nil || m.redeliveries == nil {
		return
	}
	m.redeliveries.Add(context.Background(), int64(n))
}

// noteRedelivery counts a message the server is delivering again. A message
// whose metadata cannot be read is left to the caller's own poison handling.
func noteRedelivery(m *metrics, msg jetstream.Msg) {
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 1 {
		m.redelivered(1)
	}
}
