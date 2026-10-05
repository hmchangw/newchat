package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Label values. Every label is a closed enum so the attribute sets are built
// once in newMetricsFrom and looked up on the recording path.
const (
	sourceEvents  = "events"
	sourceMembers = "members"

	outcomeArchived = "archived" // every document created or verified, message acked
	outcomeFailed   = "failed"   // a document needs a retry or was rejected
	outcomeNak      = "nak"      // the whole batch was handed back
	outcomeConflict = "conflict" // an existing document holds a different record
	outcomePoison   = "poison"   // undecodable or malformed, terminated
	outcomeSkip     = "skip"     // an event type the archive does not keep, acked

	labelUnknown = "unknown"
)

var (
	eventSources  = []string{sourceEvents, sourceMembers}
	eventOutcomes = []string{outcomeArchived, outcomeFailed, outcomeNak, outcomeConflict, outcomePoison, outcomeSkip}
	failureStores = []string{"bucket", "index"}
	blobOutcomes  = []string{
		"archived", "exists",
		"skipped_" + skipSize, "skipped_" + skipMissing, "skipped_" + skipLegacy, "skipped_" + skipHost,
	}
)

// lagBuckets spans a healthy lag (one fill interval) to a multi-hour outage.
var lagBuckets = []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600}

type eventKey struct{ source, outcome string }

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
	lagSeconds     metric.Float64Histogram
	blobLagSeconds metric.Float64Histogram

	eventOpts   map[eventKey]metric.MeasurementOption
	failureOpts map[string]metric.MeasurementOption
	blobOpts    map[string]metric.MeasurementOption
	lagOpts     map[string]metric.MeasurementOption
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
	if m.eventsTotal, err = meter.Int64Counter("archive_events_total", metric.WithDescription("Events settled, by lane and outcome.")); err != nil {
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
	if m.lagSeconds, err = meter.Float64Histogram("archive_lag_seconds", metric.WithUnit("s"),
		metric.WithDescription("Seconds from an event's time to its archived ack, by lane."), metric.WithExplicitBucketBoundaries(lagBuckets...)); err != nil {
		return nil, fmt.Errorf("metric archive_lag_seconds: %w", err)
	}
	if m.blobLagSeconds, err = meter.Float64Histogram("archive_blob_lag_seconds", metric.WithUnit("s"),
		metric.WithDescription("Seconds from a created event's time to the ack of its archived attachments."), metric.WithExplicitBucketBoundaries(lagBuckets...)); err != nil {
		return nil, fmt.Errorf("metric archive_blob_lag_seconds: %w", err)
	}

	m.eventOpts = make(map[eventKey]metric.MeasurementOption, len(eventSources)*len(eventOutcomes)+1)
	for _, src := range eventSources {
		for _, o := range eventOutcomes {
			m.eventOpts[eventKey{src, o}] = metric.WithAttributeSet(attribute.NewSet(attribute.String("source", src), attribute.String("outcome", o)))
		}
	}
	m.eventOpts[eventKey{labelUnknown, labelUnknown}] = metric.WithAttributeSet(attribute.NewSet(
		attribute.String("source", labelUnknown), attribute.String("outcome", labelUnknown)))
	m.failureOpts = make(map[string]metric.MeasurementOption, len(failureStores)+1)
	for _, s := range append(append([]string(nil), failureStores...), labelUnknown) {
		m.failureOpts[s] = metric.WithAttributeSet(attribute.NewSet(attribute.String("store", s)))
	}
	m.blobOpts = make(map[string]metric.MeasurementOption, len(blobOutcomes)+1)
	for _, o := range append(append([]string(nil), blobOutcomes...), labelUnknown) {
		m.blobOpts[o] = metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", o)))
	}
	m.lagOpts = make(map[string]metric.MeasurementOption, len(eventSources)+1)
	for _, src := range append(append([]string(nil), eventSources...), labelUnknown) {
		m.lagOpts[src] = metric.WithAttributeSet(attribute.NewSet(attribute.String("source", src)))
	}
	return &m, nil
}

// lookup returns the precomputed option for key, or the "unknown" one so an
// unlisted value is still counted without minting a new series per call.
func lookup[K comparable](opts map[K]metric.MeasurementOption, key, unknown K) metric.MeasurementOption {
	if o, ok := opts[key]; ok {
		return o
	}
	return opts[unknown]
}

func (m *metrics) segments(n, bytes int) {
	if m == nil || m.segmentsTotal == nil {
		return
	}
	m.segmentsTotal.Add(context.Background(), int64(n))
	m.segmentBytes.Record(context.Background(), int64(bytes))
}

func (m *metrics) events(source, outcome string, n int) {
	if m == nil || m.eventsTotal == nil || n <= 0 {
		return
	}
	m.eventsTotal.Add(context.Background(), int64(n), lookup(m.eventOpts, eventKey{source, outcome}, eventKey{labelUnknown, labelUnknown}))
}

func (m *metrics) writeFailure(store string) {
	if m == nil || m.writeFailures == nil {
		return
	}
	m.writeFailures.Add(context.Background(), 1, lookup(m.failureOpts, store, labelUnknown))
}

func (m *metrics) blobs(outcome string, bytes int64) {
	if m == nil || m.blobsTotal == nil {
		return
	}
	m.blobsTotal.Add(context.Background(), 1, lookup(m.blobOpts, outcome, labelUnknown))
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

// lag records how long an event waited between its own time and its ack.
func (m *metrics) lag(source string, d time.Duration) {
	if m == nil || m.lagSeconds == nil {
		return
	}
	m.lagSeconds.Record(context.Background(), d.Seconds(), lookup(m.lagOpts, source, labelUnknown))
}

// blobLag records the same for a created event's attachments.
func (m *metrics) blobLag(d time.Duration) {
	if m == nil || m.blobLagSeconds == nil {
		return
	}
	m.blobLagSeconds.Record(context.Background(), d.Seconds())
}

// noteRedelivery counts a message the server is delivering again. A message
// whose metadata cannot be read is left to the caller's own poison handling.
func noteRedelivery(m *metrics, msg jetstream.Msg) {
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 1 {
		m.redelivered(1)
	}
}
