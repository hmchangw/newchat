package main

import (
	"context"
	"strconv"

	"github.com/flywindy/o11y"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// metricsScope is the instrumentation scope for every instrument in this file.
const metricsScope = "github.com/hmchangw/chat/search-sync-worker"

// flushOutcome classifies one ES bulk flush round-trip. Per-item failures are
// counted separately — a flush whose request succeeded is "success" even when
// some items inside it failed.
type flushOutcome string

const (
	flushSuccess        flushOutcome = "success"
	flushRequestError   flushOutcome = "request_error"
	flushResultMismatch flushOutcome = "result_mismatch"
)

var allFlushOutcomes = []flushOutcome{flushSuccess, flushRequestError, flushResultMismatch}

// msgDisposition is a terminal (outcome, reason) pair for one source JetStream
// message. Every label value is a closed enum so attribute sets are precomputed
// once per collection and looked up by value on the hot path.
type msgDisposition struct {
	outcome string // acked | nakked
	reason  string
}

var (
	dispAckedSuccess         = msgDisposition{"acked", "success"}
	dispAckedFiltered        = msgDisposition{"acked", "filtered"}
	dispAckedPoison          = msgDisposition{"acked", "poison"}
	dispNakkedRequestFailed  = msgDisposition{"nakked", "request_failed"}
	dispNakkedItemFailed     = msgDisposition{"nakked", "item_failed"}
	dispNakkedResultMismatch = msgDisposition{"nakked", "result_mismatch"}
)

var allDispositions = []msgDisposition{
	dispAckedSuccess, dispAckedFiltered, dispAckedPoison,
	dispNakkedRequestFailed, dispNakkedItemFailed, dispNakkedResultMismatch,
}

// syncMetrics owns the worker's ES-sync instruments. Instrument-creation
// failures fall back to no-op instruments so telemetry can never block startup.
type syncMetrics struct {
	flushDuration metric.Float64Histogram
	flushActions  metric.Int64Histogram
	itemFailures  metric.Int64Counter
	messages      metric.Int64Counter
	parentResolve metric.Float64Histogram
	resolvedOpt   metric.MeasurementOption
	unresolvedOpt metric.MeasurementOption
}

func newSyncMetrics(meter metric.Meter) *syncMetrics {
	noopMeter := noop.NewMeterProvider().Meter("search-sync-fallback")
	// The o11y SDK only registers bucket views for its own instrument names, so
	// latency histograms carry the shared boundaries as an instrument advisory.
	latency := metric.WithExplicitBucketBoundaries(o11y.DefaultLatencyBuckets()...)

	flushDuration, err := meter.Float64Histogram("chat.search.sync.bulk.flush.duration",
		metric.WithDescription("ES bulk flush round-trip duration by outcome."), metric.WithUnit("s"), latency)
	if err != nil {
		flushDuration, _ = noopMeter.Float64Histogram("chat.search.sync.bulk.flush.duration")
	}
	flushActions, err := meter.Int64Histogram("chat.search.sync.bulk.flush.actions",
		metric.WithDescription("ES bulk actions attempted per flush."),
		metric.WithExplicitBucketBoundaries(1, 10, 50, 100, 250, 500, 1000, 2000))
	if err != nil {
		flushActions, _ = noopMeter.Int64Histogram("chat.search.sync.bulk.flush.actions")
	}
	itemFailures, err := meter.Int64Counter("chat.search.sync.bulk.item.failures",
		metric.WithDescription("ES bulk items that failed, by action type and bounded status."))
	if err != nil {
		itemFailures, _ = noopMeter.Int64Counter("chat.search.sync.bulk.item.failures")
	}
	messages, err := meter.Int64Counter("chat.search.sync.messages",
		metric.WithDescription("Source JetStream messages by terminal disposition."))
	if err != nil {
		messages, _ = noopMeter.Int64Counter("chat.search.sync.messages")
	}
	parentResolve, err := meter.Float64Histogram("chat.search.sync.parent.resolve.duration",
		metric.WithDescription("Thread-parent createdAt ES self-lookup duration."), metric.WithUnit("s"), latency)
	if err != nil {
		parentResolve, _ = noopMeter.Float64Histogram("chat.search.sync.parent.resolve.duration")
	}

	return &syncMetrics{
		flushDuration: flushDuration,
		flushActions:  flushActions,
		itemFailures:  itemFailures,
		messages:      messages,
		parentResolve: parentResolve,
		resolvedOpt:   metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", "resolved"))),
		unresolvedOpt: metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", "unresolved"))),
	}
}

// recordParentResolve is nil-safe so an unwired resolver stays a no-op.
func (m *syncMetrics) recordParentResolve(ctx context.Context, seconds float64, resolved bool) {
	if m == nil {
		return
	}
	opt := m.unresolvedOpt
	if resolved {
		opt = m.resolvedOpt
	}
	m.parentResolve.Record(ctx, seconds, opt)
}

// collectionMetrics binds syncMetrics to one collection's bounded label sets.
type collectionMetrics struct {
	m          *syncMetrics
	collection string
	actionsOpt metric.MeasurementOption
	flushOpts  map[flushOutcome]metric.MeasurementOption
	msgOpts    map[msgDisposition]metric.MeasurementOption
}

func (m *syncMetrics) forCollection(name string) *collectionMetrics {
	flushOpts := make(map[flushOutcome]metric.MeasurementOption, len(allFlushOutcomes))
	for _, o := range allFlushOutcomes {
		flushOpts[o] = metric.WithAttributeSet(attribute.NewSet(
			attribute.String("collection", name),
			attribute.String("outcome", string(o)),
		))
	}
	msgOpts := make(map[msgDisposition]metric.MeasurementOption, len(allDispositions))
	for _, d := range allDispositions {
		msgOpts[d] = metric.WithAttributeSet(attribute.NewSet(
			attribute.String("collection", name),
			attribute.String("outcome", d.outcome),
			attribute.String("reason", d.reason),
		))
	}
	return &collectionMetrics{
		m:          m,
		collection: name,
		actionsOpt: metric.WithAttributeSet(attribute.NewSet(attribute.String("collection", name))),
		flushOpts:  flushOpts,
		msgOpts:    msgOpts,
	}
}

// recordFlush records one bulk flush round-trip and the batch size it attempted.
func (c *collectionMetrics) recordFlush(ctx context.Context, outcome flushOutcome, seconds float64, actions int) {
	if c == nil {
		return
	}
	c.m.flushDuration.Record(ctx, seconds, c.flushOpts[outcome])
	c.m.flushActions.Record(ctx, int64(actions), c.actionsOpt)
}

// recordMessages counts n source messages sharing one terminal disposition.
func (c *collectionMetrics) recordMessages(ctx context.Context, d msgDisposition, n int) {
	if c == nil || n == 0 {
		return
	}
	c.m.messages.Add(ctx, int64(n), c.msgOpts[d])
}

// recordItemFailure counts one failed bulk item. This is a cold path, so the
// attribute set is built per call instead of precomputed per status.
func (c *collectionMetrics) recordItemFailure(ctx context.Context, action string, status int) {
	if c == nil {
		return
	}
	c.m.itemFailures.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("collection", c.collection),
		attribute.String("action", action),
		attribute.String("status", bulkStatusLabel(status)),
	)))
}

// bulkStatusLabel maps an ES bulk item status to a bounded label value: the
// diagnostically interesting codes stay exact, everything else buckets by class.
func bulkStatusLabel(status int) string {
	switch status {
	case 400, 404, 409, 413, 429, 500, 503:
		return strconv.Itoa(status)
	}
	switch {
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}
