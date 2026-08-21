package main

import (
	"context"

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
	flushRequestFailed  flushOutcome = "request_failed"
	flushResultMismatch flushOutcome = "result_mismatch"
)

var allFlushOutcomes = []flushOutcome{flushSuccess, flushRequestFailed, flushResultMismatch}

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
}

func newSyncMetrics(meter metric.Meter) *syncMetrics {
	noopMeter := noop.NewMeterProvider().Meter("search-sync-fallback")
	// The o11y SDK only registers bucket views for its own instrument names, so
	// latency histograms carry the shared boundaries as an instrument advisory.
	latency := metric.WithExplicitBucketBoundaries(o11y.DefaultLatencyBuckets()...)

	flushDuration, err := meter.Float64Histogram("search_sync_worker_bulk_flush_duration",
		metric.WithDescription("ES bulk flush round-trip duration by outcome."), metric.WithUnit("s"), latency)
	if err != nil {
		flushDuration, _ = noopMeter.Float64Histogram("search_sync_worker_bulk_flush_duration")
	}
	flushActions, err := meter.Int64Histogram("search_sync_worker_bulk_flush_actions",
		metric.WithDescription("ES bulk actions attempted per flush."),
		metric.WithExplicitBucketBoundaries(1, 10, 50, 100, 250, 500, 1000, 2000))
	if err != nil {
		flushActions, _ = noopMeter.Int64Histogram("search_sync_worker_bulk_flush_actions")
	}
	itemFailures, err := meter.Int64Counter("search_sync_worker_bulk_item_failures",
		metric.WithDescription("ES bulk items that failed, by action type and bounded status."))
	if err != nil {
		itemFailures, _ = noopMeter.Int64Counter("search_sync_worker_bulk_item_failures")
	}
	messages, err := meter.Int64Counter("search_sync_worker_messages",
		metric.WithDescription("JetStream delivery attempts by disposition; a nakked delivery recurs on each redelivery."))
	if err != nil {
		messages, _ = noopMeter.Int64Counter("search_sync_worker_messages")
	}
	parentResolve, err := meter.Float64Histogram("search_sync_worker_parent_resolve_duration",
		metric.WithDescription("Thread-parent createdAt ES self-lookup duration."), metric.WithUnit("s"), latency)
	if err != nil {
		parentResolve, _ = noopMeter.Float64Histogram("search_sync_worker_parent_resolve_duration")
	}

	return &syncMetrics{
		flushDuration: flushDuration,
		flushActions:  flushActions,
		itemFailures:  itemFailures,
		messages:      messages,
		parentResolve: parentResolve,
	}
}

// collectionMetrics binds syncMetrics to one collection's bounded label sets.
type collectionMetrics struct {
	m             *syncMetrics
	collection    string
	actionsOpt    metric.MeasurementOption
	resolvedOpt   metric.MeasurementOption
	unresolvedOpt metric.MeasurementOption
	flushOpts     map[flushOutcome]metric.MeasurementOption
	msgOpts       map[msgDisposition]metric.MeasurementOption
	// itemFailureOpts is keyed by the closed action set crossed with the
	// bounded status labels bulkStatusLabel can return.
	itemFailureOpts map[itemFailureKey]metric.MeasurementOption
}

type itemFailureKey struct {
	action string
	status string
}

// allBulkActions is the closed set of actions this worker emits, and
// allBulkStatusLabels is every value bulkStatusLabel can return.
var (
	allBulkActions      = []string{"index", "delete", "update"}
	allBulkStatusLabels = []string{"400", "404", "409", "413", "429", "500", "503", "4xx", "5xx", "other"}
)

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
	itemFailureOpts := make(map[itemFailureKey]metric.MeasurementOption, len(allBulkActions)*len(allBulkStatusLabels))
	for _, action := range allBulkActions {
		for _, status := range allBulkStatusLabels {
			itemFailureOpts[itemFailureKey{action, status}] = metric.WithAttributeSet(attribute.NewSet(
				attribute.String("collection", name),
				attribute.String("action", action),
				attribute.String("status", status),
			))
		}
	}
	return &collectionMetrics{
		m:          m,
		collection: name,
		actionsOpt: metric.WithAttributeSet(attribute.NewSet(attribute.String("collection", name))),
		resolvedOpt: metric.WithAttributeSet(attribute.NewSet(
			attribute.String("collection", name), attribute.String("outcome", "resolved"))),
		unresolvedOpt: metric.WithAttributeSet(attribute.NewSet(
			attribute.String("collection", name), attribute.String("outcome", "unresolved"))),
		flushOpts:       flushOpts,
		msgOpts:         msgOpts,
		itemFailureOpts: itemFailureOpts,
	}
}

// recordParentResolve is nil-safe so an unwired resolver stays a no-op.
func (c *collectionMetrics) recordParentResolve(ctx context.Context, seconds float64, resolved bool) {
	if c == nil {
		return
	}
	opt := c.unresolvedOpt
	if resolved {
		opt = c.resolvedOpt
	}
	c.m.parentResolve.Record(ctx, seconds, opt)
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

// recordItemFailure counts one failed bulk item.
//
// The attribute set is precomputed, like every other recorder here. This path
// is only quiet while Elasticsearch is healthy: under the 429 backpressure
// these metrics exist to diagnose it runs once per rejected action — up to
// BULK_BATCH_SIZE per flush, and a Teams migration batch contributes one
// action per migrated message, so a single source message can drive the whole
// inner loop. Building a set per call there costs 264 ns and an allocation
// each, against 9.6 ns for a map lookup, exactly when the system is already
// under strain.
func (c *collectionMetrics) recordItemFailure(ctx context.Context, action string, status int) {
	if c == nil {
		return
	}
	if opt, ok := c.itemFailureOpts[itemFailureKey{action, bulkStatusLabel(status)}]; ok {
		c.m.itemFailures.Add(ctx, 1, opt)
		return
	}
	// An action outside the closed set still counts, without minting a label.
	c.m.itemFailures.Add(ctx, 1, c.actionsOpt)
}

// bulkStatusLabel maps an ES bulk item status to a bounded label value: the
// diagnostically interesting codes stay exact, everything else buckets by class.
func bulkStatusLabel(status int) string {
	// Returned as literals rather than strconv.Itoa: Go only interns small
	// integers, so Itoa(429) allocates on every rejected item — which is
	// exactly the case this label exists for.
	switch status {
	case 400:
		return "400"
	case 404:
		return "404"
	case 409:
		return "409"
	case 413:
		return "413"
	case 429:
		return "429"
	case 500:
		return "500"
	case 503:
		return "503"
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
