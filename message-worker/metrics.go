package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/cassutil"
)

// consumerInfoFunc reads the durable consumer's current state. Injected so the
// lag poller is testable without a live NATS connection.
type consumerInfoFunc func(ctx context.Context) (*jetstream.ConsumerInfo, error)

// metrics holds message-worker's durability instruments. The gauge-backing atomics
// are read by observable callbacks, so the poller never blocks a metric export.
type metrics struct {
	historyWriteFailures metric.Int64Counter
	dropped              metric.Int64Counter
	dropSuppressed       metric.Int64Counter

	// Precomputed one option per label value, as nats_metrics.go does. All three
	// label domains are closed and tiny, and these fire on the failure path — which
	// during an outage is the entire feed plus every redelivery of the backlog — so
	// allocating a KeyValue slice and re-sorting an attribute.Set per call is waste
	// at exactly the moment the service is most loaded.
	classOpts  map[string]metric.MeasurementOption
	codeOpts   map[string]metric.MeasurementOption
	reasonOpts map[string]metric.MeasurementOption

	numPending         atomic.Uint64
	ackFloorAgeSeconds atomic.Int64
	degraded           atomic.Int64
}

func newMetrics() (*metrics, error) {
	m := &metrics{}
	meter := otel.Meter("message-worker")

	failures, err := meter.Int64Counter("message_worker_history_write_failures_total",
		metric.WithDescription("Cassandra history write failures by error class (the trigger for the degraded marker)"))
	if err != nil {
		return nil, fmt.Errorf("history write failures counter: %w", err)
	}
	m.historyWriteFailures = failures

	dropped, err := meter.Int64Counter("message_worker_history_dropped_total",
		metric.WithDescription("messages destroyed after a request-class Cassandra failure outlasted the retry window, by CQL code"))
	if err != nil {
		return nil, fmt.Errorf("history dropped counter: %w", err)
	}
	m.dropped = dropped

	suppressed, err := meter.Int64Counter("message_worker_history_drop_suppressed_total",
		metric.WithDescription("drops withheld by the rate cap or the kill switch, by reason"))
	if err != nil {
		return nil, fmt.Errorf("history drop suppressed counter: %w", err)
	}
	m.dropSuppressed = suppressed

	pending, err := meter.Int64ObservableGauge("message_worker_consumer_num_pending",
		metric.WithDescription("messages on the stream not yet delivered to this consumer"))
	if err != nil {
		return nil, fmt.Errorf("num pending gauge: %w", err)
	}
	ackAge, err := meter.Int64ObservableGauge("message_worker_ack_floor_age_seconds",
		metric.WithDescription("age of the consumer's ack floor — the oldest-unacked proxy that catches a stuck retry loop"))
	if err != nil {
		return nil, fmt.Errorf("ack floor age gauge: %w", err)
	}
	degradedGauge, err := meter.Int64ObservableGauge("message_worker_history_degraded",
		metric.WithDescription("1 while the site's history-degraded marker is set, else 0"))
	if err != nil {
		return nil, fmt.Errorf("degraded gauge: %w", err)
	}

	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		// #nosec G115 -- NumPending is a queue depth, far below int64 max
		o.ObserveInt64(pending, int64(m.numPending.Load()))
		o.ObserveInt64(ackAge, m.ackFloorAgeSeconds.Load())
		o.ObserveInt64(degradedGauge, m.degraded.Load())
		return nil
	}, pending, ackAge, degradedGauge); err != nil {
		return nil, fmt.Errorf("register gauge callback: %w", err)
	}

	m.classOpts = attrOpts("class", cassutil.CQLInfra.String(), cassutil.CQLRequest.String())
	m.codeOpts = attrOpts("code", "invalid", "syntax", "other", "none")
	m.reasonOpts = attrOpts("reason", dropSuppressedRateLimited, dropSuppressedDisabled)

	return m, nil
}

// attrOpts precomputes one MeasurementOption per value of a closed label domain.
func attrOpts(key string, values ...string) map[string]metric.MeasurementOption {
	opts := make(map[string]metric.MeasurementOption, len(values))
	for _, v := range values {
		opts[v] = metric.WithAttributeSet(attribute.NewSet(attribute.String(key, v)))
	}
	return opts
}

// opt returns the precomputed option for value, falling back to an allocated one if
// a caller ever passes a value outside the declared domain. Telemetry must not be the
// thing that drops a data point, so an unknown label is recorded rather than skipped.
func (m *metrics) opt(opts map[string]metric.MeasurementOption, value string) metric.MeasurementOption {
	if o, ok := opts[value]; ok {
		return o
	}
	return metric.WithAttributes(attribute.String("value", value))
}

// onHistoryWriteFailure counts a failed Cassandra history write, labelled with the
// error class settle decided on. The label is what makes a migration-induced wave of
// request-class failures visible BEFORE the retry window elapses and messages start
// being destroyed.
func (m *metrics) onHistoryWriteFailure(class string) {
	if m == nil {
		return
	}
	m.historyWriteFailures.Add(context.Background(), 1, m.opt(m.classOpts, class))
}

// onDropped counts a message destroyed by the give-up path, labelled with the CQL
// code (never the error text — an unbounded label is a cardinality bomb).
// Call it only AFTER a successful Ack: a failed Ack leaves the message alive, and
// counting it here would inflate a metric whose whole purpose is measuring destruction.
func (m *metrics) onDropped(code string) {
	if m == nil {
		return
	}
	m.dropped.Add(context.Background(), 1, m.opt(m.codeOpts, code))
}

// The only two reasons a drop is withheld. Bounded by construction: an operator
// releasing the kill switch needs to distinguish them, and nothing else may become a
// label value.
const (
	dropSuppressedRateLimited = "rate_limited"
	dropSuppressedDisabled    = "disabled"
)

// onDropSuppressed counts a drop the guards withheld. Without it the brake is
// invisible — an operator cannot tell whether releasing the kill switch is safe, or
// whether the rate cap is the only thing standing between a wave and the feed.
func (m *metrics) onDropSuppressed(reason string) {
	if m == nil {
		return
	}
	m.dropSuppressed.Add(context.Background(), 1, m.opt(m.reasonOpts, reason))
}

func (m *metrics) setDegraded(degraded bool) {
	if m == nil {
		return
	}
	var v int64
	if degraded {
		v = 1
	}
	m.degraded.Store(v)
}

func (m *metrics) setLag(numPending uint64, ackFloorAgeSeconds float64) {
	if m == nil {
		return
	}
	m.numPending.Store(numPending)
	m.ackFloorAgeSeconds.Store(int64(ackFloorAgeSeconds))
}

// startLagPoller samples consumer state on a ticker until the returned stop is
// called. Returns stop rather than leaking the goroutine's lifetime to the caller.
// The first sample is taken at startup rather than one interval in, so the gauges
// are populated before the first scrape.
func startLagPoller(ctx context.Context, m *metrics, info consumerInfoFunc, every time.Duration) func() {
	return startTicker(ctx, every, tickOnStart, func(ctx context.Context) {
		ci, err := info(ctx)
		if err != nil {
			slog.WarnContext(ctx, "consumer info poll failed", "error", err)
			return
		}
		if ci == nil {
			return
		}
		var age float64
		if ci.AckFloor.Last != nil {
			age = time.Since(*ci.AckFloor.Last).Seconds()
		}
		m.setLag(ci.NumPending, age)
	})
}
