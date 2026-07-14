package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metrics holds the transformer's instruments: processed throughput plus the nak/term
// dispositions that flag a stuck or poison-heavy stream. Nil-safe so unit tests run without a meter.
type metrics struct {
	processed         metric.Int64Counter
	naks              metric.Int64Counter
	terms             metric.Int64Counter
	skipped           metric.Int64Counter
	recovered         metric.Int64Counter
	historyRejected   metric.Int64Counter
	exhausted         metric.Int64Counter
	threadLinkDropped metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	m := otel.Meter("oplog-transformer")
	processed, err := m.Int64Counter("oplog_transformer_events_processed_total",
		metric.WithDescription("oplog events handled and acked, by op"))
	if err != nil {
		return nil, fmt.Errorf("processed counter: %w", err)
	}
	naks, err := m.Int64Counter("oplog_transformer_naks_total",
		metric.WithDescription("transient failures naked for redelivery"))
	if err != nil {
		return nil, fmt.Errorf("naks counter: %w", err)
	}
	terms, err := m.Int64Counter("oplog_transformer_terms_total",
		metric.WithDescription("poison/undecodable events termed (never redelivered)"))
	if err != nil {
		return nil, fmt.Errorf("terms counter: %w", err)
	}
	skipped, err := m.Int64Counter("oplog_transformer_events_skipped_total",
		metric.WithDescription("events deliberately skipped (system messages etc.), by reason"))
	if err != nil {
		return nil, fmt.Errorf("skipped counter: %w", err)
	}
	recovered, err := m.Int64Counter("oplog_transformer_degraded_recovered_total",
		metric.WithDescription("degraded events recovered via a source lookup, by collection"))
	if err != nil {
		return nil, fmt.Errorf("recovered counter: %w", err)
	}
	historyRejected, err := m.Int64Counter("oplog_transformer_history_rejected_total",
		metric.WithDescription("history replies classified as permanent rejections (termed), by code"))
	if err != nil {
		return nil, fmt.Errorf("history rejected counter: %w", err)
	}
	exhausted, err := m.Int64Counter("oplog_transformer_exhausted_total",
		metric.WithDescription("events termed after reaching MaxDeliver (would-be silent JetStream drops), by op"))
	if err != nil {
		return nil, fmt.Errorf("exhausted counter: %w", err)
	}
	threadLinkDropped, err := m.Int64Counter("oplog_transformer_thread_link_dropped_total",
		metric.WithDescription("thread replies published without their parent link (parent missing/corrupt), by reason"))
	if err != nil {
		return nil, fmt.Errorf("thread link dropped counter: %w", err)
	}
	return &metrics{
		processed: processed, naks: naks, terms: terms, skipped: skipped,
		recovered: recovered, historyRejected: historyRejected, exhausted: exhausted,
		threadLinkDropped: threadLinkDropped,
	}, nil
}

func opAttr(op string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("op", op))
}

func (m *metrics) onProcessed(ctx context.Context, op string) {
	if m == nil {
		return
	}
	m.processed.Add(ctx, 1, opAttr(op))
}

func (m *metrics) onNak(ctx context.Context, op string) {
	if m == nil {
		return
	}
	m.naks.Add(ctx, 1, opAttr(op))
}

func (m *metrics) onTerm(ctx context.Context, op string) {
	if m == nil {
		return
	}
	m.terms.Add(ctx, 1, opAttr(op))
}

func (m *metrics) onSkipped(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.skipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// onThreadLinkDropped records a reply published without its parent link because the parent was
// missing or corrupt in the source. The reply is preserved; only the thread linkage is lost.
func (m *metrics) onThreadLinkDropped(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.threadLinkDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// onRecovered records a degraded event whose missing field was recovered via a source lookup.
func (m *metrics) onRecovered(ctx context.Context, collection string) {
	if m == nil {
		return
	}
	m.recovered.Add(ctx, 1, metric.WithAttributes(attribute.String("collection", collection)))
}

// onHistoryRejected records a history reply classified as a permanent rejection (the event is
// termed), labelled by the rejecting errcode so a genuine permanent failure is alertable.
func (m *metrics) onHistoryRejected(ctx context.Context, code string) {
	if m == nil {
		return
	}
	m.historyRejected.Add(ctx, 1, metric.WithAttributes(attribute.String("code", code)))
}

// onExhausted records an event termed because it reached MaxDeliver — what would otherwise be a
// silent JetStream-side drop after the redelivery cap. Distinct from poison terms; alert on it.
func (m *metrics) onExhausted(ctx context.Context, op string) {
	if m == nil {
		return
	}
	m.exhausted.Add(ctx, 1, opAttr(op))
}
