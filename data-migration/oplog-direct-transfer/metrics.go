package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metrics holds the service's instruments. Nil-safe (tests run without a meter).
type metrics struct {
	processed metric.Int64Counter
	naks      metric.Int64Counter
	terms     metric.Int64Counter
	skipped   metric.Int64Counter
	exhausted metric.Int64Counter
	writes    metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	m := otel.Meter("oplog-direct-transfer")
	processed, err := m.Int64Counter("oplog_direct_transfer_events_processed_total",
		metric.WithDescription("oplog events handled and acked, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("processed counter: %w", err)
	}
	naks, err := m.Int64Counter("oplog_direct_transfer_naks_total",
		metric.WithDescription("transient failures naked for redelivery, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("naks counter: %w", err)
	}
	terms, err := m.Int64Counter("oplog_direct_transfer_terms_total",
		metric.WithDescription("poison/undecodable events termed, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("terms counter: %w", err)
	}
	skipped, err := m.Int64Counter("oplog_direct_transfer_events_skipped_total",
		metric.WithDescription("events deliberately skipped, by reason"))
	if err != nil {
		return nil, fmt.Errorf("skipped counter: %w", err)
	}
	exhausted, err := m.Int64Counter("oplog_direct_transfer_exhausted_total",
		metric.WithDescription("events termed after reaching MaxDeliver, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("exhausted counter: %w", err)
	}
	writes, err := m.Int64Counter("oplog_direct_transfer_writes_total",
		metric.WithDescription("target writes, by collection+action (upsert/delete)"))
	if err != nil {
		return nil, fmt.Errorf("writes counter: %w", err)
	}
	return &metrics{processed: processed, naks: naks, terms: terms, skipped: skipped, exhausted: exhausted, writes: writes}, nil
}

func opCollAttr(op, collection string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("op", op), attribute.String("collection", collection))
}

func (m *metrics) onProcessed(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.processed.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onNak(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.naks.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onTerm(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.terms.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onSkipped(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.skipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *metrics) onExhausted(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.exhausted.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onWrite(ctx context.Context, collection, action string) {
	if m == nil {
		return
	}
	m.writes.Add(ctx, 1, metric.WithAttributes(attribute.String("collection", collection), attribute.String("action", action)))
}
