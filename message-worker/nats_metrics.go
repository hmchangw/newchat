package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type persistenceMetrics struct{ outcomes metric.Int64Counter }

func newPersistenceMetrics(meter metric.Meter) *persistenceMetrics {
	counter, _ := meter.Int64Counter("message_worker_persistence_total",
		metric.WithDescription("Cassandra message persistence attempts by bounded message kind and result."))
	return &persistenceMetrics{outcomes: counter}
}

func (m *persistenceMetrics) Record(ctx context.Context, kind, result string) {
	if m == nil || m.outcomes == nil {
		return
	}
	switch kind {
	case "user", "system", "thread_reply", "teams_migration", "unknown":
	default:
		kind = "unknown"
	}
	if result != "success" && result != "error" {
		result = "error"
	}
	m.outcomes.Add(ctx, 1, metric.WithAttributes(
		attribute.String("message_kind", kind),
		attribute.String("result", result),
	))
}
