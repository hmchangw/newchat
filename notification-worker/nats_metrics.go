package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type notificationMetrics struct{ outcomes metric.Int64Counter }

func newNotificationMetrics(meter metric.Meter) *notificationMetrics {
	counter, _ := meter.Int64Counter("notification_worker_outcomes_total",
		metric.WithDescription("Notification processing outcomes by bounded kind and result."))
	return &notificationMetrics{outcomes: counter}
}

func (m *notificationMetrics) Record(ctx context.Context, kind, result string) {
	if m == nil || m.outcomes == nil {
		return
	}
	if kind != "push" && kind != "notification" {
		kind = "unknown"
	}
	switch result {
	case "sent", "suppressed", "publish_failed", "failed":
	default:
		result = "failed"
	}
	m.outcomes.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", kind), attribute.String("result", result)))
}
