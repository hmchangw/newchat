package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type gatekeeperMetrics struct{ messages metric.Int64Counter }

func newGatekeeperMetrics(meter metric.Meter) *gatekeeperMetrics {
	counter, _ := meter.Int64Counter("message_gatekeeper_messages_total",
		metric.WithDescription("Gatekeeper business outcomes by bounded result and reason."))
	return &gatekeeperMetrics{messages: counter}
}

func (m *gatekeeperMetrics) Record(ctx context.Context, result, reason string) {
	if m == nil || m.messages == nil {
		return
	}
	switch result {
	case "accepted", "rejected", "retry", "failed":
	default:
		result = "failed"
	}
	switch reason {
	case "none", "invalid_subject", "invalid_payload", "not_subscribed", "room_restricted", "canonical_publish", "dependency", "unknown":
	default:
		reason = "unknown"
	}
	m.messages.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
		attribute.String("reason", reason),
	))
}
