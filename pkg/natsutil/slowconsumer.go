package natsutil

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// slowConsumerDropped counts messages dropped by a subscription that could not
// keep up, tagged by subject and queue group.
var slowConsumerDropped metric.Int64Counter

func init() {
	var err error
	slowConsumerDropped, err = otel.Meter("nats").Int64Counter(
		"nats_slow_consumer_dropped_total",
		metric.WithDescription("Messages dropped by a NATS subscription that could not keep up, by subject and queue group"),
	)
	if err != nil {
		// Fall back to a no-op counter so recording stays safe even if the
		// global meter provider is not initialised at package init time.
		slowConsumerDropped, _ = noop.NewMeterProvider().Meter("nats").
			Int64Counter("nats_slow_consumer_dropped_total")
	}
}

// slowConsumerFields builds the slog fields describing a slow consumer.
// Every accessor is best-effort: sub is nil for connection-level async errors,
// and Dropped/Pending/PendingLimits all error on an already-closed
// subscription, which happens routinely during shutdown. A diagnostic path
// must never panic or mask the event it is reporting.
func slowConsumerFields(sub *nats.Subscription) []any {
	if sub == nil {
		return []any{"subject", "unknown"}
	}
	fields := []any{"subject", sub.Subject, "queue", sub.Queue}
	if dropped, err := sub.Dropped(); err == nil {
		fields = append(fields, "dropped", dropped)
	}
	// nbytes, not bytes — a local named bytes shadows the stdlib package and
	// trips the linter's shadow check.
	if msgs, nbytes, err := sub.Pending(); err == nil {
		fields = append(fields, "pending_msgs", msgs, "pending_bytes", nbytes)
	}
	if msgs, nbytes, err := sub.PendingLimits(); err == nil {
		fields = append(fields, "limit_msgs", msgs, "limit_bytes", nbytes)
	}
	return fields
}

// subDropped returns the subscription's drop count, or 0 when it cannot be read.
func subDropped(sub *nats.Subscription) int {
	if sub == nil {
		return 0
	}
	dropped, err := sub.Dropped()
	if err != nil {
		return 0
	}
	return dropped
}

// logSlowConsumer reports a slow consumer and records the drop count.
func logSlowConsumer(log *slog.Logger, sub *nats.Subscription) {
	dropped := subDropped(sub)
	logSlowConsumerAt(log, dropped, slowConsumerFields(sub))
	if dropped > 0 && sub != nil {
		slowConsumerDropped.Add(context.Background(), int64(dropped),
			metric.WithAttributes(
				attribute.String("subject", sub.Subject),
				attribute.String("queue", sub.Queue),
			))
	}
}

// logSlowConsumerAt picks the level: dropped messages on a core subscription are
// unrecoverable loss and should page, so they log at ERROR. A slow consumer that
// has not dropped anything yet is a warning.
func logSlowConsumerAt(log *slog.Logger, dropped int, fields []any) {
	if dropped > 0 {
		log.Error("nats slow consumer, messages dropped", fields...)
		return
	}
	log.Warn("nats slow consumer", fields...)
}
