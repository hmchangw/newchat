package main

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// controlBypassed counts requests admitted without a Valkey-backed control because Valkey
// was unavailable. A non-zero rate means rate limiting or duplicate suppression is off for
// that window — alert on it, since the bots keep serving and nothing else surfaces the gap.
var controlBypassed metric.Int64Counter

func init() {
	m := otel.Meter("botplatform")

	var err error
	controlBypassed, err = m.Int64Counter(
		"bot_control_bypassed_total",
		metric.WithDescription("Bot requests that bypassed a Valkey-backed control due to Valkey unavailability"),
	)
	if err != nil {
		// Fall back to a no-op counter so the service still runs if the global meter
		// provider is not yet initialised at package init time.
		controlBypassed, _ = noop.NewMeterProvider().Meter("botplatform").
			Int64Counter("bot_control_bypassed_total")
	}
}

// There are only three controls, so their attribute sets are built once at init
// rather than allocated and sorted on every bypassed request.
var (
	controlRateLimitCaller = metric.WithAttributes(attribute.String("control", "rate_limit_caller"))
	controlRateLimitGlobal = metric.WithAttributes(attribute.String("control", "rate_limit_global"))
	controlIdempotency     = metric.WithAttributes(attribute.String("control", "idempotency"))
)

// bypassControl records one fail-open admission: the log carries the cause for
// diagnosis, the counter is what to alert on since it survives log sampling.
// Shared by every control so their bypass telemetry cannot drift apart.
//
// The log goes through valkeyutil.LogDegraded, which drops to Debug once the
// circuit is open — otherwise a Valkey outage emits one Warn per bot request,
// unthrottled, for its whole duration. The counter is unconditional, so the
// signal you alert on never depends on log level.
func bypassControl(ctx context.Context, control string, attrs metric.MeasurementOption, err error, logArgs ...any) {
	valkeyutil.LogDegraded(ctx, "bot "+control+" unavailable; admitting request", err, logArgs...)
	controlBypassed.Add(ctx, 1, attrs)
}
