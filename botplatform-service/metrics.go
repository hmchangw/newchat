package main

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// controlBypassed counts bot requests admitted without a Valkey-backed control
// because Valkey was unavailable. A non-zero rate means rate limiting or
// duplicate suppression is currently off — alert on it. The bots keep serving,
// so nothing else surfaces the gap.
var controlBypassed metric.Int64Counter

func init() {
	m := otel.Meter("botplatform")

	var err error
	controlBypassed, err = m.Int64Counter(
		"bot_control_bypassed_total",
		metric.WithDescription("Bot requests that bypassed a Valkey-backed control because Valkey was unavailable"),
	)
	if err != nil {
		// Fall back to a no-op so the hot path stays safe if the global meter
		// provider is not yet installed at package init time.
		controlBypassed, _ = noop.NewMeterProvider().Meter("botplatform").
			Int64Counter("bot_control_bypassed_total")
	}
}

// There are only three controls, so their attribute sets are built once rather
// than allocated and sorted on every bypassed request.
var (
	controlRateLimitCaller = metric.WithAttributes(attribute.String("control", "rate_limit_caller"))
	controlRateLimitGlobal = metric.WithAttributes(attribute.String("control", "rate_limit_global"))
	controlIdempotency     = metric.WithAttributes(attribute.String("control", "idempotency"))
)

// bypassControl records one fail-open admission. The log carries the cause for
// diagnosis; the counter is what to alert on, since it survives log sampling
// and does not depend on log level. Shared by every control so their bypass
// telemetry cannot drift apart.
func bypassControl(ctx context.Context, control string, attrs metric.MeasurementOption, err error, logArgs ...any) {
	slog.WarnContext(ctx, "bot "+control+" unavailable; admitting request",
		append(logArgs, "error", err)...)
	controlBypassed.Add(ctx, 1, attrs)
}
