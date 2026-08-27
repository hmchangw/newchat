package valkeyutil

import (
	"context"

	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	// breakerTransitions counts circuit breaker state changes, tagged by
	// breaker name and the from/to states.
	breakerTransitions metric.Int64Counter
	// breakerState reports the current state: 0 closed, 1 half-open, 2 open.
	breakerState metric.Int64Gauge
)

func init() {
	m := otel.Meter("valkey")

	var err error
	breakerTransitions, err = m.Int64Counter(
		"valkey_breaker_transitions_total",
		metric.WithDescription("Valkey circuit breaker state transitions, by breaker and from/to state"),
	)
	if err != nil {
		// Fall back to a no-op counter so the program continues to run even if
		// the global meter provider is not yet initialised at package init time.
		breakerTransitions, _ = noop.NewMeterProvider().Meter("valkey").
			Int64Counter("valkey_breaker_transitions_total")
	}

	breakerState, err = m.Int64Gauge(
		"valkey_breaker_state",
		metric.WithDescription("Current Valkey circuit breaker state: 0 closed, 1 half-open, 2 open"),
	)
	if err != nil {
		breakerState, _ = noop.NewMeterProvider().Meter("valkey").
			Int64Gauge("valkey_breaker_state")
	}
}

// stateValue encodes a breaker state for the gauge.
func stateValue(s gobreaker.State) int64 {
	switch s {
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	default:
		return 0
	}
}

// recordBreakerState emits the current-state gauge. Called at construction as
// well as on transition, so a process whose breaker never trips still exports a
// steady 0 rather than no series at all.
func recordBreakerState(name string, state gobreaker.State) {
	breakerState.Record(context.Background(), stateValue(state), metric.WithAttributes(
		attribute.String("breaker", name),
	))
}

// recordBreakerTransition emits the transition counter and current-state gauge.
// Takes states rather than their strings: passing both invited a caller to hand
// over a label and a state that disagree, which no compiler would catch.
func recordBreakerTransition(name string, from, to gobreaker.State) {
	breakerTransitions.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("breaker", name),
		attribute.String("from", from.String()),
		attribute.String("to", to.String()),
	))
	recordBreakerState(name, to)
}
