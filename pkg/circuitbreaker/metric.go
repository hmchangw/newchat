package circuitbreaker

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// StateMetricName is the gauge every breaker publishes its state on, so one
// dashboard query covers all of them:
//
//	max by (breaker) (circuit_breaker_state)
//
// The downstream is a label, not part of the name — this package guards
// whatever a service puts behind it, not MongoDB specifically.
const StateMetricName = "circuit_breaker_state"

// StateGauge publishes breaker state transitions on a single shared instrument,
// tagged with the breaker's name. The name attribute is what makes a service
// with several breakers legible: they all record to one instrument, so without
// it their datapoints overwrite each other and the metric ends up describing
// whichever breaker moved last rather than any of them.
type StateGauge struct {
	gauge metric.Int64Gauge
}

// NewStateGauge creates the shared breaker-state gauge on meter. Most callers
// want the package-level Tracked instead, which uses the global meter.
func NewStateGauge(meter metric.Meter) (StateGauge, error) {
	// The name is written as a literal rather than StateMetricName because
	// pkg/obs's instrument registry guard reads source: a constant here is
	// invisible to it, so the instrument would ship undocumented with the guard
	// still green (pkg/obs.TestInstrumentNamesAreLiterals). The two cannot drift
	// -- the gauge tests read datapoints through gaugeValue, which matches on
	// StateMetricName, so a literal that drifted from it would find nothing.
	g, err := meter.Int64Gauge("circuit_breaker_state",
		metric.WithDescription("Circuit breaker state (0=closed, 1=open, 2=half-open)"))
	if err != nil {
		return StateGauge{}, fmt.Errorf("create %s gauge: %w", StateMetricName, err)
	}
	return StateGauge{gauge: g}, nil
}

// Track returns an Option that logs every transition of the named breaker and
// records its new state on the gauge. name identifies which breaker the series
// describes (e.g. "subscription", "roommeta", "atrestdek") and must be distinct
// within a service.
func (s StateGauge) Track(ctx context.Context, name string) Option {
	// Built once at wiring time rather than per transition: Track runs once per
	// breaker during startup, and the closure below reuses this set for every
	// transition that breaker ever makes.
	attrs := metric.WithAttributes(attribute.String("breaker", name))
	return WithOnTransition(func(from, to State) {
		slog.WarnContext(ctx, "circuit breaker transition",
			"breaker", name, "from", from.String(), "to", to.String())
		// attrs is the set built above, once per breaker at wiring time, not per
		// recording call. metrics-no-per-call-attribute-set is intraprocedural
		// taint and cannot tell a closure capture from a local rebuilt on every
		// call: both put the source and the sink inside one function. The rule's
		// own note calls "building it in a constructor and looking it up here"
		// the shape it wants, and this is that -- Track is the constructor, the
		// captured variable is the lookup.
		// nosemgrep: metrics-no-per-call-attribute-set
		s.gauge.Record(ctx, int64(to), attrs)
	})
}

// def is the package-default gauge backed by the global meter, so services do
// not each have to construct one and handle a metric-registration error.
var def StateGauge

func init() {
	g, err := NewStateGauge(otel.Meter("circuitbreaker"))
	if err != nil {
		// Fall back to a no-op instrument so recording is always safe even if
		// the global meter provider rejects instrument creation at init time.
		g, _ = NewStateGauge(noop.NewMeterProvider().Meter("circuitbreaker"))
	}
	def = g
}

// Tracked returns an Option wiring the named breaker to the package-default
// gauge. This is the one-liner services want:
//
//	circuitbreaker.New(fails, cooldown, circuitbreaker.Tracked(ctx, "subscription"))
func Tracked(ctx context.Context, name string) Option { return def.Track(ctx, name) }
