package cassutil

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability supplies the OpenTelemetry providers cassutil needs to attach
// o11y query/connect observers (spans + operation and connection metrics) to
// the session. It is the minimal interface the helper depends on — *o11y.SDK
// satisfies it directly via TracerProvider() and MeterProvider(), so call sites
// pass the SDK without cassutil importing it (accept interfaces, CLAUDE.md §3).
type Observability interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

type connectConfig struct {
	obs Observability
}

// Option configures Connect. The zero config attaches no instrumentation so
// existing call sites keep working unchanged and migrate incrementally.
type Option func(*connectConfig)

// WithObservability instruments the session via o11y/cassandra using the
// supplied providers. When omitted, Connect creates a plain session.
func WithObservability(o Observability) Option {
	return func(c *connectConfig) { c.obs = o }
}

func newConnectConfig(opts ...Option) connectConfig {
	var cfg connectConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}
