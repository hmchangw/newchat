package mongoutil

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability supplies the OpenTelemetry providers mongoutil needs to attach
// o11y MongoDB command spans plus operation-duration and connection-pool
// metrics. It is the minimal interface the helper depends on — *o11y.SDK
// satisfies it directly via its TracerProvider() and MeterProvider() methods,
// so call sites pass the SDK without mongoutil importing it (accept interfaces,
// per CLAUDE.md §3).
type Observability interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

type connectConfig struct {
	obs Observability
}

// Option configures Connect. Options are additive; the zero config attaches no
// instrumentation so existing call sites keep working unchanged and migrate
// incrementally.
type Option func(*connectConfig)

// WithObservability instruments the Mongo client via o11y/mongo using the
// supplied providers. When omitted, Connect attaches no instrumentation.
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
