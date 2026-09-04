package searchengine

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability supplies the OpenTelemetry providers searchengine needs to build
// an instrumented Elasticsearch client: spans from the first-party
// instrumentation plus the SDK-owned operation-duration histogram (o11y ADR
// 0027). The facade never falls back to the global MeterProvider, so both must
// be supplied. It is the minimal interface the helper depends on — *o11y.SDK
// satisfies it directly via TracerProvider() and MeterProvider() (accept
// interfaces, CLAUDE.md §3).
type Observability interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

type connectConfig struct {
	obs Observability
}

// Option configures New. The zero config builds a plain client so existing call
// sites keep working unchanged and migrate incrementally.
type Option func(*connectConfig)

// WithObservability builds the Elasticsearch client via o11y/elasticsearch using
// the supplied tracer provider, so the adapter's operations emit ES-semantic
// spans. Ignored for non-Elasticsearch backends. When omitted, New builds a
// plain client.
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
