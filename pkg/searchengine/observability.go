package searchengine

import (
	"go.opentelemetry.io/otel/trace"
)

// Observability supplies the OpenTelemetry tracer provider searchengine needs to
// build an instrumented Elasticsearch client. The o11y/elasticsearch integration
// is trace-only (it accepts only a TracerProvider — see o11y ADR 0020 §6), so
// this is the minimal interface the helper depends on; *o11y.SDK satisfies it
// directly via TracerProvider() (accept interfaces, CLAUDE.md §3).
type Observability interface {
	TracerProvider() trace.TracerProvider
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
