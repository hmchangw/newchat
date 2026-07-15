package minioutil

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability supplies the OpenTelemetry providers minioutil needs to build
// an instrumented MinIO client (o11y/minio logical-operation spans + duration
// metrics). It is the minimal interface the helper depends on — *o11y.SDK
// satisfies it directly via TracerProvider() and MeterProvider(), so call sites
// pass the SDK without minioutil importing it (accept interfaces, CLAUDE.md §3).
type Observability interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

type connectConfig struct {
	obs Observability
}

// Option configures Connect. The zero config builds a plain client so existing
// call sites keep working unchanged and migrate incrementally.
type Option func(*connectConfig)

// WithObservability builds the client via o11y/minio using the supplied
// providers. When omitted, Connect builds a plain minio-go client.
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
