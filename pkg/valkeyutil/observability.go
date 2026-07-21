package valkeyutil

import (
	o11yredis "github.com/flywindy/o11y/redis"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability supplies the OpenTelemetry providers valkeyutil needs to attach
// o11y/redis command spans and operation/pool metrics to the cluster client. It
// is the minimal interface the helper depends on — *o11y.SDK satisfies it
// directly via TracerProvider() and MeterProvider(), so call sites pass the SDK
// without valkeyutil importing it (accept interfaces, CLAUDE.md §3).
type Observability interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

type connectConfig struct {
	obs       Observability
	redisOpts []o11yredis.Option
}

// Option configures ConnectCluster. The zero config attaches no instrumentation
// so existing call sites keep working unchanged and migrate incrementally.
type Option func(*connectConfig)

// WithObservability instruments the client via o11y/redis using the supplied
// providers. When omitted, ConnectCluster attaches no instrumentation.
func WithObservability(o Observability) Option {
	return func(c *connectConfig) { c.obs = o }
}

// WithRedisOptions passes low-level o11y/redis options through to the wrapped
// client. It is intended for instrumentation behavior only, such as suppressing
// startup/background noise; callers should keep command text disabled unless a
// debugging session explicitly needs it.
func WithRedisOptions(opts ...o11yredis.Option) Option {
	return func(c *connectConfig) {
		c.redisOpts = append(c.redisOpts, opts...)
	}
}

// WithRequireParentSpan keeps Redis command spans only when the command context
// is already part of a traced request/consumer flow. This drops startup probes
// and background client noise while preserving in-request cache spans.
func WithRequireParentSpan(enabled bool) Option {
	return WithRedisOptions(o11yredis.WithRequireParentSpan(enabled))
}

// WithIgnoredCommands suppresses spans and operation-duration samples for the
// named Redis commands. Prefer WithRequireParentSpan when the noisy commands are
// only background probes, because command-name filtering affects all callers.
func WithIgnoredCommands(names ...string) Option {
	return WithRedisOptions(o11yredis.WithIgnoredCommands(names...))
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
