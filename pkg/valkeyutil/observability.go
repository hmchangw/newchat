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
	// requireReachable makes the startup probe fatal. Off by default: see
	// WithRequireReachable.
	requireReachable bool
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

// WithRequireReachable makes the startup PING fatal, so an unreachable cluster
// fails the dial instead of returning a usable client.
//
// It is off by default, and deliberately so. A shared datastore is the same for
// every replica, so gating startup on its reachability means a Valkey outage
// overlapping a rollout, autoscale or node drain crashloops every pod at once —
// including the message path — and the crashloop outlives the outage. go-redis
// dials lazily and self-heals per call, so a pod that starts during an outage
// recovers on its own once Valkey returns.
//
// The caller that wants this is the one-shot CLI: tools/seed-sample-data has no
// fallback and no next call to self-heal into, so aborting the run is right.
func WithRequireReachable() Option {
	return func(c *connectConfig) { c.requireReachable = true }
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
