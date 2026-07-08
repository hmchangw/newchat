// Package obs wires the platform's observability stack on top of flywindy/o11y.
// It owns the boilerplate every service would otherwise duplicate: parsing the
// observability environment, supplying the four required service-identity
// options, defaulting endpoints, installing the SDK's providers as the
// OpenTelemetry globals, and setting the SDK logger as the slog default.
//
// Callers receive the real *o11y.SDK (the full SDK API stays available) plus a
// shutdown func to defer. Shared pkg/* connect helpers should accept the
// minimal provider interface they need (TracerProvider/MeterProvider), not the
// concrete SDK, per CLAUDE.md "accept interfaces, return structs".
package obs

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/flywindy/o11y"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Config is parsed from the environment. Variable names follow OpenTelemetry
// standard conventions where one exists so operators and tooling recognize
// them; pillar toggles (O11Y_*_ENABLED) are read by the SDK directly. The trace
// sampler IS read here (OTEL_TRACES_SAMPLER[_ARG]) and mapped to an o11y option,
// because the SDK does not read those env vars itself — see samplerOptions.
type Config struct {
	// ServiceName drives service.name on every span/metric/log. It defaults to a
	// visible placeholder rather than being required so a missing env degrades to
	// mislabeled telemetry, not a startup crash-loop; production deploys MUST set
	// OTEL_SERVICE_NAME (a "unknown-service" service map entry is the signal it wasn't).
	ServiceName    string            `env:"OTEL_SERVICE_NAME" envDefault:"unknown-service"`
	ServiceVersion string            `env:"SERVICE_VERSION" envDefault:"dev"`
	Environment    string            `env:"DEPLOY_ENV" envDefault:"development"`
	Namespace      string            `env:"SERVICE_NAMESPACE" envDefault:"chat"`
	OTLPEndpoint   string            `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://localhost:4318"`
	OTLPHeaders    map[string]string `env:"OTEL_EXPORTER_OTLP_HEADERS" envKeyValSeparator:"="`
	PrometheusHost string            `env:"OTEL_EXPORTER_PROMETHEUS_HOST" envDefault:""`
	PrometheusPort string            `env:"OTEL_EXPORTER_PROMETHEUS_PORT" envDefault:"2112"`

	// Head sampling. Standard OTel env vars, but the o11y SDK does NOT read them
	// itself (it only honors WithSamplingRatio/WithTraceSampler options), so
	// samplerOptions maps them to the right option. Empty/always_on = 100%.
	// NOTE: each NATS hop is a detached root, so a ratio samples hops
	// independently — see docs/specs/o11y-performance-and-sampling.md.
	TracesSampler    string  `env:"OTEL_TRACES_SAMPLER" envDefault:""`
	TracesSamplerArg float64 `env:"OTEL_TRACES_SAMPLER_ARG" envDefault:"1"`
}

func parseConfig() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse obs config: %w", err)
	}
	return cfg, nil
}

// metricsAddr combines the Prometheus host and port into the host:port form the
// SDK's WithMetricsAddr expects. An empty host yields ":<port>" (all interfaces).
func (c *Config) metricsAddr() string {
	return net.JoinHostPort(c.PrometheusHost, c.PrometheusPort)
}

// options builds the o11y options for this config. WithOTLPHeaders is appended
// only when headers are set so an empty map does not override SDK behaviour.
func (c *Config) options() []o11y.Option {
	opts := []o11y.Option{
		o11y.WithServiceName(c.ServiceName),
		o11y.WithServiceVersion(c.ServiceVersion),
		o11y.WithEnvironment(c.Environment),
		o11y.WithServiceNamespace(c.Namespace),
		o11y.WithOTLPEndpoint(c.OTLPEndpoint),
		o11y.WithMetricsAddr(c.metricsAddr()),
	}
	if len(c.OTLPHeaders) > 0 {
		opts = append(opts, o11y.WithOTLPHeaders(c.OTLPHeaders))
	}
	opts = append(opts, c.samplerOptions()...)
	return opts
}

// samplerOptions maps the standard OTEL_TRACES_SAMPLER[_ARG] env vars onto o11y
// sampler options. The SDK does not read these env vars, so without this an
// operator setting OTEL_TRACES_SAMPLER would silently get the SDK default (100%).
// Recognized values follow the OTel spec; an unknown value logs a warning and
// falls back to the 100% default rather than failing startup.
func (c *Config) samplerOptions() []o11y.Option {
	switch strings.ToLower(strings.TrimSpace(c.TracesSampler)) {
	case "", "always_on", "parentbased_always_on":
		return nil // SDK default is ParentBased(AlwaysSample) = 100%
	case "always_off", "parentbased_always_off":
		return []o11y.Option{o11y.WithTraceSampler(sdktrace.NeverSample())}
	case "traceidratio":
		return []o11y.Option{o11y.WithTraceSampler(sdktrace.TraceIDRatioBased(c.TracesSamplerArg))}
	case "parentbased_traceidratio":
		return []o11y.Option{o11y.WithSamplingRatio(c.TracesSamplerArg)}
	default:
		slog.Warn("unknown OTEL_TRACES_SAMPLER; falling back to default (100%)",
			"value", c.TracesSampler)
		return nil
	}
}

// Init parses Config from the environment, starts the o11y SDK, installs the
// SDK's providers and propagator as the OpenTelemetry globals (so library
// instrumentation that reads the global provider — e.g. otelhttp / o11y/gin —
// stays correlated), sets the SDK logger as the slog default (so existing
// slog.Info calls gain trace correlation for free), and returns the SDK plus a
// shutdown func to defer.
func Init(ctx context.Context) (*o11y.SDK, func(context.Context) error, error) {
	cfg, err := parseConfig()
	if err != nil {
		return nil, nil, err
	}

	sdk, err := o11y.Init(ctx, cfg.options()...)
	if err != nil {
		return nil, nil, fmt.Errorf("init o11y sdk: %w", err)
	}

	otel.SetTracerProvider(sdk.TracerProvider())
	otel.SetMeterProvider(sdk.MeterProvider())
	otel.SetTextMapPropagator(sdk.Propagator)
	slog.SetDefault(sdk.Logger)

	return sdk, sdk.Shutdown, nil
}
