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

	"github.com/caarlos0/env/v11"
	"github.com/flywindy/o11y"
	"go.opentelemetry.io/otel"
)

// Config is parsed from the environment. Variable names follow OpenTelemetry
// standard conventions where one exists so operators and tooling recognize
// them; pillar toggles (O11Y_*_ENABLED) and the trace sampler
// (OTEL_TRACES_SAMPLER[_ARG]) are read by the SDK directly and are not
// duplicated here.
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
	return opts
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
