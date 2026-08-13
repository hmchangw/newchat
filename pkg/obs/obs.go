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
	"slices"
	"strings"
	"sync/atomic"

	"github.com/caarlos0/env/v11"
	"github.com/flywindy/o11y"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	// RoomIDKey and SiteIDKey are application-defined OTel attribute keys. OTel
	// does not define generic chat-room or chat-site semantic conventions, so
	// the chat.* namespace prevents collisions with standard keys.
	RoomIDKey = "chat.room.id"
	SiteIDKey = "chat.site.id"
)

// managedBaggageKeys is the single source of truth for the keys this package
// materializes onto spans and logs. options() registers them, and public
// ingress clears exactly this set — a key registered in one place but missing
// from the other would be forgeable by a caller.
var managedBaggageKeys = []string{o11y.UserNameKey, RoomIDKey, SiteIDKey}

// ManagedBaggageKeys returns a copy of the baggage keys this package treats as
// trusted identity.
func ManagedBaggageKeys() []string {
	return slices.Clone(managedBaggageKeys)
}

var (
	contextAttributesEnabled atomic.Bool
	userBaggageEnabled       atomic.Bool
)

// Config is parsed from the environment. Variable names follow OpenTelemetry
// standard conventions where one exists so operators and tooling recognize
// them; pillar toggles (O11Y_*_ENABLED) are read by the SDK directly. The trace
// sampler IS read here (OTEL_TRACES_SAMPLER[_ARG]) and mapped to an o11y option,
// because the SDK does not read those env vars itself — see samplerOptions.
type Config struct {
	// Enabled is the master switch. It defaults to FALSE so a service with no
	// observability env ships zero-impact: all pillars off, no exporters, no
	// Prometheus listener, no runtime-metrics goroutine, and (via natsutil) the
	// NATS per-message trace machinery stays gated off — the hot path runs at
	// ~native cost. stdout JSON logging is always retained. Set O11Y_ENABLED=true
	// (plus a reachable collector) to turn observability on; the individual
	// O11Y_*_ENABLED pillar toggles then apply. When Enabled is false those
	// per-pillar toggles are ignored (the master switch wins).
	Enabled bool `env:"O11Y_ENABLED" envDefault:"false"`
	// UserBaggageEnabled is a separate PII opt-in. It controls both creating
	// user.name baggage at trusted request boundaries and materializing it onto
	// spans/logs. Room/site identifiers remain enabled whenever O11Y_ENABLED is.
	UserBaggageEnabled bool `env:"O11Y_USER_BAGGAGE_ENABLED" envDefault:"false"`

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
	// independently — see docs/specs/o11y/o11y-performance-and-sampling.md.
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
	if !c.Enabled {
		// Master switch off: force every pillar off regardless of the individual
		// O11Y_*_ENABLED env defaults (which are true in the SDK). o11y.Init then
		// builds only noop providers + a stdout logger — no exporters, no
		// listener, no goroutines. See natsutil for the matching NATS-gate skip.
		return append(opts,
			o11y.WithTraceEnabled(false),
			o11y.WithMetricsEnabled(false),
			o11y.WithLogEnabled(false),
			o11y.WithProfilingEnabled(false),
		)
	}
	// Derived from managedBaggageKeys so registration and the public-ingress
	// clear list cannot drift; user.name has its own PII opt-in option below.
	opts = append(opts, o11y.WithBaggageAttributes(slices.DeleteFunc(
		ManagedBaggageKeys(), func(k string) bool { return k == o11y.UserNameKey })...))
	if c.UserBaggageEnabled {
		opts = append(opts, o11y.WithUserBaggage())
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
	return initSDK(ctx, nil, nil)
}

// InitWithLoggerHandler is Init with an optional wrapper around the SDK slog
// handler. Services that support per-request verbose logging use this to keep
// their admission handler while still exporting every admitted record through
// the SDK's OTLP logger. minLevel configures the SDK's inner handlers; the
// wrapper remains responsible for per-record admission above that floor.
func InitWithLoggerHandler(ctx context.Context, minLevel slog.Level, wrap func(slog.Handler) slog.Handler) (*o11y.SDK, func(context.Context) error, error) {
	return initSDK(ctx, &minLevel, wrap)
}

// PublicIngressPropagator accepts distributed trace context at an Internet-
// facing HTTP boundary without accepting caller-controlled W3C baggage. The
// handler can rebuild trusted identity after authentication. Using this as the
// server middleware's extractor is important: clearing baggage in a later Gin
// middleware would be too late to prevent the entry span's OnStart processor
// from materializing forged values.
func PublicIngressPropagator() propagation.TextMapPropagator {
	return propagation.TraceContext{}
}

func initSDK(ctx context.Context, minLevel *slog.Level, wrap func(slog.Handler) slog.Handler) (*o11y.SDK, func(context.Context) error, error) {
	// Avoid retaining an earlier successful initialization's policy if a later
	// initialization in the same process (notably a test) fails.
	contextAttributesEnabled.Store(false)
	userBaggageEnabled.Store(false)

	cfg, err := parseConfig()
	if err != nil {
		return nil, nil, err
	}

	opts := cfg.options()
	if minLevel != nil {
		opts = append(opts, o11y.WithLogLevel(*minLevel))
	}
	sdk, err := o11y.Init(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("init o11y sdk: %w", err)
	}
	contextAttributesEnabled.Store(cfg.Enabled)
	userBaggageEnabled.Store(cfg.Enabled && cfg.UserBaggageEnabled)

	otel.SetTracerProvider(sdk.TracerProvider())
	otel.SetMeterProvider(sdk.MeterProvider())
	otel.SetTextMapPropagator(sdk.Propagator)
	logger := sdk.Logger
	if wrap != nil {
		logger = slog.New(wrap(sdk.Logger.Handler()))
	}
	slog.SetDefault(logger)

	return sdk, sdk.Shutdown, nil
}

// ContextWithIdentity attaches trusted request identity to ctx as W3C baggage
// and to the current entry span. Baggage lets o11y's span processor enrich all
// child driver spans and its slog handler enrich contextual logs; explicitly
// setting the current span is necessary because it started before middleware
// could authenticate or parse the identity.
//
// account is PII and is added only when O11Y_USER_BAGGAGE_ENABLED=true; an
// authoritative non-empty account still removes a superseded inbound value
// when the opt-in is off. Room and site use application-scoped keys because
// OpenTelemetry defines no generic semantic convention for them. Empty values
// preserve trusted baggage received from an internal hop.
func ContextWithIdentity(ctx context.Context, account, roomID, siteID string) context.Context {
	if !contextAttributesEnabled.Load() {
		return ctx
	}

	if account != "" {
		ctx = withoutBaggageAttributes(ctx, o11y.UserNameKey)
		if userBaggageEnabled.Load() {
			next, err := o11y.ContextWithUser(ctx, account)
			if err != nil {
				slog.WarnContext(ctx, "telemetry identity attribute omitted", "attribute", o11y.UserNameKey, "error", err)
			} else {
				ctx = next
				o11y.SetUser(ctx, account)
			}
		}
	}
	return contextWithBaggageAttributes(ctx,
		attribute.String(RoomIDKey, roomID),
		attribute.String(SiteIDKey, siteID),
	)
}

// ContextWithPublicIdentity is ContextWithIdentity for a boundary a client can
// reach directly, where inbound baggage is caller-controlled rather than a
// trusted upstream hop's. Every managed key is dropped first — including those
// this boundary has no trusted value for, which ContextWithIdentity would
// otherwise leave in place — so an identifier the subject does not carry (a
// static site token, say) cannot be supplied by the caller instead.
//
// This is the NATS counterpart of PublicIngressPropagator: HTTP can refuse
// baggage at extraction, but a NATS propagator is per-connection, so the drop
// happens on the first hop of the handler chain. Dropping also zeroes the
// entry span's attributes, which the SDK's OnStart processor materialized from
// the forged values before any handler ran.
func ContextWithPublicIdentity(ctx context.Context, account, roomID, siteID string) context.Context {
	if !contextAttributesEnabled.Load() {
		// Emission is off — NATS tracing can still be forced on by
		// OTEL_NATS_TRACING_ENABLED or the flag relay, so the caller's values can
		// reach an observability-enabled downstream service. Nothing here will
		// overwrite them, so every managed key has to go.
		return withoutBaggageAttributes(ctx, managedBaggageKeys...)
	}
	// ContextWithIdentity drops each key it has a trusted value for, so only the
	// unsupplied ones need clearing here.
	ctx = withoutBaggageAttributes(ctx, unsuppliedManagedKeys(account, roomID, siteID)...)
	return ContextWithIdentity(ctx, account, roomID, siteID)
}

// unsuppliedManagedKeys returns the managed keys this boundary has no trusted
// value for. TestUnsuppliedManagedKeys pins it against ManagedBaggageKeys so a
// key added to one is not forgotten here.
func unsuppliedManagedKeys(account, roomID, siteID string) []string {
	keys := make([]string, 0, len(managedBaggageKeys))
	if account == "" {
		keys = append(keys, o11y.UserNameKey)
	}
	if roomID == "" {
		keys = append(keys, RoomIDKey)
	}
	if siteID == "" {
		keys = append(keys, SiteIDKey)
	}
	return keys
}

// contextWithBaggageAttributes sets each non-empty attribute as baggage and
// materializes the accepted ones onto the current span in one write. Empty
// values are skipped, which preserves trusted baggage from an internal hop.
// The o11y SDK validates one member per call, so the baggage rebuilds cannot be
// batched; the span write can, and is.
func contextWithBaggageAttributes(ctx context.Context, kvs ...attribute.KeyValue) context.Context {
	accepted := make([]attribute.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		key, value := string(kv.Key), kv.Value.AsString()
		if value == "" {
			continue
		}
		ctx = withoutBaggageAttributes(ctx, key)
		next, err := o11y.ContextWithBaggageValue(ctx, key, value)
		if err != nil {
			slog.WarnContext(ctx, "telemetry identity attribute omitted", "attribute", key, "error", err)
			continue
		}
		ctx = next
		accepted = append(accepted, kv)
	}
	if len(accepted) > 0 {
		trace.SpanFromContext(ctx).SetAttributes(accepted...)
	}
	return ctx
}

// withoutBaggageAttributes removes superseded inbound values before trusted
// ones are set. If a replacement is rejected (for example, over the SDK size
// limit), the old value must not survive in propagation or on the current entry
// span. Empty span attributes are used only when OnStart already materialized a
// value that OpenTelemetry cannot otherwise delete.
//
// Keys absent from the baggage are skipped, so the common case costs one map
// lookup each and neither a baggage rebuild nor a span write.
func withoutBaggageAttributes(ctx context.Context, keys ...string) context.Context {
	bag := baggage.FromContext(ctx)
	present := make([]string, 0, len(keys))
	cleared := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		if bag.Member(key).Key() == "" {
			continue
		}
		present = append(present, key)
		cleared = append(cleared, attribute.String(key, ""))
	}
	if len(present) == 0 {
		return ctx
	}
	ctx = o11y.ContextWithoutBaggageValues(ctx, present...)
	trace.SpanFromContext(ctx).SetAttributes(cleared...)
	return ctx
}
