package natsutil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const defaultReconnectWait = 2 * time.Second

// Connect opens a traced NATS connection with sensible reconnect defaults.
// The NATS client name is taken from the HOSTNAME env var (pod name in
// Kubernetes, container ID in Docker). When credsFile is non-empty it is
// mounted as the user credentials (JWT + NKey); when empty the connection
// authenticates without credentials. Extra opts are appended and override any
// same-kind default.
//
// The initial connect fails fast: if NATS is unreachable at startup, the
// caller receives the error and is expected to log + exit. Reconnect handlers
// fire only after the first successful connect.
//
// tp and prop are wired into the underlying o11y/nats layer so trace context
// propagates across publishers and subscribers without touching global
// OpenTelemetry state — pass sdk.TracerProvider() and sdk.Propagator from the
// service's obs.Init.
func Connect(ctx context.Context, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, opts ...nats.Option) (*o11ynats.Conn, error) {
	// o11y/nats gates trace-context propagation on two env flags (both default
	// off) and exposes no programmatic override — when unset, Publish/Subscribe
	// skip header injection/extraction and cross-NATS trace continuity silently
	// breaks. This system always wants NATS tracing on, so enable the flags here
	// unless an operator explicitly set them (e.g. to "false" to opt out).
	enableNATSTracing()

	if credsFile != "" {
		if _, err := os.Stat(credsFile); err != nil {
			return nil, fmt.Errorf("nats creds file %q: %w", credsFile, err)
		}
	}

	name := os.Getenv("HOSTNAME")
	log := slog.With("component", "nats", "name", name)
	baseOpts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(defaultReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Warn("nats connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Error("nats async error", "error", err)
		}),
	}
	baseOpts = append(baseOpts, opts...)
	// Credentials are just another nats.Option in the o11y/nats path; mounting
	// them via UserCredentials keeps a single Connect call regardless of auth.
	if credsFile != "" {
		baseOpts = append(baseOpts, nats.UserCredentials(credsFile))
	}

	conn, err := o11ynats.Connect(ctx, url, tp, prop, baseOpts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	return conn, nil
}

// enableNATSTracing turns on the o11y/nats (Marz otelnats/oteljetstream) trace
// gates unless an operator already set them. Both must be truthy for traceparent
// to flow across NATS; there is no programmatic override, so this is the single
// enforcement point for every entrypoint (services, dev, tools, tests).
//
// It is itself gated on the O11Y_ENABLED master switch (default off, matching
// pkg/obs): when observability is off we deliberately leave the gates unset so
// otelnats skips per-message header inject/extract and span work — the NATS hot
// path runs at ~native cost. An operator can still set either gate env
// explicitly to override in either direction.
func enableNATSTracing() {
	if !o11yEnabled() {
		return
	}
	for _, k := range []string{
		"OTEL_INSTRUMENTATION_GO_TRACING_ENABLED",
		"OTEL_NATS_TRACING_ENABLED",
	} {
		if _, ok := os.LookupEnv(k); !ok {
			_ = os.Setenv(k, "true")
		}
	}
}

// o11yEnabled reports the O11Y_ENABLED master switch (default false), kept in
// sync with pkg/obs so NATS tracing follows the same on/off as the SDK.
func o11yEnabled() bool {
	v, ok := os.LookupEnv("O11Y_ENABLED")
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	return err == nil && b
}
