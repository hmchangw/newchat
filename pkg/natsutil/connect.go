package natsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultReconnectWait = 2 * time.Second
	// defaultDrainTimeout bounds the subscription-drain phase. Worst case is
	// this plus drainConnection's internal FlushTimeout(5s) = 15s.
	//
	// shutdown.Wait creates ONE context for the whole shutdown and runs the
	// hooks sequentially over it (pkg/shutdown/shutdown.go:21-32), so this
	// budget is shared, not per-hook: a 15s worst-case drain leaves ~10s of
	// the 25s for every remaining hook (DB disconnects, HTTP shutdown, o11y
	// flush), which are sub-second in practice. A drain that actually needs
	// longer than 10s means a wedged handler — a finding to surface, not a
	// wait to extend.
	//
	// The library default is 30s (nats.DefaultDrainTimeout), larger than the
	// entire shutdown budget, so it could never be the timeout that fires.
	defaultDrainTimeout = 10 * time.Second
)

// Connect opens a NATS connection with sensible reconnect defaults.
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
// tp, prop, and tracingEnabled are wired into the underlying o11y/nats layer.
// Production callers should pass sdk.TracerProvider(), sdk.Propagator, and
// sdk.Toggles.Trace from the same obs.Init result. This value is the
// connection-local default; otel-nats v0.9 resolves relay > environment >
// option > module default, so OTEL_NATS_TRACING_ENABLED or a configured relay
// can override it in either direction. With no higher-precedence source, false
// selects the direct path without tracing or propagation overhead.
func Connect(ctx context.Context, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, tracingEnabled bool, opts ...nats.Option) (*o11ynats.Conn, error) {
	return connect(ctx, url, credsFile, tp, prop, tracingEnabled, nil, false, opts...)
}

// ConnectWithMetrics opens a NATS connection and records its bounded lifecycle
// metrics. It is intentionally opt-in so a focused failure campaign does not
// expand instrumentation to every natsutil caller.
func ConnectWithMetrics(ctx context.Context, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, tracingEnabled bool, meterProvider metric.MeterProvider, opts ...nats.Option) (*o11ynats.Conn, error) {
	var metrics *connMetrics
	if meterProvider != nil {
		metrics = newConnMetrics(meterProvider.Meter(connMetricsScope))
	}
	return connect(ctx, url, credsFile, tp, prop, tracingEnabled, metrics, false, opts...)
}

// connectLazy is Connect for a home cluster that may be down at startup: the
// initial dial failing returns a connection in the RECONNECTING state that keeps
// dialing in the background, instead of an error. Configuration faults (a
// missing creds file, a bad URL) still fail fast — retrying those would hide
// them forever. See BuddyDialer.ConnectHome for when this is the right call.
func connectLazy(ctx context.Context, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, tracingEnabled bool, meterProvider metric.MeterProvider) (*o11ynats.Conn, error) {
	var metrics *connMetrics
	if meterProvider != nil {
		metrics = newConnMetrics(meterProvider.Meter(connMetricsScope))
	}
	return connect(ctx, url, credsFile, tp, prop, tracingEnabled, metrics, true)
}

func connect(ctx context.Context, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, tracingEnabled bool, metrics *connMetrics, lazy bool, opts ...nats.Option) (*o11ynats.Conn, error) {
	if credsFile != "" {
		if _, err := os.Stat(credsFile); err != nil {
			return nil, fmt.Errorf("nats creds file %q: %w", credsFile, err)
		}
	}

	name := os.Getenv("HOSTNAME")
	log := slog.With("component", "nats", "name", name)
	connState := metrics.Connection()
	baseOpts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(defaultReconnectWait),
		nats.DrainTimeout(defaultDrainTimeout),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			connState.Disconnected(ctx, err)
			log.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			connState.Reconnected(ctx)
			log.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			connState.Closed(ctx)
			log.Warn("nats connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			connState.AsyncError(ctx, err)
			if errors.Is(err, nats.ErrSlowConsumer) {
				logSlowConsumer(log, sub)
				return
			}
			log.Error("nats async error", "error", err)
		}),
	}
	if lazy {
		baseOpts = append(baseOpts,
			nats.RetryOnFailedConnect(true),
			// Fires only for the initial connect, and only once it lands — the
			// synchronous gauge update below is skipped when the dial is still
			// in flight, so this is the one place that reports it.
			nats.ConnectHandler(func(c *nats.Conn) {
				connState.Connected(ctx)
				log.Info("nats connected", "url", c.ConnectedUrl())
			}),
		)
	}
	baseOpts = append(baseOpts, opts...)
	// Credentials are just another nats.Option in the o11y/nats path; mounting
	// them via UserCredentials keeps a single Connect call regardless of auth.
	if credsFile != "" {
		baseOpts = append(baseOpts, nats.UserCredentials(credsFile))
	}

	conn, err := o11ynats.ConnectWithOptions(
		ctx,
		url,
		tp,
		prop,
		o11ynats.WithTracingEnabled(tracingEnabled),
		o11ynats.WithNATSOptions(baseOpts...),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	if conn.NatsConn().Status() != nats.CONNECTED {
		// Only a lazy dial gets here: the first attempt failed and the client
		// is retrying in the background. The ConnectHandler reports the gauge
		// when it lands.
		log.Warn("nats unreachable at startup; connecting in the background", "url", url)
		return conn, nil
	}
	// The library fires no callback for the first successful connect on a
	// fail-fast dial, so the gauge would stay at zero until the first disconnect
	// without this.
	connState.Connected(ctx)
	return conn, nil
}
