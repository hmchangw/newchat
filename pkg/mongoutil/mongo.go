package mongoutil

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	o11ymongo "github.com/flywindy/o11y/mongo"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.opentelemetry.io/otel"
)

// cleanups holds the o11y instrumentation teardown func for each instrumented
// client so Disconnect can run it (flushing SDK-owned pool metrics) without
// changing Connect's return signature — keeping the migration incremental.
var cleanups sync.Map // *mongo.Client -> func(context.Context) error

func Connect(ctx context.Context, uri, username, password string, opts ...Option) (*mongo.Client, error) {
	return connect(ctx, buildClientOptions(uri, username, password), uri, opts...)
}

// ConnectRead connects a read-oriented client: the same instrumented
// connect/ping flow as Connect with ReadPreference=secondaryPreferred, so reads
// can be served by secondaries. For services that split Mongo traffic into
// separate read and write clients (e.g. teams-user-sync).
func ConnectRead(ctx context.Context, uri, username, password string, opts ...Option) (*mongo.Client, error) {
	return connect(ctx, buildReadClientOptions(uri, username, password), uri, opts...)
}

// WithDegradedStart makes Connect warn and return a usable client on an unreachable MongoDB (the driver
// reconnects on its own), for services whose primary datastore is not MongoDB. Rejected credentials still fail.
func WithDegradedStart() Option {
	return func(c *connectConfig) { c.degradedStart = true }
}

// startupPingTimeout floors Connect's one-time ping bound: the driver has no default operation timeout,
// so an overloaded MongoDB that answers hello would otherwise hang startup. A sooner caller deadline wins.
const startupPingTimeout = 10 * time.Second

// startupPingMargin is the dial-and-handshake allowance on top of the server-selection timeout.
const startupPingMargin = 5 * time.Second

// driverServerSelectionDefault is the driver's ServerSelectionTimeout when neither options nor URI set one.
const driverServerSelectionDefault = 30 * time.Second

// startupPingBound derives the ping bound from the effective server-selection timeout (nil or zero: the
// driver default): the floor covers the pooled 2s default; a longer configured wait keeps its own margin.
func startupPingBound(serverSelectionTimeout *time.Duration) time.Duration {
	sst := driverServerSelectionDefault
	if serverSelectionTimeout != nil && *serverSelectionTimeout > 0 {
		sst = *serverSelectionTimeout
	}
	return max(startupPingTimeout, sst+startupPingMargin)
}

// isAuthError reports whether err is MongoDB rejecting our credentials, as a CommandError (operations) or
// a driver.Error (pool events). The whole tree is walked: errors.As would stop at a code-0 CommandError.
func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	switch e := err.(type) {
	case mongo.CommandError:
		if isAuthCode(e.Code, e.Message) {
			return true
		}
	case driver.Error:
		if isAuthCode(e.Code, e.Message) {
			return true
		}
	}
	switch u := err.(type) {
	case interface{ Unwrap() error }:
		return isAuthError(u.Unwrap())
	case interface{ Unwrap() []error }:
		for _, inner := range u.Unwrap() {
			if isAuthError(inner) {
				return true
			}
		}
	}
	return false
}

// isAuthCode allowlists the codes meaning "credentials rejected": 18, 13, 11 (wrong user or authSource),
// 8000 (Atlas), 334, and BadValue 2 only naming a mechanism. Code-0 network errors and 50 stay outages.
func isAuthCode(code int32, message string) bool {
	switch code {
	case 18, 13, 11, 8000, 334:
		return true
	case 2:
		return strings.Contains(strings.ToLower(message), "mechanism")
	}
	return false
}

// credentialProbe records the first credential rejection the pool reports: with MinPoolSize > 0 a warm-up
// connection fails SCRAM first and clears the pool, so the ping sees only "pool cleared" with no code.
type credentialProbe struct {
	mu  sync.Mutex
	err error
}

func (p *credentialProbe) monitor() *event.PoolMonitor {
	return &event.PoolMonitor{Event: func(e *event.PoolEvent) {
		if e.Type != event.ConnectionPoolCleared || !isAuthError(e.Error) {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.err == nil {
			p.err = e.Error
		}
	}}
}

// rejection returns the recorded credential rejection, or nil.
func (p *credentialProbe) rejection() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func connect(ctx context.Context, clientOpts *options.ClientOptions, uri string, opts ...Option) (*mongo.Client, error) {
	cfg := newConnectConfig(opts...)
	cfg.applyTuning(clientOpts)

	cfg.applyReadPreference(clientOpts)

	// Before instrumentation, so o11y chains after it; the probe must see the pool clears the ping cannot.
	var probe credentialProbe
	clientOpts.SetPoolMonitor(probe.monitor())

	var cleanup func(context.Context) error
	if cfg.obs != nil {
		// Propagator comes from the OTel global (obs.Init installs sdk.Propagator
		// there) rather than the Observability interface, matching o11y/mongo's
		// own examples. It is effectively inert for a Mongo client anyway — the
		// server never extracts traceparent — so spans still nest via ctx.
		c, err := o11ymongo.Instrument(clientOpts, cfg.obs.TracerProvider(), cfg.obs.MeterProvider(), otel.GetTextMapPropagator())
		if err != nil {
			return nil, fmt.Errorf("instrument mongo client: %w", err)
		}
		cleanup = c
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		runCleanup(cleanup)
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	// Stored before the ping: the degraded path keeps the client, which must own its teardown to flush pool metrics.
	if cleanup != nil {
		cleanups.Store(client, cleanup)
	}
	pingCtx, cancel := context.WithTimeout(ctx, startupPingBound(clientOpts.ServerSelectionTimeout))
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		rejected := probe.rejection()
		if rejected != nil {
			// The pool saw the rejection the ping could not: carry it as the cause, keep the ping's text.
			err = fmt.Errorf("credentials rejected on a pooled connection: %w (ping: %v)", rejected, err)
		}
		// Fatal only for a credential rejection (see WithDegradedStart) and a ctx that ended: the caller asked to stop.
		if !cfg.degradedStart || ctx.Err() != nil || rejected != nil || isAuthError(err) {
			Disconnect(context.Background(), client)
			return nil, fmt.Errorf("mongo ping: %w", err)
		}
		slog.Warn("mongo ping failed at startup; continuing in degraded mode",
			"uri", sanitizeURI(uri), "error", err)
		return client, nil
	}
	slog.Info("connected to MongoDB", "uri", sanitizeURI(uri))
	return client, nil
}

// applyReadPreference binds an explicit WithReadPreference onto clientOpts,
// overriding whatever it carried (e.g. ConnectRead's secondaryPreferred). A nil
// preference is left alone so an unset option never clobbers a URI-provided value.
func (c connectConfig) applyReadPreference(clientOpts *options.ClientOptions) {
	if c.readPref != nil {
		clientOpts.SetReadPreference(c.readPref)
	}
}

// sanitizeURI reduces a connection string to scheme://host/path so it is safe
// to log: userinfo (user:pass@) may embed credentials, and query options can
// carry secrets too (e.g. authMechanismProperties session tokens,
// proxyPassword).
func sanitizeURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "invalid-uri"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.Redacted()
}

func Disconnect(ctx context.Context, client *mongo.Client) {
	if v, ok := cleanups.LoadAndDelete(client); ok {
		if fn, ok := v.(func(context.Context) error); ok {
			if err := fn(ctx); err != nil {
				slog.Error("mongo instrumentation cleanup failed", "error", err)
			}
		}
	}
	if err := client.Disconnect(ctx); err != nil {
		slog.Error("mongo disconnect failed", "error", err)
	}
}

func runCleanup(cleanup func(context.Context) error) {
	if cleanup != nil {
		_ = cleanup(context.Background())
	}
}

func buildClientOptions(uri, username, password string) *options.ClientOptions {
	opts := options.Client().ApplyURI(uri)
	if username != "" && password != "" {
		opts.SetAuth(options.Credential{
			Username: username,
			Password: password,
		})
	}
	return opts
}

func buildReadClientOptions(uri, username, password string) *options.ClientOptions {
	return buildClientOptions(uri, username, password).SetReadPreference(readpref.SecondaryPreferred())
}
