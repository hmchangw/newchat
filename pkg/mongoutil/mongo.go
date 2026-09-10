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

// WithDegradedStart downgrades an unreachable MongoDB at startup from an error
// to a warning: Connect returns a usable client and the service starts. For a
// service whose primary datastore is not MongoDB — the driver reconnects on its
// own (SDAM) once MongoDB returns, and the call sites already handle its errors
// through breakers, L2 caches and fail-open paths.
//
// Rejected credentials still return an error: the server answered, so that is
// misconfiguration, not an outage. So do an unparseable URI and an
// instrumentation failure, which fail before the ping. Everything the ping
// cannot tell from an outage — a wrong host or port, TLS, a timeout — starts
// degraded with a warning naming the error: the driver keeps re-dialling on
// its own, and for a service designated degradable a false degrade costs a
// warning and a cache-served pod where a false fatal costs the outage.
func WithDegradedStart() Option {
	return func(c *connectConfig) { c.degradedStart = true }
}

// startupPingTimeout is the floor of Connect's one-time ping bound, applied
// independently of the caller's ctx (services pass context.Background). The
// driver sets no operation timeout by default, so a MongoDB that answers hello
// but is too overloaded to serve a ping would otherwise hang startup
// indefinitely — neither failing nor degrading. An earlier caller deadline
// still wins.
const startupPingTimeout = 10 * time.Second

// startupPingMargin is the dial-and-handshake allowance added on top of the
// server-selection timeout when that is what sets the ping bound.
const startupPingMargin = 5 * time.Second

// driverServerSelectionDefault is the driver's own ServerSelectionTimeout when
// neither the options nor the URI set one.
const driverServerSelectionDefault = 30 * time.Second

// startupPingBound derives the ping bound from the client's effective
// server-selection timeout (nil or zero: the driver default): the 10s floor
// covers the pooled 2s default with room for a dial and an auth handshake,
// while a pool-less caller — or an operator who raised the selection timeout
// to ride out an election — keeps the wait they configured plus a margin,
// instead of a constant they cannot see turning that election into a fatal
// exit.
func startupPingBound(serverSelectionTimeout *time.Duration) time.Duration {
	sst := driverServerSelectionDefault
	if serverSelectionTimeout != nil && *serverSelectionTimeout > 0 {
		sst = *serverSelectionTimeout
	}
	return max(startupPingTimeout, sst+startupPingMargin)
}

// isAuthError reports whether err is MongoDB rejecting our credentials — the
// one startup failure WithDegradedStart never tolerates. The driver surfaces
// it as the public CommandError (see wrapErrors in mongo/errors.go) on an
// operation, and as the raw driver.Error inside a ConnectionPoolCleared event
// (credentialProbe), so both are matched. The whole error tree is searched,
// not just the first match: a ping that lost the warm-pool race fails with a
// code-0 CommandError over the cleared pool, joined with the rejection the
// probe recorded, and errors.As would stop at the code 0.
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

// isAuthCode is the allowlist of server error codes that mean "credentials
// rejected". It is deliberately an allowlist: a server-answered error is not
// enough on its own, since a socket reset mid-ping surfaces as code 0 with a
// NetworkError label and an overloaded server past the bound answers
// MaxTimeMSExpired (50) — both outages a degradable service must survive.
//
// 18 AuthenticationFailed (wrong password); 13 Unauthorized; 11 UserNotFound
// (wrong user or authSource — the server looked and found nobody, still a
// rejection, not an outage); 8000 AtlasError, Atlas's "bad auth"; 334
// MechanismUnavailable; and BadValue (2) only when the message names the
// mechanism, which is how a server without the configured SCRAM variant
// answers — a plain BadValue is an ordinary bad command.
func isAuthCode(code int32, message string) bool {
	switch code {
	case 18, 13, 11, 8000, 334:
		return true
	case 2:
		return strings.Contains(strings.ToLower(message), "mechanism")
	}
	return false
}

// credentialProbe records the first credential rejection the connection pool
// reports. With a warm floor (MinPoolSize > 0) the pool's background
// connections race the startup ping to the server: the first to fail SCRAM
// clears the pool, and the ping's own checkout then receives only "pool
// cleared" — no CommandError, no code — so the ping alone cannot tell a
// rejection from an outage. The pool emits ConnectionPoolCleared, carrying the
// handshake error, before it fails the queued checkouts, so consulting the
// probe after a failed ping is deterministic where retrying the ping is not
// (each pool re-ready re-runs the same race).
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

	// Set before instrumentation: o11y chains its own monitor after one already
	// set, and the probe must see the pool clears the ping cannot (see
	// credentialProbe). Nothing earlier sets a monitor (a URI cannot).
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
	// Stored before the ping: the degraded path below keeps the client, so the
	// client must already own its instrumentation teardown or Disconnect at
	// shutdown would not flush the SDK's pool metrics.
	if cleanup != nil {
		cleanups.Store(client, cleanup)
	}
	pingCtx, cancel := context.WithTimeout(ctx, startupPingBound(clientOpts.ServerSelectionTimeout))
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		rejected := probe.rejection()
		if rejected != nil {
			// The pool saw the rejection the ping could not: carry it as the
			// cause and keep the ping's own error as text.
			err = fmt.Errorf("credentials rejected on a pooled connection: %w (ping: %v)", rejected, err)
		}
		// Only a credential rejection is fatal here — see WithDegradedStart —
		// and a caller that has stopped waiting: a cancelled or expired ctx is
		// the caller asking to stop, not MongoDB being down, so it is returned
		// rather than turned into a live client.
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
