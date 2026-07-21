package mongoutil

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	o11ymongo "github.com/flywindy/o11y/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
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

func connect(ctx context.Context, clientOpts *options.ClientOptions, uri string, opts ...Option) (*mongo.Client, error) {
	cfg := newConnectConfig(opts...)

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
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		runCleanup(cleanup)
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	if cleanup != nil {
		cleanups.Store(client, cleanup)
	}
	slog.Info("connected to MongoDB", "uri", sanitizeURI(uri))
	return client, nil
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
