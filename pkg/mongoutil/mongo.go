package mongoutil

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func Connect(ctx context.Context, uri, username, password string) (*mongo.Client, error) {
	return connect(ctx, buildClientOptions(uri, username, password), uri)
}

// ConnectRead connects a read-oriented client: the same connect/ping flow as
// Connect with ReadPreference=secondaryPreferred, so reads can be served by
// secondaries. For services that split Mongo traffic into separate read and
// write clients (e.g. teams-user-sync).
func ConnectRead(ctx context.Context, uri, username, password string) (*mongo.Client, error) {
	return connect(ctx, buildReadClientOptions(uri, username, password), uri)
}

func connect(ctx context.Context, opts *options.ClientOptions, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	slog.Info("connected to MongoDB", "uri", sanitizeURI(uri))
	return client, nil
}

// sanitizeURI strips any userinfo (user:pass@) from a connection string so it
// is safe to log — URIs may embed credentials instead of using the separate
// username/password parameters.
func sanitizeURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "invalid-uri"
	}
	u.User = nil
	return u.Redacted()
}

func Disconnect(ctx context.Context, client *mongo.Client) {
	if err := client.Disconnect(ctx); err != nil {
		slog.Error("mongo disconnect failed", "error", err)
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
