package valkeyutil

import (
	"context"
	"errors"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// Config is an env-tagged Valkey connection configuration. Add it as a named
// field to a service's config (e.g. `Valkey valkeyutil.Config`) and pass it to
// Connect or ConnectOptional, rather than declaring the two env vars and
// re-writing the dial block per service.
//
// Ten services had hand-rolled that block. Three of the copies differed in ways
// nobody chose: one omitted request-scoped span filtering, and the three failure
// policies (exit, log-and-continue, return) were distributed across them by
// accident rather than by which tier was actually optional.
type Config struct {
	Addrs    []string `env:"VALKEY_ADDRS" envSeparator:","`
	Password string   `env:"VALKEY_PASSWORD"`
}

// Enabled reports whether a Valkey is configured. No addresses is a valid
// deployment — every tier treats a nil Client as "off" — so it is not an error.
func (c Config) Enabled() bool { return len(c.Addrs) > 0 }

// Validate rejects an unconfigured Valkey, for a service that cannot run without
// one. Most services can — a nil Client disables the tier — so this is opt-in
// rather than part of Connect, and the three services that need it call it
// beside their PoolConfig.Validate.
func (c Config) Validate() error {
	if !c.Enabled() {
		return errors.New("VALKEY_ADDRS must not be empty")
	}
	return nil
}

// dial and dialRaw are the cluster dialers, package vars so tests can exercise
// the failure policies below without a network.
var (
	dial    = ConnectCluster
	dialRaw = dialCluster
)

// Instrumented is the fleet-standard instrumentation bundle: the o11y providers
// plus request-scoped spans only.
//
// Command spans are kept only when the command already sits inside a traced
// request or consumer flow, which drops startup probes and background client
// chatter. Bundled rather than left to each call site because the one service
// that passed the providers without it emitted that noise for no chosen reason.
func Instrumented(o Observability) Option {
	return func(c *connectConfig) {
		WithObservability(o)(c)
		WithRequireParentSpan(true)(c)
	}
}

// Connect dials the cluster described by cfg, returning (nil, nil) when no
// addresses are configured. Use it where Valkey is a hard dependency and the
// caller decides what a failure means.
func Connect(ctx context.Context, cfg Config, opts ...Option) (Client, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	return dial(ctx, cfg.Addrs, cfg.Password, opts...)
}

// ConnectRaw is Connect for a caller that needs the concrete client — badgecache
// takes a *redis.ClusterClient. It returns (nil, nil) when no addresses are
// configured, so the caller branches on the client rather than on the config.
//
// Prefer Connect. This exists so the three services with that requirement share
// the dial instead of re-implementing construct-instrument-ping by hand.
func ConnectRaw(ctx context.Context, cfg Config, opts ...Option) (*redis.ClusterClient, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	return dialRaw(ctx, cfg.Addrs, cfg.Password, opts...)
}

// ConnectOptional dials cfg and degrades to a nil Client on failure, logging
// once against label. For a tier that is an optimisation rather than a startup
// dependency — a cache that falls through, an invalidation that the TTL
// reconciles — where exiting would trade a degraded service for no service.
//
// Every tier and bust in this repo accepts a nil Client, so the caller needs no
// further branch.
func ConnectOptional(ctx context.Context, cfg Config, label string, opts ...Option) Client {
	client, err := Connect(ctx, cfg, opts...)
	if err != nil {
		slog.ErrorContext(ctx, "valkey connect failed, continuing without it", "tier", label, "error", err)
		return nil
	}
	return client
}
