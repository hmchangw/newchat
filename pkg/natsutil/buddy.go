package natsutil

import (
	"context"
	"log/slog"

	o11ynats "github.com/flywindy/o11y/nats"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ConnectBuddy opens the secondary connection to a site's buddy cluster, where
// its standby failover streams live.
//
// Unlike the home connection — which is fail-fast, because a service with no bus
// cannot work — this NEVER fails startup. A buddy that is already down when we
// start is a double fault, and running home-lane-only is strictly better than
// refusing to boot. A nil return means "no failover lane"; callers skip binding
// it and carry on.
//
// An empty url means the buddy is unconfigured, which is a normal single-site
// deployment, not an error.
func ConnectBuddy(ctx context.Context, url, credsFile string, tp trace.TracerProvider,
	prop propagation.TextMapPropagator, tracingEnabled bool,
) *o11ynats.Conn {
	if url == "" {
		return nil
	}
	conn, err := Connect(ctx, url, credsFile, tp, prop, tracingEnabled)
	if err != nil {
		slog.WarnContext(ctx, "buddy nats connect failed; running without the failover lane",
			"url", url, "error", err)
		return nil
	}
	slog.InfoContext(ctx, "buddy nats connected", "url", url)
	return conn
}

// BuddyConfig is the buddy-lane env block, embedded by every service that binds
// a standby lane:
//
//	Buddy natsutil.BuddyConfig `envPrefix:"BUDDY_"`
//
// Shared rather than re-declared per service so the tags, defaults, and the
// meaning of "unset" cannot drift across the fleet.
type BuddyConfig struct {
	// SiteID is the site whose NATS cluster hosts this site's standby failover
	// streams. It is also the cluster name placement is asserted against, so an
	// empty value means there is nothing to verify a standby stream is on.
	SiteID string `env:"SITE_ID" envDefault:""`
	// NatsURL is the buddy cluster's NATS URL. Unlike NATS_URL this is not
	// fail-fast: an unreachable buddy degrades to a home-only service.
	NatsURL string `env:"NATS_URL" envDefault:""`
}

// Enabled reports whether a buddy lane is configured. Both halves are required:
// a URL with no site ID has no cluster name to assert placement against, and a
// site ID with no URL has nothing to dial. Neither set is the normal
// single-site deployment, not an error.
func (c BuddyConfig) Enabled() bool { return c.SiteID != "" && c.NatsURL != "" }

// OnlyIf disables the lane unless ok holds, for services whose failover lane is
// conditional on something beyond configuration — a pipeline with no standby
// streams, a migration mode, a deployment with no federation peers.
//
// It narrows and never widens: an unconfigured buddy stays unconfigured. Say
// `cfg.Buddy.OnlyIf(wiring.HasFailover())` rather than reassigning the struct to
// its zero value, so the intent is stated rather than inferred from emptiness —
// and so a field added to BuddyConfig later is not silently blanked at every
// gating site.
func (c BuddyConfig) OnlyIf(ok bool) BuddyConfig {
	if !ok {
		return BuddyConfig{}
	}
	return c
}

// BindBuddy dials the buddy cluster and hands the live connection and its
// JetStream context to bind, which readies the standby streams and binds
// whatever consumers the service needs.
//
// bind gets both because a failover lane usually consumes over JetStream but
// publishes over core NATS, and both must go to the buddy: the home cluster is
// the one that is down.
//
// It never fails startup. An unconfigured buddy, a refused connection, a
// JetStream init failure, or a bind error all log and leave the service running
// home-lane-only — a buddy that is already down at startup is a double fault,
// and refusing to boot over a peer cluster we only need during an outage would
// turn it into an outage of its own.
//
// The returned Conn is nil exactly when no connection was established. A bind
// error still returns the live connection so shutdown drains it rather than
// leaking it; callers detect the degraded lane through whatever bind assigns.
func BindBuddy(ctx context.Context, cfg BuddyConfig, credsFile string, tp trace.TracerProvider,
	prop propagation.TextMapPropagator, tracingEnabled bool,
	bind func(context.Context, *o11ynats.Conn, o11ynats.JetStream) error,
) *o11ynats.Conn {
	if !cfg.Enabled() {
		return nil
	}
	conn := ConnectBuddy(ctx, cfg.NatsURL, credsFile, tp, prop, tracingEnabled)
	if conn == nil {
		return nil
	}
	js, err := conn.JetStream()
	if err != nil {
		slog.WarnContext(ctx, "buddy jetstream init failed; running without the failover lane",
			"buddy_site_id", cfg.SiteID, "error", err)
		return conn
	}
	if err := bind(ctx, conn, js); err != nil {
		slog.WarnContext(ctx, "failover lane unavailable; running without it",
			"buddy_site_id", cfg.SiteID, "error", err)
		return conn
	}
	slog.InfoContext(ctx, "failover lane bound", "buddy_site_id", cfg.SiteID)
	return conn
}

// DrainBuddy is the shutdown hook for a possibly-nil buddy connection, so the
// nil check lives in one place rather than in every service's shutdown list.
func DrainBuddy(conn *o11ynats.Conn) func(context.Context) error {
	return func(ctx context.Context) error {
		if conn == nil {
			return nil
		}
		return Drain(ctx, conn)
	}
}
