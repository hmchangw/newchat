package natsutil

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/flywindy/o11y"
	o11ynats "github.com/flywindy/o11y/nats"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ConnectFailoverSite opens the secondary connection to a site's failover cluster, where
// its standby failover streams live.
//
// Unlike the home connection — which is fail-fast, because a service with no bus
// cannot work — this NEVER fails startup. A failover that is already down when we
// start is a double fault, and running home-lane-only is strictly better than
// refusing to boot. A nil return means "no failover lane"; callers skip binding
// it and carry on.
//
// An empty url means the failover is unconfigured, which is a normal single-site
// deployment, not an error.
func ConnectFailoverSite(ctx context.Context, url, credsFile string, tp trace.TracerProvider,
	prop propagation.TextMapPropagator, tracingEnabled bool,
) *o11ynats.Conn {
	if url == "" {
		return nil
	}
	conn, err := Connect(ctx, url, credsFile, tp, prop, tracingEnabled)
	if err != nil {
		slog.WarnContext(ctx, "failover nats connect failed; running without the failover lane",
			"url", url, "error", err)
		return nil
	}
	slog.InfoContext(ctx, "failover nats connected", "url", url)
	return conn
}

// FailoverSiteConfig is the failover-lane env block, embedded by every service that binds
// a standby lane:
//
//	Failover natsutil.FailoverSiteConfig `envPrefix:"FAILOVER_"`
//
// Shared rather than re-declared per service so the tags, defaults, and the
// meaning of "unset" cannot drift across the fleet.
type FailoverSiteConfig struct {
	// SiteID is the site whose NATS cluster hosts this site's standby failover
	// streams. It is also the cluster name placement is asserted against, so an
	// empty value means there is nothing to verify a standby stream is on.
	SiteID string `env:"SITE_ID" envDefault:""`
	// NatsURL is the failover cluster's NATS URL. Unlike NATS_URL this is not
	// fail-fast: an unreachable failover degrades to a home-only service.
	NatsURL string `env:"NATS_URL" envDefault:""`
}

// Enabled reports whether a failover lane is configured. Both halves are required:
// a URL with no site ID has no cluster name to assert placement against, and a
// site ID with no URL has nothing to dial. Neither set is the normal
// single-site deployment, not an error.
func (c FailoverSiteConfig) Enabled() bool { return c.SiteID != "" && c.NatsURL != "" }

// SameSiteAsHome reports whether the configured failover cluster is the service's
// own cluster.
//
// Under a per-site buddy this could not happen: a buddy was some other site, and
// naming yourself was a typo nobody made. A SHARED failover site is one value
// applied fleet-wide, so the site that hosts it receives its own id here along
// with everyone else — and if that site also runs chat services, they would bind
// a standby lane on the cluster the lane exists to outlive.
//
// Nothing downstream catches it. CheckPlacement compares the stream's cluster
// against this same SiteID, so the assertion passes; the lane binds, reports
// ready, and is a no-op the moment it is needed. That is why this is a config
// question answered before the dial rather than a placement question answered
// after it.
func (c FailoverSiteConfig) SameSiteAsHome(homeSiteID string) bool {
	return c.Enabled() && homeSiteID != "" && c.SiteID == homeSiteID
}

// OnlyIf disables the lane unless ok holds, for services whose failover lane is
// conditional on something beyond configuration — a pipeline with no standby
// streams, a migration mode, a deployment with no federation peers.
//
// It narrows and never widens: an unconfigured failover stays unconfigured. Say
// this rather than zeroing the struct at each site, which states the gate and
// cannot silently blank a field added to FailoverSiteConfig later.
func (c FailoverSiteConfig) OnlyIf(ok bool) FailoverSiteConfig {
	if !ok {
		return FailoverSiteConfig{}
	}
	return c
}

// FailoverDialer is everything needed to dial a site's failover cluster, built once
// per service from its config and observability SDK. It replaces the five-value
// tuple (config, creds file, tracer, propagator, tracing flag) that every lane
// binder used to take and every service forwarded verbatim.
type FailoverDialer struct {
	Config         FailoverSiteConfig
	CredsFile      string
	TracerProvider trace.TracerProvider
	Propagator     propagation.TextMapPropagator
	TracingEnabled bool
}

// ConnectHome dials the service's own cluster. Without a failover it fails fast,
// exactly as Connect does: a single-site service with no bus has nothing to do,
// and crash-looping is the right signal. With a failover configured the dial is
// lazy — a home cluster that is down at startup yields a RECONNECTING
// connection that keeps dialing in the background, so a pod that restarts
// during the outage still boots, binds its failover lane, and joins the home lane
// the moment the cluster returns. Without that, every rollout, eviction or
// crash during an outage removed a worker from the standby lane too.
//
// Home is "the connection the lane comes up on" only once it is CONNECTED:
// callers bind JetStream consumers through BindWhenConnected, which defers the
// bind until then. Core subscriptions need no such care — nats.go buffers a
// subscription made while reconnecting and replays it on connect.
//
// meterProvider may be nil to skip connection metrics.
func (d *FailoverDialer) ConnectHome(ctx context.Context, url string, meterProvider metric.MeterProvider) (*o11ynats.Conn, error) {
	if !d.Config.Enabled() {
		conn, err := ConnectWithMetrics(ctx, url, d.CredsFile, d.TracerProvider, d.Propagator, d.TracingEnabled, meterProvider)
		if err != nil {
			return nil, fmt.Errorf("connect home nats: %w", err)
		}
		return conn, nil
	}
	conn, err := connectLazy(ctx, url, d.CredsFile, d.TracerProvider, d.Propagator, d.TracingEnabled, meterProvider)
	if err != nil {
		return nil, fmt.Errorf("connect home nats: %w", err)
	}
	return conn, nil
}

// NewFailoverDialer builds the dialer every failover-capable service needs from
// its config and the observability SDK. One constructor, so the tracing tuple
// cannot be copied wrong from one main to the next.
func NewFailoverDialer(cfg FailoverSiteConfig, homeSiteID, credsFile string, sdk *o11y.SDK) *FailoverDialer {
	if cfg.SameSiteAsHome(homeSiteID) {
		// Disabled rather than fatal. This fires on the site that HOSTS the shared
		// failover cluster, whose own services are otherwise healthy; refusing to
		// boot them would turn a lane that was never going to help into an outage.
		// ERROR, not WARN: unlike an unreachable failover site, nothing here
		// recovers on its own.
		slog.Error("failover site is this service's own site — standby lane disabled, running home-only",
			"site_id", homeSiteID, "failover_site_id", cfg.SiteID)
		cfg = FailoverSiteConfig{}
	}
	return &FailoverDialer{
		Config: cfg, CredsFile: credsFile,
		TracerProvider: sdk.TracerProvider(), Propagator: sdk.Propagator, TracingEnabled: sdk.Toggles.Trace,
	}
}

// ConnectHomeJS is ConnectHome plus the JetStream context every service builds
// next. The context needs no live server — it only fixes the API prefix — so
// it is ready even while a lazy dial is still in flight.
func (d *FailoverDialer) ConnectHomeJS(ctx context.Context, url string, meterProvider metric.MeterProvider) (*o11ynats.Conn, o11ynats.JetStream, error) {
	conn, err := d.ConnectHome(ctx, url, meterProvider)
	if err != nil {
		return nil, nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		return nil, nil, fmt.Errorf("init home jetstream: %w", err)
	}
	return conn, js, nil
}

// OnlyIf narrows the dialer's config exactly as FailoverSiteConfig.OnlyIf does, so a
// service gates its lane on the value it already passes around.
func (d *FailoverDialer) OnlyIf(ok bool) *FailoverDialer {
	narrowed := *d
	narrowed.Config = d.Config.OnlyIf(ok)
	return &narrowed
}

// Bind dials the failover cluster and hands the live connection and its JetStream
// context to bind, which readies the standby streams and binds whatever the
// service needs there.
//
// bind gets both because a failover lane usually consumes over JetStream but
// publishes over core NATS, and both must go to the failover: the home cluster is
// the one that is down.
//
// It never fails startup. An unconfigured failover, a refused connection, a
// JetStream init failure, or a bind error all log and leave the service running
// home-lane-only — a failover that is already down at startup is a double fault,
// and refusing to boot over a peer cluster we only need during an outage would
// turn it into an outage of its own.
//
// The returned Conn is nil exactly when no connection was established. A bind
// error still returns the live connection so shutdown drains it rather than
// leaking it; callers detect the degraded lane through whatever bind assigns.
func (d *FailoverDialer) Bind(ctx context.Context, bind func(context.Context, *o11ynats.Conn, o11ynats.JetStream) error) *o11ynats.Conn {
	if !d.Config.Enabled() {
		return nil
	}
	conn := ConnectFailoverSite(ctx, d.Config.NatsURL, d.CredsFile, d.TracerProvider, d.Propagator, d.TracingEnabled)
	if conn == nil {
		return nil
	}
	js, err := conn.JetStream()
	if err != nil {
		slog.WarnContext(ctx, "failover jetstream init failed; running without the failover lane",
			"failover_site_id", d.Config.SiteID, "error", err)
		return conn
	}
	if err := bind(ctx, conn, js); err != nil {
		slog.WarnContext(ctx, "failover lane unavailable; running without it",
			"failover_site_id", d.Config.SiteID, "error", err)
		return conn
	}
	slog.InfoContext(ctx, "failover lane bound", "failover_site_id", d.Config.SiteID)
	return conn
}

// DrainFailoverSite is the shutdown hook for a possibly-nil failover connection, so the
// nil check lives in one place rather than in every service's shutdown list.
func DrainFailoverSite(conn *o11ynats.Conn) func(context.Context) error {
	return func(ctx context.Context) error {
		if conn == nil {
			return nil
		}
		return Drain(ctx, conn)
	}
}
