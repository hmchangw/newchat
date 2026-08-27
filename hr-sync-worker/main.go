package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsiter"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

// durableName is shared across sites — each site's consumer lives on its own
// HR-{siteID} stream, so the same durable name never collides.
const durableName = "hr-sync-worker"

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoWriteURI, cfg.MongoWriteUsername, cfg.MongoWritePassword, mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	otelJS, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, otelJS, cfg.SiteIDs, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	handler := NewHandler(newMongoStore(mongoClient.Database(cfg.MongoWriteDB)))

	consumeCtxs := make([]*jsiter.Supervisor, 0, len(cfg.SiteIDs))
	checks := make([]health.Check, 0, len(cfg.SiteIDs)+1)
	checks = append(checks, natsutil.HealthCheck(nc))
	for _, siteID := range cfg.SiteIDs {
		cc, err := startSiteConsumer(ctx, otelJS, handler, siteID, cfg.Consumer)
		if err != nil {
			slog.Error("start site consumer failed", "site", siteID, "error", err)
			os.Exit(1)
		}
		consumeCtxs = append(consumeCtxs, cc)
		checks = append(checks, cc.HealthCheck())
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, false, checks...)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("hr-sync-worker started", "sites", cfg.SiteIDs)

	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error {
			for _, cc := range consumeCtxs {
				cc.Stop()
			}
			return nil
		},
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error {
			mongoutil.Disconnect(ctx, mongoClient)
			return nil
		},
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// startSiteConsumer wires one durable, strictly-sequential consumer on the
// site's HR stream. MaxAckPending=1 so a quit can never overtake the upsert
// that precedes it (low volume — one publish burst per sync run).
func startSiteConsumer(ctx context.Context, js o11ynats.JetStream, handler *Handler, siteID string, s stream.ConsumerSettings) (*jsiter.Supervisor, error) {
	streamCfg := stream.OrgSyncStream(siteID)
	process := func(msgCtx context.Context, msg jetstream.Msg) {
		jobguard.Run(msg, func() {
			handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
			data, err := natsutil.DecodePayload(msg)
			if err != nil {
				// a bad frame won't decode on redelivery → poison
				jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, errcode.Permanent(errcode.BadRequest(fmt.Sprintf("decode payload: %s", err.Error()))))
				return
			}
			jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, handler.HandleMessage(handlerCtx, msg.Subject(), data))
		})
	}

	open := jsiter.ConsumeFrom(jsiter.Resolve(js, streamCfg.Name, buildConsumerConfig(s)), process)
	return jsiter.NewSupervisor(ctx, "hr-sync-"+siteID, open)
}

// buildConsumerConfig adds the durable name and this worker's two overrides;
// everything else comes from ConsumerSettings. MaxDeliver=-1 never drops a feed
// batch (jsretry backoff spaces the retries) and exempts the schedule from the
// len(BackOff)<=MaxDeliver rule; MaxAckPending=1 keeps the lane strictly
// sequential so a quit cannot overtake the upsert before it.
func buildConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	s.MaxDeliver = -1
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durableName
	cc.MaxAckPending = 1
	return cc
}
