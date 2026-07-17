package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

// durableName is shared across sites — each site's consumer lives on its own
// HR_{siteID} stream, so the same durable name never collides.
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

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
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

	consumeCtxs := make([]o11ynats.ConsumeContext, 0, len(cfg.SiteIDs))
	for _, siteID := range cfg.SiteIDs {
		cc, err := startSiteConsumer(ctx, otelJS, handler, siteID)
		if err != nil {
			slog.Error("start site consumer failed", "site", siteID, "error", err)
			os.Exit(1)
		}
		consumeCtxs = append(consumeCtxs, cc)
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, false,
		natsutil.HealthCheck(nc),
	)
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
		func(ctx context.Context) error {
			mongoutil.Disconnect(ctx, mongoClient)
			nc.Close()
			return nil
		},
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// startSiteConsumer wires one durable, strictly-sequential consumer on the
// site's HR stream. MaxAckPending=1 so a quit can never overtake the upsert
// that precedes it (low volume — one publish burst per sync run).
func startSiteConsumer(ctx context.Context, js o11ynats.JetStream, handler *Handler, siteID string) (o11ynats.ConsumeContext, error) {
	streamCfg := stream.OrgSyncStream(siteID)
	consCfg := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait:    30 * time.Second,
		MaxDeliver: -1, // never drop a feed batch; jsretry backoff spaces the retries
		MaxWaiting: 512,
	})
	consCfg.Durable = durableName
	consCfg.MaxAckPending = 1
	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consCfg)
	if err != nil {
		return nil, err
	}
	return cons.Consume(ctx, func(msgCtx context.Context, msg jetstream.Msg) {
		jobguard.Run(msg, func() {
			handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
			jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, handler.HandleMessage(handlerCtx, msg.Subject(), msg.Data()))
		})
	})
}
