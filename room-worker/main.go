package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL       string          `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string          `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string          `env:"SITE_ID"         envDefault:"site-local"`
	MongoURI      string          `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string          `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string          `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string          `env:"MONGO_PASSWORD"  envDefault:""`
	MaxWorkers    int             `env:"MAX_WORKERS"     envDefault:"100"`
	Bootstrap     bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "room-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	streamCfg := stream.Rooms(cfg.SiteID)

	store := NewMongoStore(mongoClient.Database(cfg.MongoDB))
	handler := NewHandler(store, cfg.SiteID, func(ctx context.Context, subj string, data []byte, msgID string) error {
		if msgID == "" {
			// Ephemeral client-delivery subjects (subscription updates, member
			// events) — core NATS fan-out, not persisted.
			return nc.Publish(ctx, subj, data)
		}
		// JetStream-backed subjects (MESSAGES_CANONICAL, OUTBOX) — block on
		// PubAck so stream-accept failures surface as errors and trigger NAK.
		// Server honors Nats-Msg-Id for stream-level dedup within the window.
		_, err := js.Publish(ctx, subj, data, jetstream.WithMsgID(msgID))
		return err
	})

	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, jetstream.ConsumerConfig{
		Durable: "room-worker", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(2 * cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	go func() {
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				handler.HandleJetStreamMsg(msgCtx, msg)
			}()
		}
	}()

	slog.Info("room-worker running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			iter.Stop()
			return nil
		},
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("worker drain timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
	)
}
