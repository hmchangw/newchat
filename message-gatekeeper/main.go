package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL            string          `env:"NATS_URL,required"`
	NatsCredsFile      string          `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID             string          `env:"SITE_ID,required"`
	MongoURI           string          `env:"MONGO_URI,required"`
	MongoDB            string          `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername      string          `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword      string          `env:"MONGO_PASSWORD"  envDefault:""`
	MaxWorkers         int             `env:"MAX_WORKERS"     envDefault:"100"`
	LargeRoomThreshold int             `env:"LARGE_ROOM_THRESHOLD" envDefault:"500"`
	ChatBaseURL        string          `env:"CHAT_BASE_URL"   envDefault:"http://localhost:3000"`
	Bootstrap          bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "message-gatekeeper")
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
	db := mongoClient.Database(cfg.MongoDB)

	store := NewMongoStore(db)
	pub := func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		ack, err := js.PublishMsg(ctx, msg, opts...)
		if err != nil {
			return nil, fmt.Errorf("publish to %q: %w", msg.Subject, err)
		}
		return ack, nil
	}
	reply := func(ctx context.Context, msg *nats.Msg) error {
		if err := nc.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("reply to %q: %w", msg.Subject, err)
		}
		return nil
	}
	parentFetcher := newHistoryParentFetcher(nc, cfg.ChatBaseURL)
	handler := NewHandler(store, pub, reply, cfg.SiteID, parentFetcher, cfg.LargeRoomThreshold)

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	messagesCfg := stream.Messages(cfg.SiteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, messagesCfg.Name, jetstream.ConsumerConfig{
		Durable:   "message-gatekeeper",
		AckPolicy: jetstream.AckExplicitPolicy,
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
				handlerCtx := natsutil.ContextWithRequestIDFromHeaders(msgCtx, msg.Headers())
				handler.HandleJetStreamMsg(handlerCtx, msg)
			}()
		}
	}()

	slog.Info("message-gatekeeper running", "site", cfg.SiteID)

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
