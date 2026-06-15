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

	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	NatsURL            string                  `env:"NATS_URL,required"`
	NatsCredsFile      string                  `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID             string                  `env:"SITE_ID,required"`
	MongoURI           string                  `env:"MONGO_URI,required"`
	MongoDB            string                  `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername      string                  `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword      string                  `env:"MONGO_PASSWORD"  envDefault:""`
	MaxWorkers         int                     `env:"MAX_WORKERS"     envDefault:"100"`
	LargeRoomThreshold int                     `env:"LARGE_ROOM_THRESHOLD" envDefault:"500"`
	ChatBaseURL        string                  `env:"CHAT_BASE_URL"   envDefault:"http://localhost:3000"`
	SubCacheSize       int                     `env:"GATEKEEPER_SUB_CACHE_SIZE"  envDefault:"100000"`
	SubCacheTTL        time.Duration           `env:"GATEKEEPER_SUB_CACHE_TTL"   envDefault:"2m"`
	RoomMetaCacheSize  int                     `env:"ROOM_META_CACHE_SIZE"       envDefault:"10000"`
	RoomMetaCacheTTL   time.Duration           `env:"ROOM_META_CACHE_TTL"        envDefault:"2m"`
	ValkeyAddrs        []string                `env:"VALKEY_ADDRS"               envSeparator:","`
	ValkeyPassword     string                  `env:"VALKEY_PASSWORD"            envDefault:""`
	RoomMetaL2TTL      time.Duration           `env:"ROOM_META_L2_TTL"           envDefault:"15m"`
	UserCacheSize      int                     `env:"USER_CACHE_SIZE"            envDefault:"10000"`
	UserCacheTTL       time.Duration           `env:"USER_CACHE_TTL"             envDefault:"5m"`
	Consumer           stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap          bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	DebugLog           logctx.Config           `envPrefix:"DEBUG_LOG_"`
}

func main() {
	// Wrap the base JSON handler so per-request X-Debug rungs can surface
	// flow/debug/trace edges even though the floor stays at INFO; RenderLevelNames
	// prints the custom FLOW/TRACE levels by name.
	logctx.SetupDefault(os.Stdout)

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

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

	var metaValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		metaValkey, err = valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
		if err != nil {
			slog.Error("valkey connect (room-meta L2) failed", "error", err)
			os.Exit(1)
		}
		slog.Info("room-meta L2 cache enabled", "ttl", cfg.RoomMetaL2TTL)
	}

	mongoStore := NewMongoStore(db, metaValkey, cfg.RoomMetaL2TTL)
	withMeta, err := newCachedMetaStore(mongoStore, cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL)
	if err != nil {
		slog.Error("init room meta cache failed", "error", err)
		os.Exit(1)
	}
	store, err := newCachedSubStore(withMeta, cfg.SubCacheSize, cfg.SubCacheTTL)
	if err != nil {
		slog.Error("init subscription cache failed", "error", err)
		os.Exit(1)
	}
	users, err := userstore.NewCache(userstore.NewMongoStore(db.Collection("users")),
		cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user meta cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("gatekeeper caches enabled",
		"sub_cache_size", cfg.SubCacheSize, "sub_cache_ttl", cfg.SubCacheTTL,
		"room_meta_cache_size", cfg.RoomMetaCacheSize, "room_meta_cache_ttl", cfg.RoomMetaCacheTTL,
		"user_cache_size", cfg.UserCacheSize, "user_cache_ttl", cfg.UserCacheTTL,
	)
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
	handler := NewHandler(store, users, pub, reply, cfg.SiteID, parentFetcher, cfg.LargeRoomThreshold)

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	messagesCfg := stream.Messages(cfg.SiteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, messagesCfg.Name, buildConsumerConfig(cfg.Consumer))
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
				handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
				handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
				logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
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
		func(_ context.Context) error { valkeyutil.Disconnect(metaValkey); return nil },
	)
}

// buildConsumerConfig returns the durable consumer config for
// message-gatekeeper. Centralized so it is unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-gatekeeper"
	return cc
}
