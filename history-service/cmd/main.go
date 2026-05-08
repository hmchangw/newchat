package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/publisher"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "history-service")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(cfg.NATS.URL, cfg.NATS.CredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	cassSession, err := cassutil.Connect(
		cfg.Cassandra.Hosts,
		cfg.Cassandra.Keyspace,
		cfg.Cassandra.Username,
		cfg.Cassandra.Password,
	)
	if err != nil {
		slog.Error("cassandra connect failed", "error", err)
		os.Exit(1)
	}

	var keyStore roomkeystore.RoomKeyStore
	if cfg.Encryption.Enabled {
		if cfg.Valkey.Addr == "" {
			slog.Error("encryption enabled but VALKEY_ADDR is empty")
			os.Exit(1)
		}
		keyStore, err = roomkeystore.NewValkeyStore(roomkeystore.Config{
			Addr:        cfg.Valkey.Addr,
			Password:    cfg.Valkey.Password,
			GracePeriod: 0, // history-service never rotates keys; grace period is irrelevant
		})
		if err != nil {
			slog.Error("valkey connect failed", "error", err)
			os.Exit(1)
		}
	}

	cassRepo := cassrepo.NewRepository(cassSession)
	db := mongoClient.Database(cfg.Mongo.DB)
	subRepo := mongorepo.NewSubscriptionRepo(db)
	roomRepo := mongorepo.NewRoomRepo(db)
	threadRoomRepo := mongorepo.NewThreadRoomRepo(db)

	if err := threadRoomRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure thread_rooms indexes failed", "error", err)
		os.Exit(1)
	}

	pub := publisher.New(nc)
	svc := service.New(cassRepo, subRepo, roomRepo, pub, threadRoomRepo, keyStore, cfg.Encryption.Enabled)
	router := natsrouter.New(nc, "history-service")
	router.Use(natsrouter.Recovery())
	router.Use(natsrouter.Logging())

	svc.RegisterHandlers(router, cfg.SiteID)

	slog.Info("history-service running", "site", cfg.SiteID, "encryption", cfg.Encryption.Enabled)

	hooks := []func(context.Context) error{
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
	}
	if keyStore != nil {
		hooks = append(hooks, func(ctx context.Context) error { return keyStore.Close() })
	}
	hooks = append(hooks,
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { cassutil.Close(cassSession); return nil },
	)
	shutdown.Wait(ctx, 25*time.Second, hooks...)
}
