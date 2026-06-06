package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/publisher"
	"github.com/hmchangw/chat/history-service/internal/readcache"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/userstore"
)

// checkConfig validates positive-integer config knobs and exits the process on
// the first violation. Kept centralized so future int-bounded settings have one
// place to land.
func checkConfig(cfg *config.Config) {
	checks := []struct {
		name  string
		value int
	}{
		{"MESSAGE_BUCKET_HOURS", cfg.MessageBucketHours},
		{"MESSAGE_READ_MAX_BUCKETS", cfg.MessageReadMaxBuckets},
		{"MESSAGE_HISTORY_FLOOR_DAYS", cfg.MessageHistoryFloorDays},
		{"LARGE_ROOM_THRESHOLD", cfg.LargeRoomThreshold},
		{"MAX_PINNED_PER_ROOM", cfg.MaxPinnedPerRoom},
	}
	for _, c := range checks {
		if c.value < 1 {
			slog.Error("invalid config", c.name, c.value)
			os.Exit(1)
		}
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}

	checkConfig(&cfg)
	slog.Info("message bucket configured",
		"hours", cfg.MessageBucketHours,
		"maxBuckets", cfg.MessageReadMaxBuckets,
		"historyFloorDays", cfg.MessageHistoryFloorDays,
		"largeRoomThreshold", cfg.LargeRoomThreshold,
		"maxPinnedPerRoom", cfg.MaxPinnedPerRoom,
		"pinEnabled", cfg.PinEnabled,
	)

	bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)

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

	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	cassSession, err := cassutil.Connect(cassutil.Config{
		Hosts:    cfg.Cassandra.Hosts,
		Keyspace: cfg.Cassandra.Keyspace,
		Username: cfg.Cassandra.Username,
		Password: cfg.Cassandra.Password,
		NumConns: cfg.Cassandra.NumConns,
	})
	if err != nil {
		slog.Error("cassandra connect failed", "error", err)
		os.Exit(1)
	}

	var (
		cipher       atrest.Cipher
		vaultWrapper atrest.KeyWrapperCloser
	)
	if cfg.Atrest.Enabled {
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		if err != nil {
			slog.Error("failed to construct Vault key wrapper", "addr", cfg.Vault.Address, "error", err)
			os.Exit(1)
		}
		vaultWrapper = w
		dekColl := mongoClient.Database(cfg.Mongo.DB).Collection(atrest.CollectionName)
		cipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}

	cassRepo := cassrepo.NewRepository(cassSession, bucketSizer, cfg.MessageReadMaxBuckets, cipher)
	db := mongoClient.Database(cfg.Mongo.DB)
	subRepo := mongorepo.NewSubscriptionRepo(db)
	roomRepo := mongorepo.NewRoomRepo(db)
	threadRoomRepo := mongorepo.NewThreadRoomRepo(db)
	customEmojiRepo := mongorepo.NewCustomEmojiRepo(db)
	userStore := userstore.NewMongoStore(db.Collection("users"))

	if err := threadRoomRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure thread_rooms indexes failed", "error", err)
		os.Exit(1)
	}
	if err := customEmojiRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure custom_emojis indexes failed", "error", err)
		os.Exit(1)
	}

	cachedEmojis, err := emoji.NewCachedLookup(customEmojiRepo, cfg.CustomEmojiCacheSize, cfg.CustomEmojiCacheTTL)
	if err != nil {
		slog.Error("init custom emoji cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("custom emoji cache configured",
		"size", cfg.CustomEmojiCacheSize,
		"ttl", cfg.CustomEmojiCacheTTL,
	)

	// Front the per-request Mongo reads with process-local LRU+TTL caches.
	var subSource service.SubscriptionRepository = subRepo
	if cfg.SubCacheSize > 0 && cfg.SubCacheTTL > 0 {
		sc, err := readcache.NewSubscriptionCache(subRepo, cfg.SubCacheSize, cfg.SubCacheTTL)
		if err != nil {
			slog.Error("init subscription cache failed", "error", err)
			os.Exit(1)
		}
		subSource = sc
		slog.Info("subscription cache enabled", "size", cfg.SubCacheSize, "ttl", cfg.SubCacheTTL)
	}

	var roomSource service.RoomRepository = roomRepo
	if cfg.RoomCacheSize > 0 && cfg.RoomCacheTTL > 0 {
		rc, err := readcache.NewRoomCache(roomRepo, cfg.RoomCacheSize, cfg.RoomCacheTTL)
		if err != nil {
			slog.Error("init room cache failed", "error", err)
			os.Exit(1)
		}
		roomSource = rc
		slog.Info("room cache enabled", "size", cfg.RoomCacheSize, "ttl", cfg.RoomCacheTTL)
	}

	pub := publisher.New(js)
	svc := service.New(cassRepo, subSource, roomSource, pub, threadRoomRepo, userStore, cachedEmojis, &cfg)
	router := natsrouter.New(nc, "history-service")
	router.Use(natsrouter.Recovery())
	// RequestID must precede any handler that reads request_id from ctx —
	// otherwise Classify's log line records an empty value.
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Logging())

	svc.RegisterHandlers(router, cfg.SiteID)

	slog.Info("history-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { cassutil.Close(cassSession); return nil },
		func(ctx context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
	)
}
