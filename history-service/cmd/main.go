package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/publisher"
	"github.com/hmchangw/chat/history-service/internal/readcache"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
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
	logctx.SetupDefault(os.Stdout)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
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

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	readPref, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.Mongo.ReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithObservability(sdk),
		mongoutil.WithReadPreference(readPref),
		mongoutil.WithMaxPoolSize(cfg.Mongo.MaxPoolSize),
		mongoutil.WithMinPoolSize(cfg.Mongo.MinPoolSize),
	)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())

	cassSession, err := cassutil.Connect(cassutil.Config{
		Hosts:    cfg.Cassandra.Hosts,
		Keyspace: cfg.Cassandra.Keyspace,
		Username: cfg.Cassandra.Username,
		Password: cfg.Cassandra.Password,
		NumConns: cfg.Cassandra.NumConns,
	}, cassutil.WithObservability(sdk))
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
		// DEKs are written by other services; pin to primary so a fresh key isn't
		// missed on a lagging secondary.
		dekColl := mongoClient.Database(cfg.Mongo.DB).Collection(atrest.CollectionName,
			options.Collection().SetReadPreference(readpref.Primary()))
		cipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}

	cassRepo := cassrepo.NewRepository(cassSession, bucketSizer, cfg.MessageReadMaxBuckets, cipher)
	db := mongoClient.Database(cfg.Mongo.DB)
	subRepo := mongorepo.NewSubscriptionRepo(db)
	roomRepo := mongorepo.NewRoomRepo(db)
	threadRoomRepo := mongorepo.NewThreadRoomRepo(db)
	threadSubRepo := mongorepo.NewThreadSubscriptionRepo(db)
	userStore := userstore.NewMongoStore(db.Collection("users"))
	appRepo := mongorepo.NewAppRepo(db)

	if err := threadRoomRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure thread_rooms indexes failed", "error", err)
		os.Exit(1)
	}
	if err := threadSubRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure thread_subscriptions indexes failed", "error", err)
		os.Exit(1)
	}

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

	var opts []service.Option
	if cfg.PreviewCacheSize > 0 && cfg.PreviewCacheTTL > 0 {
		pc, err := readcache.NewPreviewCache(cfg.PreviewCacheSize, cfg.PreviewCacheTTL)
		if err != nil {
			slog.Error("init preview cache failed", "error", err)
			os.Exit(1)
		}
		opts = append(opts, service.WithPreviewCache(pc))
		slog.Info("preview cache enabled", "size", cfg.PreviewCacheSize, "ttl", cfg.PreviewCacheTTL)
	}

	pub := publisher.New(js)
	svc := service.New(cassRepo, subSource, roomSource, pub, threadRoomRepo, threadSubRepo, userStore, appRepo, &cfg, opts...)

	// Bound in-flight handlers so a burst is shed at the door instead of piling
	// unbounded concurrent work onto the (now explicitly capped) Mongo pool.
	routerOpts := []natsrouter.Option{natsrouter.WithSiteID(cfg.SiteID)}
	if cfg.MaxConcurrency > 0 {
		routerOpts = append(routerOpts, natsrouter.WithMaxConcurrency(cfg.MaxConcurrency))
	}
	router := natsrouter.New(nc, "history-service", routerOpts...)
	router.Use(natsrouter.Recovery())
	// RequestID must precede any handler that reads request_id from ctx —
	// otherwise Classify's log line records an empty value.
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Logging())
	// Deadline every request so a slow Mongo/Cassandra op is cancelled and its
	// pooled connection released rather than held until the pool starves.
	if cfg.RequestTimeout > 0 {
		router.Use(natsrouter.HandlerTimeout(cfg.RequestTimeout))
	}

	svc.RegisterHandlers(router, cfg.SiteID)

	slog.Info("connection guards configured",
		"maxPoolSize", cfg.Mongo.MaxPoolSize,
		"minPoolSize", cfg.Mongo.MinPoolSize,
		"maxConcurrency", cfg.MaxConcurrency,
		"requestTimeout", cfg.RequestTimeout,
	)

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		cassutil.HealthCheck(cassSession),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("history-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { cassutil.Close(cassSession); return nil },
		func(ctx context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
