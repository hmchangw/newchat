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
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/pagefit"
	"github.com/hmchangw/chat/pkg/preview"
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

	sharedMetrics := natsmetrics.NewFromProvider(sdk.MeterProvider())
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)
	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
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
		mongoutil.WithPool(cfg.Pool),
		mongoutil.WithObservability(sdk),
		mongoutil.WithReadPreference(readPref),
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
		cipher        atrest.Cipher
		previewCipher atrest.Cipher
		vaultWrapper  atrest.KeyWrapperCloser
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
		// Preview DEKs live in their own collection (written by broadcast-worker), so
		// they need their own cipher over the same wrapper. Sharing one cipher would
		// also share its DEK cache across two id spaces for no benefit.
		previewDEKColl := mongoClient.Database(cfg.Mongo.DB).Collection(preview.DEKCollection,
			options.Collection().SetReadPreference(readpref.Primary()))
		previewCipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(previewDEKColl), cfg.Atrest)
	}

	cassRepo := cassrepo.NewRepository(cassSession, bucketSizer, cfg.MessageReadMaxBuckets, cipher)
	db := mongoClient.Database(cfg.Mongo.DB)
	subRepo := mongorepo.NewSubscriptionRepo(db)
	roomRepo := mongorepo.NewRoomRepo(db, previewCipher, preview.Key{SiteID: cfg.SiteID, Epoch: cfg.PreviewKeyEpoch})
	threadRoomRepo := mongorepo.NewThreadRoomRepo(db)
	threadSubRepo := mongorepo.NewThreadSubscriptionRepo(db)
	userStore := userstore.NewMongoStore(db.Collection("users"))
	appRepo := mongorepo.NewAppRepo(db)

	if err := threadRoomRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure thread_rooms indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	if err := threadSubRepo.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure thread_subscriptions indexes failed; continuing (indexes are best-effort)", "error", err)
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

	// Collapses the repeated account lookups a scroll-back makes while resolving
	// legacy members_removed rows. Sized and expired like the other services
	// fronting this store — see the config field for why the TTL is not longer.
	var userSource service.UserStore = userStore
	if cfg.UserCacheSize > 0 && cfg.UserCacheTTL > 0 {
		uc, err := userstore.NewCache(userStore, cfg.UserCacheSize, cfg.UserCacheTTL)
		if err != nil {
			slog.Error("init user cache failed", "error", err)
			os.Exit(1)
		}
		userSource = uc
		slog.Info("user cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)
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

	// Fronts rooms.get's lazy fallback only: a room served from a stored preview
	// never reaches the walk this caches.
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

	pub := publisher.New(js, publisher.WithMetrics(publishMetrics))
	// A zero Budget disables trimming, so the toggle needs no handler branch.
	pageBudget := pagefit.Budget{}
	if cfg.PageTrimming {
		pageBudget = pagefit.Resolve(cfg.MaxResponseBytes, nc.NatsConn().MaxPayload(), pagefit.DefaultReserve)
	} else {
		slog.Warn("page trimming DISABLED — oversize replies fail with response_too_large")
	}
	opts = append(opts, service.WithPageBudget(pageBudget))
	svc := service.New(cassRepo, subSource, roomSource, pub, threadRoomRepo, threadSubRepo, userSource, appRepo, &cfg, opts...)

	// Default middleware chain (Recovery, RequestID, Logging) plus this service's
	// per-site + metrics router options and the guard's admission cap; the
	// per-request timeout (free a connection stuck on a slow op) is applied after.
	routerOpts := append([]natsrouter.Option{
		natsrouter.WithSiteID(cfg.SiteID),
		natsrouter.WithMetrics(publishMetrics),
	}, cfg.Guard.Options()...)
	router := natsrouter.Default(nc, "history-service", routerOpts...)
	router.Use(cfg.Guard.TimeoutMiddleware()...)

	svc.RegisterHandlers(router, cfg.SiteID)

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
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
