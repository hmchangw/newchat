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
	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
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
	"github.com/hmchangw/chat/pkg/roomtimescache"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
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

// subAuthReadThrough is the shared-L2 subscription access check: a closure that
// runs subauthcache.ReadThrough behind the circuit breaker. account/roomID order
// matches service.SubscriptionRepository.
type subAuthReadThrough func(ctx context.Context, account, roomID string) (sharedSince *time.Time, subscribed bool, err error)

// subL2Source is the always-present subscription base source. The access check
// (GetHistorySharedSince) runs through the shared Valkey L2 read-through, itself
// breaker-guarded, so a Mongo outage fails open regardless of whether the L1
// process-local cache is enabled. The full-subscription read (GetSubscription,
// pin/unpin) delegates to the raw Mongo repo unchanged. This keeps L2/breaker
// outage survival active symmetrically with message-gatekeeper.
type subL2Source struct {
	l2    subAuthReadThrough
	inner service.SubscriptionRepository
}

func (s subL2Source) GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
	return s.l2(ctx, account, roomID)
}

func (s subL2Source) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	return s.inner.GetSubscription(ctx, account, roomID)
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

	// Shared subauthcache L2 (Valkey). nil disables the L2 tier — the base
	// source's read-through then falls straight through to the breaker-guarded
	// Mongo loader (still fronted, regardless of the L1 cache).
	var subValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		// Log and continue rather than exit: both tiers this client feeds (the
		// subscription authz L2 and the at-rest DEK L2) are fail-open caches that
		// degrade to Mongo. Exiting here would turn an optional accelerator into a
		// hard startup dependency and take history reads down with Valkey.
		client, connErr := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
		)
		if connErr != nil {
			slog.Error("valkey connect (subauth L2) failed; L2 caches disabled", "error", connErr)
		} else {
			subValkey = client
		}
		slog.Info("subauth L2 cache configured", "enabled", subValkey != nil && cfg.SubL2TTL > 0, "ttl", cfg.SubL2TTL)
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
		// Front the Mongo DEK store with the shared Valkey L2 so an active room's
		// key stays reachable during a Mongo outage (the in-process DEK cache
		// expires on a fixed TTL and cannot refetch while Mongo is down).
		// subValkey is the client already connected for the subauth L2; a nil
		// client disables the tier. The DEK breaker is deliberately separate from
		// the subscription breaker so the two health signals stay independent.
		dekBreaker := circuitbreaker.New(cfg.DEKBreakerFails, cfg.DEKBreakerCooldown,
			circuitbreaker.Tracked(ctx, "atrestdek"))
		dekStore := atrest.NewL2DEKStore(atrest.NewMongoDEKStore(dekColl), subValkey,
			cfg.DEKL2TTL, dekBreaker, atrest.DefaultL2Recorder())
		cipher = atrest.NewCipher(w, dekStore, cfg.Atrest)
		slog.Info("at-rest DEK L2 configured", "enabled", subValkey != nil && cfg.DEKL2TTL > 0, "ttl", cfg.DEKL2TTL)
		// Preview DEKs live in their own collection (written by broadcast-worker), so
		// they need their own cipher over the same wrapper. Sharing one cipher would
		// also share its DEK cache across two id spaces for no benefit.
		//
		// No L2 tier on this one: the L2 above exists so a Cassandra-backed message
		// read can still open its body while Mongo is down. A stored preview is read
		// FROM Mongo, so its DEK is never needed on a request the outage did not
		// already fail — caching it would front a read that cannot happen.
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

	// Front the per-request Mongo reads with process-local LRU+TTL caches. The
	// subscription L1's loader runs through the shared Valkey L2 (subauthcache),
	// itself breaker-guarded so a Mongo outage fails open instead of stalling.
	// Pinned to primary for the same reason as dekColl above: what this reads is
	// written into a shared 90-minute L2 that every service trusts, so a
	// replica-lagged read does not merely serve one stale answer — it publishes
	// a just-revoked subscription as authorization for the whole TTL, and the
	// outage TTL-slide can extend that further.
	subsPrimary := mongoutil.CollectionWithReadPreference(db.Collection("subscriptions"), readpref.Primary())
	subTier := subauthcache.NewTier(subValkey, subsPrimary, cfg.SubL2TTL,
		circuitbreaker.New(cfg.MongoBreakerFails, cfg.MongoBreakerCooldown,
			circuitbreaker.Tracked(ctx, "subscription")),
		cachemetrics.For("subauth", "l2"))
	// history-service needs only the access-window bound out of the shared
	// projection, so this adapts the tier's SubAuth to that narrower shape.
	subL2 := func(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
		auth, subscribed, err := subTier.Resolve(ctx, roomID, account)
		if err != nil || !subscribed {
			return nil, false, err
		}
		var ss *time.Time
		if auth.HistorySharedSince != nil {
			t := time.UnixMilli(*auth.HistorySharedSince).UTC()
			ss = &t
		}
		return ss, true, nil
	}

	// The breaker-guarded L2 read-through is the ALWAYS-present base source, so
	// outage survival stays active even when the L1 cache is disabled
	// (SubCacheSize/TTL = 0). The L1 cache, when enabled, simply layers on top:
	// its loader calls base.GetHistorySharedSince, which already runs the
	// L2/breaker chain.
	base := subL2Source{l2: subL2, inner: subRepo}
	var subSource service.SubscriptionRepository = base
	if cfg.SubCacheSize > 0 && cfg.SubCacheTTL > 0 {
		sc, err := readcache.NewSubscriptionCache(base, cfg.SubCacheSize, cfg.SubCacheTTL)
		if err != nil {
			slog.Error("init subscription cache failed", "error", err)
			os.Exit(1)
		}
		subSource = sc
		slog.Info("subscription cache enabled",
			"size", cfg.SubCacheSize, "ttl", cfg.SubCacheTTL,
			"sub_l2_ttl", cfg.SubL2TTL,
			"mongo_breaker_fails", cfg.MongoBreakerFails, "mongo_breaker_cooldown", cfg.MongoBreakerCooldown,
		)
	} else {
		slog.Info("subscription L1 cache disabled; L2/breaker outage survival remains active",
			"sub_l2_ttl", cfg.SubL2TTL,
			"mongo_breaker_fails", cfg.MongoBreakerFails, "mongo_breaker_cooldown", cfg.MongoBreakerCooldown,
		)
	}

	// Fence the room reads before the L1 cache wraps them: they fail open, but
	// only once they fail FAST (see breakerRoomRepo). Its own breaker, so
	// room-read health and subscription-read health cannot mask each other.
	guardedRooms := newBreakerRoomRepo(roomRepo, circuitbreaker.New(
		cfg.MongoBreakerFails, cfg.MongoBreakerCooldown,
		circuitbreaker.Tracked(ctx, "roomtimes"),
		circuitbreaker.WithFailurePredicate(roomBreakerFailure)))

	// Room-times L2. Reuses the client already connected for the subauth tier; a
	// nil client or a zero TTL leaves the service's no-op in place, so the bucket
	// walk simply stays as wide as the configured history floor.
	var roomTimes service.RoomTimesCache
	if subValkey != nil && cfg.RoomTimesL2TTL > 0 {
		roomTimes = roomtimescache.NewTier(subValkey, cfg.RoomTimesL2TTL, cachemetrics.For("roomtimes", "l2"))
	}
	slog.Info("room-times L2 configured",
		"enabled", roomTimes != nil, "ttl", cfg.RoomTimesL2TTL)

	// The seeder goes BENEATH the room cache, so only an authoritative read
	// writes the tier. Above it, a cache hit would write Valkey on every history
	// request that has no usable client hint.
	roomReader := service.RoomRepository(guardedRooms)
	if roomTimes != nil {
		roomReader = roomTimesSeeder{RoomRepository: guardedRooms, times: roomTimes}
	}

	roomSource := roomReader
	if cfg.RoomCacheSize > 0 && cfg.RoomCacheTTL > 0 {
		rc, err := readcache.NewRoomCache(roomReader, cfg.RoomCacheSize, cfg.RoomCacheTTL)
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

	// The service reads the tier on the degraded path; the seeder above writes it.
	if roomTimes != nil {
		opts = append(opts, service.WithRoomTimesCache(roomTimes))
	}

	svc := service.New(cassRepo, subSource, roomSource, pub, threadRoomRepo, threadSubRepo, userStore, appRepo, &cfg, opts...)

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
		func(_ context.Context) error { valkeyutil.Disconnect(subValkey); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
