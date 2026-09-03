package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// ESConfig bundles the search backend knobs. BACKEND is the key
// `pkg/searchengine.New` reads to choose between elasticsearch/opensearch.
type ESConfig struct {
	URL           string `env:"URL,required"`
	Backend       string `env:"BACKEND"          envDefault:"elasticsearch"`
	Username      string `env:"USERNAME"         envDefault:""`
	Password      string `env:"PASSWORD"         envDefault:""`
	TLSSkipVerify bool   `env:"TLS_SKIP_VERIFY"  envDefault:"false"`
}

type NATSConfig struct {
	URL       string `env:"URL,required"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

type MongoConfig struct {
	URI      string `env:"URI,required"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
	// ReadPreference: read-only service; secondaryPreferred offloads the primary.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
}

// UsersAPIConfig carries the third-party HR endpoint settings.
// URL is required; Token is optional (TBD when the third-party auth scheme
// is known — see TODO(searchUsers-thirdparty) in users_client.go).
type UsersAPIConfig struct {
	URL     string        `env:"URL,required"`
	Timeout time.Duration `env:"TIMEOUT" envDefault:"5s"`
	Token   string        `env:"TOKEN"   envDefault:""`
}

// SearchConfig groups the request-shape knobs — size caps, cache TTL, and
// the recent-window filter bound. All optional with sane defaults so a
// minimal environment only needs URL + NATS_URL + VALKEY_ADDRS.
type SearchConfig struct {
	DocCounts               int           `env:"DOC_COUNTS"                 envDefault:"25"`
	MaxDocCounts            int           `env:"MAX_DOC_COUNTS"             envDefault:"100"`
	RestrictedRoomsCacheTTL time.Duration `env:"RESTRICTED_ROOMS_CACHE_TTL" envDefault:"5m"`
	RecentWindow            time.Duration `env:"RECENT_WINDOW"              envDefault:"8760h"`
	RequestTimeout          time.Duration `env:"REQUEST_TIMEOUT"            envDefault:"10s"`
	HealthAddr              string        `env:"HEALTH_ADDR"                envDefault:":9090"`
	// HR/App cache knobs size the pod-local L1 caches fronting the enrichment
	// lookups in enrich.go. A non-positive size or TTL disables that cache.
	// The TTL is the worst-case staleness of an HR name or an app name in a
	// search result, and the worst-case delay before a newly-created user or
	// app stops rendering as a bare account name.
	HRCacheSize  int           `env:"HR_CACHE_SIZE"              envDefault:"130000"`
	HRCacheTTL   time.Duration `env:"HR_CACHE_TTL"               envDefault:"24h"`
	AppCacheSize int           `env:"APP_CACHE_SIZE"             envDefault:"1000"`
	AppCacheTTL  time.Duration `env:"APP_CACHE_TTL"              envDefault:"24h"`
}

// Validate rejects a positive-but-tiny cache TTL. expirable.LRU derives its
// reaper tick as ttl/100 and passes it to time.NewTicker, which panics on zero
// in a goroutine nothing recovers — so an operator typo like "50ns" would crash
// the process at startup instead of failing cleanly here. Zero stays legal: it
// is the documented disable switch.
func (c *SearchConfig) Validate() error {
	for _, k := range []struct {
		name string
		ttl  time.Duration
	}{
		{"SEARCH_HR_CACHE_TTL", c.HRCacheTTL},
		{"SEARCH_APP_CACHE_TTL", c.AppCacheTTL},
	} {
		if k.ttl > 0 && k.ttl < minCacheTTL {
			return fmt.Errorf("%s must be 0 (disabled) or at least %s, got %s", k.name, minCacheTTL, k.ttl)
		}
	}
	return nil
}

// Config is the root service config. Note that ES and Search share the
// `SEARCH_` env prefix — the fields on the two structs (URL/BACKEND vs
// DOC_COUNTS/MAX_DOC_COUNTS/RECENT_WINDOW/REQUEST_TIMEOUT/…) don't
// collide today, but any new field added to either must be checked
// against the other or moved to a distinct prefix to avoid silent env
// shadowing.
type Config struct {
	SiteID string   `env:"SITE_ID,required"`
	ES     ESConfig `envPrefix:"SEARCH_"`
	Valkey valkeyutil.Config
	NATS   NATSConfig `envPrefix:"NATS_"`
	// Buddy is the peer cluster hosting this site's standby lanes; the service
	// also answers displaced clients' RPCs there while its own NATS is down.
	Buddy    natsutil.BuddyConfig `envPrefix:"BUDDY_"`
	Search   SearchConfig         `envPrefix:"SEARCH_"`
	Mongo    MongoConfig          `envPrefix:"MONGO_"`
	UsersAPI UsersAPIConfig       `envPrefix:"USERS_API_"`
	DebugLog logctx.Config        `envPrefix:"DEBUG_LOG_"`
	// UNPREFIXED on purpose — must match search-sync-worker / es-index-migrator
	// exactly. On SearchConfig they would pick up envPrefix:"SEARCH_" and drift
	// from the writer silently (wildcard read + allow_no_indices ⇒ empty hits).
	UserRoomIndex     string `env:"USER_ROOM_INDEX,required,notEmpty"`
	SpotlightIndex    string `env:"SPOTLIGHT_INDEX,required,notEmpty"`
	SpotlightOrgIndex string `env:"SPOTLIGHT_ORG_INDEX,required,notEmpty"`
	// ShowTeamsRoom controls whether Teams-migrated rooms/messages (origin
	// "teams") appear in search results; false hides them (reversible read-time
	// filter — see pkg/model.OriginTeams).
	ShowTeamsRoom bool `env:"SHOW_TEAMS_ROOM" envDefault:"false"`
	// ShowTeamsAccounts allowlists accounts that see Teams rooms/messages even when
	// ShowTeamsRoom is false — an ops-managed set, comma-separated.
	ShowTeamsAccounts []string `env:"SHOW_TEAMS_ROOM_ACCOUNTS" envSeparator:","`
	// Pool caps the Mongo connection pool (MONGO_MAX_POOL_SIZE / MONGO_MIN_POOL_SIZE).
	Pool mongoutil.PoolConfig
	// Guard bounds in-flight handlers (MAX_CONCURRENCY) and per-request duration
	// (REQUEST_TIMEOUT) so a burst can't saturate the Mongo pool.
	Guard natsrouter.GuardConfig
}

// teamsAccountSet builds a lookup set from the SHOW_TEAMS_ROOM_ACCOUNTS list, dropping blanks.
func teamsAccountSet(accounts []string) map[string]bool {
	if len(accounts) == 0 {
		return nil
	}
	set := make(map[string]bool, len(accounts))
	for _, a := range accounts {
		if a = strings.TrimSpace(a); a != "" {
			set[a] = true
		}
	}
	return set
}

func main() {
	logctx.SetupDefault(os.Stdout)

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Valkey.Validate(); err != nil {
		slog.Error("invalid valkey config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Guard.Validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Search.Validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	spotlightBase, _, ok := searchindex.StripVersion(cfg.SpotlightIndex)
	if !ok {
		slog.Error("invalid config", "name", "SPOTLIGHT_INDEX", "value", cfg.SpotlightIndex, "reason", "must end with -v<N>, e.g. spotlight-site-a-v1")
		os.Exit(1)
	}
	spotlightReadPattern := fmt.Sprintf("%s-*", spotlightBase)

	spotlightOrgBase, _, ok := searchindex.StripVersion(cfg.SpotlightOrgIndex)
	if !ok {
		slog.Error("invalid config", "name", "SPOTLIGHT_ORG_INDEX", "value", cfg.SpotlightOrgIndex, "reason", "must end with -v<N>, e.g. spotlightorg-site-a-v1")
		os.Exit(1)
	}
	spotlightOrgReadPattern := fmt.Sprintf("%s-*", spotlightOrgBase)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}
	// App metrics are emitted through the SDK meter (exposed on the SDK's
	// Prometheus endpoint alongside runtime/SDK metrics) — no separate listener.
	if err := initMetrics(sdk.Meter("search-service")); err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend:       cfg.ES.Backend,
		URL:           cfg.ES.URL,
		Username:      cfg.ES.Username,
		Password:      cfg.ES.Password,
		TLSSkipVerify: cfg.ES.TLSSkipVerify,
	}, searchengine.WithObservability(sdk))
	if err != nil {
		slog.Error("search engine connect failed", "error", err)
		os.Exit(1)
	}

	valkey, err := valkeyutil.Connect(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	readPref, err := mongoutil.ParseReadPreference(cfg.Mongo.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.Mongo.ReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
	mongoDB := mongoClient.Database(cfg.Mongo.DB)

	usersRC := restyutil.New(
		cfg.UsersAPI.URL,
		restyutil.WithTimeout(cfg.UsersAPI.Timeout),
	)
	usersClient := newHTTPUsersClient(usersRC, cfg.UsersAPI.Token)

	store := newESStore(engine, cfg.UserRoomIndex)
	cache := newValkeyCache(valkey)
	mongoStore := newMongoStore(mongoDB)

	ensureCtx, ensureCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := mongoStore.ensureIndexes(ensureCtx); err != nil {
		slog.Warn("ensure mongo indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	ensureCancel()

	// The cache fronts only the account-keyed enrichment lookups; enrich.go
	// sees the same MongoStore either way. Built once here — expirable.LRU's
	// reaper goroutine lives for the process.
	cachedMongo := newCachedMongoStore(mongoStore, cacheConfig{
		HRSize:  cfg.Search.HRCacheSize,
		HRTTL:   cfg.Search.HRCacheTTL,
		AppSize: cfg.Search.AppCacheSize,
		AppTTL:  cfg.Search.AppCacheTTL,
	})
	slog.Info("enrichment caches configured",
		"enabled", cachedMongo != mongoStore,
		"hr_size", cfg.Search.HRCacheSize, "hr_ttl", cfg.Search.HRCacheTTL,
		"app_size", cfg.Search.AppCacheSize, "app_ttl", cfg.Search.AppCacheTTL)

	handlerCfg := &handlerConfig{
		SiteID:                  cfg.SiteID,
		DocCounts:               cfg.Search.DocCounts,
		MaxDocCounts:            cfg.Search.MaxDocCounts,
		RestrictedRoomsCacheTTL: cfg.Search.RestrictedRoomsCacheTTL,
		RecentWindow:            cfg.Search.RecentWindow,
		RequestTimeout:          cfg.Search.RequestTimeout,
		UserRoomIndex:           cfg.UserRoomIndex,
		SpotlightReadPattern:    spotlightReadPattern,
		SpotlightOrgReadPattern: spotlightOrgReadPattern,
		ShowTeamsRoom:           cfg.ShowTeamsRoom,
		ShowTeamsAccounts:       teamsAccountSet(cfg.ShowTeamsAccounts),
	}

	publishMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics).Publisher(cfg.SiteID)
	// One handler per lane over the same stores and caches; only the room
	// client speaks NATS, and it must go out on the connection the lane's
	// requests arrive on. No home JetStream: this service publishes nothing.
	dialer := natsutil.BuddyDialer{
		Config: cfg.Buddy, CredsFile: cfg.NATS.CredsFile,
		TracerProvider: sdk.TracerProvider(), Propagator: sdk.Propagator, TracingEnabled: sdk.Toggles.Trace,
	}
	routers, err := failoverlane.BindRouters(ctx, nc, nil, &dialer,
		func(_ context.Context, conn *o11ynats.Conn, _ o11ynats.JetStream, _ subject.Lane) (*natsrouter.Router, error) {
			handler := newHandler(store, cachedMongo, usersClient, cache, handlerCfg)
			handler.room = newRoomClient(conn)
			router := natsrouter.New(conn, "search-service",
				append(cfg.Guard.Options(), natsrouter.WithSiteID(cfg.SiteID), natsrouter.WithMetrics(publishMetrics))...)
			router.Use(natsrouter.RequestID())
			router.Use(natsrouter.Recovery())
			router.Use(natsrouter.Logging())
			router.Use(cfg.Guard.TimeoutMiddleware()...)
			handler.Register(router)
			return router, nil
		})
	if err != nil {
		slog.Error("bind routers failed", "error", err)
		os.Exit(1)
	}

	// Health-only listener. All four timeouts guard against hung probes tying
	// up a goroutine indefinitely on an operator-exposed port. App metrics moved
	// to the SDK meter (SDK Prometheus endpoint), so this port serves only
	// /healthz + /readyz now.
	//
	// Bind synchronously so a port conflict fails startup loudly — otherwise
	// ListenAndServe's error would surface in a goroutine and the service would
	// run happily with no health endpoint. Serve(listener) takes ownership of
	// the listener from here on; Shutdown() closes it.
	healthMux := http.NewServeMux()
	health.Register(healthMux, 5*time.Second,
		natsutil.HealthCheck(nc),
	)
	healthServer := &http.Server{
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	healthListener, err := net.Listen("tcp", cfg.Search.HealthAddr)
	if err != nil {
		slog.Error("health server listen failed", "addr", cfg.Search.HealthAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("health server listening", "addr", cfg.Search.HealthAddr)
		if err := healthServer.Serve(healthListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server failed", "error", err)
		}
	}()

	slog.Info("search-service running",
		"site", cfg.SiteID,
		"backend", cfg.ES.Backend,
		"valkey", cfg.Valkey.Addrs,
	)

	// Wait for in-flight handlers BEFORE nc.Drain so they can't touch torn-down deps.
	hooks := append(routers.ShutdownHooks(),
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(_ context.Context) error { valkeyutil.Disconnect(valkey); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthServer.Shutdown(ctx) },
		// obsShutdown last so the SDK Prometheus endpoint can serve the final
		// drain-window observations (incl. app metrics) before it closes.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
	shutdown.Wait(ctx, 25*time.Second, hooks...)
}
