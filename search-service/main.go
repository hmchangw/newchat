package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/restyutil"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/shutdown"
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

type ValkeyConfig struct {
	Addrs    []string `env:"ADDRS,required" envSeparator:","`
	Password string   `env:"PASSWORD"        envDefault:""`
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
	UserRoomIndex           string        `env:"USER_ROOM_INDEX,required"`
	SpotlightIndex          string        `env:"SPOTLIGHT_INDEX,required"`
	MetricsAddr             string        `env:"METRICS_ADDR"               envDefault:":9090"`
}

// Config is the root service config. Note that ES and Search share the
// `SEARCH_` env prefix — the fields on the two structs (URL/BACKEND vs
// DOC_COUNTS/MAX_DOC_COUNTS/RECENT_WINDOW/REQUEST_TIMEOUT/…) don't
// collide today, but any new field added to either must be checked
// against the other or moved to a distinct prefix to avoid silent env
// shadowing.
type Config struct {
	SiteID   string         `env:"SITE_ID,required"`
	ES       ESConfig       `envPrefix:"SEARCH_"`
	Valkey   ValkeyConfig   `envPrefix:"VALKEY_"`
	NATS     NATSConfig     `envPrefix:"NATS_"`
	Search   SearchConfig   `envPrefix:"SEARCH_"`
	Mongo    MongoConfig    `envPrefix:"MONGO_"`
	UsersAPI UsersAPIConfig `envPrefix:"USERS_API_"`
	DebugLog logctx.Config  `envPrefix:"DEBUG_LOG_"`
}

func main() {
	logctx.SetupDefault(os.Stdout)

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	spotlightBase, _, ok := searchindex.StripVersion(cfg.Search.SpotlightIndex)
	if !ok {
		slog.Error("invalid config", "name", "SEARCH_SPOTLIGHT_INDEX", "value", cfg.Search.SpotlightIndex, "reason", "must end with -v<N>, e.g. spotlight-site-a-v1")
		os.Exit(1)
	}
	spotlightReadPattern := fmt.Sprintf("%s-*", spotlightBase)

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "search-service")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend:       cfg.ES.Backend,
		URL:           cfg.ES.URL,
		Username:      cfg.ES.Username,
		Password:      cfg.ES.Password,
		TLSSkipVerify: cfg.ES.TLSSkipVerify,
	})
	if err != nil {
		slog.Error("search engine connect failed", "error", err)
		os.Exit(1)
	}

	valkey, err := valkeyutil.ConnectCluster(ctx, cfg.Valkey.Addrs, cfg.Valkey.Password)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
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
	mongoDB := mongoClient.Database(cfg.Mongo.DB)

	usersRC := restyutil.New(
		cfg.UsersAPI.URL,
		restyutil.WithTimeout(cfg.UsersAPI.Timeout),
	)
	usersClient := newHTTPUsersClient(usersRC, cfg.UsersAPI.Token)

	store := newESStore(engine, cfg.Search.UserRoomIndex)
	cache := newValkeyCache(valkey)
	mongoStore := newMongoStore(mongoDB)

	ensureCtx, ensureCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := mongoStore.ensureIndexes(ensureCtx); err != nil {
		ensureCancel()
		slog.Error("ensure mongo indexes failed", "error", err)
		os.Exit(1)
	}
	ensureCancel()
	handler := newHandler(store, mongoStore, usersClient, cache, &handlerConfig{
		SiteID:                  cfg.SiteID,
		DocCounts:               cfg.Search.DocCounts,
		MaxDocCounts:            cfg.Search.MaxDocCounts,
		RestrictedRoomsCacheTTL: cfg.Search.RestrictedRoomsCacheTTL,
		RecentWindow:            cfg.Search.RecentWindow,
		RequestTimeout:          cfg.Search.RequestTimeout,
		UserRoomIndex:           cfg.Search.UserRoomIndex,
		SpotlightReadPattern:    spotlightReadPattern,
	})

	router := natsrouter.New(nc, "search-service")
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Recovery())
	router.Use(natsrouter.Logging())
	handler.Register(router)

	// /metrics-only listener. All four timeouts guard against hung
	// scrapers tying up a goroutine indefinitely on an operator-exposed
	// port.
	//
	// Bind synchronously so a port conflict fails startup loudly —
	// otherwise ListenAndServe's error would surface in a goroutine and
	// the service would run happily with no /metrics, silently losing
	// observability. Serve(listener) takes ownership of the listener
	// from here on; Shutdown() closes it.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metricsHandler())
	health.Register(metricsMux, 5*time.Second,
		natsutil.HealthCheck(nc),
	)
	metricsServer := &http.Server{
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	metricsListener, err := net.Listen("tcp", cfg.Search.MetricsAddr)
	if err != nil {
		slog.Error("metrics server listen failed", "addr", cfg.Search.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics server listening", "addr", cfg.Search.MetricsAddr)
		if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	slog.Info("search-service running",
		"site", cfg.SiteID,
		"backend", cfg.ES.Backend,
		"valkey", cfg.Valkey.Addrs,
	)

	shutdown.Wait(ctx, 25*time.Second,
		// Wait for in-flight handlers BEFORE nc.Drain so they can't touch torn-down deps.
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(_ context.Context) error { valkeyutil.Disconnect(valkey); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		// /metrics last so Prometheus can scrape the final drain-window observations.
		func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
	)
}
