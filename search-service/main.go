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

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
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
	// UNPREFIXED on purpose — must match search-sync-worker / es-index-migrator
	// exactly. On SearchConfig they would pick up envPrefix:"SEARCH_" and drift
	// from the writer silently (wildcard read + allow_no_indices ⇒ empty hits).
	UserRoomIndex     string `env:"USER_ROOM_INDEX,required,notEmpty"`
	SpotlightIndex    string `env:"SPOTLIGHT_INDEX,required,notEmpty"`
	SpotlightOrgIndex string `env:"SPOTLIGHT_ORG_INDEX,required,notEmpty"`
	// MaxConcurrency caps in-flight request handlers so a burst is shed at the
	// door (ErrUnavailable) instead of piling unbounded work onto Elasticsearch/
	// MongoDB. 0 disables the cap (unbounded spawn).
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"256"`
	// ShowTeamsRoom controls whether Teams-migrated rooms/messages (origin
	// "teams") appear in search results; false hides them (reversible read-time
	// filter — see pkg/model.OriginTeams).
	ShowTeamsRoom bool `env:"SHOW_TEAMS_ROOM" envDefault:"false"`
	// ShowTeamsAccounts allowlists accounts that see Teams rooms/messages even when
	// ShowTeamsRoom is false — an ops-managed set, comma-separated.
	ShowTeamsAccounts []string `env:"SHOW_TEAMS_ROOM_ACCOUNTS" envSeparator:","`
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

	valkey, err := valkeyutil.ConnectCluster(ctx, cfg.Valkey.Addrs, cfg.Valkey.Password,
		valkeyutil.WithObservability(sdk),
		valkeyutil.WithRequireParentSpan(true),
	)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(ctx, cfg.NATS.URL, cfg.NATS.CredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
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
		mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref), mongoutil.WithLazyConnect())
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
	mongoutil.EnsureIndexesBestEffort(ensureCtx, "search-service store", mongoStore.ensureIndexes)
	ensureCancel()
	handler := newHandler(store, mongoStore, usersClient, cache, &handlerConfig{
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
	})
	handler.room = newRoomClient(nc)

	// Bound in-flight handlers so a burst is shed at the door (ErrUnavailable)
	// instead of piling unbounded work onto Elasticsearch/MongoDB.
	// MAX_CONCURRENCY=0 disables.
	routerOpts := []natsrouter.Option{natsrouter.WithSiteID(cfg.SiteID)}
	if cfg.MaxConcurrency > 0 {
		routerOpts = append(routerOpts, natsrouter.WithMaxConcurrency(cfg.MaxConcurrency))
	}
	router := natsrouter.New(nc, "search-service", routerOpts...)
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Recovery())
	router.Use(natsrouter.Logging())
	handler.Register(router)

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

	shutdown.Wait(ctx, 25*time.Second,
		// Wait for in-flight handlers BEFORE nc.Drain so they can't touch torn-down deps.
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(_ context.Context) error { valkeyutil.Disconnect(valkey); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthServer.Shutdown(ctx) },
		// obsShutdown last so the SDK Prometheus endpoint can serve the final
		// drain-window observations (incl. app metrics) before it closes.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
