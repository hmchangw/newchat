package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	o11yredis "github.com/flywindy/o11y/redis"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/badgecache"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID        string `env:"SITE_ID"                   envDefault:"site-local"`
	// LEGACY_ROOM_ORIGINS is a JSON array of {siteID, origin} objects mapping
	// each room-origin site to the legacy URL substituted into ${roomOrigin}.
	LegacyRoomOrigins legacyRoomOrigins `env:"LEGACY_ROOM_ORIGINS"`
	MongoURI          string            `env:"MONGO_URI,required"`
	MongoDB           string            `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername     string            `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword     string            `env:"MONGO_PASSWORD"            envDefault:""`
	// MongoReadPreference routes the store's display/list reads to secondaries; the
	// client stays on primary for authz/dedup/read-after-write.
	MongoReadPreference string `env:"MONGO_READ_PREFERENCE" envDefault:"secondaryPreferred"`
	// Pool caps the Mongo connection pool (MONGO_MAX_POOL_SIZE/MONGO_MIN_POOL_SIZE)
	// so a burst can't open unbounded connections. Env tags already carry the
	// MONGO_ prefix, so this stays a top-level field (never under envPrefix:"MONGO_").
	Pool mongoutil.PoolConfig
	// Guard bounds in-flight request handlers (MAX_CONCURRENCY) and per-request
	// duration (REQUEST_TIMEOUT) so a burst or slow dependency can't saturate the
	// Mongo pool with unbounded, indefinitely-held work.
	Guard              natsrouter.GuardConfig
	MaxRoomSize        int           `env:"MAX_ROOM_SIZE"             envDefault:"1000"`
	MaxBatchSize       int           `env:"MAX_BATCH_SIZE"            envDefault:"1000"`
	MemberListTimeout  time.Duration `env:"MEMBER_LIST_TIMEOUT"       envDefault:"5s"`
	RoomKeyGracePeriod time.Duration `env:"ROOM_KEY_GRACE_PERIOD"     envDefault:"24h"`
	// RoomKeyRetiredTTL: retention for rotated-out keys; see roomkeystore.WithRetiredKeys for the 2x-cache-TTL rule.
	RoomKeyRetiredTTL        time.Duration   `env:"ROOM_KEY_RETIRED_TTL"      envDefault:"20m"`
	HealthAddr               string          `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled             bool            `env:"PPROF_ENABLED" envDefault:"false"`
	Bootstrap                bootstrapConfig `envPrefix:"BOOTSTRAP_"`
	RestrictedRoomMinMembers int             `env:"RESTRICTED_ROOM_MIN_MEMBERS" envDefault:"5"`
	// Microsoft Teams integration. Teams* credentials are required only for the
	// meetings RPC (Graph onlineMeeting create); the deep-link RPCs use only
	// EmailDomain. When TenantID/ClientID/ClientSecret are unset the meetings RPC
	// returns errTeamsNotConfigured; the deep-link RPCs still work.
	TeamsTenantID     string `env:"TEAMS_TENANT_ID"          envDefault:""`
	TeamsClientID     string `env:"TEAMS_CLIENT_ID"          envDefault:""`
	TeamsClientSecret string `env:"TEAMS_CLIENT_SECRET"      envDefault:""`
	TeamsEmailDomain  string `env:"TEAMS_EMAIL_DOMAIN"       envDefault:"dev.local"`
	// TeamsTLSInsecure disables Graph TLS verification (dev/on-prem self-signed
	// certs only). Never enable in production.
	TeamsTLSInsecure bool `env:"TEAMS_TLS_INSECURE" envDefault:"false"`
	// GraphUserAgent overrides the User-Agent header on Graph requests (meetings
	// path). Empty falls back to the msgraph browser default. Named GRAPH_USER_AGENT
	// for consistency with user-presence-service.
	GraphUserAgent string `env:"GRAPH_USER_AGENT" envDefault:""`
	// GraphProxyURL, when set, routes the meetings Graph client through this
	// proxy explicitly (overriding HTTPS_PROXY/HTTP_PROXY). Must include a scheme
	// and host, e.g. "http://proxy.corp:8080". Empty falls back to the standard
	// proxy env vars.
	GraphProxyURL string `env:"GRAPH_PROXY_URL" envDefault:""`
	// GraphProxyUsername and GraphProxyPassword authenticate to GRAPH_PROXY_URL
	// with HTTP Basic. Kept separate from the URL so a password carrying URL
	// metacharacters needs no percent-encoding; they override any userinfo
	// embedded in the URL. GRAPH_PROXY_PASSWORD is a secret — never log it.
	// Setting either without GRAPH_PROXY_URL fails at client construction.
	GraphProxyUsername   string `env:"GRAPH_PROXY_USERNAME" envDefault:""`
	GraphProxyPassword   string `env:"GRAPH_PROXY_PASSWORD" envDefault:""`
	RoomMembersLimit     int    `env:"ROOM_MEMBERS_LIMIT"       envDefault:"500"`
	RoomMembersCallLimit int    `env:"ROOM_MEMBERS_CALL_LIMIT"  envDefault:"20"`
	// Atrest/Vault drive eager at-rest DEK provisioning at room creation.
	// When Atrest.Enabled is false the DEK is created lazily by message-worker.
	Atrest   atrest.Config      // env vars already prefixed ATREST_*
	Vault    atrest.VaultConfig // env vars already prefixed (VAULT_*, ATREST_VAULT_*)
	DebugLog logctx.Config      `envPrefix:"DEBUG_LOG_"`
	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`
	// RoomSubjectMode: same-site room .event namespace — global (default) | dual | local. See pkg/subject.RoomRouteMode.
	RoomSubjectMode string `env:"ROOM_SUBJECT_MODE" envDefault:"global"`
	// ValkeyAddrs seeds the Valkey cluster backing the badge cache
	// (pkg/badgecache); empty disables it (clear hooks become no-ops).
	ValkeyAddrs    []string `env:"VALKEY_ADDRS" envDefault:"" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`
	// BadgeCacheTTL bounds how long a badge set survives without a refresh.
	// Keep identical across all badge-cache writers.
	BadgeCacheTTL time.Duration `env:"BADGE_CACHE_TTL" envDefault:"24h"`
	// RoomLocalityGrace: post-flip dual-publish window. Must match across all publisher services.
	RoomLocalityGrace time.Duration `env:"ROOM_LOCALITY_GRACE" envDefault:"168h"`
}

// legacyRoomOrigin maps a site to its legacy origin URL (incl. scheme).
type legacyRoomOrigin struct {
	SiteID string `json:"siteID"`
	Origin string `json:"origin"`
}

// legacyRoomOrigins is parsed from the LEGACY_ROOM_ORIGINS env var — a JSON
// array of {siteID, origin} objects, indexed at parse time into a siteID→URL
// map. Implements encoding.TextUnmarshaler so caarlos0/env populates it
// directly from the env string (same pattern as media-service CLUSTER_DOMAINS).
type legacyRoomOrigins struct {
	byID map[string]string
}

func (l *legacyRoomOrigins) UnmarshalText(text []byte) error {
	var entries []legacyRoomOrigin
	if err := json.Unmarshal(text, &entries); err != nil {
		return fmt.Errorf("parse LEGACY_ROOM_ORIGINS json: %w", err)
	}
	l.byID = make(map[string]string, len(entries))
	for _, e := range entries {
		if _, dup := l.byID[e.SiteID]; dup {
			return fmt.Errorf("parse LEGACY_ROOM_ORIGINS json: duplicate siteID %q", e.SiteID)
		}
		l.byID[e.SiteID] = e.Origin
	}
	return nil
}

func main() {
	logctx.SetupDefault(os.Stdout)

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)
	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid mongo pool config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Guard.Validate(); err != nil {
		slog.Error("invalid guard config", "error", err)
		os.Exit(1)
	}
	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}
	if cfg.MemberListTimeout <= 0 {
		slog.Error("invalid MEMBER_LIST_TIMEOUT: must be > 0", "value", cfg.MemberListTimeout)
		os.Exit(1)
	}
	if cfg.RestrictedRoomMinMembers <= 0 {
		slog.Error("invalid RESTRICTED_ROOM_MIN_MEMBERS: must be > 0", "value", cfg.RestrictedRoomMinMembers)
		os.Exit(1)
	}
	roomRouteMode, err := subject.ParseRoomRouteMode(cfg.RoomSubjectMode)
	if err != nil {
		slog.Error("invalid ROOM_SUBJECT_MODE", "error", err)
		os.Exit(1)
	}
	subject.SetRoomLocalityGrace(cfg.RoomLocalityGrace)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	sharedMetrics := natsmetrics.NewFromProvider(sdk.MeterProvider())
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)
	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)

	keyStore, err := roomkeystore.OpenMongo(ctx, db, cfg.RoomKeyGracePeriod, cfg.RoomKeyRetiredTTL)
	if err != nil {
		slog.Error("open room key store failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	readPref, err := mongoutil.ParseReadPreference(cfg.MongoReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.MongoReadPreference, "error", err)
		os.Exit(1)
	}
	slog.Info("mongo secondary-read preference configured", "readPreference", readPref.Mode().String())

	store := NewMongoStore(db, WithReadPreference(readPref))
	// Bounded timeout so a hung createIndexes surfaces at startup.
	ensureCtx, ensureCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := store.EnsureIndexes(ensureCtx); err != nil {
		slog.Warn("ensure store indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	ensureCancel()

	// Read receipts resolve the target message through history-service (which
	// owns message history) over NATS, so room-service has no direct Cassandra
	// dependency. A history-service outage degrades only read receipts
	// (errcode.Unavailable); core room/membership/subscription operations are
	// all MongoDB-backed and unaffected.
	msgReader := newHistoryMessageReader(nc, cfg.SiteID, withHistoryMetrics(publishMetrics))

	// Graph clients back the meetings RPC. Constructed only when the Azure app
	// credentials are present; otherwise the meetings RPC reports not-configured
	// while the deep-link RPCs keep working. One app-only client serves both the
	// meetings (Client) and directory (DirectoryReader, User.Read.All) surfaces —
	// the directory lookup resolves organizer/attendee object IDs on the same
	// Service Principal that creates the meeting.
	var graphClient msgraph.Client
	var directoryClient msgraph.DirectoryReader
	if cfg.TeamsTenantID != "" && cfg.TeamsClientID != "" && cfg.TeamsClientSecret != "" {
		graphCfg := msgraph.Config{
			TenantID:              cfg.TeamsTenantID,
			ClientID:              cfg.TeamsClientID,
			ClientSecret:          cfg.TeamsClientSecret,
			TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,
			ProxyURL:              cfg.GraphProxyURL,
			ProxyUsername:         cfg.GraphProxyUsername,
			ProxyPassword:         cfg.GraphProxyPassword,
			UserAgent:             cfg.GraphUserAgent,
		}
		if cfg.TeamsTLSInsecure {
			slog.Warn("Graph TLS verification disabled — dev/on-prem only, never production", "TEAMS_TLS_INSECURE", true)
		}
		graphClient, directoryClient, err = msgraph.NewMeetingsDirectoryClient(graphCfg)
		if err != nil {
			slog.Error("build graph meetings client", "error", err)
			os.Exit(1)
		}
	}

	// Eager at-rest DEK provisioning: when enabled, room creation provisions
	// the room's wrapped DEK so the first message write doesn't pay the create
	// cost. message-worker's lazy creation remains the fallback for remote
	// sites (the DEK is per-site) and pre-rollout rooms.
	var vaultWrapper atrest.KeyWrapperCloser
	var dekProvisioner DEKProvisioner
	if cfg.Atrest.Enabled {
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		if err != nil {
			slog.Error("failed to construct Vault key wrapper", "addr", cfg.Vault.Address, "error", err)
			os.Exit(1)
		}
		vaultWrapper = w
		dekColl := mongoClient.Database(cfg.MongoDB).Collection(atrest.CollectionName)
		dekProvisioner = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}

	// Empty VALKEY_ADDRS disables the badge cache — the clear hooks become
	// no-ops (nil-checked in handler.go).
	var badge badgeCache
	var valkeyClient *redis.ClusterClient
	if len(cfg.ValkeyAddrs) > 0 {
		valkeyClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.ValkeyAddrs,
			Password: cfg.ValkeyPassword,
		})
		// o11yredis.Wrap adds tracing+metrics in place, mirroring
		// pkg/valkeyutil's instrumentCluster.
		if _, err := o11yredis.Wrap(valkeyClient, sdk.TracerProvider(), sdk.MeterProvider()); err != nil {
			slog.Error("instrument valkey client failed", "error", err)
			os.Exit(1)
		}
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := valkeyClient.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			slog.Error("valkey connect failed", "error", err)
			os.Exit(1)
		}
		badge = badgecache.New(valkeyClient, cfg.BadgeCacheTTL, badgecache.DefaultMaxCount)
		slog.Info("badge cache enabled", "ttl", cfg.BadgeCacheTTL)
	} else {
		slog.Warn("badge cache DISABLED — VALKEY_ADDRS is empty (dev only)")
	}

	memberListClient := NewNATSMemberListClient(nc.NatsConn(), cfg.MemberListTimeout, withMemberListMetrics(publishMetrics))
	handler := NewHandler(store, keyStore, memberListClient, msgReader, cfg.SiteID, cfg.MaxRoomSize, cfg.MaxBatchSize, cfg.MemberListTimeout, cfg.RestrictedRoomMinMembers,
		func(ctx context.Context, subj string, data []byte, msgID string) error {
			msg := natsutil.NewMsg(ctx, subj, data)
			var opts []jetstream.PublishOpt
			if msgID != "" {
				opts = append(opts, jetstream.WithMsgID(msgID))
			}
			_, err := js.PublishMsg(ctx, msg, opts...)
			destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
			publishMetrics.Attempt(ctx, destination, operation, err)
			if err != nil {
				return fmt.Errorf("publish to %q: %w", subj, err)
			}
			return nil
		},
		func(ctx context.Context, subj string, data []byte) error {
			err := nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
			destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
			publishMetrics.Attempt(ctx, destination, operation, err)
			if err != nil {
				return fmt.Errorf("publish core to %q: %w", subj, err)
			}
			return nil
		},
		cfg.LegacyRoomOrigins.byID,
		nc.NatsConn().MaxPayload(),
		roomRouteMode,
	)
	handler.dekProvisioner = dekProvisioner
	handler.badge = badge
	handler.graphClient = graphClient
	handler.directoryClient = directoryClient
	handler.teamsMeetingStore = store
	handler.teamsEmailDomain = cfg.TeamsEmailDomain
	handler.roomMembersLimit = cfg.RoomMembersLimit
	handler.roomMembersCallLimit = cfg.RoomMembersCallLimit

	router := natsrouter.DefaultGuarded(nc, "room-service", cfg.Guard)
	handler.Register(router)

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("room-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error {
			if closer, ok := keyStore.(interface{ Close() error }); ok {
				return closer.Close()
			}
			return nil
		},
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
		func(ctx context.Context) error {
			if valkeyClient == nil {
				return nil
			}
			return valkeyClient.Close()
		},
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
