package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	NatsURL                  string          `env:"NATS_URL,required"`
	NatsCredsFile            string          `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID                   string          `env:"SITE_ID"                   envDefault:"site-local"`
	SiteURL                  string          `env:"SITE_URL,required"`
	MongoURI                 string          `env:"MONGO_URI,required"`
	MongoDB                  string          `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername            string          `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword            string          `env:"MONGO_PASSWORD"            envDefault:""`
	MaxRoomSize              int             `env:"MAX_ROOM_SIZE"             envDefault:"1000"`
	MaxBatchSize             int             `env:"MAX_BATCH_SIZE"            envDefault:"1000"`
	MemberListTimeout        time.Duration   `env:"MEMBER_LIST_TIMEOUT"       envDefault:"5s"`
	RoomKeyGracePeriod       time.Duration   `env:"ROOM_KEY_GRACE_PERIOD"     envDefault:"24h"`
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
	TeamsTLSInsecure     bool `env:"TEAMS_TLS_INSECURE" envDefault:"false"`
	RoomMembersLimit     int  `env:"ROOM_MEMBERS_LIMIT"       envDefault:"500"`
	RoomMembersCallLimit int  `env:"ROOM_MEMBERS_CALL_LIMIT"  envDefault:"20"`
	// Atrest/Vault drive eager at-rest DEK provisioning at room creation.
	// When Atrest.Enabled is false the DEK is created lazily by message-worker.
	Atrest   atrest.Config      // env vars already prefixed ATREST_*
	Vault    atrest.VaultConfig // env vars already prefixed (VAULT_*, ATREST_VAULT_*)
	DebugLog logctx.Config      `envPrefix:"DEBUG_LOG_"`
}

func main() {
	logctx.SetupDefault(os.Stdout)

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)
	if cfg.MemberListTimeout <= 0 {
		slog.Error("invalid MEMBER_LIST_TIMEOUT: must be > 0", "value", cfg.MemberListTimeout)
		os.Exit(1)
	}
	if cfg.RestrictedRoomMinMembers <= 0 {
		slog.Error("invalid RESTRICTED_ROOM_MIN_MEMBERS: must be > 0", "value", cfg.RestrictedRoomMinMembers)
		os.Exit(1)
	}

	siteURL, err := url.Parse(cfg.SiteURL)
	if err != nil || siteURL.Scheme == "" || siteURL.Host == "" {
		slog.Error("invalid SITE_URL: must be an absolute URL with scheme and host",
			"value", cfg.SiteURL, "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "room-service")
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

	if cfg.RoomKeyGracePeriod <= 0 {
		slog.Error("ROOM_KEY_GRACE_PERIOD must be a positive duration",
			"room_key_grace_period", cfg.RoomKeyGracePeriod)
		os.Exit(1)
	}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), cfg.RoomKeyGracePeriod)

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	store := NewMongoStore(db)
	// Bounded timeout so a hung createIndexes surfaces at startup.
	ensureCtx, ensureCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := store.EnsureIndexes(ensureCtx); err != nil {
		ensureCancel()
		slog.Error("ensure store indexes failed", "error", err)
		os.Exit(1)
	}
	ensureCancel()

	// Read receipts resolve the target message through history-service (which
	// owns message history) over NATS, so room-service has no direct Cassandra
	// dependency. A history-service outage degrades only read receipts
	// (errcode.Unavailable); core room/membership/subscription operations are
	// all MongoDB-backed and unaffected.
	msgReader := newHistoryMessageReader(nc, cfg.SiteID)

	// Graph client backs the meetings RPC. Constructed only when the Azure app
	// credentials are present; otherwise the meetings RPC reports not-configured
	// while the deep-link RPCs keep working.
	var graphClient msgraph.Client
	if cfg.TeamsTenantID != "" && cfg.TeamsClientID != "" && cfg.TeamsClientSecret != "" {
		if cfg.TeamsTLSInsecure {
			slog.Warn("Graph TLS verification disabled — dev/on-prem only, never production", "TEAMS_TLS_INSECURE", true)
		}
		graphClient = msgraph.New(msgraph.Config{
			TenantID:              cfg.TeamsTenantID,
			ClientID:              cfg.TeamsClientID,
			ClientSecret:          cfg.TeamsClientSecret,
			TLSInsecureSkipVerify: cfg.TeamsTLSInsecure,
		})
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

	memberListClient := NewNATSMemberListClient(nc.NatsConn(), cfg.MemberListTimeout)
	handler := NewHandler(store, keyStore, memberListClient, msgReader, cfg.SiteID, cfg.MaxRoomSize, cfg.MaxBatchSize, cfg.MemberListTimeout, cfg.RestrictedRoomMinMembers,
		func(ctx context.Context, subj string, data []byte, msgID string) error {
			msg := natsutil.NewMsg(ctx, subj, data)
			var opts []jetstream.PublishOpt
			if msgID != "" {
				opts = append(opts, jetstream.WithMsgID(msgID))
			}
			if _, err := js.PublishMsg(ctx, msg, opts...); err != nil {
				return fmt.Errorf("publish to %q: %w", subj, err)
			}
			return nil
		},
		func(ctx context.Context, subj string, data []byte) error {
			if err := nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data)); err != nil {
				return fmt.Errorf("publish core to %q: %w", subj, err)
			}
			return nil
		},
		siteURL,
		nc.NatsConn().MaxPayload(),
	)
	handler.dekProvisioner = dekProvisioner
	handler.graphClient = graphClient
	handler.teamsMeetingStore = store
	handler.teamsEmailDomain = cfg.TeamsEmailDomain
	handler.roomMembersLimit = cfg.RoomMembersLimit
	handler.roomMembersCallLimit = cfg.RoomMembersCallLimit

	router := natsrouter.New(nc, "room-service")
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
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
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
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
		func(ctx context.Context) error { return healthStop(ctx) },
	)
}
