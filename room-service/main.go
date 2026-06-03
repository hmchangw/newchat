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
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
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
	ValkeyAddrs              []string        `env:"VALKEY_ADDRS,required"     envSeparator:","`
	ValkeyPassword           string          `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyGracePeriod        time.Duration   `env:"VALKEY_KEY_GRACE_PERIOD,required"`
	CassandraHosts           string          `env:"CASSANDRA_HOSTS,required"`
	CassandraKeyspace        string          `env:"CASSANDRA_KEYSPACE"        envDefault:"chat"`
	CassandraUsername        string          `env:"CASSANDRA_USERNAME"        envDefault:""`
	CassandraPassword        string          `env:"CASSANDRA_PASSWORD"        envDefault:""`
	CassandraNumConns        int             `env:"CASSANDRA_NUM_CONNS"       envDefault:"8"`
	Bootstrap                bootstrapConfig `envPrefix:"BOOTSTRAP_"`
	RestrictedRoomMinMembers int             `env:"RESTRICTED_ROOM_MIN_MEMBERS" envDefault:"5"`
	// Atrest/Vault drive eager at-rest DEK provisioning at room creation.
	// When Atrest.Enabled is false the DEK is created lazily by message-worker.
	Atrest atrest.Config      // env vars already prefixed ATREST_*
	Vault  atrest.VaultConfig // env vars already prefixed (VAULT_*, ATREST_VAULT_*)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
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

	keyStore, err := roomkeystore.NewValkeyClusterStore(roomkeystore.ClusterConfig{
		Addrs:       cfg.ValkeyAddrs,
		Password:    cfg.ValkeyPassword,
		GracePeriod: cfg.ValkeyGracePeriod,
	})
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

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

	cassSession, err := cassutil.Connect(cassutil.Config{
		Hosts:    cfg.CassandraHosts,
		Keyspace: cfg.CassandraKeyspace,
		Username: cfg.CassandraUsername,
		Password: cfg.CassandraPassword,
		NumConns: cfg.CassandraNumConns,
	})
	if err != nil {
		slog.Error("cassandra connect failed", "error", err)
		os.Exit(1)
	}
	cassReader := NewCassMessageReader(cassSession)

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
	handler := NewHandler(store, keyStore, memberListClient, cassReader, cfg.SiteID, cfg.MaxRoomSize, cfg.MaxBatchSize, cfg.MemberListTimeout, cfg.RestrictedRoomMinMembers,
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

	if err := handler.RegisterCRUD(nc); err != nil {
		slog.Error("register CRUD handlers failed", "error", err)
		os.Exit(1)
	}

	slog.Info("room-service running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error {
			if closer, ok := keyStore.(interface{ Close() error }); ok {
				return closer.Close()
			}
			return nil
		},
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { cassutil.Close(cassSession); return nil },
		func(context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
	)
}
