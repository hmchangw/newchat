package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jsiter"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`
	SiteID        string `env:"SITE_ID,required"`

	CassandraHosts    string `env:"CASSANDRA_HOSTS,required"`
	CassandraKeyspace string `env:"CASSANDRA_KEYSPACE,required"`
	CassandraUsername string `env:"CASSANDRA_USERNAME"`
	CassandraPassword string `env:"CASSANDRA_PASSWORD"`
	CassandraNumConns int    `env:"CASSANDRA_NUM_CONNS" envDefault:"4"`

	MessageBucketHours int `env:"MESSAGE_BUCKET_HOURS" envDefault:"360"`

	MaxWorkers int                     `env:"MAX_WORKERS" envDefault:"100"`
	Consumer   stream.ConsumerSettings `envPrefix:"CONSUMER_"`

	MongoURI      string `env:"MONGO_URI"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`
	// ReadPreference: the at-rest DEK collection is this service's only Mongo read.
	// primaryPreferred, because a stale DEK read cannot diverge ($setOnInsert plus a
	// re-read comparison) but a primary-only one blocks encryption outright.
	ReadPreference string `env:"MONGO_READ_PREFERENCE" envDefault:"primaryPreferred"`
	Pool           mongoutil.PoolConfig
	Atrest         atrest.Config
	Vault          atrest.VaultConfig `envPrefix:"VAULT_"`

	HealthAddr   string          `env:"HEALTH_ADDR"   envDefault:":8081"`
	PProfEnabled bool            `env:"PPROF_ENABLED" envDefault:"false"`
	Bootstrap    bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("bot-message-worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Pool.Validate(); err != nil {
		return fmt.Errorf("validate mongo pool config: %w", err)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("init jetstream: %w", err)
	}

	cassSess, err := cassutil.Connect(cassutil.Config{
		Hosts:    cfg.CassandraHosts,
		Keyspace: cfg.CassandraKeyspace,
		Username: cfg.CassandraUsername,
		Password: cfg.CassandraPassword,
		NumConns: cfg.CassandraNumConns,
	}, cassutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect cassandra: %w", err)
	}

	bucket := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)

	// Validated unconditionally: parsing only inside the ATREST branch would let a
	// bad value lie dormant and surface as a crash-loop at a later flag flip.
	readPref, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	if err != nil {
		return fmt.Errorf("parse mongo read preference: %w", err)
	}

	var cipher atrest.Cipher
	var vaultWrapper atrest.KeyWrapperCloser
	if cfg.Atrest.Enabled {
		if cfg.MongoURI == "" {
			return fmt.Errorf("ATREST_ENABLED=true requires MONGO_URI for the DEK collection")
		}
		mc, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
			mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk),
			mongoutil.WithReadPreference(readPref))
		if err != nil {
			return fmt.Errorf("connect mongo: %w", err)
		}
		slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
		defer mongoutil.Disconnect(ctx, mc)
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		if err != nil {
			return fmt.Errorf("vault wrapper: %w", err)
		}
		vaultWrapper = w
		dekColl := mc.Database(cfg.MongoDB).Collection(atrest.CollectionName)
		cipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		return fmt.Errorf("bootstrap streams: %w", err)
	}

	store := NewCassandraStore(cassSess, bucket, cipher)
	h := newHandler(store, cfg.SiteID)

	streamCfg := stream.BotMessagesCanonical(cfg.SiteID)
	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.SiteID)
	open := jsiter.PullFrom(jsiter.Resolve(js, streamCfg.Name, consumerCfg), jetstream.PullMaxMessages(2*cfg.MaxWorkers))

	iter, err := jsiter.NewPump(ctx, consumerCfg.Durable, open)
	if err != nil {
		return fmt.Errorf("messages iter: %w", err)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup
	go func() {
		for {
			mCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				h.HandleJetStreamMsg(mCtx, msg)
			}()
		}
	}()

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		iter.HealthCheck(),
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("bot-message-worker running", "site", cfg.SiteID)
	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error { iter.Stop(); return nil },
		func(dctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-dctx.Done():
				return fmt.Errorf("worker drain: %w", dctx.Err())
			}
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(_ context.Context) error { cassSess.Close(); return nil },
		func(_ context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
		func(dctx context.Context) error { return healthStop(dctx) },
		func(dctx context.Context) error { return obsShutdown(dctx) },
	)
	return nil
}

// buildConsumerConfig adds the durable name and filter; everything else comes
// from ConsumerSettings.
func buildConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "bot-message-worker"
	cc.FilterSubject = subject.BotCanonicalCreated(siteID)
	return cc
}
