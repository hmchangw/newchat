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

	MessageBucketHours int `env:"MESSAGE_BUCKET_HOURS" envDefault:"72"`

	MaxWorkers int `env:"MAX_WORKERS" envDefault:"100"`
	MaxDeliver int `env:"MAX_DELIVER" envDefault:"5"`

	MongoURI      string `env:"MONGO_URI"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`
	Atrest        atrest.Config
	Vault         atrest.VaultConfig `envPrefix:"VAULT_"`

	HealthAddr   string          `env:"HEALTH_ADDR"   envDefault:":8081"`
	PProfEnabled bool            `env:"PPROF_ENABLED" envDefault:"false"`
	Bootstrap    bootstrapConfig `envPrefix:"BOOTSTRAP_"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("bot-msg-worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
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

	var cipher atrest.Cipher
	var vaultWrapper atrest.KeyWrapperCloser
	if cfg.Atrest.Enabled {
		if cfg.MongoURI == "" {
			return fmt.Errorf("ATREST_ENABLED=true requires MONGO_URI for the DEK collection")
		}
		mc, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
		if err != nil {
			return fmt.Errorf("connect mongo: %w", err)
		}
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
	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, jetstream.ConsumerConfig{
		Durable:       "bot-msg-worker",
		FilterSubject: subject.BotCanonicalCreated(cfg.SiteID),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    cfg.MaxDeliver,
		BackOff:       []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second},
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
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
	)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}

	slog.Info("bot-msg-worker running", "site", cfg.SiteID)
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
		func(_ context.Context) error { return nc.Drain() },
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
