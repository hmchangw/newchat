package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

type config struct {
	NatsURL            string                  `env:"NATS_URL,required"`
	NatsCredsFile      string                  `env:"NATS_CREDS_FILE"      envDefault:""`
	SiteID             string                  `env:"SITE_ID,required"`
	CassandraHosts     string                  `env:"CASSANDRA_HOSTS"      envDefault:"localhost"`
	CassandraKeyspace  string                  `env:"CASSANDRA_KEYSPACE"   envDefault:"chat"`
	CassandraUsername  string                  `env:"CASSANDRA_USERNAME"   envDefault:""`
	CassandraPassword  string                  `env:"CASSANDRA_PASSWORD"   envDefault:""`
	CassandraNumConns  int                     `env:"CASSANDRA_NUM_CONNS"  envDefault:"8"`
	MaxWorkers         int                     `env:"MAX_WORKERS"          envDefault:"100"`
	MessageBucketHours int                     `env:"MESSAGE_BUCKET_HOURS" envDefault:"72"`
	MongoURI           string                  `env:"MONGO_URI,required"`
	MongoDB            string                  `env:"MONGO_DB"             envDefault:"chat"`
	MongoUsername      string                  `env:"MONGO_USERNAME"       envDefault:""`
	MongoPassword      string                  `env:"MONGO_PASSWORD"       envDefault:""`
	UserCacheSize      int                     `env:"USER_CACHE_SIZE"      envDefault:"10000"`
	UserCacheTTL       time.Duration           `env:"USER_CACHE_TTL"       envDefault:"5m"`
	HealthAddr         string                  `env:"HEALTH_ADDR"          envDefault:":8081"`
	PProfEnabled       bool                    `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr        string                  `env:"METRICS_ADDR"         envDefault:":9090"`
	Consumer           stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap          bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Atrest             atrest.Config
	Vault              atrest.VaultConfig
	DebugLog           logctx.Config `envPrefix:"DEBUG_LOG_"`
}

func main() {
	logctx.SetupDefault(os.Stdout)
	pretouchJSON()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	if cfg.MessageBucketHours < 1 {
		slog.Error("invalid config", "MESSAGE_BUCKET_HOURS", cfg.MessageBucketHours)
		os.Exit(1)
	}
	slog.Info("message bucket configured", "hours", cfg.MessageBucketHours)

	bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "message-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}
	meterShutdown, err := otelutil.InitMeter("message-worker")
	if err != nil {
		slog.Error("init meter failed", "error", err)
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

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongodb connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	us, err := userstore.NewCache(userstore.NewMongoStore(db.Collection("users")),
		cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)

	var (
		cipher       atrest.Cipher
		vaultWrapper atrest.KeyWrapperCloser
	)
	if cfg.Atrest.Enabled {
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		if err != nil {
			slog.Error("failed to construct Vault key wrapper", "addr", cfg.Vault.Address, "error", err)
			os.Exit(1)
		}
		vaultWrapper = w
		dekColl := db.Collection(atrest.CollectionName)
		cipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}

	store := NewCassandraStore(cassSession, bucketSizer, cipher)
	threadStore := newThreadStoreMongo(db)
	if err := threadStore.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure thread store indexes failed", "error", err)
		os.Exit(1)
	}
	handler := NewHandler(store, us, threadStore, cfg.SiteID, func(ctx context.Context, subj string, data []byte, msgID string) error {
		// NewMsg re-stamps X-Request-ID and X-Debug from ctx so correlation and
		// verbose-tracing intent ride onto downstream badge/inbox events.
		msg := natsutil.NewMsg(ctx, subj, data)
		if msgID == "" {
			if err := nc.PublishMsg(ctx, msg); err != nil {
				return fmt.Errorf("publish nats message to %s: %w", subj, err)
			}
			return nil
		}
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
			return fmt.Errorf("publish jetstream message to %s with msgID %s: %w", subj, msgID, err)
		}
		return nil
	})

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	canonicalCfg := stream.MessagesCanonical(cfg.SiteID)

	cons, err := js.CreateOrUpdateConsumer(ctx, canonicalCfg.Name, buildConsumerConfig(cfg.Consumer, cfg.SiteID))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(2 * cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	go func() {
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-sem
					wg.Done()
				}()
				// jobguard recovers handler panics — this goroutine runs outside
				// natsrouter's Recovery middleware, so an unrecovered panic would
				// crash the worker and crash-loop on JetStream redelivery.
				jobguard.Run(msg, func() {
					handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
					handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
					logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
					handler.HandleJetStreamMsg(handlerCtx, msg)
				})
			}()
		}
	}()

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	// Bind synchronously so a port conflict fails startup loudly rather than
	// running blind — /metrics exposes the atrest DEK and user-cache counters.
	metricsServer := otelutil.MetricsServer()
	metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("metrics listen failed", "addr", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	slog.Info("message-worker running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			iter.Stop()
			return nil
		},
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("worker drain timed out: %w", ctx.Err())
			}
		},
		// Stop /metrics late so Prometheus can scrape the final drain-window counts,
		// then flush the meter provider before client connections close.
		func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return meterShutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { cassutil.Close(cassSession); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
		func(ctx context.Context) error { return healthStop(ctx) },
	)
}

// buildConsumerConfig restricts the consumer to canonical .created subjects:
// history-service publishes .updated and .deleted to the same stream and
// already wrote Cassandra synchronously for those, so re-processing them here
// would duplicate writes.
func buildConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-worker"
	cc.FilterSubject = subject.MsgCanonicalCreated(siteID)
	return cc
}
