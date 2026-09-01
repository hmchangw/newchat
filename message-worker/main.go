package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	// Mode selects which stream/consumer this pod binds: "default" runs the live
	// .created feed off MESSAGES-CANONICAL; "teams" runs the Teams-migration batch
	// feed off MESSAGES-TEAMS. Two deploys of the same binary, gated by env only.
	Mode               string `env:"MODE"                 envDefault:"default"`
	NatsURL            string `env:"NATS_URL,required"`
	NatsCredsFile      string `env:"NATS_CREDS_FILE"      envDefault:""`
	SiteID             string `env:"SITE_ID,required"`
	CassandraHosts     string `env:"CASSANDRA_HOSTS"      envDefault:"localhost"`
	CassandraKeyspace  string `env:"CASSANDRA_KEYSPACE"   envDefault:"chat"`
	CassandraUsername  string `env:"CASSANDRA_USERNAME"   envDefault:""`
	CassandraPassword  string `env:"CASSANDRA_PASSWORD"   envDefault:""`
	CassandraNumConns  int    `env:"CASSANDRA_NUM_CONNS"  envDefault:"8"`
	MaxWorkers         int    `env:"MAX_WORKERS"          envDefault:"100"`
	MessageBucketHours int    `env:"MESSAGE_BUCKET_HOURS" envDefault:"360"`
	MongoURI           string `env:"MONGO_URI,required"`
	MongoDB            string `env:"MONGO_DB"             envDefault:"chat"`
	MongoUsername      string `env:"MONGO_USERNAME"       envDefault:""`
	MongoPassword      string `env:"MONGO_PASSWORD"       envDefault:""`
	ReadPreference     string `env:"MONGO_READ_PREFERENCE"      envDefault:"primaryPreferred"`
	Pool               mongoutil.PoolConfig
	UserCacheSize      int           `env:"USER_CACHE_SIZE"      envDefault:"10000"`
	UserCacheTTL       time.Duration `env:"USER_CACHE_TTL"       envDefault:"5m"`
	UserL2             userstore.TTLConfig
	HealthAddr         string `env:"HEALTH_ADDR"          envDefault:":8081"`
	PProfEnabled       bool   `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr        string `env:"METRICS_ADDR"         envDefault:":9090"`
	Valkey             valkeyutil.Config
	DEKL2              atrest.TTLConfig
	Breaker            mongoutil.BreakerConfig
	DEKBreaker         atrest.BreakerConfig
	Consumer           stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Buddy              natsutil.BuddyConfig    `envPrefix:"BUDDY_"`
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

	if cfg.Mode != "default" && cfg.Mode != "teams" {
		slog.Error("invalid config", "MODE", cfg.Mode, "reason", `must be "default" or "teams"`)
		os.Exit(1)
	}

	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Breaker.Validate(""); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	if cfg.MessageBucketHours < 1 {
		slog.Error("invalid config", "MESSAGE_BUCKET_HOURS", cfg.MessageBucketHours)
		os.Exit(1)
	}
	slog.Info("message bucket configured", "hours", cfg.MessageBucketHours)

	bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}
	sharedMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics)
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)
	domainMetrics := newPersistenceMetrics(sdk.MeterProvider().Meter("message-worker"))

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

	cassSession, err := cassutil.Connect(cassutil.Config{
		Hosts:    cfg.CassandraHosts,
		Keyspace: cfg.CassandraKeyspace,
		Username: cfg.CassandraUsername,
		Password: cfg.CassandraPassword,
		NumConns: cfg.CassandraNumConns,
	}, cassutil.WithObservability(sdk))
	if err != nil {
		slog.Error("cassandra connect failed", "error", err)
		os.Exit(1)
	}

	// Mongo writes precede the Cassandra write (handler.go:159-201), so an outage
	// aborts before persisting rather than persisting against a stale read.
	readPref, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.ReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongodb connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
	db := mongoClient.Database(cfg.MongoDB)
	// One Valkey client for every L2 tier in this service (at-rest DEK, users).
	// Empty VALKEY_ADDRS disables all of them; each tier falls straight through
	// to Mongo, as before.
	//
	// A connect failure must NOT be fatal. This worker is the sole persister of
	// message history to Cassandra; exiting here would crash-loop the pod over a
	// fail-open cache tier and stop every write — strictly worse than the outage
	// the L2 exists to survive. A nil client is the documented "L2 off" contract
	// (NewL2DEKStore and valkeyutil.Disconnect both accept it).
	valkeyClient := valkeyutil.ConnectOptional(ctx, cfg.Valkey, "DEK and user L2", valkeyutil.Instrumented(sdk))
	if cfg.Valkey.Enabled() {
		slog.Info("valkey L2 tiers configured", "dek_enabled", valkeyClient != nil && cfg.DEKL2.TTL > 0, "dek_ttl", cfg.DEKL2.TTL)
	}

	userBreaker := cfg.Breaker.New(ctx, "user",
		circuitbreaker.WithFailurePredicate(userstore.BreakerFailure))
	us, err := userstore.Resilient(db.Collection("users"), userBreaker,
		valkeyClient, cfg.UserL2.TTL, cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL,
		"l2_enabled", valkeyClient != nil && cfg.UserL2.TTL > 0, "l2_ttl", cfg.UserL2.TTL)

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
		// message-worker is the sole persister, so its DEK breaker opening is the
		// difference between messages being written and being parked. Publish it.
		dekBreaker := cfg.DEKBreaker.New(ctx, "atrestdek")
		dekStore := atrest.NewL2DEKStore(atrest.NewMongoDEKStore(dekColl), valkeyClient,
			cfg.DEKL2.TTL, dekBreaker, atrest.DefaultL2Recorder())
		cipher = atrest.NewCipher(w, dekStore, cfg.Atrest)
	}

	store := NewCassandraStore(cassSession, bucketSizer, cipher)
	threadStore := newThreadStoreMongo(db)
	if err := threadStore.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure thread store indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	handler := NewHandler(store, us, threadStore, cfg.SiteID, func(ctx context.Context, subj string, data []byte, msgID string) error {
		// NewMsg re-stamps X-Request-ID and X-Debug from ctx so correlation and
		// verbose-tracing intent ride onto downstream badge/inbox events.
		msg := natsutil.NewMsg(ctx, subj, data)
		if msgID == "" {
			err := nc.PublishMsg(ctx, msg)
			publishMetrics.Failure(ctx, natsmetrics.DestinationRecipientEvent, natsmetrics.OperationThreadTCount, err)
			if err != nil {
				return fmt.Errorf("publish nats message to %s: %w", subj, err)
			}
			return nil
		}
		_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
		publishMetrics.Failure(ctx, natsmetrics.DestinationOutbox, natsmetrics.OperationRecipientPublish, err)
		if err != nil {
			return fmt.Errorf("publish jetstream message to %s with msgID %s: %w", subj, msgID, err)
		}
		return nil
	}, withPersistenceMetrics(domainMetrics))

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Mode, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	streamName := stream.MessagesCanonical(cfg.SiteID).Name
	if cfg.Mode == "teams" {
		streamName = stream.MessagesTeams(cfg.SiteID).Name
	}

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode, cfg.SiteID)
	consumerMetrics := sharedMetrics.Consumer(natsmetrics.ConsumerConfig{
		Site:   cfg.SiteID,
		Stream: streamName, Consumer: consumerCfg.Durable,
	})
	consumerMetrics.LoopStopped(ctx)
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, consumerCfg)
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}
	consumerMetrics.LoopStarted(ctx)

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	// Built unconditionally in both modes: the consumer filter already scopes each
	// pod to its own mode's subject, so a default-mode pod never sees teamsBatchSubj
	// and this handler just sits unused. Batches are transformed + written straight
	// to Cassandra — never re-published, so broadcast/notification stay silent;
	// search-sync indexes off the same subject on its own MESSAGES-TEAMS consumer.
	// The migration only runs against the central site, so cfg.SiteID here is the
	// central site — the same one HR-sync's own users.upsert publishes to.
	teamsMigration := newTeamsBatchHandler(store, newMongoHRIdentityStore(db), cfg.SiteID,
		func(ctx context.Context, users []model.IUserWithChange) error {
			data, err := json.Marshal(users)
			if err != nil {
				return errcode.MarshalFailed("user identity fanout", err)
			}
			_, err = js.PublishMsg(ctx, natsutil.NewMsg(ctx, subject.OrgSyncUsersUpsert(cfg.SiteID), data))
			publishMetrics.Failure(ctx, natsmetrics.DestinationUserSync, natsmetrics.OperationTeamsUserUpsert, err)
			if err != nil {
				return fmt.Errorf("publish user identity fanout: %w", err)
			}
			return nil
		}, domainMetrics)
	teamsBatchSubj := subject.MsgTeamsCanonicalBatch(cfg.SiteID)
	process := canonicalProcessor(handler, teamsMigration, teamsBatchSubj)

	natsutil.RunPool(iter, sem, &wg, process,
		natsutil.WithLaneMetrics(consumerMetrics, consumerCfg.MaxDeliver))

	// Buddy lane. BindBuddy never fails startup — on any failure buddyLane stays
	// nil and the service runs home-only. Default mode only: teams is a one-time
	// migration path with no standby stream.
	binder := failoverlane.Binder{
		SiteID: cfg.SiteID, Buddy: cfg.Buddy,
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: cfg.MaxWorkers,
		Sem: sem, WG: &wg, Metrics: sharedMetrics,
	}
	var buddyLane *natsutil.Lane
	buddyConn := natsutil.BindBuddy(ctx, cfg.Buddy.OnlyIf(cfg.Mode != "teams"), cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace,
		func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			var bErr error
			buddyLane, bErr = binder.Bind(ctx, bjs, &failoverlane.LaneSpec{
				Stream:   stream.MessagesCanonicalFailover(cfg.SiteID),
				Consumer: buildFailoverConsumerConfig(cfg.Consumer, cfg.SiteID),
			}, process)
			return bErr
		})

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("message-worker running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		// Stop both iterators before draining, so neither lane pulls new work
		// while the other is still finishing. Both feed one WaitGroup.
		func(ctx context.Context) error {
			consumerMetrics.LoopStopped(ctx)
			iter.Stop()
			buddyLane.Stop()
			return nil
		},
		func(ctx context.Context) error {
			return natsutil.WaitPool(ctx, &wg)
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
		func(ctx context.Context) error { cassutil.Close(cassSession); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
		func(_ context.Context) error { valkeyutil.Disconnect(valkeyClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// buildConsumerConfig returns the durable consumer config for the given mode.
// default mode binds only the live .created feed on MESSAGES-CANONICAL (.updated/
// .deleted are excluded — history-service already wrote Cassandra synchronously for
// runLane drains one pull iterator across a bounded worker pool. Shared by the
// home and buddy lanes so both get identical concurrency and panic handling —
// a failover-lane message is still this site's message.

// those, so re-processing would duplicate writes). teams mode binds only the Teams
// migration batch subject on MESSAGES-TEAMS, its own durable.
// buildConsumerConfig applies the outage retry budget: a thread reply whose
// thread-room write cannot reach MongoDB is NAKed, and at the package default
// it would be dropped after ~2.6 minutes — after the gatekeeper already told the
// sender the message was accepted. The longer budget holds it in the stream
// until MongoDB returns.
func buildConsumerConfig(s stream.ConsumerSettings, mode, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(stream.WithOutageRetryBudget(s, jsretry.DefaultBackoff))
	if mode == "teams" {
		cc.Durable = "message-worker-teams"
		cc.FilterSubjects = []string{subject.MsgTeamsCanonicalBatch(siteID)}
		return cc
	}
	cc.Durable = "message-worker"
	cc.FilterSubjects = []string{subject.MsgCanonicalCreated(siteID)}
	return cc
}

// canonicalProcessor returns the consume loop's per-message body: subject
// dispatch plus request-id and log-context stamping, wrapped by jobguard so a
// handler panic Acks instead of crash-looping on JetStream redelivery. Named
// rather than inlined in main so the consumer test drives this exact
// composition, matching broadcast-worker's guardedProcessor.
func canonicalProcessor(h *Handler, teams *teamsBatchHandler, teamsBatchSubj string) func(context.Context, jetstream.Msg) {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		jobguard.Run(msg, func() {
			handlerCtx, _ := logctx.ConsumeContext(msgCtx, msg.Headers(), msg.Subject(), msg.Data())
			// Dispatch by subject: the one-time .teams.batch migration writes
			// straight to Cassandra; the live .created feed runs the normal pipeline.
			if msg.Subject() == teamsBatchSubj {
				teams.consume(handlerCtx, msg)
				return
			}
			h.HandleJetStreamMsg(handlerCtx, msg)
		})
	}
}

// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// MESSAGES-CANONICAL-FAILOVER lane. Distinct durable from the home lane so the
// two keep independent cursors; the handler and the Cassandra writes are
// identical, because a failover-lane message is still this site's message and
// still belongs in this site's keyspace.
//
// Default mode only — teams mode is a one-time migration path with no failover
// lane.
func buildFailoverConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-worker-failover"
	cc.FilterSubjects = []string{subject.FailoverMsgCanonicalCreated(siteID)}
	return cc
}
