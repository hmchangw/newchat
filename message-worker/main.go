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
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/histdegrade"
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
	Bootstrap          bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Atrest             atrest.Config
	Vault              atrest.VaultConfig
	DebugLog           logctx.Config `envPrefix:"DEBUG_LOG_"`
	DegradeRefresh     time.Duration `env:"DEGRADE_REFRESH_INTERVAL" envDefault:"5s"`
	// InvalidRetryWindow is how long a request-class Cassandra failure (see
	// cqlclass.go) is retried before the message is dropped, measured as
	// accumulated NAK backoff. A duration rather than a delivery count because the
	// question it answers is "will this error resolve on its own?": schema drift
	// resolves when the migration finishes. 1h matches the outage length this
	// service is sized for and sits above any plausible rolling migration.
	InvalidRetryWindow time.Duration `env:"INVALID_RETRY_WINDOW" envDefault:"1h"`
	// HistoryDropEnabled is the brake for a migration that turns every write
	// request-class: false keeps NAKing past the window instead of dropping.
	HistoryDropEnabled bool `env:"HISTORY_DROP_ENABLED" envDefault:"true"`
	// MaxDropsPerMinute caps destruction per pod per minute, the unattended
	// counterpart to HistoryDropEnabled. Genuine poison is a trickle, so 10/min is
	// generous for real bad rows while capping a schema-drift wave at well under 1% of
	// a ~34/s feed: "the whole hour's feed is destroyed" becomes "a few hundred
	// messages are destroyed and the metric is screaming". Per-pod and in-process, so
	// N pods allow N × this in aggregate.
	MaxDropsPerMinute uint64 `env:"MAX_DROPS_PER_MINUTE" envDefault:"10"`
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

	if cfg.InvalidRetryWindow <= 0 {
		slog.Error("invalid config", "INVALID_RETRY_WINDOW", cfg.InvalidRetryWindow.String(),
			"reason", "a non-positive window would drop a request-class failure on its first delivery")
		os.Exit(1)
	}
	if cfg.MaxDropsPerMinute < 1 {
		slog.Error("invalid config", "MAX_DROPS_PER_MINUTE", cfg.MaxDropsPerMinute,
			"reason", "a zero cap would suppress every drop, retrying an unwritable message forever")
		os.Exit(1)
	}
	// The window's effect on redeliveries is logged, not left implied: the count is
	// what an operator sees in consumer state, and deriving it from the backoff
	// schedule by hand is what made the previous count-based knob confusing.
	slog.Info("history drop policy configured",
		"invalid_retry_window", cfg.InvalidRetryWindow.String(),
		"approx_deliveries", deliveriesToDrop(cfg.InvalidRetryWindow),
		"drop_enabled", cfg.HistoryDropEnabled,
		"max_drops_per_minute", cfg.MaxDropsPerMinute)

	// time.NewTicker panics on a non-positive interval, and the refresher builds its
	// ticker inside a goroutine — a bad value would crash the pod after startup
	// looked clean, instead of here.
	if cfg.DegradeRefresh <= 0 {
		slog.Error("invalid config", "DEGRADE_REFRESH_INTERVAL", cfg.DegradeRefresh.String())
		os.Exit(1)
	}

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
	publishFn := func(ctx context.Context, subj string, data []byte, msgID string) error {
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
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Mode, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	streamName := stream.MessagesCanonical(cfg.SiteID).Name
	if cfg.Mode == "teams" {
		streamName = stream.MessagesTeams(cfg.SiteID).Name
	}

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode, cfg.SiteID)
	if err := validateConsumerConfig(&consumerCfg, cfg.Mode); err != nil {
		slog.Error("invalid consumer config", "error", err, "mode", cfg.Mode)
		os.Exit(1)
	}

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

	mtr, err := newMetrics()
	if err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}
	stopLagPoller := startLagPoller(ctx, mtr, func(ctx context.Context) (*jetstream.ConsumerInfo, error) {
		return cons.Info(ctx)
	}, 15*time.Second)

	degradeTr := newDegradeTracker(histdegrade.NewStore(db), cfg.SiteID,
		func(ctx context.Context) (uint64, uint64, error) {
			ci, err := cons.Info(ctx)
			if err != nil {
				return 0, 0, fmt.Errorf("consumer info: %w", err)
			}
			// NumAckPending rides along with NumPending: history has caught up only
			// when nothing is left to deliver AND nothing is still cycling through
			// redelivery.
			// #nosec G115 -- NumAckPending is a queue depth bounded by MaxAckPending; never negative
			return ci.NumPending, uint64(ci.NumAckPending), nil
		}, mtr, nil)
	// tickAfterInterval, not tickOnStart: the marker is read at startup by the
	// first message that needs it, and an immediate tick would race the consumer
	// coming up.
	stopDegradeRefresher := startTicker(ctx, cfg.DegradeRefresh, tickAfterInterval, degradeTr.Refresh)

	// The live handler sees a store that tags its own errors as history failures, so
	// no call site can reopen the loss window by forgetting to wrap. teamsMigration
	// below deliberately gets the bare store: that lane is a separate stream and
	// durable, and a bulk-migration persist failure must not tell every live client
	// on the site that their history is incomplete.
	handler := NewHandler(historyStore{store}, us, threadStore, cfg.SiteID, publishFn, mtr, degradeTr,
		newDropPolicy(cfg.InvalidRetryWindow, cfg.HistoryDropEnabled, cfg.MaxDropsPerMinute, nil),
		withPersistenceMetrics(domainMetrics))

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

	wg.Add(1)
	go func() {
		// The loop itself is counted so shutdown, which stops the iterator and
		// then waits on wg, cannot pass through while a message Next already
		// returned is still on its way to a worker.
		defer wg.Done()
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				consumerMetrics.LoopFailed(context.Background(), err)
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(msgCtx context.Context, msg jetstream.Msg) {
				tracked := consumerMetrics.Track(msgCtx, msg, natsmetrics.EventTypeFromSubject(msg.Subject()), consumerCfg.MaxDeliver)
				msg = tracked
				msgCtx = tracked.Context(msgCtx)
				defer func() {
					tracked.Finish(msgCtx)
					<-sem
					wg.Done()
				}()
				process(msgCtx, msg)
			}(msgCtx, msg)
		}
	}()

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("message-worker running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			consumerMetrics.LoopStopped(ctx)
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
		func(ctx context.Context) error { stopLagPoller(); return nil },
		func(ctx context.Context) error { stopDegradeRefresher(); return nil },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
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

// teamsMaxDeliver is the redelivery cap teams mode falls back to when the
// service-wide CONSUMER_MAX_DELIVER is the default mode's -1 (both modes are the
// same binary and share one env block). Matches stream.ConsumerSettings' own
// default, so a teams pod keeps the plain finite-cap behavior.
const teamsMaxDeliver = 5

// buildConsumerConfig returns the durable consumer config for the given mode.
// default mode binds only the live .created feed on MESSAGES-CANONICAL (.updated/
// .deleted are excluded — history-service already wrote Cassandra synchronously for
// those, so re-processing would duplicate writes). teams mode binds only the Teams
// migration batch subject on MESSAGES-TEAMS, its own durable.
// The two modes take opposite redelivery caps, and neither is left to the env.
//
// Default mode uses stream.WithUnlimitedRedelivery: the give-up decision lives in
// settle.go, which drops only a request-class failure that outlived its retry
// window, rather than letting JetStream terminate a message no one persisted.
// Applied to the settings — not to the built config — because MaxDeliver has to be
// unlimited before backOffSchedule derives the steps; mutating cc.MaxDeliver
// afterwards leaves the clamp already fired against a cap that no longer applies.
//
// Teams mode keeps a finite cap: it settles through plain jsretry.Settle with no
// give-up path of its own, so an unbounded NAK there would never retire anything.
// It takes the outage retry budget so a thread reply whose thread-room write cannot
// reach MongoDB is held in the stream until MongoDB returns, rather than dropped
// after ~2.6 minutes — after the gatekeeper already told the sender it was accepted.
func buildConsumerConfig(s stream.ConsumerSettings, mode, siteID string) jetstream.ConsumerConfig {
	if mode == "teams" {
		cc := stream.DurableConsumerDefaults(stream.WithOutageRetryBudget(s, jsretry.DefaultBackoff))
		cc.Durable = "message-worker-teams"
		cc.FilterSubjects = []string{subject.MsgTeamsCanonicalBatch(siteID)}
		if cc.MaxDeliver <= 0 {
			// -1 (or jetstream's 0 "unset") would turn the batch handler's Nak into
			// an infinite retry with nothing to retire it. 0 is folded in here
			// because jetstream reads it as "no cap", not as "cap of zero".
			cc.MaxDeliver = teamsMaxDeliver
		}
		return cc
	}
	cc := stream.DurableConsumerDefaults(stream.WithUnlimitedRedelivery(s))
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

// validateConsumerConfig rejects a consumer whose redelivery cap contradicts its
// mode's give-up policy. It re-checks in main what buildConsumerConfig just set, so
// a future edit that lets CONSUMER_MAX_DELIVER back through fails at startup rather
// than silently restoring the loss boundary this service exists to remove:
// JetStream terminating a message that broadcast-worker already delivered and
// search-sync-worker already indexed, while the marker reports history complete.
func validateConsumerConfig(cc *jetstream.ConsumerConfig, mode string) error {
	if mode == "teams" {
		if cc.MaxDeliver <= 0 {
			return fmt.Errorf("teams mode needs a finite MaxDeliver (has %d): an infinite NAK would never give up", cc.MaxDeliver)
		}
		return nil
	}
	if cc.MaxDeliver >= 0 {
		return fmt.Errorf("default mode needs MaxDeliver=-1 (has %d): a finite cap terminates messages behind settle.go's give-up decision", cc.MaxDeliver)
	}
	return nil
}
