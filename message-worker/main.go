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
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/histdegrade"
	"github.com/hmchangw/chat/pkg/jobguard"
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
	Pool               mongoutil.PoolConfig
	UserCacheSize      int           `env:"USER_CACHE_SIZE"      envDefault:"10000"`
	UserCacheTTL       time.Duration `env:"USER_CACHE_TTL"       envDefault:"5m"`
	HealthAddr         string        `env:"HEALTH_ADDR"          envDefault:":8081"`
	PProfEnabled       bool          `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr        string        `env:"METRICS_ADDR"         envDefault:":9090"`
	DegradeRefresh     time.Duration `env:"DEGRADE_REFRESH_INTERVAL" envDefault:"5s"`
	// MinStreamMaxAge is the MESSAGES-CANONICAL retention this service's retry policy
	// assumes. With MaxDeliver=-1 the stream is the only durability boundary left, and
	// retention is ops/IaC state no code in this repo can see — so the service reads it
	// back at startup and says so when it is short. Advisory: checked, logged and
	// gauged, never fatal. 0 disables the check.
	MinStreamMaxAge time.Duration `env:"MIN_STREAM_MAX_AGE" envDefault:"24h"`
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
	MaxDropsPerMinute uint64                  `env:"MAX_DROPS_PER_MINUTE" envDefault:"10"`
	Consumer          stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap         bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Atrest            atrest.Config
	Vault             atrest.VaultConfig
	DebugLog          logctx.Config `envPrefix:"DEBUG_LOG_"`
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
	sharedMetrics := natsmetrics.NewFromProvider(sdk.MeterProvider())
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

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
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
		slog.Warn("ensure thread store indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	publishFn := func(ctx context.Context, subj string, data []byte, msgID string) error {
		// NewMsg re-stamps X-Request-ID and X-Debug from ctx so correlation and
		// verbose-tracing intent ride onto downstream badge/inbox events.
		msg := natsutil.NewMsg(ctx, subj, data)
		if msgID == "" {
			err := nc.PublishMsg(ctx, msg)
			publishMetrics.Attempt(ctx, natsmetrics.DestinationRecipientEvent, natsmetrics.OperationThreadTCount, err)
			if err != nil {
				return fmt.Errorf("publish nats message to %s: %w", subj, err)
			}
			return nil
		}
		_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
		publishMetrics.Attempt(ctx, natsmetrics.DestinationOutbox, natsmetrics.OperationRecipientPublish, err)
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

	reportStreamRetention(ctx, func(ctx context.Context) (*jetstream.StreamInfo, error) {
		st, err := js.Stream(ctx, streamName)
		if err != nil {
			return nil, fmt.Errorf("open stream %s: %w", streamName, err)
		}
		return st.Info(ctx)
	}, streamName, cfg.MinStreamMaxAge, mtr)

	// The marker is adopted synchronously here, before the consume loop below can
	// process a single replayed row: a pod restarted mid-outage must not spend its
	// first refresh interval acting as though the site were healthy.
	degradeTr, stopDegradeRefresher := startDegradeTracker(ctx, histdegrade.NewStore(db), cfg.SiteID,
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
		}, mtr, cfg.DegradeRefresh)

	handler := NewHandler(store, us, threadStore, cfg.SiteID, publishFn, mtr, degradeTr,
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
				return fmt.Errorf("marshal user identity fanout: %w", err)
			}
			_, err = js.PublishMsg(ctx, natsutil.NewMsg(ctx, subject.OrgSyncUsersUpsert(cfg.SiteID), data))
			publishMetrics.Attempt(ctx, natsmetrics.DestinationUserSync, natsmetrics.OperationTeamsUserUpsert, err)
			if err != nil {
				return fmt.Errorf("publish user identity fanout: %w", err)
			}
			return nil
		}, domainMetrics)
	teamsBatchSubj := subject.MsgTeamsCanonicalBatch(cfg.SiteID)

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
				// jobguard recovers handler panics — this goroutine runs outside
				// natsrouter's Recovery middleware, so an unrecovered panic would
				// crash the worker and crash-loop on JetStream redelivery.
				jobguard.Run(msg, func() {
					handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
					handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
					// Dispatch by subject: the one-time .teams.batch migration
					// writes straight to Cassandra; the live .created feed runs the
					// normal pipeline.
					if msg.Subject() == teamsBatchSubj {
						teamsMigration.consume(handlerCtx, msg)
						return
					}
					logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
					handler.HandleJetStreamMsg(handlerCtx, msg)
				})
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
//
// The two modes take opposite redelivery caps, and neither is left to the env:
// default mode is MaxDeliver=-1 (retry forever — the give-up decision lives in
// settle.go, which drops only a request-class failure that outlived its retry
// window, instead of letting JetStream terminate a message no one persisted), while
// teams mode keeps a finite cap because it settles through plain jsretry.Settle.
func buildConsumerConfig(s stream.ConsumerSettings, mode, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	if mode == "teams" {
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
	cc.Durable = "message-worker"
	cc.FilterSubjects = []string{subject.MsgCanonicalCreated(siteID)}
	cc.MaxDeliver = -1
	return cc
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
