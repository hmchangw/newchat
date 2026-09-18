package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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
	"github.com/hmchangw/chat/pkg/threadcount"
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
	Thread             threadcount.Policy
	Consumer           stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Buddy              natsutil.BuddyConfig    `envPrefix:"BUDDY_"`
	Bootstrap          bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Atrest             atrest.Config
	Vault              atrest.VaultConfig
	DebugLog           logctx.Config `envPrefix:"DEBUG_LOG_"`
	DegradeRefresh     time.Duration `env:"DEGRADE_REFRESH_INTERVAL" envDefault:"5s"`
	// DegradeMarkDelay is how long history writes must keep failing before the site
	// is marked degraded. The marker is site-wide and is held for drainTailGrace (20
	// minutes) past the drain, so marking on the first failed write turned any
	// one-second blip the retry resolves into a 20-minute site-wide "history
	// incomplete" notice for every room. 30s clears the first three backoff rungs
	// (1s, 5s, 30s), so a blip that resolves on retry never marks while a real outage
	// still marks within about half a minute. 0 marks immediately.
	DegradeMarkDelay time.Duration `env:"DEGRADE_MARK_DELAY" envDefault:"30s"`
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
	if err := cfg.Thread.Validate(); err != nil {
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

	// Default mode only: teams is a one-time migration path with no standby
	// stream, so it has no buddy lane and keeps the fail-fast home dial. With a
	// buddy the dial is lazy, so a pod that restarts while home is down still
	// boots and serves the buddy lane.
	dialer := natsutil.NewBuddyDialer(cfg.Buddy.OnlyIf(cfg.Mode != "teams"), cfg.NatsCredsFile, sdk)
	nc, js, err := dialer.ConnectHomeJS(ctx, cfg.NatsURL, sdk.MeterProvider())
	if err != nil {
		slog.Error("nats connect failed", "error", err)
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
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref),
		mongoutil.WithDegradedStart())
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
	valkeyClient := valkeyDial(ctx, cfg.Valkey, sdk)
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
		dekStore := atrest.NewL2DEKStore(atrest.NewMongoDEKStore(dekColl),
			valkeyutil.Breakered(valkeyClient, cfg.Valkey.Breaker.New(ctx, "msgworkerdekl2")),
			cfg.DEKL2.TTL, dekBreaker, atrest.DefaultL2Recorder())
		cipher = atrest.NewCipher(w, dekStore, cfg.Atrest)
	}

	store := NewCassandraStore(cassSession, bucketSizer, cipher, WithThreadPolicy(cfg.Thread))
	threadStore := newThreadStoreMongo(db)
	ensureCtx, ensureCancel := context.WithTimeout(ctx, mongoutil.IndexEnsureTimeout)
	if err := threadStore.EnsureIndexes(ensureCtx); err != nil {
		slog.Warn("ensure thread store indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	ensureCancel()

	streamName := stream.MessagesCanonical(cfg.SiteID).Name
	if cfg.Mode == "teams" {
		streamName = stream.MessagesTeams(cfg.SiteID).Name
	}

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode, cfg.SiteID)
	if err := validateConsumerConfig(&consumerCfg, cfg.Mode); err != nil {
		slog.Error("invalid consumer config", "error", err, "mode", cfg.Mode)
		os.Exit(1)
	}

	mtr, err := newMetrics()
	if err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}

	// The home consumer is created inside the deferred bind, so the pollers below
	// resolve it per call rather than capturing it: during a home outage there is
	// no consumer to read, and an error is the honest answer — the alternative is
	// a nil capture taken before the cluster came back.
	homeConsumerInfo := func(ctx context.Context) (*jetstream.ConsumerInfo, error) {
		cons, err := js.Consumer(ctx, streamName, consumerCfg.Durable)
		if err != nil {
			return nil, fmt.Errorf("resolve home consumer %s: %w", consumerCfg.Durable, err)
		}
		return cons.Info(ctx)
	}
	stopLagPoller := startLagPoller(ctx, mtr, homeConsumerInfo, 15*time.Second)

	degradeTr := newDegradeTracker(histdegrade.NewStore(db), cfg.SiteID,
		func(ctx context.Context) (uint64, uint64, error) {
			ci, err := homeConsumerInfo(ctx)
			if err != nil {
				return 0, 0, fmt.Errorf("consumer info: %w", err)
			}
			// NumAckPending rides along with NumPending: history has caught up only
			// when nothing is left to deliver AND nothing is still cycling through
			// redelivery.
			// #nosec G115 -- NumAckPending is a queue depth bounded by MaxAckPending; never negative
			return ci.NumPending, uint64(ci.NumAckPending), nil
		}, mtr, nil, cfg.DegradeMarkDelay)
	// tickAfterInterval, not tickOnStart: the marker is read at startup by the
	// first message that needs it, and an immediate tick would race the consumer
	// coming up.
	stopDegradeRefresher := startTicker(ctx, cfg.DegradeRefresh, tickAfterInterval, degradeTr.Refresh)

	// One handler per lane; see failoverlane.BuildHandler for why nothing that
	// speaks NATS may be shared between them. An empty msgID is an ephemeral
	// client delivery on core NATS; otherwise the publish is JetStream-backed,
	// blocking on PubAck with msgID as the Nats-Msg-Id the server dedups on.
	//
	// Both lanes get historyStore{store}, so neither can reopen the loss window
	// by forgetting to tag its own errors as history failures — a failover-lane
	// message is still this site's message, written to this site's keyspace, and
	// its degradation marker is this site's marker. teamsMigration below
	// deliberately keeps the bare store: that lane is a separate stream and
	// durable, and a bulk-migration persist failure must not tell every live
	// client on the site that their history is incomplete.
	newLaneHandler := func(conn *o11ynats.Conn, laneJS o11ynats.JetStream, lane subject.Lane) *Handler {
		core := natsutil.CorePublishFunc(conn, publishMetrics,
			natsutil.WithPublishLabels(natsmetrics.DestinationRecipientEvent, natsmetrics.OperationThreadTCount))
		js := natsutil.JetStreamPublishFunc(laneJS, publishMetrics,
			natsutil.WithPublishLabels(natsmetrics.DestinationOutbox, natsmetrics.OperationRecipientPublish))
		publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
			if msgID == "" {
				return core(ctx, subj, data)
			}
			return js(ctx, subj, data, msgID)
		}
		return NewHandler(historyStore{store}, us, threadStore, cfg.SiteID, publish, mtr, degradeTr,
			newDropPolicy(cfg.InvalidRetryWindow, cfg.HistoryDropEnabled, cfg.MaxDropsPerMinute, nil),
			withPersistenceMetrics(domainMetrics), withLane(lane))
	}
	handler := newLaneHandler(nc, js, subject.LaneHome)

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

	// Armed before the lanes bind: a lane guard's SelfShutdown raises SIGTERM on
	// this process, and a signal raised before the handler exists is fatal.
	sig := shutdown.Signals()

	// One handler per lane; the migration path is home-only, so the failover
	// lane's processor carries no teams handler. The buddy lane never fails
	// startup — on any failure the service runs home-only.
	lanes, err := failoverlane.BindLanes(ctx, nc, js, dialer, &failoverlane.LanesSpec{
		SiteID: cfg.SiteID, MaxWorkers: cfg.MaxWorkers, Metrics: sharedMetrics,
		Bootstrap: cfg.Bootstrap.Enabled,
		Home: failoverlane.HomeLane{
			Stream: stream.Config{Name: streamName}, Consumer: consumerCfg,
			Bootstrap: func(ctx context.Context, js o11ynats.JetStream) error {
				return bootstrapStreams(ctx, js, cfg.SiteID, cfg.Mode, cfg.Bootstrap.Enabled)
			},
		},
		Buddy: &failoverlane.BuddyLane{
			Stream:   stream.MessagesCanonicalFailover(cfg.SiteID),
			Consumer: buildFailoverConsumerConfig(cfg.Consumer, cfg.SiteID),
		},
	}, func(_ context.Context, conn *o11ynats.Conn, laneJS o11ynats.JetStream, lane subject.Lane) (func(context.Context, jetstream.Msg), error) {
		if lane == subject.LaneHome {
			return stampRedelivery(canonicalProcessor(handler, teamsMigration, teamsBatchSubj)), nil
		}
		return stampRedelivery(canonicalProcessor(newLaneHandler(conn, laneJS, lane), nil, "")), nil
	})
	if err != nil {
		slog.Error("bind lanes failed", "error", err)
		os.Exit(1)
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		lanes.Check(),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("message-worker running", "site", cfg.SiteID)

	// The lanes mark their own guards and stop both iterators inside StopHooks.
	hooks := append(lanes.StopHooks(), lanes.DrainHooks()...)
	hooks = append(hooks,
		func(context.Context) error { stopLagPoller(); return nil },
		func(context.Context) error { stopDegradeRefresher(); return nil },
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
	// WaitOn, not Wait: a lane guard's SelfShutdown raises SIGTERM on this
	// process, and only the armed channel routes that into the graceful path.
	shutdown.WaitOn(ctx, sig, 25*time.Second, hooks...)
}

// stampRedelivery marks the context when a message is a JetStream redelivery,
// so the thread-reply writer can tell one from a first delivery: its tcount
// increment is not idempotent.
//
// Applied per lane rather than inside the shared pool because it is this
// service's semantics, not every consumer's — and applied to both lanes,
// because a redelivery on the buddy double-counts exactly as it would on home.
func stampRedelivery(process func(context.Context, jetstream.Msg)) func(context.Context, jetstream.Msg) {
	return func(ctx context.Context, msg jetstream.Msg) {
		process(natsutil.StampRedelivery(ctx, msg), msg)
	}
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
			// straight to Cassandra; the live .created feed runs the normal
			// pipeline. A nil teams handler means this lane has no migration
			// path at all (the failover lane), so never dispatch into it.
			if teams != nil && msg.Subject() == teamsBatchSubj {
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

// valkeyDial is this service's Valkey dial, extracted so a test can run exactly
// what main runs against a Valkey that is down. See TestValkeyStartupSurvivesOutage.
func valkeyDial(ctx context.Context, cfg valkeyutil.Config, sdk valkeyutil.Observability) valkeyutil.Client {
	return valkeyutil.ConnectOptional(ctx, cfg, "DEK and user L2", valkeyutil.Instrumented(sdk))
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
