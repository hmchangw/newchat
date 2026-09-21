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
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/errcode"
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
	"github.com/hmchangw/chat/pkg/retrylane"
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
	Bootstrap          bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Atrest             atrest.Config
	Vault              atrest.VaultConfig
	DebugLog           logctx.Config `envPrefix:"DEBUG_LOG_"`
	// Retry is the tiered-redelivery lane; disabled by default. See pkg/retrylane.
	// Only meaningful for default mode's live .created feed — teamsbatch.go's
	// Teams-migration path settles with plain jsretry.Settle regardless of it.
	Retry retrylane.Settings `envPrefix:"RETRY_"`
}

// Durables for the two consumers a message-worker pod can bind, one per MODE.
// Shared between buildConsumerConfig and the retry lane so the lane's Consumer
// identity (which routes an escalation's subject back to this consumer) can
// never drift from the hot consumer's own Durable.
const (
	defaultConsumerDurable = "message-worker"
	teamsConsumerDurable   = "message-worker-teams"
)

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

	store := NewCassandraStore(cassSession, bucketSizer, cipher, WithThreadPolicy(cfg.Thread))
	threadStore := newThreadStoreMongo(db)
	ensureCtx, ensureCancel := context.WithTimeout(ctx, mongoutil.IndexEnsureTimeout)
	if err := threadStore.EnsureIndexes(ensureCtx); err != nil {
		slog.Warn("ensure thread store indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	ensureCancel()

	// retryLane escalates a message off the hot consumer's ack-pending budget once its
	// in-place fast-rung budget is spent. It backs only the default-mode live .created
	// feed handled by HandleJetStreamMsg — teamsbatch.go's Teams-migration path settles
	// with plain jsretry.Settle regardless, a different stream with batch semantics,
	// out of scope. RETRY_LANE_ENABLED gates new escalations only: the retry consumer
	// bound below (default mode only) drains regardless of the flag, so disabling it
	// cannot strand messages already parked on RETRY-{siteID}. See pkg/retrylane.
	retryLane := &retrylane.Lane{
		Consumer:  defaultConsumerDurable,
		SiteID:    cfg.SiteID,
		Enabled:   cfg.Retry.Enabled,
		FastSteps: cfg.Retry.FastSteps,
		Publish: func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
			_, err := js.PublishMsg(ctx, &nats.Msg{Subject: subj, Data: data, Header: hdr},
				jetstream.WithMsgID(msgID))
			return err
		},
	}
	// The fast rungs stay in place on the hot consumer; SlowBackoff's tail runs on the
	// retry lane instead. Disabled (or misconfigured to a non-positive FastSteps, which
	// also disables escalation in Lane.shouldEscalate), the handler keeps running the
	// full schedule in place. The clamp on the upper bound guards the slice so a
	// FastSteps above len(DefaultBackoff) cannot panic on the handler's first failure.
	fastBackoff := jsretry.DefaultBackoff
	if cfg.Retry.Enabled && cfg.Retry.FastSteps > 0 {
		steps := cfg.Retry.FastSteps
		if steps > len(jsretry.DefaultBackoff) {
			steps = len(jsretry.DefaultBackoff)
		}
		fastBackoff = jsretry.DefaultBackoff[:steps]
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
	}, withPersistenceMetrics(domainMetrics), withRetryLane(retryLane, fastBackoff))

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
				// Mark retries so the thread-reply writer can tell a redelivery
				// from a first delivery: its tcount increment is not idempotent.
				msgCtx = natsutil.StampRedelivery(msgCtx, msg)
				defer func() {
					tracked.Finish(msgCtx)
					<-sem
					wg.Done()
				}()
				// tracked.Escalated is derived per message: the hook closes over this
				// delivery's own recorder, so passing it down (rather than mutating a
				// shared Lane) keeps the escalation label race-free across goroutines.
				process(msgCtx, msg, tracked.Escalated)
			}(msgCtx, msg)
		}
	}()

	// The retry consumer only exists for default mode — HandleJetStreamMsg (the
	// only settler that ever escalates onto RETRY-{siteID}) is never invoked in
	// teams mode, whose filter subject excludes it entirely. Left nil in teams
	// mode; the shutdown step below checks for that before stopping it.
	var (
		retryIter            o11ynats.MessagesContext
		retryConsumerMetrics *natsmetrics.Consumer
		retryCons            o11ynats.Consumer
		retryConsumerCfg     jetstream.ConsumerConfig
	)
	if cfg.Mode == "default" {
		retryStreamCfg := stream.Retry(cfg.SiteID)
		retryConsumerCfg = retrylane.ConsumerConfig(cfg.SiteID, defaultConsumerDurable, &cfg.Retry)
		retryConsumerMetrics = sharedMetrics.Consumer(natsmetrics.ConsumerConfig{
			Site:   cfg.SiteID,
			Stream: retryStreamCfg.Name, Consumer: retryConsumerCfg.Durable,
		})
		retryConsumerMetrics.LoopStopped(ctx)
		// The retry consumer binds whenever RETRY-{siteID} exists — see the
		// rollback-asymmetry note above. The one tolerated failure is the stream
		// simply not being provisioned while the lane is off: phase 1 ships dark,
		// so that must not crash-loop this hot-path worker (retrylane.SkipMissingStream).
		var bindErr error
		retryCons, bindErr = js.CreateOrUpdateConsumer(ctx, retryStreamCfg.Name, retryConsumerCfg)
		if bindErr != nil && !retrylane.SkipMissingStream(ctx, retryStreamCfg.Name, cfg.Retry.Enabled, bindErr) {
			slog.Error("create retry consumer failed", "error", bindErr)
			os.Exit(1)
		}
	}
	// nil outside default mode, and when the lane is off with RETRY-{siteID}
	// unprovisioned — in which case nothing can be parked there to drain.
	if retryCons != nil {
		// The retry lane does not escalate again in phases 0-3, so it settles with
		// plain jsretry.Settle on the slow-rung schedule relocated off the hot consumer.
		slowBackoff := retrylane.SlowBackoff(cfg.Retry.FastSteps, jsretry.DefaultBackoff)

		retryIter, err = retryCons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.Retry.Consumer.MaxWorkers))
		if err != nil {
			slog.Error("retry messages failed", "error", err)
			os.Exit(1)
		}
		retryConsumerMetrics.LoopStarted(ctx)

		// Sized off RETRY_CONSUMER_MAX_WORKERS (default 10), not the hot loop's
		// MAX_WORKERS: both loops run in this process, so reusing MaxWorkers would make
		// the in-flight cap 2×MaxWorkers exactly during the incident that fills the retry
		// lane. Its own semaphore, never shared with the hot loop — sharing one would let
		// a retry backlog starve live deliveries of slots.
		retrySem := make(chan struct{}, cfg.Retry.Consumer.MaxWorkers)

		wg.Add(1)
		go func() {
			// Counted on the same wg as the hot loop so shutdown's single wg.Wait()
			// step covers both consume loops.
			defer wg.Done()
			for {
				msgCtx, msg, err := retryIter.Next()
				if err != nil {
					retryConsumerMetrics.LoopFailed(context.Background(), err)
					return
				}
				retrySem <- struct{}{}
				wg.Add(1)
				go func(msgCtx context.Context, msg jetstream.Msg) {
					// Classified from X-Retry-Origin-Subject: a retry subject's tail is
					// "slow", so classifying from msg.Subject() would label every
					// retry-lane delivery event_type="unknown".
					tracked := retryConsumerMetrics.Track(msgCtx, msg,
						natsmetrics.EventTypeFromSubject(retrylane.OriginSubject(msg.Headers(), msg.Subject())),
						retryConsumerCfg.MaxDeliver)
					msg = tracked
					msgCtx = tracked.Context(msgCtx)
					defer func() {
						tracked.Finish(msgCtx)
						<-retrySem
						wg.Done()
					}()
					jobguard.Run(msg, func() {
						handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
						handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
						logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
						jsretry.Settle(handlerCtx, msg, slowBackoff, handler.process(handlerCtx, msg))
					})
				}(msgCtx, msg)
			}
		}()
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("message-worker running", "site", cfg.SiteID, "retry_lane_enabled", cfg.Retry.Enabled)

	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error {
			consumerMetrics.LoopStopped(ctx)
			iter.Stop()
			// Stopped unconditionally alongside the hot iterator whenever it exists
			// (default mode only) — see the rollback-asymmetry note above.
			if retryIter != nil {
				retryConsumerMetrics.LoopStopped(ctx)
				retryIter.Stop()
			}
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

// buildConsumerConfig returns the durable consumer config for the given mode.
// default mode binds only the live .created feed on MESSAGES-CANONICAL (.updated/
// .deleted are excluded — history-service already wrote Cassandra synchronously for
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
		cc.Durable = teamsConsumerDurable
		cc.FilterSubjects = []string{subject.MsgTeamsCanonicalBatch(siteID)}
		return cc
	}
	cc.Durable = defaultConsumerDurable
	cc.FilterSubjects = []string{subject.MsgCanonicalCreated(siteID)}
	return cc
}

// canonicalProcessor returns the consume loop's per-message body: subject
// dispatch plus request-id and log-context stamping, wrapped by jobguard so a
// handler panic Acks instead of crash-looping on JetStream redelivery. Named
// rather than inlined in main so the consumer test drives this exact
// composition, matching broadcast-worker's guardedProcessor.
func canonicalProcessor(h *Handler, teams *teamsBatchHandler, teamsBatchSubj string) func(context.Context, jetstream.Msg, func()) {
	return func(msgCtx context.Context, msg jetstream.Msg, onEscalate func()) {
		jobguard.Run(msg, func() {
			handlerCtx, _ := logctx.ConsumeContext(msgCtx, msg.Headers(), msg.Subject(), msg.Data())
			// Dispatch by subject: the one-time .teams.batch migration writes
			// straight to Cassandra; the live .created feed runs the normal pipeline.
			if msg.Subject() == teamsBatchSubj {
				teams.consume(handlerCtx, msg)
				return
			}
			h.HandleJetStreamMsg(handlerCtx, msg, onEscalate)
		})
	}
}
