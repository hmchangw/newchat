package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
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
	NatsURL            string                  `env:"NATS_URL,required"`
	NatsCredsFile      string                  `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID             string                  `env:"SITE_ID,required"`
	MongoURI           string                  `env:"MONGO_URI,required"`
	MongoDB            string                  `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername      string                  `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword      string                  `env:"MONGO_PASSWORD"  envDefault:""`
	MaxWorkers         int                     `env:"MAX_WORKERS"     envDefault:"100"`
	LargeRoomThreshold int                     `env:"LARGE_ROOM_THRESHOLD" envDefault:"500"`
	MaxAttachments     int                     `env:"MAX_ATTACHMENTS"      envDefault:"1"`
	MaxAttachmentBytes int                     `env:"MAX_ATTACHMENT_BYTES" envDefault:"8192"`
	ChatBaseURL        string                  `env:"CHAT_BASE_URL"   envDefault:"http://localhost:3000"`
	SubCacheSize       int                     `env:"GATEKEEPER_SUB_CACHE_SIZE"  envDefault:"100000"`
	SubCacheTTL        time.Duration           `env:"GATEKEEPER_SUB_CACHE_TTL"   envDefault:"2m"`
	RoomMetaCacheSize  int                     `env:"ROOM_META_CACHE_SIZE"       envDefault:"10000"`
	RoomMetaCacheTTL   time.Duration           `env:"ROOM_META_CACHE_TTL"        envDefault:"2m"`
	ValkeyAddrs        []string                `env:"VALKEY_ADDRS"               envSeparator:","`
	ValkeyPassword     string                  `env:"VALKEY_PASSWORD"            envDefault:""`
	RoomMetaL2TTL      time.Duration           `env:"ROOM_META_L2_TTL"           envDefault:"15m"`
	UserCacheSize      int                     `env:"USER_CACHE_SIZE"            envDefault:"10000"`
	UserCacheTTL       time.Duration           `env:"USER_CACHE_TTL"             envDefault:"5m"`
	HealthAddr         string                  `env:"HEALTH_ADDR"                envDefault:":8081"`
	PProfEnabled       bool                    `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr        string                  `env:"METRICS_ADDR"               envDefault:":9090"`
	Consumer           stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Buddy              natsutil.BuddyConfig    `envPrefix:"BUDDY_"`
	Bootstrap          bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	DebugLog           logctx.Config           `envPrefix:"DEBUG_LOG_"`
	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`
}

func main() {
	// Wrap the base JSON handler so per-request X-Debug rungs can surface
	// flow/debug/trace edges even though the floor stays at INFO; RenderLevelNames
	// prints the custom FLOW/TRACE levels by name.
	logctx.SetupDefault(os.Stdout)
	pretouchJSON()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}
	sharedMetrics := natsmetrics.NewFromProvider(sdk.MeterProvider())
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)
	domainMetrics := newGatekeeperMetrics(sdk.MeterProvider().Meter("message-gatekeeper"))

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

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)

	var metaValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		metaValkey, err = valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
		)
		if err != nil {
			slog.Error("valkey connect (room-meta L2) failed", "error", err)
			os.Exit(1)
		}
		slog.Info("room-meta L2 cache enabled", "ttl", cfg.RoomMetaL2TTL)
	}

	mongoStore := NewMongoStore(db, metaValkey, cfg.RoomMetaL2TTL)
	withMeta, err := newCachedMetaStore(mongoStore, cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL)
	if err != nil {
		slog.Error("init room meta cache failed", "error", err)
		os.Exit(1)
	}
	store, err := newCachedSubStore(withMeta, cfg.SubCacheSize, cfg.SubCacheTTL)
	if err != nil {
		slog.Error("init subscription cache failed", "error", err)
		os.Exit(1)
	}
	users, err := userstore.NewCache(userstore.NewMongoStore(db.Collection("users")),
		cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user meta cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("gatekeeper caches enabled",
		"sub_cache_size", cfg.SubCacheSize, "sub_cache_ttl", cfg.SubCacheTTL,
		"room_meta_cache_size", cfg.RoomMetaCacheSize, "room_meta_cache_ttl", cfg.RoomMetaCacheTTL,
		"user_cache_size", cfg.UserCacheSize, "user_cache_ttl", cfg.UserCacheTTL,
	)
	pub := func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		ack, err := js.PublishMsg(ctx, msg, opts...)
		publishMetrics.Attempt(ctx, natsmetrics.DestinationCanonical, natsmetrics.OperationCanonicalPublish, err)
		if err != nil {
			return nil, fmt.Errorf("publish to %q: %w", msg.Subject, err)
		}
		return ack, nil
	}
	reply := func(ctx context.Context, msg *nats.Msg) error {
		err := nc.PublishMsg(ctx, msg)
		publishMetrics.Attempt(ctx, natsmetrics.DestinationClientResponse, natsmetrics.OperationClientResponse, err)
		if err != nil {
			return fmt.Errorf("reply to %q: %w", msg.Subject, err)
		}
		return nil
	}
	parentFetcher := newHistoryParentFetcher(nc, cfg.ChatBaseURL, publishMetrics)
	handler := NewHandler(store, users, pub, reply, cfg.SiteID, parentFetcher, cfg.LargeRoomThreshold, cfg.MaxAttachments, cfg.MaxAttachmentBytes, cfg.ChatBaseURL, withGatekeeperMetrics(domainMetrics))

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	messagesCfg := stream.Messages(cfg.SiteID)
	consumerCfg := buildConsumerConfig(cfg.Consumer)
	consumerMetrics := sharedMetrics.Consumer(natsmetrics.ConsumerConfig{
		Site:   cfg.SiteID,
		Stream: messagesCfg.Name, Consumer: consumerCfg.Durable,
	})
	consumerMetrics.LoopStopped(ctx)
	cons, err := js.CreateOrUpdateConsumer(ctx, messagesCfg.Name, consumerCfg)
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		slog.Error("bind MESSAGES lane failed", "error", err)
		os.Exit(1)
	}
	consumerMetrics.LoopStarted(ctx)

	var wg sync.WaitGroup
	// One pool shared by both lanes, so a bound buddy lane does not double this
	// service's in-flight validations against MongoDB.
	sem := make(chan struct{}, cfg.MaxWorkers)

	natsmetrics.StartInPool(ctx, iter, consumerMetrics, sem, consumerCfg.MaxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		gatekeeperProcessor(handler, false))

	// Buddy lane. BindBuddy never fails startup — on any failure buddyLane stays
	// nil and the service runs home-only.
	binder := natsutil.FailoverBinder{
		Service: cfg.ServiceName, SiteID: cfg.SiteID, Buddy: cfg.Buddy,
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: cfg.MaxWorkers,
		Sem: sem, WG: &wg, Metrics: sharedMetrics,
	}
	var buddyLane *natsutil.Lane
	buddyConn := natsutil.BindBuddy(ctx, cfg.Buddy, cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace,
		func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			var bErr error
			buddyLane, bErr = binder.Bind(ctx, bjs, &natsutil.LaneSpec{
				Stream: stream.MessagesFailover(cfg.SiteID),
				// The canonical standby is published to, not consumed: a
				// validated failover send must have somewhere to go.
				AlsoEnsure: []stream.Config{stream.MessagesCanonicalFailover(cfg.SiteID)},
				Consumer:   buildFailoverConsumerConfig(cfg.Consumer),
			}, gatekeeperHandler(handler, true))
			return bErr
		})

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("message-gatekeeper running", "site", cfg.SiteID)

	shutdown.Wait(ctx, 25*time.Second,
		// Stop both iterators before draining either, so neither lane pulls
		// new work while the other is still finishing.
		func(ctx context.Context) error {
			consumerMetrics.LoopStopped(ctx)
			iter.Stop()
			buddyLane.Stop()
			return nil
		},
		// Both lanes feed one WaitGroup, so waiting on it drains both.
		func(ctx context.Context) error { return natsutil.WaitPool(ctx, &wg) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(_ context.Context) error { valkeyutil.Disconnect(metaValkey); return nil },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// buildConsumerConfig returns the durable consumer config for
// message-gatekeeper. Centralized so it is unit-testable without NATS.
// gatekeeperProcessor is the per-message body both lanes run. failoverLane is
// fixed by the consumer that delivered the message, so it is bound once here
// rather than re-derived from every message's subject.
// gatekeeperHandler is gatekeeperProcessor for callers that hand back a plain
// jetstream.Msg — the failover binder's handler shape.
func gatekeeperHandler(handler *Handler, failoverLane bool) func(context.Context, jetstream.Msg) {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		gatekeeperSettle(msgCtx, msg, handler, failoverLane)
	}
}

func gatekeeperProcessor(handler *Handler, failoverLane bool) natsmetrics.ProcessMessage {
	return func(msgCtx context.Context, msg *natsmetrics.Message) {
		gatekeeperSettle(msgCtx, msg, handler, failoverLane)
	}
}

// gatekeeperSettle is the per-message body both lanes run.
func gatekeeperSettle(msgCtx context.Context, msg jetstream.Msg, handler *Handler, failoverLane bool) {
	handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
	handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
	logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
	handler.HandleJetStreamMsg(handlerCtx, msg, failoverLane)
}

func buildConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-gatekeeper"
	return cc
}

// buildFailoverConsumerConfig is the durable consumer on the buddy-hosted
// MESSAGES-FAILOVER lane, which carries sends from clients displaced by this
// site's own NATS outage. Distinct durable from the home lane so the two keep
// independent cursors.
func buildFailoverConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "message-gatekeeper-failover"
	return cc
}

// canonicalSubjectForLane picks the canonical subject matching the lane a send
// arrived on. A failover-lane message must go to the failover canonical stream:
// the live one lives on the cluster that is down, so publishing there would
// send it nowhere.
func canonicalSubjectForLane(siteID string, failover bool) string {
	if failover {
		return subject.FailoverMsgCanonicalCreated(siteID)
	}
	return subject.MsgCanonicalCreated(siteID)
}
