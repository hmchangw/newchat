package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/bytedance/sonic"

	"github.com/caarlos0/env/v11"
	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	NatsURL       string `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID        string `env:"SITE_ID"                   envDefault:"default"`
	MongoURI      string `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB       string `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"            envDefault:""`
	// MongoReadPreference: read-only service (fan-out lookups); secondaryPreferred
	// offloads the primary.
	MongoReadPreference string `env:"MONGO_READ_PREFERENCE"     envDefault:"secondaryPreferred"`
	// MongoUserReadPreference covers the settings read that gates push delivery.
	// Kept off the client-wide preference (the other collections tolerate more
	// lag), but primaryPreferred rather than primary: a stale mute is recoverable,
	// a push pipeline that dies with the primary is not.
	MongoUserReadPreference string `env:"MONGO_USER_READ_PREFERENCE" envDefault:"primaryPreferred"`
	Pool                    mongoutil.PoolConfig
	MaxWorkers              int           `env:"MAX_WORKERS"               envDefault:"100"`
	LargeRoomThreshold      int           `env:"LARGE_ROOM_THRESHOLD"      envDefault:"500"`
	PushRecipientBatchSize  int           `env:"PUSH_RECIPIENT_BATCH_SIZE" envDefault:"100"`
	RoomMetaCacheSize       int           `env:"ROOM_META_CACHE_SIZE"      envDefault:"10000"`
	RoomMetaCacheTTL        time.Duration `env:"ROOM_META_CACHE_TTL"       envDefault:"2m"`
	RoomMetaL2              roommetacache.TTLConfig
	Valkey                  valkeyutil.Config
	RoomSubCache            roomsubcache.TTLConfig
	Breaker                 mongoutil.BreakerConfig
	PresenceBatchSize       int                     `env:"PRESENCE_BATCH_SIZE"       envDefault:"512"`
	PresenceRPCTimeout      time.Duration           `env:"PRESENCE_RPC_TIMEOUT"      envDefault:"2s"`
	PresenceEnabled         bool                    `env:"PRESENCE_RPC_ENABLED"      envDefault:"false"` // false → noopPresenceSnapshotter; set true once presence service is available
	BadgeCountEnabled       bool                    `env:"BADGE_COUNT_RPC_ENABLED"   envDefault:"true"`  // true → per-recipient UnreadCounts stamped via badge.count.batch; set false to disable (nil badgeClient, no counts)
	UserSettingsEnabled     bool                    `env:"USER_SETTINGS_ENABLED"     envDefault:"true"`  // false → noopUserSettings, i.e. pre-enforcement behaviour; kill switch, not a rollout gate
	UserSettingsBatchSize   int                     `env:"USER_SETTINGS_BATCH_SIZE"  envDefault:"512"`
	UserSettingsTimeout     time.Duration           `env:"USER_SETTINGS_TIMEOUT"     envDefault:"2s"`
	UserCacheSize           int                     `env:"USER_CACHE_SIZE"           envDefault:"10000"`
	UserCacheTTL            time.Duration           `env:"USER_CACHE_TTL"            envDefault:"5m"`
	MentionNamesEnabled     bool                    `env:"MENTION_NAMES_ENABLED"     envDefault:"true"` // false → MentionNames nil, i.e. only @all/@here substituted; kill switch for a sick users collection
	MentionNamesTimeout     time.Duration           `env:"MENTION_NAMES_TIMEOUT"     envDefault:"2s"`
	Mode                    stream.Pipeline         `env:"MODE,required"` // user | bot; drives all stream/subject wiring via pkg/stream.Resolve
	Consumer                stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Buddy                   natsutil.BuddyConfig    `envPrefix:"BUDDY_"`
	Bootstrap               bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	HealthAddr              string                  `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled            bool                    `env:"PPROF_ENABLED" envDefault:"false"`
}

// natsLane is every HandlerDeps field bound to one NATS connection, split out
// so a lane cannot be built by copying another lane's deps and swapping a
// single field; see failoverlane.HandlerFor.
type natsLane struct {
	Parent   ParentFetcher
	Presence PresenceSnapshotter
	Badge    badgeClient
	Emitter  Emitter
}

// bind returns a copy of base with every connection-bound field replaced by
// this lane's. base is taken by pointer only to avoid copying it twice; it is
// never mutated, so both lanes can bind the same base.
func (l natsLane) bind(base *HandlerDeps) HandlerDeps {
	deps := *base
	deps.Parent = l.Parent
	deps.Presence = l.Presence
	deps.BadgeClient = l.Badge
	deps.Emitter = l.Emitter
	return deps
}

func main() {
	pretouchJSON()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Valkey.Validate(); err != nil {
		slog.Error("invalid valkey config", "error", err)
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

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}
	sharedMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics)
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)
	domainMetrics := newNotificationMetrics(sdk.MeterProvider().Meter("notification-worker"))

	readPref, err := mongoutil.ParseReadPreference(cfg.MongoReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.MongoReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
	db := mongoClient.Database(cfg.MongoDB)
	// Pinned to primary: this feeds roomsubcache, a SHARED 90-minute entry every
	// service reads. A replica-lagged read here does not just miss one removal —
	// it republishes a removed member as a room member for the whole TTL, and
	// broadcast-worker and notification-worker both deliver to that list.
	subCol := mongoutil.CollectionWithReadPreference(db.Collection("subscriptions"), readpref.Primary())
	threadRoomCol := db.Collection("thread_rooms")
	// Pinned for the same reason as subCol: roomsCol is read through into
	// roommetacache, a SHARED key that message-gatekeeper and broadcast-worker
	// also read, so a lagging secondary republishes a renamed or just-deleted
	// room to all of them for the whole TTL.
	roomsCol := mongoutil.CollectionWithReadPreference(db.Collection("rooms"), readpref.Primary())
	// Settings gate push delivery, so this collection keeps its own preference
	// rather than the client-wide one. The other collections here tolerate more
	// replica lag and keep it.
	userReadPref, err := mongoutil.ParseReadPreference(cfg.MongoUserReadPreference)
	if err != nil {
		slog.Error("invalid mongo user read preference", "value", cfg.MongoUserReadPreference, "error", err)
		os.Exit(1)
	}
	slog.Info("mongo user read preference configured", "readPreference", userReadPref.Mode().String())
	usersCol := mongoutil.CollectionWithReadPreference(db.Collection("users"), userReadPref)

	valkeyClient, err := valkeyutil.Connect(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	// Built here rather than inside the loader: the tier's closures escape to the
	// heap, so constructing one per L1 miss would allocate on every cold room.
	metaTier := roommetacache.NewL2Tier(valkeyClient, roomsCol, cfg.RoomMetaL2.TTL,
		nil, cachemetrics.For("roommeta", "l2"))
	roomMetaCache, err := roommetacache.New(cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL, metaTier.Get)
	if err != nil {
		slog.Error("init room-meta cache failed", "error", err)
		os.Exit(1)
	}

	cache := roomsubcache.NewValkeyCache(valkeyClient)
	// The shared loader stamps each member's HOME site for the per-site badge
	// RPC. It has to: the cache key is shared, so whichever service fills it
	// first decides what every other service reads — a loader that skipped the
	// stamp here would be undone by a broadcast-worker cold fill anyway.
	// roomsubcache.NewLookup then adds the TTL slide, so a warm member list
	// survives an outage that outlasts its deadline.
	loader := roomsubcache.NewMongoLoader(subCol, usersCol)
	// Guard the loader, not the Lookup: an open breaker must still serve L2 hits.
	memberBreaker := cfg.Breaker.New(ctx, "roomsub",
		circuitbreaker.WithFailurePredicate(memberBreakerFailure))
	memberLookup := roomsubcache.NewLookup(cache,
		roomsubcache.GuardLoader(loader, memberBreaker), cfg.RoomSubCache.TTL)

	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	dialer := natsutil.BuddyDialer{
		Config: cfg.Buddy, CredsFile: cfg.NatsCredsFile,
		TracerProvider: sdk.TracerProvider(), Propagator: sdk.Propagator, TracingEnabled: sdk.Toggles.Trace,
	}

	otelJS, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	// Both modes filter on .created — notifications fire on new messages only,
	// not on edits/deletes/pins/reactions.
	wiring := stream.Resolve(cfg.Mode, cfg.SiteID)

	if err := bootstrapStreams(ctx, otelJS, wiring.CanonicalStream.Name, wiring.CanonicalCreated, wiring.PushStream.Name, wiring.PushInputWildcard, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("notification-worker"), wiring.CanonicalCreated)
	consumerMetrics := sharedMetrics.Consumer(natsmetrics.ConsumerConfig{
		Site:   cfg.SiteID,
		Stream: wiring.CanonicalStream.Name, Consumer: consumerCfg.Durable,
	})
	consumerMetrics.LoopStopped(ctx)
	cons, err := otelJS.CreateOrUpdateConsumer(ctx, wiring.CanonicalStream.Name, consumerCfg)
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}
	// laneNATS builds every connection-bound dependency from one connection, so
	// both lanes are wired the same way and neither can inherit the other's.
	laneNATS := func(conn *o11ynats.Conn, js o11ynats.JetStream, lane subject.Lane) natsLane {
		l := natsLane{
			Parent: newHistoryParentFetcher(conn, publishMetrics),
			// The broker advertises max_payload in its INFO on connect, so this
			// is always in step with the server. An env var was a second source
			// of truth that silently dropped batches whenever it drifted below
			// the real limit.
			Emitter:  newMobileEmitter(&jsPublisher{js: js, metrics: publishMetrics}, wiring.PushSend(lane), clampPayloadCap(conn.NatsConn().MaxPayload())),
			Presence: noopPresenceSnapshotter{},
		}
		if cfg.PresenceEnabled {
			l.Presence = newBulkPresenceSource(
				&natsPresenceRequester{nc: conn.NatsConn()},
				cfg.SiteID,
				cfg.PresenceBatchSize,
				cfg.PresenceRPCTimeout,
				publishMetrics,
			)
		}
		if cfg.BadgeCountEnabled {
			l.Badge = newNatsBadgeClient(conn)
		}
		return l
	}

	var settings UserSettingsSnapshotter = noopUserSettings{}
	if cfg.UserSettingsEnabled {
		settings = newMongoUserSettings(usersCol, cfg.UserSettingsBatchSize, cfg.UserSettingsTimeout)
	}

	// Display names for the push body read from the client-wide read preference
	// (MONGO_READ_PREFERENCE, secondaryPreferred by default), not the
	// primary-pinned usersCol above: a renamed user tolerates replica lag —
	// and up to USER_CACHE_TTL of cache staleness — unlike the mute settings
	// that gate delivery.
	var mentionNames MentionNameResolver
	if cfg.MentionNamesEnabled {
		userCache, cerr := userstore.NewCache(userstore.NewMongoStore(db.Collection("users")),
			cfg.UserCacheSize, cfg.UserCacheTTL)
		if cerr != nil {
			// Degrade rather than exit: a bad cache size/TTL costs display names in
			// push bodies, which is not worth refusing to deliver notifications at all.
			slog.Error("init user cache failed, mention display names disabled", "error", cerr)
		} else {
			mentionNames = newUserMentionNames(userCache, cfg.MentionNamesTimeout)
			slog.Info("mention display names enabled", "user_cache_size", cfg.UserCacheSize,
				"user_cache_ttl", cfg.UserCacheTTL, "lookup_timeout", cfg.MentionNamesTimeout)
		}
	} else {
		slog.Info("mention display names disabled", "reason", "MENTION_NAMES_ENABLED=false")
	}

	// Everything a handler needs that is the same on both lanes. The
	// connection-bound fields (Parent, Presence, BadgeClient, Emitter) are left
	// zero here and filled in per lane by natsLane.bind — Mongo and Valkey are
	// still up when NATS is not, so only the NATS-facing deps are rebuilt.
	baseDeps := HandlerDeps{
		Members:            memberLookup,
		Followers:          newMongoThreadFollowers(threadRoomCol),
		Settings:           settings,
		Hook:               noopVetoer{},
		RoomMeta:           roomMetaCache,
		MentionNames:       mentionNames,
		LargeRoomThreshold: cfg.LargeRoomThreshold,
		RecipientBatchSize: cfg.PushRecipientBatchSize,
		Metrics:            domainMetrics,
	}
	handler := NewHandler(laneNATS(nc, otelJS, subject.LaneHome).bind(&baseDeps))

	// Bounded worker drains the channel so slow Valkey doesn't block NATS dispatch; drops are safe because TTLs reconcile staleness.
	invalCtx, invalCancel := context.WithCancel(ctx)
	invalCh := make(chan string, 256)
	var invalWG sync.WaitGroup
	invalWG.Add(1)
	go func() {
		defer invalWG.Done()
		for roomID := range invalCh {
			memberLookup.Invalidate(invalCtx, roomID)
		}
	}()

	// Mute is the only canonical member event still on this stream; add/remove invalidation rides on MESSAGES-CANONICAL sys-messages.
	// DeliverNewPolicy: skip history on restart; roomsubcache TTL reconciles any boundary staleness.
	roomsCfg := stream.Rooms(cfg.SiteID)
	invalCons, err := otelJS.CreateOrUpdateConsumer(ctx, roomsCfg.Name, jetstream.ConsumerConfig{
		Durable:       cfg.Mode.ConsumerName("notification-worker-room-event-invalidate"),
		FilterSubject: subject.RoomCanonicalMemberEvent(cfg.SiteID, model.CanonicalMemberEventMuted),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		slog.Error("create canonical member event consumer failed", "error", err)
		os.Exit(1)
	}
	invalIter, err := invalCons.Messages(ctx, jetstream.PullMaxMessages(64))
	if err != nil {
		slog.Error("canonical member event iterator failed", "error", err)
		os.Exit(1)
	}
	go func() {
		for {
			_, msg, err := invalIter.Next()
			if err != nil {
				return
			}
			var evt model.CanonicalMemberEvent
			if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
				slog.Warn("canonical member event decode failed", "error", err)
				_ = msg.Ack()
				continue
			}
			if evt.RoomID != "" {
				select {
				case invalCh <- evt.RoomID:
				default:
					slog.Warn("invalidation queue full, dropping (TTL will reconcile)", "roomId", evt.RoomID)
				}
			}
			_ = msg.Ack()
		}
	}()

	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}
	consumerMetrics.LoopStarted(ctx)

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	natsmetrics.StartInPool(ctx, iter, consumerMetrics, sem, consumerCfg.MaxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		notifyProcessor(handler, domainMetrics))

	// Buddy lane. Never fails startup — on any failure buddyLane stays nil and
	// the service runs home-only. HasFailover gates the bot pipeline out.
	binder := failoverlane.Binder{
		SiteID: cfg.SiteID, Dialer: dialer.OnlyIf(wiring.HasFailover()),
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: cfg.MaxWorkers,
		Sem: sem, WG: &wg, Metrics: sharedMetrics,
	}
	buddyLane, buddyConn := binder.BindLane(ctx, &failoverlane.LaneSpec{
		Stream: wiring.CanonicalFailoverStream,
		// The push standby stream is published to, not consumed: it must exist
		// before the first failover notification is built.
		AlsoEnsure: []stream.Config{wiring.PushFailoverStream},
		Consumer: buildConsumerConfig(cfg.Consumer,
			cfg.Mode.FailoverConsumerName("notification-worker"), wiring.CanonicalFailoverCreated),
	}, func(_ context.Context, conn *o11ynats.Conn, laneJS o11ynats.JetStream, lane subject.Lane) (func(context.Context, jetstream.Msg), error) {
		return notifyHandler(NewHandler(laneNATS(conn, laneJS, lane).bind(&baseDeps)), domainMetrics), nil
	})

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("notification-worker started",
		"site", cfg.SiteID,
		"large_room_threshold", cfg.LargeRoomThreshold,
		"push_recipient_batch_size", cfg.PushRecipientBatchSize,
		"valkey_addrs", cfg.Valkey.Addrs,
		"presence_enabled", cfg.PresenceEnabled,
		"badge_count_enabled", cfg.BadgeCountEnabled,
		"user_settings_enabled", cfg.UserSettingsEnabled,
	)

	shutdown.Wait(ctx, 25*time.Second,
		// Stop both iterators before draining, so neither lane pulls new work
		// while the other is still finishing. Both feed one WaitGroup.
		func(_ context.Context) error {
			consumerMetrics.LoopStopped(context.Background())
			iter.Stop()
			buddyLane.Stop()
			return nil
		},
		func(ctx context.Context) error {
			return natsutil.WaitPool(ctx, &wg)
		},
		func(_ context.Context) error {
			invalIter.Stop()
			return nil
		},
		func(stepCtx context.Context) error {
			close(invalCh) // stop accepting work; worker drains the buffer
			done := make(chan struct{})
			go func() { invalWG.Wait(); close(done) }()
			select {
			case <-done:
			case <-stepCtx.Done():
				invalCancel() // unblock an in-flight Valkey DEL so the worker exits
				<-done
			}
			invalCancel() // always release the context (idempotent)
			return nil
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(_ context.Context) error { valkeyutil.Disconnect(valkeyClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// notifyProcessor is the per-message body both lanes run, so the home lane and
// the buddy lane settle a message identically — a failover-lane event is still
// this site's event.
// notifyHandler is notifyProcessor for callers that hand back a plain
// jetstream.Msg — the failover binder's handler shape.
func notifyHandler(handler *Handler, domainMetrics *notificationMetrics) func(context.Context, jetstream.Msg) {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		notifySettle(msgCtx, msg, handler, domainMetrics)
	}
}

func notifyProcessor(handler *Handler, domainMetrics *notificationMetrics) natsmetrics.ProcessMessage {
	return func(msgCtx context.Context, msg *natsmetrics.Message) {
		notifySettle(msgCtx, msg, handler, domainMetrics)
	}
}

// notifySettle is the per-message body both lanes run.
func notifySettle(msgCtx context.Context, msg jetstream.Msg, handler *Handler, domainMetrics *notificationMetrics) {
	// jobguard recovers handler panics — this goroutine runs outside natsrouter's Recovery
	// middleware, so an unrecovered panic would crash the worker and crash-loop on redelivery.
	jobguard.Run(msg, func() {
		handlerCtx, reqID := logctx.ConsumeContext(msgCtx, msg.Headers(), msg.Subject(), msg.Data())
		// Migrated events carry X-Migration: live — the source already delivered them, so
		// this live-delivery worker must not re-notify. Ack and drop without invoking the handler.
		if natsutil.IsMigrationLiveHeader(msg.Headers()) {
			slog.Info("skipping migrated event (no re-notify)", "subject", msg.Subject(), "request_id", reqID)
			if err := msg.Ack(); err != nil {
				slog.Error("failed to ack migrated message", "error", err, "request_id", reqID)
			}
			domainMetrics.Record(handlerCtx, notifyKindPush, notifySuppressed)
			return
		}
		// Transient failures retry with backoff (never drop); malformed events Ack-drop as poison.
		jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, handler.HandleMessage(handlerCtx, msg.Data()))
	})
}

// buildConsumerConfig returns the durable consumer config, centralized so it's
// unit-testable without NATS; durable/filterSubject are pipeline-driven so the
// binary can bind to user or bot streams, and the failover lane reuses it with
// its own durable and filter.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durable
	cc.FilterSubjects = []string{filterSubject}
	return cc
}
