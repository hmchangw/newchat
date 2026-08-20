package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/bytedance/sonic"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
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
	MongoReadPreference    string                  `env:"MONGO_READ_PREFERENCE"     envDefault:"secondaryPreferred"`
	MaxWorkers             int                     `env:"MAX_WORKERS"               envDefault:"100"`
	LargeRoomThreshold     int                     `env:"LARGE_ROOM_THRESHOLD"      envDefault:"500"`
	PushRecipientBatchSize int                     `env:"PUSH_RECIPIENT_BATCH_SIZE" envDefault:"100"`
	RoomMetaCacheSize      int                     `env:"ROOM_META_CACHE_SIZE"      envDefault:"10000"`
	RoomMetaCacheTTL       time.Duration           `env:"ROOM_META_CACHE_TTL"       envDefault:"2m"`
	RoomMetaL2TTL          time.Duration           `env:"ROOM_META_L2_TTL"          envDefault:"15m"`
	ValkeyAddrs            []string                `env:"VALKEY_ADDRS"              envSeparator:","`
	ValkeyPassword         string                  `env:"VALKEY_PASSWORD"           envDefault:""`
	RoomSubCacheTTL        time.Duration           `env:"ROOMSUBCACHE_TTL"          envDefault:"5m"`
	PresenceBatchSize      int                     `env:"PRESENCE_BATCH_SIZE"       envDefault:"512"`
	PresenceRPCTimeout     time.Duration           `env:"PRESENCE_RPC_TIMEOUT"      envDefault:"2s"`
	PresenceEnabled        bool                    `env:"PRESENCE_RPC_ENABLED"      envDefault:"false"` // false → noopPresenceSnapshotter; set true once presence service is available
	BadgeCountEnabled      bool                    `env:"BADGE_COUNT_RPC_ENABLED"   envDefault:"true"`  // true → per-recipient UnreadCounts stamped via badge.count.batch; set false to disable (nil badgeClient, no counts)
	UserSettingsEnabled    bool                    `env:"USER_SETTINGS_ENABLED"     envDefault:"true"`  // false → noopUserSettings, i.e. pre-enforcement behaviour; kill switch, not a rollout gate
	UserSettingsBatchSize  int                     `env:"USER_SETTINGS_BATCH_SIZE"  envDefault:"512"`
	UserSettingsTimeout    time.Duration           `env:"USER_SETTINGS_TIMEOUT"     envDefault:"2s"`
	UserCacheSize          int                     `env:"USER_CACHE_SIZE"           envDefault:"10000"`
	UserCacheTTL           time.Duration           `env:"USER_CACHE_TTL"            envDefault:"5m"`
	MentionNamesEnabled    bool                    `env:"MENTION_NAMES_ENABLED"     envDefault:"true"` // false → MentionNames nil, i.e. only @all/@here substituted; kill switch for a sick users collection
	MentionNamesTimeout    time.Duration           `env:"MENTION_NAMES_TIMEOUT"     envDefault:"2s"`
	Mode                   stream.Pipeline         `env:"MODE,required"` // user | bot; drives all stream/subject wiring via pkg/stream.Resolve
	Consumer               stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap              bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	HealthAddr             string                  `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled           bool                    `env:"PPROF_ENABLED" envDefault:"false"`
}

// mongoMemberLoader loads a room's member list and stamps each member's HOME
// site from the users collection (one batch $in per cache fill). Not
// Subscription.siteId — that is the ROOM's home site, which would misroute
// badge RPCs. This deliberately revises the "no users-collection lookups"
// contract (docs/notification-worker-downstream-contracts.md §3), cache-fill
// time only.
type mongoMemberLoader struct {
	col   *mongo.Collection // subscriptions
	users *mongo.Collection // users — home-site (siteId) lookup
}

func (m *mongoMemberLoader) Load(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	projection := bson.M{
		"u._id":              1,
		"u.account":          1,
		"u.isBot":            1,
		"roomType":           1,
		"muted":              1,
		"historySharedSince": 1,
	}
	cur, err := m.col.Find(ctx, bson.M{"roomId": roomID}, options.Find().SetProjection(projection))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for room %s: %w", roomID, err)
	}
	defer cur.Close(ctx)

	var out []roomsubcache.Member
	for cur.Next(ctx) {
		var doc struct {
			User struct {
				ID      string `bson:"_id"`
				Account string `bson:"account"`
				IsBot   bool   `bson:"isBot"`
			} `bson:"u"`
			RoomType           model.RoomType `bson:"roomType"`
			Muted              bool           `bson:"muted"`
			HistorySharedSince *time.Time     `bson:"historySharedSince"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode subscription: %w", err)
		}
		var hssMs *int64
		if doc.HistorySharedSince != nil {
			ms := doc.HistorySharedSince.UnixMilli()
			hssMs = &ms
		}
		out = append(out, roomsubcache.Member{
			ID:                 doc.User.ID,
			Account:            doc.User.Account,
			RoomType:           doc.RoomType,
			IsBot:              doc.User.IsBot,
			Muted:              doc.Muted,
			HistorySharedSince: hssMs,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscriptions: %w", err)
	}
	if err := m.fillHomeSites(ctx, roomID, out); err != nil {
		return nil, err
	}
	return out, nil
}

// fillHomeSites stamps each member's HomeSiteID from the users collection
// ({account $in members}, projected to {account, siteId}). An account missing
// from users leaves HomeSiteID empty — that member degrades to no badge count
// downstream rather than misrouting the RPC.
func (m *mongoMemberLoader) fillHomeSites(ctx context.Context, roomID string, members []roomsubcache.Member) error {
	if len(members) == 0 {
		return nil
	}
	accounts := make([]string, 0, len(members))
	for i := range members {
		accounts = append(accounts, members[i].Account)
	}
	cur, err := m.users.Find(ctx, bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"_id": 0, "account": 1, "siteId": 1}))
	if err != nil {
		return fmt.Errorf("find home sites for room %s members: %w", roomID, err)
	}
	defer cur.Close(ctx)

	siteByAccount := make(map[string]string, len(accounts))
	for cur.Next(ctx) {
		var doc struct {
			Account string `bson:"account"`
			SiteID  string `bson:"siteId"`
		}
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode user home site: %w", err)
		}
		siteByAccount[doc.Account] = doc.SiteID
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate user home sites: %w", err)
	}
	for i := range members {
		members[i].HomeSiteID = siteByAccount[members[i].Account]
	}
	return nil
}

func main() {
	pretouchJSON()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if len(cfg.ValkeyAddrs) == 0 {
		slog.Error("VALKEY_ADDRS required")
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
		mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
	db := mongoClient.Database(cfg.MongoDB)
	subCol := db.Collection("subscriptions")
	threadRoomCol := db.Collection("thread_rooms")
	roomsCol := db.Collection("rooms")
	// Settings gate push delivery, so a stale read means a user who just muted
	// still gets notified; route to primary regardless of the client-wide
	// preference. The other collections here tolerate replica lag and keep it.
	usersCol := mongoutil.CollectionWithReadPreference(db.Collection("users"), readpref.Primary())

	valkeyClient, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
		valkeyutil.WithObservability(sdk),
		valkeyutil.WithRequireParentSpan(true),
	)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	metaRec := cachemetrics.For("roommeta", "l2")
	roomMetaCache, err := roommetacache.New(cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL,
		func(ctx context.Context, roomID string) (roommetacache.Meta, error) {
			return roommetacache.ReadThrough(ctx, valkeyClient, roomsCol, roomID, cfg.RoomMetaL2TTL, metaRec)
		})
	if err != nil {
		slog.Error("init room-meta cache failed", "error", err)
		os.Exit(1)
	}

	cache := roomsubcache.NewValkeyCache(valkeyClient)
	loader := &mongoMemberLoader{col: subCol, users: usersCol}
	memberLookup := newCachedMemberLookup(cache, loader.Load, cfg.RoomSubCacheTTL)

	nc, err := natsutil.ConnectWithMetrics(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace, sdk.MeterProvider())
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
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
	// The broker advertises max_payload in its INFO on connect, so this is
	// always in step with the server. An env var was a second source of truth
	// that silently dropped batches whenever it drifted below the real limit.
	emitter := newMobileEmitter(&jsPublisher{js: otelJS, metrics: publishMetrics}, wiring.PushSendSubject, clampPayloadCap(nc.NatsConn().MaxPayload()))

	var presence PresenceSnapshotter = noopPresenceSnapshotter{}
	if cfg.PresenceEnabled {
		presence = newBulkPresenceSource(
			&natsPresenceRequester{nc: nc.NatsConn()},
			cfg.SiteID,
			cfg.PresenceBatchSize,
			cfg.PresenceRPCTimeout,
			publishMetrics,
		)
	}

	var badge badgeClient
	if cfg.BadgeCountEnabled {
		badge = newNatsBadgeClient(nc)
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

	handler := NewHandler(HandlerDeps{
		Members:            memberLookup,
		Followers:          newMongoThreadFollowers(threadRoomCol),
		Parent:             newHistoryParentFetcher(nc, publishMetrics),
		Presence:           presence,
		Settings:           settings,
		Hook:               noopVetoer{},
		Emitter:            emitter,
		RoomMeta:           roomMetaCache,
		MentionNames:       mentionNames,
		BadgeClient:        badge,
		LargeRoomThreshold: cfg.LargeRoomThreshold,
		RecipientBatchSize: cfg.PushRecipientBatchSize,
		Metrics:            domainMetrics,
	})

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
				// jobguard recovers handler panics — this goroutine runs outside natsrouter's Recovery
				// middleware, so an unrecovered panic would crash the worker and crash-loop on redelivery.
				jobguard.Run(msg, func() {
					handlerCtx, reqID := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
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

	slog.Info("notification-worker started",
		"site", cfg.SiteID,
		"large_room_threshold", cfg.LargeRoomThreshold,
		"push_recipient_batch_size", cfg.PushRecipientBatchSize,
		"valkey_addrs", cfg.ValkeyAddrs,
		"presence_enabled", cfg.PresenceEnabled,
		"badge_count_enabled", cfg.BadgeCountEnabled,
		"user_settings_enabled", cfg.UserSettingsEnabled,
	)

	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error {
			consumerMetrics.LoopStopped(context.Background())
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
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(_ context.Context) error { valkeyutil.Disconnect(valkeyClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// buildConsumerConfig returns the durable consumer config, centralized so it's unit-testable
// without NATS; durable/filterSubject are env-driven so the binary can bind to user or bot pipelines.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durable
	cc.FilterSubjects = []string{filterSubject}
	return cc
}
