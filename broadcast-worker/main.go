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
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/atrest"
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
	"github.com/hmchangw/chat/pkg/preview"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type encryptionConfig struct {
	Enabled bool `env:"ENABLED" envDefault:"false"`
}

type config struct {
	NatsURL       string `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID        string `env:"SITE_ID"                   envDefault:"default"`
	// AllSiteIDs is every site in the supercluster, this one included. Peers are
	// the destinations for the cross-site room-activity refresh; empty (the
	// single-site default) disables it.
	AllSiteIDs    []string `env:"ALL_SITE_IDS" envSeparator:","`
	MongoURI      string   `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB       string   `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername string   `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword string   `env:"MONGO_PASSWORD"            envDefault:""`
	// MongoReadPreference: reads only; writes always hit the primary. secondaryPreferred
	// offloads reads (a just-joined member is recovered via history).
	MongoReadPreference string `env:"MONGO_READ_PREFERENCE"     envDefault:"secondaryPreferred"`
	// MongoKeyReadPreference covers room keys and preview DEKs. Separate from the
	// client-wide preference because key freshness matters more than room meta, but
	// primaryPreferred rather than primary: a stale DEK read cannot diverge
	// ($setOnInsert plus a re-read comparison) and a missing room key is already a
	// retryable error, so failing outright only costs encrypted delivery.
	MongoKeyReadPreference string `env:"MONGO_KEY_READ_PREFERENCE" envDefault:"primaryPreferred"`
	Pool                   mongoutil.PoolConfig
	MaxWorkers             int `env:"MAX_WORKERS"               envDefault:"100"`
	// RoomActivityRefreshInterval caps how often one room's position is announced
	// cross-site. Generous by design: the subscription list serves ordering from a
	// cache with a far longer TTL, so a position a few seconds behind is
	// indistinguishable from a fresh one. Non-positive announces on every message.
	RoomActivityRefreshInterval time.Duration `env:"ROOM_ACTIVITY_REFRESH_INTERVAL" envDefault:"5s"`
	UserCacheSize               int           `env:"USER_CACHE_SIZE"           envDefault:"10000"`
	UserCacheTTL                time.Duration `env:"USER_CACHE_TTL"            envDefault:"5m"`
	UserL2                      userstore.TTLConfig
	RoomMetaCacheSize           int                     `env:"ROOM_META_CACHE_SIZE"      envDefault:"10000"`
	RoomMetaCacheTTL            time.Duration           `env:"ROOM_META_CACHE_TTL"       envDefault:"2m"`
	RoomKeyGracePeriod          time.Duration           `env:"ROOM_KEY_GRACE_PERIOD"     envDefault:"24h"`
	RoomKeyCacheTTL             time.Duration           `env:"ROOM_KEY_CACHE_TTL"        envDefault:"10m"`
	RoomKeyCacheSize            int                     `env:"ROOM_KEY_CACHE_SIZE"       envDefault:"50000"`
	Breaker                     mongoutil.BreakerConfig `envPrefix:"BROADCAST_"`
	RoomKeyRetiredTTL           time.Duration           `env:"ROOM_KEY_RETIRED_TTL"      envDefault:"30m"` // read only, to fail fast when too short for this cache's TTL
	RoomMetaL2                  roommetacache.TTLConfig
	RoomSubCache                roomsubcache.TTLConfig
	Valkey                      valkeyutil.Config
	ValkeyKeyGracePeriod        time.Duration   `env:"VALKEY_KEY_GRACE_PERIOD" envDefault:"24h"`
	HealthAddr                  string          `env:"HEALTH_ADDR"              envDefault:":8081"`
	PProfEnabled                bool            `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr                 string          `env:"METRICS_ADDR"             envDefault:":9090"`
	Mode                        stream.Pipeline `env:"MODE,required"` // user | bot; drives all stream/subject wiring via pkg/stream.Resolve
	RoomSubjectMode             string          `env:"ROOM_SUBJECT_MODE"        envDefault:"global"`
	// RoomLocalityGrace: post-flip dual-publish window. Must match across all publisher services.
	RoomLocalityGrace time.Duration `env:"ROOM_LOCALITY_GRACE"      envDefault:"168h"`
	// ThreadViewSubjectEnabled: kill switch for the thread-scoped view lane.
	ThreadViewSubjectEnabled bool `env:"THREAD_VIEW_SUBJECT_ENABLED" envDefault:"true"`
	// FailoverRevertGrace: post-restoration dual-publish window. Must outlast the
	// client revert backoff (capped at 5m) — raising the client cap without
	// raising this reopens the silent recovery gap dual-publishing exists to close.
	FailoverRevertGrace time.Duration           `env:"FAILOVER_REVERT_GRACE" envDefault:"30m"`
	Consumer            stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Buddy               natsutil.BuddyConfig    `envPrefix:"BUDDY_"`
	Bootstrap           bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Encryption          encryptionConfig        `envPrefix:"ENCRYPTION_"`
	DebugLog            logctx.Config           `envPrefix:"DEBUG_LOG_"`
	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`

	// Atrest/Vault gate room-list preview persistence. Distinct from Encryption
	// above, which is the client-facing room-key transport: this is at-rest
	// protection for the preview body this worker writes into the room doc.
	Atrest atrest.Config      // env vars are already prefixed ATREST_*
	Vault  atrest.VaultConfig // env vars are already prefixed (VAULT_*, ATREST_VAULT_*)
	// PreviewKeyEpoch selects the site preview DEK (preview:{siteID}:{epoch}).
	// Rotation is an ops action — bump and redeploy. Keep it in step with
	// history-service, which reads what this writes: a reader on another epoch
	// treats the preview as absent.
	PreviewKeyEpoch int `env:"PREVIEW_KEY_EPOCH" envDefault:"1"`
	// PreviewFlushInterval is how often the buffered room-preview writes drain to
	// MongoDB. Coalescing collapses a room's whole interval into one document
	// update, so this is what bounds the write rate, not the message rate. It
	// also bounds how long a room's stored preview trails its newest message —
	// keep it short: history-service serves a preview only while its freshness
	// key matches the room's lastMsgId, which roomlist-worker advances on its own
	// (equally short) cadence, and a room whose two halves disagree is served by
	// the Cassandra walk instead.
	PreviewFlushInterval time.Duration `env:"PREVIEW_FLUSH_INTERVAL" envDefault:"250ms"`
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

	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Breaker.Validate("BROADCAST_"); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	if err := model.SetPlatformAdminAccountPrefix(cfg.AdminAcctPrefix); err != nil {
		slog.Error("invalid ADMIN_ACCT_PREFIX", "error", err)
		os.Exit(1)
	}

	roomRouteMode, err := subject.ParseRoomRouteMode(cfg.RoomSubjectMode)
	if err != nil {
		slog.Error("invalid ROOM_SUBJECT_MODE", "error", err)
		os.Exit(1)
	}
	subject.SetRoomLocalityGrace(cfg.RoomLocalityGrace)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}
	sharedMetrics := natsmetrics.NewFromProviderIfEnabled(sdk.MeterProvider(), sdk.Toggles.Metrics)
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)
	// Same gate as the line above, for the same reason. Every recorder on
	// *broadcastMetrics already guards a nil receiver, so the toggle collapses
	// the per-recipient Delivery call to a return instead of to a no-op
	// instrument that still costs a map lookup and an SDK Add.
	var domainMetrics *broadcastMetrics
	if sdk.Toggles.Metrics {
		domainMetrics = newBroadcastMetrics(sdk.MeterProvider().Meter("broadcast-worker"))
	}

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
	keyReadPref, err := mongoutil.ParseReadPreference(cfg.MongoKeyReadPreference)
	if err != nil {
		slog.Error("invalid mongo key read preference", "value", cfg.MongoKeyReadPreference, "error", err)
		os.Exit(1)
	}
	slog.Info("mongo key read preference configured", "readPreference", keyReadPref.Mode().String())
	db := mongoClient.Database(cfg.MongoDB)
	valkeyClient, err := valkeyutil.Connect(ctx, cfg.Valkey, valkeyutil.Instrumented(sdk))
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}
	if valkeyClient != nil {
		slog.Info("valkey L2 tiers enabled", "room_meta_ttl", cfg.RoomMetaL2.TTL, "user_ttl", cfg.UserL2.TTL)
	}
	// One breaker for every Mongo call site in this service, not one per site.
	// The breaker tracks a single fact — is Mongo reachable — and every call site
	// is evidence about it, so they share one failure budget: N breakers at
	// threshold T cost N*T stalled calls before the service is fully fenced,
	// which is the delay the breaker exists to remove. Call sites differ only in
	// which "healthy absence" they can return, and mongoBreakerFailure exempts
	// all of them.
	mongoBreaker := cfg.Breaker.New(ctx, "mongo",
		circuitbreaker.WithFailurePredicate(mongoBreakerFailure))
	// subscriptions is pinned to primary: it feeds roomsubcache, a SHARED
	// 90-minute entry that notification-worker reads too, so a replica-lagged
	// read here republishes a removed member to every consumer of that key for
	// the whole TTL rather than costing one stale fan-out. Same reasoning as
	// roomsPrimary below.
	subsPrimary := mongoutil.CollectionWithReadPreference(db.Collection("subscriptions"), readpref.Primary())
	// rooms is pinned for the identical reason: it is what GetRoomMeta reads
	// through into roommetacache, another key shared with message-gatekeeper and
	// notification-worker. A lagging secondary there republishes a renamed or
	// just-deleted room to all of them for the TTL.
	roomsPrimaryStore := mongoutil.CollectionWithReadPreference(db.Collection("rooms"), readpref.Primary())
	// users is the third collection behind the same shared key: the roomsubcache
	// loader reads it to stamp HomeSiteID, and notification-worker routes its
	// per-site badge RPC off that field. A secondary that has not yet replicated
	// a just-added member stamps it empty for whoever wins the cold fill, and
	// every reader of the key inherits that for the TTL — so it is pinned
	// alongside subscriptions and rooms, as notification-worker already does.
	usersPrimary := mongoutil.CollectionWithReadPreference(db.Collection("users"), readpref.Primary())
	store := NewMongoStore(roomsPrimaryStore, subsPrimary, db.Collection("thread_rooms"),
		usersPrimary, valkeyClient, cfg.RoomMetaL2.TTL, cfg.RoomSubCache.TTL, mongoBreaker)

	var (
		previewCipher atrest.Cipher
		vaultWrapper  atrest.KeyWrapperCloser
	)
	if cfg.Atrest.Enabled {
		if cfg.PreviewKeyEpoch < 1 {
			// The epoch is part of the DEK id, so a non-positive value mints a
			// sentinel rotation could never move forward from.
			slog.Error("PREVIEW_KEY_EPOCH must be >= 1", "preview_key_epoch", cfg.PreviewKeyEpoch)
			os.Exit(1)
		}
		// Degrade, don't refuse to start. This worker exists for the canonical fan-out;
		// the preview is an optional rider on it, and the runtime seal already degrades
		// on the same dependency failing. Exiting here would stop message delivery for
		// every room on this site because an optional feature's key store was down --
		// history-service still serves previews from the lazy walk meanwhile.
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		if err != nil {
			slog.Error("Vault key wrapper unavailable; starting with room-preview persistence disabled",
				"addr", cfg.Vault.Address, "error", err)
		} else {
			vaultWrapper = w
			// The preview DEK lives in its own collection, and is written here on first
			// use; pin to primary so a just-minted key isn't missed on a lagging secondary.
			dekColl := db.Collection(preview.DEKCollection, options.Collection().SetReadPreference(keyReadPref))
			previewCipher = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
			slog.Info("room-preview persistence enabled", "site_id", cfg.SiteID, "key_epoch", cfg.PreviewKeyEpoch)
		}
	} else {
		slog.Info("room-preview persistence disabled (ATREST_ENABLED=false); the room doc must never hold a plaintext body")
	}
	// Cached: the lookup sits on the message path and an app's name changes about as
	// often as the app is renamed, so an uncached read per bot message bought nothing.
	//
	// Fenced INSIDE the cache, which is where the read actually happens. It is the last
	// Mongo read on the fan-out path with no tier of its own, and a stopped Mongo does
	// not error it — it stalls, for the seal's whole 2s bound, on every cold bot account.
	// Behind the breaker that becomes an instant error, and the caller already degrades
	// to the composed display name on one. Errors are deliberately not cached, so the
	// name resolves again the moment Mongo does.
	sealer := newPreviewSealer(previewCipher, preview.Key{SiteID: cfg.SiteID, Epoch: cfg.PreviewKeyEpoch},
		preview.CachedAppNameLookup(guardedAppNameLookup(newAppNameRepo(db.Collection("apps")), mongoBreaker)))

	// The buffered writer is the one MongoDB write this service still performs, and it
	// exists only while the sealer does: with no cipher there is nothing to store, and
	// the room document must never hold a plaintext body.
	var previews *previewWriter
	if cfg.PreviewFlushInterval <= 0 {
		slog.Error("PREVIEW_FLUSH_INTERVAL must be a positive duration",
			"preview_flush_interval", cfg.PreviewFlushInterval)
		os.Exit(1)
	}
	if sealer.enabled() {
		previews = newPreviewWriter(store)
	}
	if err := store.EnsureIndexes(ctx); err != nil {
		slog.Warn("ensure indexes failed; continuing (indexes are best-effort)", "error", err)
	}
	cachedStore, err := newCachedMetaStore(store, cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL)
	if err != nil {
		slog.Error("init room meta cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("room-meta-cache enabled", "size", cfg.RoomMetaCacheSize, "ttl", cfg.RoomMetaCacheTTL)
	us, err := userstore.Resilient(db.Collection("users"), mongoBreaker,
		valkeyClient, cfg.UserL2.TTL, cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)

	var keyStore roomkeystore.RoomKeyStore
	if cfg.Encryption.Enabled {
		if cfg.RoomKeyGracePeriod <= 0 {
			slog.Error("ROOM_KEY_GRACE_PERIOD must be a positive duration",
				"room_key_grace_period", cfg.RoomKeyGracePeriod)
			os.Exit(1)
		}
		// Room keys are written by other services; pin to primary so a fresh/rotated
		// key isn't missed on a lagging secondary.
		roomsForKeys := db.Collection("rooms", options.Collection().SetReadPreference(keyReadPref))
		keyStore = roomkeystore.NewMongoStore(roomsForKeys, cfg.RoomKeyGracePeriod)
	}

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

	wiring := stream.Resolve(cfg.Mode, cfg.SiteID)

	if err := bootstrapStreams(ctx, js, wiring.CanonicalStream.Name, wiring.CanonicalWildcard, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("broadcast-worker"), wiring.CanonicalWildcard)
	consumerMetrics := sharedMetrics.Consumer(natsmetrics.ConsumerConfig{
		Site:   cfg.SiteID,
		Stream: wiring.CanonicalStream.Name, Consumer: consumerCfg.Durable,
	})
	consumerMetrics.LoopStopped(ctx)
	cons, err := js.CreateOrUpdateConsumer(ctx, wiring.CanonicalStream.Name, consumerCfg)
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	publisher := &natsPublisher{nc: nc, metrics: publishMetrics}
	// The cross-site room-position announce. It used to ride the rooms.lastMsgAt
	// flush; that write is roomlist-worker's now, so it fires from the fan-out path
	// instead — same one-per-created-message coverage, and the room meta the
	// cross-site test needs is already in the handler's hand.
	var activity *roomActivityRefresher
	if peers := remotePeers(cfg.SiteID, cfg.AllSiteIDs); len(peers) > 0 {
		activity = newRoomActivityRefresher(
			roomActivityPublisher(publisher, cfg.SiteID, peers), cfg.RoomActivityRefreshInterval)
		slog.Info("cross-site room-activity refresh enabled",
			"peers", peers, "refresh_interval", cfg.RoomActivityRefreshInterval)
	} else {
		// Correct for a single-site deployment, but in a federated one it means
		// remote chat lists cannot order this site's rooms — say so rather than
		// leaving a missing ALL_SITE_IDS to look like everything is fine.
		slog.Warn("cross-site room-activity refresh disabled: no remote peers configured",
			"site", cfg.SiteID, "all_site_ids", cfg.AllSiteIDs)
	}

	// A background context, not ctx: the flush loop has to outlive the signal that
	// stops the consumer, so the last buffered batch lands after in-flight handlers
	// have drained rather than being cancelled alongside them.
	flushCtx, flushCancel := context.WithCancel(context.Background())
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		previews.Run(flushCtx, cfg.PreviewFlushInterval)
	}()
	if previews != nil {
		slog.Info("room-preview writer enabled", "flush_interval", cfg.PreviewFlushInterval)
	}

	var keyProvider RoomKeyProvider = keyStore
	var keyCache *CachedKeyProvider
	switch {
	case !cfg.Encryption.Enabled:
		// No encryption: the key provider is never consulted, leave it unwrapped.
	case cfg.RoomKeyCacheTTL <= 0 || cfg.RoomKeyCacheSize <= 0:
		slog.Info("room-key cache disabled", "ttl", cfg.RoomKeyCacheTTL, "size", cfg.RoomKeyCacheSize)
	case !keyCacheTTLSafe(cfg.RoomKeyCacheTTL, cfg.RoomKeyGracePeriod):
		// Caching beyond the grace period could serve a rotated-out key that clients can no longer decrypt; refuse to cache rather than risk it.
		slog.Warn("room-key cache disabled: TTL must be below key grace period",
			"ttl", cfg.RoomKeyCacheTTL, "grace_period", cfg.RoomKeyGracePeriod)
	case !retiredTTLSafe(cfg.RoomKeyRetiredTTL, cfg.RoomKeyCacheTTL):
		// Too short a retention breaks the client's later fetch; refuse to start.
		slog.Error("ROOM_KEY_RETIRED_TTL must be at least twice ROOM_KEY_CACHE_TTL",
			"retired_ttl", cfg.RoomKeyRetiredTTL, "cache_ttl", cfg.RoomKeyCacheTTL)
		os.Exit(1)
	default:
		keyCache = NewCachedKeyProvider(keyStore, cfg.RoomKeyCacheSize, cfg.RoomKeyCacheTTL)
		keyProvider = keyCache
		slog.Info("room-key cache enabled", "size", cfg.RoomKeyCacheSize, "ttl", cfg.RoomKeyCacheTTL)
	}

	// JetStream publish for the OUTBOX mention relay; the client fan-out stays on
	// core NATS (publisher above).
	outboxPublish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		_, err := js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data), jetstream.WithMsgID(msgID))
		if err != nil {
			destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
			publishMetrics.Failure(ctx, destination, operation, err)
			return fmt.Errorf("publish jetstream message to %s with msgID %s: %w", subj, msgID, err)
		}
		return nil
	}

	parentFetcher := newHistoryParentFetcher(nc, publishMetrics)
	// One handler per lane, differing only in how each resolves the room-route
	// mode. The home lane consults the restore tracker so it dual-publishes
	// through the window in which clients are still finding their way back; the
	// failover lane always routes global, because every client of a site whose
	// NATS is down is on some other cluster.
	homeRestores := natsutil.TrackRestores(ctx, nc)
	handler := NewHandler(cachedStore, us, publisher, keyProvider, parentFetcher, cfg.Encryption.Enabled,
		subject.NewLaneRouter(roomRouteMode, subject.LaneHome, homeRestores.RestoredAt, cfg.FailoverRevertGrace),
		withBroadcastMetrics(domainMetrics), withOutboxFederation(cfg.SiteID, outboxPublish),
		withRoomActivityRefresh(activity), withThreadViewSubject(cfg.ThreadViewSubjectEnabled),
		withPreviewSealer(sealer, previews))

	// Core-NATS queue subscriber for server-broadcast events (e.g. thread tcount badge).
	// Fire-and-forget: errors are logged inside HandleServerBroadcast; no retry path.
	broadcastSub, err := nc.QueueSubscribe(ctx, subject.ServerBroadcastWildcard(cfg.SiteID), "broadcast-worker",
		func(msgCtx context.Context, msg *nats.Msg) {
			broadcastCtx, _ := logctx.ConsumeContext(msgCtx, msg.Header, msg.Subject, msg.Data)
			handler.HandleServerBroadcast(broadcastCtx, msg.Data)
		})
	if err != nil {
		slog.Error("subscribe server-broadcast failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}
	consumerMetrics.LoopStarted(ctx)

	var wg sync.WaitGroup
	// One pool shared by both lanes: a buddy lane with its own semaphore would
	// take this service to 2xMAX_WORKERS in-flight handlers against the same
	// MongoDB and Valkey, even though the two lanes carry the same site's work.
	sem := make(chan struct{}, cfg.MaxWorkers)
	natsmetrics.StartInPool(ctx, iter, consumerMetrics, sem, consumerCfg.MaxDeliver, &wg,
		func(msg jetstream.Msg) natsmetrics.EventType { return natsmetrics.EventTypeFromSubject(msg.Subject()) },
		guardedProcessor(broadcastProcessor(handler)))

	// Buddy lane. BindBuddy never fails startup — on any failure buddyLane stays
	// nil and the service runs home-only. HasFailover gates the bot pipeline out
	// without a mode check here.
	binder := failoverlane.Binder{
		SiteID: cfg.SiteID, Buddy: cfg.Buddy,
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: cfg.MaxWorkers,
		Sem: sem, WG: &wg, Metrics: sharedMetrics,
	}
	var buddyLane *natsutil.Lane
	buddyConn := natsutil.BindBuddy(ctx, cfg.Buddy.OnlyIf(wiring.HasFailover()), cfg.NatsCredsFile,
		sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace,
		func(ctx context.Context, bconn *o11ynats.Conn, bjs o11ynats.JetStream) error {
			// The failover handler publishes on the BUDDY connection: the home
			// cluster is the one that is down, and the displaced clients this
			// broadcast is for are on the buddy.
			failoverHandler := NewHandler(cachedStore, us, &natsPublisher{nc: bconn}, keyProvider,
				parentFetcher, cfg.Encryption.Enabled,
				subject.NewLaneRouter(roomRouteMode, subject.LaneFailover, nil, cfg.FailoverRevertGrace),
				withBroadcastMetrics(domainMetrics), withOutboxFederation(cfg.SiteID, outboxPublish),
				withRoomActivityRefresh(activity), withThreadViewSubject(cfg.ThreadViewSubjectEnabled),
				withPreviewSealer(sealer, previews))
			var bErr error
			buddyLane, bErr = binder.Bind(ctx, bjs, &failoverlane.LaneSpec{
				Stream: wiring.CanonicalFailoverStream,
				Consumer: buildConsumerConfig(cfg.Consumer,
					cfg.Mode.FailoverConsumerName("broadcast-worker"), wiring.CanonicalFailoverWildcard),
			}, guardedHandler(broadcastProcessor(failoverHandler)))
			return bErr
		})

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("broadcast-worker started", "site", cfg.SiteID, "encryption", cfg.Encryption.Enabled)

	hooks := []func(context.Context) error{
		func(_ context.Context) error {
			return broadcastSub.Unsubscribe()
		},
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
		// Stop the preview writer AFTER in-flight handlers drain, so anything they
		// buffered on the way out lands in its final flush — and WAIT for that
		// flush, since Mongo is disconnected a few hooks below.
		func(ctx context.Context) error {
			flushCancel()
			select {
			case <-flushDone:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("room-preview final flush timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
	}
	if keyStore != nil {
		hooks = append(hooks, func(ctx context.Context) error { return keyStore.Close() })
	}
	if vaultWrapper != nil {
		hooks = append(hooks, func(_ context.Context) error { return vaultWrapper.Close() })
	}
	hooks = append(hooks,
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(_ context.Context) error { valkeyutil.Disconnect(valkeyClient); return nil },
		// Flush observability LAST so all prior teardown telemetry is exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)

	shutdown.Wait(ctx, 25*time.Second, hooks...)
}

// natsPublisher adapts *o11ynats.Conn to the Publisher interface.
type natsPublisher struct {
	nc      *o11ynats.Conn
	metrics natsmetrics.Publisher
}

func (p *natsPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	err := p.nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subject, data))
	p.metrics.Failure(ctx, natsmetrics.DestinationRecipientEvent, natsmetrics.OperationRecipientPublish, err)
	if err != nil {
		return fmt.Errorf("publish to %q: %w", subject, err)
	}
	return nil
}

// messageProcessor handles one consumed message, performing its own Ack/Nak.
type messageProcessor func(msgCtx context.Context, msg jetstream.Msg)

// broadcastProcessor builds the per-message processing closure: stamp the request ID, run
// the handler, then settle via jsretry (short first retry; malformed events Ack-drop).
func broadcastProcessor(handler *Handler) messageProcessor {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		// X-Migration: live events are NOT filtered here — during the legacy→new backend release
		// switch we still need broadcast to fan them out so live clients see the messages.
		handlerCtx, _ := logctx.ConsumeContext(msgCtx, msg.Headers(), msg.Subject(), msg.Data())
		// flow: hop entry with stream-wait latency time-diffing can't see. Gate the block so
		// msg.Metadata() and arg-building are skipped on the hot path (slog.Log builds args before Enabled runs).
		if logctx.Enabled(handlerCtx, logctx.LevelFlow) {
			streamWaitMs := int64(-1)
			if meta, mErr := msg.Metadata(); mErr == nil && meta != nil {
				streamWaitMs = time.Since(meta.Timestamp).Milliseconds()
			}
			slog.Log(handlerCtx, logctx.LevelFlow, "broadcast received",
				"phase", "received", "request_id", natsutil.RequestIDFromContext(handlerCtx),
				"subject", msg.Subject(), "bytes", len(msg.Data()), "stream_wait_ms", streamWaitMs)
		}
		jsretry.Settle(handlerCtx, msg, jsretry.LowLatencyBackoff, handler.HandleMessage(handlerCtx, msg.Data()))
	}
}

// guardedProcessor adapts a messageProcessor to natsmetrics.Consume, keeping
// jobguard's panic recovery in the composition so a handler panic Acks instead
// of crash-looping on JetStream redelivery. The integration test drives this
// exact composition rather than a parallel copy of the loop.
// guardedHandler is guardedProcessor for callers that hand back a plain
// jetstream.Msg — the failover binder's handler shape.
func guardedHandler(process messageProcessor) func(context.Context, jetstream.Msg) {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		jobguard.Run(msg, func() { process(msgCtx, msg) })
	}
}

func guardedProcessor(process messageProcessor) natsmetrics.ProcessMessage {
	return func(msgCtx context.Context, msg *natsmetrics.Message) {
		jobguard.Run(msg, func() { process(msgCtx, msg) })
	}
}

// buildConsumerConfig returns the durable consumer config, centralized so it's unit-testable
// without NATS; durable/filterSubject are env-driven so the binary can bind to user or bot streams.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	// Outage retry budget: fan-out that cannot complete must wait for the
	// dependency rather than drop the message after ~2.6 minutes.
	cc := stream.DurableConsumerDefaults(stream.WithOutageRetryBudget(s, jsretry.LowLatencyBackoff))
	cc.Durable = durable
	cc.FilterSubject = filterSubject
	return cc
}
