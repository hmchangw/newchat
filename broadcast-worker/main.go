package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/roomkeystore"
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
	NatsURL              string                  `env:"NATS_URL"                  envDefault:"nats://localhost:4222"`
	NatsCredsFile        string                  `env:"NATS_CREDS_FILE"           envDefault:""`
	SiteID               string                  `env:"SITE_ID"                   envDefault:"default"`
	MongoURI             string                  `env:"MONGO_URI"                 envDefault:"mongodb://localhost:27017"`
	MongoDB              string                  `env:"MONGO_DB"                  envDefault:"chat"`
	MongoUsername        string                  `env:"MONGO_USERNAME"            envDefault:""`
	MongoPassword        string                  `env:"MONGO_PASSWORD"            envDefault:""`
	MaxWorkers           int                     `env:"MAX_WORKERS"               envDefault:"100"`
	LastMsgFlushInterval time.Duration           `env:"LAST_MSG_FLUSH_INTERVAL"   envDefault:"250ms"`
	UserCacheSize        int                     `env:"USER_CACHE_SIZE"           envDefault:"10000"`
	UserCacheTTL         time.Duration           `env:"USER_CACHE_TTL"            envDefault:"5m"`
	RoomMetaCacheSize    int                     `env:"ROOM_META_CACHE_SIZE"      envDefault:"10000"`
	RoomMetaCacheTTL     time.Duration           `env:"ROOM_META_CACHE_TTL"       envDefault:"2m"`
	RoomKeyGracePeriod   time.Duration           `env:"ROOM_KEY_GRACE_PERIOD"     envDefault:"24h"`
	RoomKeyCacheTTL      time.Duration           `env:"ROOM_KEY_CACHE_TTL"        envDefault:"10m"`
	RoomKeyCacheSize     int                     `env:"ROOM_KEY_CACHE_SIZE"       envDefault:"50000"`
	RoomMetaL2TTL        time.Duration           `env:"ROOM_META_L2_TTL"          envDefault:"15m"`
	ValkeyAddrs          []string                `env:"VALKEY_ADDRS"              envSeparator:","`
	ValkeyPassword       string                  `env:"VALKEY_PASSWORD"           envDefault:""`
	ValkeyKeyGracePeriod time.Duration           `env:"VALKEY_KEY_GRACE_PERIOD" envDefault:"24h"`
	HealthAddr           string                  `env:"HEALTH_ADDR"              envDefault:":8081"`
	PProfEnabled         bool                    `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr          string                  `env:"METRICS_ADDR"             envDefault:":9090"`
	InputStream          string                  `env:"INPUT_STREAM,required"`
	InputSubjectFilter   string                  `env:"INPUT_SUBJECT_FILTER,required"`
	ConsumerName         string                  `env:"CONSUMER_NAME"             envDefault:"broadcast-worker"`
	Consumer             stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap            bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	Encryption           encryptionConfig        `envPrefix:"ENCRYPTION_"`
	DebugLog             logctx.Config           `envPrefix:"DEBUG_LOG_"`
	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_tchatadmin_"`
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
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), metaValkey, cfg.RoomMetaL2TTL)
	if err := store.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
	cachedStore, err := newCachedMetaStore(store, cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL)
	if err != nil {
		slog.Error("init room meta cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("room-meta-cache enabled", "size", cfg.RoomMetaCacheSize, "ttl", cfg.RoomMetaCacheTTL)
	us, err := userstore.NewCache(userstore.NewMongoStore(db.Collection("users")),
		cfg.UserCacheSize, cfg.UserCacheTTL)
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
		keyStore = roomkeystore.NewMongoStore(db.Collection("rooms"), cfg.RoomKeyGracePeriod)
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.InputStream, cfg.InputSubjectFilter, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.InputStream, buildConsumerConfig(cfg.Consumer, cfg.ConsumerName, cfg.InputSubjectFilter))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	publisher := &natsPublisher{nc: nc}
	// Coalesce per-message rooms.lastMsgAt writes into periodic BulkWrites — the handler still calls
	// UpdateRoomLastMessage; the coalescing wrapper buffers it and drains via flushCtx/Run.
	coalescer := newCoalescingStore(cachedStore, store)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	go coalescer.Run(flushCtx, cfg.LastMsgFlushInterval, 5*time.Second)
	slog.Info("last-msg coalescer enabled", "flush_interval", cfg.LastMsgFlushInterval)

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
	default:
		keyCache = NewCachedKeyProvider(keyStore, cfg.RoomKeyCacheSize, cfg.RoomKeyCacheTTL)
		keyProvider = keyCache
		slog.Info("room-key cache enabled", "size", cfg.RoomKeyCacheSize, "ttl", cfg.RoomKeyCacheTTL)
	}

	parentFetcher := newHistoryParentFetcher(nc)
	handler := NewHandler(coalescer, us, publisher, keyProvider, parentFetcher, cfg.Encryption.Enabled)

	// Core-NATS queue subscriber for server-broadcast events (e.g. thread tcount badge).
	// Fire-and-forget: errors are logged inside HandleServerBroadcast; no retry path.
	broadcastSub, err := nc.QueueSubscribe(ctx, subject.ServerBroadcastWildcard(cfg.SiteID), "broadcast-worker",
		func(msgCtx context.Context, msg *nats.Msg) {
			broadcastCtx, _ := natsutil.StampRequestID(msgCtx, msg.Header, msg.Subject)
			broadcastCtx = logctx.Admit(broadcastCtx, msg.Header)
			logctx.CapturePayload(broadcastCtx, "consumed", msg.Subject, msg.Data)
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

	var wg sync.WaitGroup
	go consumeLoop(iter, broadcastProcessor(handler), cfg.MaxWorkers, &wg)

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
		// Stop the coalescer AFTER in-flight handlers drain so any final buffered UpdateRoomLastMessage calls land in this last flush.
		func(_ context.Context) error {
			flushCancel()
			return nil
		},
		func(ctx context.Context) error { return nc.Drain() },
	}
	if keyStore != nil {
		hooks = append(hooks, func(ctx context.Context) error { return keyStore.Close() })
	}
	hooks = append(hooks,
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(_ context.Context) error { valkeyutil.Disconnect(metaValkey); return nil },
		// Flush observability LAST so all prior teardown telemetry is exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)

	shutdown.Wait(ctx, 25*time.Second, hooks...)
}

// natsPublisher adapts *o11ynats.Conn to the Publisher interface.
type natsPublisher struct {
	nc *o11ynats.Conn
}

func (p *natsPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if err := p.nc.PublishMsg(ctx, natsutil.NewMsg(ctx, subject, data)); err != nil {
		return fmt.Errorf("publish to %q: %w", subject, err)
	}
	return nil
}

// messageProcessor handles one consumed message, performing its own Ack/Nak.
type messageProcessor func(msgCtx context.Context, msg jetstream.Msg)

// messageIterator is the slice of o11y/nats MessagesContext that consumeLoop drives —
// an interface so the loop is testable against a real embedded JetStream consumer.
type messageIterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
}

// broadcastProcessor builds the per-message processing closure: stamp the request ID, run
// the handler, then settle via jsretry (short first retry; malformed events Ack-drop).
func broadcastProcessor(handler *Handler) messageProcessor {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		handlerCtx, reqID := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
		// Migrated events carry X-Migration: live — the source already delivered them, so this
		// live-delivery worker must not re-fan them out. Ack and drop without invoking the handler.
		if natsutil.IsMigrationLiveHeader(msg.Headers()) {
			slog.Info("skipping migrated event (no re-broadcast)", "subject", msg.Subject(), "request_id", reqID)
			if err := msg.Ack(); err != nil {
				slog.Error("failed to ack migrated message", "error", err, "request_id", reqID)
			}
			return
		}
		handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
		logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
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

// consumeLoop drains iter under a maxWorkers-bounded semaphore, tracking in-flight handlers on wg
// so shutdown can wait; jobguard.Run recovers handler panics to avoid an un-acked crash-loop on JetStream redelivery.
func consumeLoop(iter messageIterator, process messageProcessor, maxWorkers int, wg *sync.WaitGroup) {
	sem := make(chan struct{}, maxWorkers)
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
			jobguard.Run(msg, func() { process(msgCtx, msg) })
		}()
	}
}

// buildConsumerConfig returns the durable consumer config, centralized so it's unit-testable
// without NATS; durable/filterSubject are env-driven so the binary can bind to user or bot streams.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durable
	cc.FilterSubject = filterSubject
	return cc
}
