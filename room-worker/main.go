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
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	// Mode selects which stream/consumer this pod binds: "default" runs the ROOMS
	// member/create/rename ops; "teams" runs the Teams-migration room-create batch
	// off ROOMS-TEAMS. Two deploys of the same binary, gated by env only.
	Mode          string `env:"MODE"            envDefault:"default"`
	NatsURL       string `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"site-local"`
	MongoURI      string `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"  envDefault:""`
	// Pool caps the Mongo connection pool (MONGO_MAX_POOL_SIZE/MONGO_MIN_POOL_SIZE)
	// so a burst can't open unbounded connections. Env tags already carry the
	// MONGO_ prefix, so this stays a top-level field (never under envPrefix:"MONGO_").
	Pool mongoutil.PoolConfig
	// Guard bounds in-flight request handlers (MAX_CONCURRENCY) and per-request
	// duration (REQUEST_TIMEOUT) for the serverCreateDM RPC so a burst can't
	// saturate the Mongo pool with unbounded, indefinitely-held work.
	Guard             natsrouter.GuardConfig
	MaxWorkers        int                     `env:"MAX_WORKERS"        envDefault:"100"`
	KeyFanoutWorkers  int                     `env:"KEY_FANOUT_WORKERS" envDefault:"32"` // see defaultKeyFanoutWorkers in handler.go
	UserCacheSize     int                     `env:"USER_CACHE_SIZE"    envDefault:"10000"`
	UserCacheTTL      time.Duration           `env:"USER_CACHE_TTL"     envDefault:"5m"`
	RoomMetaCacheSize int                     `env:"ROOM_META_CACHE_SIZE" envDefault:"10000"`
	RoomMetaCacheTTL  time.Duration           `env:"ROOM_META_CACHE_TTL"  envDefault:"60s"`
	Consumer          stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap         bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	HealthAddr        string                  `env:"HEALTH_ADDR" envDefault:":8081"`
	PProfEnabled      bool                    `env:"PPROF_ENABLED" envDefault:"false"`
	MetricsAddr       string                  `env:"METRICS_ADDR" envDefault:":9090"`
	DebugLog          logctx.Config           `envPrefix:"DEBUG_LOG_"`

	// Grace window during which a rotated-out previous key remains valid for decrypt.
	RoomKeyGracePeriod time.Duration `env:"ROOM_KEY_GRACE_PERIOD" envDefault:"24h"`

	// RoomKeyRetiredTTL: retention for rotated-out keys; see roomkeystore.WithRetiredKeys for the 2x-cache-TTL rule.
	RoomKeyRetiredTTL time.Duration `env:"ROOM_KEY_RETIRED_TTL" envDefault:"20m"`

	// MemberCountReconcileTTL bounds how often the add-member hot path runs a
	// full O(room) recompute of userCount/appCount. Between recomputes the
	// counts are maintained incrementally ($inc by the actual delta); a full
	// reconcile runs at most once per room per TTL as a drift safety net. 0
	// forces a recompute on every add (legacy behaviour).
	MemberCountReconcileTTL time.Duration `env:"MEMBER_COUNT_RECONCILE_TTL" envDefault:"60s"`

	// Valkey backs best-effort room-meta L2 cache invalidation. Optional: when
	// VALKEY_ADDRS is empty the bust is a no-op (the L2 TTL reconciles).
	ValkeyAddrs    []string `env:"VALKEY_ADDRS"    envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`

	// Atrest/Vault drive eager at-rest DEK provisioning for synchronously-created
	// DM rooms. When Atrest.Enabled is false the DEK is created lazily by message-worker.
	Atrest atrest.Config      // env vars already prefixed ATREST_*
	Vault  atrest.VaultConfig // env vars already prefixed (VAULT_*, ATREST_VAULT_*)

	// AdminAcctPrefix overrides the platform-admin account prefix (ADMIN_ACCT_PREFIX); keep it identical across services.
	AdminAcctPrefix string `env:"ADMIN_ACCT_PREFIX" envDefault:"p_admin"`
	// RoomSubjectMode: same-site room .event namespace — global (default) | dual | local. See pkg/subject.RoomRouteMode.
	RoomSubjectMode string `env:"ROOM_SUBJECT_MODE" envDefault:"global"`
	// RoomLocalityGrace: post-flip dual-publish window. Must match across all publisher services.
	RoomLocalityGrace time.Duration `env:"ROOM_LOCALITY_GRACE" envDefault:"168h"`
}

func main() {
	logctx.SetupDefault(os.Stdout)

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
		slog.Error("invalid mongo pool config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Guard.Validate(); err != nil {
		slog.Error("invalid guard config", "error", err)
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

	sharedMetrics := natsmetrics.NewFromProvider(sdk.MeterProvider())
	publishMetrics := sharedMetrics.Publisher(cfg.SiteID)

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

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Mode, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	keyStore, err := roomkeystore.OpenMongo(ctx, mongoClient.Database(cfg.MongoDB), cfg.RoomKeyGracePeriod, cfg.RoomKeyRetiredTTL)
	if err != nil {
		slog.Error("open room key store failed", "error", err)
		os.Exit(1)
	}

	var metaValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		metaValkey, err = valkeyutil.ConnectClusterLazy(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
			valkeyutil.WithObservability(sdk),
			valkeyutil.WithRequireParentSpan(true),
			valkeyutil.WithBreakerName("room-worker"),
		)
		if err != nil {
			slog.Error("valkey connect (room-meta L2 invalidation) failed", "error", err)
			os.Exit(1)
		}
		slog.Info("room-meta L2 invalidation enabled")
	}

	keySender := roomkeysender.NewSender(nc.NatsConn(), roomkeysender.WithMetrics(publishMetrics))

	// Eager at-rest DEK provisioning for synchronously-created DM rooms (the
	// serverCreateDM path bypasses room-service's create-room flow). nil when
	// disabled; message-worker's lazy creation remains the fallback.
	var vaultWrapper atrest.KeyWrapperCloser
	var dekProvisioner DEKProvisioner
	if cfg.Atrest.Enabled {
		w, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
		if err != nil {
			slog.Error("failed to construct Vault key wrapper", "addr", cfg.Vault.Address, "error", err)
			os.Exit(1)
		}
		vaultWrapper = w
		dekColl := mongoClient.Database(cfg.MongoDB).Collection(atrest.CollectionName)
		dekProvisioner = atrest.NewCipher(w, atrest.NewMongoDEKStore(dekColl), cfg.Atrest)
	}

	// Mode picks the stream: "default" consumes ROOMS (member/create/rename ops),
	// "teams" consumes ROOMS-TEAMS (the Teams-migration room-create batch). Two
	// deploys of the same binary, gated by env only.
	streamCfg := stream.Rooms(cfg.SiteID)
	if cfg.Mode == "teams" {
		streamCfg = stream.RoomsTeams(cfg.SiteID)
	}

	store := NewMongoStore(mongoClient.Database(cfg.MongoDB))
	// User cache is on by default; a non-positive size disables it cleanly rather
	// than failing startup (the LRU constructor rejects size<=0).
	if cfg.UserCacheSize > 0 {
		if err := store.EnableUserCache(cfg.UserCacheSize, cfg.UserCacheTTL); err != nil {
			slog.Error("failed to enable user cache", "error", err)
			os.Exit(1)
		}
		slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)
	} else {
		slog.Info("user-cache disabled", "size", cfg.UserCacheSize)
	}
	// Room-meta cache is on by default; a non-positive size disables it cleanly
	// rather than failing startup (the LRU constructor rejects size<=0).
	if cfg.RoomMetaCacheSize > 0 {
		if err := store.EnableRoomMetaCache(cfg.RoomMetaCacheSize, cfg.RoomMetaCacheTTL); err != nil {
			slog.Error("failed to enable room-meta cache", "error", err)
			os.Exit(1)
		}
		slog.Info("room-meta-cache enabled", "size", cfg.RoomMetaCacheSize, "ttl", cfg.RoomMetaCacheTTL)
	} else {
		slog.Info("room-meta-cache disabled", "size", cfg.RoomMetaCacheSize)
	}
	handler := NewHandler(store, cfg.SiteID, func(ctx context.Context, subj string, data []byte, msgID string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
		if msgID == "" {
			// Ephemeral client-delivery — core NATS, not persisted.
			err := nc.PublishMsg(ctx, msg)
			publishMetrics.Attempt(ctx, destination, operation, err)
			if err != nil {
				return fmt.Errorf("publish to %q: %w", subj, err)
			}
			return nil
		}
		// JetStream-backed (MESSAGES-CANONICAL, INBOX) — block on PubAck; server honors Nats-Msg-Id for dedup.
		_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
		publishMetrics.Attempt(ctx, destination, operation, err)
		if err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}, keyStore, keySender, roomRouteMode)
	handler.SetKeyFanoutWorkers(cfg.KeyFanoutWorkers)
	// Teams room-reconcile's external-user-identity fanout (chat.hr.{siteID}.users.upsert),
	// mirroring message-worker's Teams sender-resolver publish (feat/migrated-user-fanout).
	handler.publishUsers = func(ctx context.Context, users []model.IUserWithChange) error {
		data, err := json.Marshal(users)
		if err != nil {
			return fmt.Errorf("marshal user identity fanout: %w", err)
		}
		subj := subject.OrgSyncUsersUpsert(cfg.SiteID)
		_, err = js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data))
		destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
		publishMetrics.Attempt(ctx, destination, operation, err)
		if err != nil {
			return fmt.Errorf("publish user identity fanout: %w", err)
		}
		return nil
	}
	handler.dekProvisioner = dekProvisioner
	handler.valkey = metaValkey
	handler.reconcileTTL = cfg.MemberCountReconcileTTL

	router := natsrouter.DefaultGuarded(nc, "room-worker", cfg.Guard)
	natsrouter.Register(router, subject.RoomCreateDMSync(cfg.SiteID), handler.serverCreateDM)

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	consumerCfg := buildConsumerConfig(cfg.Consumer, cfg.Mode)
	consumerMetrics := sharedMetrics.Consumer(natsmetrics.ConsumerConfig{
		Site:   cfg.SiteID,
		Stream: streamCfg.Name, Consumer: consumerCfg.Durable,
	})
	consumerMetrics.LoopStopped(ctx)
	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
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
				tracked := consumerMetrics.Track(msgCtx, msg, natsmetrics.RoomEventTypeFromSubject(msg.Subject()), consumerCfg.MaxDeliver)
				msgCtx = tracked.Context(msgCtx)
				// runJobWithRecovery contains handler panics (it Acks — drops — the
				// poison message) so this async goroutine, which runs outside
				// natsrouter's recovery middleware, can't crash the worker.
				defer func() {
					tracked.Finish(msgCtx)
					<-sem
					wg.Done()
				}()
				runJobWithRecovery(msgCtx, handler, tracked)
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

	slog.Info("room-worker running", "site", cfg.SiteID)

	// Shutdown ordering: drain inbound work first, then close client connections,
	// THEN flush observability exporters. Reverse order drops traces/metrics
	// emitted during NATS drain, mongo disconnect, and keyStore close.
	hooks := []func(ctx context.Context) error{
		func(ctx context.Context) error {
			// Mark the loop down before stopping the iterator: the Next error that
			// Stop provokes is a clean shutdown, and LoopFailed only reports a
			// terminal cause while the loop still reads as up.
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
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return keyStore.Close() },
		func(_ context.Context) error { valkeyutil.Disconnect(metaValkey); return nil },
		func(context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
	}

	// healthStop then obsShutdown LAST so all prior teardown telemetry is exported.
	hooks = append(hooks,
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)

	shutdown.Wait(ctx, 25*time.Second, hooks...)
}

// jobProcessor is the slice of the handler that the consumer goroutine drives;
// narrowing it to an interface lets runJobWithRecovery be unit-tested with a
// panicking stub (no NATS connection required).
type jobProcessor interface {
	HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg)
}

// runJobWithRecovery processes one async job and contains any panic so the
// worker survives. A panic ACKS the message (poison-pill drop) rather than
// Naking — a deterministic panic (e.g. odd-arg WithMetadata, WithCause on an
// *errcode.Error) would otherwise loop on redelivery until MaxDeliver and
// hammer the worker through every backoff. This mirrors natsrouter.Recovery,
// which Acks-on-panic with an Internal reply.
func runJobWithRecovery(msgCtx context.Context, handler jobProcessor, msg jetstream.Msg) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in async job handler — dropping (Ack)", "panic", r, "subject", msg.Subject())
			if ackErr := msg.Ack(); ackErr != nil {
				slog.Error("failed to ack after panic", "error", ackErr)
			}
		}
	}()
	// Defensive mint: room-service stamps an X-Request-ID at publish time (its
	// RequestID middleware mints one when the client omits it), so by the time a
	// message lands on the ROOMS stream the header should always be a valid UUID.
	// If we end up minting here, room-service failed to stamp one — an anomaly
	// worth an Error log, because downstream InboxDedupID / message-ID generation
	// derives dedup keys from the request ID. Note: clients that retry without a
	// stable X-Request-ID still defeat dedup upstream (room-service mints a fresh
	// ID each attempt); the boundary no longer rejects them. See
	// docs/error-handling.md §3a.
	inbound := ""
	if h := msg.Headers(); h != nil {
		inbound = h.Get(natsutil.RequestIDHeader)
	}
	id, replaced := idgen.ResolveRequestID(inbound)
	if replaced || inbound == "" {
		slog.Error("ROOMS stream message missing or invalid X-Request-ID — minting defensively; room-service should have stamped one",
			"inbound", inbound, "subject", msg.Subject())
	}
	handlerCtx := natsutil.WithRequestID(msgCtx, id)
	handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
	logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())
	handler.HandleJetStreamMsg(handlerCtx, msg)
}

// buildConsumerConfig returns the durable consumer config for the given mode:
// teams mode gets its own durable so the two deploys track independent progress
// on their separate streams. Centralized so it is unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings, mode string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "room-worker"
	if mode == "teams" {
		cc.Durable = "room-worker-teams"
	}
	return cc
}
