package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	NatsURL          string                  `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile    string                  `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID           string                  `env:"SITE_ID"         envDefault:"site-local"`
	MongoURI         string                  `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB          string                  `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername    string                  `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword    string                  `env:"MONGO_PASSWORD"  envDefault:""`
	MaxWorkers       int                     `env:"MAX_WORKERS"        envDefault:"100"`
	KeyFanoutWorkers int                     `env:"KEY_FANOUT_WORKERS" envDefault:"32"` // see defaultKeyFanoutWorkers in handler.go
	Consumer         stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap        bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	DebugLog         logctx.Config           `envPrefix:"DEBUG_LOG_"`

	// Grace window during which a rotated-out previous key remains valid for decrypt.
	RoomKeyGracePeriod time.Duration `env:"ROOM_KEY_GRACE_PERIOD" envDefault:"24h"`

	// Valkey backs best-effort room-meta L2 cache invalidation. Optional: when
	// VALKEY_ADDRS is empty the bust is a no-op (the L2 TTL reconciles).
	ValkeyAddrs    []string `env:"VALKEY_ADDRS"    envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`

	// Atrest/Vault drive eager at-rest DEK provisioning for synchronously-created
	// DM rooms. When Atrest.Enabled is false the DEK is created lazily by message-worker.
	Atrest atrest.Config      // env vars already prefixed ATREST_*
	Vault  atrest.VaultConfig // env vars already prefixed (VAULT_*, ATREST_VAULT_*)
}

func main() {
	logctx.SetupDefault(os.Stdout)

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	if cfg.RoomKeyGracePeriod <= 0 {
		slog.Error("ROOM_KEY_GRACE_PERIOD must be a positive duration",
			"room_key_grace_period", cfg.RoomKeyGracePeriod)
		os.Exit(1)
	}

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "room-worker")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}

	meterShutdown, err := otelutil.InitMeter("room-worker")
	if err != nil {
		slog.Error("init meter failed", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	keyStore := roomkeystore.NewMongoStore(mongoClient.Database(cfg.MongoDB).Collection("rooms"), cfg.RoomKeyGracePeriod)

	var metaValkey valkeyutil.Client
	if len(cfg.ValkeyAddrs) > 0 {
		metaValkey, err = valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
		if err != nil {
			slog.Error("valkey connect (room-meta L2 invalidation) failed", "error", err)
			os.Exit(1)
		}
		slog.Info("room-meta L2 invalidation enabled")
	}

	keySender := roomkeysender.NewSender(nc.NatsConn())

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

	streamCfg := stream.Rooms(cfg.SiteID)

	store := NewMongoStore(mongoClient.Database(cfg.MongoDB))
	handler := NewHandler(store, cfg.SiteID, func(ctx context.Context, subj string, data []byte, msgID string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		if msgID == "" {
			// Ephemeral client-delivery — core NATS, not persisted.
			if err := nc.PublishMsg(ctx, msg); err != nil {
				return fmt.Errorf("publish to %q: %w", subj, err)
			}
			return nil
		}
		// JetStream-backed (MESSAGES_CANONICAL, OUTBOX) — block on PubAck; server honors Nats-Msg-Id for dedup.
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}, keyStore, keySender)
	handler.SetKeyFanoutWorkers(cfg.KeyFanoutWorkers)
	handler.dekProvisioner = dekProvisioner
	handler.valkey = metaValkey

	router := natsrouter.New(nc, "room-worker")
	router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Logging())
	natsrouter.Register(router, subject.RoomCreateDMSync(cfg.SiteID), handler.serverCreateDM)

	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, buildConsumerConfig(cfg.Consumer))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(2 * cfg.MaxWorkers))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	sem := make(chan struct{}, cfg.MaxWorkers)
	var wg sync.WaitGroup

	go func() {
		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				// recover() must run BEFORE the slot release so a panicking handler
				// (e.g. a WithCause/WithMetadata misuse) Naks and is redelivered
				// instead of crashing the worker — the async path runs outside
				// natsrouter's recovery middleware.
				defer func() {
					<-sem
					wg.Done()
				}()
				runJobWithRecovery(msgCtx, handler, msg)
			}()
		}
	}()

	slog.Info("room-worker running", "site", cfg.SiteID)

	// Shutdown ordering: drain inbound work first, then close client connections,
	// THEN flush observability exporters. Reverse order drops traces/metrics
	// emitted during NATS drain, mongo disconnect, and keyStore close.
	hooks := []func(ctx context.Context) error{
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
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return keyStore.Close() },
		func(_ context.Context) error { valkeyutil.Disconnect(metaValkey); return nil },
		func(context.Context) error {
			if vaultWrapper != nil {
				return vaultWrapper.Close()
			}
			return nil
		},
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return meterShutdown(ctx) },
	}

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
	// worth an Error log, because downstream OutboxDedupID / message-ID generation
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

// buildConsumerConfig returns the durable consumer config for
// room-worker. Centralized so it is unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "room-worker"
	return cc
}
