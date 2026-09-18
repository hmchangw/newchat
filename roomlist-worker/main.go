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
	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/failoverlane"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"default"`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"  envDefault:""`
	// No MONGO_READ_PREFERENCE: this service only writes, and writes always go
	// to the primary.
	//
	// Pool carries the fleet's connection ceiling and, in
	// Pool.ServerSelectionTimeout, the bound that matters most here: a stopped
	// MongoDB does not error, it goes quiet, and the driver waits 30s by default
	// — long enough for a flush to outlive the 25s shutdown budget and be
	// SIGKILLed mid-batch. Bounding it turns the stall into the NakWithDelay
	// this worker is built around, so back-pressure engages promptly instead of
	// a flush goroutine parking on a dead socket.
	Pool          mongoutil.PoolConfig
	FlushInterval time.Duration `env:"FLUSH_INTERVAL" envDefault:"250ms"`
	// FlushTimeout bounds ONE periodic flush. Run drives Flush synchronously, so
	// an unbounded write stalls every later flush too. Keep it below
	// CONSUMER_ACK_WAIT (default 30s) or a wedged flush still outlives the ack
	// deadline and the batch redelivers underneath it; main validates that.
	FlushTimeout time.Duration `env:"FLUSH_TIMEOUT"  envDefault:"10s"`
	HealthAddr   string        `env:"HEALTH_ADDR"    envDefault:":8081"`
	PProfEnabled bool          `env:"PPROF_ENABLED"  envDefault:"false"`
	// Mode selects the canonical stream/subject wiring via pkg/stream.Resolve.
	Mode      stream.Pipeline         `env:"MODE,required"`
	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	// Buddy is the peer cluster hosting this site's standby lanes. When set,
	// the worker also consumes MESSAGES-CANONICAL-FAILOVER there, so room and
	// subscription state keeps moving while this site's own NATS is down.
	Buddy    natsutil.BuddyConfig `envPrefix:"BUDDY_"`
	DebugLog logctx.Config        `envPrefix:"DEBUG_LOG_"`
}

func main() {
	logctx.SetupDefault(os.Stdout)
	pretouchJSON()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	// Fail fast rather than run with a flush that can outlive the ack deadline:
	// the whole point of bounding the flush is that it gives up BEFORE JetStream
	// redelivers the batch underneath it. EffectiveAckWait, not the configured
	// field, because the server enforces BackOff[0] when a schedule is derived.
	if err := validateFlushBudget(cfg.FlushInterval, cfg.FlushTimeout, cfg.Consumer.EffectiveAckWait()); err != nil {
		slog.Error("invalid flush configuration", "error", err)
		os.Exit(1)
	}
	// WithPool applies these below, but only Validate rejects the values that
	// quietly disable the protection: a zero max pool size (the driver reads it
	// as unbounded) and a negative server-selection timeout (read as no bound at
	// all — the stopped-Mongo hang this worker's back-pressure depends on
	// avoiding).
	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("invalid mongo pool configuration", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	// Majority, enforced here rather than left to the URI: the flusher acks a
	// batch's JetStream messages once BulkWrite returns, so a write that a
	// primary failover rolls back is one no redelivery will ever replace. The
	// room-list state it carried — badges, lastMsgAt, lastSeenAt — is simply lost.
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk),
		mongoutil.WithWriteConcern(writeconcern.Majority()))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))

	wiring := stream.Resolve(cfg.Mode, cfg.SiteID)
	// The bot pipeline has no standby stream, so HasFailover gates the buddy
	// lane out there; it also keeps the home dial fail-fast, since without a
	// buddy a pod that cannot reach home has nothing to do.
	dialer := natsutil.NewBuddyDialer(cfg.Buddy.OnlyIf(wiring.HasFailover()), cfg.NatsCredsFile, sdk)
	// Lazy home dial; see natsutil.BuddyDialer.ConnectHome.
	nc, js, err := dialer.ConnectHomeJS(ctx, cfg.NatsURL, nil)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	// Four times the ack-pending ceiling: high enough that ordinary traffic —
	// where a message carries a handful of mentions — never trips it, low enough
	// that the pathological case drains long before BulkSetMentions grows past
	// what one flush can write. Derived rather than a separate knob so it cannot
	// drift from the bound it exists to complement.
	f := newFlusher(store, 4*cfg.Consumer.MaxAckPending, cfg.FlushTimeout)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	flushDone := make(chan struct{})
	go func() { f.Run(flushCtx, cfg.FlushInterval, cfg.FlushTimeout); close(flushDone) }()
	slog.Info("room-list state flusher started",
		"flush_interval", cfg.FlushInterval, "flush_timeout", cfg.FlushTimeout)

	// Armed BEFORE either consume loop starts, because a loop can raise SIGTERM
	// on this process the moment it fails (pkg/loopguard).
	sig := shutdown.Signals()

	var wg sync.WaitGroup
	// One guard per lane, both built before the binds. The home bind is deferred
	// until its cluster is reachable and the buddy bind can fail outright, so a
	// readiness row created only on success would not cover the window before it
	// — and a guard whose loop never starts reads ready, which is what keeps an
	// undialled buddy home-only instead of permanently unready.
	homeConsume := loopguard.New("consume-loop-home", loopguard.SelfShutdown)
	buddyConsume := loopguard.New("consume-loop-buddy", loopguard.SelfShutdown)

	// Home lane, bound once the home connection is up — immediately in the
	// ordinary case, later if the pod booted during an outage. Dev-only stream
	// bootstrap rides inside for the same reason: it needs the server too.
	homeLane, err := natsutil.BindWhenConnected(ctx, nc, wiring.CanonicalStream.Name, func(ctx context.Context) (func(), error) {
		if err := bootstrapStreams(ctx, js, wiring.CanonicalStream.Name, wiring.CanonicalWildcard, cfg.Bootstrap.Enabled); err != nil {
			return nil, fmt.Errorf("bootstrap streams: %w", err)
		}
		cons, err := js.CreateOrUpdateConsumer(ctx, wiring.CanonicalStream.Name,
			buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("roomlist-worker"), wiring.CanonicalWildcard))
		if err != nil {
			return nil, fmt.Errorf("create consumer: %w", err)
		}
		// PullMaxMessages is bounded by MaxAckPending anyway; a modest buffer
		// keeps the single consume goroutine fed without over-fetching during an
		// outage.
		iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(pullBatch))
		if err != nil {
			return nil, fmt.Errorf("messages: %w", err)
		}
		wg.Add(1)
		go consumeLoop(iter, f, &wg, homeConsume)
		return iter.Stop, nil
	})
	if err != nil {
		slog.Error("bind home lane failed", "error", err)
		os.Exit(1)
	}

	// Buddy lane: the same body against the same flusher, fed from the standby
	// canonical stream on the buddy. Never fails startup — on any failure
	// buddyLane stays nil and the worker runs home-only. A one-slot pool keeps
	// it sequential like the home loop; the flusher is the serialization point
	// between the two lanes, and it is mutex-guarded.
	//
	// OnLoopStop is what closes the gap this worker used to have: its dead-loop
	// detector watched the home loop only, so a buddy iterator that died
	// mid-outage stopped applying room state while readiness still passed.
	binder := failoverlane.Binder{
		SiteID: cfg.SiteID, Dialer: dialer,
		Bootstrap: cfg.Bootstrap.Enabled, MaxWorkers: pullBatch / 2,
		Sem: make(chan struct{}, 1), WG: &wg, OnLoopStop: buddyConsume.Stopped,
	}
	buddyLane, buddyConn := binder.BindLane(ctx, failoverLaneSpec(cfg.Consumer, cfg.Mode, &wiring),
		func(context.Context, *o11ynats.Conn, o11ynats.JetStream, subject.Lane) (func(context.Context, jetstream.Msg), error) {
			return canonicalHandler(f), nil
		})

	// The consume-loop check sits alongside the NATS one deliberately: NATS stays
	// healthy in precisely the failure where the loop has died, so probing the
	// connection alone cannot detect a worker that has stopped doing its job.
	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		natsutil.LanesCheck(homeLane.Ready, buddyLane.Bound),
		homeConsume.Check(),
		buddyConsume.Check(),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("roomlist-worker started", "site", cfg.SiteID, "mode", string(cfg.Mode))

	shutdown.WaitOn(ctx, sig, 25*time.Second,
		// Mark the stop as intended BEFORE iter.Stop(), so the consume loop's
		// exit is not mistaken for a failure and does not re-signal a process
		// that is already on its way down.
		// Both guards before either Stop, or a clean teardown reads as a death
		// and re-signals a process already on its way out.
		func(_ context.Context) error {
			homeConsume.BeginShutdown()
			buddyConsume.BeginShutdown()
			return nil
		},
		// Both iterators stop before the drain below, so neither lane pulls new
		// work while the other is still finishing. Both feed one WaitGroup.
		func(_ context.Context) error { homeLane.Stop(); buddyLane.Stop(); return nil },
		func(ctx context.Context) error { return natsutil.WaitPool(ctx, &wg) },
		// Stop the flusher only after the consume loop drains, so the final
		// flush includes every intent it added.
		func(ctx context.Context) error {
			flushCancel()
			select {
			case <-flushDone:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("final flush timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		natsutil.DrainBuddy(buddyConn),
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		// Flush observability LAST so all prior teardown telemetry is exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// messageIterator is the slice of the o11y/nats MessagesContext the consume loop
// drives — an interface so the loop is testable without a live consumer.
type messageIterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
}

// validateFlushBudget rejects a configuration in which a held message can
// outlive its ack deadline. ackWait is the deadline the server will enforce
// (stream.ConsumerSettings.EffectiveAckWait), not necessarily the configured one.
//
// The budget is 2*timeout+interval. Run drives Flush SYNCHRONOUSLY, so the
// worst case for a message is three waits, not one: the flush already running
// when it arrives can burn a full timeout, the loop then waits out the ticker
// for up to interval, and only then does its own flush run for up to another
// timeout. Charging a single timeout understates that by a whole flush and
// admits configs — FLUSH_TIMEOUT=20s against a 30s AckWait, say — where
// JetStream redelivers the batch from underneath a flush that is still writing.
// Each duration must be positive before the arithmetic means anything. A
// non-positive value passes the budget comparison trivially — 2*0+0 is under
// any AckWait, and a negative timeout drags the budget below zero — and then
// fails at runtime, which is the one outcome this function exists to prevent:
// a non-positive interval makes flushloop.Run log and return without ever
// starting, and a non-positive timeout hands every flush an already-expired
// context — either way no batch ever lands, nothing is ever acked, and the
// consumer stalls silently with MaxDeliver=-1 holding the messages.
func validateFlushBudget(interval, timeout, ackWait time.Duration) error {
	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"FLUSH_INTERVAL", interval},
		{"FLUSH_TIMEOUT", timeout},
		{"CONSUMER_ACK_WAIT", ackWait},
	} {
		if d.value <= 0 {
			return fmt.Errorf("%s must be positive, got %s", d.name, d.value)
		}
	}
	if budget := 2*timeout + interval; budget >= ackWait {
		return fmt.Errorf("2 × FLUSH_TIMEOUT (%s) + FLUSH_INTERVAL (%s) = %s must be less than CONSUMER_ACK_WAIT (%s), or a held message outlives its ack deadline and redelivers while its flush is still running",
			timeout, interval, budget, ackWait)
	}
	return nil
}

// consumeLoop drains iter into the flusher. It is a single goroutine with no
// worker pool: the per-message work is a sonic unmarshal plus a regex parse and
// a map merge with no I/O, so concurrency would only add contention on the
// batch mutex. Messages are NOT settled here — the flusher settles them once
// their batch reaches MongoDB.
// state records why the loop stopped so readiness can reflect it and, on an
// unexpected stop, asks the process to restart; see pkg/loopguard.
func consumeLoop(iter messageIterator, f *flusher, wg *sync.WaitGroup, state *loopguard.Guard) {
	defer wg.Done()
	handle := canonicalHandler(f)
	for {
		msgCtx, msg, err := iter.Next()
		if err != nil {
			// Every exit is recorded, including the deliberate iter.Stop() during
			// shutdown: a worker that has stopped consuming is not ready either
			// way, and during shutdown failing readiness is what drains it from
			// the load balancer.
			if natsmetrics.Recoverable(err) {
				slog.Warn("consume loop stalled; retrying", "loop", "consume-loop", "error", err)
				continue
			}
			state.Stopped(err)
			return
		}
		handle(msgCtx, msg)
	}
}

// pullBatch is both lanes' PullMaxMessages: bounded by MaxAckPending anyway, a
// modest buffer keeps a lane fed without over-fetching during an outage.
const pullBatch = 256

// canonicalHandler is the per-message body both lanes run against one flusher.
// The room state a failover-lane message derives is this site's, written to this
// site's Mongo, exactly as on the home lane; only the stream it arrived on differs.
func canonicalHandler(f *flusher) func(context.Context, jetstream.Msg) {
	return func(msgCtx context.Context, msg jetstream.Msg) {
		// jobguard recovers a panic from the derive/add path so a single
		// malformed-but-parseable event can't crash this goroutine. With
		// MaxDeliver=-1 an un-acked in-flight message would otherwise
		// redeliver forever after every pod restart, turning a deterministic
		// panic into a crash loop with no way for the message to fall out of
		// the stream. On panic the message stays un-acked and JetStream
		// redelivers it after AckWait, same as any other transient failure.
		jobguard.Guard(msg.Subject(), func() {
			handlerCtx, _ := logctx.ConsumeContext(msgCtx, msg.Headers(), msg.Subject(), msg.Data())

			var evt eventProjection
			if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
				// Malformed payload — it will never parse on redelivery. Settle it
				// immediately rather than holding it for a flush it can't join.
				jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff,
					errcode.Permanent(errcode.BadRequest("malformed message event")))
				return
			}
			handlerCtx = obs.ContextWithIdentity(handlerCtx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)
			// Drain inline when the batch reaches its mention budget rather than
			// waiting out the interval: the cost lands on this goroutine, which
			// is the back-pressure that keeps the unbounded map bounded.
			if f.add(deriveIntents(&evt), heldMsg{ctx: handlerCtx, msg: msg}) {
				f.flushNow(handlerCtx)
			}
		})
	}
}

// failoverLaneSpec is the durable on the buddy-hosted MESSAGES-CANONICAL-FAILOVER
// stream. Its own durable, so the two lanes keep independent cursors, with the
// home lane's unlimited redelivery: a MongoDB outage during a NATS outage must
// still park messages rather than drop room state.
func failoverLaneSpec(s stream.ConsumerSettings, mode stream.Pipeline, w *stream.Wiring) *failoverlane.BuddyLane {
	return &failoverlane.BuddyLane{
		Stream:   w.CanonicalFailoverStream,
		Consumer: buildConsumerConfig(s, mode.FailoverConsumerName("roomlist-worker"), w.CanonicalFailoverWildcard),
	}
}

// buildConsumerConfig returns the durable consumer config, centralized so it's
// unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	// Unlimited redelivery: a MongoDB outage must not exhaust MaxDeliver and
	// silently drop badges. Poison messages are handled by classifyFlushErr
	// Ack-dropping server-rejected writes, not by a delivery cap.
	cc := stream.DurableConsumerDefaults(stream.WithUnlimitedRedelivery(s))
	cc.Durable = durable
	cc.FilterSubject = filterSubject
	// New (not All): these writes are derived from live traffic. Replaying the
	// whole canonical stream on first deploy would re-apply historical writes as
	// one large burst for no benefit. DeliverPolicy is honored only at creation.
	cc.DeliverPolicy = jetstream.DeliverNewPolicy
	return cc
}
