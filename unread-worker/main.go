package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL       string `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"default"`
	MongoURI      string `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"  envDefault:""`
	// No MONGO_READ_PREFERENCE: this service only writes, and writes always go
	// to the primary.
	//
	// MongoSelectTimeout bounds server selection, matching every other online
	// service. A stopped MongoDB does not error, it goes quiet, and the driver
	// waits 30s by default — long enough for a flush to outlive the 25s
	// shutdown budget and be SIGKILLed mid-batch. Bounding it turns the stall
	// into the NakWithDelay this worker is built around, so back-pressure
	// engages promptly instead of a flush goroutine parking on a dead socket.
	MongoSelectTimeout time.Duration `env:"MONGO_SERVER_SELECTION_TIMEOUT" envDefault:"2s"`
	FlushInterval      time.Duration `env:"FLUSH_INTERVAL" envDefault:"250ms"`
	// FlushTimeout bounds ONE periodic flush. Run drives Flush synchronously, so
	// an unbounded write stalls every later flush too. Keep it below
	// CONSUMER_ACK_WAIT (default 30s) or a wedged flush still outlives the ack
	// deadline and the batch redelivers underneath it; main validates that.
	FlushTimeout time.Duration `env:"FLUSH_TIMEOUT"  envDefault:"10s"`
	HealthAddr   string        `env:"HEALTH_ADDR"    envDefault:":8081"`
	MetricsAddr  string        `env:"METRICS_ADDR"   envDefault:":9090"`
	PProfEnabled bool          `env:"PPROF_ENABLED"  envDefault:"false"`
	// Mode selects the canonical stream/subject wiring via pkg/stream.Resolve.
	Mode      stream.Pipeline         `env:"MODE,required"`
	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	DebugLog  logctx.Config           `envPrefix:"DEBUG_LOG_"`
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
	// redelivers the batch underneath it.
	if err := validateFlushBudget(cfg.FlushInterval, cfg.FlushTimeout, cfg.Consumer.AckWait); err != nil {
		slog.Error("invalid flush configuration", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithObservability(sdk),
		mongoutil.WithServerSelectionTimeout(cfg.MongoSelectTimeout))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
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

	cons, err := js.CreateOrUpdateConsumer(ctx, wiring.CanonicalStream.Name,
		buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("unread-worker"), wiring.CanonicalWildcard))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	f := newFlusher(store)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	flushDone := make(chan struct{})
	go func() { f.Run(flushCtx, cfg.FlushInterval, cfg.FlushTimeout); close(flushDone) }()
	slog.Info("unread-state flusher started",
		"flush_interval", cfg.FlushInterval, "flush_timeout", cfg.FlushTimeout)

	// PullMaxMessages is bounded by MaxAckPending anyway; a modest buffer keeps
	// the single consume goroutine fed without over-fetching during an outage.
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(256))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	// Armed BEFORE the consume loop starts, because the loop can raise this
	// signal itself the moment it fails. Until signal.Notify runs, SIGTERM keeps
	// its default disposition and would kill the process outright rather than
	// run the graceful path — and a signal raised before the handler exists is
	// lost to it. The buffered channel latches an early one for shutdown.WaitOn.
	// No signal.Stop: the registration must outlive every path that could raise
	// it, and main returning is the process ending anyway.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	consume := consumeState{onUnexpectedStop: requestSelfShutdown}
	wg.Add(1)
	go consumeLoop(iter, f, &wg, &consume)

	// The consume-loop check sits alongside the NATS one deliberately: NATS stays
	// healthy in precisely the failure where the loop has died, so probing the
	// connection alone cannot detect a worker that has stopped doing its job.
	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
		consume.Check(),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("unread-worker started", "site", cfg.SiteID, "mode", string(cfg.Mode))

	shutdown.WaitOn(ctx, sig, 25*time.Second,
		// Mark the stop as intended BEFORE iter.Stop(), so the consume loop's
		// exit is not mistaken for a failure and does not re-signal a process
		// that is already on its way down.
		func(_ context.Context) error { consume.beginShutdown(); return nil },
		func(_ context.Context) error { iter.Stop(); return nil },
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("consume loop drain timed out: %w", ctx.Err())
			}
		},
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

// consumeState tracks whether the consume loop is still running, so /readyz can
// answer the question that actually matters for this worker: is it consuming?
//
// It exists because the loop returns on any iterator error and nothing else
// observes that — the only wg.Wait() is the shutdown drain. Without this the pod
// keeps reporting ready on a healthy NATS connection while writing nothing at
// all, which is the failure mode hardest to notice and slowest to diagnose.
// Readiness alone cannot recover this: nothing routes traffic to a queue
// worker, and liveness always answers 200 by design, so a 503 on /readyz makes
// a dead consumer visible without ever replacing it. onUnexpectedStop is what
// closes that gap — it asks the process to terminate so the supervisor starts a
// fresh one, which is the only actor able to rebuild the iterator.
type consumeState struct {
	reason   atomic.Pointer[error]
	stopping atomic.Bool
	// onUnexpectedStop runs when the loop stops for any reason other than a
	// shutdown already under way. Tests substitute a recorder.
	onUnexpectedStop func()
}

// beginShutdown marks the stop that follows as intended, so tearing the worker
// down does not look like a failure and re-signal a process already exiting.
func (s *consumeState) beginShutdown() { s.stopping.Store(true) }

func (s *consumeState) stopped(err error) {
	s.reason.Store(&err)
	if s.stopping.Load() || s.onUnexpectedStop == nil {
		return
	}
	s.onUnexpectedStop()
}

// requestSelfShutdown raises SIGTERM on this process so shutdown.Wait runs the
// ordinary graceful teardown — draining the final flush rather than abandoning
// the batch, which os.Exit here would do.
func requestSelfShutdown() {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		slog.Error("cannot signal self after consume loop stopped; the pod will stay up and idle", "error", err)
		return
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		slog.Error("cannot signal self after consume loop stopped; the pod will stay up and idle", "error", err)
	}
}

// validateFlushBudget rejects a configuration in which a held message can
// outlive its ack deadline.
//
// The budget is 2*timeout+interval. Run drives Flush SYNCHRONOUSLY, so the
// worst case for a message is three waits, not one: the flush already running
// when it arrives can burn a full timeout, the loop then waits out the ticker
// for up to interval, and only then does its own flush run for up to another
// timeout. Charging a single timeout understates that by a whole flush and
// admits configs — FLUSH_TIMEOUT=20s against a 30s AckWait, say — where
// JetStream redelivers the batch from underneath a flush that is still writing.
func validateFlushBudget(interval, timeout, ackWait time.Duration) error {
	if budget := 2*timeout + interval; budget >= ackWait {
		return fmt.Errorf("2 × FLUSH_TIMEOUT (%s) + FLUSH_INTERVAL (%s) = %s must be less than CONSUMER_ACK_WAIT (%s), or a held message outlives its ack deadline and redelivers while its flush is still running",
			timeout, interval, budget, ackWait)
	}
	return nil
}

// Check reports not-ready once the loop has stopped. The readiness contract is
// "can this pod do its job", and a worker whose only goroutine has exited
// cannot, whatever the reason.
func (s *consumeState) Check() health.Check {
	return health.Check{Name: "consume-loop", Probe: func(context.Context) error {
		if err := s.reason.Load(); err != nil {
			return fmt.Errorf("consume loop stopped: %w", *err)
		}
		return nil
	}}
}

// consumeLoop drains iter into the flusher. It is a single goroutine with no
// worker pool: the per-message work is a sonic unmarshal plus a regex parse and
// a map merge with no I/O, so concurrency would only add contention on the
// batch mutex. Messages are NOT settled here — the flusher settles them once
// their batch reaches MongoDB.
// state records why the loop stopped so readiness can reflect it; see
// consumeState.
func consumeLoop(iter messageIterator, f *flusher, wg *sync.WaitGroup, state *consumeState) {
	defer wg.Done()
	for {
		msgCtx, msg, err := iter.Next()
		if err != nil {
			// Every exit is recorded, including the deliberate iter.Stop() during
			// shutdown: a worker that has stopped consuming is not ready either
			// way, and during shutdown failing readiness is what drains it from
			// the load balancer.
			state.stopped(err)
			slog.Error("unread-worker consume loop stopped; no further unread state will be written",
				"error", err)
			return
		}
		// jobguard recovers a panic from the derive/add path so a single
		// malformed-but-parseable event can't crash this goroutine. With
		// MaxDeliver=-1 an un-acked in-flight message would otherwise
		// redeliver forever after every pod restart, turning a deterministic
		// panic into a crash loop with no way for the message to fall out of
		// the stream. On panic the message stays un-acked and JetStream
		// redelivers it after AckWait, same as any other transient failure.
		jobguard.Guard(msg.Subject(), func() {
			handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
			handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
			logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())

			var evt eventProjection
			if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
				// Malformed payload — it will never parse on redelivery. Settle it
				// immediately rather than holding it for a flush it can't join.
				jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff,
					errcode.Permanent(errcode.BadRequest("malformed message event")))
				return
			}
			handlerCtx = obs.ContextWithIdentity(handlerCtx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)
			f.add(deriveIntents(&evt), heldMsg{ctx: handlerCtx, msg: msg})
		})
	}
}

// buildConsumerConfig returns the durable consumer config, centralized so it's
// unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durable
	cc.FilterSubject = filterSubject
	// Unlimited redelivery: a MongoDB outage must not exhaust MaxDeliver and
	// silently drop badges. Poison messages are handled by classifyFlushErr
	// Ack-dropping server-rejected writes, not by a delivery cap.
	cc.MaxDeliver = -1
	// New (not All): these writes are derived from live traffic. Replaying the
	// whole canonical stream on first deploy would re-apply historical writes as
	// one large burst for no benefit. DeliverPolicy is honored only at creation.
	cc.DeliverPolicy = jetstream.DeliverNewPolicy
	return cc
}
