package jsiter

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/health"
)

// ConsumeContext is the running-consumption handle Consume returns. Both
// jetstream's and the o11y wrapper's ConsumeContext satisfy it.
type ConsumeContext interface {
	Stop()
}

// NewConsume starts consumption and reports later asynchronous failures through
// onError. Implementations must pass onError to jetstream.ConsumeErrHandler —
// without one, a Consume that the server stops reports nothing at all: the
// handler simply never fires again.
type NewConsume func(ctx context.Context, onError func(error)) (ConsumeContext, error)

// Supervisor keeps a callback-style Consume running.
//
// Consume has the same split as the pull iterator: a missed idle heartbeat
// leaves consumption running and needs nothing, while a status like "consumer
// deleted" makes nats.go stop the subscription outright (pull.go:718-722,
// :285). Nothing surfaces on that path, so the service goes on looking healthy
// while its handler is never called again — the stall lasts until someone
// restarts the pod. Supervisor watches the error handler and restarts the
// subscription for exactly the failures that killed it.
type Supervisor struct {
	name  string
	ctx   context.Context
	start NewConsume

	// sleepFn parks for one backoff step, reporting false when Stop or context
	// cancellation cut the wait short. It is a field so tests can drive the
	// restart path without real delays.
	sleepFn func(context.Context, time.Duration) bool

	// nowFn stamps the transient run; a field so tests can drive the window
	// without real time.
	nowFn func() time.Time

	mu sync.Mutex
	// gen identifies the current subscription round. Every onError callback
	// carries the round it was wired for, so an error still buffered on a
	// superseded subscription cannot tear down its replacement.
	gen        uint64
	failedGen  uint64
	cur        ConsumeContext
	transients int
	lastTrans  time.Time
	restarting bool

	up      atomic.Bool
	stopped atomic.Bool
	done    chan struct{}
	once    sync.Once
}

// transientWindow bounds the back-to-back run transientEscalation counts.
// Consume gives no progress signal — messages reach the caller's handler, not
// the error handler — so the run is bounded by time instead: misses that are
// part of one stall arrive together, while unrelated ones must not accumulate
// until they tear down a healthy subscription.
const transientWindow = 2 * time.Minute

// Supervise starts consumption and returns the supervisor watching it. A
// failure here is a startup failure — the caller decides whether to exit.
//
// ctx bounds every later restart, so pass the service context.
func Supervise(ctx context.Context, name string, start NewConsume) (*Supervisor, error) {
	s := &Supervisor{name: name, ctx: ctx, start: start, done: make(chan struct{})}
	s.sleepFn = s.sleep
	s.nowFn = time.Now

	if err := s.begin(); err != nil {
		// A round that died during start spawned a restart before begin could
		// report the failure. The caller is about to discard this supervisor, so
		// release that goroutine rather than leave it retrying in the dark.
		s.Stop()
		return nil, err
	}
	return s, nil
}

// IsUp reports whether consumption is currently running. It goes false while a
// restart is in flight and after Stop.
func (s *Supervisor) IsUp() bool { return s.up.Load() }

// HealthCheck reports the supervisor's state as a dependency probe. Wire it next
// to the NATS connection check: the connection being up says nothing about
// whether this consumer is still delivering, which is the failure that hides.
func (s *Supervisor) HealthCheck() health.Check {
	return health.Check{
		Name: "jetstream-consumer:" + s.name,
		Probe: func(context.Context) error {
			if !s.IsUp() {
				return fmt.Errorf("%s consumption is down", s.name)
			}
			return nil
		},
	}
}

// Stop ends supervision and the consumption it is watching. It is idempotent
// and unblocks a restart parked on backoff, so it is safe in a shutdown hook.
func (s *Supervisor) Stop() {
	s.once.Do(func() {
		s.stopped.Store(true)
		s.up.Store(false)
		close(s.done)

		s.mu.Lock()
		cur := s.cur
		s.mu.Unlock()
		if cur != nil {
			cur.Stop()
		}
	})
}

// begin starts one round of consumption and installs its handle.
//
// A status error can reach observe while start is still returning, before the
// handle exists. observe marks the round instead of acting on a nil handle, and
// begin drops the subscription here — otherwise the doomed one would keep
// running beside its replacement, which on a MaxAckPending=1 lane means two
// consumers on a durable that must stay strictly sequential.
func (s *Supervisor) begin() error {
	s.mu.Lock()
	s.gen++
	gen := s.gen
	s.mu.Unlock()

	cc, err := s.start(s.ctx, func(err error) { s.observe(gen, err) })
	if err != nil {
		return fmt.Errorf("start %s consumption: %w", s.name, err)
	}

	s.mu.Lock()
	failed := s.failedGen == gen
	if !failed {
		s.cur = cc
		// Published under the lock observe takes, so a failure recorded for this
		// round cannot be overtaken by this store.
		s.up.Store(true)
	}
	s.mu.Unlock()

	if failed {
		cc.Stop()
		return fmt.Errorf("start %s consumption: subscription failed while starting", s.name)
	}
	// Stop sets stopped before up, so re-reading it here cannot miss a Stop that
	// already flipped up to false.
	if s.stopped.Load() {
		s.up.Store(false)
	}
	return nil
}

// observe is the ConsumeErrHandler. It runs on a nats.go goroutine, so it must
// never block: a restart is handed to a goroutine of our own.
func (s *Supervisor) observe(gen uint64, err error) {
	if s.stopped.Load() {
		return
	}

	// Every branch below acts on supervisor state, so a superseded round is
	// dropped up front: its late error must not touch the round that replaced it.
	s.mu.Lock()
	current := gen == s.gen
	s.mu.Unlock()
	if !current {
		return
	}

	disposition := Classify(err)
	if disposition == Transient {
		s.mu.Lock()
		if gen != s.gen {
			s.mu.Unlock()
			return
		}
		// A gap wider than the window starts a fresh run rather than adding to
		// one that ended long ago.
		now := s.nowFn()
		if s.transients > 0 && now.Sub(s.lastTrans) > transientWindow {
			s.transients = 0
		}
		s.transients++
		s.lastTrans = now
		attempts := s.transients
		s.mu.Unlock()

		if attempts < transientEscalation {
			slog.WarnContext(s.ctx, "jetstream consumption hit a recoverable error",
				"consumer", s.name, "attempt", attempts, "error", err)
			return
		}
		slog.WarnContext(s.ctx, "jetstream consumption kept failing, restarting",
			"consumer", s.name, "attempts", attempts, "error", err)
		disposition = Fatal
	}

	if disposition == Stopped {
		// Recorded like a failure even though nothing is restarted: without it,
		// a connection closing while start is still returning would let begin
		// install the dead subscription and publish it as healthy.
		s.mu.Lock()
		if gen == s.gen {
			s.failedGen = gen
			s.up.Store(false)
		}
		s.mu.Unlock()
		slog.ErrorContext(s.ctx, "jetstream consumption ended",
			"consumer", s.name, "error", err)
		return
	}

	// One restart at a time: a single failure can surface as a burst of errors,
	// and each one spawning its own restart would leave competing subscriptions.
	s.mu.Lock()
	if gen != s.gen {
		s.mu.Unlock()
		return
	}
	// Record the failure even while a restart is already running. The
	// replacement is a new generation, and Consume reports a terminal error
	// exactly once: discarding it as a duplicate would let begin install an
	// already-stopped subscription and mark it up, freezing the lane for good
	// behind a green health check.
	s.failedGen = gen
	cur := s.cur
	s.cur = nil
	s.up.Store(false)
	spawn := !s.restarting
	s.restarting = true
	s.mu.Unlock()

	slog.ErrorContext(s.ctx, "jetstream consumption stopped, restarting",
		"consumer", s.name, "error", err)
	// nil when start has not returned the handle yet; begin releases that round.
	if cur != nil {
		cur.Stop()
	}
	if spawn {
		go s.restart()
	}
}

// restart re-establishes consumption, retrying on backoff until it succeeds.
// Giving up would leave the service silently consuming nothing, which is the
// bug; a peer site that is down comes back.
func (s *Supervisor) restart() {
	for attempt := 0; ; attempt++ {
		if !s.sleepFn(s.ctx, backoffStep(attempt)) {
			s.abandonRestart()
			return
		}
		if s.stopped.Load() {
			s.abandonRestart()
			return
		}

		if err := s.begin(); err != nil {
			slog.ErrorContext(s.ctx, "jetstream consumption restart failed, retrying",
				"consumer", s.name, "attempt", attempt+1, "error", err)
			continue
		}

		// Accept the round only if it has not failed already. Clearing
		// restarting under the same lock observe takes closes the window where a
		// replacement dies just as this loop declares victory — the flag stays
		// set, so this loop keeps going instead of stranding a dead round.
		s.mu.Lock()
		if s.failedGen == s.gen {
			s.mu.Unlock()
			slog.WarnContext(s.ctx, "jetstream replacement failed while starting, retrying",
				"consumer", s.name, "attempt", attempt+1)
			continue
		}
		s.transients = 0
		s.restarting = false
		cur := s.cur
		s.mu.Unlock()

		// Stop landing mid-restart would otherwise leave this round running, and
		// begin's up.Store(true) reporting a subscription we just released.
		if s.stopped.Load() {
			s.up.Store(false)
			if cur != nil {
				cur.Stop()
			}
			return
		}
		slog.InfoContext(s.ctx, "jetstream consumption restarted",
			"consumer", s.name, "attempts", attempt+1)
		return
	}
}

// abandonRestart releases the restart slot when the loop gives up without
// installing a round, so a later failure can start a fresh one.
func (s *Supervisor) abandonRestart() {
	s.mu.Lock()
	s.restarting = false
	s.mu.Unlock()
}

// sleep parks for a jittered d, returning false when Stop or context
// cancellation cut the wait short.
func (s *Supervisor) sleep(ctx context.Context, d time.Duration) bool {
	return sleepUntil(ctx, s.done, d)
}
