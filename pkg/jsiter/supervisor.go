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

	mu         sync.Mutex
	cur        ConsumeContext
	transients int
	restarting bool

	up      atomic.Bool
	stopped atomic.Bool
	done    chan struct{}
	once    sync.Once
}

// Supervise starts consumption and returns the supervisor watching it. A
// failure here is a startup failure — the caller decides whether to exit.
//
// ctx bounds every later restart, so pass the service context.
func Supervise(ctx context.Context, name string, start NewConsume) (*Supervisor, error) {
	s := &Supervisor{name: name, ctx: ctx, start: start, done: make(chan struct{})}
	s.sleepFn = s.sleep

	cc, err := s.begin()
	if err != nil {
		return nil, err
	}
	s.cur = cc
	s.up.Store(true)
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

// begin starts one round of consumption wired to observe.
func (s *Supervisor) begin() (ConsumeContext, error) {
	cc, err := s.start(s.ctx, s.observe)
	if err != nil {
		return nil, fmt.Errorf("start %s consumption: %w", s.name, err)
	}
	return cc, nil
}

// observe is the ConsumeErrHandler. It runs on a nats.go goroutine, so it must
// never block: a restart is handed to a goroutine of our own.
func (s *Supervisor) observe(err error) {
	if s.stopped.Load() {
		return
	}

	disposition := Classify(err)
	if disposition == Transient {
		s.mu.Lock()
		s.transients++
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
		s.up.Store(false)
		slog.ErrorContext(s.ctx, "jetstream consumption ended",
			"consumer", s.name, "error", err)
		return
	}

	// One restart at a time: a single failure can surface as a burst of errors,
	// and each one spawning its own restart would leave competing subscriptions.
	s.mu.Lock()
	if s.restarting {
		s.mu.Unlock()
		return
	}
	s.restarting = true
	cur := s.cur
	s.mu.Unlock()

	s.up.Store(false)
	slog.ErrorContext(s.ctx, "jetstream consumption stopped, restarting",
		"consumer", s.name, "error", err)
	if cur != nil {
		cur.Stop()
	}
	go s.restart()
}

// restart re-establishes consumption, retrying on backoff until it succeeds.
// Giving up would leave the service silently consuming nothing, which is the
// bug; a peer site that is down comes back.
func (s *Supervisor) restart() {
	defer func() {
		s.mu.Lock()
		s.restarting = false
		s.mu.Unlock()
	}()

	for attempt := 0; ; attempt++ {
		if !s.sleepFn(s.ctx, backoffStep(attempt)) {
			return
		}
		if s.stopped.Load() {
			return
		}

		cc, err := s.begin()
		if err != nil {
			slog.ErrorContext(s.ctx, "jetstream consumption restart failed, retrying",
				"consumer", s.name, "attempt", attempt+1, "error", err)
			continue
		}

		s.mu.Lock()
		s.cur = cc
		s.transients = 0
		s.mu.Unlock()

		// Stop landing mid-restart would otherwise leave this round running.
		if s.stopped.Load() {
			cc.Stop()
			return
		}
		s.up.Store(true)
		slog.InfoContext(s.ctx, "jetstream consumption restarted",
			"consumer", s.name, "attempts", attempt+1)
		return
	}
}

// sleep parks for a jittered d, returning false when Stop or context
// cancellation cut the wait short.
func (s *Supervisor) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(jitter(d))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.done:
		return false
	case <-ctx.Done():
		return false
	}
}
