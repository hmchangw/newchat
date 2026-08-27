package jsiter

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
)

// Nexter is the message-delivery half of Iterator, for callers that only read.
type Nexter interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
}

// Iterator is the pull-iterator surface Pump drives. Both jetstream's
// MessagesContext and the o11y wrapper's iterator satisfy it.
type Iterator interface {
	Nexter
	Stop()
}

// OpenIterator builds a fresh iterator. It runs again on every rebuild, so it
// must re-resolve the consumer rather than close over a stale handle.
type OpenIterator func(context.Context) (Iterator, error)

// Pump wraps an Iterator so recoverable errors are retried and a stopped
// iterator is rebuilt, behind the same Next signature the loops already call.
//
// Next is not safe for concurrent use; drive one Pump from one goroutine.
type Pump struct {
	name string
	ctx  context.Context
	open OpenIterator

	// sleepFn parks for one backoff step; a field so tests skip real delays.
	sleepFn func(context.Context, time.Duration) bool

	// mu guards cur alone: Stop reads it from the shutdown goroutine while
	// rebuild swaps it. Everything below belongs to the Next goroutine.
	mu  sync.Mutex
	cur Iterator

	// attempt indexes RebuildBackoff across consecutive rebuilds, not just the
	// tries within one, so a replacement that dies on arrival keeps backing off
	// instead of hammering the first step. Windowed by lastAttempt — see
	// SeedAttempt for why a delivery is not taken as proof of health.
	attempt     int
	lastAttempt time.Time

	// nowFn is a field so tests drive the escalation window without real time.
	nowFn func() time.Time

	up      atomic.Bool
	stopped atomic.Bool
	done    chan struct{}
	once    sync.Once
}

// NewPump builds the first iterator and returns a Pump driving it. ctx bounds
// every later rebuild, so pass the service context rather than a per-call one.
func NewPump(ctx context.Context, name string, open OpenIterator) (*Pump, error) {
	p := &Pump{name: name, ctx: ctx, open: open, done: make(chan struct{}), nowFn: time.Now}
	p.sleepFn = p.sleep

	iter, err := open(ctx)
	if err != nil {
		return nil, fmt.Errorf("build %s consumer iterator: %w", name, err)
	}
	p.cur = iter
	p.up.Store(true)
	return p, nil
}

// IsUp reports whether a live iterator is held: false while rebuilding and
// after Stop. Deliberately not "time since the last message" — a quiet stream
// is healthy, and treating silence as a stall would restart idle workers.
func (p *Pump) IsUp() bool { return p.up.Load() }

// HealthCheck probes this consumer, which a live NATS connection says nothing
// about.
func (p *Pump) HealthCheck() health.Check { return Check(p.name, p.IsUp) }

// Next returns the next message, absorbing recoverable errors and rebuilding
// when the server stops the iterator. An error means consumption is over:
// ErrStopped after Stop, or the underlying terminal error.
func (p *Pump) Next(opts ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	transients := 0
	for {
		if p.stopped.Load() {
			return nil, nil, ErrStopped
		}

		msgCtx, msg, err := p.current().Next(opts...)
		if err == nil {
			return msgCtx, msg, nil
		}
		if p.stopped.Load() {
			return nil, nil, ErrStopped
		}

		disposition := Classify(err)
		if disposition == Transient {
			transients++
			if transients < TransientEscalation {
				slog.WarnContext(p.ctx, "jetstream iterator hit a recoverable error, retrying",
					"consumer", p.name, "attempt", transients, "error", err)
				continue
			}
			slog.WarnContext(p.ctx, "jetstream iterator kept failing without delivering, rebuilding",
				"consumer", p.name, "attempts", transients, "error", err)
			disposition = Fatal
		}

		if disposition == Stopped {
			p.up.Store(false)
			slog.ErrorContext(p.ctx, "jetstream iterator ended consumption",
				"consumer", p.name, "error", err)
			return nil, nil, fmt.Errorf("consume %s: %w", p.name, err)
		}

		slog.ErrorContext(p.ctx, "jetstream iterator stopped, rebuilding",
			"consumer", p.name, "error", err)
		if rebuildErr := p.rebuild(); rebuildErr != nil {
			return nil, nil, rebuildErr
		}
		transients = 0
	}
}

// Stop releases the iterator and makes every later Next return ErrStopped. It
// is idempotent and unblocks a Next parked on backoff, so it is safe in a
// shutdown hook.
func (p *Pump) Stop() {
	p.once.Do(func() {
		p.stopped.Store(true)
		p.up.Store(false)
		close(p.done)
		p.current().Stop()
	})
}

// current reads the live iterator under the lock that rebuild swaps it under.
func (p *Pump) current() Iterator {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur
}

// rebuild replaces a stopped iterator, retrying on backoff until it succeeds.
// Giving up would leave the service silently consuming nothing, which is the
// bug this package exists to prevent.
func (p *Pump) rebuild() error {
	p.up.Store(false)
	p.current().Stop()

	now := p.nowFn()
	first := SeedAttempt(p.attempt, p.lastAttempt, now)
	p.lastAttempt = now

	for attempt := first; ; attempt++ {
		p.attempt = attempt

		if !p.sleepFn(p.ctx, BackoffStep(attempt)) {
			if p.stopped.Load() {
				return ErrStopped
			}
			slog.ErrorContext(p.ctx, "jetstream iterator rebuild abandoned",
				"consumer", p.name, "error", p.ctx.Err())
			return fmt.Errorf("rebuild %s consumer iterator: %w", p.name, p.ctx.Err())
		}
		if p.stopped.Load() {
			return ErrStopped
		}

		iter, err := p.open(p.ctx)
		if err != nil {
			slog.ErrorContext(p.ctx, "jetstream iterator rebuild failed, retrying",
				"consumer", p.name, "attempt", attempt+1, "error", err)
			continue
		}

		p.mu.Lock()
		p.cur = iter
		p.mu.Unlock()

		// Publish up before re-reading stopped: Stop sets stopped first, so this
		// ordering cannot leave a stopped pump reporting healthy.
		p.up.Store(true)
		if p.stopped.Load() {
			p.up.Store(false)
			iter.Stop()
			return ErrStopped
		}
		// attempt+1, not 0: this iterator has merely been built, not proven.
		p.attempt = attempt + 1
		slog.InfoContext(p.ctx, "jetstream iterator rebuilt",
			"consumer", p.name, "attempts", attempt+1)
		return nil
	}
}

func (p *Pump) sleep(ctx context.Context, d time.Duration) bool {
	return SleepUntil(ctx, p.done, d)
}
