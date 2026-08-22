// Package jsiter keeps a JetStream pull iterator consuming across the errors
// nats.go surfaces through Next.
//
// Next reports two very different things through one return value. A missed
// idle heartbeat (jetstream.ErrNoHeartbeat, reported by default because
// Messages sets ReportMissingHeartbeats) leaves the iterator live — the client
// has already re-issued the pull request — and the caller is expected to call
// Next again. A server status like "consumer deleted" instead stops the
// iterator for good and the caller is expected to build a new one.
//
// A loop that returns on any error treats the first kind as the second: one
// hiccup on the inter-site link exceeds two heartbeat intervals, the pump
// goroutine exits, and the service consumes nothing more. Nothing notices,
// because the NATS connection is still up and the process is still healthy, so
// the stall lasts until someone restarts the pod. Cross-site consumers see this
// first: their heartbeats and pull expiries cross a supercluster gateway.
//
// Pump absorbs both cases behind the same Next signature the loops already
// call, so an error from Pump.Next means consumption is genuinely over.
package jsiter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
)

// ErrStopped is returned by Next once Stop has been called.
var ErrStopped = errors.New("consumer pump stopped")

// RebuildBackoff spaces the attempts to rebuild a stopped iterator. The last
// entry is reused once attempts run past the schedule, so a peer site that
// stays down is retried every 30s forever rather than giving up — coming back
// on its own is the whole point. Delays are jittered so a fleet that lost the
// same gateway does not rebuild in lockstep.
var RebuildBackoff = []time.Duration{
	100 * time.Millisecond,
	1 * time.Second,
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
}

// transientEscalation caps how many transient errors may arrive back to back
// before the pump stops trusting the iterator and rebuilds it. A stall that
// never delivers a message looks exactly like repeated heartbeat misses, and
// retrying forever against a subscription the server no longer feeds is the
// silent-stall failure in a new shape.
const transientEscalation = 3

// Disposition is what a caller should do about an error returned by Next.
type Disposition int

const (
	// Stopped means consumption is over: the connection or context is gone and
	// no iterator can be built against it.
	Stopped Disposition = iota
	// Transient means the iterator is still live — call Next again.
	Transient
	// Fatal means the iterator was stopped and a new one must be built.
	Fatal
)

func (d Disposition) String() string {
	switch d {
	case Stopped:
		return "stopped"
	case Transient:
		return "transient"
	case Fatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// Classify maps an error from a pull iterator's Next onto the action it calls
// for. Unrecognized errors are Fatal: rebuilding costs one consumer API call,
// while mistaking a dead iterator for a live one costs every message until the
// pod restarts.
//
// jetstream.ErrMsgIteratorClosed is Fatal here because Pump checks its own stop
// signal first — an unexplained closure means the subscription died underneath.
func Classify(err error) Disposition {
	switch {
	case err == nil:
		return Transient
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return Stopped
	case errors.Is(err, jetstream.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionClosed):
		return Stopped
	case errors.Is(err, jetstream.ErrNoHeartbeat), errors.Is(err, nats.ErrTimeout):
		return Transient
	default:
		return Fatal
	}
}

// Iterator is the pull-iterator surface Pump drives. Both jetstream's
// MessagesContext and the o11y wrapper's iterator satisfy it.
type Iterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
	Stop()
}

// NewIterator builds a fresh iterator. It runs again on every rebuild, so it
// must re-resolve the consumer rather than close over a stale handle.
type NewIterator func(context.Context) (Iterator, error)

// Pump wraps an Iterator so transient errors are retried and a stopped iterator
// is rebuilt. It satisfies the same Next signature the consumer loops already
// use, so adopting it is a change of construction, not of loop shape.
type Pump struct {
	name    string
	ctx     context.Context
	newIter NewIterator

	// sleepFn parks for one backoff step, reporting false when Stop or context
	// cancellation cut the wait short. It is a field so tests can drive the
	// rebuild path without real delays; the default jitters the step it is given.
	sleepFn func(context.Context, time.Duration) bool

	mu   sync.Mutex
	iter Iterator

	up      atomic.Bool
	stopped atomic.Bool
	done    chan struct{}
	once    sync.Once
}

// New builds the first iterator and returns a Pump driving it. A failure here
// is a startup failure — the caller decides whether to exit.
//
// ctx bounds every later rebuild, so pass the service context rather than a
// per-call one.
func New(ctx context.Context, name string, newIter NewIterator) (*Pump, error) {
	p := &Pump{name: name, ctx: ctx, newIter: newIter, done: make(chan struct{})}
	p.sleepFn = p.sleep

	iter, err := newIter(ctx)
	if err != nil {
		return nil, fmt.Errorf("build %s consumer iterator: %w", name, err)
	}
	p.iter = iter
	p.up.Store(true)
	return p, nil
}

// IsUp reports whether the pump currently holds a live iterator. It goes false
// while a rebuild is in flight and after Stop.
//
// Deliberately not "time since the last message": a stream that is merely quiet
// is healthy, and treating silence as a stall would restart idle workers.
func (p *Pump) IsUp() bool { return p.up.Load() }

// HealthCheck reports the pump's state as a dependency probe. Wire it next to
// the NATS connection check: the connection being up says nothing about whether
// this consumer is still receiving, which is the failure that hides.
func (p *Pump) HealthCheck() health.Check {
	return health.Check{
		Name: "jetstream-consumer:" + p.name,
		Probe: func(context.Context) error {
			if !p.IsUp() {
				return fmt.Errorf("%s consumer iterator is down", p.name)
			}
			return nil
		},
	}
}

// Next returns the next message, absorbing transient errors and rebuilding the
// iterator when the server stops it. An error means consumption is over:
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
			if transients < transientEscalation {
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

// Stop releases the current iterator and makes every later Next return
// ErrStopped. It is idempotent and unblocks a Next parked on rebuild backoff,
// so it is safe to call from a shutdown hook.
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
	return p.iter
}

// rebuild replaces a stopped iterator, retrying on backoff until it succeeds.
// It returns only when the pump is stopped or its context is done — giving up
// would leave the service silently consuming nothing, which is the bug.
func (p *Pump) rebuild() error {
	p.up.Store(false)
	p.current().Stop()

	for attempt := 0; ; attempt++ {
		if !p.sleepFn(p.ctx, backoffStep(attempt)) {
			if p.stopped.Load() {
				return ErrStopped
			}
			return fmt.Errorf("rebuild %s consumer iterator: %w", p.name, p.ctx.Err())
		}
		if p.stopped.Load() {
			return ErrStopped
		}

		iter, err := p.newIter(p.ctx)
		if err != nil {
			slog.ErrorContext(p.ctx, "jetstream iterator rebuild failed, retrying",
				"consumer", p.name, "attempt", attempt+1, "error", err)
			continue
		}

		p.mu.Lock()
		p.iter = iter
		p.mu.Unlock()

		// Publish up first, then re-read stopped: Stop sets stopped before it
		// clears up, so this ordering cannot leave a stopped pump reporting
		// healthy. A Stop landing while the build was in flight already released
		// the old iterator, so this one would otherwise be left running with
		// nobody reading it — its messages sit un-acked until AckWait.
		p.up.Store(true)
		if p.stopped.Load() {
			p.up.Store(false)
			iter.Stop()
			return ErrStopped
		}
		slog.InfoContext(p.ctx, "jetstream iterator rebuilt",
			"consumer", p.name, "attempts", attempt+1)
		return nil
	}
}

// backoffStep returns the schedule entry for attempt, reusing the last one once
// attempts run past the schedule.
func backoffStep(attempt int) time.Duration {
	if attempt >= len(RebuildBackoff) {
		attempt = len(RebuildBackoff) - 1
	}
	return RebuildBackoff[attempt]
}

// sleep parks for a jittered d, returning false when Stop or context
// cancellation cut the wait short.
func (p *Pump) sleep(ctx context.Context, d time.Duration) bool {
	return sleepUntil(ctx, p.done, d)
}

// sleepUntil is the one backoff wait in this package: jitter and cancellation
// semantics stay identical for the pump and the supervisor.
func sleepUntil(ctx context.Context, done <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(Jitter(d))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	case <-ctx.Done():
		return false
	}
}

// Jitter applies AWS "equal jitter": half the step plus a random amount up to
// the other half, so a fleet that lost one gateway does not rebuild in lockstep
// — every recovering consumer must space its own retries, not just these two.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	// #nosec G404 -- rebuild jitter, not security-sensitive
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
